"""Read-only bridge from retained protocol snapshots into the existing archive.

The reader uses unregistered ``GET /events`` and ``GET /received/messages/{id}``.
It does not register a consumer, activate, or ack, so it is not a required
consumer and cannot hold B2 projection or the bot's receive lane. It never
opens an official WeChat database. Missing snapshots are recorded and skipped.
"""
from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sqlite3
import time

from chatlog_keeper.bot.protocol_gateway import GatewayError


SOURCE_PATH = "protocol-received-snapshots"
SOURCE_IDENTITY = {"kind": "protocol_received_snapshot", "version": 1}
SOURCE_FINGERPRINT = "protocol-received-snapshots-v1"
TABLE_PREFIX = "received:"
ARCHIVEABLE = frozenset({"ready", "outgoing", "system"})


class BridgeError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class BridgePaused(BridgeError):
    def __init__(self):
        super().__init__("bridge_paused")


class ArchiveBridge:
    def __init__(self, *, gateway, archive_dir, state_path, archive_account,
                 floor=None, limit=100, before_page=None, pace=0):
        self.gateway = gateway
        self.archive_dir = Path(archive_dir)
        self.archive_account = archive_account
        if not isinstance(archive_account, str) or not archive_account:
            raise BridgeError("invalid_archive_account")
        if floor is not None and (not isinstance(floor, datetime) or floor.tzinfo is None):
            raise BridgeError("invalid_catchup_floor")
        supplied_floor = int(floor.timestamp()) if floor is not None else None
        if supplied_floor is not None and not 0 <= supplied_floor <= 253402214400:
            raise BridgeError("invalid_catchup_floor")
        self.limit = limit
        self.before_page = before_page
        if type(pace) not in (int, float) or pace < 0 or pace > 5:
            raise BridgeError("invalid_bridge_pace")
        self.pace = pace
        self.state_path = Path(state_path)
        if self.state_path.is_symlink():
            raise BridgeError("unsafe_bridge_state")
        if supplied_floor is None and not self.state_path.is_file():
            raise BridgeError("catchup_floor_required")
        self.state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.state_path, timeout=5, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS staged(
                id INTEGER PRIMARY KEY CHECK(id=1),
                after_cursor TEXT NOT NULL,
                next_cursor TEXT NOT NULL,
                items_json TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS assignments(
                received_id TEXT PRIMARY KEY,
                row_id INTEGER NOT NULL UNIQUE);
            CREATE TABLE IF NOT EXISTS gaps(
                gap_id INTEGER PRIMARY KEY,
                event_cursor TEXT,
                received_id TEXT,
                reason TEXT NOT NULL,
                detail TEXT NOT NULL,
                recorded_at REAL NOT NULL);
        """)
        self.conn.commit()
        try:
            saved_floor = self.conn.execute("SELECT value FROM meta WHERE key='catchup_floor'").fetchone()
            if saved_floor is None:
                if supplied_floor is None:
                    raise BridgeError("catchup_floor_required")
                self.floor = supplied_floor
                self.conn.execute("INSERT INTO meta VALUES('catchup_floor',?)", (str(self.floor),))
                self.conn.commit()
            else:
                self.floor = int(saved_floor[0])
                if supplied_floor is not None and supplied_floor != self.floor:
                    raise BridgeError("catchup_floor_changed")
        except BaseException:
            self.conn.close()
            raise
        self._source = None

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()

    def cursor(self):
        row = self.conn.execute("SELECT value FROM meta WHERE key='cursor'").fetchone()
        return row[0] if row else "0"

    def start_after_prune(self, resumed_after):
        """Record a pruned prefix and continue from the first retained cursor.

        Used when the gateway would reject cursor 0. This does not read an
        official WeChat database and does not register a consumer.
        """
        if self.cursor() != "0" or self.conn.execute("SELECT COUNT(*) FROM staged").fetchone()[0]:
            return False
        if not isinstance(resumed_after, str) or not resumed_after.isdigit() or resumed_after == "0":
            raise BridgeError("invalid_resume_cursor")
        self._gap("0", None, "events_pruned", {"resumed_after": resumed_after})
        self.conn.execute("INSERT INTO meta(key,value) VALUES('cursor',?)", (resumed_after,))
        self.conn.commit()
        return True

    def gap_count(self):
        return self.conn.execute("SELECT COUNT(*) FROM gaps").fetchone()[0]

    def poll_once(self):
        """Stage one event page, write retained snapshots, then advance the local cursor.

        A transport failure leaves the staged page in place. The next call
        retries that page and does not fetch past it.
        """
        if self.before_page is not None and not self.before_page():
            raise BridgePaused()
        staged = self.conn.execute("SELECT after_cursor,next_cursor,items_json FROM staged WHERE id=1").fetchone()
        if staged is None:
            after = self.cursor()
            page, aligned = self._page_from(after)
            if page is None:
                return {"fetched": False, "inserted": 0, "skipped": 0, "gaps": 0, "caught_up": True}
            payload = json.dumps(page["items"], ensure_ascii=False, sort_keys=True)
            self.conn.execute("INSERT INTO staged(id,after_cursor,next_cursor,items_json) VALUES(1,?,?,?)",
                              (aligned, page["nextCursor"], payload))
            self.conn.commit()
            staged = (aligned, page["nextCursor"], payload)
        after, nxt, payload = staged
        items = json.loads(payload)
        inserted, skipped, gaps = self._write_items(items)
        self.conn.execute("BEGIN IMMEDIATE")
        self.conn.execute("INSERT INTO meta(key,value) VALUES('cursor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (nxt,))
        self.conn.execute("DELETE FROM staged WHERE id=1")
        self.conn.commit()
        return {"fetched": True, "inserted": inserted, "skipped": skipped, "gaps": gaps,
                "caught_up": not items, "cursor": nxt, "after": after}

    def _page_from(self, after):
        page, error = self._fetch(after)
        if error is None:
            return page, after
        if error.code != "event_cursor_expired":
            raise error
        resumed = self._lowest_retained(after)
        self._gap(after, None, "events_pruned", {"resumed_after": resumed})
        page, error = self._fetch(resumed)
        if error is not None:
            raise error
        return page, resumed

    def _lowest_retained(self, after):
        expired = int(after)
        probe = 1 if expired == 0 else expired + 1
        found = None
        while probe < 10 ** 15:
            _page, error = self._fetch(str(probe))
            if error is None:
                found = probe
                break
            if error.code != "event_cursor_expired":
                raise error
            expired = probe
            probe = probe * 2 + 1
        if found is None:
            raise BridgeError("event_cursor_unavailable")
        low = expired
        high = found
        while low + 1 < high:
            mid = (low + high) // 2
            _page, error = self._fetch(str(mid))
            if error is None:
                high = mid
            elif error.code == "event_cursor_expired":
                low = mid
            else:
                raise error
        return str(high)

    def _hold(self):
        if self.pace:
            time.sleep(self.pace)
        if self.before_page is not None and not self.before_page():
            raise BridgePaused()

    def _fetch(self, after):
        self._hold()
        try:
            return self.gateway.receive(after=after, limit=self.limit), None
        except GatewayError as error:
            return None, error

    def _write_items(self, items):
        inserted = skipped = gaps = 0
        grouped = {}
        for event in items:
            if event.get("kind") != "message.received":
                skipped += 1
                continue
            data = event.get("data") or {}
            disposition = data.get("disposition")
            received_id = data.get("receivedId")
            if disposition not in ARCHIVEABLE:
                skipped += 1
                continue
            self._hold()
            try:
                snapshot = self.gateway.get_received_message(received_id)
            except GatewayError as error:
                if error.code == "gateway_transport_unavailable":
                    raise
                self._gap(event.get("cursor"), received_id, "snapshot_missing", {"code": error.code})
                gaps += 1
                continue
            row = self._row(event.get("cursor"), snapshot)
            if row is None:
                skipped += 1
                continue
            if row is False:
                gaps += 1
                continue
            grouped.setdefault(row["group_id"], []).append(row)
        committed, conflicts = self._commit_all(grouped)
        inserted += committed
        gaps += conflicts
        return inserted, skipped, gaps

    def _row(self, cursor, snapshot):
        received_id = snapshot.get("receivedId")
        if snapshot.get("disposition") not in ARCHIVEABLE:
            return None
        created = _unix(snapshot.get("createdAt"))
        thread_id = snapshot.get("threadId")
        server_id = snapshot.get("serverId")
        sender_id = snapshot.get("senderId")
        body = snapshot.get("body")
        if created is None or not thread_id or not server_id or not sender_id or not isinstance(body, str):
            self._gap(cursor, received_id, "snapshot_incomplete", {})
            return False
        if created < self.floor:
            return None
        kind = snapshot.get("type")
        if type(kind) is not int:
            self._gap(cursor, received_id, "snapshot_incomplete", {})
            return False
        quote = snapshot.get("quote") if isinstance(snapshot.get("quote"), dict) else None
        reply = None
        if quote and isinstance(quote.get("serverId"), str):
            reply = {"server_id": quote["serverId"], "text": quote.get("text") if isinstance(quote.get("text"), str) else ""}
        row = {
            "_event_cursor": cursor,
            "_received_id": received_id,
            "group_id": thread_id,
            "row_id": self._row_id(received_id),
            "server_id": server_id,
            "create_time": created,
            "sender_id": sender_id,
            "real_sender_id": sender_id,
            "sender_name": "",
            "local_type": kind,
            "message_content": body,
            "parsed": {
                "msg_type": kind,
                "text": body,
                "parse_status": "parsed" if kind == 1 else "protocol_snapshot",
                "mentions": [],
                "mentions_status": "missing_metadata",
                "reply": reply,
            },
        }
        if snapshot.get("disposition") == "system":
            # A verified revoke is new source evidence, not permission to
            # replace the target's archived body. Other system traffic stays
            # outside this text projection.
            from chatlog_keeper.message_model import parse_record
            parsed = parse_record({**row, "group_id": thread_id})
            if (parsed.get("event") or {}).get("event_type") != "message_revoke":
                return None
            row["parsed"] = parsed
        return row

    def _row_id(self, received_id):
        row = self.conn.execute("SELECT row_id FROM assignments WHERE received_id=?", (received_id,)).fetchone()
        if row:
            return row[0]
        current = self.conn.execute("SELECT COALESCE(MAX(row_id),0) FROM assignments").fetchone()[0]
        assigned = current + 1
        self.conn.execute("INSERT INTO assignments(received_id,row_id) VALUES(?,?)", (received_id, assigned))
        self.conn.commit()
        return assigned

    def _commit_all(self, grouped):
        if not grouped:
            return 0, 0
        inserted = conflicts = 0
        for group_id, rows in grouped.items():
            result = self._commit(group_id, rows)
            inserted += result["inserted"]
            conflicts += result["conflicts"]
        return inserted, conflicts

    def _commit(self, group_id, rows):
        source = self._ensure_source()
        table = TABLE_PREFIX + group_id
        from chatlog_keeper.archive import ArchiveStore
        from chatlog_keeper.core._store_batch import source_batch
        with ArchiveStore(self.archive_dir) as archive:
            with source_batch(archive, enabled=True):
                archive.get_stream(source["source_ref"], source["generation"], table, group_id, 0)
                result = {"inserted": 0, "updated": 0, "message_refs": [], "conflicts": 0}
                for row in rows:
                    existing = archive.conn.execute(
                        "SELECT * FROM messages WHERE account_id=? AND group_id=? AND server_id=?",
                        (self.archive_account, group_id, row["server_id"])).fetchone()
                    conflict = self._overlap_conflict(archive, existing, source, table, row) if existing else None
                    if conflict:
                        self._gap(row["_event_cursor"], row["_received_id"], "snapshot_conflict",
                                  {"field": conflict, "message_ref": existing["message_ref"], "snapshot": row})
                        result["conflicts"] += 1
                    else:
                        saved = archive.commit_page(source["source_ref"], source["generation"], table, group_id,
                                                    [row], mode="backfill", checkpoint=row["row_id"],
                                                    preserve_existing=True)
                        result["inserted"] += saved["inserted"]
                        result["updated"] += saved["updated"]
                        result["message_refs"].extend(saved["message_refs"])
                checkpoint = max(row["row_id"] for row in rows)
                archive.commit_page(source["source_ref"], source["generation"], table, group_id,
                                    [], mode="backfill", checkpoint=checkpoint, complete=True,
                                    preserve_existing=True)
                return result

    @staticmethod
    def _overlap_conflict(archive, existing, source, table, row):
        # A sparse snapshot cannot correct a known identity or rich body. Keep
        # conflicting evidence in the bridge's private gap journal for review.
        for key, value in (("sender_id", row["sender_id"]), ("create_time", row["create_time"]),
                           ("msg_type", row["parsed"]["msg_type"])):
            if existing[key] and existing[key] != value:
                return key
        if existing["msg_type"] == 1 and existing["text"] and existing["text"] != row["parsed"]["text"]:
            return "text"
        prior = archive.get_origin_info(source["source_ref"], source["generation"], table, row["row_id"])
        if prior is not None and prior["message_content"] != row["message_content"]:
            return "source_content"
        return None

    def _ensure_source(self):
        if self._source is not None:
            return self._source
        from chatlog_keeper.archive import ArchiveStore
        with ArchiveStore(self.archive_dir) as archive:
            self._source = archive.register_source(
                self.archive_account, SOURCE_PATH, SOURCE_IDENTITY, SOURCE_FINGERPRINT)
        return self._source

    def _gap(self, cursor, received_id, reason, detail):
        self.conn.execute(
            "INSERT INTO gaps(event_cursor,received_id,reason,detail,recorded_at) VALUES(?,?,?,?,?)",
            (cursor, received_id, reason, json.dumps(detail, sort_keys=True), time.time()))
        self.conn.commit()


def _unix(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return int(parsed.timestamp())


def archive_account(archive_dir):
    """Use the single account already stored in the archive, without migrating it."""
    path = Path(archive_dir) / "archive" / "archive.sqlite"
    con = sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
    try:
        version = con.execute("PRAGMA user_version").fetchone()[0]
        rows = [row[0] for row in con.execute("SELECT DISTINCT account_id FROM messages")]
    finally:
        con.close()
    if len(rows) != 1 or not rows[0]:
        raise BridgeError("archive_account_unresolved")
    if version not in (4, 5):
        raise BridgeError("unsupported_archive_version")
    return rows[0], version


def bot_lanes_hold(status):
    """True while both bot lanes are ready and the account is not logged out.

    A missing account block means the bot's status probe did not finish. That
    is not a logout. A transport error in the last few seconds means this
    bridge should wait.
    """
    if not isinstance(status, dict):
        return False
    account = status.get("account") if isinstance(status.get("account"), dict) else {}
    lanes = status.get("lanes") or {}
    receive = lanes.get("receive") if isinstance(lanes.get("receive"), dict) else {}
    send = lanes.get("send") if isinstance(lanes.get("send"), dict) else {}
    if receive.get("state") != "ready" or send.get("state") != "ready":
        return False
    if account.get("authenticated") is False or account.get("login_required") is True:
        return False
    parsed = _unix(status.get("updated_at")) if isinstance(status.get("updated_at"), str) else None
    if parsed is None or time.time() - parsed > 90:
        return False
    failure = _unix(receive.get("last_failure_at")) if isinstance(receive.get("last_failure_at"), str) else None
    if receive.get("last_error") == "gateway_transport_unavailable" and failure is not None and time.time() - failure < 20:
        return False
    return True
