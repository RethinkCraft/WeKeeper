"""Durable, independent receive journal for the pinned native protocol API.

No model, business write, automatic reply, or chat read-state change lives here.
Use one retained inbox and ProtocolGateway journal per required consumer. Only
activate after initial profile sync has finished: the caller supplies an explicit
head cursor and a recorded readiness proof. A cursor alone cannot distinguish
old history committed late by initial sync. This module does not infer readiness
from an event's ``origin``; catchup events after activation remain eligible.

Native message.committed events carry messageId/threadId/direction/origin, not
the original serverId. Entire event pages are durably staged before projection
reads. A second transaction saves all resolved identities/requests and advances
the local cursor; only then is the required consumer ACK sent. Missing or
incomplete projection leaves the staged page recoverable and unacknowledged.

The pinned store allocates consecutive per-account event sequences and prunes
only prefixes. A gap, changed binding, conflicting content, or remote ACK ahead
of local durable work stops progress; none silently resets the cursor to head.
The gateway and inbox databases must not be independently restored or replaced.
Gateway identity checks are not an atomic fence against concurrent restoration.
This journal establishes durable receive deduplication, not exactly-once sends
or phone delivery. Request completion is an explicit caller decision.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading

from .protocol_gateway import ProtocolGateway, _cursor, _identifier, _json, _server_id


class InboxError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class InboxBatch:
    committed_cursor: str
    acknowledged_cursor: str
    received_events: int = 0
    new_requests: int = 0


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _private_file(path):
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
        os.close(descriptor)
        raise InboxError("unsafe_inbox_file")
    os.fchmod(descriptor, 0o600)
    return descriptor


class ProtocolInbox:
    """One account/consumer, with a process lock for its entire open lifetime.

    ``activate`` is needed once, with a verified initial-sync barrier and head.
    Reopen the same file and call ``poll`` to resume; never choose a new head on
    restart. Keep the gateway transport journal too, for server/account binding.
    ``pending_requests`` returns persisted work even if the gateway is offline.
    """

    def __init__(self, gateway: ProtocolGateway, *, path, own_username, consumer):
        self.gateway = gateway
        self.own_username = _identifier(own_username, maximum=256)
        if not isinstance(consumer, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", consumer):
            raise InboxError("invalid_inbox_consumer")
        self.consumer = consumer
        self.path = Path(path).expanduser().absolute()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._lease = _private_file(Path(str(self.path) + ".lock"))
        try:
            fcntl.flock(self._lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self._lease)
            self._lease = None
            raise InboxError("inbox_already_open") from None
        self._db = None
        try:
            for suffix in ("", "-wal", "-shm"):
                if Path(str(self.path) + suffix).is_symlink():
                    raise InboxError("unsafe_inbox_file")
            os.close(_private_file(self.path))
            self._db = sqlite3.connect(self.path, timeout=10, isolation_level=None,
                                       check_same_thread=False)
            self._db.row_factory = sqlite3.Row
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.execute("PRAGMA foreign_keys=ON")
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS inbox_meta (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    schema_version INTEGER NOT NULL, endpoint TEXT NOT NULL,
                    account_id TEXT NOT NULL, own_username TEXT NOT NULL,
                    consumer TEXT NOT NULL, cutover_cursor TEXT, readiness_proof TEXT,
                    committed_cursor TEXT, acknowledged_cursor TEXT);
                CREATE TABLE IF NOT EXISTS inbox_page (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    after_cursor TEXT NOT NULL, next_cursor TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS inbox_events (
                    cursor TEXT PRIMARY KEY, raw_json TEXT NOT NULL,
                    received_at TEXT NOT NULL, committed INTEGER NOT NULL DEFAULT 0,
                    message_id TEXT, request_id TEXT, projection_json TEXT,
                    classification TEXT);
                CREATE TABLE IF NOT EXISTS inbox_requests (
                    request_id TEXT PRIMARY KEY, own_username TEXT NOT NULL,
                    account_id TEXT NOT NULL, thread_id TEXT NOT NULL,
                    server_id TEXT NOT NULL, message_id TEXT NOT NULL,
                    event_cursor TEXT NOT NULL, received_at TEXT NOT NULL,
                    message_json TEXT NOT NULL, content_json TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'pending', completed_at TEXT,
                    UNIQUE(own_username,thread_id,server_id));
                CREATE TABLE IF NOT EXISTS inbox_message_ids (
                    message_id TEXT PRIMARY KEY, request_id TEXT NOT NULL
                    REFERENCES inbox_requests(request_id));
            """)
            with self._transaction():
                self._db.execute("""INSERT OR IGNORE INTO inbox_meta
                    (singleton,schema_version,endpoint,account_id,own_username,consumer)
                    VALUES(1,1,?,?,?,?)""", (gateway.base_url, gateway.account_id,
                                             self.own_username, consumer))
                meta = self._meta()
                if (meta["schema_version"] != 1 or
                        (meta["endpoint"], meta["account_id"], meta["own_username"], meta["consumer"]) !=
                        (gateway.base_url, gateway.account_id, self.own_username, consumer)):
                    raise InboxError("inbox_binding_changed")
        except BaseException:
            self.close()
            raise

    @contextmanager
    def _transaction(self):
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._db.execute("COMMIT")
        except BaseException:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
            raise

    def _meta(self):
        return self._db.execute("SELECT * FROM inbox_meta WHERE singleton=1").fetchone()

    def activate(self, *, cutover_cursor, initial_sync_ready):
        """Record an operator/checkpoint proof and explicit post-sync event head.

        initial_sync_ready is a nonempty evidence reference, not a truthy flag.
        This declaration must be established outside the inbox. It cannot be
        reconstructed from live/catchup labels. Registration failure is retained
        locally and can be retried with these exact same activation parameters.
        """
        _cursor(cutover_cursor)
        if (not isinstance(initial_sync_ready, str) or not initial_sync_ready.strip()
                or len(initial_sync_ready) > 4096):
            raise InboxError("inbox_initial_sync_proof_required")
        with self._lock:
            self.gateway.require_account(self.own_username)
            with self._transaction():
                meta = self._meta()
                if meta["cutover_cursor"] is None:
                    self._db.execute("""UPDATE inbox_meta SET cutover_cursor=?,readiness_proof=?,
                        committed_cursor=?,acknowledged_cursor=? WHERE singleton=1""",
                        (cutover_cursor, initial_sync_ready, cutover_cursor, cutover_cursor))
                elif (meta["cutover_cursor"], meta["readiness_proof"]) != (cutover_cursor, initial_sync_ready):
                    raise InboxError("inbox_cutover_changed")
            state = self.gateway.register_consumer(self.consumer, after=cutover_cursor)
            self._reconcile(state)

    def _reconcile(self, state):
        meta = self._meta()
        if state["registeredAfter"] != meta["cutover_cursor"]:
            raise InboxError("inbox_consumer_binding_changed")
        remote = int(state["acknowledgedCursor"])
        committed = int(meta["committed_cursor"])
        if remote > committed:
            raise InboxError("inbox_remote_ack_ahead")
        if (remote < int(meta["acknowledged_cursor"])
                or int(state["deliveredCursor"]) < committed):
            raise InboxError("inbox_remote_cursor_regressed")
        with self._transaction():
            self._db.execute("UPDATE inbox_meta SET acknowledged_cursor=? WHERE singleton=1",
                             (state["acknowledgedCursor"],))

    def _ack_committed(self):
        meta = self._meta()
        if meta["committed_cursor"] == meta["acknowledged_cursor"]:
            return
        result = self.gateway.ack(self.consumer, meta["committed_cursor"])
        if result["cursor"] != meta["committed_cursor"]:
            raise InboxError("inbox_remote_ack_ahead")
        with self._transaction():
            self._db.execute("UPDATE inbox_meta SET acknowledged_cursor=? WHERE singleton=1",
                             (result["cursor"],))

    def _stage(self, page):
        after = self._meta()["committed_cursor"]
        expected = int(after)
        for event in page["items"]:
            expected += 1
            if event["cursor"] != str(expected):
                raise InboxError("inbox_event_cursor_gap")
        if page["nextCursor"] != str(expected):
            raise InboxError("inbox_event_cursor_gap")
        received_at = _now()
        with self._transaction():
            for event in page["items"]:
                raw = _json(event)
                prior = self._db.execute("SELECT raw_json FROM inbox_events WHERE cursor=?",
                                         (event["cursor"],)).fetchone()
                if prior is not None and prior["raw_json"] != raw:
                    raise InboxError("inbox_event_content_conflict")
                data = event.get("data")
                message_id = (data.get("messageId") if event["kind"] == "message.committed" else
                              data.get("receivedId") if event["kind"] == "message.received" else None)
                self._db.execute("""INSERT OR IGNORE INTO inbox_events
                    (cursor,raw_json,received_at,message_id) VALUES(?,?,?,?)""",
                    (event["cursor"], raw, received_at, message_id))
                self._stage_event(event)
            self._db.execute("INSERT INTO inbox_page VALUES(1,?,?)", (after, page["nextCursor"]))

    def _resolve(self, event):
        if event["kind"] != "message.committed":
            return None
        data = event["data"]
        if data.get("direction") == "outgoing":
            return None
        if data.get("direction") != "incoming":
            raise InboxError("inbox_event_direction_unresolved")
        message = self.gateway.get_message(data["messageId"], thread_id=data["threadId"])
        # wechat-core/profile.rs::normalized_message intentionally clears both
        # sender and direction for these system types. They are complete system
        # notices, not unfinished user messages. Preserve the projection and
        # classification without creating a reply request.
        if type(message.get("type")) is int and message["type"] in (10000, 10002):
            return None, message, None
        # These fields are nullable in the API. Never dedupe or quote a partial
        # projection using body text, local message ID, or an invented sender.
        if (message.get("serverId") is None or message.get("senderId") is None
                or message.get("direction") is None or message.get("type") is None
                or (message.get("type") == 1 and message.get("body") is None)):
            raise InboxError("inbox_message_projection_incomplete")
        _server_id(message["serverId"])
        _identifier(message["senderId"], maximum=256)
        if (message["direction"] != "incoming" or type(message["type"]) is not int
                or (message.get("body") is not None and not isinstance(message["body"], str))):
            raise InboxError("inbox_message_projection_conflict")
        identity = [self.own_username, message["threadId"], message["serverId"]]
        request_id = "protocol:" + hashlib.sha256(_json(identity).encode()).hexdigest()
        # IDs/version and mutable mention/media enrichment are not message
        # content. Preserve the full first projection separately for consumers.
        content = {key: message.get(key) for key in
                   ("senderId", "direction", "type", "appMessageType", "body", "quote")}
        return request_id, message, _json(content)

    def _commit_staged(self):
        page = self._db.execute("SELECT * FROM inbox_page WHERE singleton=1").fetchone()
        if page is None:
            return 0, 0
        rows = self._db.execute("SELECT * FROM inbox_events WHERE committed=0 ORDER BY CAST(cursor AS INTEGER)").fetchall()
        # No HTTP request runs inside a local write transaction. If any read
        # fails the entire page remains staged, with original receive times.
        resolved = [(row, self._resolve(json.loads(row["raw_json"]))) for row in rows]
        return self._commit_resolved(page, rows, resolved)

    def _commit_resolved(self, page, rows, resolved):
        """Apply already resolved snapshots; caller owns the local DB mutex."""
        added = 0
        with self._transaction():
            if self._meta()["committed_cursor"] != page["after_cursor"]:
                raise InboxError("inbox_local_cursor_conflict")
            current_page = self._db.execute("SELECT * FROM inbox_page WHERE singleton=1").fetchone()
            current_rows = self._db.execute("SELECT cursor,raw_json FROM inbox_events WHERE committed=0 ORDER BY CAST(cursor AS INTEGER)").fetchall()
            if (current_page is None or dict(current_page) != dict(page)
                    or [(r["cursor"], r["raw_json"]) for r in current_rows] !=
                       [(r["cursor"], r["raw_json"]) for r in rows]):
                raise InboxError("inbox_staged_page_changed")
            for row, result in resolved:
                if result is not None:
                    request_id, message, content = result
                    self._db.execute("UPDATE inbox_events SET projection_json=?,classification=? WHERE cursor=?",
                                     (_json(message), "incoming_request" if request_id else message.get("disposition", "system_notice"), row["cursor"]))
                    if request_id is None:
                        self._save_non_request(message)
                        continue
                    prior = self._db.execute("SELECT content_json FROM inbox_requests WHERE request_id=?", (request_id,)).fetchone()
                    if prior is not None and prior["content_json"] != content:
                        raise InboxError("inbox_message_content_conflict")
                    mapped = self._db.execute("SELECT request_id FROM inbox_message_ids WHERE message_id=?", (message["id"],)).fetchone()
                    if mapped is not None and mapped["request_id"] != request_id:
                        raise InboxError("inbox_message_identity_conflict")
                    if prior is None:
                        self._db.execute("""INSERT INTO inbox_requests
                            (request_id,own_username,account_id,thread_id,server_id,message_id,
                             event_cursor,received_at,message_json,content_json)
                            VALUES(?,?,?,?,?,?,?,?,?,?)""", (request_id, self.own_username,
                            self.gateway.account_id, message["threadId"], message["serverId"],
                            message["id"], row["cursor"], row["received_at"], _json(message), content))
                        added += 1
                    self._db.execute("INSERT OR IGNORE INTO inbox_message_ids VALUES(?,?)", (message["id"], request_id))
                    self._db.execute("UPDATE inbox_events SET request_id=? WHERE cursor=?", (request_id, row["cursor"]))
                    self._save_request(request_id, message)
                else:
                    kind = json.loads(row["raw_json"])["kind"]
                    self._db.execute("UPDATE inbox_events SET classification=? WHERE cursor=?",
                                     ("outgoing" if kind == "message.committed" else "non_message", row["cursor"]))
            self._db.execute("UPDATE inbox_events SET committed=1 WHERE committed=0")
            self._db.execute("UPDATE inbox_meta SET committed_cursor=? WHERE singleton=1", (page["next_cursor"],))
            self._db.execute("DELETE FROM inbox_page WHERE singleton=1")
        return len(rows), added

    def _save_non_request(self, message):
        """Subclass hook inside the page transaction for source-state changes."""

    def _stage_event(self, event):
        """Subclass hook inside raw custody transaction; no network allowed."""

    def _save_request(self, request_id, message):
        """Subclass hook after identity/aliases exist, inside page transaction."""

    def _completion_blocked(self, request_id):
        return False

    def poll(self, *, limit=100):
        """Recover an unfinished page/ACK, then ingest at most one new page."""
        with self._lock:
            meta = self._meta()
            if meta["cutover_cursor"] is None:
                raise InboxError("inbox_not_activated")
            self.gateway.require_account(self.own_username)
            self._reconcile(self.gateway.register_consumer(self.consumer, after=meta["cutover_cursor"]))
            self._ack_committed()
            if self._db.execute("SELECT 1 FROM inbox_page").fetchone() is None:
                page = self.gateway.receive(consumer=self.consumer, after=meta["committed_cursor"], limit=limit)
                self._reconcile(page["consumer"])
                if not page["items"]:
                    state = self._meta()
                    return InboxBatch(state["committed_cursor"], state["acknowledged_cursor"])
                self._stage(page)
            count, added = self._commit_staged()
            self._ack_committed()
            state = self._meta()
            return InboxBatch(state["committed_cursor"], state["acknowledged_cursor"], count, added)

    def pending_requests(self, *, limit=100):
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise InboxError("invalid_inbox_request_limit")
        with self._lock:
            rows = self._db.execute("""SELECT * FROM inbox_requests WHERE state='pending'
                ORDER BY CAST(event_cursor AS INTEGER) LIMIT ?""", (limit,)).fetchall()
            return [{key: json.loads(value) if key == "message_json" else value
                     for key, value in dict(row).items() if key != "content_json"} for row in rows]

    def request_state(self, request_id):
        """Read business custody state without claiming phone delivery."""
        _identifier(request_id)
        with self._lock:
            row = self._db.execute("SELECT state FROM inbox_requests WHERE request_id=?", (request_id,)).fetchone()
            if row is None:
                raise InboxError("inbox_request_not_found")
            return row[0]

    def complete(self, request_id):
        """Record caller completion; this does not assert send/phone delivery."""
        _identifier(request_id)
        with self._lock, self._transaction():
            state = self.request_state(request_id)
            if state == "completed":
                return
            if state == "quarantined" or self._completion_blocked(request_id):
                raise InboxError("inbox_request_quarantined")
            changed = self._db.execute("""UPDATE inbox_requests SET state='completed',
                completed_at=COALESCE(completed_at,?) WHERE request_id=?""", (_now(), request_id)).rowcount
            if not changed:
                raise InboxError("inbox_request_not_found")

    def close(self):
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None
            if self._lease is not None:
                os.close(self._lease)
                self._lease = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
