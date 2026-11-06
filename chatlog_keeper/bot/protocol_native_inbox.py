"""Native receive custody, independent of official-profile projection.

The gateway registers the consumer and records the first cutover atomically.
Only native message.received events create requests. Late profile projection
cannot turn old history into new work. Raw native snapshots stay in the gateway;
unresolved snapshots are also retained here and reported explicitly, while
unrelated ready requests can proceed. No model, tools, or sending is run here.
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
from datetime import datetime

from .protocol_gateway import _json
from .protocol_inbox import InboxBatch, InboxError, ProtocolInbox

# Activate only on start/error/account events, or this idle bound. Ordinary
# receive polls must not contend for the account lock held by projection/send.
_ACTIVATION_INTERVAL_SECONDS = 60.0


# Keep the partial-index predicates identical to the readers. These contain
# only trusted SQL, never event content. Unrelated history must not be scanned
# while the short SQLite mutex is held for a source authorization check.
_BLOCKED = """(classification='quarantined'
    OR (json_extract(raw_json,'$.kind')='message.received'
        AND json_extract(raw_json,'$.data.disposition')='quarantined'))"""
_RECALLED = """json_extract(raw_json,'$.kind')='message.recalled'
    AND json_type(raw_json,'$.data.threadId')='text'
    AND json_type(raw_json,'$.data.serverId')='text'"""


class ProtocolNativeInbox(ProtocolInbox):
    def __init__(self, *args, **kwargs):
        # Base initialization may call our close() on failure. Always acquire
        # the poll serializer before the short-lived SQLite mutex, never reverse.
        self._poll_lock = threading.RLock()
        self._denied_sources = {}  # Negative-only guard if saving a verified block fails.
        self._last_activation_at = 0.0
        self._needs_activation = True
        super().__init__(*args, **kwargs)
        try:
            self._db.execute("""CREATE TABLE IF NOT EXISTS native_checkpoint
                (singleton INTEGER PRIMARY KEY CHECK(singleton=1), checkpoint_json TEXT NOT NULL)""")
            self._db.execute("""CREATE INDEX IF NOT EXISTS native_quarantine_message
                ON inbox_events(message_id) WHERE classification='quarantined'""")
            self._db.execute("""CREATE INDEX IF NOT EXISTS native_request_aliases
                ON inbox_message_ids(request_id,message_id)""")
            self._db.execute(f"""CREATE INDEX IF NOT EXISTS native_blocked_received
                ON inbox_events(message_id) WHERE {_BLOCKED}""")
            self._db.execute(f"""CREATE INDEX IF NOT EXISTS native_blocked_identity
                ON inbox_events(json_extract(projection_json,'$.accountId'),
                    json_extract(projection_json,'$.ownUsername'),
                    json_extract(projection_json,'$.threadId'),
                    json_extract(projection_json,'$.serverId')) WHERE {_BLOCKED}""")
            self._db.execute(f"""CREATE INDEX IF NOT EXISTS native_recalled_identity
                ON inbox_events(json_extract(raw_json,'$.data.threadId'),
                    json_extract(raw_json,'$.data.serverId')) WHERE {_RECALLED}""")
            if self._meta()["cutover_cursor"] is not None and self._checkpoint() is None:
                raise InboxError("inbox_profile_activation_not_native")
        except BaseException:
            self.close()
            raise

    def _checkpoint(self):
        row = self._db.execute("SELECT checkpoint_json FROM native_checkpoint WHERE singleton=1").fetchone()
        return json.loads(row[0]) if row else None

    def _view(self):
        return dict(self._meta()), self._checkpoint()

    def _check_view(self, expected):
        if self._view() != expected:
            raise InboxError("inbox_local_state_changed")

    def _activation(self, *, initialize):
        with self._lock:
            expected = self._view()
        result = self.gateway.activate_native_consumer(self.consumer, own_username=self.own_username)
        checkpoint = result["checkpoint"]
        with self._lock:
            self._check_view(expected)
            with self._transaction():
                previous = self._checkpoint()
                if previous is None:
                    if not initialize:
                        raise InboxError("inbox_not_activated")
                    cursor = checkpoint["eventCursor"]
                    encoded = _json(checkpoint)
                    self._db.execute("INSERT INTO native_checkpoint VALUES(1,?)", (encoded,))
                    self._db.execute("""UPDATE inbox_meta SET cutover_cursor=?,readiness_proof=?,
                        committed_cursor=?,acknowledged_cursor=? WHERE singleton=1""",
                        (cursor, encoded, cursor, cursor))
                elif previous != checkpoint:
                    raise InboxError("inbox_native_checkpoint_changed")
            self._reconcile(result["consumer"])

    def activate(self):
        """First activation only: the server chooses and proves the boundary."""
        with self._poll_lock:
            self._activation(initialize=True)
            self._mark_activated()

    def _mark_activated(self):
        self._last_activation_at = time.monotonic()
        self._needs_activation = False

    def _activation_due(self):
        return (self._needs_activation
                or (time.monotonic() - self._last_activation_at) >= _ACTIVATION_INTERVAL_SECONDS)

    @staticmethod
    def _page_requires_activation(page):
        for item in page.get("items") or ():
            kind = item.get("kind") if isinstance(item, dict) else None
            if isinstance(kind, str) and (kind.startswith("account.") or kind.startswith("operation.")):
                return True
        return False

    def _resolve(self, event):
        if event["kind"] != "message.received":
            return None
        data = event["data"]
        with self._lock:
            boundary = self._checkpoint()["receivedBoundary"]
        if int(data["firstReceivedSequence"]) <= int(boundary):
            return None
        message = self.gateway.get_received_message(data["receivedId"])
        downgraded = (data["disposition"] in {"ready", "outgoing", "system"}
            and message["disposition"] == "quarantined"
            and message.get("reason") in {"message_recalled", "native_message_identity_conflict"})
        if (message["firstReceivedSequence"] != data["firstReceivedSequence"]
                or (message["disposition"] != data["disposition"] and not downgraded)):
            raise InboxError("inbox_received_event_conflict")
        # Base custody uses message_id as a storage column; its value here is
        # exclusively the protocol received ID, never an official-profile row.
        message = {**message, "id": message["receivedId"]}
        if message["disposition"] != "ready":
            return None, message, None
        if not message["quoteEligible"]:
            raise InboxError("inbox_received_quote_unavailable")
        identity = [self.own_username, message["threadId"], message["serverId"]]
        request_id = "protocol:" + hashlib.sha256(_json(identity).encode()).hexdigest()
        content = {key: message.get(key) for key in
                   ("senderId", "direction", "type", "appMessageType", "body", "quote")}
        return request_id, message, _json(content)

    def _save_non_request(self, message):
        if message.get("disposition") == "quarantined":
            # A later recall/conflict can arrive after request custody but
            # before execution. Suspend pending work in this same transaction;
            # completed business/sends remain historical facts, not rolled back.
            self._db.execute("""UPDATE inbox_requests SET state='quarantined'
                WHERE state='pending' AND request_id IN
                (SELECT request_id FROM inbox_message_ids WHERE message_id=?)""",
                (message["receivedId"],))
            if message.get("threadId") is not None and message.get("serverId") is not None:
                self._db.execute("""UPDATE inbox_requests SET state='quarantined'
                    WHERE state='pending' AND own_username=? AND account_id=? AND thread_id=? AND server_id=?""",
                    (self.own_username, self.gateway.account_id, message["threadId"], message["serverId"]))

    def _stage_event(self, event):
        data = event.get("data") or {}
        if event["kind"] == "message.received" and data.get("disposition") == "quarantined":
            self._db.execute("""UPDATE inbox_requests SET state='quarantined' WHERE state='pending'
                AND request_id IN(SELECT request_id FROM inbox_message_ids WHERE message_id=?)""",
                (data["receivedId"],))
        elif event["kind"] == "message.recalled":
            if isinstance(data.get("threadId"), str) and isinstance(data.get("serverId"), str):
                self._db.execute("""UPDATE inbox_requests SET state='quarantined' WHERE state='pending'
                    AND own_username=? AND account_id=? AND thread_id=? AND server_id=?""",
                    (self.own_username, self.gateway.account_id, data["threadId"], data["serverId"]))

    def _save_request(self, request_id, message):
        if self.source_quarantine_reason(message["receivedId"]):
            self._db.execute("UPDATE inbox_requests SET state='quarantined' WHERE request_id=? AND state='pending'",
                             (request_id,))

    def _completion_blocked(self, request_id):
        row = self._db.execute("SELECT message_id FROM inbox_requests WHERE request_id=?", (request_id,)).fetchone()
        return bool(row and self.source_quarantine_reason(row[0]))

    def _ack_committed(self):
        with self._lock:
            expected = self._view()
            meta = expected[0]
            if meta["committed_cursor"] == meta["acknowledged_cursor"]:
                return
        result = self.gateway.ack(self.consumer, meta["committed_cursor"])
        with self._lock:
            self._check_view(expected)
            if result["cursor"] != meta["committed_cursor"]:
                raise InboxError("inbox_remote_ack_ahead")
            with self._transaction():
                self._db.execute("UPDATE inbox_meta SET acknowledged_cursor=? WHERE singleton=1", (result["cursor"],))

    def _commit_staged(self):
        with self._lock:
            expected = self._view()
            page = self._db.execute("SELECT * FROM inbox_page WHERE singleton=1").fetchone()
            if page is None:
                return 0, 0
            rows = self._db.execute("SELECT * FROM inbox_events WHERE committed=0 ORDER BY CAST(cursor AS INTEGER)").fetchall()
        resolved = []
        for row in rows:
            result = self._resolve(json.loads(row["raw_json"]))
            with self._lock:
                self._check_view(expected)
                current = self._db.execute("SELECT * FROM inbox_events WHERE cursor=?", (row["cursor"],)).fetchone()
                if current is None or current["raw_json"] != row["raw_json"] or current["committed"]:
                    raise InboxError("inbox_staged_page_changed")
                message = result[1] if result is not None else None
                if (current["classification"] == "quarantined"
                        and (message is None or message.get("disposition") != "quarantined")):
                    raise InboxError("inbox_received_event_conflict")
                if message is not None and message.get("disposition") == "quarantined":
                    # A later HTTP read may fail. Preserve this verified negative
                    # fact immediately; the page and ACK remain unfinished.
                    self._denied_sources[message["receivedId"]] = message
                    with self._transaction():
                        self._db.execute("UPDATE inbox_events SET projection_json=?,classification='quarantined' WHERE cursor=?",
                                         (_json(message), row["cursor"]))
                        self._save_non_request(message)
                    self._denied_sources.pop(message["receivedId"], None)
                elif message is not None and message["receivedId"] in self._denied_sources:
                    raise InboxError("inbox_received_event_conflict")
            resolved.append((row, result))
        with self._lock:
            self._check_view(expected)
            return self._commit_resolved(page, rows, resolved)

    def poll(self, *, limit=100):
        with self._poll_lock:
            with self._lock:
                if self._checkpoint() is None:
                    raise InboxError("inbox_not_activated")
            try:
                if self._activation_due():
                    self._activation(initialize=False)
                    self._mark_activated()
                self._ack_committed()
                with self._lock:
                    expected = self._view()
                    staged = self._db.execute("SELECT 1 FROM inbox_page").fetchone() is not None
                if not staged:
                    page = self.gateway.receive(consumer=self.consumer, after=expected[0]["committed_cursor"], limit=limit)
                    if self._page_requires_activation(page):
                        # Account/operation events: re-verify identity before using the page.
                        self._activation(initialize=False)
                        self._mark_activated()
                        with self._lock:
                            expected = self._view()
                    with self._lock:
                        self._check_view(expected)
                        if self._db.execute("SELECT 1 FROM inbox_page").fetchone() is not None:
                            raise InboxError("inbox_staged_page_changed")
                        self._reconcile(page["consumer"])
                        if not page["items"]:
                            state = self._meta()
                            return InboxBatch(state["committed_cursor"], state["acknowledged_cursor"])
                        self._stage(page)
                count, added = self._commit_staged()
                self._ack_committed()
                with self._lock:
                    state = self._meta()
                    return InboxBatch(state["committed_cursor"], state["acknowledged_cursor"], count, added)
            except Exception:
                self._needs_activation = True
                raise

    def close(self):
        with self._poll_lock:
            super().close()

    def quarantined_messages(self, *, limit=100):
        """Unresolved native records; ACK never labels them successful replies."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise InboxError("invalid_inbox_request_limit")
        with self._lock:
            rows = self._db.execute("""SELECT e.cursor,e.received_at,e.projection_json,
                (SELECT r.state FROM inbox_requests r JOIN inbox_message_ids m
                 ON m.request_id=r.request_id WHERE m.message_id=e.message_id) AS request_state
                FROM inbox_events e WHERE e.classification='quarantined'
                AND NOT EXISTS(SELECT 1 FROM inbox_events newer
                    WHERE newer.message_id=e.message_id AND newer.classification='quarantined'
                    AND CAST(newer.cursor AS INTEGER)>CAST(e.cursor AS INTEGER))
                ORDER BY CAST(e.cursor AS INTEGER) LIMIT ?""", (limit,)).fetchall()
            return [{"event_cursor": row["cursor"], "received_at": row["received_at"],
                     "request_state": row["request_state"],
                     "message": json.loads(row["projection_json"])} for row in rows]

    def source_quarantine_reason(self, received_id):
        """Deny from durable raw/projection evidence, never a positive cache.

        Raw safety controls apply before page projection/ACK. An in-memory
        negative is retained only if its verified snapshot could not be saved;
        it can deny an operation, never authorize one or replace source proof.
        """
        with self._lock:
            source = self._db.execute("""SELECT r.thread_id,r.server_id FROM inbox_requests r
                JOIN inbox_message_ids m ON m.request_id=r.request_id
                WHERE m.message_id=? AND r.account_id=? AND r.own_username=?""",
                (received_id, self.gateway.account_id, self.own_username)).fetchone()
            thread, server = (source["thread_id"], source["server_id"]) if source else (None, None)
            aliases = {r[0] for r in self._db.execute("""SELECT b.message_id FROM inbox_message_ids a
                JOIN inbox_message_ids b ON a.request_id=b.request_id WHERE a.message_id=?""", (received_id,))}
            for key, message in self._denied_sources.items():
                if (key == received_id or key in aliases or (source is not None
                        and message.get("accountId") == self.gateway.account_id
                        and message.get("ownUsername") == self.own_username
                        and message.get("threadId") == thread and message.get("serverId") == server)):
                    return message.get("reason") or "source_quarantined"
            row = self._db.execute(f"""SELECT q.projection_json,q.raw_json FROM (
                SELECT cursor,projection_json,raw_json FROM inbox_events
                    WHERE {_BLOCKED} AND message_id IN
                        (SELECT ? UNION SELECT b.message_id FROM inbox_message_ids a
                         JOIN inbox_message_ids b ON a.request_id=b.request_id WHERE a.message_id=?)
                UNION ALL
                SELECT cursor,projection_json,raw_json FROM inbox_events
                    WHERE {_BLOCKED}
                        AND json_extract(projection_json,'$.accountId')=?
                        AND json_extract(projection_json,'$.ownUsername')=?
                        AND json_extract(projection_json,'$.threadId')=?
                        AND json_extract(projection_json,'$.serverId')=?
                UNION ALL
                SELECT cursor,projection_json,raw_json FROM inbox_events
                    WHERE {_RECALLED} AND json_extract(raw_json,'$.data.threadId')=?
                        AND json_extract(raw_json,'$.data.serverId')=?) q
                ORDER BY CAST(q.cursor AS INTEGER) DESC LIMIT 1""",
                (received_id, received_id, self.gateway.account_id, self.own_username,
                 thread, server, thread, server)).fetchone()
            if row is None:
                return None
            if row["projection_json"]:
                return json.loads(row["projection_json"]).get("reason") or "source_quarantined"
            return "message_recalled" if json.loads(row["raw_json"])["kind"] == "message.recalled" else "source_quarantined"

    def nearby_context(self, request_id, *, limit=5):
        """Earlier ordinary text from durable native custody, never an archive.

        Match the existing worker's ten-minute/five-message context window.
        Receipt order is an additional boundary: later arrivals cannot become
        background for an earlier request even when their source clock is old.
        Completed requests remain usable context, but any locally known
        quarantine of the source or its aliases excludes it, including a later
        recall whose raw event is durable but whose page is not yet committed.
        This reads only local knowledge; it never refreshes a gateway snapshot.
        """
        if type(limit) is not int or not 0 <= limit <= 5:
            raise InboxError("invalid_inbox_context_limit")
        with self._lock:
            trigger = self._db.execute("""SELECT * FROM inbox_requests
                WHERE request_id=? AND account_id=? AND own_username=?""",
                (request_id, self.gateway.account_id, self.own_username)).fetchone()
            if trigger is None:
                raise InboxError("inbox_request_not_found")
            if not limit:
                return []
            source = json.loads(trigger["message_json"])
            created = source["createdAt"]
            try:
                stamp = datetime.fromisoformat(created.replace("Z", "+00:00"))
                if stamp.tzinfo is None:
                    raise ValueError
            except (AttributeError, TypeError, ValueError):
                raise InboxError("inbox_context_time_invalid") from None
            # Deduplicate by native identity, not body. A repeated event or a
            # second received ID for the same SID must occupy one context slot.
            rows = self._db.execute(f"""WITH eligible AS (
                SELECT e.cursor,e.projection_json,
                    json_extract(e.projection_json,'$.serverId') AS server_id,
                    julianday(json_extract(e.projection_json,'$.createdAt')) AS created,
                    ROW_NUMBER() OVER (
                        PARTITION BY json_extract(e.projection_json,'$.serverId')
                        ORDER BY CAST(e.cursor AS INTEGER)) AS identity_order
                FROM inbox_events e
                WHERE e.committed=1 AND e.classification IN ('incoming_request','outgoing')
                    AND CAST(e.cursor AS INTEGER)<CAST(? AS INTEGER)
                    AND json_extract(e.projection_json,'$.accountId')=?
                    AND json_extract(e.projection_json,'$.ownUsername')=?
                    AND json_extract(e.projection_json,'$.threadId')=?
                    AND json_type(e.projection_json,'$.type')='integer'
                    AND json_extract(e.projection_json,'$.type')=1
                    AND json_type(e.projection_json,'$.serverId')='text'
                    AND json_type(e.projection_json,'$.senderId')='text'
                    AND json_type(e.projection_json,'$.body')='text'
                    AND ((json_extract(e.projection_json,'$.disposition')='ready'
                            AND json_extract(e.projection_json,'$.direction')='incoming')
                        OR (json_extract(e.projection_json,'$.disposition')='outgoing'
                            AND json_extract(e.projection_json,'$.direction')='outgoing'))
                    AND json_extract(e.projection_json,'$.serverId')<>?
                    AND julianday(json_extract(e.projection_json,'$.createdAt'))
                        BETWEEN julianday(?,'-600 seconds') AND julianday(?)
                    AND NOT EXISTS (
                        SELECT 1 FROM inbox_events q
                        WHERE {_BLOCKED} AND q.message_id IN
                            (SELECT e.message_id UNION SELECT b.message_id FROM inbox_message_ids a
                             JOIN inbox_message_ids b ON a.request_id=b.request_id
                             WHERE a.message_id=e.message_id))
                    AND NOT EXISTS (
                        SELECT 1 FROM inbox_events q WHERE {_BLOCKED}
                            AND json_extract(q.projection_json,'$.accountId')=?
                            AND json_extract(q.projection_json,'$.ownUsername')=?
                            AND json_extract(q.projection_json,'$.threadId')=?
                            AND json_extract(q.projection_json,'$.serverId')=
                                json_extract(e.projection_json,'$.serverId'))
                    AND NOT EXISTS (
                        SELECT 1 FROM inbox_events q WHERE {_RECALLED}
                            AND json_extract(q.raw_json,'$.data.threadId')=?
                            AND json_extract(q.raw_json,'$.data.serverId')=
                                json_extract(e.projection_json,'$.serverId')))
                SELECT cursor,projection_json FROM eligible WHERE identity_order=1
                ORDER BY created DESC,CAST(cursor AS INTEGER) DESC LIMIT ?""",
                (trigger["event_cursor"], self.gateway.account_id, self.own_username,
                 trigger["thread_id"], trigger["server_id"], created, created,
                 self.gateway.account_id, self.own_username, trigger["thread_id"], trigger["thread_id"], limit)).fetchall()
            result = []
            for row in reversed(rows):
                message = json.loads(row["projection_json"])
                if self.source_quarantine_reason(message["receivedId"]) or any(
                        blocked.get("accountId") == self.gateway.account_id
                        and blocked.get("ownUsername") == self.own_username
                        and blocked.get("threadId") == message["threadId"]
                        and blocked.get("serverId") == message["serverId"]
                        for blocked in self._denied_sources.values()):
                    continue
                identity = [self.own_username, message["threadId"], message["serverId"]]
                result.append({"message_ref": "native:" + hashlib.sha256(_json(identity).encode()).hexdigest(),
                    "sender_id": message["senderId"], "sender_name": message["senderId"],
                    "text": message["body"],
                    "create_time": datetime.fromisoformat(message["createdAt"].replace("Z", "+00:00")).timestamp(),
                    "server_id": message["serverId"], "received_id": message["receivedId"]})
            return result
