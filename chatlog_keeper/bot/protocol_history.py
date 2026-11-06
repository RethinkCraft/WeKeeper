"""Conversation context across the legacy and native delivery paths.

Native acceptance is not phone delivery and never changes legacy send states.
Legacy account ownership is established by the caller's verified group_ref
migration mapping, not by guessing a real username from an archive account ID.
History is context only: current business facts still require the current MCP
result. All transport observations here are local outbox reads, never network.
"""
from __future__ import annotations

import hashlib
import json

from .protocol_gateway import _identifier, _json
from .protocol_outbox import OutboxError


class ProtocolHistoryStore:
    def __init__(self, store, outbox, *, own_username, account_id):
        self.own_username = _identifier(own_username, maximum=256)
        self.account_id = _identifier(account_id, maximum=256)
        if (outbox.own_username != self.own_username or outbox.account_id != self.account_id):
            raise ValueError("protocol_history_account_mismatch")
        self._store, self._outbox = store, outbox

    def __getattr__(self, name):
        return getattr(self._store, name)

    @staticmethod
    def _native_marker(row, event, quote):
        # A malformed native record must not qualify through legacy sent flags.
        return (event.get("transport") == "native_protocol" or "native_source" in event
                or "gateway_account_id" in event or row["event_id"].startswith("protocol:")
                or row["message_ref"].startswith("native:")
                or quote.get("transport") == "native_protocol" or "received_id" in quote)

    def _native_accepted(self, row, event, quote):
        source = event.get("native_source")
        if (not isinstance(source, dict) or event.get("transport") != "native_protocol"
                or event.get("account_id") != self.own_username
                or event.get("gateway_account_id") != self.account_id
                or source.get("accountId") != self.account_id
                or source.get("ownUsername") != self.own_username
                or source.get("disposition") != "ready" or source.get("quoteEligible") is not True
                or source.get("direction") != "incoming"
                or source.get("senderId") != row["sender_id"]
                or source.get("senderId") == self.own_username
                or event.get("sender_id") != row["sender_id"]
                or source.get("body") != row["user_text"]
                or event.get("text") != row["user_text"]
                or source.get("threadId") != event.get("group_id")
                or source.get("serverId") != event.get("server_id")
                or event.get("event_id") != row["event_id"]
                or event.get("group_ref") != row["group_ref"]
                or event.get("message_ref") != row["message_ref"]):
            return False
        if not all(isinstance(source.get(key), str) and source[key] for key in
                   ("threadId", "serverId", "receivedId", "sourceSha256")):
            return False
        identity = hashlib.sha256(_json([
            self.own_username, source["threadId"], source["serverId"]]).encode()).hexdigest()
        expected_quote = {"transport": "native_protocol", "message_ref": "native:" + identity,
                          "group_ref": row["group_ref"], "account_id": self.account_id,
                          "own_username": self.own_username, "thread_id": source["threadId"],
                          "received_id": source.get("receivedId"), "server_id": source["serverId"],
                          "source_sha256": source.get("sourceSha256")}
        if (row["event_id"] != "protocol:" + identity or row["message_ref"] != "native:" + identity
                or quote != event.get("reply_to")
                or any(quote.get(key) != value for key, value in expected_quote.items())):
            return False
        frozen = json.loads(row["frozen_json"]) if row["frozen_json"] is not None else None
        if (not isinstance(frozen, dict) or frozen.get("request_id") != row["event_id"]
                or frozen.get("reply_id") != row["reply_id"] or frozen.get("source") != source
                or frozen.get("text") != row["assistant_text"]
                or not isinstance(frozen.get("evidence"), dict)):
            return False
        try:
            sent = self._outbox.get(row["reply_id"])
        except OutboxError as error:
            if error.code == "outbox_reply_not_found":
                return False
            raise
        return bool(sent and sent.get("state") == "accepted"
                    and sent.get("request_id") == row["event_id"]
                    and sent.get("reply_id") == row["reply_id"]
                    and sent.get("source") == source and sent.get("text") == frozen["text"]
                    and sent.get("evidence") == frozen.get("evidence")
                    and sent.get("thread_id") == source["threadId"]
                    and sent.get("server_id") == source["serverId"])

    def history(self, group_ref, persona_id, version, limit=10):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("invalid_bot_history_limit")
        binding = self._store.binding(group_ref)
        if not binding or (binding.get("bot_wxid") and binding["bot_wxid"] != self.own_username):
            return []
        turns, before = [], None
        # Keyset pages let older accepted turns survive any number of newer
        # queued/unknown rows, without loading an unbounded conversation at once.
        while len(turns) < limit:
            seek = " AND (r.event_ts,r.event_order,r.event_id)<(?,?,?)" if before else ""
            parameters = [group_ref, persona_id, str(version), binding["business_id"]]
            if before:
                parameters.extend(before)
            parameters.append(max(32, limit))
            with self._store.lock:
                rows = self._store.conn.execute("""SELECT r.event_id,r.message_ref,r.group_ref,
                    r.sender_id,r.text AS user_text,r.event,r.event_ts,r.event_order,
                    r.status AS request_status,o.reply_id,o.text AS assistant_text,
                    o.reply_to,o.status AS reply_status,m.value AS frozen_json
                    FROM requests r JOIN reply_outbox o USING(event_id)
                    LEFT JOIN meta m ON m.key='protocol_frozen_'||r.event_id
                    WHERE r.group_ref=? AND r.persona_id=? AND r.persona_version=?
                    AND r.business_id=? AND o.kind='final' AND o.text IS NOT NULL"""
                    + seek + " ORDER BY r.event_ts DESC,r.event_order DESC,r.event_id DESC LIMIT ?",
                    parameters).fetchall()
            if not rows:
                break
            before = tuple(rows[-1][key] for key in ("event_ts", "event_order", "event_id"))
            for row in rows:
                event, quote = json.loads(row["event"]), json.loads(row["reply_to"])
                if not isinstance(event, dict) or not isinstance(quote, dict):
                    continue
                if not all(isinstance(row[key], str) and row[key].strip()
                           for key in ("user_text", "assistant_text")):
                    continue
                if any(value is not None and value != self.own_username
                       for value in (event.get("bot_wxid"), quote.get("bot_wxid"))):
                    continue
                if self._native_marker(row, event, quote):
                    eligible = self._native_accepted(row, event, quote)
                else:
                    eligible = row["request_status"] == "completed" and row["reply_status"] == "sent"
                if eligible:
                    turns.append((row["user_text"], row["assistant_text"]))
                    if len(turns) == limit:
                        break
        return [message for user, assistant in reversed(turns) for message in
                ({"role": "user", "content": user}, {"role": "assistant", "content": assistant})]
