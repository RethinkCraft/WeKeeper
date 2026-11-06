"""Frozen replies and conservative delivery recovery for the native gateway.

Keep this file and the gateway client's journal together across restarts. One
owner may open an outbox at a time. A committed submission attempt is never
automatically reset to queued, even when a crash preceded the client claim.
There is no model, profile lookup, UI fallback, or phone-delivery inference.
Source isolation or revoked delivery authorization can reject an unattempted
reply atomically. After an attempt, either remains a separate observation and
cannot erase the actual send outcome. The compatible ``source_blocked`` field
also records delivery-authorization reasons such as ``configuration_changed``.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time

from .protocol_gateway import (GatewayError, NATIVE_ADMISSION_REJECTIONS, _cursor, _identifier, _json,
                               _key, _server_id, _source_hash)

_SOURCE_BLOCK_REASONS = frozenset({"source_quarantined", "message_recalled", "native_message_identity_conflict",
                                   "configuration_changed", "suppressed_stale"})


class OutboxError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _block_reason(reason):
    if not isinstance(reason, str) or reason not in _SOURCE_BLOCK_REASONS:
        raise OutboxError("invalid_outbox_block_reason")
    return reason


def _private_file(path):
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
            raise OutboxError("unsafe_outbox_file")
        os.fchmod(fd, 0o600)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _document(value):
    def validate(item):
        if isinstance(item, dict):
            if not all(isinstance(key, str) for key in item):
                raise OutboxError("invalid_outbox_document")
            for child in item.values():
                validate(child)
        elif isinstance(item, list):
            for child in item:
                validate(child)
        elif item is not None and type(item) not in {str, bool, int, float}:
            raise OutboxError("invalid_outbox_document")
    if not isinstance(value, dict):
        raise OutboxError("invalid_outbox_document")
    validate(value)
    try:
        encoded = _json(value)
        if len(encoded.encode()) > 2 * 1024 * 1024:
            raise OutboxError("outbox_document_too_large")
        return encoded
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise OutboxError("invalid_outbox_document") from None


class ProtocolOutbox:
    def __init__(self, path, *, account_id, own_username):
        self.account_id = _identifier(account_id, maximum=256)
        self.own_username = _identifier(own_username, maximum=256)
        self.path = Path(path).expanduser().absolute()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._inflight = set()
        self._db = None
        self._lease = _private_file(Path(str(self.path) + ".lock"))
        try:
            try:
                fcntl.flock(self._lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise OutboxError("outbox_already_open") from None
            for suffix in ("", "-wal", "-shm", "-journal"):
                target = Path(str(self.path) + suffix)
                if target.is_symlink():
                    raise OutboxError("unsafe_outbox_file")
                if target.exists():
                    os.close(_private_file(target))
            os.close(_private_file(self.path))
            self._db = sqlite3.connect(self.path, timeout=10, isolation_level=None, check_same_thread=False)
            self._db.row_factory = sqlite3.Row
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS outbox_meta(
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    account_id TEXT NOT NULL,own_username TEXT NOT NULL,endpoint TEXT);
                CREATE TABLE IF NOT EXISTS replies(
                    sequence INTEGER PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,
                    reply_id TEXT NOT NULL UNIQUE,thread_id TEXT NOT NULL,server_id TEXT NOT NULL,
                    source_json TEXT NOT NULL,text TEXT NOT NULL,evidence_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN('queued','submitting','accepted','rejected','unknown')),
                    created_at TEXT NOT NULL,submitted_at TEXT,updated_at TEXT NOT NULL,
                    result_json TEXT,error_code TEXT,source_blocked TEXT,UNIQUE(thread_id,server_id));
            """)
            with self._transaction():
                if "observations_json" not in {row[1] for row in self._db.execute("PRAGMA table_info(replies)")}:
                    self._db.execute("ALTER TABLE replies ADD COLUMN observations_json TEXT NOT NULL DEFAULT '{}'")
                if "source_blocked" not in {row[1] for row in self._db.execute("PRAGMA table_info(replies)")}:
                    self._db.execute("ALTER TABLE replies ADD COLUMN source_blocked TEXT")
                row = self._db.execute("SELECT * FROM outbox_meta").fetchone()
                if row and (row["account_id"], row["own_username"]) != (self.account_id, self.own_username):
                    raise OutboxError("outbox_account_changed")
                self._db.execute("INSERT OR IGNORE INTO outbox_meta VALUES(1,?,?,NULL)",
                                 (self.account_id, self.own_username))
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
            self._db.execute("ROLLBACK")
            raise

    def _source(self, source):
        required = {"receivedId", "firstReceivedSequence", "accountId", "ownUsername", "sourceSha256",
                    "disposition", "reason", "threadId", "serverId", "senderId", "direction", "type",
                    "appMessageType", "createdAt", "body", "mentions", "quote", "quoteEligible"}
        encoded = _document(source)
        source = json.loads(encoded)
        if (not required.issubset(source) or source["accountId"] != self.account_id
                or source["ownUsername"] != self.own_username or source["disposition"] != "ready"
                or source["reason"] is not None or source["direction"] != "incoming"
                or source["senderId"] == self.own_username or source["quoteEligible"] is not True
                or type(source["type"]) is not int or not 0 < source["type"] <= 2147483647
                or not isinstance(source["body"], str) or not isinstance(source["mentions"], dict)
                or (source["quote"] is not None and not isinstance(source["quote"], dict))
                or (source["appMessageType"] is not None and
                    (type(source["appMessageType"]) is not int or not 0 < source["appMessageType"] <= 2147483647))
                or ("id" in source and source["id"] != source["receivedId"])):
            raise OutboxError("invalid_outbox_source")
        for name in ("receivedId", "threadId", "senderId"):
            _identifier(source[name], maximum=4096 if name == "receivedId" else 256)
        _server_id(source["serverId"])
        _source_hash(source["sourceSha256"])
        if _cursor(source["firstReceivedSequence"]) == "0":
            raise OutboxError("invalid_outbox_source")
        try:
            if datetime.fromisoformat(source["createdAt"].replace("Z", "+00:00")).tzinfo is None:
                raise ValueError()
        except (AttributeError, ValueError):
            raise OutboxError("invalid_outbox_source") from None
        return encoded

    def freeze(self, *, request_id, reply_id, source, text, evidence, blocked_reason=None):
        _identifier(request_id)
        _key(reply_id)
        if blocked_reason is not None:
            _block_reason(blocked_reason)
        source_json, evidence_json = self._source(source), _document(evidence)
        source = json.loads(source_json)
        if not isinstance(text, str) or not text or len(text.encode()) > 64 * 1024:
            raise OutboxError("invalid_outbox_text")
        frozen = (request_id, reply_id, source_json, text, evidence_json)
        with self._lock, self._transaction():
            rows = self._db.execute("""SELECT * FROM replies WHERE request_id=? OR reply_id=?
                OR (thread_id=? AND server_id=?)""",
                (request_id, reply_id, source["threadId"], source["serverId"])).fetchall()
            if rows:
                if len(rows) != 1 or tuple(rows[0][k] for k in
                        ("request_id", "reply_id", "source_json", "text", "evidence_json")) != frozen:
                    raise OutboxError("outbox_freeze_conflict")
                if blocked_reason is not None:
                    return self._block_locked(reply_id, blocked_reason)
            else:
                now = _now()
                self._db.execute("""INSERT INTO replies(request_id,reply_id,thread_id,server_id,
                    source_json,text,evidence_json,state,created_at,updated_at,source_blocked,error_code)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (request_id, reply_id, source["threadId"], source["serverId"], source_json,
                     text, evidence_json, "rejected" if blocked_reason else "queued", now, now,
                     blocked_reason, "outbox_source_blocked" if blocked_reason else None))
            return self.get(reply_id)

    def _block_locked(self, reply_id, reason):
        previous = self.get(reply_id)
        if previous["source_blocked"] == reason:
            return previous
        # Queued means no attempt was committed. Once attempted, retain the
        # actual outcome: a source observation cannot undo an in-flight send.
        queued = previous["state"] == "queued"
        self._db.execute("""UPDATE replies SET source_blocked=?,state=?,error_code=?,updated_at=?
            WHERE reply_id=?""", (reason, "rejected" if queued else previous["state"],
                "outbox_source_blocked" if queued else previous["error_code"], _now(), reply_id))
        return self.get(reply_id)

    def block(self, reply_id, reason):
        """Block an unattempted reply; preserve attempted outcomes and the reason."""
        _key(reply_id)
        _block_reason(reason)
        with self._lock, self._transaction():
            return self._block_locked(reply_id, reason)

    @staticmethod
    def _row(row):
        value = dict(row)
        value["source"] = json.loads(value.pop("source_json"))
        value["evidence"] = json.loads(value.pop("evidence_json"))
        value["observations"] = json.loads(value.pop("observations_json"))
        result = value.pop("result_json")
        value["result"] = json.loads(result) if result else None
        value["intent"] = value["result"].get("intent") if value["result"] else None
        value["phone_received"] = False
        return value

    def delivery_states(self):
        """Operational identities/states only, for the independent control lane."""
        with self._lock:
            return dict(self._db.execute("SELECT request_id,state FROM replies"))

    def get(self, reply_id):
        _key(reply_id)
        with self._lock:
            row = self._db.execute("SELECT * FROM replies WHERE reply_id=?", (reply_id,)).fetchone()
            if row is None:
                raise OutboxError("outbox_reply_not_found")
            return self._row(row)

    def pending(self, *, limit=100):
        """Only new submissions; unknown or accepted work never occupies this queue."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise OutboxError("invalid_outbox_limit")
        with self._lock:
            busy = sorted(self._inflight)
            exclude = " AND reply_id NOT IN (" + ",".join("?" for _ in busy) + ")" if busy else ""
            return [self._row(row) for row in self._db.execute(
                "SELECT * FROM replies WHERE state='queued'" + exclude + " ORDER BY sequence LIMIT ?",
                (*busy, limit))]

    def awaiting_status(self, *, limit=100):
        """Oldest unresolved observations first; separate from new submissions."""
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise OutboxError("invalid_outbox_limit")
        with self._lock:
            busy = sorted(self._inflight)
            exclude = " AND reply_id NOT IN (" + ",".join("?" for _ in busy) + ")" if busy else ""
            return [self._row(row) for row in self._db.execute(
                "SELECT * FROM replies WHERE state IN('submitting','unknown')" + exclude
                + " ORDER BY updated_at,sequence LIMIT ?", (*busy, limit))]

    def _bind(self, gateway):
        if gateway.account_id != self.account_id:
            raise OutboxError("outbox_gateway_account_mismatch")
        gateway.require_account(self.own_username)
        with self._lock, self._transaction():
            endpoint = self._db.execute("SELECT endpoint FROM outbox_meta").fetchone()[0]
            if endpoint is not None and endpoint != gateway.base_url:
                raise OutboxError("outbox_gateway_endpoint_changed")
            self._db.execute("UPDATE outbox_meta SET endpoint=? WHERE singleton=1", (gateway.base_url,))

    def _save(self, reply_id, *, state, result=None, error=None, observation=None):
        with self._lock, self._transaction():
            previous = self.get(reply_id)
            if previous["state"] in {"accepted", "rejected"}:
                return previous
            encoded = _json(asdict(result)) if result else None
            observed = previous["observations"]
            now = _now()
            if observation is not None:
                if observation["operation"] == "submission":
                    observed.setdefault("first_submission", observation)
                else:
                    observed["last_status_check"] = observation
            if result is not None:
                observed.setdefault("first_gateway_result_observed_at", now)
            if state == "accepted":
                observed.setdefault("first_accepted_observed_at", now)
            self._db.execute("""UPDATE replies SET state=?,result_json=COALESCE(?,result_json),
                error_code=?,updated_at=?,observations_json=? WHERE reply_id=?""",
                (state, encoded, error, now, _json(observed), reply_id))
            return self.get(reply_id)

    def _result(self, reply_id, result, *, observation=None):
        previous = self.get(reply_id)
        intent = result.intent
        if result.idempotency_key != reply_id:
            raise OutboxError("outbox_send_identity_mismatch")
        if intent is not None:
            if (intent.get("accountId") != self.account_id
                    or intent.get("destination") != previous["thread_id"]
                    or (previous["intent"] and any(previous["intent"].get(field) is not None
                        and previous["intent"][field] != intent.get(field)
                        for field in ("id", "clientId", "serverId")))):
                raise OutboxError("outbox_send_identity_mismatch")
            _identifier(intent.get("id"))
            if intent.get("clientId") is not None:
                _identifier(intent["clientId"])
        if result.outcome == "accepted" and intent and intent.get("state") in {"accepted", "sent", "server_accepted"}:
            _server_id(intent.get("serverId"))
            return self._save(reply_id, state="accepted", result=result, observation=observation)
        if (getattr(result, "direct_post_rejection", False) is True and result.fresh is True
                and result.outcome == "rejected" and intent is None and result.http_status == 409
                and result.error_code in NATIVE_ADMISSION_REJECTIONS
                and observation is not None and observation.get("operation") == "submission"):
            return self._save(reply_id, state="rejected", result=result,
                              error=result.error_code, observation=observation)
        if result.fresh and intent and intent.get("state") == "failed" and intent.get("errorCode") == "service_rejected":
            return self._save(reply_id, state="rejected", result=result, error="service_rejected", observation=observation)
        if result.fresh and intent and intent.get("state") in {"queued", "submitting"}:
            return self._save(reply_id, state="submitting", result=result, observation=observation)
        reason = result.error_code or (intent or {}).get("errorCode")
        if not isinstance(reason, str) or not re.fullmatch(r"[A-Za-z0-9_]{1,100}", reason):
            reason = "outbox_send_outcome_unknown"
        return self._save(reply_id, state="unknown", result=result, error=reason, observation=observation)

    def _run(self, gateway, reply_id, *, submit):
        with self._lock:
            row = self.get(reply_id)
            if (row["state"] in {"accepted", "rejected"} or reply_id in self._inflight
                    or (row["state"] == "queued" and not submit)):
                return row
            if row["error_code"] in {"gateway_idempotency_conflict", "outbox_send_identity_mismatch"}:
                # These require manual reconciliation, never another command.
                # Rotate them as reviewed so a bounded status worker can still
                # reach later tasks whose remote result may now be available.
                return self._save(reply_id, state=row["state"], error=row["error_code"])
            self._inflight.add(reply_id)
        started = time.monotonic()
        observation = {"operation": "submission" if row["state"] == "queued" else "status",
                       "clock": "host_utc_with_monotonic_offsets", "started_at": _now(),
                       "semantic": "Python gateway call; not wire send, original receipt arrival, or phone delivery"}
        def finished():
            observation["ended_offset_seconds"] = max(0, time.monotonic() - started)
            observation["ended_at"] = _now()
            return observation
        try:
            # Network calls never hold the database mutex or a SQLite transaction.
            self._bind(gateway)
            observation["account_preflight_end_offset_seconds"] = max(0, time.monotonic() - started)
            if row["state"] == "queued":
                with self._lock, self._transaction():
                    changed = self._db.execute("""UPDATE replies SET state='submitting',submitted_at=?,updated_at=?
                        WHERE reply_id=? AND state='queued' AND source_blocked IS NULL""",
                        (_now(), _now(), reply_id)).rowcount
                    if not changed:
                        # A concurrent block may have run while require_account
                        # was on the network. Never submit from the stale row.
                        return self.get(reply_id)
                source = row["source"]
                observation["command_start_offset_seconds"] = max(0, time.monotonic() - started)
                result = gateway.send_received_quote(thread_id=source["threadId"],
                    received_id=source["receivedId"], server_id=source["serverId"],
                    source_sha256=source["sourceSha256"], text=row["text"], idempotency_key=reply_id)
            else:
                observation["command_start_offset_seconds"] = max(0, time.monotonic() - started)
                result = gateway.get_send_status(reply_id)
            return self._result(reply_id, result, observation=finished())
        except (GatewayError, OutboxError) as error:
            if self.get(reply_id)["state"] == "queued":
                raise
            code = ("outbox_submission_not_claimed" if error.code == "gateway_send_key_not_claimed"
                    else error.code if re.fullmatch(r"[A-Za-z0-9_]{1,100}", error.code)
                    else "outbox_gateway_failure")
            return self._save(reply_id, state="unknown", error=code, observation=finished())
        finally:
            with self._lock:
                self._inflight.discard(reply_id)

    def deliver_one(self, gateway, reply_id):
        """Submit queued work once; an existing attempt gets status queries only."""
        return self._run(gateway, reply_id, submit=True)

    def reconcile(self, gateway, reply_id):
        """Resolve an existing attempt without ever posting a send command."""
        return self._run(gateway, reply_id, submit=False)

    def close(self):
        with self._lock:
            if self._inflight:
                raise OutboxError("outbox_delivery_in_flight")
            if self._db is not None:
                self._db.close()
                self._db = None
            if self._lease is not None:
                os.close(self._lease)
                self._lease = None

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
