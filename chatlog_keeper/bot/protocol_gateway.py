"""Independent client for agent-wechat's authenticated loopback /v1 API.

This adapter does not use the business store, model, archive or UI sender.
The small local journal contains pending/recovery work, not canonical WeChat
history. Keep it across restarts: a previously claimed key is inspected by
default. Controlled fault tests may explicitly recover a missing intent using
the identical key and payload, only within the original daemon runtime epoch.

serverId is stored in the gateway registry and survives restart, but it also
survives restoring an old snapshot. It is NOT proof that intents were retained.
Recovery therefore additionally requires the original ownershipGeneration,
account ID and ownUsername, and an explicit recover_missing_intent=True call.
Use this only after excluding concurrent writers, profile replacement and
database restore. The HTTP API has no atomic expected-generation fence: a
restart/restore between the identity check and POST remains a race. Recovery
is disabled by default and cannot establish exactly-once phone delivery.

Wire contracts: wechat-api/{contracts.rs,lib.rs,media.rs,actions.rs} in the
pinned agent-wechat source. An accepted send is not phone delivery evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
from urllib.parse import quote, urlsplit

import httpx

NATIVE_ADMISSION_REJECTIONS = frozenset({"authorization_generation_changed", "profile_ownership_required"})


class GatewayError(RuntimeError):
    """A bounded diagnostic; never includes a bearer token or response body."""

    def __init__(self, code, *, http_status=None):
        self.code = code
        self.http_status = http_status
        self.retryable = False
        super().__init__(code)


@dataclass(frozen=True)
class SendResult:
    idempotency_key: str
    outcome: str
    intent: dict | None = None
    error_code: str | None = None
    http_status: int | None = None
    # No response from this API establishes receipt on the user's phone.
    phone_received: bool = False
    retryable: bool = False
    fresh: bool = True
    # Only this client's actual POST response may set this field. Preflight,
    # status lookups and server JSON cannot certify an earlier send attempt.
    direct_post_rejection: bool = False


def _identifier(value, *, maximum=4096):
    if (not isinstance(value, str) or not value or value in {".", ".."}
            or len(value.encode("utf-8")) > maximum
            or any(ord(char) < 32 or ord(char) == 127 or char.isspace() for char in value)):
        raise GatewayError("invalid_gateway_identifier")
    return value


def _server_id(value):
    if (not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]{0,19}", value)
            or int(value) > 18446744073709551615):
        raise GatewayError("invalid_gateway_server_id")
    return value


def _cursor(value):
    if (not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]{0,18}", value)
            or int(value) > 9223372036854775807):
        raise GatewayError("invalid_gateway_cursor")
    return value


def _key(value):
    if (not isinstance(value, str) or not value or len(value) > 200
            or any(not 32 <= ord(char) <= 126 for char in value)):
        raise GatewayError("invalid_gateway_idempotency_key")
    return value


def _source_hash(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise GatewayError("invalid_gateway_source_hash")
    return value


def _base_url(value):
    try:
        parsed = urlsplit(value)
        address = ipaddress.ip_address(parsed.hostname or "")
        port = parsed.port
    except (TypeError, ValueError):
        raise GatewayError("gateway_requires_loopback_url") from None
    if (parsed.scheme not in {"http", "https"} or not address.is_loopback
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment or parsed.path.rstrip("/") not in {"", "/v1"}):
        raise GatewayError("gateway_requires_loopback_url")
    host = f"[{address}]" if address.version == 6 else str(address)
    return f"{parsed.scheme}://{host}" + (f":{port}" if port is not None else "") + "/v1"


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


class _IntentJournal:
    def __init__(self, path, endpoint):
        self.path = Path(path).expanduser().absolute()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        for target in (self.path, Path(str(self.path) + "-wal"), Path(str(self.path) + "-shm")):
            if target.is_symlink():
                raise GatewayError("unsafe_gateway_journal")
        descriptor = os.open(self.path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                raise GatewayError("unsafe_gateway_journal")
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        try:
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS gateway_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS send_claims(
                  account_id TEXT NOT NULL,key TEXT NOT NULL,payload TEXT NOT NULL,
                  intent_id TEXT,intent TEXT,ownership_generation TEXT,
                  PRIMARY KEY(account_id,key));
                CREATE TABLE IF NOT EXISTS gateway_accounts(
                  account_id TEXT PRIMARY KEY,own_username TEXT NOT NULL);
            """)
            with self.conn:
                self.conn.execute("BEGIN IMMEDIATE")
                if "ownership_generation" not in {row[1] for row in self.conn.execute("PRAGMA table_info(send_claims)")}:
                    self.conn.execute("ALTER TABLE send_claims ADD COLUMN ownership_generation TEXT")
                row = self.conn.execute("SELECT value FROM gateway_meta WHERE key='endpoint'").fetchone()
                if row and row[0] != endpoint:
                    raise GatewayError("gateway_journal_endpoint_changed")
                self.conn.execute("INSERT OR IGNORE INTO gateway_meta VALUES('endpoint',?)", (endpoint,))
        except BaseException:
            self.conn.close()
            raise

    def bind(self, server_id, account, own_username):
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            server = self.conn.execute("SELECT value FROM gateway_meta WHERE key='server_id'").fetchone()
            if server is not None and server[0] != server_id:
                raise GatewayError("gateway_server_identity_changed")
            bound = self.conn.execute("SELECT own_username FROM gateway_accounts WHERE account_id=?", (account,)).fetchone()
            if bound is not None and bound[0] != own_username:
                raise GatewayError("gateway_account_identity_changed")
            if bound is None and self.conn.execute("SELECT 1 FROM send_claims WHERE account_id=? LIMIT 1", (account,)).fetchone():
                # Pre-binding journals cannot retroactively prove which daemon
                # accepted their requests. Keep them for manual reconciliation.
                raise GatewayError("gateway_existing_claims_unbound")
            self.conn.execute("INSERT OR IGNORE INTO gateway_meta VALUES('server_id',?)", (server_id,))
            self.conn.execute("INSERT OR IGNORE INTO gateway_accounts VALUES(?,?)", (account, own_username))

    def claim(self, account, key, payload, ownership_generation):
        body = _json(payload)
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            prior = self.conn.execute("SELECT payload FROM send_claims WHERE account_id=? AND key=?", (account, key)).fetchone()
            if prior:
                if prior[0] != body:
                    raise GatewayError("gateway_idempotency_conflict")
                return False
            # COMMIT precedes the POST. An explicit missing-intent recovery
            # must still use these same bytes/key and original runtime epoch.
            self.conn.execute("INSERT INTO send_claims(account_id,key,payload,ownership_generation) VALUES(?,?,?,?)",
                              (account, key, body, ownership_generation))
            return True

    def get(self, account, key):
        with self.lock:
            row = self.conn.execute("SELECT * FROM send_claims WHERE account_id=? AND key=?", (account, key)).fetchone()
        if row is None:
            raise GatewayError("gateway_send_key_not_claimed")
        return {**dict(row), "payload": json.loads(row["payload"]),
                "intent": json.loads(row["intent"]) if row["intent"] else None}

    def save(self, account, key, intent):
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            prior = self.get(account, key)
            if prior["intent_id"] is not None and prior["intent_id"] != intent["id"]:
                raise GatewayError("gateway_send_identity_changed")
            # A delayed queued response cannot erase a known accepted receipt.
            if prior["intent"] and prior["intent"]["state"] in {"accepted", "sent", "server_accepted"}:
                if intent["state"] not in {"accepted", "sent", "server_accepted"}:
                    return prior["intent"]
                if prior["intent"].get("serverId") != intent.get("serverId"):
                    raise GatewayError("gateway_send_identity_changed")
            self.conn.execute("UPDATE send_claims SET intent_id=?,intent=? WHERE account_id=? AND key=?",
                              (intent["id"], _json(intent), account, key))
            return intent

    def close(self):
        with self.lock:
            self.conn.close()


class ProtocolGateway:
    """No automatic login, send retry, event ack, model call or read-state edit.

    ``receive`` returns the native event page including unknown event kinds.
    Persist those events before explicitly calling ``ack``. The journal must
    live in the application's retained private state, not a temporary directory.
    ``transport`` is an optional httpx transport for isolated contract tests.
    """

    def __init__(self, *, token, account_id, journal_path,
                 base_url="http://127.0.0.1:6174", timeout=10.0, transport=None):
        self.base_url = _base_url(base_url)
        self.account_id = _identifier(account_id, maximum=256)
        if (not isinstance(token, str) or not token or len(token) > 8192
                or any(not 33 <= ord(char) <= 126 for char in token)):
            raise GatewayError("invalid_gateway_token")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 60:
            raise GatewayError("invalid_gateway_timeout")
        self._journal = _IntentJournal(journal_path, self.base_url)
        self._http = httpx.Client(transport=transport, trust_env=False, follow_redirects=False,
                                 timeout=timeout, headers={"Authorization": "Bearer " + token,
                                                         "Accept": "application/json"})
        self._account_path = self.base_url + "/accounts/" + quote(self.account_id, safe="")

    def _request(self, method, path, *, params=None, body=None, key=None, global_scope=False):
        try:
            endpoint = self.base_url if global_scope else self._account_path
            headers = {"Idempotency-Key": key} if key is not None else {}
            content = None
            if body is not None:
                content = _json(body).encode("utf-8")
                headers["Content-Type"] = "application/json"
            response = self._http.request(method, endpoint + path, params=params,
                                          content=content, headers=headers)
        except httpx.HTTPError:
            raise GatewayError("gateway_transport_unavailable") from None
        try:
            value = response.json()
        except ValueError:
            raise GatewayError("gateway_invalid_response", http_status=response.status_code) from None
        if not 200 <= response.status_code < 300:
            error = value.get("error") if isinstance(value, dict) else None
            code = error.get("code") if isinstance(error, dict) else None
            if not isinstance(code, str) or not re.fullmatch(r"[a-zA-Z0-9_]{1,100}", code):
                code = "gateway_http_error"
            raise GatewayError(code, http_status=response.status_code)
        if not isinstance(value, dict):
            raise GatewayError("gateway_invalid_response", http_status=response.status_code)
        return value

    def _bound_identity(self):
        status = self._request("GET", "/status", global_scope=True)
        server_id = _identifier(status.get("serverId"), maximum=256)
        generation = _identifier(status.get("ownershipGeneration"), maximum=256)
        account = self._request("GET", "")
        if account.get("id") != self.account_id:
            raise GatewayError("gateway_account_identity_changed")
        own_username = _identifier(account.get("ownUsername"), maximum=256)
        expected = getattr(self, "_expected_own_username", None)
        if expected is not None and own_username != expected:
            raise GatewayError("gateway_expected_account_mismatch")
        self._journal.bind(server_id, self.account_id, own_username)
        return generation

    def account_status(self):
        """Read bounded account health without binding send/receive permission."""
        account = self._request("GET", "")
        if account.get("id") != self.account_id:
            raise GatewayError("gateway_account_identity_changed")
        connection = account.get("connection")
        if connection is not None and (not isinstance(connection, str)
                or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", connection)):
            raise GatewayError("gateway_invalid_account_status")
        return {
            "authenticated": account.get("authenticated") is True,
            "connection": connection,
            "ownUsername": account.get("ownUsername"),
        }

    def require_account(self, own_username):
        """Bind the caller's intended real account before any probe submission."""
        own_username = _identifier(own_username, maximum=256)
        prior = getattr(self, "_expected_own_username", None)
        if prior is not None and prior != own_username:
            raise GatewayError("gateway_expected_account_mismatch")
        self._expected_own_username = own_username
        self._bound_identity()

    def capabilities(self):
        value = self._request("GET", "/capabilities")
        if not isinstance(value.get("nativeRuntime"), bool):
            raise GatewayError("gateway_invalid_capabilities")
        # Preserve quotedTextSendLiveValidated verbatim; no synthetic probe
        # can upgrade this flag or establish phone delivery.
        return value

    @staticmethod
    def _consumer(value, consumer):
        if (not isinstance(value, dict) or value.get("consumerId") != consumer
                or value.get("required") is not True):
            raise GatewayError("gateway_invalid_consumer")
        registered = _cursor(value.get("registeredAfter"))
        acknowledged = _cursor(value.get("acknowledgedCursor"))
        delivered = _cursor(value.get("deliveredCursor"))
        if not int(registered) <= int(acknowledged) <= int(delivered):
            raise GatewayError("gateway_invalid_consumer")
        return value

    def register_consumer(self, consumer, *, after="0"):
        """Explicitly protect events after a previously processed cursor.

        An expired cursor or changed registration is an error; this method
        never silently registers at the current head or replaces a consumer.
        """
        _identifier(consumer, maximum=128)
        _cursor(after)
        value = self._request("PUT", "/consumers/" + quote(consumer, safe=""), body={"after": after})
        state = self._consumer(value, consumer)
        if state["registeredAfter"] != after:
            raise GatewayError("gateway_consumer_registration_mismatch")
        return state

    def consumer_status(self, consumer):
        _identifier(consumer, maximum=128)
        return self._consumer(self._request("GET", "/consumers/" + quote(consumer, safe="")), consumer)

    def activate_native_consumer(self, consumer, *, own_username):
        """Atomically register at the gateway's verified native receive boundary.

        Repeating activation returns the original checkpoint, even after a
        daemon restart. No caller-provided cursor or readiness flag is accepted.
        """
        _identifier(consumer, maximum=128)
        self.require_account(own_username)
        value = self._request("POST", "/consumers/" + quote(consumer, safe="") + "/activate",
                              body={"ownUsername": own_username})
        state = self._consumer(value.get("consumer"), consumer)
        checkpoint = value.get("checkpoint")
        if (not isinstance(checkpoint, dict) or type(checkpoint.get("schemaVersion")) is not int
                or checkpoint["schemaVersion"] != 1
                or checkpoint.get("accountId") != self.account_id
                or checkpoint.get("ownUsername") != own_username
                or checkpoint.get("readiness") != "native_ingress_caught_up"):
            raise GatewayError("gateway_invalid_receive_checkpoint")
        if _cursor(checkpoint.get("eventCursor")) != state["registeredAfter"]:
            raise GatewayError("gateway_invalid_receive_checkpoint")
        _cursor(checkpoint.get("receivedBoundary"))
        _identifier(checkpoint.get("ownershipGeneration"), maximum=256)
        with self._journal.lock:
            server = self._journal.conn.execute("SELECT value FROM gateway_meta WHERE key='server_id'").fetchone()
        if server is None or checkpoint.get("serverId") != server[0]:
            raise GatewayError("gateway_invalid_receive_checkpoint")
        return value

    def receive(self, *, consumer=None, after=None, limit=100):
        """Fetch events without acking. Unregistered reads do not retain events."""
        if after is not None:
            _cursor(after)
        if type(limit) is not int or not 1 <= limit <= 200:
            raise GatewayError("invalid_gateway_event_limit")
        params = {"limit": limit}
        if consumer is not None:
            _identifier(consumer, maximum=128)
            path = "/consumers/" + quote(consumer, safe="") + "/events"
            if after is not None:
                params["after"] = after
        else:
            path = "/events"
            after = after or "0"
            params["after"] = after
        page = self._request("GET", path, params=params)
        if consumer is not None:
            state = self._consumer(page.get("consumer"), consumer)
            if after is None:
                after = state["acknowledgedCursor"]
        if not isinstance(page.get("items"), list) or len(page["items"]) > limit:
            raise GatewayError("gateway_invalid_events")
        last = after
        for event in page["items"]:
            if not isinstance(event, dict):
                raise GatewayError("gateway_invalid_events")
            cursor = _cursor(event.get("cursor"))
            if int(cursor) <= int(last) or not isinstance(event.get("kind"), str) or not event["kind"]:
                raise GatewayError("gateway_invalid_events")
            if event["kind"] == "message.committed":
                data = event.get("data")
                if not isinstance(data, dict):
                    raise GatewayError("gateway_invalid_events")
                _identifier(data.get("messageId"))
                _identifier(data.get("threadId"), maximum=256)
            if event["kind"] == "message.received":
                data = event.get("data")
                if (not isinstance(data, dict)
                        or data.get("disposition") not in {"ready", "outgoing", "system", "quarantined"}):
                    raise GatewayError("gateway_invalid_events")
                _identifier(data.get("receivedId"))
                if _cursor(data.get("firstReceivedSequence")) == "0":
                    raise GatewayError("gateway_invalid_events")
            last = cursor
        if _cursor(page.get("nextCursor")) != last:
            raise GatewayError("gateway_invalid_events")
        if consumer is not None and int(last) > int(state["deliveredCursor"]):
            raise GatewayError("gateway_invalid_consumer")
        return page

    def ack(self, consumer, cursor):
        _identifier(consumer, maximum=128)
        _cursor(cursor)
        result = self._request("POST", "/consumers/" + quote(consumer, safe="") + "/ack", body={"cursor": cursor})
        if int(_cursor(result.get("cursor"))) < int(cursor) or result.get("messageReadStateChanged") is not False:
            raise GatewayError("gateway_invalid_ack")
        return result

    def get_message(self, message_id, *, thread_id=None, server_id=None):
        _identifier(message_id)
        if thread_id is not None:
            _identifier(thread_id, maximum=256)
        if server_id is not None:
            _server_id(server_id)
        message = self._request("GET", "/messages/" + quote(message_id, safe=""))
        if message.get("id") != message_id or message.get("accountId") != self.account_id:
            raise GatewayError("gateway_message_identity_mismatch")
        _identifier(message.get("threadId"), maximum=256)
        if message.get("senderId") is not None:
            _identifier(message["senderId"], maximum=256)
        if message.get("serverId") is not None:
            _server_id(message["serverId"])
        if ((thread_id is not None and message["threadId"] != thread_id)
                or (server_id is not None and message.get("serverId") != server_id)):
            raise GatewayError("gateway_message_identity_mismatch")
        reference = message.get("quote")
        if reference is not None:
            if not isinstance(reference, dict):
                raise GatewayError("gateway_invalid_message")
            _server_id(reference.get("serverId"))
            if reference.get("senderId") is not None:
                _identifier(reference["senderId"], maximum=256)
        return message

    def get_received_message(self, received_id, *, thread_id=None, server_id=None, source_sha256=None):
        """Read a server-owned native snapshot, without official-profile lookup.

        Quarantined snapshots remain explicit pending work; nullable identity
        must never be replaced by display text or a locally guessed sender.
        """
        _identifier(received_id)
        self._bound_identity()
        value = self._request("GET", "/received/messages/" + quote(received_id, safe=""))
        if value.get("receivedId") != received_id or value.get("accountId") != self.account_id:
            raise GatewayError("gateway_received_identity_mismatch")
        own = _identifier(value.get("ownUsername"), maximum=256)
        with self._journal.lock:
            bound = self._journal.conn.execute("SELECT own_username FROM gateway_accounts WHERE account_id=?",
                                               (self.account_id,)).fetchone()
        if bound is None or own != bound[0]:
            raise GatewayError("gateway_received_identity_mismatch")
        if _cursor(value.get("firstReceivedSequence")) == "0":
            raise GatewayError("gateway_invalid_received_message")
        _source_hash(value.get("sourceSha256"))
        disposition = value.get("disposition")
        if disposition not in {"ready", "outgoing", "system", "quarantined"} or type(value.get("quoteEligible")) is not bool:
            raise GatewayError("gateway_invalid_received_message")
        for field in ("threadId", "senderId"):
            if value.get(field) is not None:
                _identifier(value[field], maximum=256)
        if value.get("serverId") is not None:
            _server_id(value["serverId"])
        if disposition == "ready":
            if (value.get("threadId") is None or value.get("senderId") is None
                    or value.get("serverId") is None or value.get("direction") != "incoming"
                    or type(value.get("type")) is not int or not isinstance(value.get("body"), str)):
                raise GatewayError("gateway_invalid_received_message")
        if disposition in {"system", "quarantined"} and value["quoteEligible"]:
            raise GatewayError("gateway_invalid_received_message")
        if ((thread_id is not None and value.get("threadId") != thread_id)
                or (server_id is not None and value.get("serverId") != server_id)
                or (source_sha256 is not None and value["sourceSha256"] != source_sha256)):
            raise GatewayError("gateway_received_identity_mismatch")
        return value

    def _inspect_intent(self, key, value):
        prior = self._journal.get(self.account_id, key)
        if (value.get("accountId") != self.account_id
                or value.get("destination") != prior["payload"]["threadId"]
                or "kind" in value or "threadId" in value
                or not isinstance(value.get("state"), str) or not value["state"]):
            raise GatewayError("gateway_send_identity_mismatch")
        _identifier(value.get("id"))
        if prior["intent_id"] is not None and value["id"] != prior["intent_id"]:
            raise GatewayError("gateway_send_identity_mismatch")
        for name in ("clientId", "localMessageId"):
            if value.get(name) is not None:
                _identifier(value[name])
        if value.get("serverId") is not None:
            _server_id(value["serverId"])
        if value["state"] in {"accepted", "sent", "server_accepted"}:
            _server_id(value.get("serverId"))
        return self._journal.save(self.account_id, key, value)

    @staticmethod
    def _result(key, intent, *, error=None, fresh=True):
        state = intent.get("state") if intent else None
        outcome = ("accepted" if state in {"accepted", "sent", "server_accepted"}
                   else "queued" if state in {"queued", "submitting"}
                   else "failed" if state in {"failed", "cancelled"} else "unknown")
        if not fresh and outcome != "accepted":
            outcome = "unknown"
        # "failed" preserves a daemon state, not proof of server rejection or
        # permission to send again. needs_review and new states stay unknown.
        return SendResult(key, outcome, intent, error.code if error else None,
                          error.http_status if error else None, fresh=fresh)

    def get_send_status(self, idempotency_key):
        key = _key(idempotency_key)
        self._bound_identity()
        return self._lookup_send_status(key)

    def _lookup_send_status(self, key):
        prior = self._journal.get(self.account_id, key)
        try:
            if prior["intent_id"] is not None:
                value = self._request("GET", "/sends/" + quote(prior["intent_id"], safe=""))
            else:
                value = self._request("GET", "/sends/by-idempotency-key", params={"key": key})
            return self._result(key, self._inspect_intent(key, value))
        except GatewayError as error:
            # Absence after a lost response does not prove non-submission. A
            # retained accepted receipt remains evidence even during outage.
            return self._result(key, prior["intent"], error=error, fresh=False)

    def send_quote(self, *, thread_id, message_id, server_id, text, idempotency_key,
                   recover_missing_intent=False):
        _identifier(thread_id, maximum=256)
        _identifier(message_id)
        _server_id(server_id)
        payload = {"kind": "text", "threadId": thread_id, "text": text,
                   "quote": {"messageId": message_id, "serverId": server_id}}
        return self._submit_quote(payload, idempotency_key, recover_missing_intent)

    def send_received_quote(self, *, thread_id, received_id, server_id, source_sha256,
                            text, idempotency_key, recover_missing_intent=False):
        """Submit only the native snapshot variant; never fall back to a profile."""
        _identifier(thread_id, maximum=256)
        _identifier(received_id)
        _server_id(server_id)
        _source_hash(source_sha256)
        payload = {"kind": "text", "threadId": thread_id, "text": text,
                   "quote": {"receivedId": received_id, "serverId": server_id,
                             "sourceSha256": source_sha256}}
        return self._submit_quote(payload, idempotency_key, recover_missing_intent)

    def _submit_quote(self, payload, idempotency_key, recover_missing_intent):
        key = _key(idempotency_key)
        text = payload["text"]
        if not isinstance(text, str) or not text or len(text.encode("utf-8")) > 64 * 1024:
            raise GatewayError("invalid_gateway_text")
        if type(recover_missing_intent) is not bool:
            raise GatewayError("invalid_gateway_recovery_option")
        generation = self._bound_identity()
        if not self._journal.claim(self.account_id, key, payload, generation):
            result = self._lookup_send_status(key)
            if (not recover_missing_intent or result.http_status != 404
                    or result.error_code != "not_found" or result.intent is not None):
                return result
            prior = self._journal.get(self.account_id, key)
            # Losing a previously observed intent is not an admission gap.
            # The caller must reconcile that evidence, not create a new row.
            if prior["intent_id"] is not None:
                return result
            if prior["ownership_generation"] != generation or self._bound_identity() != generation:
                return self._result(key, None, error=GatewayError("gateway_recovery_epoch_changed"), fresh=False)
            # Same runtime + explicit caller exclusion of restore/other writers
            # is a controlled recovery precondition, not a general HTTP fence.
            # Claim guarantees payload equality, so reuse its retained bytes.
            payload = prior["payload"]
        try:
            value = self._request("POST", "/sends", body=payload, key=key)
            return self._result(key, self._inspect_intent(key, value))
        except GatewayError as error:
            if error.http_status == 409 and error.code in NATIVE_ADMISSION_REJECTIONS:
                return SendResult(key, "rejected", error_code=error.code, http_status=409,
                                  direct_post_rejection=True)
            if error.http_status is not None and 400 <= error.http_status < 500 and error.http_status != 408:
                # Preserve the actual API rejection. The claimed key stays
                # retained, and later calls can only inspect its status.
                raise
            # A lost recovery response gets a lookup only. No recursive POST,
            # even when the explicit recovery option was supplied this time.
            return self._lookup_send_status(key)

    def close(self):
        self._http.close()
        self._journal.close()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
