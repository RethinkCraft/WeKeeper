"""Durable inbox fault checks against the pinned native HTTP field contracts.

All account data is synthetic and every HTTP call uses MockTransport. These
checks do not establish actual sync readiness or native/phone delivery.
"""
from copy import deepcopy
import json
import sqlite3

import httpx
import pytest

from chatlog_keeper.bot.protocol_gateway import GatewayError, ProtocolGateway
from chatlog_keeper.bot.protocol_inbox import InboxError, ProtocolInbox


ACCOUNT = "synthetic-account"
OWN = "synthetic-bot"
THREAD = "synthetic-group@chatroom"
CONSUMER = "inbox-fixture"
NOW = "2026-10-29T12:00:00Z"


def event(cursor, message_id=None, *, direction="incoming", origin="catchup"):
    return {"cursor": str(cursor), "kind": "message.committed" if message_id else "sync.committed",
            "data": {"messageId": message_id, "threadId": THREAD,
                     "direction": direction, "origin": origin} if message_id else {"profileCheckpoint": "complete"},
            "createdAt": NOW}


def message(message_id="opaque-1", server_id="18446744073709551615", **changes):
    return {"id": message_id, "accountId": ACCOUNT, "threadId": THREAD,
            "serverId": server_id, "senderId": "synthetic-sender", "type": 1,
            "direction": "incoming", "body": "synthetic same text", "createdAt": NOW,
            "mentions": {"state": "known", "userIds": [OWN], "self": True,
                         "all": False, "source": "msgsource"}, "media": [], "version": "1", **changes}


class NativeFixture:
    """Small HTTP peer: required registration, delivered watermark, monotonic ACK."""

    def __init__(self, tmp_path):
        self.root = tmp_path
        self.events = []
        self.messages = {}
        self.registered = None
        self.acknowledged = "0"
        self.delivered = "0"
        self.own_username = OWN
        self.server_id = "synthetic-server"
        self.calls = []
        self.on_ack = None
        self.fail_ack = None
        self.on_receive = None

    def add(self, server_id="18446744073709551615", *, message_id=None, **changes):
        message_id = message_id or "opaque-" + str(len(self.events) + 1)
        self.events.append(event(len(self.events) + 1, message_id))
        self.messages[message_id] = message(message_id, server_id, **changes)
        return message_id

    def state(self):
        return {"consumerId": CONSUMER, "required": True, "registeredAfter": self.registered,
                "acknowledgedCursor": self.acknowledged, "deliveredCursor": self.delivered}

    def handle(self, request):
        assert request.url.host == "127.0.0.1"
        assert request.headers["authorization"] == "Bearer synthetic-token"
        self.calls.append((request.method, request.url.path))
        if request.url.path == "/v1/status":
            return httpx.Response(200, json={"serverId": self.server_id,
                                           "ownershipGeneration": "synthetic-runtime"})
        path = request.url.path.removeprefix("/v1/accounts/" + ACCOUNT)
        if path == "":
            return httpx.Response(200, json={"id": ACCOUNT, "ownUsername": self.own_username})
        if path == "/consumers/" + CONSUMER and request.method == "PUT":
            after = json.loads(request.content)["after"]
            if self.registered is None:
                self.registered = self.acknowledged = self.delivered = after
            elif self.registered != after:
                return httpx.Response(409, json={"error": {"code": "consumer_registration_conflict"}})
            return httpx.Response(200, json=self.state())
        if path == "/consumers/" + CONSUMER + "/events":
            if self.on_receive:
                result = self.on_receive(request)
                if result is not None:
                    return result
            after = request.url.params.get("after", self.acknowledged)
            assert int(after) <= int(self.delivered)
            items = [item for item in self.events if int(item["cursor"]) > int(after)]
            items = items[:int(request.url.params["limit"])]
            next_cursor = items[-1]["cursor"] if items else after
            self.delivered = str(max(int(self.delivered), int(next_cursor)))
            return httpx.Response(200, json={"items": items, "nextCursor": next_cursor,
                                           "consumer": self.state()})
        if path == "/consumers/" + CONSUMER + "/ack":
            cursor = json.loads(request.content)["cursor"]
            assert int(cursor) <= int(self.delivered)
            if self.on_ack:
                self.on_ack(cursor)
            if self.fail_ack == "before":
                raise httpx.ReadTimeout("synthetic", request=request)
            self.acknowledged = str(max(int(self.acknowledged), int(cursor)))
            if self.fail_ack == "after":
                raise httpx.ReadTimeout("synthetic", request=request)
            return httpx.Response(200, json={"cursor": self.acknowledged, "messageReadStateChanged": False})
        if path.startswith("/messages/"):
            projected = self.messages.get(path.removeprefix("/messages/"))
            return httpx.Response(200, json=projected) if projected else httpx.Response(
                404, json={"error": {"code": "not_found"}})
        raise AssertionError((request.method, path))

    def gateway(self):
        return ProtocolGateway(token="synthetic-token", account_id=ACCOUNT,
                               journal_path=self.root / "gateway.sqlite",
                               transport=httpx.MockTransport(self.handle))

    def inbox(self, gateway, **changes):
        return ProtocolInbox(gateway, path=self.root / "inbox.sqlite",
                             own_username=changes.pop("own_username", OWN),
                             consumer=changes.pop("consumer", CONSUMER), **changes)

    def sql(self, query):
        with sqlite3.connect(self.root / "inbox.sqlite") as db:
            return db.execute(query).fetchall()

    def activate(self, inbox, cursor="0"):
        inbox.activate(cutover_cursor=cursor, initial_sync_ready="synthetic-profile-checkpoint-complete")

    @property
    def acks(self):
        return [call for call in self.calls if call[1].endswith("/ack")]


@pytest.fixture
def native(tmp_path):
    return NativeFixture(tmp_path)


def test_explicit_post_sync_cutover_and_restart_catchup(native):
    native.add("11")  # Existing history below the explicit ready barrier.
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox, "1")
        native.add("12")  # catchup is valid after the barrier, not discarded.
        result = inbox.poll()
        pending = inbox.pending_requests()
        assert result.new_requests == 1
        assert [item["server_id"] for item in pending] == ["12"]
        assert pending[0]["event_cursor"] == "2"
        assert pending[0]["message_id"] == "opaque-2"
        assert pending[0]["request_id"] not in {"2", "opaque-2", "12"}
        assert pending[0]["received_at"]
    native.add("13")
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        assert inbox.pending_requests() == pending
        assert inbox.poll().new_requests == 1
        assert [item["server_id"] for item in inbox.pending_requests()] == ["12", "13"]
        assert native.registered == "1"


@pytest.mark.parametrize("proof", [None, True, False, "", "   "])
def test_initial_sync_readiness_cannot_be_inferred_or_boolean(native, proof):
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        with pytest.raises(InboxError, match="inbox_not_activated"):
            inbox.poll()
        with pytest.raises(InboxError, match="inbox_initial_sync_proof_required"):
            inbox.activate(cutover_cursor="0", initial_sync_ready=proof)
        assert native.calls == []


def test_activation_cannot_change_cutover(native):
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        with pytest.raises(InboxError, match="inbox_cutover_changed"):
            native.activate(inbox, "1")
        assert native.registered == "0"


def test_entire_page_requests_and_raw_events_durable_before_ack(native):
    native.add("9007199254740993")
    native.add("18446744073709551615")
    native.events.append(event(3))

    def check_disk(cursor):
        assert cursor == "3"
        assert native.sql("SELECT COUNT(*) FROM inbox_events WHERE committed=1") == [(3,)]
        assert native.sql("SELECT COUNT(*) FROM inbox_requests") == [(2,)]
        assert native.sql("SELECT committed_cursor FROM inbox_meta") == [("3",)]
        assert native.sql("SELECT COUNT(*) FROM inbox_page") == [(0,)]

    native.on_ack = check_disk
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        assert inbox.poll().new_requests == 2
        assert inbox.poll().received_events == 0
        assert [item["server_id"] for item in inbox.pending_requests()] == ["9007199254740993", "18446744073709551615"]


@pytest.mark.parametrize("failure", ["before", "after"])
def test_ack_failure_and_restart_reconcile_without_lost_or_duplicate_request(native, failure):
    native.add()
    native.fail_ack = failure
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        with pytest.raises(GatewayError, match="gateway_transport_unavailable"):
            inbox.poll()
        pending = inbox.pending_requests()
        assert len(pending) == 1
        assert native.sql("SELECT committed_cursor,acknowledged_cursor FROM inbox_meta") == [("1", "0")]
    native.fail_ack = None
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        assert inbox.poll().new_requests == 0
        assert inbox.pending_requests() == pending
        assert native.acknowledged == "1"
        assert len(native.acks) == (2 if failure == "before" else 1)


def test_missing_projection_staged_before_read_then_recovered_on_restart(native):
    first = native.add("10")
    second = native.add("20")
    projection = native.messages.pop(second)
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        with pytest.raises(GatewayError, match="not_found"):
            inbox.poll()
        assert native.acks == []
        assert inbox.pending_requests() == []  # No half-page requests escape.
        assert native.sql("SELECT COUNT(*) FROM inbox_events WHERE committed=0") == [(2,)]
        times = native.sql("SELECT received_at FROM inbox_events ORDER BY cursor")
    native.messages[second] = projection
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        result = inbox.poll()
        assert result.received_events == 2
        assert result.new_requests == 2
        assert native.acknowledged == "2"
        assert native.sql("SELECT received_at FROM inbox_events ORDER BY cursor") == times
        reads = [path for method, path in native.calls if path.endswith("/events")]
        assert len(reads) == 1  # Recover durable page, not a new live-only read.
        assert inbox.pending_requests()[0]["message_id"] == first


@pytest.mark.parametrize("field", ["serverId", "senderId", "direction", "type", "body"])
def test_partial_projection_cannot_be_acked(native, field):
    source = native.add()
    previous = native.messages[source][field]
    native.messages[source][field] = None
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        with pytest.raises(InboxError, match="inbox_message_projection_incomplete"):
            inbox.poll()
        assert native.acks == []
        native.messages[source][field] = previous
        assert inbox.poll().new_requests == 1


def test_same_native_identity_dedupes_different_events_and_opaque_ids(native):
    native.add("22")
    native.add("22", version="new-view")
    native.add("23", senderId="another-sender")
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        assert inbox.poll().new_requests == 2
        assert native.sql("SELECT COUNT(*) FROM inbox_events") == [(3,)]
        assert native.sql("SELECT COUNT(*) FROM inbox_message_ids") == [(3,)]
        pending = inbox.pending_requests()
        inbox.complete(pending[0]["request_id"])
    native.add("22")
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        assert inbox.poll().new_requests == 0
        assert [item["server_id"] for item in inbox.pending_requests()] == ["23"]


@pytest.mark.parametrize("change", [{"body": "changed"}, {"senderId": "other"}, {"type": 3}])
def test_same_server_identity_changed_content_rolls_back_entire_page(native, change):
    native.add("100")
    native.add("100", **change)
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        with pytest.raises(InboxError, match="inbox_message_content_conflict"):
            inbox.poll()
        assert inbox.pending_requests() == []
        assert native.acks == []
        assert native.sql("SELECT committed_cursor FROM inbox_meta") == [("0",)]
        assert native.sql("SELECT COUNT(*) FROM inbox_events WHERE committed=0") == [(2,)]


def test_same_opaque_id_cannot_be_remapped_to_different_server_identity(native):
    source = native.add("100")
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        inbox.poll()
        native.events.append(event(2, source))
        native.messages[source]["serverId"] = "200"
        with pytest.raises(InboxError, match="inbox_message_identity_conflict"):
            inbox.poll()
        assert native.acknowledged == "1"
        assert len(inbox.pending_requests()) == 1


def test_same_server_id_in_distinct_threads_is_distinct_request(native):
    native.add("100")
    source = native.add("100", threadId="other-group@chatroom")
    native.events[-1]["data"]["threadId"] = "other-group@chatroom"
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        assert inbox.poll().new_requests == 2


def test_outgoing_and_unknown_events_are_durable_without_requests(native):
    native.events = [event(1, "outgoing-local", direction="outgoing", origin="local_send"), event(2)]
    native.events[1]["kind"] = "future.event"
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        batch = inbox.poll()
        assert (batch.received_events, batch.new_requests) == (2, 0)
        assert native.sql("SELECT COUNT(*) FROM inbox_events WHERE committed=1") == [(2,)]
        assert not any("/messages/" in path for _, path in native.calls)


@pytest.mark.parametrize("system_type", [10000, 10002])
def test_normalized_system_notice_does_not_block_user_message_same_page(native, system_type):
    native.add("10", type=system_type, senderId=None, direction=None, body=None)
    native.add("20")
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        batch = inbox.poll()
        assert (batch.received_events, batch.new_requests) == (2, 1)
        assert native.acknowledged == "2"
        assert [item["server_id"] for item in inbox.pending_requests()] == ["20"]
        saved = native.sql("SELECT classification,projection_json FROM inbox_events WHERE cursor='1'")[0]
        assert saved[0] == "system_notice"
        assert json.loads(saved[1])["type"] == system_type
        assert json.loads(saved[1])["senderId"] is None


@pytest.mark.parametrize("stage", ["raw", "requests"])
def test_disk_write_failure_never_acks_and_retries_same_page(native, stage):
    native.add("100")
    native.add("200")
    table = "inbox_events" if stage == "raw" else "inbox_requests"
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        # Real SQLite abort after the first row proves full transaction rollback.
        native.sql(f"CREATE TRIGGER full_disk BEFORE INSERT ON {table} WHEN (SELECT COUNT(*) FROM {table})=1 BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END")
        with pytest.raises(sqlite3.IntegrityError, match="synthetic disk failure"):
            inbox.poll()
        assert native.acks == []
        assert inbox.pending_requests() == []
        assert native.sql("SELECT committed_cursor FROM inbox_meta") == [("0",)]
        assert native.sql("SELECT COUNT(*) FROM inbox_events") == [(0 if stage == "raw" else 2,)]
        native.sql("DROP TRIGGER full_disk")
        assert inbox.poll().new_requests == 2
        assert native.acknowledged == "2"


def test_ack_response_local_write_failure_recovers_from_remote_ack(native):
    native.add()
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        native.sql("""CREATE TRIGGER full_disk BEFORE UPDATE OF acknowledged_cursor ON inbox_meta
            WHEN NEW.acknowledged_cursor='1' BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END""")
        with pytest.raises(sqlite3.IntegrityError, match="synthetic disk failure"):
            inbox.poll()
        assert native.acknowledged == "1"
        assert len(inbox.pending_requests()) == 1
        native.sql("DROP TRIGGER full_disk")
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        inbox.poll()
        assert native.sql("SELECT acknowledged_cursor FROM inbox_meta") == [("1",)]
        assert len(native.acks) == 1


def test_cursor_gap_and_expiry_do_not_reset_to_head(native):
    native.add()
    native.events[0]["cursor"] = "2"
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        with pytest.raises(InboxError, match="inbox_event_cursor_gap"):
            inbox.poll()
        assert native.acks == []
        assert native.sql("SELECT committed_cursor FROM inbox_meta") == [("0",)]
        native.on_receive = lambda _: httpx.Response(410, json={"error": {"code": "event_cursor_expired"}})
        with pytest.raises(GatewayError, match="event_cursor_expired"):
            inbox.poll()
        assert native.registered == "0"


def test_remote_ack_ahead_is_not_treated_as_local_success(native):
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        native.acknowledged = native.delivered = "2"
        with pytest.raises(InboxError, match="inbox_remote_ack_ahead"):
            inbox.poll()
        assert inbox.pending_requests() == []
        assert native.acks == []


def test_remote_rollback_stops_before_further_receive(native):
    native.add()
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        inbox.poll()
        native.acknowledged = native.delivered = "0"
        count = len(native.calls)
        with pytest.raises(InboxError, match="inbox_remote_cursor_regressed"):
            inbox.poll()
        assert not any(path.endswith("/events") for _, path in native.calls[count:])


@pytest.mark.parametrize("change,code", [("account", "gateway_expected_account_mismatch"),
                                         ("server", "gateway_server_identity_changed")])
def test_real_account_or_gateway_change_stops_before_receive(native, change, code):
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        if change == "account":
            native.own_username = "different-account"
        else:
            native.server_id = "different-server"
        with pytest.raises(GatewayError, match=code):
            inbox.poll()
        assert native.acks == []
        assert not any(path.endswith("/events") for _, path in native.calls)


def test_inbox_binding_and_exclusive_writer_survive_restart(native):
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        with pytest.raises(InboxError, match="inbox_already_open"):
            native.inbox(gateway)
    with native.gateway() as gateway:
        with pytest.raises(InboxError, match="inbox_binding_changed"):
            native.inbox(gateway, own_username="other-own-wxid")
        with pytest.raises(InboxError, match="inbox_binding_changed"):
            native.inbox(gateway, consumer="other-consumer")
        with native.inbox(gateway) as inbox:
            assert inbox.poll().new_requests == 0


def test_raw_event_conflict_is_not_ignored(native):
    native.add()
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        # An already journaled cursor must compare exact canonical event data.
        raw = deepcopy(native.events[0])
        raw["data"]["origin"] = "different"
        with sqlite3.connect(native.root / "inbox.sqlite") as db:
            db.execute("INSERT INTO inbox_events(cursor,raw_json,received_at) VALUES(?,?,?)",
                       ("1", json.dumps(raw, sort_keys=True, separators=(",", ":")), NOW))
        with pytest.raises(InboxError, match="inbox_event_content_conflict"):
            inbox.poll()
        assert native.acks == []


@pytest.mark.parametrize("field,value", [("serverId", 123), ("threadId", 123), ("id", 123)])
def test_numeric_identity_is_rejected_without_ack(native, field, value):
    source = native.add()
    native.messages[source][field] = value
    with native.gateway() as gateway, native.inbox(gateway) as inbox:
        native.activate(inbox)
        with pytest.raises(GatewayError):
            inbox.poll()
        assert native.acks == []
