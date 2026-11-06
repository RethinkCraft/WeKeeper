"""Native receive boundary and custody tests; no live accounts or sends."""
from copy import deepcopy
import json

import httpx
import pytest

from chatlog_keeper.bot.protocol_gateway import GatewayError
from chatlog_keeper.bot.protocol_inbox import InboxError
from chatlog_keeper.bot.protocol_native_inbox import ProtocolNativeInbox
from test_bot_protocol_inbox import NativeFixture, ACCOUNT, OWN, CONSUMER, event


class DirectFixture(NativeFixture):
    def __init__(self, path):
        super().__init__(path)
        self.checkpoint_cursor = "0"
        self.received_boundary = "0"
        self.checkpoint = None
        self.ready = True
        self.lose_activation_response = False

    def add(self, server_id="18446744073709551615", **changes):
        disposition = changes.pop("disposition", "ready")
        identifier = super().add(server_id, **changes)
        seq = str(len(self.events))
        self.events[-1]["kind"] = "message.received"
        self.events[-1]["data"] = {"receivedId": identifier, "firstReceivedSequence": seq,
                                    "disposition": disposition}
        self.messages[identifier].update(receivedId=identifier, firstReceivedSequence=seq,
            ownUsername=OWN, sourceSha256="a" * 64, disposition=disposition,
            reason="unsupported_type" if disposition == "quarantined" else None,
            quoteEligible=disposition == "ready")
        return identifier

    def handle(self, request):
        path = request.url.path.removeprefix("/v1/accounts/" + ACCOUNT)
        if path == "/consumers/" + CONSUMER + "/activate":
            self.calls.append((request.method, request.url.path))
            assert request.method == "POST"
            assert json.loads(request.content) == {"ownUsername": OWN}
            if self.checkpoint is None:
                if not self.ready:
                    return httpx.Response(409, json={"error": {"code": "native_ingress_not_ready"}})
                self.registered = self.acknowledged = self.delivered = self.checkpoint_cursor
                self.checkpoint = {"schemaVersion": 1, "accountId": ACCOUNT, "ownUsername": OWN,
                    "ownershipGeneration": "synthetic-runtime", "serverId": self.server_id,
                    "receivedBoundary": self.received_boundary, "eventCursor": self.checkpoint_cursor,
                    "readiness": "native_ingress_caught_up"}
            if self.lose_activation_response:
                raise httpx.ReadTimeout("synthetic", request=request)
            return httpx.Response(200, json={"consumer": self.state(), "checkpoint": self.checkpoint})
        if path.startswith("/received/messages/"):
            self.calls.append((request.method, request.url.path))
            message = self.messages.get(path.removeprefix("/received/messages/"))
            return httpx.Response(200, json=message) if message else httpx.Response(
                404, json={"error": {"code": "not_found"}})
        assert not path.startswith("/messages/"), "native inbox must never read profile messages"
        return super().handle(request)

    def inbox(self, gateway):
        return ProtocolNativeInbox(gateway, path=self.root / "inbox.sqlite",
                                   own_username=OWN, consumer=CONSUMER)


@pytest.fixture
def direct(tmp_path):
    return DirectFixture(tmp_path)


def test_native_checkpoint_required_and_no_caller_readiness_override(direct):
    direct.ready = False
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        with pytest.raises(InboxError, match="inbox_not_activated"):
            inbox.poll()
        with pytest.raises(TypeError):
            inbox.activate(cutover_cursor="0", initial_sync_ready="pretend-ready")
        with pytest.raises(GatewayError, match="native_ingress_not_ready"):
            inbox.activate()
        assert direct.sql("SELECT COUNT(*) FROM native_checkpoint") == [(0,)]


def test_lost_activation_response_reuses_original_server_boundary(direct):
    direct.checkpoint_cursor = direct.received_boundary = "10"
    direct.lose_activation_response = True
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        with pytest.raises(GatewayError):
            inbox.activate()
        assert direct.sql("SELECT COUNT(*) FROM native_checkpoint") == [(0,)]
    direct.lose_activation_response = False
    direct.checkpoint_cursor = direct.received_boundary = "20"  # head moved meanwhile
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        assert inbox._checkpoint()["receivedBoundary"] == "10"
        assert direct.sql("SELECT committed_cursor FROM inbox_meta") == [("10",)]


def test_late_old_native_record_and_profile_commit_never_become_new_requests(direct):
    direct.checkpoint_cursor = "1"
    direct.received_boundary = "9"
    old = direct.add("10")
    late = direct.add("11")
    direct.events[-1]["data"]["firstReceivedSequence"] = "8"
    direct.messages[late]["firstReceivedSequence"] = "8"
    direct.events.append(event(3, "late-profile-row"))
    current = direct.add("12")
    direct.events[-1]["data"]["firstReceivedSequence"] = "10"
    direct.messages[current]["firstReceivedSequence"] = "10"
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        batch = inbox.poll()
        assert batch.new_requests == 1
        assert [item["server_id"] for item in inbox.pending_requests()] == ["12"]
        assert direct.acknowledged == "4"
        assert not any(path.endswith((old, late, "late-profile-row")) for _, path in direct.calls)


def test_quarantine_remains_explicit_without_blocking_unrelated_ready_message(direct):
    direct.add("10", disposition="quarantined", senderId=None, body=None)
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        assert inbox.poll().new_requests == 1
        assert direct.acknowledged == "2"
        assert [item["server_id"] for item in inbox.pending_requests()] == ["20"]
        quarantine = inbox.quarantined_messages()
        assert len(quarantine) == 1
        assert quarantine[0]["message"]["reason"] == "unsupported_type"
        assert quarantine[0]["message"]["senderId"] is None
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        assert inbox.quarantined_messages() == quarantine
        assert inbox.poll().new_requests == 0


@pytest.mark.parametrize("disposition", ["system", "outgoing"])
def test_other_native_dispositions_preserve_snapshot_without_reply(direct, disposition):
    direct.add("10", disposition=disposition)
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        assert inbox.poll().new_requests == 0
        assert direct.sql("SELECT classification FROM inbox_events") == [(disposition,)]


@pytest.mark.parametrize("failure", ["before", "after"])
def test_native_ack_response_loss_and_restart_never_recreates_request(direct, failure):
    direct.add()
    direct.fail_ack = failure
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        with pytest.raises(GatewayError):
            inbox.poll()
        requests = inbox.pending_requests()
        checkpoint = deepcopy(inbox._checkpoint())
        assert len(requests) == 1
    direct.fail_ack = None
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        assert inbox.poll().new_requests == 0
        assert inbox._checkpoint() == checkpoint
        assert inbox.pending_requests() == requests
        assert direct.acknowledged == "1"


def test_native_snapshot_missing_preserves_whole_page_until_restart(direct):
    key = direct.add()
    snapshot = direct.messages.pop(key)
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        with pytest.raises(GatewayError, match="not_found"):
            inbox.poll()
        assert not direct.acks
        assert direct.sql("SELECT COUNT(*) FROM inbox_events WHERE committed=0") == [(1,)]
    direct.messages[key] = snapshot
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        assert inbox.poll().new_requests == 1


def test_checkpoint_mutation_is_rejected_without_new_receive(direct):
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        direct.checkpoint["receivedBoundary"] = "99"
        inbox._needs_activation = True  # B1: ordinary polls skip activate; force the check path.
        with pytest.raises(InboxError, match="inbox_native_checkpoint_changed"):
            inbox.poll()
        assert not any(path.endswith("/events") for _, path in direct.calls)


@pytest.mark.parametrize("field,value", [("serverId", "other-server"), ("ownUsername", "other"),
                                        ("eventCursor", "1"), ("readiness", "profile_caught_up")])
def test_gateway_rejects_unbound_activation_checkpoint(direct, field, value):
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        direct.checkpoint[field] = value
        inbox._needs_activation = True  # B1: ordinary polls skip activate; force the check path.
        with pytest.raises(GatewayError, match="gateway_invalid_receive_checkpoint"):
            inbox.poll()


def downgrade(direct, key, reason="message_recalled"):
    source = direct.messages[key]
    source.update(disposition="quarantined", reason=reason, quoteEligible=False)
    direct.events.append({"cursor": str(len(direct.events) + 1), "kind": "message.received",
        "data": {"receivedId": key, "firstReceivedSequence": source["firstReceivedSequence"],
                 "disposition": "quarantined"}})


def test_completion_cannot_erase_later_quarantine(direct):
    key = direct.add("10")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        request_id = inbox.pending_requests()[0]["request_id"]
        downgrade(direct, key); inbox.poll()
        with pytest.raises(InboxError, match="inbox_request_quarantined"):
            inbox.complete(request_id)
        assert inbox.request_state(request_id) == "quarantined"


@pytest.mark.parametrize("reason", ["message_recalled", "native_message_identity_conflict"])
@pytest.mark.parametrize("prior", ["ready", "outgoing", "system"])
def test_downgraded_snapshot_before_old_ready_event_does_not_block_next_request(direct, reason, prior):
    key = direct.add("10", disposition=prior)
    direct.add("20")
    downgrade(direct, key, reason)
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        assert inbox.poll().new_requests == 1
        assert direct.acknowledged == "3"
        assert [item["server_id"] for item in inbox.pending_requests()] == ["20"]
        assert len(inbox.quarantined_messages()) == 1
        assert inbox.quarantined_messages()[0]["message"]["reason"] == reason


@pytest.mark.parametrize("completed", [False, True])
def test_later_quarantine_suspends_only_pending_work_and_remains_visible(direct, completed):
    key = direct.add("10")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        inbox.poll()
        request_id = inbox.pending_requests()[0]["request_id"]
        if completed:
            inbox.complete(request_id)
        downgrade(direct, key)
        direct.add("20")

        def inspect_atomic_state(cursor):
            assert cursor == "3"
            assert direct.sql("SELECT state FROM inbox_requests WHERE server_id='10'") == [
                ("completed" if completed else "quarantined",)]
            assert direct.sql("SELECT COUNT(*) FROM inbox_events WHERE committed=1") == [(3,)]

        direct.on_ack = inspect_atomic_state
        inbox.poll()
        assert [item["server_id"] for item in inbox.pending_requests()] == ["20"]
        record = inbox.quarantined_messages()[0]
        assert record["request_state"] == ("completed" if completed else "quarantined")
        assert inbox.request_state(request_id) == ("completed" if completed else "quarantined")
        assert inbox.source_quarantine_reason(key) == "message_recalled"
        assert inbox.source_quarantine_reason("unknown-source") is None


def test_unexplained_disposition_mutation_cannot_silently_replace_old_event(direct):
    key = direct.add()
    direct.messages[key].update(disposition="quarantined", quoteEligible=False, reason="invented")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        with pytest.raises(InboxError, match="inbox_received_event_conflict"):
            inbox.poll()
        assert not direct.acks


def request_for(inbox, server_id):
    return next(row["request_id"] for row in inbox.pending_requests() if row["server_id"] == server_id)


def test_nearby_same_body_preserves_distinct_ids_dedupes_sid_and_survives_restart(direct):
    direct.add("10", createdAt="2026-10-29T12:00:00Z")
    direct.add("10", createdAt="2026-10-29T12:00:00Z")  # received-ID alias
    direct.add("11", createdAt="2026-10-29T11:59:59Z")
    direct.add("20", createdAt="2026-10-29T12:00:01Z")
    direct.add("21", createdAt="2026-10-29T11:59:58Z")  # later receipt, older clock
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        trigger = request_for(inbox, "20")
        inbox.complete(request_for(inbox, "10"))  # context is not only pending work
        calls = list(direct.calls)
        context = inbox.nearby_context(trigger)
        assert direct.calls == calls
        assert [row["server_id"] for row in context] == ["11", "10"]
        assert len({row["message_ref"] for row in context}) == 2
        assert {row["text"] for row in context} == {"synthetic same text"}
        assert [row["create_time"] for row in context] == sorted(row["create_time"] for row in context)
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        calls = list(direct.calls)
        assert inbox.nearby_context(trigger) == context
        assert direct.calls == calls


def test_nearby_preserves_equal_timestamp_receipt_order_and_configured_bound(direct):
    for sid in range(10, 18):
        direct.add(str(sid))
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        trigger = request_for(inbox, "20")
        assert [r["server_id"] for r in inbox.nearby_context(trigger)] == ["13", "14", "15", "16", "17"]
        assert [r["server_id"] for r in inbox.nearby_context(trigger, limit=2)] == ["16", "17"]
        assert inbox.nearby_context(trigger, limit=0) == []
        for invalid in (-1, 6, True, 1.0):
            with pytest.raises(InboxError, match="invalid_inbox_context_limit"):
                inbox.nearby_context(trigger, limit=invalid)
        with pytest.raises(InboxError, match="inbox_request_not_found"):
            inbox.nearby_context("unknown")


def test_nearby_only_same_group_ordinary_text_in_prior_ten_minutes(direct):
    direct.add("10", createdAt="2026-10-29T11:49:59Z")
    direct.add("11", createdAt="2026-10-29T19:50:00+08:00")  # inclusive 600-second edge
    direct.add("12", createdAt="2026-10-29T12:00:01Z")  # clock is after trigger
    direct.add("13", threadId="another-group@chatroom")
    direct.add("14", type=49, appMessageType=57, body="quoted source is not ordinary text")
    direct.add("15", disposition="system", type=10000)
    direct.add("16", disposition="outgoing", direction="outgoing", senderId=OWN)
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        rows = inbox.nearby_context(request_for(inbox, "20"))
        assert [r["server_id"] for r in rows] == ["11", "16"]
        assert rows[1]["sender_name"] == OWN


@pytest.mark.parametrize("field", ["accountId", "ownUsername"])
def test_nearby_cannot_leak_foreign_account_projection(direct, field):
    foreign = direct.add("10")
    direct.add("11")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        # Even an imported foreign-account row must not join local context.
        snapshot = {**direct.messages[foreign], field: "foreign-identity"}
        inbox._db.execute("UPDATE inbox_events SET projection_json=? WHERE message_id=?",
                          (json.dumps(snapshot), foreign))
        assert [r["server_id"] for r in inbox.nearby_context(request_for(inbox, "20"))] == ["11"]


def test_nearby_excludes_recalled_completed_alias_outgoing_and_initial_quarantine(direct):
    direct.add("10")
    alias = direct.add("10")
    direct.add("11", disposition="quarantined")
    outgoing = direct.add("12", disposition="outgoing", direction="outgoing", senderId=OWN)
    direct.add("13")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        trigger = request_for(inbox, "20")
        inbox.complete(request_for(inbox, "10"))
        downgrade(direct, alias)
        direct.messages[alias].update(serverId=None, threadId=None, senderId=None)
        downgrade(direct, outgoing)
        inbox.poll()
        assert [r["server_id"] for r in inbox.nearby_context(trigger)] == ["13"]


def test_nearby_excludes_quarantine_with_same_sid_but_unmapped_received_id(direct):
    direct.add("10")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        trigger = request_for(inbox, "20")
        direct.add("10", disposition="quarantined")
        inbox.poll()
        assert inbox.nearby_context(trigger) == []


def test_nearby_does_not_read_staged_uncommitted_projection(direct):
    prior = direct.add("10")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        inbox._db.execute("UPDATE inbox_events SET committed=0 WHERE message_id=?", (prior,))
        assert inbox.nearby_context(request_for(inbox, "20")) == []


def test_nearby_excludes_durable_quarantine_even_when_later_projection_blocks_page(direct):
    direct.add("10")
    alias = direct.add("10")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        trigger = request_for(inbox, "20")
        downgrade(direct, alias)
        missing = direct.add("30")
        direct.messages.pop(missing)
        with pytest.raises(GatewayError, match="not_found"):
            inbox.poll()
        assert direct.acknowledged == "3"
        assert direct.sql("SELECT COUNT(*) FROM inbox_events WHERE committed=0") == [(2,)]
        assert inbox.nearby_context(trigger) == []
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        assert inbox.nearby_context(trigger) == []


def test_ordinary_poll_skips_activation_within_interval(direct):
    direct.add("10")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate()
        before = [call for call in direct.calls if call[1].endswith("/activate")]
        assert inbox.poll().new_requests == 1
        after = [call for call in direct.calls if call[1].endswith("/activate")]
        assert after == before
        assert any(path.endswith("/events") for _, path in direct.calls)
