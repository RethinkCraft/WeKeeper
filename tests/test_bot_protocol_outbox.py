"""Native outbox recovery boundaries, using only a synthetic HTTP transport."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from itertools import count
import json
import sqlite3
import threading

import httpx
import pytest

from chatlog_keeper.bot.protocol_gateway import GatewayError, ProtocolGateway
from chatlog_keeper.bot.protocol_outbox import OutboxError, ProtocolOutbox


ACCOUNT, OWN, THREAD = "synthetic-account", "synthetic-bot", "synthetic-group@chatroom"
MAX_ID = "18446744073709551615"


def source(server_id=MAX_ID, **changes):
    return {"receivedId": "recv-" + server_id, "firstReceivedSequence": "10", "accountId": ACCOUNT,
            "ownUsername": OWN, "sourceSha256": "a" * 64, "disposition": "ready", "reason": None,
            "threadId": THREAD, "serverId": server_id, "senderId": "synthetic-peer", "direction": "incoming",
            "type": 1, "appMessageType": None, "createdAt": "2026-10-29T12:00:00Z", "body": "same input",
            "mentions": {"state": "known", "self": True, "userIds": [OWN], "all": False},
            "quote": None, "quoteEligible": True, **changes}


def frozen(index=1, **changes):
    return {"request_id": f"request-{index}", "reply_id": f"reply-{index}",
            "source": source(str(int(MAX_ID) - index + 1)), "text": "frozen answer",
            "evidence": {"business": "synthetic", "version": "1", "facts": [{"status": "done"}]}, **changes}


class Harness:
    def __init__(self, root):
        self.root = root
        self.requests, self.posts, self.intents = [], [], {}
        self.own_username = OWN
        self.on_post = None
        self.hide_status = False
        self.send_state, self.send_error = "accepted", None

    def outbox(self):
        return ProtocolOutbox(self.root / "outbox.sqlite", account_id=ACCOUNT, own_username=OWN)

    def client(self):
        return ProtocolGateway(token="synthetic-not-real-token", account_id=ACCOUNT,
                               journal_path=self.root / "gateway.sqlite", transport=httpx.MockTransport(self.handle))

    def handle(self, request):
        self.requests.append(request)
        if request.url.path == "/v1/status":
            return httpx.Response(200, json={"serverId": "synthetic-gateway", "ownershipGeneration": "epoch-1"})
        if request.url.path == "/v1/accounts/" + ACCOUNT:
            return httpx.Response(200, json={"id": ACCOUNT, "ownUsername": self.own_username})
        route = request.url.path.removeprefix("/v1/accounts/" + ACCOUNT)
        if route == "/sends" and request.method == "POST":
            key = request.headers["Idempotency-Key"]
            self.posts.append(request)
            self.intents[key] = {"id": "intent-" + key, "accountId": ACCOUNT, "destination": THREAD,
                "state": self.send_state, "clientId": str(100 + len(self.posts)),
                "serverId": str(int(MAX_ID) - 100 - len(self.posts)) if self.send_state == "accepted" else None,
                "errorCode": self.send_error, "createdAt": "2026-10-29T12:00:00Z"}
            if self.on_post:
                self.on_post(request)
            return httpx.Response(202, json=self.intents[key])
        if route.startswith("/sends/") and request.method == "GET":
            key = request.url.params["key"] if route.endswith("by-idempotency-key") else route.removeprefix("/sends/intent-")
            value = self.intents.get(key)
            if self.hide_status or value is None:
                return httpx.Response(404, json={"error": {"code": "not_found"}})
            return httpx.Response(200, json=value)
        raise AssertionError((request.method, request.url.path))


@pytest.fixture
def harness(tmp_path):
    return Harness(tmp_path)


def lose_response(request):
    raise httpx.ReadTimeout("synthetic private failure", request=request)


def test_freeze_is_immutable_atomic_replay_and_keeps_large_ids_as_strings(harness):
    value = frozen()
    with harness.outbox() as outbox:
        first = outbox.freeze(**value)
        value["source"]["body"] = "changed outside outbox"
        value["evidence"]["facts"][0]["status"] = "changed outside outbox"
        assert outbox.freeze(**frozen()) == first
        assert outbox.get("reply-1")["source"]["serverId"] == MAX_ID
        assert outbox.get("reply-1")["evidence"]["facts"][0]["status"] == "done"
        assert outbox.path.stat().st_mode & 0o777 == 0o600
    with harness.outbox() as outbox:
        assert outbox.get("reply-1") == first


def test_first_acceptance_observation_is_retained_across_replay_and_reopen(harness):
    harness.send_state = "queued"
    with harness.outbox() as outbox, harness.client() as gateway:
        outbox.freeze(**frozen())
        first = outbox.deliver_one(gateway, "reply-1")
        timing = first["observations"]
        assert "first_accepted_observed_at" not in timing
        span = timing["first_submission"]
        assert 0 <= span["account_preflight_end_offset_seconds"] <= span["command_start_offset_seconds"] <= span["ended_offset_seconds"]
        harness.intents["reply-1"].update(state="accepted", serverId="54321")
        accepted = outbox.reconcile(gateway, "reply-1")
        assert accepted["state"] == "accepted" and accepted["phone_received"] is False
        observations = accepted["observations"]
        assert observations["first_submission"] == span
        assert observations["first_gateway_result_observed_at"] == timing["first_gateway_result_observed_at"]
        assert observations["first_accepted_observed_at"]
        assert outbox.reconcile(gateway, "reply-1")["observations"] == observations
        assert len(harness.posts) == 1
    with harness.outbox() as outbox:
        assert outbox.get("reply-1")["observations"] == observations


def test_lost_response_keeps_attempt_span_without_faking_receipt_time(harness):
    harness.on_post = lose_response
    harness.hide_status = True
    with harness.outbox() as outbox, harness.client() as gateway:
        outbox.freeze(**frozen())
        unknown = outbox.deliver_one(gateway, "reply-1")
        assert unknown["state"] == "unknown"
        observations = unknown["observations"]
        assert observations["first_submission"]["ended_offset_seconds"] >= 0
        assert "first_accepted_observed_at" not in observations
        assert unknown["phone_received"] is False


@pytest.mark.parametrize("change", [
    {"reply_id": "another-reply"}, {"request_id": "another-request"}, {"text": "different answer"},
    {"evidence": {"version": "2"}}, {"source": source(sourceSha256="b" * 64)},
    {"source": source(body="changed source text")}, {"source": source(senderId="other-peer")},
    {"source": source("42")},
])
def test_any_frozen_identity_content_or_fact_change_is_rejected(harness, change):
    with harness.outbox() as outbox:
        before = outbox.freeze(**frozen())
        with pytest.raises(OutboxError, match="outbox_freeze_conflict"):
            outbox.freeze(**frozen(**change))
        assert outbox.get("reply-1") == before
        with pytest.raises(OutboxError, match="outbox_freeze_conflict"):
            outbox.freeze(**frozen(2, source=source()))


@pytest.mark.parametrize("change", [
    {"accountId": "other-account"}, {"ownUsername": "other-bot"}, {"direction": "outgoing"},
    {"senderId": OWN}, {"serverId": int(MAX_ID)}, {"sourceSha256": "wrong"}, {"type": True},
    {"quoteEligible": False}, {"disposition": "quarantined"}, {"createdAt": "2026-10-29"},
    {"id": "profile-message-must-not-replace-native-identity"},
])
def test_invalid_or_wrong_account_source_never_freezes(harness, change):
    with harness.outbox() as outbox:
        with pytest.raises((OutboxError, GatewayError)):
            outbox.freeze(**frozen(source=source(**change)))
        assert outbox.pending() == []


def test_missing_source_fields_and_non_json_facts_are_rejected(harness):
    with harness.outbox() as outbox:
        partial = source()
        del partial["mentions"]
        for change in [{"source": partial}, {"evidence": {1: "ambiguous key"}},
                       {"evidence": {"number": float("nan")}}]:
            with pytest.raises(OutboxError):
                outbox.freeze(**frozen(**change))
        assert outbox.pending() == []


def test_failed_freeze_leaves_no_partial_reply_or_evidence(harness):
    with harness.outbox() as outbox:
        outbox._db.execute("CREATE TEMP TRIGGER fail_freeze BEFORE INSERT ON replies BEGIN SELECT RAISE(ABORT,'synthetic'); END")
        with pytest.raises(sqlite3.IntegrityError):
            outbox.freeze(**frozen())
        assert outbox._db.execute("SELECT count(*) FROM replies").fetchone()[0] == 0
        outbox._db.execute("DROP TRIGGER fail_freeze")
        assert outbox.freeze(**frozen())["evidence"] == frozen()["evidence"]


def test_lost_http_response_queries_original_key_without_another_post(harness):
    harness.on_post = lose_response
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        result = outbox.deliver_one(gateway, "reply-1")
        assert result["state"] == "accepted"
        assert result["phone_received"] is False
        assert result["reply_id"] == "reply-1"
        assert len(harness.posts) == 1
        body = json.loads(harness.posts[0].content)
        assert body["quote"] == {"receivedId": source()["receivedId"], "serverId": MAX_ID, "sourceSha256": "a" * 64}
        assert body["text"] == frozen()["text"]
        assert outbox.freeze(**frozen()) == result


def test_accepted_restart_has_no_gateway_call_or_second_submission(harness):
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        accepted = outbox.deliver_one(gateway, "reply-1")
    before = len(harness.requests)
    with harness.client() as gateway, harness.outbox() as outbox:
        assert outbox.deliver_one(gateway, "reply-1") == accepted
        assert outbox.reconcile(gateway, "reply-1") == accepted
        assert outbox.pending() == []
    assert len(harness.requests) == before
    assert len(harness.posts) == 1


def test_unknown_does_not_block_later_reply_and_reconciles_without_resending(harness):
    harness.on_post, harness.hide_status = lose_response, True
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        assert outbox.deliver_one(gateway, "reply-1")["state"] == "unknown"
        outbox.freeze(**frozen(2))
        assert [r["reply_id"] for r in outbox.pending()] == ["reply-2"]
        harness.on_post, harness.hide_status = None, False
        assert outbox.deliver_one(gateway, "reply-2")["state"] == "accepted"
        assert outbox.reconcile(gateway, "reply-1")["state"] == "accepted"
    assert len(harness.posts) == 2


class SimulatedCrash(BaseException):
    pass


def test_crash_after_outbox_claim_but_before_client_claim_stays_unknown(harness, monkeypatch):
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        monkeypatch.setattr(gateway, "send_received_quote", lambda **_kwargs: (_ for _ in ()).throw(SimulatedCrash()))
        with pytest.raises(SimulatedCrash):
            outbox.deliver_one(gateway, "reply-1")
        assert outbox.get("reply-1")["state"] == "submitting"
    with harness.client() as gateway, harness.outbox() as outbox:
        unknown = outbox.deliver_one(gateway, "reply-1")
        assert unknown["state"] == "unknown"
        assert unknown["error_code"] == "outbox_submission_not_claimed"
        assert outbox.block("reply-1", "source_quarantined")["state"] == "unknown"
        recovered = outbox.reconcile(gateway, "reply-1")
        assert recovered["source_blocked"] == "source_quarantined"
        assert recovered["error_code"] == "outbox_submission_not_claimed"
        assert harness.posts == []


def test_crash_after_client_claim_and_remote_acceptance_recovers_by_original_key(harness):
    harness.on_post = lambda _request: (_ for _ in ()).throw(SimulatedCrash())
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        with pytest.raises(SimulatedCrash):
            outbox.deliver_one(gateway, "reply-1")
    harness.on_post = None
    with harness.client() as gateway, harness.outbox() as outbox:
        assert outbox.reconcile(gateway, "reply-1")["state"] == "accepted"
    assert len(harness.posts) == 1


def test_local_receipt_commit_failure_recovers_without_send(harness):
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        outbox._db.execute("CREATE TEMP TRIGGER fail_accept BEFORE UPDATE ON replies WHEN NEW.state='accepted' BEGIN SELECT RAISE(ABORT,'synthetic'); END")
        with pytest.raises(sqlite3.IntegrityError):
            outbox.deliver_one(gateway, "reply-1")
        assert outbox.get("reply-1")["state"] == "submitting"
    with harness.client() as gateway, harness.outbox() as outbox:
        assert outbox.reconcile(gateway, "reply-1")["state"] == "accepted"
    assert len(harness.posts) == 1


@pytest.mark.parametrize("state,error,expected", [
    ("queued", None, "submitting"), ("submitting", None, "submitting"),
    ("failed", "service_rejected", "rejected"), ("failed", "transport_failed", "unknown"),
    ("cancelled", None, "unknown"), ("needs_review", "submission_outcome_unknown", "unknown"),
])
def test_only_confirmed_service_rejection_is_rejected(harness, state, error, expected):
    harness.send_state, harness.send_error = state, error
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        assert outbox.deliver_one(gateway, "reply-1")["state"] == expected
        assert outbox.pending() == []


def test_stale_queued_status_is_unknown_and_native_reason_is_retained(harness):
    harness.send_state = "queued"
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        assert outbox.deliver_one(gateway, "reply-1")["state"] == "submitting"
        harness.hide_status = True
        assert outbox.reconcile(gateway, "reply-1")["state"] == "unknown"
        harness.hide_status = False
        harness.intents["reply-1"].update(state="needs_review", errorCode="quote_receipt_unverified")
        result = outbox.reconcile(gateway, "reply-1")
        assert result["state"] == "unknown"
        assert result["error_code"] == "quote_receipt_unverified"
    assert len(harness.posts) == 1


def test_changed_send_identity_is_retained_for_review_not_associated_with_reply(harness):
    harness.send_state = "queued"
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        original = outbox.deliver_one(gateway, "reply-1")
        harness.intents["reply-1"].update(state="accepted", clientId="999", serverId="9007199254740999")
        result = outbox.reconcile(gateway, "reply-1")
        assert result["state"] == "unknown"
        assert result["error_code"] == "outbox_send_identity_mismatch"
        assert result["intent"] == original["intent"]
        calls = len(harness.requests)
        reviewed = outbox.reconcile(gateway, "reply-1")
        assert reviewed["state"] == "unknown"
        assert reviewed["error_code"] == result["error_code"]
        assert reviewed["intent"] == result["intent"]
        assert len(harness.requests) == calls


def test_conflict_with_preexisting_client_key_cannot_claim_an_unrelated_receipt(harness):
    with harness.client() as gateway, harness.outbox() as outbox:
        gateway.require_account(OWN)
        original = source()
        gateway.send_received_quote(thread_id=THREAD, received_id=original["receivedId"], server_id=MAX_ID,
            source_sha256=original["sourceSha256"], text="different existing reply", idempotency_key="reply-1")
        outbox.freeze(**frozen())
        result = outbox.deliver_one(gateway, "reply-1")
        assert result["state"] == "unknown"
        assert result["error_code"] == "gateway_idempotency_conflict"
        assert result["intent"] is None
        reviewed = outbox.reconcile(gateway, "reply-1")
        assert reviewed["state"] == "unknown"
        assert reviewed["error_code"] == result["error_code"]
        assert reviewed["intent"] is None
    assert len(harness.posts) == 1


def test_network_call_does_not_hold_outbox_lock_or_duplicate_same_reply(harness):
    entered, release = threading.Event(), threading.Event()
    def block_first(request):
        if request.headers["Idempotency-Key"] == "reply-1":
            entered.set()
            assert release.wait(5)
    harness.on_post = block_first
    with harness.client() as gateway, harness.outbox() as outbox, ThreadPoolExecutor(2) as pool:
        outbox.freeze(**frozen())
        first = pool.submit(outbox.deliver_one, gateway, "reply-1")
        try:
            assert entered.wait(2)
            assert outbox.deliver_one(gateway, "reply-1")["state"] == "submitting"
            assert outbox.awaiting_status() == []  # the pending POST is already in flight
            outbox.freeze(**frozen(2))
            second = pool.submit(outbox.deliver_one, gateway, "reply-2")
            assert second.result(timeout=2)["state"] == "accepted"
        finally:
            release.set()
        assert first.result(timeout=2)["state"] == "accepted"
    assert len(harness.posts) == 2


def test_slow_identity_preflight_cannot_occupy_pending_queue_head(harness, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    with harness.client() as gateway, harness.outbox() as outbox, ThreadPoolExecutor(2) as pool:
        require = gateway.require_account
        def block_first_identity(own):
            if not entered.is_set():
                entered.set()
                assert release.wait(5)
            require(own)
        monkeypatch.setattr(gateway, "require_account", block_first_identity)
        outbox.freeze(**frozen())
        first = pool.submit(outbox.deliver_one, gateway, "reply-1")
        try:
            assert entered.wait(2)
            outbox.freeze(**frozen(2))
            assert [r["reply_id"] for r in outbox.pending(limit=1)] == ["reply-2"]
            second = pool.submit(outbox.deliver_one, gateway, "reply-2")
            assert second.result(timeout=2)["state"] == "accepted"
        finally:
            release.set()
        assert first.result(timeout=2)["state"] == "accepted"
    assert len(harness.posts) == 2


def test_exclusive_owner_and_real_account_binding(harness):
    with harness.outbox() as outbox:
        outbox.freeze(**frozen())
        with pytest.raises(OutboxError, match="outbox_already_open"):
            harness.outbox()
        harness.own_username = "replacement-bot"
        with harness.client() as gateway, pytest.raises(GatewayError, match="gateway_expected_account_mismatch"):
            outbox.deliver_one(gateway, "reply-1")
        assert outbox.get("reply-1")["state"] == "queued"
    with pytest.raises(OutboxError, match="outbox_account_changed"):
        ProtocolOutbox(harness.root / "outbox.sqlite", account_id=ACCOUNT, own_username="replacement-bot")
    assert harness.posts == []


def test_symlink_companion_is_not_followed(harness):
    victim = harness.root / "do-not-change"
    victim.write_text("preserve")
    (harness.root / "outbox.sqlite-wal").symlink_to(victim)
    with pytest.raises(OutboxError, match="unsafe_outbox_file"):
        harness.outbox()
    assert victim.read_text() == "preserve"


def test_source_blocked_before_projection_freezes_business_result_without_queuing(harness):
    with harness.client() as gateway, harness.outbox() as outbox:
        # Business already froze this answer before the inbox observed recall.
        committed_business = frozen()
        result = outbox.freeze(**committed_business, blocked_reason="message_recalled")
        assert result["state"] == "rejected"
        assert result["source_blocked"] == "message_recalled"
        assert result["error_code"] == "outbox_source_blocked"
        assert result["submitted_at"] is None
        for field in ("source", "text", "evidence"):
            assert result[field] == committed_business[field]
        assert outbox.pending() == []
        assert outbox.deliver_one(gateway, "reply-1") == result
        assert outbox.reconcile(gateway, "reply-1") == result
        assert outbox.freeze(**committed_business) == result
    assert harness.requests == []


def test_queued_configuration_block_preserves_facts_without_submitting(harness):
    with harness.client() as gateway, harness.outbox() as outbox:
        original = outbox.freeze(**frozen())
        blocked = outbox.block("reply-1", "configuration_changed")
        assert blocked["state"] == "rejected"
        assert blocked["source_blocked"] == "configuration_changed"
        assert blocked["submitted_at"] is None
        for field in ("source", "text", "evidence"):
            assert blocked[field] == original[field]
        assert outbox.pending() == []
        assert outbox.deliver_one(gateway, "reply-1") == blocked
    assert harness.requests == []


@pytest.mark.parametrize("via_freeze", [False, True])
def test_existing_queued_reply_is_atomically_blocked(harness, via_freeze):
    with harness.client() as gateway, harness.outbox() as outbox:
        frozen_reply = outbox.freeze(**frozen())
        blocked = (outbox.freeze(**frozen(), blocked_reason="source_quarantined") if via_freeze
                   else outbox.block("reply-1", "source_quarantined"))
        assert blocked["state"] == "rejected"
        assert blocked["source_blocked"] == "source_quarantined"
        assert blocked["submitted_at"] is None
        assert blocked["source"] == frozen_reply["source"]
        outbox.deliver_one(gateway, "reply-1")
        outbox.freeze(**frozen(2))
        assert [r["reply_id"] for r in outbox.pending()] == ["reply-2"]
        assert outbox.deliver_one(gateway, "reply-2")["state"] == "accepted"
    assert len(harness.posts) == 1
    assert harness.posts[0].headers["Idempotency-Key"] == "reply-2"


def test_blocked_freeze_inserts_rejected_directly_and_rolls_back_together(harness):
    with harness.outbox() as outbox:
        outbox._db.execute("""CREATE TEMP TRIGGER forbid_queued BEFORE INSERT ON replies
            WHEN NEW.state='queued' BEGIN SELECT RAISE(ABORT,'queued window forbidden'); END""")
        assert outbox.freeze(**frozen(), blocked_reason="source_quarantined")["state"] == "rejected"
        outbox._db.execute("DROP TRIGGER forbid_queued")
        outbox.freeze(**frozen(2))
        outbox._db.execute("""CREATE TEMP TRIGGER fail_block BEFORE UPDATE ON replies
            WHEN NEW.source_blocked IS NOT NULL BEGIN SELECT RAISE(ABORT,'synthetic'); END""")
        with pytest.raises(sqlite3.IntegrityError):
            outbox.freeze(**frozen(2), blocked_reason="message_recalled")
        row = outbox.get("reply-2")
        assert row["state"] == "queued"
        assert row["source_blocked"] is None
        assert row["evidence"] == frozen(2)["evidence"]


@pytest.mark.parametrize("change", [{"text": "changed answer"}, {"evidence": {"changed": True}},
                                     {"source": source(body="changed original")}])
def test_blocked_freeze_does_not_authorize_rewriting_frozen_content(harness, change):
    with harness.outbox() as outbox:
        original = outbox.freeze(**frozen())
        with pytest.raises(OutboxError, match="outbox_freeze_conflict"):
            outbox.freeze(**frozen(**change), blocked_reason="message_recalled")
        assert outbox.get("reply-1") == original


@pytest.mark.parametrize("reason", ["", "invalid source with private content", "service_rejected", False, 1])
def test_block_reason_is_a_controlled_source_code(harness, reason):
    with harness.outbox() as outbox:
        original = outbox.freeze(**frozen())
        with pytest.raises(OutboxError, match="invalid_outbox_block_reason"):
            outbox.block("reply-1", reason)
        with pytest.raises(OutboxError, match="invalid_outbox_block_reason"):
            outbox.freeze(**frozen(), blocked_reason=reason)
        assert outbox.get("reply-1") == original


def test_accepted_receipt_is_not_reversed_by_source_block_or_restart(harness):
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        accepted = outbox.deliver_one(gateway, "reply-1")
        blocked = outbox.freeze(**frozen(), blocked_reason="message_recalled")
        assert blocked["state"] == "accepted"
        assert blocked["source_blocked"] == "message_recalled"
        assert blocked["result"] == accepted["result"]
        assert blocked["error_code"] == accepted["error_code"]
        assert blocked["submitted_at"] == accepted["submitted_at"]
    calls = len(harness.requests)
    with harness.client() as gateway, harness.outbox() as outbox:
        assert outbox.reconcile(gateway, "reply-1") == blocked
        assert outbox.deliver_one(gateway, "reply-1") == blocked
    assert len(harness.requests) == calls
    assert len(harness.posts) == 1


@pytest.mark.parametrize("unknown", [False, True])
def test_attempted_block_preserves_send_state_and_only_queries_status(harness, unknown):
    harness.send_state = "queued"
    if unknown:
        harness.on_post, harness.hide_status = lose_response, True
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())
        attempted = outbox.deliver_one(gateway, "reply-1")
        assert attempted["state"] == ("unknown" if unknown else "submitting")
        blocked = outbox.block("reply-1", "native_message_identity_conflict")
        assert blocked["state"] == attempted["state"]
        assert blocked["error_code"] == attempted["error_code"]
        assert blocked["source_blocked"] == "native_message_identity_conflict"
        outbox.deliver_one(gateway, "reply-1")
        outbox.reconcile(gateway, "reply-1")
        assert outbox.pending() == []
    assert len(harness.posts) == 1


@pytest.mark.parametrize("via_freeze", [False, True])
def test_block_during_network_preflight_prevents_stale_row_submission(harness, monkeypatch, via_freeze):
    entered, release = threading.Event(), threading.Event()
    with harness.client() as gateway, harness.outbox() as outbox, ThreadPoolExecutor(1) as pool:
        require = gateway.require_account
        def preflight(own):
            entered.set()
            assert release.wait(5)
            require(own)
        monkeypatch.setattr(gateway, "require_account", preflight)
        outbox.freeze(**frozen())
        future = pool.submit(outbox.deliver_one, gateway, "reply-1")
        try:
            assert entered.wait(2)
            blocked = (outbox.freeze(**frozen(), blocked_reason="message_recalled") if via_freeze
                       else outbox.block("reply-1", "message_recalled"))
            assert blocked["state"] == "rejected"
        finally:
            release.set()
        assert future.result(timeout=2)["state"] == "rejected"
        assert outbox.get("reply-1")["submitted_at"] is None
    assert harness.posts == []


def test_source_block_during_inflight_post_retains_actual_acceptance(harness):
    entered, release = threading.Event(), threading.Event()
    def in_post(_request):
        entered.set()
        assert release.wait(5)
    harness.on_post = in_post
    with harness.client() as gateway, harness.outbox() as outbox, ThreadPoolExecutor(1) as pool:
        outbox.freeze(**frozen())
        future = pool.submit(outbox.deliver_one, gateway, "reply-1")
        try:
            assert entered.wait(2)
            assert outbox.block("reply-1", "message_recalled")["state"] == "submitting"
        finally:
            release.set()
        result = future.result(timeout=2)
        assert result["state"] == "accepted"
        assert result["source_blocked"] == "message_recalled"
    assert len(harness.posts) == 1


def test_prior_outbox_schema_is_migrated_without_losing_frozen_business_result(harness):
    with harness.outbox() as outbox:
        original = outbox.freeze(**frozen())
    with sqlite3.connect(harness.root / "outbox.sqlite") as db:
        db.execute("ALTER TABLE replies DROP COLUMN source_blocked")
    with harness.outbox() as outbox:
        assert outbox.get("reply-1") == original
        blocked = outbox.block("reply-1", "source_quarantined")
        assert blocked["state"] == "rejected"
        assert blocked["evidence"] == original["evidence"]
        assert blocked["source"] == original["source"]


def test_status_queue_contains_only_unresolved_attempts_and_rotates_unknown(harness):
    with harness.client() as gateway, harness.outbox() as outbox:
        outbox.freeze(**frozen())  # remains queued
        for index, state, error in [(2, "queued", None), (3, "needs_review", "submission_outcome_unknown"),
                                     (4, "accepted", None), (5, "failed", "service_rejected")]:
            harness.send_state, harness.send_error = state, error
            outbox.freeze(**frozen(index))
            outbox.deliver_one(gateway, f"reply-{index}")
        assert [r["reply_id"] for r in outbox.pending()] == ["reply-1"]
        assert [r["reply_id"] for r in outbox.awaiting_status()] == ["reply-2", "reply-3"]
        assert outbox.reconcile(gateway, "reply-2")["state"] == "submitting"
        assert [r["reply_id"] for r in outbox.awaiting_status(limit=1)] == ["reply-3"]
        before = outbox.get("reply-3")["updated_at"]
        assert outbox.reconcile(gateway, "reply-3")["state"] == "unknown"
        assert outbox.get("reply-3")["updated_at"] > before
        assert [r["reply_id"] for r in outbox.awaiting_status(limit=1)] == ["reply-2"]
    assert len(harness.posts) == 4


def test_status_lookup_failure_still_rotates_without_blocking_new_submissions(harness):
    harness.on_post, harness.hide_status = lose_response, True
    with harness.client() as gateway, harness.outbox() as outbox:
        for index in (1, 2):
            outbox.freeze(**frozen(index))
            assert outbox.deliver_one(gateway, f"reply-{index}")["state"] == "unknown"
        outbox.freeze(**frozen(3))
        assert [r["reply_id"] for r in outbox.awaiting_status(limit=1)] == ["reply-1"]
        assert outbox.reconcile(gateway, "reply-1")["state"] == "unknown"
        assert [r["reply_id"] for r in outbox.awaiting_status(limit=1)] == ["reply-2"]
        assert [r["reply_id"] for r in outbox.pending()] == ["reply-3"]
    assert len(harness.posts) == 2


def test_manual_identity_conflict_rotates_without_another_gateway_query(harness):
    with harness.client() as gateway, harness.outbox() as outbox:
        original = source()
        gateway.send_received_quote(thread_id=THREAD, received_id=original["receivedId"], server_id=MAX_ID,
            source_sha256=original["sourceSha256"], text="preexisting other body", idempotency_key="reply-1")
        outbox.freeze(**frozen())
        assert outbox.deliver_one(gateway, "reply-1")["error_code"] == "gateway_idempotency_conflict"
        harness.send_state = "queued"
        outbox.freeze(**frozen(2))
        outbox.deliver_one(gateway, "reply-2")
        assert [r["reply_id"] for r in outbox.awaiting_status(limit=1)] == ["reply-1"]
        calls = len(harness.requests)
        assert outbox.reconcile(gateway, "reply-1")["state"] == "unknown"
        assert len(harness.requests) == calls
        assert [r["reply_id"] for r in outbox.awaiting_status(limit=1)] == ["reply-2"]


@pytest.mark.parametrize("limit", [True, 0, 1001])
def test_status_queue_limit_is_bounded(harness, limit):
    with harness.outbox() as outbox, pytest.raises(OutboxError, match="invalid_outbox_limit"):
        outbox.awaiting_status(limit=limit)


@pytest.mark.parametrize("manual_conflict", [False, True])
def test_more_than_one_status_page_cannot_starve_behind_permanent_unknowns(harness, monkeypatch, manual_conflict):
    ticks = count()
    epoch = datetime(2026, 10, 29, tzinfo=timezone.utc)
    monkeypatch.setattr("chatlog_keeper.bot.protocol_outbox._now",
        lambda: (epoch + timedelta(microseconds=next(ticks))).isoformat(timespec="microseconds"))
    harness.send_state, harness.send_error = "needs_review", "submission_outcome_unknown"
    with harness.client() as gateway, harness.outbox() as outbox:
        for index in range(1, 102):
            item = frozen(index)
            if manual_conflict:
                message = item["source"]
                gateway.send_received_quote(thread_id=THREAD, received_id=message["receivedId"],
                    server_id=message["serverId"], source_sha256=message["sourceSha256"],
                    text="preexisting different answer", idempotency_key=item["reply_id"])
            outbox.freeze(**item)
            assert outbox.deliver_one(gateway, item["reply_id"])["state"] == "unknown"
        first_page = outbox.awaiting_status(limit=100)
        assert [row["reply_id"] for row in first_page] == [f"reply-{n}" for n in range(1, 101)]
        posts = len(harness.posts)
        for row in first_page:
            assert outbox.reconcile(gateway, row["reply_id"])["state"] == "unknown"
        assert outbox.awaiting_status(limit=1)[0]["reply_id"] == "reply-101"
        assert len(harness.posts) == posts
        assert outbox.pending() == []
