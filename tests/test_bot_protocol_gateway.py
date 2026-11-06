"""Protocol transport contract checks; every HTTP request stays in MockTransport.

Fixtures follow agent-wechat wechat-api contracts.rs, lib.rs, media.rs and
actions.rs. These checks establish adapter behavior, not native/phone delivery.
"""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import sqlite3

import httpx
import pytest

from chatlog_keeper.bot.protocol_gateway import GatewayError, ProtocolGateway


TOKEN = "synthetic-gateway-token-not-a-real-credential"
ACCOUNT = "fixture-account"
THREAD = "fixture-group@chatroom"
MESSAGE = "opaque-source-qualified-message"
SERVER_ID = "18446744073709551615"
KEY = "fixture-reply-1"
NOW = "2026-10-29T12:00:00Z"
SEND = dict(thread_id=THREAD, message_id=MESSAGE, server_id=SERVER_ID,
            text="这是本次固定回复。", idempotency_key=KEY)
PAYLOAD = {"kind": "text", "threadId": THREAD, "text": SEND["text"],
           "quote": {"messageId": MESSAGE, "serverId": SERVER_ID}}


def native_intent(state="queued", **changes):
    # Actual NativeSendIntent fields; there is no kind/threadId/success flag.
    value = {"id": "fixture-send-id", "accountId": ACCOUNT, "destination": THREAD,
             "state": state, "clientId": "12345", "serverId": None,
             "errorCode": None, "createdAt": NOW}
    if state == "accepted":
        value.update(serverId="9007199254740999", localMessageId="opaque-outgoing-message")
    return {**value, **changes}


def native_message(**changes):
    return {"id": MESSAGE, "accountId": ACCOUNT, "threadId": THREAD,
            "serverId": SERVER_ID, "senderId": "fixture-sender", "type": 1,
            "direction": "incoming", "body": "相同正文", "createdAt": NOW,
            "mentions": {"state": "known", "userIds": ["fixture-bot"], "self": True,
                         "all": False, "source": "msgsource"}, "version": "fixture-v1",
            "media": [], **changes}


def consumer_state(consumer="fixture-consumer", *, acknowledged="0", delivered="0"):
    return {"consumerId": consumer, "required": True, "registeredAfter": "0",
            "acknowledgedCursor": acknowledged, "deliveredCursor": delivered}


class Harness:
    def __init__(self, path):
        self.path = path
        self.requests = []
        self.intent = native_intent()
        self.message = native_message()
        self.post_failure = None
        self.lookup_failure = None
        self.response_status = 202
        self.custom = None
        self.server_id = "fixture-gateway-registry-id"
        self.ownership_generation = "fixture-runtime-epoch"
        self.own_username = "fixture-bot"

    def client(self, **kwargs):
        return ProtocolGateway(token=TOKEN, account_id=ACCOUNT, journal_path=self.path,
                               transport=httpx.MockTransport(self.handle), **kwargs)

    def handle(self, request):
        assert request.url.host == "127.0.0.1"
        assert request.headers["authorization"] == "Bearer " + TOKEN
        self.requests.append(request)
        if request.url.path == "/v1/status":
            return httpx.Response(200, json={"serverId": self.server_id,
                "ownershipGeneration": self.ownership_generation, "state": "ready"})
        if request.url.path == "/v1/accounts/" + ACCOUNT:
            return httpx.Response(200, json={"id": ACCOUNT, "ownUsername": self.own_username,
                                           "authenticated": True})
        if self.custom:
            response = self.custom(request)
            if response is not None:
                return response
        route = request.url.path.removeprefix("/v1/accounts/" + ACCOUNT)
        if request.method == "POST" and route == "/sends":
            assert request.headers["Idempotency-Key"] == KEY
            if self.post_failure:
                return self.post_failure(request)
            return httpx.Response(self.response_status, json=self.intent)
        if request.method == "GET" and route.startswith("/sends/"):
            if route == "/sends/by-idempotency-key":
                assert request.url.params["key"] == KEY
            else:
                assert route == "/sends/fixture-send-id"
            if self.lookup_failure:
                return self.lookup_failure(request)
            return httpx.Response(200, json=self.intent)
        if request.method == "GET" and route.startswith("/messages/"):
            return httpx.Response(200, json=self.message)
        if request.method == "GET" and route == "/capabilities":
            return httpx.Response(200, json={"nativeRuntime": True, "officialBinaryRequired": False,
                "quotedTextSend": True, "quotedTextSendScopes": ["read", "send"],
                "quotedTextSendLiveValidated": False})
        raise AssertionError((request.method, route))

    @property
    def sends(self):
        return [request for request in self.requests if request.method == "POST" and request.url.path.endswith("/sends")]


@pytest.fixture
def harness(tmp_path):
    return Harness(tmp_path / "gateway" / "transport.sqlite")


def lost_response(request):
    raise httpx.ReadTimeout("private transport detail must not escape", request=request)


def missing_intent(_request):
    return httpx.Response(404, json={"error": {"code": "not_found"}})


def test_authentication_and_capabilities_do_not_upgrade_live_validation(harness):
    with harness.client() as client:
        capabilities = client.capabilities()
    assert capabilities["quotedTextSend"] is True
    assert capabilities["quotedTextSendLiveValidated"] is False
    assert harness.requests[0].url.path == f"/v1/accounts/{ACCOUNT}/capabilities"
    assert harness.path.stat().st_mode & 0o777 == 0o600
    assert TOKEN.encode() not in harness.path.read_bytes()


def test_expected_real_account_is_checked_before_any_send(harness):
    with harness.client() as client:
        with pytest.raises(GatewayError, match="gateway_expected_account_mismatch"):
            client.require_account("different-real-account")
        with pytest.raises(GatewayError, match="gateway_expected_account_mismatch"):
            client.send_quote(**SEND)
    assert harness.sends == []


def test_account_cannot_change_between_probe_preflight_and_send(harness):
    with harness.client() as client:
        client.require_account(harness.own_username)
        harness.own_username = "replacement-account"
        with pytest.raises(GatewayError, match="gateway_expected_account_mismatch"):
            client.send_quote(**SEND)
    assert harness.sends == []


@pytest.mark.parametrize("url", ["https://example.invalid", "http://localhost:6174", "http://0.0.0.0:6174",
    "http://127.0.0.1@evil.invalid", "http://user:secret@127.0.0.1:6174", "ftp://127.0.0.1",
    "http://127.0.0.1:6174/v1/other", "http://127.0.0.1:6174?token=secret", "http://127.0.0.1/#secret"])
def test_refuses_non_loopback_or_credential_urls(harness, url):
    with pytest.raises(GatewayError, match="gateway_requires_loopback_url"):
        harness.client(base_url=url)
    assert not harness.requests


def test_redirect_does_not_forward_bearer_or_change_destination(harness):
    harness.custom = lambda request: httpx.Response(302, headers={"location": "https://example.invalid"}, json={})
    with harness.client() as client:
        with pytest.raises(GatewayError) as error:
            client.capabilities()
    assert error.value.http_status == 302
    assert len(harness.requests) == 1
    assert TOKEN not in str(error.value)


def test_source_identity_uses_opaque_ids_and_preserves_large_unsigned_server_id(harness):
    with harness.client() as client:
        message = client.get_message(MESSAGE, thread_id=THREAD, server_id=SERVER_ID)
    assert message["serverId"] == SERVER_ID
    assert isinstance(message["serverId"], str)
    assert harness.requests[0].url.path.endswith("/messages/" + MESSAGE)


def received_message(**changes):
    return {"receivedId": "recv-fixture", "firstReceivedSequence": "19", "accountId": ACCOUNT,
            "ownUsername": "fixture-bot", "sourceSha256": "a" * 64, "disposition": "ready",
            "reason": None, "threadId": THREAD, "serverId": SERVER_ID, "senderId": "fixture-peer",
            "direction": "incoming", "type": 1, "appMessageType": None, "createdAt": NOW,
            "body": "原始协议消息", "mentions": {"state": "known", "userIds": ["fixture-bot"],
            "self": True, "all": False, "source": "msgsource"}, "quote": None,
            "quoteEligible": True, **changes}


def test_native_snapshot_works_without_any_profile_read(harness):
    harness.custom = lambda request: httpx.Response(200, json=received_message())
    with harness.client() as client:
        message = client.get_received_message("recv-fixture", thread_id=THREAD,
                                              server_id=SERVER_ID, source_sha256="a" * 64)
    assert message["serverId"] == SERVER_ID
    assert harness.requests[-1].url.path.endswith("/received/messages/recv-fixture")
    assert not any("/messages/" in str(r.url) and "/received/" not in str(r.url) for r in harness.requests)


@pytest.mark.parametrize("changes", [
    {"receivedId": "other"}, {"accountId": "other"}, {"ownUsername": "other"},
    {"serverId": 18446744073709551615}, {"serverId": None}, {"senderId": None},
    {"sourceSha256": "invalid"}, {"firstReceivedSequence": "0"},
    {"disposition": "quarantined", "quoteEligible": True}, {"body": None},
])
def test_native_snapshot_rejects_partial_or_changed_identity(harness, changes):
    harness.custom = lambda request: httpx.Response(200, json=received_message(**changes))
    with harness.client() as client, pytest.raises(GatewayError):
        client.get_received_message("recv-fixture")
    assert not harness.sends


def test_quarantined_native_record_is_retained_as_unavailable(harness):
    harness.custom = lambda request: httpx.Response(200, json=received_message(
        disposition="quarantined", reason="sender_unresolved", quoteEligible=False,
        senderId=None, body=None))
    with harness.client() as client:
        result = client.get_received_message("recv-fixture")
    assert result["disposition"] == "quarantined"
    assert result["senderId"] is None
    assert not harness.sends


def test_native_send_freezes_snapshot_hash_and_replays_only_same_intent(harness):
    send = {key: value for key, value in SEND.items() if key != "message_id"}
    send.update(received_id="recv-fixture", source_sha256="a" * 64)
    with harness.client() as client:
        first = client.send_received_quote(**send)
    with harness.client() as client:
        again = client.send_received_quote(**send)
        with pytest.raises(GatewayError, match="idempotency_conflict"):
            client.send_received_quote(**{**send, "source_sha256": "b" * 64})
    assert first.intent["id"] == again.intent["id"]
    assert len(harness.sends) == 1
    assert json.loads(harness.sends[0].content)["quote"] == {
        "receivedId": "recv-fixture", "sourceSha256": "a" * 64, "serverId": SERVER_ID}


@pytest.mark.parametrize("changes", [{"id": "other"}, {"accountId": "other"}, {"threadId": "other@chatroom"},
    {"serverId": "18446744073709551614"}, {"serverId": 9007199254740999}, {"senderId": 42},
    {"quote": {"serverId": 42}}, {"quote": {"serverId": "42", "senderId": 42}}])
def test_message_readback_rejects_mismatched_or_numeric_identity(harness, changes):
    harness.message.update(changes)
    with harness.client() as client, pytest.raises(GatewayError):
        client.get_message(MESSAGE, thread_id=THREAD, server_id=SERVER_ID)


def test_first_submission_matches_real_quote_contract_then_only_inspects(harness):
    with harness.client() as client:
        queued = client.send_quote(**SEND)
        assert queued.outcome == "queued"
        assert queued.intent == native_intent()
        harness.intent = native_intent("accepted")
        accepted = client.send_quote(**SEND)
    assert json.loads(harness.sends[0].content) == PAYLOAD
    assert len(harness.sends) == 1
    assert harness.requests[-1].method == "GET"
    assert accepted.outcome == "accepted"
    assert accepted.intent["serverId"] == "9007199254740999"
    assert accepted.phone_received is False
    assert accepted.retryable is False


@pytest.mark.parametrize("field,value", [("server_id", 42), ("server_id", "042"), ("server_id", "0"),
    ("server_id", "18446744073709551616"), ("message_id", 42), ("thread_id", 42),
    ("idempotency_key", "bad\nkey"), ("text", ""), ("text", "中" * 22000)])
def test_invalid_send_inputs_never_create_a_request(harness, field, value):
    with harness.client() as client, pytest.raises(GatewayError):
        client.send_quote(**{**SEND, field: value})
    assert not harness.requests


@pytest.mark.parametrize("field,value", [("text", "changed"), ("message_id", "other"),
    ("server_id", "42"), ("thread_id", "other@chatroom")])
def test_key_payload_conflicts_are_rejected_across_reopen(harness, field, value):
    with harness.client() as client:
        client.send_quote(**SEND)
    with harness.client() as client, pytest.raises(GatewayError, match="gateway_idempotency_conflict"):
        client.send_quote(**{**SEND, field: value})
    assert len(harness.sends) == 1


def test_lost_post_response_recovers_same_key_without_second_post(harness):
    harness.post_failure = lost_response
    harness.intent = native_intent("accepted")
    with harness.client() as client:
        result = client.send_quote(**SEND)
    assert result.outcome == "accepted"
    assert result.phone_received is False
    assert len(harness.sends) == 1
    assert harness.requests[-1].url.path.endswith("/sends/by-idempotency-key")
    assert harness.requests[-1].url.params["key"] == KEY
    assert TOKEN.encode() not in harness.path.read_bytes()


def test_unknown_lookup_remains_unknown_after_restart_and_never_posts_again(harness):
    harness.post_failure = lost_response
    harness.lookup_failure = missing_intent
    with harness.client() as client:
        first = client.send_quote(**SEND)
    assert first.outcome == "unknown"
    assert first.error_code == "not_found"
    assert first.http_status == 404
    with harness.client() as client:
        second = client.send_quote(**SEND)
        polled = client.get_send_status(KEY)
    assert second.outcome == polled.outcome == "unknown"
    assert second.retryable is False
    assert len(harness.sends) == 1
    assert all(request.url.params["key"] == KEY for request in harness.requests
               if request.url.path.endswith("/sends/by-idempotency-key"))


def test_crash_after_local_claim_before_post_is_conservatively_retained(harness, monkeypatch):
    with harness.client() as client:
        original = client._request
        def stopped(method, path, **kwargs):
            if method == "POST":
                raise KeyboardInterrupt()
            return original(method, path, **kwargs)
        monkeypatch.setattr(client, "_request", stopped)
        with pytest.raises(KeyboardInterrupt):
            client.send_quote(**SEND)
    harness.lookup_failure = missing_intent
    with harness.client() as client:
        result = client.send_quote(**SEND)
    assert result.outcome == "unknown"
    assert not harness.sends


def test_explicit_recovery_after_client_crash_reuses_claim_in_same_daemon_epoch(harness, monkeypatch):
    with harness.client() as client:
        original = client._request
        def stopped(method, path, **kwargs):
            if method == "POST":
                raise KeyboardInterrupt()
            return original(method, path, **kwargs)
        monkeypatch.setattr(client, "_request", stopped)
        with pytest.raises(KeyboardInterrupt):
            client.send_quote(**SEND)
    harness.lookup_failure = missing_intent
    harness.intent = native_intent("accepted")
    with harness.client() as client:
        result = client.send_quote(**SEND, recover_missing_intent=True)
    assert result.outcome == "accepted"
    assert len(harness.sends) == 1
    assert json.loads(harness.sends[0].content) == PAYLOAD
    assert harness.sends[0].headers["Idempotency-Key"] == KEY


def test_lost_ack_then_404_replays_same_key_into_one_remote_intent(harness):
    # The first POST was actually accepted, but its HTTP response was lost and
    # a subsequent key lookup transiently returned 404. Same-key replay must
    # use the daemon's existing intent instead of creating a second message.
    remote = {}
    def accept(request):
        key = request.headers["Idempotency-Key"]
        remote.setdefault(key, native_intent("accepted"))
        if len(harness.sends) == 1:
            return lost_response(request)
        return httpx.Response(202, json=remote[key])
    harness.post_failure = accept
    harness.lookup_failure = missing_intent
    with harness.client() as client:
        assert client.send_quote(**SEND).outcome == "unknown"
        result = client.send_quote(**SEND, recover_missing_intent=True)
    assert result.outcome == "accepted"
    assert len(harness.sends) == 2
    assert len(remote) == 1
    assert all(json.loads(request.content) == PAYLOAD for request in harness.sends)
    assert len({request.content for request in harness.sends}) == 1
    assert {request.headers["Idempotency-Key"] for request in harness.sends} == {KEY}


def test_explicit_recovery_never_loops_after_its_post_response_is_lost(harness):
    harness.post_failure = lost_response
    harness.lookup_failure = missing_intent
    with harness.client() as client:
        client.send_quote(**SEND)
        before = len(harness.sends)
        result = client.send_quote(**SEND, recover_missing_intent=True)
    assert result.outcome == "unknown"
    assert len(harness.sends) == before + 1


@pytest.mark.parametrize("lookup", [lost_response,
    lambda request: httpx.Response(500, json={"error": {"code": "not_found"}}),
    lambda request: httpx.Response(404, json={"error": {"code": "other_route_missing"}}),
    lambda request: httpx.Response(404, text="not_found"),
])
def test_recovery_requires_exact_successful_lookup_not_found_not_transport_error(harness, lookup):
    harness.post_failure = lost_response
    harness.lookup_failure = lookup
    with harness.client() as client:
        client.send_quote(**SEND)
        result = client.send_quote(**SEND, recover_missing_intent=True)
    assert result.outcome == "unknown"
    assert len(harness.sends) == 1


@pytest.mark.parametrize("field,value,code", [
    ("server_id", "replacement-registry", "gateway_server_identity_changed"),
    ("own_username", "replacement-bot", "gateway_account_identity_changed"),
])
def test_recovery_refuses_a_replaced_gateway_or_account(harness, field, value, code):
    harness.post_failure = lost_response
    harness.lookup_failure = missing_intent
    with harness.client() as client:
        client.send_quote(**SEND)
    setattr(harness, field, value)
    with harness.client() as client, pytest.raises(GatewayError, match=code):
        client.send_quote(**SEND, recover_missing_intent=True)
    assert len(harness.sends) == 1


def test_same_registry_uuid_after_restore_cannot_authorize_missing_intent_replay(harness):
    harness.post_failure = lost_response
    harness.lookup_failure = missing_intent
    with harness.client() as client:
        client.send_quote(**SEND)
    # Restoring an old registry snapshot preserves serverId. A fresh daemon
    # ownershipGeneration must block recovery even with the explicit flag.
    harness.ownership_generation = "replacement-runtime-epoch"
    with harness.client() as client:
        result = client.send_quote(**SEND, recover_missing_intent=True)
    assert result.outcome == "unknown"
    assert result.error_code == "gateway_recovery_epoch_changed"
    assert len(harness.sends) == 1


def test_normal_daemon_restart_can_still_inspect_its_retained_intent(harness):
    with harness.client() as client:
        client.send_quote(**SEND)
    harness.ownership_generation = "replacement-runtime-epoch"
    harness.intent = native_intent("accepted")
    with harness.client() as client:
        result = client.send_quote(**SEND, recover_missing_intent=True)
    assert result.outcome == "accepted"
    assert len(harness.sends) == 1


def test_recovery_rechecks_identity_after_the_404_before_post(harness):
    harness.post_failure = lost_response
    harness.lookup_failure = missing_intent
    with harness.client() as client:
        client.send_quote(**SEND)
        def replace_after_404(request):
            harness.server_id = "replacement-registry"
            return missing_intent(request)
        harness.lookup_failure = replace_after_404
        with pytest.raises(GatewayError, match="gateway_server_identity_changed"):
            client.send_quote(**SEND, recover_missing_intent=True)
    assert len(harness.sends) == 1


def test_losing_a_known_durable_intent_is_not_an_admission_gap(harness):
    with harness.client() as client:
        client.send_quote(**SEND)
        harness.lookup_failure = missing_intent
        result = client.send_quote(**SEND, recover_missing_intent=True)
    assert result.outcome == "unknown"
    assert result.intent["id"] == "fixture-send-id"
    assert len(harness.sends) == 1


def test_old_unbound_claims_cannot_retroactively_assert_gateway_identity(harness):
    with harness.client() as client:
        client.send_quote(**SEND)
    with sqlite3.connect(harness.path) as db:
        db.execute("DELETE FROM gateway_accounts")
        db.execute("DELETE FROM gateway_meta WHERE key='server_id'")
        db.execute("UPDATE send_claims SET ownership_generation=NULL")
    with harness.client() as client, pytest.raises(GatewayError, match="gateway_existing_claims_unbound"):
        client.send_quote(**SEND, recover_missing_intent=True)
    assert len(harness.sends) == 1


def test_two_clients_share_one_durable_submission_claim(harness):
    with harness.client() as first, harness.client() as second:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda client: client.send_quote(**SEND), [first, second]))
    assert all(result.outcome == "queued" for result in results)
    assert len(harness.sends) == 1


@pytest.mark.parametrize("state,outcome", [("accepted", "accepted"), ("queued", "queued"),
    ("submitting", "queued"), ("needs_review", "unknown"), ("new_state", "unknown"),
    ("failed", "failed"), ("cancelled", "failed")])
def test_native_states_and_error_reasons_are_retained_without_retry_permission(harness, state, outcome):
    harness.intent = native_intent(state, errorCode="worker_interrupted" if state == "failed" else None)
    with harness.client() as client:
        result = client.send_quote(**SEND)
    assert result.outcome == outcome
    assert result.intent == harness.intent
    assert result.retryable is result.phone_received is False


@pytest.mark.parametrize("changes", [{"accountId": "other"}, {"destination": "wrong@chatroom"},
    {"serverId": 9007199254740999}, {"serverId": None}, {"clientId": 12345},
    {"kind": "media"}, {"threadId": THREAD}])
def test_bad_send_receipt_cannot_become_accepted(harness, changes):
    harness.intent = native_intent("accepted", **changes)
    with harness.client() as client:
        result = client.send_quote(**SEND)
    assert result.outcome == "unknown"
    assert len(harness.sends) == 1


def test_status_must_keep_same_intent_identity(harness):
    with harness.client() as client:
        client.send_quote(**SEND)
        harness.intent = native_intent("accepted", id="other-send")
        result = client.get_send_status(KEY)
    assert result.outcome == "unknown"
    assert result.error_code == "gateway_send_identity_mismatch"
    assert result.intent["id"] == "fixture-send-id"


def test_known_accepted_receipt_survives_a_late_queued_result_and_outage(harness):
    harness.intent = native_intent("accepted")
    with harness.client() as client:
        accepted = client.send_quote(**SEND)
        harness.intent = native_intent("queued")
        assert client.get_send_status(KEY).intent == accepted.intent
        harness.lookup_failure = lost_response
        result = client.get_send_status(KEY)
    assert result.outcome == "accepted"
    assert result.fresh is False
    assert result.error_code == "gateway_transport_unavailable"
    assert result.phone_received is False


def test_http_rejection_exposes_actual_code_and_keeps_claim(harness):
    harness.post_failure = lambda _request: httpx.Response(409, json={"error": {"code": "idempotency_conflict"}})
    with harness.client() as client:
        with pytest.raises(GatewayError) as error:
            client.send_quote(**SEND)
        assert error.value.code == "idempotency_conflict"
        assert error.value.http_status == 409
        assert error.value.retryable is False
        harness.lookup_failure = missing_intent
        assert client.send_quote(**SEND).outcome == "unknown"
    assert len(harness.sends) == 1


def test_http_500_or_invalid_json_is_inspected_not_resubmitted(harness):
    harness.post_failure = lambda _request: httpx.Response(500, text="untrusted error body " + TOKEN)
    harness.lookup_failure = missing_intent
    with harness.client() as client:
        result = client.send_quote(**SEND)
    assert result.outcome == "unknown"
    assert TOKEN not in repr(result)
    assert len(harness.sends) == 1


def test_retained_journal_is_not_silently_reused_for_another_daemon(harness):
    with harness.client():
        pass
    with pytest.raises(GatewayError, match="gateway_journal_endpoint_changed"):
        harness.client(base_url="http://127.0.0.1:6175")


def test_message_path_is_percent_encoded(harness):
    harness.message["id"] = "opaque/id?#fragment"
    with harness.client() as client:
        assert client.get_message("opaque/id?#fragment")["id"] == "opaque/id?#fragment"
    assert b"opaque%2Fid%3F%23fragment" in harness.requests[-1].url.raw_path
    assert not harness.requests[-1].url.query


def test_receive_register_and_ack_follow_consumer_contract_without_implicit_ack(harness):
    state = consumer_state()
    events = [
        {"cursor": "1", "kind": "message.committed", "createdAt": NOW,
         "data": {"messageId": MESSAGE, "threadId": THREAD, "origin": "sync", "direction": "incoming"}},
        {"cursor": "2", "kind": "unknown.future.kind", "createdAt": NOW, "data": {"retained": True}},
    ]

    def consumer_api(request):
        path = request.url.path
        if request.method == "PUT":
            assert json.loads(request.content) == {"after": "0"}
            return httpx.Response(200, json=state)
        if path.endswith("/events"):
            assert "after" not in request.url.params
            state["deliveredCursor"] = "2"
            return httpx.Response(200, json={"items": events, "nextCursor": "2", "consumer": state})
        if path.endswith("/ack"):
            assert json.loads(request.content) == {"cursor": "2"}
            state["acknowledgedCursor"] = "2"
            return httpx.Response(200, json={"cursor": "2", "messageReadStateChanged": False})
        return httpx.Response(200, json=state)

    harness.custom = consumer_api
    with harness.client() as client:
        assert client.register_consumer("fixture-consumer") == consumer_state()
        page = client.receive(consumer="fixture-consumer")
        assert page["items"] == events
        assert page["nextCursor"] == "2"
        assert not any(request.url.path.endswith("/ack") for request in harness.requests)
        # Caller persistence is deliberately outside the HTTP client.
        with sqlite3.connect(harness.path.parent / "isolated-inbox.sqlite") as inbox:
            inbox.execute("CREATE TABLE events(cursor TEXT PRIMARY KEY,payload TEXT)")
            inbox.executemany("INSERT INTO events VALUES(?,?)", [(e["cursor"], json.dumps(e)) for e in events])
        assert client.ack("fixture-consumer", page["nextCursor"])["messageReadStateChanged"] is False
        assert client.consumer_status("fixture-consumer")["acknowledgedCursor"] == "2"


def test_ack_replay_accepts_actual_monotonic_cursor_not_read_state(harness):
    harness.custom = lambda request: httpx.Response(200, json={"cursor": "5", "messageReadStateChanged": False})
    with harness.client() as client:
        assert client.ack("fixture-consumer", "2")["cursor"] == "5"
    assert len(harness.requests) == 1


@pytest.mark.parametrize("cursor,changed", [("1", False), (2, False), ("2", True)])
def test_ack_rejects_regressed_or_numeric_cursor_and_read_state_side_effect(harness, cursor, changed):
    harness.custom = lambda request: httpx.Response(200, json={"cursor": cursor, "messageReadStateChanged": changed})
    with harness.client() as client, pytest.raises(GatewayError):
        client.ack("fixture-consumer", "2")


def test_expired_cursor_is_an_error_not_silent_head_advance(harness):
    harness.custom = lambda request: httpx.Response(410, json={"error": {"code": "event_cursor_expired"}})
    with harness.client() as client, pytest.raises(GatewayError, match="event_cursor_expired"):
        client.receive(consumer="fixture-consumer", after="0")
    assert len(harness.requests) == 1


def test_unregistered_diagnostic_receive_uses_original_events_contract(harness):
    harness.custom = lambda request: httpx.Response(200, json={"items": [], "nextCursor": "3"})
    with harness.client() as client:
        assert client.receive(after="3", limit=20) == {"items": [], "nextCursor": "3"}
    assert harness.requests[-1].url.path.endswith("/events")
    assert dict(harness.requests[-1].url.params) == {"after": "3", "limit": "20"}


@pytest.mark.parametrize("page", [
    {"items": [], "nextCursor": 0},
    {"items": [], "nextCursor": "5"},
    {"items": [{"cursor": "0", "kind": "message.committed", "data": {}}], "nextCursor": "0"},
    {"items": [{"cursor": "1", "kind": "message.committed", "data": {"messageId": 42, "threadId": THREAD}}], "nextCursor": "1"},
])
def test_invalid_event_pages_never_ack_or_advance(harness, page):
    harness.custom = lambda request: httpx.Response(200, json=deepcopy(page))
    with harness.client() as client, pytest.raises(GatewayError):
        client.receive(after="0")
    assert all(request.method == "GET" for request in harness.requests)
