import pytest

from chatlog_keeper.bot.protocol_acceptance import prepare_probe, prepare_native_probe, verify_spec, execute_one, inspect_one, assess_recipient
from chatlog_keeper.bot.protocol_gateway import GatewayError, SendResult


def source(sid="9007199254740993", sender="peer"):
    return {"id": "opaque-" + sid, "accountId": "account", "threadId": "test@chatroom",
            "serverId": sid, "senderId": sender, "body": "重复正文", "direction": "incoming", "type": 1}


def spec():
    return prepare_probe(run_id="fixed-one", account_id="account", own_username="bot",
                         thread_id="test@chatroom", sources=[source(), source("9007199254740994")])


def observation():
    plan = spec()
    return {"evidence_kind": "independent_peer", "receiver_username": "peer",
            "thread_id": plan["thread_id"], "sender_username": "bot",
            "reply_server_id": "18446744073709551615", "body": plan["cases"][0]["reply_text"],
            "quote_server_id": "9007199254740993", "artifact_sha256": "a"*64,
            "complete": True, "quote_navigation_verified": True}


def test_duplicate_body_has_distinct_source_and_send_identity():
    plan = spec()
    verify_spec(plan)
    a, b = plan["cases"]
    assert a["source_body_sha256"] == b["source_body_sha256"]
    assert a["source_server_id"] != b["source_server_id"]
    assert a["idempotency_key"] != b["idempotency_key"]


def test_plan_edits_do_not_silently_change_an_existing_send():
    plan = spec()
    plan["cases"][0]["reply_text"] = "changed"
    with pytest.raises(ValueError, match="probe_spec_changed"):
        verify_spec(plan)


@pytest.mark.parametrize("change", [
    {"evidence_kind": "local_archive"}, {"receiver_username": "bot"},
    {"thread_id": "other@chatroom"}, {"quote_server_id": "9007199254740994"},
    {"body": "partial"}, {"reply_server_id": 9007199254740993},
    {"complete": False}, {"artifact_sha256": ""}, {"sender_username": "other"},
])
def test_independent_oracle_rejects_wrong_identity_partial_and_local_echo(change):
    result = assess_recipient(spec(), "1", {**observation(), **change})
    assert not result["correlation_matched"]
    assert not result["full_acceptance_passed"]


def test_peer_receipt_never_becomes_phone_receipt():
    result = assess_recipient(spec(), "1", observation())
    assert result["correlation_matched"]
    assert not result["peer_received"] and not result["full_acceptance_passed"]
    assert not result["phone_full_received"]


def test_navigation_is_separate_from_received_quote():
    result = assess_recipient(spec(), "1", {**observation(), "quote_navigation_verified": False})
    assert result["correlation_matched"]
    assert not result["full_acceptance_passed"]


def test_an_invented_artifact_hash_cannot_pass_acceptance():
    result = assess_recipient(spec(), "1", observation())
    assert not result["evidence_artifact_verified"]
    assert not result["quote_navigation_verified"]
    assert not result["full_acceptance_passed"]


class Gateway:
    account_id = "account"

    def __init__(self):
        self.sent = []
        self.message = source()

    def get_message(self, message_id, *, thread_id, server_id):
        assert message_id == self.message["id"]
        assert thread_id == self.message["threadId"]
        assert server_id == self.message["serverId"]
        return self.message

    def require_account(self, own_username):
        assert own_username == "bot"

    def send_quote(self, **kwargs):
        self.sent.append(kwargs)
        return SendResult(kwargs["idempotency_key"], "unknown", error_code="needs_review")


def test_one_selected_probe_retains_unknown_without_claiming_receipt():
    gateway = Gateway()
    result = execute_one(gateway, spec(), "1")
    assert len(gateway.sent) == 1
    assert gateway.sent[0]["server_id"] == "9007199254740993"
    assert result["send"]["outcome"] == "unknown"
    assert not result["peer_received"] and not result["phone_full_received"]
    assert result["model_calls"] == result["business_writes"] == 0


@pytest.mark.parametrize("field,value", [("body", "changed"), ("senderId", "other"), ("recall", {}), ("type", 49)])
def test_changed_source_never_reaches_send(field, value):
    gateway = Gateway()
    gateway.message[field] = value
    with pytest.raises(GatewayError, match="probe_source_changed"):
        execute_one(gateway, spec(), "1")
    assert gateway.sent == []


def test_status_recovery_does_not_read_source_or_submit_again():
    class StatusGateway(Gateway):
        def get_message(self, *args, **kwargs):
            pytest.fail("status recovery must not require original source readability")

        def send_quote(self, **kwargs):
            pytest.fail("status recovery must never send")

        def get_send_status(self, key):
            assert key == spec()["cases"][0]["idempotency_key"]
            return SendResult(key, "unknown", error_code="needs_review")

    report = inspect_one(StatusGateway(), spec(), "1")
    assert report["operation"] == "status_only"
    assert report["send"]["outcome"] == "unknown"
    assert not report["phone_full_received"]


def test_native_probe_cannot_fall_back_to_profile_source_or_submission():
    native_source = {**source(), "receivedId": "recv-stable", "ownUsername": "bot",
                     "sourceSha256": "a" * 64, "disposition": "ready", "quoteEligible": True}
    plan = prepare_native_probe(run_id="native-one", account_id="account", own_username="bot",
                                thread_id="test@chatroom", sources=[native_source])

    class NativeGateway(Gateway):
        def get_message(self, *args, **kwargs):
            pytest.fail("profile fallback forbidden")

        def send_quote(self, **kwargs):
            pytest.fail("profile quote fallback forbidden")

        def get_received_message(self, received_id, **kwargs):
            assert received_id == "recv-stable"
            assert kwargs["source_sha256"] == "a" * 64
            return native_source

        def send_received_quote(self, **kwargs):
            assert kwargs["received_id"] == "recv-stable"
            assert kwargs["source_sha256"] == "a" * 64
            return SendResult(kwargs["idempotency_key"], "unknown")

    assert "message_id" not in plan["cases"][0]
    assert execute_one(NativeGateway(), plan, "1")["send"]["outcome"] == "unknown"
