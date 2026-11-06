"""Actual public Python client/outbox with FakeHTTP and private temporary SQL."""
import json
import httpx
import pytest
from chatlog_keeper.bot.protocol_gateway import ProtocolGateway, SendResult
from chatlog_keeper.bot.protocol_outbox import ProtocolOutbox

ACCOUNT, OWN, THREAD = "synthetic-account", "synthetic-self", "room@chatroom"
CODES = ("authorization_generation_changed", "profile_ownership_required")


class Peer:
    def __init__(self):
        self.posts = 0
        self.lookup_count = 0
        self.post_status, self.post_code = 409, CODES[0]
        self.lookup_status, self.lookup_code = 409, CODES[0]
        self.timeout = False
    def handle(self, request):
        path = request.url.path
        if path == "/v1/status":
            return httpx.Response(200, json={"serverId": "server", "ownershipGeneration": "owner"})
        if path == "/v1/accounts/" + ACCOUNT:
            return httpx.Response(200, json={"id": ACCOUNT, "ownUsername": OWN})
        if path == "/v1/accounts/" + ACCOUNT + "/sends" and request.method == "POST":
            self.posts += 1
            if self.timeout:
                raise httpx.ReadTimeout("fixed response loss", request=request)
            return httpx.Response(self.post_status, json={"error": {"code": self.post_code},
                "direct_post_rejection": True})  # Untrusted JSON never supplies provenance.
        if "/sends/" in path and request.method == "GET":
            self.lookup_count += 1
            return httpx.Response(self.lookup_status, json={"error": {"code": self.lookup_code},
                "direct_post_rejection": True})
        raise AssertionError((request.method, path))


def source():
    return {"receivedId": "received", "firstReceivedSequence": "1", "accountId": ACCOUNT,
        "ownUsername": OWN, "threadId": THREAD, "serverId": "17", "senderId": "peer",
        "sourceSha256": "a" * 64, "direction": "incoming", "disposition": "ready", "reason": None,
        "quoteEligible": True, "type": 1, "appMessageType": None, "body": "question",
        "createdAt": "2026-10-30T00:00:00Z", "mentions": {"state": "known", "userIds": [OWN]}, "quote": None}


def client(directory, peer):
    return ProtocolGateway(token="not-a-real-token", account_id=ACCOUNT,
        journal_path=directory/"gateway.sqlite", transport=httpx.MockTransport(peer.handle))


def freeze(outbox):
    return outbox.freeze(request_id="request", reply_id="reply", source=source(),
                         text="frozen answer", evidence={"request": "request"})


def send(gateway):
    return gateway.send_received_quote(thread_id=THREAD, received_id="received", server_id="17",
        source_sha256="a"*64, text="frozen answer", idempotency_key="reply")


@pytest.mark.parametrize("code", CODES)
def test_actual_post_409_is_explicit_client_and_persistent_outbox_rejection(tmp_path, code):
    peer = Peer(); peer.post_code = code
    with client(tmp_path, peer) as gateway, ProtocolOutbox(tmp_path/"outbox.sqlite", account_id=ACCOUNT, own_username=OWN) as outbox:
        freeze(outbox)
        result = outbox.deliver_one(gateway, "reply")
        assert result["state"] == "rejected"
        assert result["error_code"] == code
        assert peer.posts == 1 and peer.lookup_count == 0
        assert outbox.deliver_one(gateway, "reply")["state"] == "rejected"
        assert outbox.reconcile(gateway, "reply")["state"] == "rejected"
    with client(tmp_path, peer) as gateway, ProtocolOutbox(tmp_path/"outbox.sqlite", account_id=ACCOUNT, own_username=OWN) as outbox:
        assert outbox.get("reply")["state"] == "rejected"
        assert outbox.deliver_one(gateway, "reply")["state"] == "rejected"
    assert peer.posts == 1 and peer.lookup_count == 0


@pytest.mark.parametrize("code", CODES)
def test_public_gateway_provenance_only_describes_actual_post(tmp_path, code):
    peer = Peer(); peer.post_code = code
    with client(tmp_path, peer) as gateway:
        result = send(gateway)
        assert result.outcome == "rejected" and result.direct_post_rejection is True
        assert result.http_status == 409 and result.error_code == code and result.intent is None
        # Same retained key takes a status lookup, even if JSON tries to forge a flag.
        again = send(gateway)
        assert again.outcome == "unknown" and again.direct_post_rejection is False
    assert peer.posts == 1 and peer.lookup_count == 1


@pytest.mark.parametrize("code", CODES)
def test_queued_outbox_with_already_claimed_journal_cannot_treat_lookup_409_as_reject(tmp_path, code):
    peer = Peer(); peer.post_status = 500; peer.post_code = "operation_failed"
    with client(tmp_path, peer) as gateway:
        assert send(gateway).outcome == "unknown"
        peer.lookup_code = code
        with ProtocolOutbox(tmp_path/"outbox.sqlite", account_id=ACCOUNT, own_username=OWN) as outbox:
            freeze(outbox)
            assert outbox.deliver_one(gateway, "reply")["state"] == "unknown"
            assert outbox.deliver_one(gateway, "reply")["state"] == "unknown"
    assert peer.posts == 1


@pytest.mark.parametrize("state", ["submitting", "unknown"])
def test_old_attempt_or_forged_status_result_never_proves_rejection(tmp_path, state):
    peer = Peer(); peer.post_status = 500; peer.post_code = "operation_failed"
    with client(tmp_path, peer) as gateway, ProtocolOutbox(tmp_path/"outbox.sqlite", account_id=ACCOUNT, own_username=OWN) as outbox:
        freeze(outbox)
        assert outbox.deliver_one(gateway, "reply")["state"] == "unknown"
        with outbox._lock, outbox._transaction():
            outbox._db.execute("UPDATE replies SET state=? WHERE reply_id='reply'", (state,))
        assert outbox.reconcile(gateway, "reply")["state"] == "unknown"
        # A malicious/faulty status producer cannot certify an earlier attempt.
        if "direct_post_rejection" in SendResult.__dataclass_fields__:
            gateway.get_send_status = lambda key: SendResult(key, "rejected", error_code=CODES[0],
                http_status=409, direct_post_rejection=True)
            assert outbox.reconcile(gateway, "reply")["state"] == "unknown"
        assert outbox.deliver_one(gateway, "reply")["state"] == "unknown"
    assert peer.posts == 1


@pytest.mark.parametrize("status,code,timeout", [(500,"operation_failed",False), (409,"idempotency_conflict",False),
    (409,"authorization_generation_changed_typo",False), (408,CODES[0],False), (500,CODES[0],False), (500,"operation_failed",True)])
def test_other_errors_keep_unknown_and_do_not_resubmit(tmp_path, status, code, timeout):
    peer=Peer();peer.post_status=status;peer.post_code=code;peer.timeout=timeout
    with client(tmp_path, peer) as gateway, ProtocolOutbox(tmp_path/"outbox.sqlite", account_id=ACCOUNT, own_username=OWN) as outbox:
        freeze(outbox)
        assert outbox.deliver_one(gateway, "reply")["state"] == "unknown"
        assert outbox.reconcile(gateway, "reply")["state"] == "unknown"
        assert outbox.deliver_one(gateway, "reply")["state"] == "unknown"
    assert peer.posts == 1
