import io
import json
import os
import threading
import time
from urllib.parse import parse_qs, urlencode, urlparse

from cryptography.hazmat.primitives.asymmetric import rsa
import httpx
import jwt
import pytest

from chatlog_keeper.dashboard.siwc import DIRECT_SCOPE, ISSUER, RESOURCE, SIWCClient, SIWCError


@pytest.fixture
def signing_key():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(key.public_key()))
    jwk.update(kid="test-key", alg="RS256", use="sig")
    return key, jwk


def claims_token(key, *, audience="issued-client", nonce="nonce", subject="subject"):
    return jwt.encode({"iss": ISSUER, "aud": audience, "sub": subject, "nonce": nonce,
                       "iat": int(time.time()), "exp": int(time.time()) + 3600, "email": "sample@example.test"},
                      key, algorithm="RS256", headers={"kid": "test-key"})


def seed(client, *, expired=False, scopes=None):
    with client._locked():
        store = client._read()
        store["active_account"] = "account"
        store["accounts"]["account"] = {
            "client_id": "issued-client", "subject": "subject", "issuer": ISSUER,
            "email": "sample@example.test", "label": "Test", "access_token": "test-access",
            "refresh_token": "test-refresh", "id_token": "test-id", "scopes": scopes if scopes is not None else [DIRECT_SCOPE],
            "expires_at": time.time() - 1 if expired else time.time() + 3600,
            "model": "test-model", "ext_agent_host_id": store["host_id"],
        }
        client._write(store)


@pytest.mark.parametrize("callback_port", [0, 1455])
def test_login_uses_loopback_pkce_and_validated_independent_registration(tmp_path, signing_key, monkeypatch, callback_port):
    key, jwk = signing_key
    browser_values = {}

    def remote(request):
        if request.url.path == "/.well-known/openid-configuration":
            return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": ISSUER + "/keys"})
        if request.url.path == "/keys":
            return httpx.Response(200, json={"keys": [jwk]})
        if request.url.path.endswith("/oauth/token"):
            fields = parse_qs(request.content.decode())
            assert fields["client_id"] == ["issued-client"]
            assert fields["redirect_uri"][0].endswith("/auth/callback")
            assert fields["resource"] == [RESOURCE]
            assert fields["code_verifier"][0]
            return httpx.Response(200, json={"access_token": "new-access", "refresh_token": "new-refresh",
                       "scope": DIRECT_SCOPE + " offline_access", "expires_in": 3600,
                       "id_token": claims_token(key, nonce=browser_values["nonce"][0])})
        raise AssertionError(request.url)

    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(remote)))
    class Server:
        server_port = 1455

        def __init__(self, address, handler):
            assert address == ("127.0.0.1", callback_port)
            self.handler = handler
            self.steps = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def handle_request(self):
            request = self.handler.__new__(self.handler)
            request.headers = {"Host": "127.0.0.1:1455"}
            request.server = self
            request.wfile = io.BytesIO()
            headers, responses = {}, []
            request.send_response = lambda code: responses.append(code)
            request.send_header = lambda key, value: headers.update({key: value})
            request.end_headers = lambda: None
            request.send_error = lambda code, *_: responses.append(code)
            if self.steps == 0:
                request.path = "/login"
            elif self.steps == 1:
                request.path = "/auth/callback?state=wrong&code=x"
            else:
                request.path = "/auth/callback?" + urlencode({"state": browser_values["state"][0], "code": "code", "client_id": "issued-client"})
            request.do_GET()
            if self.steps == 0:
                assert responses == [302]
                browser_values.update(parse_qs(urlparse(headers["Location"]).query))
                assert browser_values["client_id"] == ["dynamic_agent_client"]
                assert browser_values["agent_name_hint"] == ["WeKeeper"]
            elif self.steps == 1:
                assert responses == [400]
            else:
                assert responses == [200]
            self.steps += 1

    monkeypatch.setattr("chatlog_keeper.dashboard.siwc.HTTPServer", Server)

    def launch(url):
        assert "id_token" not in url and urlparse(url).hostname == "127.0.0.1"

    result = client.login(open_browser=False, timeout=10, url_callback=launch, callback_port=callback_port)
    assert result["state"] == "model_required"
    assert "access_token" not in json.dumps(result)
    assert os.stat(client.path).st_mode & 0o777 == 0o600
    assert os.stat(client.root).st_mode & 0o777 == 0o700
    assert client._read()["accounts"][result["active_account"]]["client_id"] == "issued-client"


@pytest.mark.parametrize("wrong", ["nonce", "audience", "subject", "signature"])
def test_id_token_checks_nonce_identity_audience_and_signature(tmp_path, signing_key, wrong):
    key, jwk = signing_key

    def remote(request):
        if request.url.path.endswith("openid-configuration"):
            return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": ISSUER + "/keys"})
        return httpx.Response(200, json={"keys": [jwk]})
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(remote)))
    token = claims_token(rsa.generate_private_key(public_exponent=65537, key_size=2048) if wrong == "signature" else key,
                         nonce="other" if wrong == "nonce" else "nonce",
                         audience="other" if wrong == "audience" else "issued-client",
                         subject="other" if wrong == "subject" else "subject")
    with pytest.raises(SIWCError):
        client._validate_id(token, "issued-client", nonce="nonce", subject="subject")


def test_refresh_is_serialized_and_persists_rotating_token(tmp_path):
    calls = []

    def remote(request):
        fields = parse_qs(request.content.decode())
        calls.append(fields)
        time.sleep(0.05)
        return httpx.Response(200, json={"access_token": "renewed-access", "refresh_token": "rotated-refresh",
                                       "expires_in": 3600, "scope": DIRECT_SCOPE})
    transport = httpx.MockTransport(remote)
    one = SIWCClient(tmp_path, http_client=httpx.Client(transport=transport))
    two = SIWCClient(tmp_path, http_client=httpx.Client(transport=transport))
    seed(one, expired=True)
    tokens = []
    threads = [threading.Thread(target=lambda c=c: tokens.append(c._access()[0])) for c in (one, two)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert tokens == ["renewed-access", "renewed-access"]
    assert len(calls) == 1
    assert calls[0]["refresh_token"] == ["test-refresh"]
    assert one._read()["accounts"]["account"]["refresh_token"] == "rotated-refresh"


def test_terminal_refresh_clears_tokens_but_retains_registration(tmp_path):
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(
        lambda _: httpx.Response(400, json={"error": "invalid_grant"}))))
    seed(client, expired=True)
    with pytest.raises(SIWCError, match="invalid_grant"):
        client._access()
    saved = client._read()["accounts"]["account"]
    assert "access_token" not in saved and saved["client_id"] == "issued-client"
    assert client.status()["state"] == "login_required"


def test_temporary_refresh_does_not_erase_credentials(tmp_path):
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(
        lambda _: httpx.Response(503, json={"detail": "unavailable"}))))
    seed(client, expired=True)
    with pytest.raises(SIWCError) as caught:
        client._access()
    assert caught.value.retryable
    assert client._read()["accounts"]["account"]["refresh_token"] == "test-refresh"


def test_declined_plan_permission_blocks_inference_without_network(tmp_path):
    calls = []
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(lambda request: calls.append(request))))
    seed(client, scopes=["openid"])
    with pytest.raises(SIWCError, match="plan_permission_required"):
        client.summarize([])
    assert not calls


def stream(*events):
    return "".join("data: " + json.dumps(event) + "\n\n" for event in events)


def test_inference_requires_completed_and_valid_references(tmp_path):
    requests = []
    value = {"topics": [{"text": "A discussion", "message_refs": ["msg_one"]}], "decisions": [], "todos": []}

    def remote(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, text=stream({"type": "response.output_text.delta", "delta": json.dumps(value)},
                                              {"type": "response.completed", "response": {"status": "completed"}}))
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(remote)))
    seed(client)
    assert client.summarize([{"message_ref": "msg_one", "text": "data"}]) == value
    assert requests[0]["store"] is False and requests[0]["stream"] is True
    assert not ({"background", "max_output_tokens", "temperature", "previous_response_id", "tools"} & requests[0].keys())


@pytest.mark.parametrize("ending,expected,paused", [
    (None, "siwc_interrupted_stream", False),
    ({"type": "response.incomplete"}, "siwc_incomplete_response", False),
    ({"type": "response.failed", "response": {"error": {"code": "subscription_sharing_usage_limit_exceeded"}}},
     "subscription_sharing_usage_limit_exceeded", True),
])
def test_partial_or_failed_stream_never_becomes_success(tmp_path, ending, expected, paused):
    events = [{"type": "response.output_text.delta", "delta": '{"topics":[],"decisions":[],"todos":[]}'}]
    if ending:
        events.append(ending)
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, text=stream(*events)))))
    seed(client)
    with pytest.raises(SIWCError, match=expected) as caught:
        client.summarize([])
    assert caught.value.paused is paused


def test_fabricated_reference_is_rejected(tmp_path):
    value = {"topics": [{"text": "unsupported", "message_refs": ["msg_invented"]}], "decisions": [], "todos": []}
    response = stream({"type": "response.output_text.delta", "delta": json.dumps(value)}, {"type": "response.completed"})
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, text=response))))
    seed(client)
    with pytest.raises(SIWCError, match="invalid_summary_reference"):
        client.summarize([{"message_ref": "msg_real", "text": "record"}])


def test_provider_usage_is_saved_before_rejecting_invalid_summary_json(tmp_path):
    body = stream({"type": "response.output_text.delta", "delta": "not JSON"},
                  {"type": "response.completed", "response": {"id": "response-id", "usage": {
                      "input_tokens": 10, "output_tokens": 3, "total_tokens": 13,
                      "input_tokens_details": {"cached_tokens": 4}}}})
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(
        lambda _: httpx.Response(200, text=body, headers={"x-request-id": "request-id"}))))
    seed(client)
    with pytest.raises(SIWCError, match="invalid_summary_json"):
        client.summarize([{"message_ref": "msg_real", "text": "record"}])
    usage = client.usage_ledger.pending()[0]
    assert usage["status"] == "failed" and usage["usage"]["total_tokens"] == 13
    call = client.usage_ledger.conn.execute("SELECT request_id,response_id FROM usage_calls").fetchone()
    assert tuple(call) == ("request-id", "response-id")
    client.close()


TOOLS = [{"type": "namespace", "name": "wekeeper", "tools": [{"type": "function", "name": "search_messages",
          "description": "Read authorized evidence", "parameters": {"type": "object", "properties": {}, "additionalProperties": False}, "strict": True}]}]


def test_chat_namespace_tool_loop_streams_and_defers_finalization(tmp_path):
    requests, events, tools = [], [], []
    def remote(request):
        payload = json.loads(request.content)
        requests.append(payload)
        assert payload["tools"] == TOOLS
        assert "trusted root instruction" in payload["instructions"]
        if len(requests) == 1:
            assert len(payload["input"]) == 20
            output = [{"type": "function_call", "namespace": "wekeeper", "name": "search_messages", "call_id": "call-one", "arguments": "{}"}]
            body = stream({"type": "response.completed", "response": {"id": "r1", "output": output, "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}}})
        else:
            assert payload["input"][-1]["type"] == "function_call_output"
            body = stream({"type": "response.output_text.delta", "delta": "Answer [[msg_real]]"},
                          {"type": "response.completed", "response": {"id": "r2", "usage": {"input_tokens": 20, "output_tokens": 3, "total_tokens": 23}}})
        return httpx.Response(200, text=body, headers={"x-request-id": "q" + str(len(requests))})
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(remote)))
    seed(client)
    result = client.chat([{"role": "user", "content": str(index)} for index in range(30)], tools=TOOLS,
                         tool_handler=lambda name, args: tools.append((name, args)) or {"messages": []},
                         on_event=events.append, context={"instructions": "trusted root instruction"},
                         run_metadata={"run_id": "chat-run", "kind": "chat", "user_id": 2, "turn_id": "turn", "defer_finalization": True})
    assert result["output_text"] == "Answer [[msg_real]]"
    assert result["tool_calls"] == 1 and result["response_id"] == "r2"
    assert result["usage"]["total_tokens"] == 35
    assert tools == [("search_messages", {})]
    assert {"type": "tool", "name": "search_messages"} in events
    assert {"type": "delta", "text": "Answer [[msg_real]]"} in events
    assert not client.usage_ledger.pending()
    assert client.usage_ledger.list_running("turn")[0]["run_id"] == "chat-run"
    client.usage_ledger.finish_run("chat-run", status="completed")
    assert client.usage_ledger.pending()[0]["usage"]["total_tokens"] == 35
    client.close()


def test_unapproved_tool_does_not_execute_but_usage_is_preserved(tmp_path):
    calls = []
    body = stream({"type": "response.completed", "response": {"output": [{"type": "function_call", "name": "delete_file", "call_id": "bad", "arguments": "{}"}], "usage": {"input_tokens": 5, "output_tokens": 1, "total_tokens": 6}}})
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, text=body))))
    seed(client)
    with pytest.raises(SIWCError, match="unapproved_tool"):
        client.chat([{"role": "user", "content": "question"}], tools=TOOLS,
                    tool_handler=lambda *args: calls.append(args))
    assert not calls
    assert client.usage_ledger.pending()[0]["usage"]["total_tokens"] == 6
    assert client.usage_ledger.pending()[0]["status"] == "failed"
    client.close()


def test_global_file_lock_serializes_summary_and_chat_calls(tmp_path):
    active, peak, mutex, failures = [0], [0], threading.Lock(), []
    def remote(request):
        with mutex:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        time.sleep(0.04)
        with mutex:
            active[0] -= 1
        return httpx.Response(200, text=stream({"type": "response.output_text.delta", "delta": '{"topics":[],"decisions":[],"todos":[]}'}, {"type": "response.completed"}))
    clients = [SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(remote))) for _ in range(2)]
    seed(clients[0])
    def run(index):
        try:
            if index:
                clients[index].chat([{"role": "user", "content": "question"}])
            else:
                clients[index].summarize([])
        except Exception as exc:
            failures.append(exc)
        finally:
            clients[index].close()
    threads = [threading.Thread(target=run, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not failures and peak[0] == 1


def test_shared_pause_inside_model_lock_blocks_request_without_fake_provider_call(tmp_path):
    from chatlog_keeper.dashboard.summaries import SummaryWorker
    class Ready:
        def status(self):
            return {"state": "ready", "model": "test-model"}
    state = SummaryWorker(tmp_path, "source", client=Ready())
    state.record_error(SIWCError("subscription_sharing_usage_limit_exceeded", paused=True))
    calls = []
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(lambda request: calls.append(request))))
    seed(client)
    with pytest.raises(SIWCError, match="siwc_shared_pause"):
        client.chat([{"role": "user", "content": "question"}])
    assert not calls
    assert client.usage_ledger.conn.execute("SELECT COUNT(*) FROM usage_calls").fetchone()[0] == 0
    assert client.usage_ledger.pending()[0]["usage"] is None
    client.close()
    state.close()


def test_item_done_fallback_preserves_tools_reasoning_text_and_usage(tmp_path):
    requests, called = [], []
    reasoning = {"id": "reasoning-one", "type": "reasoning", "summary": [], "encrypted_content": "opaque-reasoning"}
    function = {"id": "function-one", "type": "function_call", "status": "completed", "arguments": "{}",
                "call_id": "call-one", "name": "search_messages", "namespace": "wekeeper"}
    message = {"id": "message-two", "type": "message", "status": "completed", "role": "assistant",
               "content": [{"type": "output_text", "text": "Verified answer [[msg_real]]", "annotations": []}]}
    def remote(request):
        payload = json.loads(request.content)
        requests.append(payload)
        if len(requests) == 1:
            body = stream({"type": "response.output_item.done", "output_index": 0, "item": reasoning},
                          {"type": "response.output_item.done", "output_index": 1, "item": function},
                          {"type": "response.completed", "response": {"id": "r1", "output": [], "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12}}})
        else:
            assert reasoning in payload["input"] and function in payload["input"]
            assert payload["input"][-1] == {"type": "function_call_output", "call_id": "call-one", "output": '{"messages":[]}'}
            body = stream({"type": "response.output_item.done", "output_index": 0, "item": message},
                          {"type": "response.completed", "response": {"id": "r2", "output": [], "usage": {"input_tokens": 20, "output_tokens": 3, "total_tokens": 23}}})
        return httpx.Response(200, text=body)
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(remote)))
    seed(client)
    result = client.chat([{"role": "user", "content": "question"}], tools=TOOLS,
                         tool_handler=lambda name, arguments: called.append((name, arguments)) or {"messages": []})
    assert called == [("search_messages", {})]
    assert len(requests) == 2 and result["output_text"] == "Verified answer [[msg_real]]"
    assert result["usage"]["total_tokens"] == 35
    assert client.usage_ledger.pending()[0]["usage"]["total_tokens"] == 35
    client.close()


def test_empty_completed_response_is_not_a_successful_chat(tmp_path):
    body = stream({"type": "response.completed", "response": {"id": "empty", "output": [], "usage": {"input_tokens": 5, "output_tokens": 1, "total_tokens": 6}}})
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, text=body))))
    seed(client)
    with pytest.raises(SIWCError, match="siwc_empty_response"):
        client.chat([{"role": "user", "content": "question"}])
    run = client.usage_ledger.pending()[0]
    assert run["status"] == "failed" and run["usage"]["total_tokens"] == 6
    client.close()
