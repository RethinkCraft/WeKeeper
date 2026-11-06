"""Retry one pre-header transport attempt, never a business execution or UI send."""
from contextlib import contextmanager
import json
import time

import httpx
import pytest

from chatlog_keeper.dashboard.siwc import SIWCClient, SIWCError
from test_siwc import seed, stream


def completed():
    return httpx.Response(200, text=stream(
        {"type": "response.output_text.delta", "delta": "在呢。"},
        {"type": "response.completed", "response": {"status": "completed"}},
    ))


def bot(client, events, *, handler=lambda *_: None, tools=None, timeout=60):
    return client.bot_chat([{"role": "user", "content": "在吗"}], instructions="简短回复。",
        tools=tools or [], tool_handler=handler, on_event=events.append, timeout=timeout,
        run_metadata={"run_id": "retry-test", "group_ref": "group_test", "bot_request_id": "request_test", "profile_id": "gpt"})


@pytest.fixture
def make_client(tmp_path):
    clients = []
    class TrackingClient(httpx.Client):
        @contextmanager
        def stream(self, *args, **kwargs):
            self.lifecycle.append("open")
            try:
                with super().stream(*args, **kwargs) as response:
                    yield response
            finally:
                self.lifecycle.append("closed")
    def make(remote):
        http = TrackingClient(transport=httpx.MockTransport(remote))
        http.lifecycle = []
        client = SIWCClient(tmp_path, http_client=http)
        seed(client)
        clients.append((client, http))
        return client, http
    yield make
    for client, http in clients:
        client.close(); http.close()


@pytest.mark.parametrize("error_type", [
    httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadError, httpx.ReadTimeout,
    httpx.WriteError, httpx.WriteTimeout, httpx.RemoteProtocolError, httpx.ProxyError,
])
def test_one_pre_header_failure_retries_identical_request_and_closes_first_attempt(make_client, error_type):
    requests, events = [], []
    def remote(request):
        requests.append(json.loads(request.content))
        if len(requests) == 1:
            raise error_type("DO_NOT_LEAK_TOKEN test-secret", request=request)
        assert http.lifecycle == ["open", "closed", "open"]
        return completed()
    client, http = make_client(remote)
    assert bot(client, events)["output_text"] == "在呢。"
    assert len(requests) == 2 and requests[0] == requests[1]
    assert http.lifecycle == ["open", "closed", "open", "closed"]
    failures = [event for event in events if event.get("type") == "failure"]
    assert failures == [{"type": "failure", "stage": "model_transport", "error": "siwc_network_error",
                         "transport_error_class": error_type.__name__}]
    assert [event for event in events if event.get("type") == "retry"] == [
        {"type": "retry", "stage": "model_transport", "attempt": 2, "transport_error_class": error_type.__name__}]
    assert "DO_NOT_LEAK_TOKEN" not in json.dumps(events)
    calls = client._ledger().conn.execute("SELECT status,usage FROM usage_calls ORDER BY rowid").fetchall()
    assert len(calls) == 2 and calls[0]["status"] == "interrupted" and calls[1]["status"] == "completed"
    assert calls[0]["usage"] is None  # Unknown billed usage is not invented as zero.


def test_two_pre_header_failures_stop_after_second_actual_attempt(make_client):
    calls, events = [], []
    def remote(request):
        calls.append(request)
        raise httpx.RemoteProtocolError("DO_NOT_LEAK_TOKEN", request=request)
    client, http = make_client(remote)
    with pytest.raises(SIWCError, match="siwc_network_error") as caught:
        bot(client, events)
    assert len(calls) == 2
    assert len([event for event in events if event.get("type") == "retry"]) == 1
    assert len([event for event in events if event.get("type") == "failure"]) == 2
    assert "DO_NOT_LEAK_TOKEN" not in str(caught.value) + json.dumps(events)
    assert client._ledger().get("retry-test")["status"] == "failed"
    assert client._ledger().conn.execute("SELECT COUNT(*) FROM usage_calls").fetchone()[0] == 2


@pytest.mark.parametrize("partial", [False, True])
def test_no_retry_after_headers_even_if_no_text_was_received(make_client, partial):
    events, calls, closed = [], [], []
    class BrokenStream(httpx.SyncByteStream):
        def __iter__(self):
            if partial:
                yield stream({"type": "response.output_text.delta", "delta": "未完成正文"}).encode()
            raise httpx.ReadError("DO_NOT_LEAK_TOKEN")
        def close(self):
            closed.append(True)
    def remote(request):
        calls.append(request)
        return httpx.Response(200, stream=BrokenStream())
    client, http = make_client(remote)
    with pytest.raises(SIWCError, match="siwc_network_error"):
        bot(client, events)
    assert len(calls) == 1 and closed == [True]
    assert not any(event.get("type") == "retry" for event in events)
    assert any(event.get("stage") == "model_transport" and event.get("transport_error_class") == "ReadError" for event in events)
    assert client._ledger().conn.execute("SELECT COUNT(*) FROM usage_calls").fetchone()[0] == 1


@pytest.mark.parametrize("status", [401, 403, 429, 503])
def test_http_error_status_is_not_transport_retry(make_client, status):
    calls, events = [], []
    def remote(request):
        calls.append(request)
        return httpx.Response(status, json={"error": {"code": "provider_test_error"}})
    client, http = make_client(remote)
    with pytest.raises(SIWCError):
        bot(client, events)
    assert len(calls) == 1
    assert not any(event.get("type") == "retry" for event in events)


@pytest.mark.parametrize("error_type", [httpx.LocalProtocolError, httpx.UnsupportedProtocol])
def test_invalid_local_request_is_not_retried(make_client, error_type):
    calls, events = [], []
    def remote(request):
        calls.append(request)
        raise error_type("DO_NOT_LEAK_TOKEN", request=request)
    client, http = make_client(remote)
    with pytest.raises(SIWCError, match="siwc_network_error") as caught:
        bot(client, events)
    assert len(calls) == 1 and caught.value.retryable is False
    assert not any(event.get("type") == "retry" for event in events)
    assert "DO_NOT_LEAK_TOKEN" not in str(caught.value) + json.dumps(events)


def test_retry_second_model_round_never_reexecutes_business_handler(make_client):
    bodies, writes, events = [], [], []
    def remote(request):
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            return httpx.Response(200, text=stream({"type": "response.completed", "response": {"output": [{
                "type": "function_call", "namespace": "business", "name": "report_offer_status", "call_id": "report-call", "arguments": "{}"}]}}))
        if len(bodies) == 2:
            raise httpx.ConnectError("connection lost before headers", request=request)
        return completed()
    client, http = make_client(remote)
    tools = [{"type": "namespace", "name": "business", "tools": [{"type": "function", "name": "report_offer_status",
        "parameters": {"type": "object", "properties": {}}}]}]
    def report(name, arguments):
        writes.append((name, arguments))
        return {"ok": True, "recorded": True}
    assert bot(client, events, handler=report, tools=tools)["output_text"] == "在呢。"
    assert writes == [("report_offer_status", {})]
    assert len(bodies) == 3 and bodies[1] == bodies[2]
    assert bodies[2]["input"][-1]["type"] == "function_call_output"
    assert client._ledger().conn.execute("SELECT COUNT(*) FROM usage_calls").fetchone()[0] == 3


def test_cloud_chat_transport_failure_is_not_automatically_retried(make_client):
    calls = []
    def remote(request):
        calls.append(request)
        raise httpx.ConnectError("temporary outage", request=request)
    client, http = make_client(remote)
    with pytest.raises(SIWCError, match="siwc_network_error"):
        client.chat([{"role": "user", "content": "synthetic question"}])
    assert len(calls) == 1


def test_retry_cannot_extend_original_deadline(make_client, monkeypatch):
    clock = [time.monotonic()]
    calls, events = [], []
    def remote(request):
        calls.append(request)
        clock[0] += 2
        raise httpx.ConnectTimeout("expired attempt", request=request)
    client, http = make_client(remote)
    monkeypatch.setattr("chatlog_keeper.dashboard.siwc.time.monotonic", lambda: clock[0])
    with pytest.raises(SIWCError, match="siwc_request_timeout"):
        bot(client, events, timeout=1)
    assert len(calls) == 1
    assert not any(event.get("type") == "retry" for event in events)


def test_second_attempt_uses_remaining_timeout_not_fresh_budget(make_client, monkeypatch):
    clock = [time.monotonic()]
    seen_timeouts, events = [], []
    def remote(request):
        seen_timeouts.append(request.extensions["timeout"]["read"])
        if len(seen_timeouts) == 1:
            clock[0] += 2
            raise httpx.ConnectError("transient failure", request=request)
        return completed()
    client, http = make_client(remote)
    monkeypatch.setattr("chatlog_keeper.dashboard.siwc.time.monotonic", lambda: clock[0])
    assert bot(client, events, timeout=5)["output_text"] == "在呢。"
    assert seen_timeouts == [5, 3]
