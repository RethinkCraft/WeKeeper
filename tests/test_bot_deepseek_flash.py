"""Flash payload and diagnostics use only mocked API responses and fake keys."""
import json

import httpx
import pytest

from chatlog_keeper.bot.providers import CompatibleProvider, ProviderError, save_credential


KEY = "not-a-real-secret-flash-test-only"
TOOLS = [{"name": "query_offer_status", "description": "查询", "parameters": {"type": "object", "properties": {}}}]


def configured(tmp_path, client):
    save_credential(tmp_path, "flash-test", KEY)
    return CompatibleProvider(tmp_path, tmp_path / "usage", {
        "id": "flash-test", "kind": "deepseek", "model": "deepseek-flash",
        "credential_ref": "flash-test", "tools_verified": True}, client=client)


def metadata(events):
    return {"provider": "deepseek", "profile_id": "flash-test", "group_ref": "group-test",
            "bot_request_id": "event-test", "_on_event": events.append}


def test_flash_every_tool_round_disables_thinking_and_reports_response_model(tmp_path):
    bodies, events, executions = [], [], []
    models = ["deepseek-flash", "DeepSeek-V4.1-Flash-1017"]
    def remote(request):
        body = json.loads(request.content)
        bodies.append(body)
        assert str(request.url) == "https://api.deepseek.com/chat/completions"
        assert body["model"] == "deepseek-flash"
        assert body["thinking"] == {"type": "disabled"}
        assert body["stream"] is False and body["max_tokens"] == 512
        assert {tool["function"]["name"] for tool in body["tools"]} == {"query_offer_status"}
        message = {"role": "assistant", "content": "今天娱乐首单已无。"}
        if len(bodies) == 1:
            message.update(content=None, tool_calls=[{"id": "call-1", "type": "function", "function": {
                "name": "query_offer_status", "arguments": "{}"}}])
        return httpx.Response(200, json={"id": "response-" + str(len(bodies)),
            "model": models[len(bodies) - 1], "choices": [{"message": message}]})
    with httpx.Client(transport=httpx.MockTransport(remote)) as client:
        p = configured(tmp_path, client)
        try:
            result = p.complete([{"role": "user", "content": "查一下首单"}], "人设", TOOLS,
                lambda name, args: executions.append((name, args)) or {"status": "unavailable"},
                metadata(events))
            assert result == "今天娱乐首单已无。"
            assert executions == [("query_offer_status", {})] and len(bodies) == 2
            assert bodies[1]["messages"][-1]["role"] == "tool"
            assert [e["model"] for e in events if e["type"] == "model"] == models
        finally:
            p.close()


@pytest.mark.parametrize("model", [None, "", ["deepseek-flash"], "unsafe\nmodel", "https://provider.test/model",
    "x" * 161, "sk-do-not-record", "Bearer-token", KEY, "prefix-" + KEY])
def test_untrusted_or_missing_response_model_uses_safe_requested_model(tmp_path, model):
    events = []
    def remote(_):
        return httpx.Response(200, json={"model": model, "choices": [{"message": {"content": "你好呀"}}]})
    with httpx.Client(transport=httpx.MockTransport(remote)) as client:
        p = configured(tmp_path, client)
        try:
            assert p.complete([], "人设", TOOLS, lambda *_: None, metadata(events)) == "你好呀"
            assert [e for e in events if e["type"] == "model"] == [{"type": "model", "model": "deepseek-flash"}]
            assert KEY not in json.dumps(events)
        finally:
            p.close()


def test_http_failure_does_not_publish_returned_model_metadata(tmp_path):
    events = []
    with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(401,
            json={"model": "untrusted-model", "error": KEY}))) as client:
        p = configured(tmp_path, client)
        try:
            with pytest.raises(ProviderError, match="bot_provider_auth"):
                p.complete([], "人设", TOOLS, lambda *_: None, metadata(events))
            assert not [e for e in events if e["type"] == "model"]
            assert KEY not in json.dumps(events)
        finally:
            p.close()
