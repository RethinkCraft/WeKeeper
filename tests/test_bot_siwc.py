"""Bot SIWC uses the existing subscription authorization, with business tools."""
import json

import httpx
import pytest

from chatlog_keeper.bot.providers import ChatGPTProvider
from chatlog_keeper.dashboard.siwc import SIWCClient, SIWCError
from chatlog_keeper.dashboard.usage import UsageLedger
from test_siwc import seed, stream


def test_subscription_bot_tool_loop_has_own_instructions_and_usage_kind(tmp_path):
    requests, results = [], []
    def remote(request):
        assert request.headers["authorization"] == "Bearer test-access"
        body = json.loads(request.content)
        requests.append(body)
        assert "只读工具" not in body["instructions"]
        assert body["instructions"] == "原创角色，业务结果优先"
        assert [t["name"] for t in body["tools"][0]["tools"]] == ["report_offer_status"]
        assert body["tools"][0]["tools"][0]["strict"] is False
        if len(requests) == 1:
            output = [{"type": "function_call", "namespace": "business", "name": "report_offer_status",
                       "call_id": "call", "arguments": '{"item":"test","status":"unavailable","evidence":"test"}'}]
            return httpx.Response(200, text=stream({"type": "response.completed", "response": {"output": output}}))
        assert body["input"][-1]["type"] == "function_call_output"
        return httpx.Response(200, text=stream({"type": "response.output_text.delta", "delta": "记好啦。"},
                                              {"type": "response.completed"}))
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(remote)))
    seed(client)
    p = ChatGPTProvider(tmp_path, {"id": "gpt", "kind": "siwc"}, client=client)
    text = p.complete([{"role": "user", "content": "记录"}], "原创角色，业务结果优先",
        [{"name": "report_offer_status", "parameters": {"type": "object", "properties": {}}}],
        lambda name, args: results.append((name, args)) or {"ok": True},
        {"run_id": "bot_case", "group_ref": "group_test", "bot_request_id": "event_test", "profile_id": "gpt"})
    assert text == "记好啦。" and len(requests) == 2 and results[0][0] == "report_offer_status"
    record = client._ledger().get("bot_case")
    assert record["kind"] == "bot_chat" and record["provider"] == "siwc"
    assert record["user_id"] is None and record["turn_id"] is None
    client.close()


def test_bot_has_three_tool_round_limit_without_executing_fourth_write(tmp_path):
    calls = []
    def remote(request):
        return httpx.Response(200, text=stream({"type": "response.completed", "response": {"output": [{
            "type": "function_call", "namespace": "business", "name": "report_offer_status",
            "call_id": "call", "arguments": "{}"}]}}))
    client = SIWCClient(tmp_path, http_client=httpx.Client(transport=httpx.MockTransport(remote)))
    seed(client)
    with pytest.raises(SIWCError, match="tool_round_limit"):
        client.bot_chat([], instructions="角色", tools=[{"type": "namespace", "name": "business", "tools": [{
            "type": "function", "name": "report_offer_status", "parameters": {"type": "object", "properties": {}}}]}],
            tool_handler=lambda *args: calls.append(args) or {"ok": True},
            run_metadata={"run_id": "rounds", "group_ref": "group_test", "bot_request_id": "event"})
    assert len(calls) == 3
    client.close()


def test_pilot_reuses_auth_and_inference_lock_but_never_writes_live_usage(tmp_path):
    live, pilot = tmp_path / "live-dashboard", tmp_path / "pilot-usage"
    def remote(request):
        assert request.headers["authorization"] == "Bearer test-access"
        return httpx.Response(200, text=stream({"type": "response.output_text.delta", "delta": "在呢。"},
                                              {"type": "response.completed"}))
    client = SIWCClient(live, http_client=httpx.Client(transport=httpx.MockTransport(remote)))
    seed(client)
    p = ChatGPTProvider(live, {"id": "gpt", "kind": "siwc"}, client=client, usage_dir=pilot)
    assert p.client.root == live / "siwc" and p.client.state_dir == live
    reply = p.complete([{"role": "user", "content": "你好"}], "人设", [{"name": "list_offer_items", "parameters": {
        "type": "object", "properties": {}, "additionalProperties": False}}], lambda *_: {},
        {"run_id": "pilot_run", "profile_id": "gpt", "group_ref": "group_test", "bot_request_id": "event"})
    assert reply == "在呢。"
    assert (live / "bot-inference-0.lock").is_file()
    assert not (live / "usage.sqlite").exists()
    client.close()
    ledger = UsageLedger(pilot)
    assert ledger.get("pilot_run")["kind"] == "bot_chat"
    ledger.close()
