"""Isolated generation: synthetic sources, local ledger and injected providers."""
import asyncio
import copy
from datetime import datetime
import json

import pytest

from chatlog_keeper.bot.reply_generator import GenerationResult, ReplyGenerator
from chatlog_keeper.bot.service import Service, tool_schemas
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config


NOW = datetime.fromisoformat("2026-10-29T10:00:00+08:00").timestamp()
DAY = "2026-10-29"


@pytest.fixture
def store(tmp_path):
    value = BotStore(tmp_path)
    value.configure(config())
    yield value
    value.close()


def source(key="one", text="你好", *, timestamp=NOW, sender="wxid_manager"):
    identity = "protocol:" + key
    return {"event_id": identity, "message_ref": identity, "group_ref": "group_a",
            "sender_id": sender, "text": text, "occurred_at": timestamp,
            "create_time": timestamp, "native_source": {
                "own_username": "wxid_robot", "thread_id": "synthetic@chatroom",
                "server_id": "123456789012345678901234567890" + key,
                "received_id": "received:" + key},
            "reply_to": {"message_ref": identity, "group_ref": "group_a",
                         "received_id": "received:" + key}}


class Provider:
    def __init__(self, complete):
        self.operation = complete
        self.closed = False

    def complete(self, *args):
        return self.operation(*args)

    def close(self):
        self.closed = True


def generator(store, tmp_path, provider, *, host_callback=None):
    class Host:
        tools = [tool["function"] for tool in tool_schemas()]

        def __init__(self, state_dir, event_id):
            assert state_dir == tmp_path
            self.service = Service(store, {"event_id": event_id, "writable": True, "now": NOW})

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def call_sync(self, name, arguments):
            if host_callback is not None:
                return host_callback(self.service, name, arguments)
            return self.service.call(name, arguments)

    return ReplyGenerator(store, state_dir=tmp_path, provider_factory=lambda _: provider,
                          host_factory=Host, clock=lambda: NOW)


def record(store, key, text, item, status):
    event = source(key, text, timestamp=NOW - 10)
    store.begin_request(event)
    result = Service(store, {"event_id": event["event_id"], "writable": True, "now": NOW}).call(
        "report_offer_status", {"item": item, "status": status, "evidence": text})
    assert result["ok"] and result["recorded"]
    return result


def test_plain_generation_needs_no_archive_sender_or_publication(store, tmp_path, monkeypatch):
    event = source()
    request = store.begin_request(event)["request"]
    monkeypatch.setattr(store, "queue_reply", lambda *args, **kwargs: pytest.fail("generator cannot publish"))
    calls = []

    def complete(messages, instructions, tools, handler, metadata):
        calls.append(metadata)
        assert metadata["_required_read_tool"] is None and metadata["_required_report"] is None
        assert messages[-1] == {"role": "user", "content": "你好"}
        return "在呢。"

    provider = Provider(complete)
    progress = {"timings": {}, "event": {"private": "not a metric"}, "future": object()}
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, [], progress=progress))
    assert isinstance(result, GenerationResult) and result.text == "在呢。"
    assert provider.closed and len(calls) == 1
    assert result.evidence["request"]["event_id"] == event["event_id"]
    assert result.evidence["request"]["source_event"] == request["event"]
    assert result.evidence["required_read"] is None and result.evidence["required_report"] is None
    assert result.evidence["tool_results"] == []
    assert result.metrics["used_mcp"] is False
    assert "not a metric" not in json.dumps(result.metrics)
    assert store.reply(event["event_id"])["status"] == "awaiting_generation"
    assert {path.name for path in tmp_path.iterdir()} == {"bot"}
    event["native_source"]["server_id"] = "changed-after-generation"
    request["event"]["text"] = "changed-after-generation"
    progress["timings"]["provider_close"] = -1
    assert result.evidence["request"]["source_event"]["text"] == "你好"
    assert result.evidence["request"]["source_event"]["native_source"]["server_id"] != "changed-after-generation"
    assert result.metrics["timings"]["provider_close"] >= 0
    json.dumps({"evidence": result.evidence, "metrics": result.metrics})


def test_query_freezes_complete_current_facts_and_private_receipts(store, tmp_path):
    saved = {
        "fun": record(store, "fun", "今天娱乐首单已无", "fun", "unavailable"),
        "rank": record(store, "rank", "今天竞技首单可用", "rank", "available"),
    }
    event = source("query", "查询全部项目今天状态")
    request = store.begin_request(event)["request"]
    attempted = []

    def host_call(service, name, arguments):
        attempted.append(name)
        if len(attempted) == 1:
            return {"ok": False, "error": "private synthetic failure"}
        return service.call(name, arguments)

    def complete(messages, instructions, tools, handler, metadata):
        assert set(metadata["_required_read_items"]) == {"fun", "rank"}
        assert metadata["_required_read_day"] == DAY
        assert not handler("query_offer_status", {"date": DAY})["ok"]
        arguments = {"date": DAY}
        output = handler("query_offer_status", arguments)
        # Model-side mutation cannot change either grounding or its evidence.
        output["items"][0]["version"] = 999
        output["items"][0]["status"] = "available"
        arguments["date"] = "2026-10-28"
        return "都可用"

    result = asyncio.run(generator(store, tmp_path, Provider(complete), host_callback=host_call).generate(event, request, []))
    assert result.text == "查询结果：2026-10-29，娱乐首单已无；竞技首单可用。"
    evidence = result.evidence
    assert evidence["required_read"] == {"tool": "query_offer_status", "items": ["fun", "rank"],
                                          "day": DAY, "default_day": DAY}
    assert evidence["request"]["business_id"] == "shop"
    assert evidence["request"]["event_id"] == event["event_id"]
    for item, receipt in saved.items():
        fact = evidence["read_facts"][item]
        assert fact["version"] == 1 and fact["day"] == DAY
        assert fact["status"] == receipt["status"]
        assert fact["message_ref"] == "protocol:" + item
    assert evidence["report_facts"] == {}
    assert len(evidence["tool_results"]) == 1
    tool = evidence["tool_results"][0]
    assert tool["index"] == 2 and tool["name"] == "query_offer_status"
    assert tool["arguments"] == {"date": DAY}
    assert tool["result"]["business_id"] == "shop"
    assert {row["item_id"]: row for row in tool["result"]["items"]} == evidence["read_facts"]
    assert result.metrics["required_read_satisfied"] is True
    assert len(result.metrics["tool_calls"]) == 2
    assert "private synthetic failure" not in json.dumps(result.metrics)
    assert "message_ref" not in json.dumps(result.metrics)


def test_report_preserves_committed_versions_and_bound_source_for_every_item(store, tmp_path):
    event = source("report", "今天娱乐首单已无；竞技首单可用；请报备")
    request = store.begin_request(event)["request"]

    def complete(messages, instructions, tools, handler, metadata):
        requirement = metadata["_required_report"]
        assert requirement["event_id"] == event["event_id"]
        for item in requirement["items"]:
            result = handler("report_offer_status", {"item": item["item_id"],
                "status": item["status"], "evidence": event["text"]})
            assert result["ok"] and result["recorded"]
            result["version"] = 900
        return "报备没成功"

    result = asyncio.run(generator(store, tmp_path, Provider(complete)).generate(event, request, []))
    assert result.text == "已记录：2026-10-29，娱乐首单已无；竞技首单可用。"
    assert result.evidence["required_report"] == {"event_id": event["event_id"], "day": DAY,
        "items": [{"item_id": "fun", "status": "unavailable"}, {"item_id": "rank", "status": "available"}]}
    facts = result.evidence["report_facts"]
    assert set(facts) == {"fun", "rank"}
    assert all(row["version"] == 1 and row["event_id"] == event["event_id"] and row["recorded"]
               for row in facts.values())
    assert len(result.evidence["tool_results"]) == 2
    for tool in result.evidence["tool_results"]:
        assert tool["result"] == facts[tool["result"]["item_id"]]
        assert tool["arguments"]["evidence"] == event["text"]
    assert len([row for row in store.audit() if row["kind"] == "report"]) == 2
    assert store.reply(event["event_id"])["status"] == "awaiting_generation"


def test_successful_optional_tool_results_are_retained_without_inventing_a_requirement(store, tmp_path):
    event = source("optional", "请列出项目")
    request = store.begin_request(event)["request"]

    def complete(messages, instructions, tools, handler, metadata):
        assert metadata["_required_read_tool"] is None
        assert handler("list_offer_items", {})["ok"]
        assert handler("query_offer_status", {"item": "fun", "date": DAY})["ok"]
        return "有娱乐首单和竞技首单。"

    result = asyncio.run(generator(store, tmp_path, Provider(complete)).generate(event, request, []))
    assert result.evidence["required_read"] is None
    assert result.evidence["read_facts"] == {}
    assert [tool["name"] for tool in result.evidence["tool_results"]] == ["list_offer_items", "query_offer_status"]
    assert result.evidence["tool_results"][1]["result"]["items"][0]["version"] == 0


@pytest.mark.parametrize("key,value", [("event_id", "other"), ("message_ref", "other"),
    ("group_ref", "group_b"), ("sender_id", "wxid_other"), ("text", "different"),
    ("create_time", NOW - 86400), ("occurred_at", NOW - 86400)])
def test_mismatched_admission_identity_never_reaches_provider(store, tmp_path, key, value):
    event = source()
    request = store.begin_request(event)["request"]
    changed = copy.deepcopy(event)
    changed[key] = value
    runner = ReplyGenerator(store, state_dir=tmp_path,
        provider_factory=lambda _: pytest.fail("identity rejected before model"))
    with pytest.raises(ValueError, match="bot_context_mismatch"):
        asyncio.run(runner.generate(changed, request, []))


@pytest.mark.parametrize("text", [None, "", "   ", "x" * 4001])
def test_invalid_final_cannot_be_presented_as_freezable_result(store, tmp_path, text):
    event = source()
    request = store.begin_request(event)["request"]
    provider = Provider(lambda *args: text)
    with pytest.raises(ValueError, match="bot_final_response_invalid"):
        asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert provider.closed
    assert store.reply(event["event_id"])["status"] == "awaiting_generation"
