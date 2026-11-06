"""Current-request read gate: fake transports, hosts and synthetic stores only."""
import asyncio
from datetime import datetime, timezone
import json
import time

import httpx
import pytest

from chatlog_keeper.bot.providers import ChatGPTProvider, CompatibleProvider, ProviderError, read_tool_succeeded
from chatlog_keeper.bot.service import tool_schemas
from chatlog_keeper.bot.store import business_day
from chatlog_keeper.bot.reply_generator import required_status_day, required_status_read


BUSINESS = {"items": [
    {"id": "fun", "name": "娱乐首单", "aliases": ["娱乐"]},
    {"id": "rank", "name": "竞技首单", "aliases": ["竞技"]},
]}
TOOLS = [{**tool["function"]} for tool in tool_schemas()]
DAY = "2026-10-28"
YESTERDAY = "2026-10-27"


def query_result(item="fun", day=DAY):
    return {"ok": True, "day": day, "items": [{"item_id": item, "status": "unknown", "day": day}]}


@pytest.mark.parametrize("text,expected", [
    ("@Chris 现在娱乐首单是什么状态", ("fun",)),
    ("娱乐首单现在状态？", ("fun",)),
    ("娱乐首单状态是什么", ("fun",)),
    ("娱乐首单现在怎么样", ("fun",)),
    ("请查询“娱乐首单”的状态", ("fun",)),
    ("帮我查一下‘娱乐’的当前状态", ("fun",)),
    ("娱乐今天还有吗", ("fun",)),
    ("不要报备竞技首单已无，只查当前状态", ("rank",)),
    ("听说娱乐已无，帮我查一下", ("fun",)),
    ("查全部首单状态", ("fun", "rank")),
    ("查询全部项目状态", ("fun", "rank")),
    ("查一下所有项目当前状态", ("fun", "rank")),
    ("查娱乐和竞技", ("fun", "rank")),
    ("娱乐首单今天已无", None),
    ("今天聊点娱乐吧", None),
    ("娱乐圈查理的新歌好听", None),
    ("娱乐圈现在怎么样", None),
    ("娱乐首单现在怎么样才能玩得开心", None),
    ("请解释‘娱乐首单现在怎么样’这句话", None),
    ("所有项目当前状态不错", None),
    ("不需要查询全部项目状态", None),
    ("帮我查一下所有项目的设计稿", None),
    ("请解释'查娱乐首单'这句话", None),
    ("他说娱乐首单现在是什么状态", None),
    ("听说娱乐首单已无，帮我查天气", None),
    ("娱乐首单今天已无，我想查一下快递", None),
    ("不要查询娱乐首单", None),
    ("不需要查询娱乐首单", None),
    ("别查娱乐首单，只查竞技首单", ("rank",)),
    ("> 查娱乐首单\n今天开心吗", None),
    ("假设查询娱乐首单", None),
    ("查未知项目状态", None),
])
def test_gate_uses_only_explicit_current_business_read(text, expected):
    assert required_status_read(text, BUSINESS) == expected


class Ledger:
    def __init__(self):
        self.metadata = None
        self.finished = None
    def start_run(self, **metadata):
        self.metadata = metadata
        assert not any(key.startswith("_") for key in metadata)
        return "synthetic"
    def record_call(self, *args, **kwargs):
        pass
    def finish_run(self, *args, **kwargs):
        self.finished = kwargs


def compatible(remote, *, clock=time.monotonic):
    # Never read credentials, even fake ones: only exercise the tool loop.
    provider = object.__new__(CompatibleProvider)
    provider.profile = {"kind": "deepseek", "model": "synthetic", "tools_verified": True}
    provider.clock, provider.key, provider.url = clock, "fake-test-key", "https://synthetic.invalid/chat/completions"
    provider.http = httpx.Client(transport=httpx.MockTransport(remote))
    provider.owned, provider.own_ledger, provider.ledger = True, False, Ledger()
    return provider


def response(message):
    return httpx.Response(200, json={"choices": [{"message": message}]})


def call(name="query_offer_status", item="fun", number=1, arguments=None):
    return {"id": "call-" + str(number), "type": "function", "function": {
        "name": name, "arguments": json.dumps({"item": item} if arguments is None else arguments)}}


def gate(items=("fun",), day=DAY):
    return {"_required_read_tool": "query_offer_status", "_required_read_items": items,
            "_required_read_day": day, "_required_read_default_day": DAY}


def test_ignored_tool_choice_final_is_repaired_in_same_loop(tmp_path):
    bodies, executions = [], []
    def remote(request):
        body = json.loads(request.content)
        bodies.append(body)
        if len(bodies) == 1:
            return response({"content": "历史说可用，所以可用。"})
        if len(bodies) == 2:
            return response({"content": None, "tool_calls": [call()]})
        return response({"content": "2026-10-28 娱乐首单未报备。"})
    provider = compatible(remote)
    try:
        result = provider.complete([], "人设", TOOLS,
            lambda name, args: executions.append((name, args)) or query_result(), gate())
        assert "未报备" in result and len(bodies) == 3
        assert executions == [("query_offer_status", {"item": "fun"})]
        for body in bodies[:2]:
            assert body["tool_choice"] == {"type": "function", "function": {"name": "query_offer_status"}}
            assert [tool["function"]["name"] for tool in body["tools"]] == ["query_offer_status"]
        assert "tool_choice" not in bodies[2]
        assert "历史说可用" not in json.dumps(bodies[1]["messages"], ensure_ascii=False)
        assert provider.ledger.finished["status"] == "completed"
    finally:
        provider.close()


@pytest.mark.parametrize("tool_result", [
    {"ok": False}, {"ok": "true"}, query_result("rank"),
])
def test_failed_or_wrong_item_read_cannot_ground_final(tool_result):
    bodies, executions = [], []
    def remote(request):
        bodies.append(json.loads(request.content))
        return response({"content": None, "tool_calls": [call()]}) if len(bodies) == 1 else response({"content": "猜测成功"})
    provider = compatible(remote)
    try:
        with pytest.raises(ProviderError, match="bot_required_read_not_satisfied"):
            provider.complete([], "", TOOLS, lambda *args: executions.append(args) or tool_result, gate())
        assert len(bodies) == 4 and len(executions) == 1
        assert provider.ledger.finished["status"] == "failed"
    finally:
        provider.close()


def test_separate_queries_accumulate_coverage_without_replaying_writes():
    executions, bodies = [], []
    def remote(request):
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            return response({"tool_calls": [call(item="fun"), call(item="rank", number=2)]})
        if len(bodies) == 2:
            return response({"tool_calls": [call("report_offer_status", number=3)]})
        return response({"content": "查询与获授权报备均已完成"})
    provider = compatible(remote)
    def handler(name, args):
        executions.append(name)
        return query_result(args["item"]) if name == "query_offer_status" else {"ok": True, "recorded": True}
    try:
        assert provider.complete([], "", TOOLS, handler, gate(("fun", "rank")))
        assert executions == ["query_offer_status", "query_offer_status", "report_offer_status"]
    finally:
        provider.close()


def test_pending_read_rejects_entire_unexpected_write_batch():
    provider = compatible(lambda _: response({"tool_calls": [call(), call("report_offer_status", number=2)]}))
    try:
        with pytest.raises(ProviderError, match="bot_unapproved_tool_call"):
            provider.complete([], "", TOOLS, lambda *_: pytest.fail("batch must be checked before execution"), gate())
    finally:
        provider.close()


def test_read_repair_never_extends_deadline():
    now, bodies = [0], []
    def remote(request):
        bodies.append(json.loads(request.content))
        now[0] = 60
        return response({"content": "不用查询也能回答"})
    provider = compatible(remote, clock=lambda: now[0])
    try:
        with pytest.raises(ProviderError, match="bot_provider_timeout"):
            provider.complete([], "", TOOLS, lambda *_: pytest.fail("no tools"), {**gate(), "_timeout": 60})
        assert len(bodies) == 1
    finally:
        provider.close()


@pytest.mark.parametrize("size,expected", [(1, 3), (3, 9)])
def test_tool_round_and_call_limits_are_preserved(size, expected):
    bodies, executions = [], []
    def remote(request):
        bodies.append(json.loads(request.content))
        return response({"tool_calls": [call(number=n) for n in range(size)]})
    provider = compatible(remote)
    try:
        with pytest.raises(ProviderError, match="bot_tool_round_limit"):
            provider.complete([], "", TOOLS, lambda *args: executions.append(args) or {"ok": False}, gate())
        assert len(bodies) == 4 and len(executions) == expected
    finally:
        provider.close()


def test_tenth_tool_call_is_not_executed():
    count = []
    provider = compatible(lambda _: response({"tool_calls": [call(number=n) for n in range(10)]}))
    try:
        with pytest.raises(ProviderError, match="bot_unapproved_tool_call"):
            provider.complete([], "", TOOLS, lambda *args: count.append(args) or {"ok": False}, gate())
        assert len(count) == 9
    finally:
        provider.close()


def test_plain_chat_has_no_forced_tool_or_execution():
    def remote(request):
        body = json.loads(request.content)
        assert "tool_choice" not in body
        return response({"content": "在呢"})
    provider = compatible(remote)
    try:
        assert provider.complete([], "人设", TOOLS, lambda *_: pytest.fail("chat never calls tools"), {}) == "在呢"
    finally:
        provider.close()


def test_chatgpt_reuses_existing_entrypoint_but_rejects_missing_required_read(tmp_path):
    class Client:
        state_dir = tmp_path
        def bot_chat(self, *args, **kwargs):
            assert not any(key.startswith("_") for key in kwargs["run_metadata"])
            return {"output_text": "历史替代查询"}
    provider = ChatGPTProvider(tmp_path, {"kind": "siwc"}, client=Client())
    with pytest.raises(ProviderError, match="bot_required_read_not_satisfied"):
        provider.complete([], "人设", TOOLS, lambda *_: pytest.fail("no calls"), gate())
    assert not (tmp_path / "bot-chat-waiting").exists()














@pytest.mark.parametrize("text,expected", [
    ("查询娱乐首单状态", DAY),
    ("查询今天娱乐首单状态", DAY),
    ("查询昨天娱乐首单状态", YESTERDAY),
    ("查询昨日娱乐首单状态", YESTERDAY),
    ("查询前天娱乐首单状态", "2026-10-26"),
    ("查询明天娱乐首单状态", "2026-10-29"),
    ("查询后天娱乐首单状态", "2026-10-30"),
    ("查询2026-10-20娱乐首单状态", "2026-10-20"),
    ("查询‘2026-10-27’的娱乐首单状态", YESTERDAY),
    ("查询‘昨天’娱乐首单状态", YESTERDAY),
    ("昨天(2026-10-27)娱乐首单状态是什么", YESTERDAY),
    ("听说昨天娱乐已无，帮我查一下", DAY),
    ("引用‘昨天娱乐首单已无’，查询今天娱乐首单状态", DAY),
])
def test_current_request_date_resolution(text, expected):
    assert required_status_day(text, BUSINESS, DAY) == expected


@pytest.mark.parametrize("text", [
    "查询今天和昨天娱乐首单状态", "查询2026-10-27到2026-10-28娱乐首单状态",
    "查询‘2026-10-27到2026-10-28’的娱乐首单状态", "查询‘今天和昨天’的娱乐首单状态",
    "查询‘2026-10-27至2026-10-28’的娱乐首单状态",
    "查上周娱乐首单状态", "查前几天娱乐首单状态", "查最近娱乐首单状态",
    "查大前天娱乐首单状态", "查昨天的前一天娱乐首单状态", "查上个月今天娱乐首单状态",
    "查10月27日娱乐首单状态", "查‘10月27日’娱乐首单状态", "查2026/10/27娱乐首单状态",
    "查2026-02-30娱乐首单状态", "查2026-2-3娱乐首单状态", "查2026-100-28娱乐首单状态",
    "查10-27娱乐首单状态", "查‘上周’娱乐首单状态", "查星期二娱乐首单状态",
])
def test_ambiguous_or_unsupported_date_is_never_silently_today(text):
    with pytest.raises(ProviderError, match="bot_required_read_date_ambiguous"):
        required_status_day(text, BUSINESS, DAY)


@pytest.mark.parametrize("arguments,output,expected_day,accepted", [
    ({}, query_result(), DAY, True),
    ({"date": DAY}, query_result(), DAY, True),
    ({"date": YESTERDAY}, query_result(day=YESTERDAY), YESTERDAY, True),
    ({"day": YESTERDAY}, query_result(day=YESTERDAY), YESTERDAY, True),
    ({}, query_result(day=YESTERDAY), DAY, False),
    ({"date": YESTERDAY}, query_result(), DAY, False),
    ({"date": DAY, "day": YESTERDAY}, query_result(), DAY, False),
    ({"date": None}, query_result(), DAY, False),
    ({}, query_result(day=YESTERDAY), YESTERDAY, False),
    ({}, {**query_result(), "day": YESTERDAY}, DAY, False),
    ({}, {**query_result(), "items": [{"item_id": "fun", "day": YESTERDAY}]}, DAY, False),
    ({}, {**query_result(), "items": [{"item_id": "fun"}]}, DAY, False),
    ({}, {"ok": True, "items": [{"item_id": "fun", "day": DAY}]}, DAY, False),
    ({}, {**query_result(), "items": [*query_result()["items"], {"item_id": "rank", "day": YESTERDAY}]}, DAY, False),
])
def test_actual_query_arguments_and_every_returned_day_must_match(arguments, output, expected_day, accepted):
    covered = set()
    assert read_tool_succeeded("query_offer_status", output, "query_offer_status", ("fun",), covered,
        arguments=arguments, expected_day=expected_day, default_day=DAY) is accepted
    assert bool(covered) is accepted


def test_wrong_date_revokes_previously_successful_coverage():
    covered = set()
    assert read_tool_succeeded("query_offer_status", query_result(), "query_offer_status", ("fun",), covered,
        arguments={}, expected_day=DAY, default_day=DAY)
    assert not read_tool_succeeded("query_offer_status", query_result(day=YESTERDAY), "query_offer_status", ("fun",), covered,
        arguments={"date": YESTERDAY}, expected_day=DAY, default_day=DAY)
    assert covered == set()


@pytest.mark.parametrize("field,value", [("_required_read_day", None), ("_required_read_default_day", None),
    ("_required_read_day", "2026-02-30"), ("_required_read_day", "2026-2-3")])
def test_gate_date_metadata_is_mandatory_and_private(field, value):
    provider = compatible(lambda _: pytest.fail("invalid host gate must fail before transport"))
    try:
        with pytest.raises(ProviderError, match="bot_invalid_required_read_date"):
            provider.complete([], "", TOOLS, lambda *_: None, {**gate(), field: value})
    finally:
        provider.close()


def test_wrong_date_repair_stays_in_same_decreasing_deadline():
    now, requests, timeouts, executions = [0], [], [], []
    def remote(request):
        requests.append(json.loads(request.content))
        timeouts.append(request.extensions["timeout"]["read"])
        if len(requests) <= 2:
            selected = YESTERDAY if len(requests) == 1 else DAY
            now[0] += 20
            return response({"tool_calls": [call(arguments={"item": "fun", "date": selected})]})
        return response({"content": "2026-10-28娱乐首单未报备"})
    provider = compatible(remote, clock=lambda: now[0])
    def handler(name, arguments):
        executions.append((name, arguments["date"]))
        return query_result(day=arguments["date"])
    try:
        assert provider.complete([], "", TOOLS, handler, {**gate(), "_timeout": 60})
        assert timeouts == [60, 40, 20]
        assert executions == [("query_offer_status", YESTERDAY), ("query_offer_status", DAY)]
        assert all("tool_choice" in request for request in requests[:2])
        assert "tool_choice" not in requests[2]
    finally:
        provider.close()


def test_later_wrong_date_cannot_reuse_a_successful_gate():
    requests = []
    def remote(request):
        requests.append(json.loads(request.content))
        if len(requests) <= 2:
            return response({"tool_calls": [call(arguments={"date": DAY if len(requests) == 1 else YESTERDAY})]})
        return response({"content": "错误日期结论"})
    provider = compatible(remote)
    try:
        with pytest.raises(ProviderError, match="bot_required_read_not_satisfied"):
            provider.complete([], "", TOOLS, lambda name, arguments: query_result(day=arguments["date"]), gate())
        assert len(requests) == 4 and "tool_choice" in requests[2]
    finally:
        provider.close()
