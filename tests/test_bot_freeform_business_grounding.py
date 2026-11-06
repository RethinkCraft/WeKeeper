"""Current business assertions in freeform replies need this request's facts."""
import asyncio
import copy

import pytest

from test_bot_reply_generator import DAY, NOW, Provider, generator, source, store
from test_bot_service import config
from chatlog_keeper.bot.reply_generator import _current_no_report, _ground_freeform_business, _optional_query_facts

BUSINESS = config()["businesses"][0]


def test_chat_does_not_restate_historical_report_as_current_verified_fact(store, tmp_path, monkeypatch):
    history = [{"role": "user", "content": "今天娱乐首单已无，请报备"},
               {"role": "assistant", "content": "已记录：2026-10-29，娱乐首单已无。"},
               {"role": "user", "content": "查询今天娱乐首单"},
               {"role": "assistant", "content": "查询结果：2026-10-29，娱乐首单已无。"}]
    monkeypatch.setattr(store, "history", lambda *args, **kwargs: list(history))
    event = source("live-chat-regression", "@Chris\u2005在吗，接个话呀")
    request = store.begin_request(event)["request"]
    nearby = [{"message_ref": "prior-report", "sender_name": "synthetic", "create_time": NOW - 20,
               "text": "今天娱乐首单已无，请报备"}]
    calls = []
    def complete(messages, instructions, tools, handler, metadata):
        calls.append(metadata)
        assert history[1] in messages
        assert any("附近消息" in message["content"] for message in messages)
        assert metadata["_required_read_tool"] is None and metadata["_required_report"] is None
        return "我在的，你说～今天娱乐首单已经报备过了，状态是已无。"
    result = asyncio.run(generator(store, tmp_path, Provider(complete)).generate(event, request, nearby))
    assert result.text == "我在的，你说～"
    assert len(calls) == 1 and result.metrics["used_mcp"] is False
    assert result.evidence["read_facts"] == result.evidence["report_facts"] == {}
    assert result.evidence["tool_results"] == []
    assert result.evidence["freeform_business_guard"] == {"reason": "unverified_business_claim_removed",
        "removed_sentences": 1, "grounded_items": [],
        "original_final": "我在的，你说～今天娱乐首单已经报备过了，状态是已无。"}


@pytest.mark.parametrize("text", ["在呢，聊聊今天发生什么吧。", "我今天状态不错，你呢？", "今天娱乐一下，放松点～",
    "有娱乐首单和竞技首单。", "请问娱乐首单现在可用吗？", "娱乐首单是什么状态？",
    "我还没核实娱乐首单现在是否可用。", "你刚说的“娱乐首单已无”，我看到了。"])
def test_pure_chat_and_explicit_nonassertions_are_unchanged(text):
    assert _ground_freeform_business(text, {}, BUSINESS, DAY) == (text, None)


@pytest.mark.parametrize("claim", ["今天娱乐首单已无。", "娱乐首单不是可用。", "娱乐首单还有一个名额。",
    "娱乐首单还剩1次。", "今天娱乐已经登记好了。", "娱乐首单状态是‘已无’。",
    "娱乐首单已无，你知道吗？", "娱乐首单已无你知道吗？", "娱乐首单已无。不过我没有核实过。",
    "娱乐首单。它还是已无。", "已经报备好了，状态是已无。"])
def test_pure_unverified_assertions_and_dependent_tail_are_removed(claim):
    final, guard = _ground_freeform_business("我在呢～" + claim, {}, BUSINESS, DAY)
    assert "已无" not in final and "可用" not in final and "登记" not in final and "报备" not in final
    assert "剩1次" not in final and "一个名额" not in final
    assert final.startswith("我在呢～")
    assert guard["reason"] == "unverified_business_claim_removed"


def query_result(*, business_id="shop", day=DAY, status="unavailable"):
    return {"ok": True, "business_id": business_id, "day": day, "items": [
        {"business_id": business_id, "item_id": "fun", "day": day, "status": status, "version": 1}]}


@pytest.mark.parametrize("change", ["failed", "wrong_business", "wrong_row_business", "wrong_day", "missing_version",
                                   "wrong_item", "wrong_row_day", "duplicate_item", "invalid_status"])
def test_pure_optional_query_snapshot_rejects_invalid_scope_or_evidence(change):
    output = query_result()
    if change == "failed": output["ok"] = False
    elif change == "wrong_business": output["business_id"] = "other"
    elif change == "wrong_row_business": output["items"][0]["business_id"] = "other"
    elif change == "wrong_day": output["day"] = "2026-10-28"
    elif change == "wrong_row_day": output["items"][0]["day"] = "2026-10-28"
    elif change == "missing_version": del output["items"][0]["version"]
    elif change == "wrong_item": output["items"][0]["item_id"] = "rank"
    elif change == "duplicate_item": output["items"].append(copy.deepcopy(output["items"][0]))
    elif change == "invalid_status": output["items"][0]["status"] = "invented"
    assert _optional_query_facts(output, {"item": "fun", "date": DAY}, BUSINESS, DAY) == {}


def test_pure_optional_valid_query_renders_only_current_request_facts_and_never_claims_a_write():
    facts = _optional_query_facts(query_result(), {"item": "fun", "date": DAY}, BUSINESS, DAY)
    final, guard = _ground_freeform_business("我在呢～娱乐首单已经报备了，还是可用的。", facts, BUSINESS, DAY)
    assert final == "我在呢～查询结果：2026-10-29，娱乐首单已无。"
    assert guard["reason"] == "business_claim_grounded_from_current_query" and guard["grounded_items"] == ["fun"]
    wrong_day = _optional_query_facts(query_result(day="2026-10-28"), {"item": "fun", "date": "2026-10-28"}, BUSINESS, DAY)
    assert wrong_day == {}


def test_pure_only_unverified_business_output_has_explicit_uncertainty_fallback():
    final, guard = _ground_freeform_business("娱乐首单状态已无。", {}, BUSINESS, DAY)
    assert final == "这条业务信息我还没有核实，需要查询后才能确认。"
    assert guard["reason"] == "unverified_business_claim_removed"


def test_pure_conditional_sentence_cannot_leave_a_dangling_write_promise_or_date():
    original = "明白，竞技首单我不动。只要你说娱乐首单已无，我就记，2026-10-29。"
    final, guard = _ground_freeform_business(original, {}, BUSINESS, DAY)
    assert final == "明白，竞技首单我不动。"
    assert guard["original_final"] == original


def no_report_inputs(text, sender="wxid_manager"):
    event = source("no-write", text, sender=sender)
    request = {**event, "business_id": "shop"}
    binding = {"enabled": True, "business_id": "shop", "business": copy.deepcopy(BUSINESS)}
    return event, request, binding


def test_pure_no_write_permission_reason_requires_current_binding_proof():
    args = no_report_inputs("今天娱乐首单恢复可用，请报备", sender="wxid_visitor")
    text, evidence = _current_no_report(*args, None, [])
    assert text == "当前账号没有该业务的报备权限，本次没有登记。"
    assert evidence["reason"] == "report_not_authorized" and evidence["recorded"] is False
    for changed in ({**args[2], "business_id": "other"}, {**args[2], "enabled": False},
                    {**args[2], "business": {"id": "shop"}}):
        assert _current_no_report(args[0], args[1], changed, None, []) is None


@pytest.mark.parametrize("text", ["不要把竞技首单报成已无，我只是问问", "有人说‘娱乐首单恢复可用’，这句话是在引用，先别登记"])
def test_pure_negated_or_quoted_no_write_does_not_assert_project_unreported(text):
    final, evidence = _current_no_report(*no_report_inputs(text), None, [])
    assert final == "明白，本次没有登记。"
    assert "未报备" not in final and "可用" not in final and "已无" not in final
    assert evidence["reason"] == "non_affirmative_source"


@pytest.mark.parametrize("receipt,attempts", [({"recorded": True}, []), ({"recorded": False}, []),
    (None, [{"ok": True, "recorded": True}]), (None, [{"ok": False, "error": "transport_result_unknown"}])])
def test_pure_no_write_cannot_hide_a_prior_receipt_or_ambiguous_tool_result(receipt, attempts):
    assert _current_no_report(*no_report_inputs("不要把竞技首单报成已无，我只是问问"), receipt, attempts) is None


def test_pure_no_write_accepts_only_definite_rejection_and_does_not_capture_generic_chat():
    final, evidence = _current_no_report(*no_report_inputs("今天娱乐首单可用，请报备", sender="wxid_visitor"), None,
        [{"ok": False, "error": "report_not_authorized"}])
    assert "报备权限" in final and evidence["tool_rejections"] == ["report_not_authorized"]
    assert _current_no_report(*no_report_inputs("今天记录了什么有趣的事"), None, []) is None


@pytest.mark.parametrize("text,sender,original,expected", [
    ("今天娱乐首单恢复可用，请报备", "wxid_visitor", "你不是授权人，这条无法登记，请找授权人报备。", "当前账号没有该业务的报备权限，本次没有登记。"),
    ("不要把竞技首单报成已无，我只是问问", "wxid_manager", "明白，竞技首单我不动。只要你说竞技首单已无，我就记，2026-10-29。", "明白，本次没有登记。"),
    ("有人说‘娱乐首单恢复可用’，这句话是在引用，先别登记", "wxid_manager", "我看到了，这句是引用，先不登记。我随时可以帮你查。", "明白，本次没有登记。"),
])
def test_current_no_write_integration_has_clear_source_bound_reason_and_private_original(store, tmp_path, text, sender, original, expected):
    event = source("no-write-integration", text, sender=sender)
    request = store.begin_request(event)["request"]
    calls = []
    def complete(messages, instructions, tools, handler, metadata):
        calls.append(metadata)
        assert metadata["_required_report"] is None
        return original
    result = asyncio.run(generator(store, tmp_path, Provider(complete)).generate(event, request, []))
    assert result.text == expected and len(calls) == 1
    assert result.evidence["current_report_outcome"]["recorded"] is False
    assert result.evidence["freeform_business_guard"]["original_final"] == original
    assert result.evidence["tool_results"] == [] and result.metrics["used_mcp"] is False
    assert store.receipt(event["event_id"]) is None
    assert original not in str(result.metrics)


@pytest.mark.parametrize("kind", ["valid", "wrong_business", "wrong_day", "later_failed"])
def test_optional_query_integration_uses_only_valid_current_snapshot(store, tmp_path, kind):
    event = source("optional-" + kind, "现在首单呢")
    request = store.begin_request(event)["request"]
    def host_call(service, name, arguments):
        assert name == "query_offer_status"
        return {"ok": False} if arguments.get("item") == "rank" else query_result(
            business_id="other" if kind == "wrong_business" else "shop",
            day="2026-10-28" if kind == "wrong_day" else DAY)
    def complete(messages, instructions, tools, handler, metadata):
        assert metadata["_required_read_tool"] is None
        handler("query_offer_status", {"item": "fun", "date": DAY})
        if kind == "later_failed":
            handler("query_offer_status", {"item": "rank", "date": DAY})
        return "我在呢～娱乐首单今天可用。"
    result = asyncio.run(generator(store, tmp_path, Provider(complete), host_callback=host_call).generate(event, request, []))
    if kind == "valid":
        assert result.text == "我在呢～查询结果：2026-10-29，娱乐首单已无。"
        assert result.evidence["optional_read_facts"]["fun"]["version"] == 1
    else:
        assert result.text == "我在呢～"
        assert not result.evidence.get("optional_read_facts")
    assert result.evidence["required_read"] is None and result.evidence["required_report"] is None
    assert result.metrics["used_mcp"] is True
