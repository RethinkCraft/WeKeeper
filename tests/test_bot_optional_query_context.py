"""Current-source relevance after a real two-round, optional MCP query.

The regression came from runtime_01_chat in the private lock-v2 benchmark.
These sources/identities are synthetic; no model or transport is opened.
"""
import asyncio
from datetime import datetime
import json

import pytest

from chatlog_keeper.bot.reply_generator import _ground_freeform_business
from chatlog_keeper.bot.service import Service
from test_bot_reply_generator import generator, source, store
from test_bot_required_read import call, compatible, response


DAY = "2026-10-30"
NOW = datetime.fromisoformat(DAY + "T10:00:00+08:00").timestamp()


def query_after_chat(store, tmp_path, text, *, final, item="fun", quote=None):
    for key, label, status in [("fun", "娱乐首单", "unavailable"), ("rank", "竞技首单", "available")]:
        prior = source("prior-" + key, "今天" + label + ("已无" if status == "unavailable" else "可用"), timestamp=NOW - 10)
        store.begin_request(prior)
        result = Service(store, {"event_id": prior["event_id"], "writable": True, "now": NOW}).call(
            "report_offer_status", {"item": key, "status": status, "evidence": prior["text"]})
        assert result["recorded"]
    event = source("current", text, timestamp=NOW)
    if quote is not None:
        event["native_source"]["quote"] = quote
    request = store.begin_request(event)["request"]
    bodies = []
    def remote(request):
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            return response({"tool_calls": [call(arguments={"item": item, "date": DAY})]})
        return response({"content": final})
    provider = compatible(remote)
    complete = provider.complete
    def original_loop(messages, instructions, tools, handler, metadata):
        # Run the identical two-round path on baseline, lock and optimized
        # source trees. Correctness here must not depend on early completion.
        metadata = dict(metadata); metadata.pop("_verified_tool_final", None)
        return complete(messages, instructions, tools, handler, metadata)
    provider.complete = original_loop
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == 2 and result.metrics["used_mcp"] is True
    assert len(result.evidence["tool_results"]) == 1
    assert result.evidence["tool_results"][0]["result"]["items"][0]["version"] == 1
    assert result.evidence.get("optional_read_facts") or result.evidence["read_facts"]
    assert store.receipt(event["event_id"]) is None
    assert len([row for row in store.audit() if row["kind"] == "report"]) == 2
    return result


@pytest.mark.parametrize("final,expected", [
    ("查好了，2026-10-30 娱乐首单的状态是已无。", "我在呢，你说～"),
    ("我在呢～娱乐首单今天可用。", "我在呢～"),
])
def test_plain_chat_cannot_be_replaced_by_unrequested_valid_optional_facts(store, tmp_path, final, expected):
    result = query_after_chat(store, tmp_path, "在吗，接个话呀", final=final)
    assert result.text == expected
    assert result.evidence["freeform_business_guard"]["reason"] == "unrequested_business_claim_removed"
    assert result.evidence["freeform_business_guard"]["grounded_items"] == []


@pytest.mark.parametrize("text,item,expected", [
    ("那竞技呢", "rank", "竞技首单可用"),
    ("娱乐怎么样", "fun", "娱乐首单已无"),
    ("现在首单呢", "fun", "娱乐首单已无"),
    ("那娱乐首单呢", "fun", "娱乐首单已无"),
])
def test_business_followups_keep_actual_current_optional_queries(store, tmp_path, text, item, expected):
    label = "娱乐首单" if item == "fun" else "竞技首单"
    result = query_after_chat(store, tmp_path, text, item=item, final=label + "现在已无。")
    assert result.text == "查询结果：" + DAY + "，" + expected + "。"


def native_quote(state="embedded"):
    return {"serverId": "12345678901234567890", "type": 1, "state": state,
            "senderId": "synthetic-peer", "embeddedText": "昨天娱乐首单可用。"}


def test_current_quote_locates_followup_subject_but_never_supplies_current_fact(store, tmp_path):
    result = query_after_chat(store, tmp_path, "那现在呢", quote=native_quote(), final="娱乐首单还是可用。")
    assert result.text == "查询结果：" + DAY + "，娱乐首单已无。"


@pytest.mark.parametrize("quote", [None, native_quote("recalled"), native_quote("missing")])
def test_unbound_followup_asks_for_project_instead_of_claiming_optional_fact(store, tmp_path, quote):
    result = query_after_chat(store, tmp_path, "那现在呢", quote=quote, final="娱乐首单现在已无。")
    assert result.text == "你是想问哪个项目的当前状态？"


def test_greeting_does_not_gain_business_authority_from_quote(store, tmp_path):
    result = query_after_chat(store, tmp_path, "在吗，接个话呀", quote=native_quote(), final="娱乐首单现在已无。")
    assert result.text == "我在呢，你说～"


@pytest.mark.parametrize("text", ["别聊娱乐首单了，陪我聊音乐", "不要查询首单，接个话呀",
                                "我只是说娱乐首单这个名字好听", "别聊娱乐首单了，你能不能讲个笑话"])
def test_negated_business_or_incidental_project_name_does_not_authorize_optional_facts(store, tmp_path, text):
    result = query_after_chat(store, tmp_path, text, final="我在呢～娱乐首单现在已无。")
    assert result.text == "我在呢～"
    assert result.evidence["freeform_business_guard"]["reason"] == "unrequested_business_claim_removed"


def test_named_project_does_not_authorize_facts_about_another_project(store, tmp_path):
    result = query_after_chat(store, tmp_path, "那竞技呢", item="fun", final="娱乐首单现在已无。")
    assert "娱乐首单已无" not in result.text
    assert result.evidence["freeform_business_guard"]["grounded_items"] == []


@pytest.mark.parametrize("identifier,text", [("game", "Let's play a game"),
    ("play", "I play with friends"), ("error", "An error occurred"), ("fun", "今天娱乐一下，放松点")])
def test_generic_ids_and_daily_alias_mentions_do_not_authorize_business_facts(identifier, text):
    business = {"items": [{"id": identifier, "name": "娱乐首单", "aliases": ["娱乐"]}]}
    facts = {identifier: {"item_id": identifier, "status": "unavailable", "day": DAY, "version": 1}}
    final, guard = _ground_freeform_business("娱乐首单今天已无。", facts, business, DAY, source_text=text)
    assert final == "我在呢，你说～" and guard["grounded_items"] == []
