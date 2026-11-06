"""A comma-list query must retain each explicitly requested status item.

The provider transport is fake; the query uses the real Service and its one
SQLite snapshot. No model, gateway, UI or production ledger is contacted.
"""
import asyncio

import pytest

from chatlog_keeper.bot.reply_generator import required_status_read
from test_bot_reply_generator import DAY, generator, record, source, store
from test_bot_required_read import BUSINESS, call
from test_bot_verified_tool_final import scripted


@pytest.mark.parametrize("text", [
    "查娱乐首单，竞技首单状态",
    "查娱乐首单,竞技首单的状态",
    "娱乐首单现在怎么样，竞技首单状态",
    "查娱乐首单，竞技首单当前状态",
    "查娱乐首单，娱乐和竞技的状态",
])
def test_only_complete_comma_item_status_lists_inherit_query_scope(text):
    assert required_status_read(text, BUSINESS) == ("fun", "rank")


@pytest.mark.parametrize("text,expected", [
    ("查娱乐首单；竞技首单状态", ("fun",)),
    ("查娱乐首单。竞技首单状态", ("fun",)),
    ("查娱乐首单\n竞技首单状态", ("fun",)),
    ("查娱乐首单，讲个笑话，竞技首单状态", ("fun",)),
    ("查娱乐首单，查天气，竞技首单状态", ("fun",)),
    ("查娱乐首单，别查竞技首单，竞技首单状态", ("fun",)),
    ("查娱乐首单，听说竞技首单状态，竞技首单状态", ("fun",)),
    ("查娱乐首单，‘竞技首单状态’，竞技首单状态", ("fun",)),
    ("查娱乐首单，竞技首单状态可用", ("fun",)),
    ("查娱乐首单，竞技首单已无", ("fun",)),
    ("查娱乐首单，竞技首单状态不错", ("fun",)),
    ("查娱乐首单，竞技首单状态设计稿", ("fun",)),
    ("别聊娱乐首单了，你能不能讲个笑话，竞技首单状态", None),
    ("听说查娱乐首单，竞技首单状态", None),
    ("不要查娱乐首单，竞技首单状态", None),
    ("查天气，竞技首单状态", None),
])
def test_query_inheritance_stops_at_unrelated_negative_or_sentence_boundaries(text, expected):
    assert required_status_read(text, BUSINESS) == expected


def test_comma_status_ellipsis_keeps_both_current_items_from_one_query(store, tmp_path):
    record(store, "seed-fun", "今天娱乐首单已无", "fun", "unavailable")
    record(store, "seed-rank", "今天竞技首单可用", "rank", "available")
    before_reports = store.conn.execute("SELECT count(*) FROM reports").fetchone()[0]
    before_audit = len(store.audit())
    event = source("ellipsis-query", "查娱乐首单，竞技首单状态")
    request = store.begin_request(event)["request"]
    queried = []

    def query(service, name, arguments):
        result = service.call(name, arguments)
        queried.append((name, arguments, result))
        return result

    provider, model_calls = scripted([[call(arguments={"date": DAY})]])
    result = asyncio.run(generator(store, tmp_path, provider, host_callback=query).generate(event, request, []))

    assert len(model_calls) == 1 and len(queried) == 1
    assert queried[0][0] == "query_offer_status"
    assert {row["item_id"] for row in queried[0][2]["items"]} == {"fun", "rank"}
    assert result.text == "查询结果：" + DAY + "，娱乐首单已无；竞技首单可用。"
    assert set(result.evidence["read_facts"]) == {"fun", "rank"}
    assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == before_reports
    assert len(store.audit()) == before_audit
    assert store.receipt(event["event_id"]) is None
