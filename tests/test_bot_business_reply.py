"""A final cannot invent a fact absent from the current request's receipts."""
import pytest

from chatlog_keeper.bot.business_reply import grounded_business_reply
from chatlog_keeper.bot.providers import ProviderError


DAY = "2026-10-29"
BUSINESS = {"items": [{"id": "fun", "name": "娱乐首单"}, {"id": "rank", "name": "竞技首单"}]}


def fact(item="fun", status="unavailable", day=DAY):
    return {"item_id": item, "status": status, "day": day}


@pytest.mark.parametrize("text", [
    "我查过了：2026-10-29 娱乐首单目前是无状态～",
    "2026-10-29 娱乐首单可用。",
    "2026-10-29 娱乐首单已无，但其实还有。",
    "不是2026-10-29 娱乐首单已无。",
    "昨天娱乐首单已无，今天能用。",
    "2026-10-28 娱乐首单已无。",
    "2026-10-29 竞技首单已无。",
    "如果2026-10-29 娱乐首单已无。",
])
def test_unverified_or_contradictory_prose_uses_only_actual_result(text):
    assert grounded_business_reply(text, {"fun": fact()}, BUSINESS, DAY) == "查询结果：2026-10-29，娱乐首单已无。"


def test_supported_natural_final_is_preserved():
    text = "查好啦，2026-10-29 娱乐首单目前已无～"
    assert grounded_business_reply(text, {"fun": fact()}, BUSINESS, DAY) == text


def test_multiple_items_keep_each_status_bound_to_its_own_id():
    facts = {"rank": fact("rank", "available"), "fun": fact()}
    assert grounded_business_reply("两个都可以用", facts, BUSINESS, DAY) == "查询结果：2026-10-29，娱乐首单已无；竞技首单可用。"


@pytest.mark.parametrize("facts", [{}, {"other": fact("other")}, {"fun": fact(day="2026-10-28")},
                                  {"fun": fact("rank")}, {"fun": fact(status="missing")}])
def test_incomplete_wrong_day_or_unbound_result_is_never_rendered(facts):
    with pytest.raises(ProviderError, match="bot_business_result_unverified"):
        grounded_business_reply("查好了", facts, BUSINESS, DAY)
