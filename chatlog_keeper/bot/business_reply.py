"""Keep business claims bound to the current tool receipt, without another LLM call."""
from __future__ import annotations

import re

from .providers import ProviderError


_LABELS = {"available": "可用", "unavailable": "已无", "unknown": "未确认"}
_WORDS = {
    "available": r"(?:可用|还有|仍然可用)",
    "unavailable": r"(?:已无|已经没有了|没有了|已经用完了|已用完)",
    "unknown": r"(?:未确认|未报备|尚未报备|状态不明)",
}


def grounded_business_reply(text, facts, business, day, *, recorded=False):
    """Keep an unambiguous short final, otherwise render verified facts.

    Arbitrary prose cannot be proven by looking for a convenient keyword.
    Only a complete, single-fact sentence is retained; multi-item answers or
    ambiguous/contradictory prose are replaced from the item-keyed receipts.
    The caller supplies only successful results from this request, never history.
    """
    names = {item["id"]: item["name"] for item in business.get("items", [])}
    if not facts or any(item not in names or not isinstance(row, dict)
                        or row.get("item_id") != item or row.get("day") != day
                        or row.get("status") not in _LABELS for item, row in facts.items()):
        raise ProviderError("bot_business_result_unverified")
    ordered = [(names[item], facts[item]["status"]) for item in names if item in facts]
    if len(ordered) == 1 and isinstance(text, str):
        name, status = ordered[0]
        prefix = (r"(?:记好啦|记好了|已记录|已经记录|已完成报备|已经记好了)" if recorded else
                  r"(?:查好啦|查好了|我查过了|查询结果|本次查询结果)")
        # Full matching rejects extra dates/items, denial, hypothetical claims,
        # historical comparisons and unsupported trailing explanations.
        pattern = (rf"(?:{prefix}[，,:：\s]*)?{re.escape(day)}[，,:：\s]*"
                   rf"{re.escape(name)}[\s]*(?:目前|当前|现在)?(?:状态)?(?:是|为)?[\s]*"
                   rf"{_WORDS[status]}(?:啦|哦|喔|呢)?[。！!~～\s]*")
        if re.fullmatch(pattern, text.strip()):
            return text.strip()
    prefix = "已记录" if recorded else "查询结果"
    return prefix + "：" + day + "，" + "；".join(name + _LABELS[status] for name, status in ordered) + "。"
