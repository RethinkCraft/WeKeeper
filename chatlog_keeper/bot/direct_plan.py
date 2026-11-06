"""Versioned native tool plans for fully parsed, current business requests.

The existing required-read/report parsers are completion floors. This module
only admits a narrower complete grammar, intersects those floors, and records
the exact commands before any MCP call. Unrecognised text keeps the model path.
"""
from __future__ import annotations

import copy
import hashlib
import json
import re


PLAN_KEY = "_native_tool_plan"
VERSION = 1
_DATE = r"(?:今天|今日|昨天|昨日|前天|前日|明天|明日|后天|后日|\d{4}-\d{2}-\d{2})"
_REPORT_DATE = r"(?:今天|今日|\d{4}-\d{2}-\d{2})"
_ALL = r"(?:(?:全部|所有|各个|各项|每个)(?:的)?项目|首单|优惠)"
_QUOTES = re.compile(r"[\"'“”‘’「」『』`]|^\s*>", re.MULTILINE)
_EXCLUDED = re.compile(r"如果|假设|要是|比如|例如|听说|据说|他说|她说|有人说|引用|转发|"
                       r"不要|不用|无需|不必|不想|别|勿|取消|撤销|删除|更正|版本|预计|条件|"
                       r"统计|比较|对比|原因|为什么|建议|排序|筛选|只要|仅当")
_ACTION = re.compile(r"(?:(?:请|帮我|替我|麻烦)\s*)?(?:报备|登记|记录)(?:一下|下)?[吧呀啊]?")
_STATUS = (r"(?:已无|已经没有了|没有了|不可用|已用完|用完了|售罄|可用|恢复可用|恢复为可用|"
           r"恢复|还有|有了|仍然有|有名额|有|未确认|待确认|不确定|状态不明|未报备)")


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _source_identity(event):
    return {key: copy.deepcopy(event.get(key)) for key in (
        "event_id", "message_ref", "group_ref", "sender_id", "text", "occurred_at", "create_time",
        "account_id", "gateway_account_id", "group_id", "server_id", "native_source")}


def _native_text(event):
    source = event.get("native_source")
    if (event.get("transport") != "native_protocol" or not isinstance(source, dict)
            or type(source.get("type")) is not int or source["type"] != 1
            or source.get("appMessageType") is not None or source.get("quote") is not None
            or source.get("body") != event.get("text") or source.get("quoteEligible") is not True
            or source.get("disposition") != "ready" or source.get("direction") != "incoming"):
        return None
    for event_key, source_key in (("account_id", "ownUsername"), ("gateway_account_id", "accountId"),
            ("group_id", "threadId"), ("server_id", "serverId"), ("sender_id", "senderId")):
        if (not isinstance(source.get(source_key), str) or not source[source_key]
                or event.get(event_key) != source[source_key]):
            return None
    if source["senderId"] == source["ownUsername"]:
        return None
    mentions = source.get("mentions")
    if (not isinstance(mentions, dict) or mentions.get("state") != "known"
            or mentions.get("userIds") != [source["ownUsername"]]):
        return None
    text = event.get("text")
    if not isinstance(text, str) or not text.strip() or len(text) > 1000:
        return None
    # The protocol proves this request mentions our account, not that every
    # leading @-looking token is a harmless display name. Consume at most one.
    text = re.sub(r"^@[^\s\u2005]+[\s\u2005]+", "", text, count=1).strip()
    if _QUOTES.search(text) or _EXCLUDED.search(text):
        return None
    return re.sub(r"[\s。.!！?？]+$", "", text)


def _entities(business):
    aliases = {}
    for item in business.get("items", []):
        for name in (item["id"], item["name"], *item.get("aliases", [])):
            if not isinstance(name, str) or not name:
                return None
            # Date-looking entity names cannot tell an explicit day token from
            # part of the entity. Leave those bindings on the model path.
            if re.search(_DATE, name):
                return None
            # A connector inside a literal entity can also look like a list of
            # shorter configured aliases. Do not guess which tokenization the
            # user intended in this first, deliberately narrow fast path.
            if re.search(r"和|与|及|、", name):
                return None
            folded = name.casefold()
            if folded in aliases and aliases[folded] != item["id"]:
                return None
            aliases[folded] = item["id"]
    if not aliases:
        return None
    entity = "(?:" + "|".join(re.escape(value) for value in sorted(aliases, key=len, reverse=True)) + ")"
    sequence = entity + r"(?:\s*(?:和|与|及|、)\s*" + entity + ")*"
    return aliases, entity, sequence


def _item_ids(text, aliases):
    parts = re.split(r"\s*(?:和|与|及|、)\s*", text.strip())
    ids = [aliases.get(part.strip().casefold()) for part in parts]
    if any(value is None for value in ids) or len(ids) != len(set(ids)):
        return None
    return set(ids)


def build_native_plan(event, business, *, current_day):
    """Pure planning only. Its result is not tool authority or current facts."""
    from .reply_generator import required_status_day, required_status_read, _REPORT_SOURCE_TOPIC
    from .providers import ProviderError
    from .service import required_report, _STATUS_PATTERNS
    from .store import business_day
    text = _native_text(event)
    entities = _entities(business)
    if text is None or entities is None:
        return None
    aliases, _, sequence = entities
    required_items = required_status_read(event["text"], business)
    report = required_report(event, business, current_day=current_day)
    if bool(required_items) == bool(report):
        return None
    ordered = [item["id"] for item in business["items"]]
    commands, selected, kind, day = [], None, None, None
    if required_items:
        if _REPORT_SOURCE_TOPIC.search(text):
            return None
        target = r"(?P<items>" + sequence + "|" + _ALL + ")"
        prefix = r"(?:(?:帮我|替我|麻烦|请|给我|只|先|再)\s*)?"
        patterns = [
            prefix + r"(?:查询|查一下|查下|查看|查|核实一下|核实)\s*(?:(?P<day>" + _DATE + r")\s*)?"
            + target + r"\s*(?:的)?(?:状态|情况)?(?:是什么|如何|怎么样|什么状态|啥状态)?",
            r"(?:(?P<day>" + _DATE + r")\s*)?" + target + r"\s*(?:的)?(?:现在|当前|目前)?(?:的)?"
            r"(?:是什么状态|什么状态|啥状态|状态(?:是?(?:什么|啥)|如何|怎样|怎么样|呢)|"
            r"(?:还|仍然)?(?:有|可用|已无)(?:了)?[吗么]|(?:是否|是不是)(?:可用|已无)|有没有|还有没有)",
        ]
        match = next((match for pattern in patterns if (match := re.fullmatch(pattern, text, re.IGNORECASE))), None)
        if match is None:
            return None
        raw_items = match["items"]
        selected = set(ordered) if re.fullmatch(_ALL, raw_items) else _item_ids(raw_items, aliases)
        if selected is None or selected != set(required_items):
            return None
        try:
            default_day = business_day(event["occurred_at"])
            day = required_status_day(match["day"] or "", business, default_day)
            if day != required_status_day(event["text"], business, default_day):
                return None
        except (ValueError, ProviderError):
            return None
        # One existing tool call provides one consistent snapshot, including
        # when the requested subset contains multiple projects.
        args = {"date": day}
        if len(selected) == 1:
            args["item"] = next(iter(selected))
        commands = [{"name": "query_offer_status", "arguments": args}]
        kind = "query"
    else:
        states, actions = {}, 0
        pattern = (r"(?:" + _REPORT_DATE + r"\s*)?(?P<items>" + sequence + r")\s*"
                   r"(?:都|均)?(?:现在|目前|当前)?(?:状态)?(?:是|为)?(?P<status>" + _STATUS + ")")
        for clause in re.split(r"[，,；;。！!\n]+", text):
            clause = clause.strip()
            if not clause:
                return None
            if _ACTION.fullmatch(clause):
                actions += 1
                continue
            match = re.fullmatch(pattern, clause, re.IGNORECASE)
            if match is None:
                return None
            ids = _item_ids(match["items"], aliases)
            statuses = {status for status, expression in _STATUS_PATTERNS.items() if expression.search(match["status"])}
            if ids is None or len(statuses) != 1 or states.keys() & ids:
                return None
            states.update({item: next(iter(statuses)) for item in ids})
        expected = {item["item_id"]: item["status"] for item in report["items"]}
        if actions != 1 or not states or states != expected or len(states) > 9:
            return None
        selected, kind, day = set(states), "report", report["day"]
        commands = [{"name": "report_offer_status", "arguments": {
            "item": item, "status": states[item], "evidence": event["text"]}}
            for item in ordered if item in selected]
    return {"version": VERSION, "kind": kind, "day": day,
            "item_ids": [item for item in ordered if item in selected], "commands": commands,
            "source_sha256": _digest(_source_identity(event)), "business_sha256": _digest(business)}


def saved_native_plan(request, business):
    """Validate the accepted saved plan. Invalid plans never change strategy."""
    event = request.get("event") or {}
    plan = event.get(PLAN_KEY)
    if plan is None:
        return None
    if (not isinstance(plan, dict) or type(plan.get("version")) is not int or plan["version"] != VERSION
            or not isinstance(plan.get("day"), str)):
        raise ValueError("bot_direct_plan_invalid")
    expected = build_native_plan(event, business, current_day=plan["day"])
    if expected is None or _canonical(plan) != _canonical(expected):
        raise ValueError("bot_direct_plan_invalid")
    return copy.deepcopy(plan)
