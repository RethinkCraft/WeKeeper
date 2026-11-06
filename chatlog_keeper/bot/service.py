"""Shared offer tools with server-bound identity and conservative report evidence."""
from __future__ import annotations

from .request_policy import CHAT_ONLY, request_capability

from datetime import date
import re

from .store import business_day


_TOOL_DEFINITIONS = [
    {"type": "function", "function": {
        "name": "list_offer_items", "description": "列出当前群所属业务的首单优惠项目及别名。",
        "parameters": {"type": "object", "properties": {}, "additionalProperties": False}}},
    {"type": "function", "function": {
        "name": "query_offer_status", "description": "查询当前业务某日的优惠状态，缺省为今天；未报备返回unknown，不得猜测可用。",
        "parameters": {"type": "object", "properties": {
            "item": {"type": "string", "description": "项目ID、名称或明确别名；省略表示全部项目。"},
            "date": {"type": "string", "description": "上海自然日YYYY-MM-DD，省略表示今天。"}}, "additionalProperties": False}}},
    {"type": "function", "function": {
        "name": "report_offer_status",
        "description": "根据当前消息本人明确陈述登记当天优惠状态。仅授权报备者可写；问句、假设、引语不算报备。项目或状态不明确时先追问。",
        "parameters": {"type": "object", "properties": {
            "item": {"type": "string", "description": "当前消息明确提及的项目ID、名称或别名。"},
            "status": {"type": "string", "enum": ["available", "unavailable", "unknown"]},
            "evidence": {"type": "string", "description": "当前消息中明确陈述项目状态的原文片段。"},
            "expected_version": {"type": "integer", "minimum": 0}},
            "required": ["item", "status", "evidence"], "additionalProperties": False}}},
]

TOOLS = _TOOL_DEFINITIONS


def tool_schemas(writable=True):
    import copy
    return copy.deepcopy(_TOOL_DEFINITIONS if writable else _TOOL_DEFINITIONS[:2])


def _failure(code, **details):
    return {"ok": False, "error": code, **details}


_STATUSES = {
    "available": "available", "可用": "available", "有": "available", "恢复": "available",
    "unavailable": "unavailable", "exhausted": "unavailable", "已无": "unavailable", "没有了": "unavailable", "已用完": "unavailable",
    "unknown": "unknown", "未确认": "unknown", "未报备": "unknown", "待确认": "unknown",
}
_QUESTION_OR_HYPOTHETICAL = re.compile(r"[?？]|(?:是否|是不是|有没有|能不能|可不可以|请问|想问|问一下|查一下|查下|查询|帮我查|假设|如果|要是|比如|例如|举例|听说|据说|转发|引用|他说|她说|说过|有人说)|[吗么](?:[呀啊呢嘛哇~～!！。\s]|$)")
_NEGATED_INSTRUCTION = re.compile(r"不是|并非|并不|不要|不用|别记|别改|勿|不许|不能|不可以|不想|无需|没说|没有说|未说|不代表|尚未用完|还没用完|尚未售罄|还没售罄|没有恢复|尚未恢复|还没恢复|未恢复|否认")
_STATUS_PATTERNS = {
    "unavailable": re.compile(r"已(?:经)?(?:无|没(?:有)?了?|用完|售罄|抢完|领完|耗尽)|(?:没有|没|用完|抢完|领完)了|售罄|不可用|没有可用|无可用|不再可用"),
    "available": re.compile(r"(?:恢复|重新)(?:为)?(?:可用|有)?|(?<!不)(?<!没有)(?<!无)(?<!不再)可用|(?:还|仍然)有|有了|有名额|有(?:$|[，,！!。\s])"),
    "unknown": re.compile(r"未(?:报备|确认)|还没确认|待确认|不确定|状态不明|清空(?:状态|记录)?"),
}


def _bound_report_statuses(text, items):
    """Associate comma clauses with named projects, retaining contradictions.

    A following clause without a new project still refers to the last named
    project(s), so '娱乐已无，还是可用' cannot become a partial positive match.
    Sentence/semicolon boundaries end that implicit association.
    """
    found = {item["id"]: set() for item in items}
    aliases = {item["id"]: tuple(value.casefold() for value in
               (item["id"], item["name"], *item.get("aliases", []))) for item in items}
    for sentence in re.split(r"[\n。！!；;]", text):
        subjects = set()
        for clause in re.split(r"[，,]", sentence):
            lowered = clause.casefold()
            named = {identifier for identifier, names in aliases.items() if any(name in lowered for name in names)}
            if named:
                subjects = named
            states = {key for key, pattern in _STATUS_PATTERNS.items() if pattern.search(clause)}
            for identifier in subjects:
                found[identifier].update(states)
    return found


def report_evidence(text, evidence, item, status, *, items=None):
    """A model suggestion is insufficient; require an affirmative source clause."""
    if not isinstance(evidence, str) or not evidence.strip() or evidence not in text:
        return False
    if _QUESTION_OR_HYPOTHETICAL.search(text) or _NEGATED_INSTRUCTION.search(text):
        return False
    # Quoted fragments, including a user's inline quotation, cannot authorize writes.
    if any(marker in text for marker in ('"', "'", '“', '”', '‘', '’', '「', '」', '『', '』', '```')):
        return False
    if any(line.lstrip().startswith(">") for line in text.splitlines()):
        return False
    scope = items if items is not None else [item]
    # Check the whole trusted source as well as the model's evidence fragment.
    # Selecting just the first clause cannot hide a conflicting state later on.
    return (_bound_report_statuses(text, scope).get(item["id"]) == {status}
            and _bound_report_statuses(evidence, scope).get(item["id"]) == {status})


def report_date_error(text, source_day, current_day):
    """Share the same source-date boundary between host policy and writes."""
    if source_day != current_day:
        return "report_expired"
    if re.search(r"昨天|昨日|前天|前日|明天|明日|后天|后日|上周|下周|上个?月|下个?月", text):
        return "report_source_date_ambiguous"
    if any(value != source_day for value in re.findall(r"\d{4}-\d{2}-\d{2}", text)):
        return "report_source_date_mismatch"
    return None


def required_report(event, business, *, current_day):
    """Require only reports already supported by this authorized source.

    This is a completion check, not write authority. The MCP Service still
    validates the actual item, evidence, dates and authorization on every call.
    """
    text = event.get("text")
    if (event.get("sender_id") not in business.get("reporter_ids", [])
            or not isinstance(text, str)):
        return None
    day = business_day(event.get("occurred_at", event.get("create_time")))
    if report_date_error(text, day, current_day):
        return None
    items = [{"item_id": item["id"], "status": status}
             for item in business.get("items", []) for status in _STATUS_PATTERNS
             if report_evidence(text, text, item, status, items=business.get("items", []))]
    return {"event_id": str(event["event_id"]), "day": day, "items": items} if items else None


class Service:
    def __init__(self, store, context=None):
        self.store = store
        self.context = dict(context or {})

    @property
    def tools(self):
        if self.context.get("event_id"):
            request = self.store.request(self.context["event_id"])
            if request is not None and request_capability(request) == CHAT_ONLY:
                return []
        return tool_schemas(bool(self.context.get("writable") and self.context.get("event_id")))

    def _scope(self):
        event_id = self.context.get("event_id")
        if event_id:
            request = self.store.request(event_id)
            if request is None:
                return None, None, _failure("bot_request_missing")
            if request_capability(request) == CHAT_ONLY:
                return None, None, _failure("bot_business_capability_denied")
            for key in ("group_ref", "sender_id", "message_ref", "text", "occurred_at"):
                if key in self.context and self.context[key] != request[key]:
                    return None, None, _failure("bot_context_mismatch")
            group_ref = request["group_ref"]
        else:
            request = None
            group_ref = self.context.get("group_ref")
        if not group_ref:
            return None, None, _failure("explicit_group_required")
        binding = self.store.binding(group_ref)
        if not binding or not binding["enabled"]:
            return None, None, _failure("bot_group_disabled")
        if request and binding["business_id"] != request["business_id"]:
            return None, None, _failure("bot_binding_changed")
        return binding, request, None

    @staticmethod
    def _item(business, name):
        if not isinstance(name, str) or not name.strip():
            return None
        needle = name.strip().casefold()
        return next((item for item in business["items"] if needle in {
            value.casefold() for value in (item["id"], item["name"], *item.get("aliases", []))}), None)

    def call(self, name, args=None):
        if name not in {tool["function"]["name"] for tool in _TOOL_DEFINITIONS}:
            return _failure("unknown_bot_tool")
        if args is None:
            args = {}
        if not isinstance(args, dict):
            return _failure("invalid_tool_arguments")
        fields = {"list_offer_items": set(), "query_offer_status": {"item", "item_id", "date", "day"},
                  "report_offer_status": {"item", "item_id", "status", "evidence", "expected_version", "date", "day"}}
        if set(args) - fields[name]:
            return _failure("invalid_tool_arguments")
        binding, request, error = self._scope()
        if error:
            return error
        business = binding["business"]
        items = [{"id": item["id"], "name": item["name"], "aliases": item.get("aliases", [])} for item in business["items"]]
        if name == "list_offer_items":
            return {"ok": True, "business_id": business["id"], "business_name": business["name"], "items": items}
        query_date = args.get("date", args.get("day"))
        try:
            if query_date is not None:
                if not isinstance(query_date, str) or date.fromisoformat(query_date).isoformat() != query_date:
                    return _failure("invalid_offer_date")
                day = query_date
            else:
                day = business_day(self.context.get("now") or (request["occurred_at"] if request else self.store.now()))
        except (TypeError, ValueError):
            return _failure("invalid_offer_date")
        item_name = args.get("item", args.get("item_id"))
        item = self._item(business, item_name) if item_name is not None else None
        if item_name is not None and item is None:
            return _failure("unknown_offer_item", needs_clarification=True, items=items)
        if name == "query_offer_status":
            selected = [item] if item else business["items"]
            snapshot = self.store.offer_statuses(business["id"], day, [row["id"] for row in selected])
            statuses = [{**status, "item_name": selected_item["name"]}
                        for selected_item, status in zip(selected, snapshot)]
            return {"ok": True, "business_id": business["id"], "business_name": business["name"], "day": day, "items": statuses}
        if not self.context.get("writable") or request is None:
            return _failure("read_only_bot_context")
        if request["status"] not in {"processing", "interrupted"}:
            return _failure("bot_request_closed")
        if request["sender_id"] not in business["reporter_ids"]:
            return _failure("report_not_authorized")
        if item is None:
            return _failure("offer_item_required", needs_clarification=True, items=items)
        status = _STATUSES.get(args.get("status")) if isinstance(args.get("status"), str) else None
        if status is None:
            return _failure("offer_status_required", needs_clarification=True)
        source_day = business_day(request["occurred_at"])
        if query_date is not None and day != source_day:
            return _failure("report_source_date_mismatch")
        # Yesterday's delayed model task cannot silently become today's report.
        date_error = report_date_error(request["text"], source_day,
                                       business_day(self.context.get("now") or self.store.now()))
        if date_error:
            return _failure(date_error, **({} if date_error == "report_expired" else {"needs_clarification": True}))
        if not report_evidence(request["text"], args.get("evidence"), item, status, items=business["items"]):
            return _failure("explicit_report_evidence_required", needs_clarification=True)
        expected = args.get("expected_version")
        if expected is not None and (not isinstance(expected, int) or isinstance(expected, bool) or expected < 0):
            return _failure("invalid_offer_version")
        return self.store.record_offer(request["event_id"], item["id"], status,
                                       expected_version=expected, evidence=args["evidence"])
