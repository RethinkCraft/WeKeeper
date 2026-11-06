"""Native admission capabilities; model context never grants business access."""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from .direct_plan import _native_text, _source_identity


CHAT_ONLY = "chat_only"
BUSINESS_CAPABLE = "business_capable"
POLICY_KEY = "_execution_policy"
_BUSINESS_WORDS = re.compile(r"首单|优惠|业务|项目|报备|查询|登记|记录|库存|状态|可用|已无|恢复|订单|报价|价格|名额|余额|账")
_SOCIAL_CLAUSE = re.compile(
    r"(?:你好|您好|哈喽|嗨|(?:你|您)?在吗|(?:你|您)?在不在|早上好|早安|午安|下午好|晚上好|晚安|"
    r"谢谢(?:你|您)?|多谢(?:你|您)?|辛苦(?:了)?|再见|拜拜|回头见|"
    r"接(?:个|一句)?话|(?:陪我|一起|来|咱们)?(?:聊聊|聊天|闲聊|说说话|聊两句)|"
    r"(?:讲|说|来)(?:个|一个)?(?:笑话|段子)|hello|hi|hey|good morning|good evening|good night)"
    r"[呀啊呢啦吧哇哦呗~～]*", re.IGNORECASE)


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode()).hexdigest()


def classify_native_request(event, business):
    """Only fully consumed social clauses can give up all business authority."""
    text = _native_text(event)
    raw = event.get("text", "")
    if text is None or len(text) > 160 or _BUSINESS_WORDS.search(raw):
        return BUSINESS_CAPABLE
    names = [business.get("id"), business.get("name")]
    for item in business.get("items", []):
        names.extend((item.get("id"), item.get("name"), *item.get("aliases", [])))
    if any(isinstance(name, str) and name and name.casefold() in raw.casefold() for name in names):
        return BUSINESS_CAPABLE
    clauses = [part.strip() for part in re.split(r"[，,。.!！?？；;\n]+", text) if part.strip()]
    return CHAT_ONLY if clauses and all(_SOCIAL_CLAUSE.fullmatch(part) for part in clauses) else BUSINESS_CAPABLE


def build_native_policy(event, business):
    if event.get("transport") != "native_protocol" or event.get("native_business") != business:
        return None
    return {"version": 1, "capability": classify_native_request(event, business),
            "source_sha256": _digest(_source_identity(event)), "business_sha256": _digest(business)}


def request_capability(request):
    event = request.get("event") or {}
    policy = event.get(POLICY_KEY)
    if policy is None:
        return BUSINESS_CAPABLE  # Old accepted work is never reclassified.
    business = event.get("native_business")
    if (not isinstance(policy, dict)
            or set(policy) != {"version", "capability", "source_sha256", "business_sha256"}
            or type(policy["version"]) is not int or policy["version"] != 1
            or policy["capability"] not in {CHAT_ONLY, BUSINESS_CAPABLE}
            or not isinstance(business, dict) or request.get("business_id") != business.get("id")
            or event.get("transport") != "native_protocol"
            or policy["source_sha256"] != _digest(_source_identity(event))
            or policy["business_sha256"] != _digest(business)
            or (policy["capability"] == CHAT_ONLY and
                ("_native_tool_plan" in event or classify_native_request(event, business) != CHAT_ONLY))):
        raise ValueError("invalid_bot_request_policy")
    return policy["capability"]


def stored_request_capability(state_dir, event_id):
    """Read an accepted capability without initializing state or retaining SQL locks."""
    path = Path(state_dir).expanduser().absolute() / "bot" / "bot.sqlite"
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        row = db.execute("SELECT event,business_id FROM requests WHERE event_id=?", (event_id,)).fetchone()
    if row is None:
        raise ValueError("bot_request_missing")
    return request_capability({"event": json.loads(row["event"]), "business_id": row["business_id"]})


def _require_business_capability(state_dir, event_id):
    from .providers import ProviderError
    if stored_request_capability(state_dir, event_id) == CHAT_ONLY:
        raise ProviderError("bot_business_capability_denied")
