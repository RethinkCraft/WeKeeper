"""Deterministic synthetic inputs; never imports credentials or a live client."""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import random


DEFAULT_SEED = 20261029
CATEGORIES = ("chat", "query", "report")


@dataclass(frozen=True)
class Expectation:
    write_status: str | None = None
    write_item: str | None = None
    mcp_required: bool = False
    required_tools: tuple[str, ...] = ()
    final_replies: int = 1
    quote_source: bool = True
    fault: str | None = None
    terminal_statuses: tuple[str, ...] = ("completed",)
    expected_errors: tuple[str, ...] = ()
    business_day_offset: int | None = None


@dataclass(frozen=True)
class Case:
    id: str
    sender: str
    text: str
    category: str
    scenario: str
    relative_schedule: float
    expectation: Expectation
    tags: tuple[str, ...] = ()

    def as_dict(self):
        return asdict(self)


def _case(index, category, scenario, scheduled, rng, *, fault=None, tags=()):
    case_id = f"synthetic_{index + 1:04d}"
    status = rng.choice(("available", "unavailable"))
    if category == "chat":
        text = rng.choice(("在吗，接个话呀", "你怎么又在摸鱼", "今天心情不错，陪我聊一句"))
        expectation = Expectation(fault=fault)
    elif category == "query":
        text = "帮我查今天娱乐首单的状态"
        expectation = Expectation(mcp_required=True, required_tools=("query_offer_status",), fault=fault)
    else:
        text = "今天娱乐首单" + ("可用，请报备" if status == "available" else "已无，请报备")
        expectation = Expectation(write_status=status, write_item="entertainment", mcp_required=True,
                                  required_tools=("report_offer_status",), fault=fault)
    # Case identity belongs only in controller/SID metadata. Repeated ordinary
    # bodies intentionally exercise the same ambiguity as real group messages.
    text = f"@台架机器人 {text}"
    sender = "visitor" if fault == "unauthorized_report" else "manager" if category == "report" else rng.choice(("manager", "peer2"))
    return Case(case_id, sender, text, category, scenario, float(scheduled), expectation, tuple(tags))


def deepseek_cases(seed=DEFAULT_SEED):
    """100 arrivals whose schedule is independent of inference completion.

    Regular traffic has 20 cases per category. The gap before arrival 31 is
    90 seconds; this is a scheduled idle gap, not proof of 90 seconds since
    the previous HTTP request completed. The runner must record actual idle.
    """
    if type(seed) is not int:
        raise ValueError("invalid_deepseek_case_seed")
    rng = random.Random(seed)
    categories = [category for category in CATEGORIES for _ in range(20)]
    rng.shuffle(categories)
    output = []
    for index, category in enumerate(categories):
        scheduled = index * 5 + (85 if index >= 30 else 0)
        tags = ("cold_start",) if index == 0 else ("idle_90s_first",) if index == 30 else ()
        output.append(_case(index, category, "regular", scheduled, rng, tags=tags))
    burst_start = output[-1].relative_schedule + 5
    for pair in range(10):
        category = ("chat", "query")[pair % 2]
        first = _case(60 + pair * 2, category, "burst", burst_start + pair * 5,
                      rng, tags=("two_sender_pair", f"pair_{pair + 1:02d}", "same_body_different_senders"))
        second = _case(61 + pair * 2, category, "burst", first.relative_schedule, rng, tags=first.tags)
        output.extend((replace(first, sender="manager"), replace(second, sender="peer2", text=first.text)))
    pressure_start = output[-1].relative_schedule + 5
    for offset in range(20):
        output.append(_case(80 + offset, CATEGORIES[offset % 3], "pressure", pressure_start + offset, rng))
    return output

