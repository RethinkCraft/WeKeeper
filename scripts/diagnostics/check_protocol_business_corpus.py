#!/usr/bin/env python3
"""Prepare/replay the original 100 native business content cases sequentially.

Default is a source-independent manifest preview: no database, credentials,
provider or network is opened. --real-model enables only the explicitly chosen
model on a fresh private state copy and synthetic receive peer, never WeChat.
Original burst/pressure/idle metadata is provenance, NOT an executed schedule.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.diagnostics.protocol_cases import deepseek_cases


SEED = 20261029


def select_cases(*, count=None, case_ids=None):
    """A subset retains corpus order; omitted cases do not seed fixture facts."""
    cases = deepseek_cases(SEED)
    if count is not None and case_ids is not None:
        raise ValueError("corpus_selection_modes_are_exclusive")
    if count is not None:
        if type(count) is not int or not 1 <= count <= len(cases):
            raise ValueError("invalid_corpus_count")
        return cases[:count]
    if case_ids is None:
        return cases
    if isinstance(case_ids, str) or not isinstance(case_ids, (list, tuple)) or not case_ids:
        raise ValueError("invalid_corpus_case_ids")
    if any(not isinstance(key, str) for key in case_ids) or len(set(case_ids)) != len(case_ids):
        raise ValueError("invalid_corpus_case_ids")
    selected = set(case_ids)
    if selected - {case.id for case in cases}:
        raise ValueError("unknown_corpus_case_id")
    return [case for case in cases if case.id in selected]


def manifest(cases):
    originals = [case.as_dict() for case in cases]
    encoded = json.dumps(originals, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {"suite": "native_original_100_content", "seed": SEED, "corpus_count": 100,
        "selected_count": len(cases), "selected_case_ids": [case.id for case in cases],
        "full_content_corpus_selected": len(cases) == 100,
        "category_counts": dict(Counter(case.category for case in cases)),
        "selected_content_sha256": hashlib.sha256(encoded.encode()).hexdigest(),
        "execution": "sequential_content_only", "original_schedule_applied": False,
        "pressure_tested": False, "idle_latency_tested": False, "phone_latency_tested": False,
        "wechat_sent": False, "phone_received": False,
        "initial_expected_state": "all items unknown/version 0 in a fresh synthetic business namespace",
        "selection_state": "only preceding selected fixture writes affect later selected queries",
        "original_scenario_tags_schedule": "provenance_only_not_executed",
        "semantic_scope": "business facts and source identity; chat naturalness is not independently graded",
        "cases": originals}


def native_cases(cases, binding):
    """Translate fixture expectations, without using production intent parsers."""
    items = [item for item in binding["business"]["items"] if item.get("name") == "娱乐首单"]
    if len(items) != 1 or not isinstance(items[0].get("id"), str) or not items[0]["id"]:
        raise ValueError("corpus_item_mapping_missing_or_ambiguous")
    item_id = items[0]["id"]
    result = []
    for case in cases:
        expected = case.expectation
        if (case.sender not in {"manager", "peer2", "visitor"} or expected.fault is not None
                or expected.business_day_offset is not None or expected.final_replies != 1):
            raise ValueError("unsupported_corpus_expectation")
        if case.category == "report":
            if (expected.write_item != "entertainment" or expected.write_status not in {"available", "unavailable"}
                    or expected.required_tools != ("report_offer_status",)):
                raise ValueError("unsupported_corpus_expectation")
            writes, reads = {item_id: expected.write_status}, []
        elif case.category == "query":
            if expected.write_status is not None or expected.required_tools != ("query_offer_status",):
                raise ValueError("unsupported_corpus_expectation")
            writes, reads = {}, [item_id]
        elif case.category == "chat":
            if expected.write_status is not None or expected.required_tools:
                raise ValueError("unsupported_corpus_expectation")
            writes, reads = {}, []
        else:
            raise ValueError("unsupported_corpus_expectation")
        result.append({"id": case.id, "text": case.text, "actor": case.sender, "category": case.category,
                       "expected_write": writes, "expected_read": reads, "manual_semantic_review": False})
    return result


def run_corpus(source_state, output, *, group_ref, own, thread, count=None, case_ids=None,
               real_model=False, provider_factory=None):
    if provider_factory is None and real_model is not True:
        raise ValueError("real_model_opt_in_required")
    if provider_factory is not None and not callable(provider_factory):
        raise ValueError("invalid_corpus_provider_factory")
    selected = select_cases(count=count, case_ids=case_ids)
    # Keep model and credential code out of the default preview path.
    from scripts.diagnostics.check_protocol_business import clone_state
    from scripts.diagnostics.check_protocol_business_edges import run_cases
    from chatlog_keeper.bot.store import BotStore

    state = clone_state(source_state, output)
    with closing(BotStore(state)) as store:
        binding = store.binding(group_ref)
        if not binding or not binding.get("enabled"):
            raise ValueError("configured_group_required")
        fixtures = native_cases(selected, binding)
        configuration = {"profile": deepcopy(binding["profile"]), "persona": deepcopy(binding.get("persona"))}
    frozen_manifest = manifest(selected)
    manifest_path = Path(output) / "corpus-manifest.json"
    manifest_path.write_text(json.dumps(frozen_manifest, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    manifest_path.chmod(0o600)
    report = run_cases(state, output, group_ref=group_ref, own=own, thread=thread,
                       cases=fixtures, provider_factory=provider_factory, suite_name="native_original_100_content")
    with closing(BotStore(state)) as store:
        after = store.binding(group_ref)
        if after["profile"] != configuration["profile"] or after.get("persona") != configuration["persona"]:
            raise ValueError("corpus_model_or_persona_changed")
    report["corpus_manifest"] = frozen_manifest
    report["model_execution"] = "injected_fixture" if provider_factory is not None else "explicit_real_model"
    report["configuration_preserved"] = {"model_profile": True, "persona": True}
    # Never inherit an upstream runner's timing/transport acceptance implication.
    report.update(execution="sequential_content_only", original_schedule_applied=False,
                  pressure_tested=False, idle_latency_tested=False, phone_latency_tested=False,
                  wechat_sent=False, phone_received=False)
    path = Path(output) / "report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    path.chmod(0o600)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-state", type=Path)
    parser.add_argument("--output", type=Path)
    for name in ("group-ref", "own", "thread"):
        parser.add_argument("--" + name)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--count", type=int)
    selection.add_argument("--case-ids", nargs="+", help="original IDs, space or comma separated")
    parser.add_argument("--real-model", action="store_true")
    args = parser.parse_args(argv)
    identifiers = [part for value in args.case_ids for part in value.split(",")] if args.case_ids is not None else None
    try:
        selected = select_cases(count=args.count, case_ids=identifiers)
    except ValueError as error:
        parser.error(str(error))
    if not args.real_model:
        print(json.dumps({"mode": "preview", "model_calls": 0, "state_opened": False,
                          "corpus_manifest": manifest(selected)}, ensure_ascii=False, indent=2))
        return 0
    if any(getattr(args, name) is None for name in ("source_state", "output", "group_ref", "own", "thread")):
        parser.error("--real-model requires explicit --source-state, --output, --group-ref, --own and --thread")
    try:
        report = run_corpus(args.source_state, args.output, group_ref=args.group_ref, own=args.own,
                            thread=args.thread, count=args.count, case_ids=identifiers, real_model=True)
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and re.fullmatch(r"[a-z_]{1,100}", str(error)) else type(error).__name__
        print(json.dumps({"passed": False, "error": code}), file=sys.stderr)
        return 1
    print(json.dumps({"passed": report["passed"], "selected_count": len(selected),
                      "execution": "sequential_content_only", "report": str(Path(args.output) / "report.json")}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
