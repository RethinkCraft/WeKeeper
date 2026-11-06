#!/usr/bin/env python3
"""Native business boundary fixtures: isolated namespace, real MCP, no sender.

Only --real-model opens an external provider. The unchanged configured model,
persona, projects and reporter list are copied into a synthetic business scope
inside a fresh state copy. This is not live-group business acceptance.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.diagnostics.check_protocol_business import MemoryReceivePeer, clone_state
from chatlog_keeper.bot.protocol_business import ProtocolBusinessWorker
from chatlog_keeper.bot.protocol_gateway import _json
from chatlog_keeper.bot.protocol_history import ProtocolHistoryStore
from chatlog_keeper.bot.protocol_native_inbox import ProtocolNativeInbox
from chatlog_keeper.bot.protocol_outbox import ProtocolOutbox
from chatlog_keeper.bot.protocol_runtime import ProviderResources
from chatlog_keeper.bot.reply_generator import ReplyGenerator
from chatlog_keeper.bot.store import BotStore, business_day


def edge_cases(binding):
    names = {item["name"]: item["id"] for item in binding["business"]["items"]}
    if not {"娱乐首单", "竞技首单"} <= names.keys():
        raise ValueError("edge_fixture_projects_missing")
    fun, rank = names["娱乐首单"], names["竞技首单"]
    all_items = list(names.values())
    def case(identifier, text, category, *, actor="manager", write=None, read=None, review=False):
        return {"id": identifier, "text": text, "actor": actor, "category": category,
                "expected_write": write or {}, "expected_read": read or [],
                "manual_semantic_review": review}
    return [
        case("edges_01_unknown", "查询全部项目状态", "query", read=all_items),
        case("edges_02_multi_report", "今天娱乐首单已无，竞技首单可用，请报备", "report",
             write={fun: "unavailable", rank: "available"}),
        case("edges_03_multi_query", "查询全部项目状态", "query", read=all_items),
        case("edges_04_unauthorized", "今天娱乐首单恢复可用，请报备", "report", actor="visitor", review=True),
        case("edges_05_negated", "不要把竞技首单报成已无，我只是问问", "report", review=True),
        case("edges_06_quoted", "有人说‘娱乐首单恢复可用’，这句话是在引用，先别登记", "report", review=True),
        case("edges_07_correction", "更正：今天娱乐首单恢复可用，请报备", "report", write={fun: "available"}),
        case("edges_08_query_after_correction", "帮我查今天娱乐首单的状态", "query", actor="peer2", read=[fun]),
    ]


def dated_final_matches(text, *, day, expected, names, recorded):
    """Whole-answer fixture grammar, independent of the production renderer.

    Each named project occurs exactly once with the declared status. Extra
    claims, contradictory trailing prose and dates are rejected, not ignored.
    """
    if not isinstance(text, str) or not expected:
        return False
    prefixes = ("记好啦|记好了|已记录|已经记录|已完成报备|已经记好了" if recorded else
                "查好啦|查好了|我查过了|查询结果|本次查询结果")
    envelope = re.fullmatch(rf"(?:(?:{prefixes})[，,:：\s]*)?{re.escape(day)}[，,:：\s]*(.+?)[。！!~～\s]*", text.strip())
    if envelope is None:
        return False
    clauses = re.split(r"[；;]", envelope.group(1))
    if len(clauses) != len(expected):
        return False
    words = {"available": "可用|还有|仍然可用", "unavailable": "已无|已经没有了|没有了|已经用完了|已用完",
             "unknown": "未确认|未报备|尚未报备|状态不明"}
    remaining = set(expected)
    for clause in clauses:
        matched = [item for item in remaining if re.fullmatch(
            rf"{re.escape(names[item])}\s*(?:目前|当前|现在)?(?:状态)?(?:是|为)?\s*"
            rf"(?:{words[expected[item]['status']]})[啦哦喔呢]?", clause.strip())]
        if len(matched) != 1:
            return False
        remaining.remove(matched[0])
    return not remaining


def _fact_matches(fact, expected):
    return isinstance(fact, dict) and all(fact.get(key) == value for key, value in expected.items())


def _ledger_fingerprint(store, fixture_business):
    # Only the synthetic namespace may acquire fixture facts. Hash the original
    # rows directly, without using the business service or its query helpers.
    rows = {
        "offers": [tuple(row) for row in store.conn.execute(
            "SELECT * FROM offers WHERE business_id<>? ORDER BY business_id,day,item_id", (fixture_business,))],
        "reports": [tuple(row) for row in store.conn.execute(
            "SELECT p.* FROM reports p JOIN requests r ON r.event_id=p.event_id "
            "WHERE r.business_id<>? ORDER BY p.event_id,p.item_id", (fixture_business,))],
        "business_audit": [tuple(row) for row in store.conn.execute(
            "SELECT * FROM audit_log WHERE business_id IS NOT NULL AND business_id<>? ORDER BY id", (fixture_business,))],
    }
    return {key: {"rows": len(value), "sha256": hashlib.sha256(_json(value).encode()).hexdigest()}
            for key, value in rows.items()}


def _validate_cases(cases, items):
    if not isinstance(cases, list) or not cases or len(cases) > 200:
        raise ValueError("invalid_fixture_cases")
    identifiers = set()
    for case in cases:
        if (not isinstance(case, dict) or not isinstance(case.get("id"), str) or not case["id"]
                or case["id"] in identifiers or not isinstance(case.get("text"), str) or not case["text"].strip()
                or case.get("actor") not in {"manager", "peer2", "visitor"}
                or case.get("category") not in {"chat", "query", "report"}
                or not isinstance(case.get("expected_write"), dict) or not isinstance(case.get("expected_read"), list)
                or type(case.get("manual_semantic_review", False)) is not bool):
            raise ValueError("invalid_fixture_case")
        identifiers.add(case["id"])
        if (not case["expected_write"].keys() <= items.keys()
                or any(value not in {"available", "unavailable"} for value in case["expected_write"].values())
                or not set(case["expected_read"]) <= items.keys()
                or len(set(case["expected_read"])) != len(case["expected_read"])
                or (case["expected_write"] and (case["category"] != "report" or case["actor"] != "manager"))
                or (case["expected_read"] and case["category"] != "query")):
            raise ValueError("invalid_fixture_expectation")


def run_cases(state, output, *, group_ref, own, thread, cases=None, provider_factory=None,
              suite_name="native_business_edges"):
    output, state = Path(output), Path(state)
    if (output / "report.json").exists():
        raise ValueError("fresh_fixture_output_required")
    with ExitStack() as stack:
        store = BotStore(state); stack.callback(store.close)
        original_binding = store.binding(group_ref)
        if (not original_binding or not original_binding.get("enabled")
                or not original_binding.get("profile") or not original_binding["business"]["reporter_ids"]):
            raise ValueError("configured_reporter_and_group_required")
        if original_binding["profile"]["kind"] == "siwc" and provider_factory is None:
            raise ValueError("probe_requires_explicit_siwc_credential_copy")
        items = {item["id"]: deepcopy(item) for item in original_binding["business"]["items"]}
        cases = deepcopy(edge_cases(original_binding) if cases is None else cases)
        _validate_cases(cases, items)
        fixture_id = "native-fixture-" + hashlib.sha256(str(output.resolve()).encode()).hexdigest()[:24]
        settings = store.config()
        if any(business["id"] == fixture_id for business in settings["businesses"]):
            raise ValueError("fixture_namespace_already_exists")
        before = _ledger_fingerprint(store, fixture_id)
        business_scope = deepcopy(original_binding["business"]); business_scope["id"] = fixture_id
        settings["businesses"].append(business_scope)
        next(group for group in settings["groups"] if group["group_ref"] == group_ref)["business_id"] = fixture_id
        store.configure(settings)
        binding = store.binding(group_ref)
        assert all(binding[key] == original_binding[key] for key in ("profile", "persona"))
        assert binding["business"]["items"] == original_binding["business"]["items"]
        assert binding["business"]["reporter_ids"] == original_binding["business"]["reporter_ids"]
        reserved = {own, *binding["business"]["reporter_ids"]}
        actors = {"manager": binding["business"]["reporter_ids"][0]}
        for name in ("peer2", "visitor"):
            value = "synthetic_native_" + name
            while value in reserved:
                value += "_"
            actors[name] = value; reserved.add(value)
        peer = MemoryReceivePeer(own, thread)
        inbox = ProtocolNativeInbox(peer, path=output / "inbox.sqlite", own_username=own, consumer="isolated-edges")
        stack.callback(inbox.close); inbox.activate()
        outbox = ProtocolOutbox(output / "outbox.sqlite", account_id=peer.account_id, own_username=own)
        stack.callback(outbox.close)
        resources = ProviderResources(store, state_dir=state, usage_dir=output / "usage", dashboard_dir=state / "dashboard")
        stack.callback(resources.close)
        model_requests = []
        def provider(profile):
            model_requests.append(time.time())
            return (provider_factory or resources.provider)(profile)
        history = ProtocolHistoryStore(store, outbox, own_username=own, account_id=peer.account_id)
        generator = ReplyGenerator(history, state_dir=state, provider_factory=provider)
        business = ProtocolBusinessWorker(inbox=inbox, store=store, generator=generator, outbox=outbox,
            bindings={thread: group_ref}, own_username=own, account_id=peer.account_id,
                usage_ledger=resources.usage_ledger())
        stack.callback(business.close)
        identifiers = [peer.add(case["text"], actors[case["actor"]]) for case in cases]
        sources = dict(zip(identifiers, deepcopy(list(peer.messages.values()))))
        if any(store.request(identifier) is not None for identifier in identifiers):
            raise ValueError("fixture_request_identity_collision")
        days = {business_day(source["createdAt"]) for source in sources.values()}
        if len(days) != 1:
            raise ValueError("fixture_crossed_day_boundary")
        day = days.pop()
        expected = {item: {"business_id": fixture_id, "day": day, "item_id": item,
                           "status": "unknown", "version": 0, "message_ref": None,
                           "sender_id": None, "group_ref": None} for item in items}
        assert inbox.poll(limit=len(cases)).new_requests == len(cases)
        results = []
        def write_report():
            after = _ledger_fingerprint(store, fixture_id)
            mechanical = len(results) == len(cases) and all(row["mechanical_passed"] for row in results) and before == after
            pending = [row["case"] for row in results if row["manual_semantic_review"]["status"] == "pending"]
            report = {"schema": 1, "suite": suite_name, "passed": mechanical and not pending,
                "mechanical_passed": mechanical, "status": "failed" if not mechanical else
                    "pending_manual_semantic_review" if pending else "passed",
                "wechat_sent": False, "phone_received": False, "transport": "synthetic memory receive; no send capability",
                "scope": "synthetic business namespace; not live-group or migrated-ledger acceptance",
                "execution": "ordered content regression; no original cadence, pressure or idle timing claims",
                "fixture_business_id": fixture_id, "source_business_id": original_binding["business_id"],
                "actors": actors, "day": day, "fixture_cases": cases, "cases": results,
                "pending_manual_cases": pending, "provider_instances": len(model_requests),
                "original_ledger_preserved": before == after, "original_ledger_before": before, "original_ledger_after": after,
                "model_profile": {key: original_binding["profile"].get(key) for key in ("id", "kind", "model", "reasoning_effort")}}
            path = output / "report.json"
            path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n"); path.chmod(0o600)
            return report
        for case, identifier in zip(cases, identifiers):
            started = time.monotonic()
            source = sources[identifier]
            for item, status in case["expected_write"].items():
                expected[item] = {"business_id": fixture_id, "day": day, "item_id": item, "status": status,
                    "version": expected[item]["version"] + 1, "event_id": identifier,
                    "message_ref": "native:" + identifier.removeprefix("protocol:"), "sender_id": source["senderId"], "group_ref": group_ref}
            business.run_once(block=True)
            projection = business.project_outbox()
            frozen = store.get("protocol_frozen_" + identifier)
            failures, evidence, final = [], {}, None
            if frozen is None:
                failures.append("missing_frozen_reply")
            else:
                evidence, final = frozen["evidence"], frozen["text"]
                queued = outbox.get(frozen["reply_id"])
                if any(frozen["source"].get(key) != value or queued["source"].get(key) != value for key, value in source.items()):
                    failures.append("frozen_identity_mismatch")
                if (queued["request_id"] != identifier or queued["text"] != final or queued["state"] != "queued"
                        or evidence.get("request", {}).get("event_id") != identifier):
                    failures.append("frozen_reply_mismatch")
                if not evidence.get("usage_run_id") or evidence.get("recovery"):
                    failures.append("generation_failed_or_evidence_missing")
                if not isinstance(final, str) or not final.strip():
                    failures.append("empty_final")
            for item, fact in expected.items():
                row = store.conn.execute("SELECT * FROM offers WHERE business_id=? AND day=? AND item_id=?",
                                         (fixture_id, day, item)).fetchone()
                if (fact["version"] == 0 and row is not None) or (fact["version"] > 0 and not _fact_matches(dict(row) if row else {}, fact)):
                    failures.append("ledger_fact_mismatch:" + item)
            report_rows = store.conn.execute("SELECT item_id,result FROM reports WHERE event_id=?", (identifier,)).fetchall()
            audit_rows = store.conn.execute("SELECT item_id,kind FROM audit_log WHERE event_id=? AND kind IN ('report','stale_report')", (identifier,)).fetchall()
            if (sorted(row["item_id"] for row in report_rows) != sorted(case["expected_write"])
                    or sorted((row["item_id"], row["kind"]) for row in audit_rows) != sorted((item, "report") for item in case["expected_write"])):
                failures.append("report_or_audit_count_mismatch")
            for row in report_rows:
                if row["item_id"] in case["expected_write"]:
                    receipt = json.loads(row["result"])
                    if not _fact_matches(receipt, expected[row["item_id"]]) or receipt.get("recorded") is not True or receipt.get("ok") is not True:
                        failures.append("report_receipt_mismatch")
            selected = case["expected_write"] or case["expected_read"]
            if selected:
                category = "report" if case["expected_write"] else "query"
                facts = evidence.get("report_facts" if category == "report" else "read_facts", {})
                wanted = {item: expected[item] for item in selected}
                if set(facts) != set(wanted) or any(not _fact_matches(facts.get(item), fact) for item, fact in wanted.items()):
                    failures.append("current_request_facts_mismatch")
                covered = set()
                for tool in evidence.get("tool_results", []):
                    value = tool.get("result", {})
                    if value.get("ok") is not True:
                        continue
                    if category == "query" and tool.get("name") == "query_offer_status" and value.get("business_id") == fixture_id and value.get("day") == day:
                        for fact in value.get("items", []):
                            if fact.get("item_id") in wanted and _fact_matches(fact, wanted[fact["item_id"]]):
                                covered.add(fact["item_id"])
                    if category == "report" and tool.get("name") == "report_offer_status" and value.get("recorded") is True:
                        if value.get("item_id") in wanted and _fact_matches(value, wanted[value["item_id"]]):
                            covered.add(value["item_id"])
                if covered != set(wanted):
                    failures.append("current_request_tools_missing_or_wrong")
                if not dated_final_matches(final, day=day, expected=wanted, names={key: item["name"] for key, item in items.items()}, recorded=category == "report"):
                    failures.append("final_business_claim_mismatch")
            if projection["errors"]:
                failures.append("outbox_projection_failed")
            review = bool(case.get("manual_semantic_review"))
            result = {"case": case["id"], "category": case["category"], "sender_id": source["senderId"], "request_id": identifier,
                "mechanical_passed": not failures, "passed": not failures and not review, "failures": failures,
                "manual_semantic_review": {"required": review, "status": "pending" if review else "not_required",
                    "question": "Does the final clearly avoid claiming an unauthorized, negated or quoted report was recorded?" if review else None},
                "final": final, "evidence": evidence, "expected_facts": deepcopy(expected),
                "case_elapsed_seconds": time.monotonic() - started, "metrics": store.get("protocol_metrics_" + identifier)}
            results.append(result); write_report()
            print(json.dumps({key: result[key] for key in ("case", "mechanical_passed", "failures", "case_elapsed_seconds")}), flush=True)
        return write_report()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("source-state", "output"):
        parser.add_argument("--" + key, type=Path, required=True)
    for key in ("group-ref", "own", "thread"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--real-model", action="store_true")
    args = parser.parse_args(argv)
    if not args.real_model:
        parser.error("--real-model is required; no clone, model or network has been opened")
    state = clone_state(args.source_state, args.output)
    report = run_cases(state, args.output, group_ref=args.group_ref, own=args.own, thread=args.thread)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
