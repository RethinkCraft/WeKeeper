#!/usr/bin/env python3
"""Independent original-100/edge content regression with explicit Direct opt-in.

The copied edge oracle keeps its business facts, current tools, frozen native
identity, SQL audit and original-ledger checks. Direct evidence replaces only
its inference-run requirement after checking the actual persisted plan, local
execution ledger and observed provider constructors. No WeChat sender exists.
This is sequential content correctness, never a latency or phone benchmark.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, closing
from copy import deepcopy
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.benchmarks.benchmark_protocol_runtime_direct import DirectAdapter, ROUTE

_RUN_LOCK = threading.Lock()
FIXTURE_HELPER_SHA256 = {
    "probe": "7090295acce49f40d899049f23bfd9504cd02329fbeb6a9db4c464126a555aaa",
    "edges": "ca7353c8564ffbe8daefdb244c078cf9f88feb3a7598e417dd297ffe59b3f1fb",
    "corpus": "32099d6848a609a69a2a6a054bdaf2e537aa77e21676df7a80adcc8e2e48b034",
}


def _execution_errors(adapter, evidence, identifier, frozen):
    errors = []
    if evidence.get("recovery"):
        errors.append("generation_failed_or_evidence_missing")
    route = evidence.get("execution_route")
    # Strategy is determined by the accepted SQLite request, never by a claim
    # in the generated evidence. A planned request cannot change to a model.
    persisted = adapter.resources.store.request(identifier) if adapter.resources is not None else None
    if not persisted:
        errors.append("persisted_execution_request_missing")
    elif "_native_tool_plan" in persisted["event"] and route != ROUTE:
        errors.append("persisted_direct_plan_changed_route")
    if route == ROUTE:
        errors.extend(adapter.local_route_errors(evidence, {"request_id": identifier}, frozen))
    elif route is not None or "execution_run_id" in evidence or "direct_plan" in evidence:
        errors.append("unknown_local_execution_route")
    else:
        if not evidence.get("usage_run_id"):
            errors.append("generation_failed_or_evidence_missing")
        with adapter.lock:
            bound = identifier in adapter.generated_requests
            constructors = deepcopy(adapter.provider_constructions)
        if not bound or any(row["request_id"] is None for row in constructors):
            errors.append("provider_observation_not_bound")
        if sum(row["request_id"] == identifier for row in constructors) != 1:
            errors.append("model_provider_construction_mismatch")
    return errors


def _run_cases(state, output, *, group_ref, own, thread, cases=None, provider_factory=None,
               suite_name="native_business_edges_direct", components, helpers, adapter, build):
    # The original edge oracle is copied below; only its inference-run condition
    # is replaced after independent persistent-plan/local-ledger verification.
    BotStore, ProviderResources, ReplyGenerator, ProtocolBusinessWorker = (
        components[name] for name in ("BotStore", "ProviderResources", "ReplyGenerator", "ProtocolBusinessWorker"))
    ProtocolHistoryStore, ProtocolNativeInbox, ProtocolOutbox, business_day = (
        components[name] for name in ("ProtocolHistoryStore", "ProtocolNativeInbox", "ProtocolOutbox", "business_day"))
    MemoryReceivePeer, _json, edge_cases, _validate_cases, _ledger_fingerprint, _fact_matches, dated_final_matches = helpers
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
                "model_profile": {key: original_binding["profile"].get(key) for key in ("id", "kind", "model", "reasoning_effort")},
                "build": build, "direct_adapter": adapter.metadata(),
                "execution_route_counts": {route: sum(row["evidence"].get("execution_route") == route for row in results)
                    for route in (ROUTE, None)}}
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
                failures.extend(_execution_errors(adapter, evidence, identifier, frozen))
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


def _private_json(path, value):
    path = Path(path)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    path.chmod(0o600)


def run_content(source_state, output, *, group_ref, own, thread, code_root=ROOT,
                suite="corpus", count=None, case_ids=None, real_model=False,
                provider_factory=None, direct_native_plan=False, cases=None):
    if provider_factory is None and real_model is not True:
        raise ValueError("real_model_opt_in_required")
    if provider_factory is not None and not callable(provider_factory):
        raise ValueError("invalid_content_provider_factory")
    if type(direct_native_plan) is not bool:
        raise ValueError("invalid_direct_content_option")
    if suite not in {"corpus", "edges"} or (suite == "edges" and (count is not None or case_ids is not None)):
        raise ValueError("invalid_direct_content_suite")
    if cases is not None and suite != "edges":
        raise ValueError("custom_cases_require_edges_suite")
    if not _RUN_LOCK.acquire(blocking=False):
        raise ValueError("concurrent_content_code_roots_not_supported")
    previous = os.environ.get("WEKEEPER_BENCHMARK_CODE_ROOT")
    try:
        # Check all already-imported production modules before importing helpers.
        # The same root is passed to every real MCP subprocess by the Host.
        from scripts.benchmarks.benchmark_protocol_runtime import load_components
        components, build = load_components(code_root)
        root = Path(build["code_root"])
        os.environ["WEKEEPER_BENCHMARK_CODE_ROOT"] = str(root)
        adapter = DirectAdapter(enabled=direct_native_plan)
        if direct_native_plan:
            module = importlib.import_module("chatlog_keeper.bot.direct_plan")
            if not Path(module.__file__).resolve().is_relative_to(root):
                raise ValueError("content_direct_plan_code_root_mismatch")
            adapter.plan_validator = module.saved_native_plan
        components = adapter.wrap_components(components)
        # A selected code root may also contain scripts. Accept only the fixed
        # reviewed fixture bytes, regardless of where Python finds the module;
        # candidate production code must never silently replace its oracle.
        from scripts.diagnostics import check_protocol_business as probe
        from scripts.diagnostics import check_protocol_business_edges as edges
        from scripts.diagnostics import check_protocol_business_corpus as corpus
        from chatlog_keeper.bot.protocol_gateway import _json
        build["content_runner_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        build["fixture_helper_sha256"] = {
            name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
            for name, module in (("probe", probe), ("edges", edges), ("corpus", corpus))}
        if build["fixture_helper_sha256"] != FIXTURE_HELPER_SHA256:
            raise ValueError("content_fixture_helper_source_changed")
        helpers = (probe.MemoryReceivePeer, _json, edges.edge_cases, edges._validate_cases,
                   edges._ledger_fingerprint, edges._fact_matches, edges.dated_final_matches)
        selected = corpus.select_cases(count=count, case_ids=case_ids) if suite == "corpus" else None
        state = probe.clone_state(source_state, output)
        with closing(components["BotStore"](state)) as store:
            binding = store.binding(group_ref)
            if not binding or not binding.get("enabled"):
                raise ValueError("configured_group_required")
            fixtures = corpus.native_cases(selected, binding) if selected is not None else cases
            configuration = {"profile": deepcopy(binding["profile"]), "persona": deepcopy(binding.get("persona"))}
        frozen_manifest = corpus.manifest(selected) if selected is not None else None
        if frozen_manifest is not None:
            _private_json(Path(output) / "corpus-manifest.json", frozen_manifest)
        report = _run_cases(state, output, group_ref=group_ref, own=own, thread=thread,
            cases=fixtures, provider_factory=provider_factory, suite_name="native_" + suite + "_content_direct",
            components=components, helpers=helpers, adapter=adapter, build=build)
        with closing(components["BotStore"](state)) as store:
            after = store.binding(group_ref)
            if after["profile"] != configuration["profile"] or after.get("persona") != configuration["persona"]:
                raise ValueError("content_model_or_persona_changed")
        report["corpus_manifest"] = frozen_manifest
        report["model_execution"] = "injected_fixture_for_model_route" if provider_factory is not None else "explicit_real_model_for_model_route"
        report["configuration_preserved"] = {"model_profile": True, "persona": True}
        report.update(execution="sequential_content_only", original_schedule_applied=False,
                      pressure_tested=False, idle_latency_tested=False, phone_latency_tested=False,
                      wechat_sent=False, phone_received=False)
        _private_json(Path(output) / "report.json", report)
        return report
    finally:
        if previous is None:
            os.environ.pop("WEKEEPER_BENCHMARK_CODE_ROOT", None)
        else:
            os.environ["WEKEEPER_BENCHMARK_CODE_ROOT"] = previous
        _RUN_LOCK.release()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-state", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--code-root", type=Path, default=ROOT)
    for name in ("group-ref", "own", "thread"):
        parser.add_argument("--" + name)
    parser.add_argument("--suite", choices=("corpus", "edges"), default="corpus")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--count", type=int)
    selection.add_argument("--case-ids", nargs="+")
    parser.add_argument("--direct-native-plan", action="store_true")
    parser.add_argument("--real-model", action="store_true")
    args = parser.parse_args(argv)
    identifiers = [part for value in args.case_ids for part in value.split(",")] if args.case_ids is not None else None
    if args.suite == "edges" and (args.count is not None or identifiers is not None):
        parser.error("edges does not accept corpus selections")
    if not args.real_model:
        preview = {"mode": "preview", "model_calls": 0, "state_opened": False,
            "suite": args.suite, "direct_native_plan_enabled": args.direct_native_plan,
            "code_root": str(args.code_root.resolve()), "execution": "sequential_content_only"}
        if args.suite == "corpus":
            from scripts.diagnostics.check_protocol_business_corpus import select_cases, manifest
            try:
                preview["corpus_manifest"] = manifest(select_cases(count=args.count, case_ids=identifiers))
            except ValueError as error:
                parser.error(str(error))
        else:
            preview["fixture_count"] = 8
            preview["project_mapping"] = "deferred_until_explicit_state_open"
        print(json.dumps(preview, ensure_ascii=False, indent=2))
        return 0
    if any(getattr(args, name) is None for name in ("source_state", "output", "group_ref", "own", "thread")):
        parser.error("--real-model requires explicit --source-state, --output, --group-ref, --own and --thread")
    try:
        report = run_content(args.source_state, args.output, group_ref=args.group_ref, own=args.own,
            thread=args.thread, code_root=args.code_root, suite=args.suite, count=args.count,
            case_ids=identifiers, real_model=True, direct_native_plan=args.direct_native_plan)
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) and re.fullmatch(r"[a-z_]{1,100}", str(error)) else type(error).__name__
        print(json.dumps({"passed": False, "error": code}), file=sys.stderr)
        return 1
    print(json.dumps({"passed": report["passed"], "selected_count": len(report["cases"]),
        "execution": "sequential_content_only", "report": str(Path(args.output) / "report.json")}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
