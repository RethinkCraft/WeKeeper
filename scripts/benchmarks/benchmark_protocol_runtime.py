#!/usr/bin/env python3
"""Scheduled native-runtime benchmark; simulated transport, never WeChat/phone.

The five production lanes, SQLite stores, generator and MCP remain real. Only
--real-model opens the configured provider on a fresh private state copy.
Default invocation previews the complete schedule without opening any state.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import threading
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
CATEGORIES = ("chat", "query", "report")
SEND_SAMPLES = [
    {"accepted": 1.774814, "http_return": .5968368620378897},
    {"accepted": 1.660948, "http_return": .4130815080134198},
    {"accepted": 1.461621, "http_return": .424806875991635},
    {"accepted": 1.561593, "http_return": .4578924389788881},
]
OPERATIONS = {"activation", "receive", "snapshot", "ack", "preflight", "status", "accepted", "http_return"}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def private_json(path, value):
    path = Path(path)
    with path.open("w", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


def default_schedule():
    """Fixed send times, independent of completion, with one deliberate burst."""
    result = []
    for cycle in range(10):
        base = cycle * 12 if cycle < 8 else 96 if cycle == 8 else 134
        offsets = (0, .1, .2) if cycle == 7 else (0, 4, 8)
        status = "unavailable" if cycle % 2 == 0 else "available"
        texts = {"report": "今天娱乐首单已无，请报备" if status == "unavailable" else "今天娱乐首单恢复可用，请报备",
                 "query": "帮我查今天娱乐首单的状态", "chat": "在吗，接个话呀"}
        for category, offset in zip(("report", "query", "chat"), offsets):
            result.append({"id": f"runtime_{cycle + 1:02d}_{category}", "category": category,
                "at_seconds": base + offset, "text": texts[category],
                "scenario": "continuous_report_query" if cycle == 7 else
                    "idle_first" if cycle == 9 and category == "report" else "normal_repeated_body",
                "write_status": status if category == "report" else None})
    return result


def validate_schedule(schedule):
    if not isinstance(schedule, list) or len(schedule) != 30:
        raise ValueError("benchmark_requires_thirty_requests")
    keys, previous, counts = set(), -1, Counter()
    for row in schedule:
        if (not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"]
                or row["id"] in keys or row.get("category") not in CATEGORIES
                or not isinstance(row.get("text"), str) or not row["text"].strip()
                or type(row.get("at_seconds")) not in (int, float)
                or not math.isfinite(row["at_seconds"]) or row["at_seconds"] < max(0, previous)
                or row["at_seconds"] > 3600 or not isinstance(row.get("scenario"), str)
                or (row.get("write_status") not in {"available", "unavailable"} if row["category"] == "report"
                    else row.get("write_status") is not None)):
            raise ValueError("invalid_benchmark_schedule")
        keys.add(row["id"]); previous = row["at_seconds"]; counts[row["category"]] += 1
    if counts != Counter({category: 10 for category in CATEGORIES}):
        raise ValueError("benchmark_requires_ten_per_category")
    if schedule[0]["category"] != "report":
        raise ValueError("benchmark_requires_report_before_query")
    return deepcopy(schedule)


def transport_configuration(read_calibration=None, overrides=None):
    """Activation/snapshot samples already contain binding; do not add it twice."""
    result = {"samples": {"activation": [.010464], "receive": [.000884], "snapshot": [.007034],
                           "preflight": [.00631], "ack": [.00631], "status": [.00631]},
        "send_samples": deepcopy(SEND_SAMPLES), "extra_seconds": {}, "jitter_seconds": {}, "seed": 20261030,
        "provenance": {"send": "four live command-to-accepted-event wall observations, including gateway entry queue; HTTP return overlaps",
            "account_gate_model": "v2: native consumer activation and quote send reserve the same FIFO account gate; snapshot and identity preflight do not",
            "account_gate_source": "fixed Rust wechat-core native_ingress.rs activate_native_consumer and runtime.rs quote perform_send",
            "send_calibration_count": 4,
            "command_to_gateway_submitting_seconds": [.502255, .365878, .400534, .378949],
            "send_limitations": "small sample; host wall event times can adjust; no raw wire/phone tail claim; entry queue is already included",
            "reads": "v2 idle-read p50 defaults; use --read-calibration to cycle all measured samples",
            "ack": "unmeasured; standalone identity-read magnitude proxy",
            "status": "unmeasured; standalone identity-read magnitude proxy",
            "receive": "empty registered-consumer GET; nonempty delivery bookkeeping and receive/decode unmeasured"}}
    if read_calibration is not None:
        path = Path(read_calibration)
        document = json.loads(path.read_text())
        names = {"activation": "existing_activation", "receive": "required_consumer_event_read",
                 "snapshot": "received_snapshot", "preflight": "require_account"}
        for key, operation in names.items():
            values = [row["seconds"] for row in document["samples"] if row["operation"] == operation]
            if not values:
                raise ValueError("calibration_operation_missing")
            result["samples"][key] = values
        result["samples"]["ack"] = list(result["samples"]["preflight"])
        result["samples"]["status"] = list(result["samples"]["preflight"])
        result["provenance"]["reads"] = "all measured v2 idle operation samples, cyclic per operation"
        result["provenance"]["read_file_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    if overrides is not None:
        if not isinstance(overrides, dict) or set(overrides) - {"samples", "send_samples", "extra_seconds", "jitter_seconds", "seed"}:
            raise ValueError("invalid_transport_configuration")
        for key, value in overrides.items():
            if key in {"samples", "extra_seconds", "jitter_seconds"}:
                result[key].update(deepcopy(value))
            else:
                result[key] = deepcopy(value)
        result["provenance"]["overrides"] = deepcopy(overrides)
    if type(result["seed"]) is not int or set(result["samples"]) != OPERATIONS - {"accepted", "http_return"}:
        raise ValueError("invalid_transport_configuration")
    for values in [*result["samples"].values(), *[list(row.values()) for row in result["send_samples"]]]:
        if not isinstance(values, list) or not values or any(type(value) not in (int, float) or not math.isfinite(value)
                                                          or not 0 <= value <= 60 for value in values):
            raise ValueError("invalid_transport_delay")
    if not result["send_samples"] or any(set(row) != {"accepted", "http_return"} for row in result["send_samples"]):
        raise ValueError("invalid_paired_send_samples")
    for key in ("extra_seconds", "jitter_seconds"):
        if set(result[key]) - OPERATIONS or any(type(value) not in (int, float) or not math.isfinite(value)
                                              or not 0 <= value <= 60 for value in result[key].values()):
            raise ValueError("invalid_transport_delay")
    return result


def load_components(code_root):
    root = Path(code_root).resolve()
    if not (root / "chatlog_keeper/bot/protocol_runtime.py").is_file():
        raise ValueError("invalid_benchmark_code_root")
    for name, module in list(sys.modules.items()):
        if name.startswith("chatlog_keeper") and getattr(module, "__file__", None):
            if not Path(module.__file__).resolve().is_relative_to(root):
                raise ValueError("benchmark_code_root_already_imported_elsewhere")
    sys.path.insert(0, str(root))
    names = {"protocol_runtime": ("ProtocolRuntime", "ProviderResources"), "protocol_business": ("ProtocolBusinessWorker",),
        "protocol_native_inbox": ("ProtocolNativeInbox",), "protocol_outbox": ("ProtocolOutbox",),
        "protocol_history": ("ProtocolHistoryStore",), "reply_generator": ("ReplyGenerator",),
        "store": ("BotStore", "business_day"), "protocol_gateway": ("SendResult", "GatewayError")}
    components, loaded_files = {}, {}
    for module, exports in names.items():
        loaded = importlib.import_module("chatlog_keeper.bot." + module)
        if not Path(loaded.__file__).resolve().is_relative_to(root):
            raise ValueError("benchmark_loaded_code_root_mismatch")
        components.update({name: getattr(loaded, name) for name in exports})
        loaded_files[module] = {"path": str(Path(loaded.__file__).resolve()),
                               "sha256": hashlib.sha256(Path(loaded.__file__).read_bytes()).hexdigest()}
    files = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
             for path in sorted((root / "chatlog_keeper").rglob("*.py"))}
    return components, {"code_root": str(root), "loaded_modules": loaded_files, "python_files": files, "python_tree_sha256": digest(files),
        "benchmark_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}


def clone_state(source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    if output.exists() or source == output or source in output.parents:
        raise ValueError("fresh_separate_output_required")
    output.mkdir(parents=True, mode=0o700)
    state = output / "state"
    (state / "bot").mkdir(parents=True, mode=0o700); state.chmod(0o700)
    uri = "file:" + quote(str(source / "bot/bot.sqlite"), safe="/") + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as original, sqlite3.connect(state / "bot/bot.sqlite") as copied:
        original.backup(copied)
    (state / "bot/bot.sqlite").chmod(0o600)
    shutil.copytree(source / "bot/secrets", state / "bot/secrets")
    for path in (state / "bot/secrets", *(state / "bot/secrets").rglob("*")):
        path.chmod(0o700 if path.is_dir() else 0o600)
    (output / "usage").mkdir(mode=0o700); (state / "dashboard").mkdir(mode=0o700)
    return state


class SimulatedServer:
    """No socket/client. Accepted is a modelled absolute deadline, not phone proof."""
    account_id, base_url = "synthetic-runtime-benchmark", "memory://native-runtime-benchmark"

    def __init__(self, own, thread, config, components, *, clock=time.monotonic, sleep=time.sleep):
        self.own, self.thread, self.config, self.components = own, thread, config, components
        self.clock, self.sleep, self.lock = clock, sleep, threading.RLock()
        self.events, self.messages, self.submissions, self.operation_counts = [], {}, {}, Counter()
        self.acknowledged = self.delivered = "0"
        self.account_gate_available_at = 0.0
        self.operation_seconds = {}

    def delay(self, operation, index=None, base=None):
        with self.lock:
            if index is None:
                index = self.operation_counts[operation]
                self.operation_counts[operation] += 1
            if base is None:
                samples = self.config["samples"][operation]
                base = samples[index % len(samples)]
            token = digest([self.config["seed"], operation, index])
            unit = int(token[:12], 16) / (16 ** 12 - 1)
            seconds = max(0, base + self.config["extra_seconds"].get(operation, 0)
                          + (2 * unit - 1) * self.config["jitter_seconds"].get(operation, 0))
            return seconds

    def pause(self, operation):
        duration = self.delay(operation)
        self.sleep(duration)
        with self.lock:
            self.operation_seconds.setdefault(operation, []).append(duration)

    def consumer(self, name):
        return {"consumerId": name, "required": True, "registeredAfter": "0",
                "acknowledgedCursor": self.acknowledged, "deliveredCursor": self.delivered}

    def activate_native_consumer(self, consumer, *, own_username):
        if own_username != self.own:
            raise ValueError("simulation_account_mismatch")
        duration = self.delay("activation")
        with self.lock:
            # Native activation acquires the same account gate as quote RPC.
            # Keep its measured work unchanged; only restore the real lock
            # dependency that v1 omitted. No simulator mutex spans the wait.
            effective = max(self.clock(), self.account_gate_available_at)
            deadline = effective + duration
            self.account_gate_available_at = deadline
        self.sleep(max(0, deadline - self.clock()))
        with self.lock:
            self.operation_seconds.setdefault("activation", []).append(duration)
            return {"consumer": self.consumer(consumer), "checkpoint": {
                "schemaVersion": 1, "accountId": self.account_id, "ownUsername": self.own,
                "ownershipGeneration": "synthetic", "serverId": "synthetic", "receivedBoundary": "0",
                "eventCursor": "0", "readiness": "native_ingress_caught_up"}}

    def receive(self, *, consumer, after, limit):
        self.pause("receive")
        with self.lock:
            rows = [row for row in self.events if int(row["cursor"]) > int(after)][:limit]
            cursor = rows[-1]["cursor"] if rows else after
            self.delivered = str(max(int(cursor), int(self.delivered)))
            return deepcopy({"items": rows, "nextCursor": cursor, "consumer": self.consumer(consumer)})

    def ack(self, consumer, cursor):
        self.pause("ack")
        with self.lock:
            if not int(self.acknowledged) <= int(cursor) <= int(self.delivered):
                raise ValueError("simulation_ack_out_of_range")
            self.acknowledged = cursor
            return {"cursor": cursor, "messageReadStateChanged": False}

    def get_received_message(self, received_id):
        self.pause("snapshot")
        with self.lock:
            return deepcopy(self.messages[received_id])

    def require_account(self, own):
        self.pause("preflight")
        if own != self.own:
            raise ValueError("simulation_account_mismatch")

    def inject(self, case, sender):
        with self.lock:
            sequence = str(len(self.events) + 1)
            sid, received = str(18000000000000000000 + int(sequence)), "runtime-source-" + sequence
            now = datetime.now(timezone.utc).isoformat()
            source = {"receivedId": received, "firstReceivedSequence": sequence, "accountId": self.account_id,
                "ownUsername": self.own, "threadId": self.thread, "serverId": sid, "senderId": sender,
                "direction": "incoming", "type": 1, "appMessageType": None, "createdAt": now,
                "body": case["text"], "mentions": {"state": "known", "userIds": [self.own]}, "quote": None,
                "quoteEligible": True, "disposition": "ready", "reason": None}
            source["sourceSha256"] = digest(source)
            self.messages[received] = source
            injected = self.clock()
            self.events.append({"cursor": sequence, "createdAt": now, "kind": "message.received", "data": {
                "receivedId": received, "firstReceivedSequence": sequence, "disposition": "ready"}})
            return {"request_id": "protocol:" + digest([self.own, self.thread, sid]),
                    "source": deepcopy(source), "injected": injected}

    def _result(self, key):
        row = self.submissions[key]
        accepted = self.clock() >= row["accepted_at"]
        intent = {"id": "synthetic-send-" + row["sequence"], "clientId": row["sequence"],
            "accountId": self.account_id, "destination": self.thread,
            "state": "accepted" if accepted else "submitting"}
        if accepted:
            intent["serverId"] = str(17000000000000000000 + int(row["sequence"]))
        return self.components["SendResult"](idempotency_key=key, outcome="accepted" if accepted else "submitting", intent=intent)

    def send_received_quote(self, *, thread_id, received_id, server_id, source_sha256, text, idempotency_key):
        with self.lock:
            source = self.messages[received_id]
            if (thread_id, server_id, source_sha256) != (source["threadId"], source["serverId"], source["sourceSha256"]):
                raise self.components["GatewayError"]("simulated_quote_source_mismatch")
            command = {"thread_id": thread_id, "received_id": received_id, "server_id": server_id,
                       "source_sha256": source_sha256, "text": text, "idempotency_key": idempotency_key}
            existing = self.submissions.get(idempotency_key)
            if existing:
                existing["post_count"] += 1
                if command != existing["command"]:
                    raise self.components["GatewayError"]("gateway_idempotency_conflict")
                return self._result(idempotency_key)
            index = int(source["firstReceivedSequence"]) - 1
            samples = self.config["send_samples"][index % len(self.config["send_samples"])]; started = self.clock()
            effective = max(started, self.account_gate_available_at)
            accepted_delay = self.delay("accepted", index, samples["accepted"])
            http_delay = self.delay("http_return", index, samples["http_return"])
            accepted_at, http_deadline = effective + accepted_delay, effective + http_delay
            self.account_gate_available_at = accepted_at
            self.submissions[idempotency_key] = {"sequence": source["firstReceivedSequence"], "command": command,
                "post_count": 1, "submitted_at": started, "effective_command_at": effective,
                "account_gate_wait_seconds": effective - started, "accepted_at": accepted_at,
                "http_return_deadline": http_deadline, "accepted_delay": accepted_delay, "http_delay": http_delay}
        # These deadlines share the command start. Never sleep HTTP + accepted.
        self.sleep(max(0, http_deadline - self.clock()))
        with self.lock:
            self.submissions[idempotency_key]["http_returned_at"] = self.clock()
            return self._result(idempotency_key)

    def get_send_status(self, key):
        self.pause("status")
        with self.lock:
            if key not in self.submissions:
                raise self.components["GatewayError"]("gateway_send_key_not_claimed")
            return self._result(key)


class SimulatedGateway:
    """Distinct lane client objects, one shared remote state; no HTTP transport."""
    def __init__(self, server):
        self.server, self.account_id, self.base_url = server, server.account_id, server.base_url

    def __getattr__(self, name):
        if name not in {"activate_native_consumer", "receive", "ack", "get_received_message",
                        "require_account", "send_received_quote", "get_send_status"}:
            raise AttributeError(name)
        return getattr(self.server, name)


def latency_summary(values, *, target_seconds=3, timeout_seconds=30):
    ordered = sorted(math.inf if value is None else value for value in values)
    def quantile(fraction):
        if not ordered:
            return None
        value = ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]
        return "infinity" if math.isinf(value) else value
    return {"count": len(ordered), "missing_count": sum(value is None for value in values),
        "timeout_count": sum(value is None or value > timeout_seconds for value in values),
        "over_target_count": sum(value is None or value > target_seconds for value in values),
        "p50_seconds": quantile(.5), "p95_seconds": quantile(.95), "max_seconds": quantile(1),
        "quantile": "nearest_rank; missing=positive_infinity; full planned denominator"}


def final_matches(text, *, day, item, status, recorded):
    prefix = "记好啦|记好了|已记录|已经记录|已完成报备|已经记好了" if recorded else "查好啦|查好了|我查过了|查询结果|本次查询结果"
    words = {"available": "可用|还有|仍然可用", "unavailable": "已无|已经没有了|没有了|已经用完了|已用完"}
    pattern = (rf"(?:(?:{prefix})[，,:：\s]*)?{re.escape(day)}[，,:：\s]*{re.escape(item)}\s*"
        rf"(?:目前|当前|现在)?(?:状态)?(?:是|为)?\s*(?:{words[status]})[啦哦喔呢]?[。！!~～\s]*")
    return isinstance(text, str) and re.fullmatch(pattern, text.strip()) is not None


def inspect_case(case, injected, frozen, outbox_row, submission, expected, report_rows, audit_rows, *, item_name, item_aliases=()):
    """Independent fixture oracle; never derives expected truth from final/model."""
    errors = []
    if frozen is None or outbox_row is None:
        return ["missing_frozen_reply"]
    if outbox_row.get("state") != "accepted":
        errors.append("local_outbox_not_accepted")
    evidence, final = frozen["evidence"], frozen["text"]
    intended = injected["source"]
    native_dto = {**intended, "id": intended["receivedId"]}  # Inbox's documented local custody alias.
    if (frozen["source"] != native_dto or outbox_row["source"] != native_dto
            or frozen["request_id"] != injected["request_id"] or outbox_row["request_id"] != injected["request_id"]
            or outbox_row["text"] != final or outbox_row["evidence"] != evidence):
        errors.append("frozen_identity_text_or_evidence_mismatch")
    if not isinstance(final, str) or not final.strip() or evidence.get("failure") or evidence.get("recovery"):
        errors.append("empty_or_failed_generation")
    if not evidence.get("usage_run_id") or evidence.get("request", {}).get("event_id") != injected["request_id"]:
        errors.append("current_request_evidence_missing")
    if submission is None:
        errors.append("missing_submission")
    else:
        expected_command = {"thread_id": intended["threadId"], "received_id": intended["receivedId"],
            "server_id": intended["serverId"], "source_sha256": intended["sourceSha256"],
            "text": final, "idempotency_key": frozen["reply_id"]}
        if submission["command"] != expected_command:
            errors.append("submitted_quote_or_body_mismatch")
        if submission["post_count"] != 1:
            errors.append("duplicate_submission")
    report = case["category"] == "report"
    if len(report_rows) != int(report) or len(audit_rows) != int(report):
        errors.append("report_or_audit_count_mismatch")
    def fact_matches(fact):
        return isinstance(fact, dict) and all(fact.get(key) == value for key, value in expected.items())
    if case["category"] in {"report", "query"}:
        field = "report_facts" if report else "read_facts"
        facts = evidence.get(field, {})
        if set(facts) != {expected["item_id"]} or not fact_matches(facts.get(expected["item_id"])):
            errors.append("current_request_fact_mismatch")
        tools = evidence.get("tool_results", [])
        valid = any(row.get("name") == ("report_offer_status" if report else "query_offer_status")
            and row.get("result", {}).get("ok") is True and
            (row["result"].get("recorded") is True and fact_matches(row["result"]) if report else
             row["result"].get("business_id") == expected["business_id"] and row["result"].get("day") == expected["day"]
             and any(fact_matches(fact) for fact in row["result"].get("items", []))) for row in tools)
        if not valid:
            errors.append("current_request_tool_missing_or_wrong")
        if report and (not report_rows or not audit_rows or not fact_matches(json.loads(report_rows[0]["result"]))
                       or json.loads(report_rows[0]["result"]).get("recorded") is not True
                       or audit_rows[0]["kind"] != "report"):
            errors.append("report_receipt_mismatch")
        if not final_matches(final, day=expected["day"], item=item_name, status=expected["status"], recorded=report):
            errors.append("final_business_claim_mismatch")
    elif isinstance(final, str):
        # These exact fixtures only ask for a greeting. No business claim is
        # required; fail conservatively rather than bless facts from history.
        subjects = "|".join(re.escape(value) for value in (item_name, *item_aliases, "首单", "项目", "报备", "登记", "状态") if value)
        if re.search(rf"(?:{subjects}).{{0,30}}(?:已无|可用|已.*(?:登记|报备)|没有了|状态)|(?:已报备|已登记)", final):
            errors.append("unsolicited_business_claim_in_chat")
    return errors


def original_ledger_fingerprint(store, scope):
    rows = {"offers": [tuple(row) for row in store.conn.execute(
        "SELECT * FROM offers WHERE business_id<>? ORDER BY business_id,day,item_id", (scope,))],
        "reports": [tuple(row) for row in store.conn.execute(
        "SELECT p.* FROM reports p JOIN requests r ON p.event_id=r.event_id WHERE r.business_id<>? ORDER BY p.event_id,p.item_id", (scope,))],
        "audits": [tuple(row) for row in store.conn.execute(
        "SELECT * FROM audit_log WHERE business_id IS NOT NULL AND business_id<>? ORDER BY id", (scope,))]}
    return {key: {"count": len(value), "sha256": digest(value)} for key, value in rows.items()}


def submission_set_errors(submissions, expected_reply_ids, expected_received_ids):
    errors = []
    if set(submissions) != set(expected_reply_ids):
        errors.append("unexpected_or_missing_submission_key")
    counts = Counter(row["command"]["received_id"] for row in submissions.values())
    if counts != Counter({key: 1 for key in expected_received_ids}):
        errors.append("unexpected_or_duplicate_submitted_source")
    return errors


def run_benchmark(source_state, output, *, group_ref, own, thread, code_root=ROOT, schedule=None,
                  transport=None, real_model=False, provider_factory=None, timeout_seconds=30):
    if provider_factory is None and real_model is not True:
        raise ValueError("real_model_opt_in_required")
    if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 300:
        raise ValueError("invalid_benchmark_timeout")
    schedule = validate_schedule(default_schedule() if schedule is None else schedule)
    transport = transport_configuration() if transport is None else deepcopy(transport)
    components, build = load_components(code_root)
    state = clone_state(source_state, output); output = Path(output)
    old_root = os.environ.get("WEKEEPER_BENCHMARK_CODE_ROOT")
    os.environ["WEKEEPER_BENCHMARK_CODE_ROOT"] = str(Path(code_root).resolve())
    def restore_environment():
        if old_root is None:
            os.environ.pop("WEKEEPER_BENCHMARK_CODE_ROOT", None)
        else:
            os.environ["WEKEEPER_BENCHMARK_CODE_ROOT"] = old_root
    with ExitStack() as stack:
        stack.callback(restore_environment)
        store = components["BotStore"](state); stack.callback(store.close)
        original = store.binding(group_ref)
        if not original or not original["enabled"] or not original["business"]["reporter_ids"]:
            raise ValueError("configured_reporter_and_group_required")
        if original["profile"]["kind"] == "siwc" and provider_factory is None:
            raise ValueError("benchmark_requires_explicit_siwc_credentials")
        matches = [item for item in original["business"]["items"] if item["name"] == "娱乐首单"]
        if len(matches) != 1:
            raise ValueError("benchmark_project_mapping_missing")
        item = matches[0]
        scope = "native-runtime-" + digest(str(output.resolve()))[:24]
        original_ledger_before = original_ledger_fingerprint(store, scope)
        settings = store.config(); namespace = deepcopy(original["business"]); namespace["id"] = scope
        settings["businesses"].append(namespace)
        next(group for group in settings["groups"] if group["group_ref"] == group_ref)["business_id"] = scope
        store.configure(settings)
        binding = store.binding(group_ref)
        if any(binding[key] != original[key] for key in ("profile", "persona")):
            raise ValueError("benchmark_configuration_changed")
        server = SimulatedServer(own, thread, transport, components)
        clients = [SimulatedGateway(server) for _ in range(3)]
        inbox = components["ProtocolNativeInbox"](clients[0], path=output / "inbox.sqlite", own_username=own, consumer="runtime-benchmark")
        stack.callback(inbox.close); inbox.activate()
        class TimedOutbox(components["ProtocolOutbox"]):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.observed_accepted, self.observed_frozen, self.observation_lock = {}, {}, threading.Lock()
            def _save(self, reply_id, **kwargs):
                result = super()._save(reply_id, **kwargs)
                if result["state"] == "accepted":
                    with self.observation_lock:
                        self.observed_accepted.setdefault(reply_id, time.monotonic())
                return result
            def freeze(self, **kwargs):
                result = super().freeze(**kwargs)
                with self.observation_lock:
                    self.observed_frozen.setdefault(result["reply_id"], time.monotonic())
                return result
        outbox = TimedOutbox(output / "outbox.sqlite", account_id=server.account_id, own_username=own)
        stack.callback(outbox.close)
        resources = components["ProviderResources"](store, state_dir=state, usage_dir=output / "usage", dashboard_dir=state / "dashboard")
        stack.callback(resources.close)
        provider_calls = []
        def provider(profile):
            provider_calls.append(time.monotonic())
            return (provider_factory or resources.provider)(profile)
        history = components["ProtocolHistoryStore"](store, outbox, own_username=own, account_id=server.account_id)
        generator = components["ReplyGenerator"](history, state_dir=state, provider_factory=provider)
        worker = components["ProtocolBusinessWorker"](inbox=inbox, store=store, generator=generator, outbox=outbox,
            bindings={thread: group_ref}, own_username=own, account_id=server.account_id, concurrency=2,
            usage_ledger=resources.usage_ledger())
        runtime = components["ProtocolRuntime"](inbox=inbox, business=worker, outbox=outbox,
            receive_gateway=clients[0], send_gateway=clients[1], status_gateway=clients[2],
            interval=.05, status_interval=1.0, close_callback=worker.close)
        stack.callback(runtime.close)
        manifest = {"schema": 1, "suite": "scheduled_native_runtime_30", "schedule": schedule,
            "schedule_sha256": digest(schedule), "transport": transport, "transport_sha256": digest(transport),
            "build": build, "interval_seconds": .05, "status_interval_seconds": 1.0, "concurrency": 2,
            "timeout_seconds": timeout_seconds, "model_execution": "real_configured_provider" if provider_factory is None else "injected_fake_provider_real_MCP",
            "model_profile": {key: original["profile"].get(key) for key in ("id", "kind", "model", "reasoning_effort")},
            "phone_received": False, "wechat_sent": False, "clock": "one process time.monotonic",
            "scope": "event available at synthetic native receive -> simulated remote acceptance and real local outbox acceptance; excludes actual receive/decode and phone"}
        private_json(output / "manifest.json", manifest)
        planned = []; version, latest = 0, None
        started = time.monotonic(); day = components["business_day"](time.time())
        runtime.start()
        for case in schedule:
            time.sleep(max(0, started + case["at_seconds"] - time.monotonic()))
            injected = server.inject(case, namespace["reporter_ids"][0])
            source = injected["source"]
            if components["business_day"](source["createdAt"]) != day:
                raise ValueError("benchmark_crossed_business_day")
            if case["category"] == "report":
                version += 1
                latest = {"business_id": scope, "day": day, "item_id": item["id"], "status": case["write_status"],
                    "version": version, "event_id": injected["request_id"],
                    "message_ref": "native:" + injected["request_id"].removeprefix("protocol:"),
                    "sender_id": source["senderId"], "group_ref": group_ref}
            if latest is None:
                raise ValueError("benchmark_requires_report_before_query")
            planned.append({"case": case, "injected": injected, "expected": deepcopy(latest)})
            print(json.dumps({"event": "injected", "case": case["id"], "offset_seconds": injected["injected"] - started}), flush=True)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            with outbox.observation_lock:
                if len(outbox.observed_accepted) == len(schedule):
                    break
            time.sleep(.02)
        cutoff = time.monotonic()
        # Stop all lanes before inspecting ledgers. Shutdown drains in-flight
        # work, but observations after cutoff remain missing for this run.
        runtime.close(); runtime_state = runtime.status()
        results, expected_reply_ids = [], []
        for plan in planned:
            case, injected, expected = plan["case"], plan["injected"], plan["expected"]
            identifier = injected["request_id"]; frozen = store.get("protocol_frozen_" + identifier)
            if frozen:
                expected_reply_ids.append(frozen["reply_id"])
            row = outbox.get(frozen["reply_id"]) if frozen and frozen["reply_id"] in outbox.observed_frozen else None
            submission = server.submissions.get(frozen["reply_id"]) if frozen else None
            remote = submission["accepted_at"] if submission and submission["accepted_at"] <= cutoff else None
            local = outbox.observed_accepted.get(frozen["reply_id"]) if frozen else None
            local = local if local is not None and local <= cutoff else None
            reports = [dict(value) for value in store.conn.execute("SELECT * FROM reports WHERE event_id=?", (identifier,))]
            audits = [dict(value) for value in store.conn.execute("SELECT * FROM audit_log WHERE event_id=? AND kind IN ('report','stale_report')", (identifier,))]
            errors = inspect_case(case, injected, frozen, row, submission, expected, reports, audits, item_name=item["name"],
                item_aliases=tuple(alias for value in namespace["items"] for alias in (value["name"], *value.get("aliases", []))))
            if remote is None or local is None:
                errors.append("missing_acceptance_at_cutoff")
            if any(value is not None and value - injected["injected"] > timeout_seconds for value in (remote, local)):
                errors.append("request_timeout")
            results.append({"case": case["id"], "category": case["category"], "scenario": case["scenario"],
                "request_id": identifier, "source": injected["source"], "expected_fact": expected,
                "schedule_seconds": case["at_seconds"], "injected_offset_seconds": injected["injected"] - started,
                "injection_lateness_seconds": max(0, injected["injected"] - started - case["at_seconds"]),
                "simulated_remote_accepted_seconds": remote - injected["injected"] if remote is not None else None,
                "local_outbox_accepted_seconds": local - injected["injected"] if local is not None else None,
                "simulated_submission": submission, "final": frozen["text"] if frozen else None,
                "evidence": frozen["evidence"] if frozen else None, "metrics": store.get("protocol_metrics_" + identifier),
                "passed": not errors, "failures": errors})
        summaries = {}
        for category in ("all", *CATEGORIES):
            rows = [row for row in results if category == "all" or row["category"] == category]
            summaries[category] = {field: latency_summary([row[field] for row in rows], timeout_seconds=timeout_seconds)
                for field in ("simulated_remote_accepted_seconds", "local_outbox_accepted_seconds")}
        lane_errors = {name: value["last_error"] for name, value in runtime_state["lane_history"].items() if value.get("last_error")}
        original_ledger_after = original_ledger_fingerprint(store, scope)
        ledger_row = store.conn.execute("SELECT * FROM offers WHERE business_id=? AND day=? AND item_id=?", (scope, day, item["id"])).fetchone()
        final_ledger_matches = ledger_row is not None and all(dict(ledger_row).get(key) == value for key, value in latest.items())
        original_preserved = original_ledger_before == original_ledger_after
        transport_errors = submission_set_errors(server.submissions, expected_reply_ids,
            [plan["injected"]["source"]["receivedId"] for plan in planned])
        correctness = (len(results) == 30 and all(row["passed"] for row in results) and not lane_errors
                       and not transport_errors and original_preserved and final_ledger_matches)
        timing_pass = all(type(summaries[category][field]["p95_seconds"]) in (int, float)
            and summaries[category][field]["p95_seconds"] <= 3 for category in CATEGORIES
            for field in ("simulated_remote_accepted_seconds", "local_outbox_accepted_seconds"))
        report = {**manifest, "cases": results, "summary": summaries, "correctness_passed": correctness,
            "simulated_timing_target_passed": timing_pass, "passed": correctness and timing_pass,
            "phone_p95_verified": False, "chat_naturalness_independently_verified": False,
            "runtime": runtime_state, "lane_errors": lane_errors, "transport_errors": transport_errors,
            "provider_instances": len(provider_calls),
            "original_ledger_preserved": original_preserved, "original_ledger_before": original_ledger_before,
            "original_ledger_after": original_ledger_after, "final_fixture_ledger_matches": final_ledger_matches,
            "duration_seconds": cutoff - started, "shutdown_seconds": time.monotonic() - cutoff,
            "limitations": ["Simulated acceptance deadlines are a calibrated model, not network observations.",
                "Production Python lanes, generator, MCP and local SQLite are exercised; ProtocolGateway client journal and Rust execution are replaced by the calibrated transport model.",
                "Native consumer activation and quote sends reserve one FIFO account gate, inferred from fixed Rust activate_native_consumer/perform_send lock scope; real burst timing is unverified. Measured operation delays are unchanged.",
                "Local acceptance is measured immediately after the production outbox commit, using a benchmark subclass observer.",
                "Current idle calibration does not establish loaded gateway tails, real receive/decode or phone timing.",
                "All thirty planned samples remain in every relevant denominator; missing is positive infinity."]}
        private_json(output / "report.json", report)
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--code-root", type=Path, default=ROOT)
    for name in ("source-state", "output", "read-calibration", "transport-config", "schedule"):
        parser.add_argument("--" + name, type=Path)
    for name in ("group-ref", "own", "thread"):
        parser.add_argument("--" + name)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--real-model", action="store_true")
    args = parser.parse_args(argv)
    schedule = validate_schedule(json.loads(args.schedule.read_text()) if args.schedule else default_schedule())
    config = transport_configuration(args.read_calibration,
        json.loads(args.transport_config.read_text()) if args.transport_config else None)
    if not args.real_model:
        print(json.dumps({"mode": "preview", "state_opened": False, "model_calls": 0, "wechat_sent": False,
                          "schedule": schedule, "schedule_sha256": digest(schedule), "transport": config}, ensure_ascii=False, indent=2))
        return 0
    if any(getattr(args, name) is None for name in ("source_state", "output", "group_ref", "own", "thread")):
        parser.error("--real-model requires --source-state, --output, --group-ref, --own and --thread")
    report = run_benchmark(args.source_state, args.output, group_ref=args.group_ref, own=args.own, thread=args.thread,
        code_root=args.code_root, schedule=schedule, transport=config, real_model=True, timeout_seconds=args.timeout_seconds)
    print(json.dumps({"passed": report["passed"], "correctness_passed": report["correctness_passed"],
        "simulated_timing_target_passed": report["simulated_timing_target_passed"], "report": str(args.output / "report.json")}))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
