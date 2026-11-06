#!/usr/bin/env python3
"""Additional in-process boundaries for the frozen v3 runtime benchmark.

The original schedule, simulator and acceptance oracle remain byte-identical.
Default invocation is still a no-state preview. This wrapper adds observers,
not production policy, and requires a fresh output directory for every run.
"""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
BASE_PATH = Path(__file__).with_name("benchmark_protocol_runtime.py")
BASE_SHA256 = '542d3bcdc368e5b71cc0d9f85e56ac56fe652722c0774491ca5fbcbaabcfe544'


def load_base():
    if hashlib.sha256(BASE_PATH.read_bytes()).hexdigest() != BASE_SHA256:
        raise ValueError("observed_benchmark_base_changed")
    spec = importlib.util.spec_from_file_location("_observed_frozen_runtime_benchmark", BASE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Boundaries:
    """In-memory timestamps only; no I/O or wrapped operation under this lock."""
    def __init__(self, *, clock=time.monotonic, max_operations=20000):
        self.clock, self.lock = clock, threading.Lock()
        self.points, self.trace_origins, self.operations = {}, {}, []
        self.max_operations, self.dropped_operations = max_operations, 0

    def point(self, kind, key, name, stamp=None):
        stamp = self.clock() if stamp is None else stamp
        with self.lock:
            self.points.setdefault((kind, key), {}).setdefault(name, stamp)

    def operation(self, name, started, ended):
        with self.lock:
            if len(self.operations) < self.max_operations:
                self.operations.append({"operation": name, "started": started, "ended": ended})
            else:
                self.dropped_operations += 1

    def trace(self, request_id, trace):
        # Reuse the existing GenerationTrace offsets, not another timer around
        # generation. This adapter deliberately rejects an incompatible trace.
        origin = getattr(trace, "_origin", None)
        snapshot = trace.snapshot()
        if type(origin) not in (int, float) or snapshot.get("offset_clock") != "process_monotonic_seconds":
            raise ValueError("observed_benchmark_trace_clock_incompatible")
        with self.lock:
            self.trace_origins[request_id] = (origin, snapshot["attempt_id"])


def observed_server(base, observer):
    class Server(base.SimulatedServer):
        def inject(self, *args, **kwargs):
            result = super().inject(*args, **kwargs)
            observer.point("request", result["request_id"], "injected", result["injected"])
            return result

        def activate_native_consumer(self, *args, **kwargs):
            started = observer.clock()
            try:
                return super().activate_native_consumer(*args, **kwargs)
            finally:
                observer.operation("activation_including_account_gate", started, observer.clock())

        def receive(self, **kwargs):
            started = observer.clock()
            result = super().receive(**kwargs)
            ended = observer.clock()
            observer.operation("receive", started, ended)
            for row in result["items"]:
                received = row.get("data", {}).get("receivedId")
                if received:
                    observer.point("received", received, "receive_started", started)
                    observer.point("received", received, "receive_returned", ended)
            return result

        def get_received_message(self, received_id):
            started = observer.clock()
            result = super().get_received_message(received_id)
            ended = observer.clock()
            observer.point("received", received_id, "snapshot_started", started)
            observer.point("received", received_id, "snapshot_returned", ended)
            return result

        def ack(self, *args, **kwargs):
            started = observer.clock()
            try:
                return super().ack(*args, **kwargs)
            finally:
                observer.operation("ack", started, observer.clock())
    return Server


def observed_components(components, observer):
    class Inbox(components["ProtocolNativeInbox"]):
        def _resolve(self, event):
            result = super()._resolve(event)
            if result is not None and result[0] and hasattr(self, "_observed_resolved"):
                self._observed_resolved.append(result[0])
            return result

        def _commit_staged(self):
            # Both the historical baseline and lock candidate have this
            # boundary. The baseline has no separate _commit_resolved method.
            started = observer.clock()
            self._observed_resolved = []
            try:
                result = super()._commit_staged()
                committed = observer.clock()
                for request_id in self._observed_resolved:
                    observer.point("request", request_id, "inbox_resolution_batch_started", started)
                    observer.point("request", request_id, "inbox_request_commit_returned", committed)
                return result
            finally:
                del self._observed_resolved

        def _stage(self, page):
            started = observer.clock()
            result = super()._stage(page)
            committed = observer.clock()
            for row in page["items"]:
                received = row.get("data", {}).get("receivedId")
                if received:
                    observer.point("received", received, "inbox_raw_append_started", started)
                    observer.point("received", received, "inbox_raw_commit_returned", committed)
            return result

    class Store(components["BotStore"]):
        def begin_request(self, event, **kwargs):
            started = observer.clock()
            result = super().begin_request(event, **kwargs)
            committed = observer.clock()
            if result["accepted"]:
                request_id = result["request"]["event_id"]
                observer.point("request", request_id, "business_admission_started", started)
                observer.point("request", request_id, "business_admission_commit_returned", committed)
            return result

        def queue_reply(self, event_id, *args, **kwargs):
            started = observer.clock()
            result = super().queue_reply(event_id, *args, **kwargs)
            committed = observer.clock()
            if result["status"] == "pending":
                observer.point("request", event_id, "business_final_commit_started", started)
                observer.point("request", event_id, "business_final_commit_returned", committed)
            return result

    class Worker(components["ProtocolBusinessWorker"]):
        def _trace_progress(self, request):
            result = super()._trace_progress(request)
            observer.trace(request["event_id"], result["_generation_trace"])
            return result

    class Outbox(components["ProtocolOutbox"]):
        def freeze(self, **kwargs):
            started = observer.clock()
            result = super().freeze(**kwargs)
            committed = observer.clock()
            observer.point("request", result["request_id"], "outbox_freeze_started", started)
            observer.point("request", result["request_id"], "outbox_freeze_commit_returned", committed)
            return result

        def _run(self, gateway, reply_id, *, submit):
            if submit:
                observer.point("reply", reply_id, "delivery_entered")
            return super()._run(gateway, reply_id, submit=submit)

    return {**components, "ProtocolNativeInbox": Inbox, "BotStore": Store,
            "ProtocolBusinessWorker": Worker, "ProtocolOutbox": Outbox}


PATH = ("injected", "receive_returned", "inbox_raw_commit_returned", "snapshot_returned",
        "inbox_request_commit_returned", "business_admission_commit_returned", "generation_queued",
        "generation_started", "generation_ended", "business_final_commit_returned",
        "outbox_freeze_commit_returned", "delivery_entered", "command_started",
        "account_gate_entered", "remote_accepted", "local_accepted")


def case_observations(case, observer):
    request = case["request_id"]
    with observer.lock:
        points = {**observer.points.get(("received", case["source"]["receivedId"]), {}),
                  **observer.points.get(("request", request), {})}
        origin = observer.trace_origins.get(request)
    trace = (case.get("metrics") or {}).get("trace") or {}
    errors = []
    if origin is not None and trace.get("attempt_id") == origin[1]:
        points.update({name: origin[0] + row["offset_seconds"] for name, row in trace["events"].items()
                       if type(row.get("offset_seconds")) in (int, float)})
    else:
        errors.append("generation_trace_origin_missing_or_mismatched")
    submission = case.get("simulated_submission")
    if submission:
        with observer.lock:
            points.update(observer.points.get(("reply", submission["command"]["idempotency_key"]), {}))
        # These are the base simulator's existing observations, not new clocks.
        points.update(command_started=submission["submitted_at"], account_gate_entered=submission["effective_command_at"],
                      http_returned=submission["http_returned_at"])
        remote = case.get("simulated_remote_accepted_seconds")
        if remote is not None:
            points["remote_accepted"] = submission["accepted_at"]
            if "injected" in points and case.get("local_outbox_accepted_seconds") is not None:
                points["local_accepted"] = points["injected"] + case["local_outbox_accepted_seconds"]
    # No inferred start from wall clocks. Missing/failed rows retain incomplete
    # observations; they are never omitted or rescued by this diagnostic.
    missing = [name for name in PATH if name not in points]
    intervals = []
    for start, end in zip(PATH, PATH[1:]):
        seconds = points[end] - points[start] if start in points and end in points else None
        if seconds is not None and seconds < 0:
            errors.append("boundary_order_conflict:" + start + ":" + end)
        intervals.append({"start": start, "end": end, "seconds": seconds})
    anchor = points.get("injected")
    return {"clock": "same_process_time.monotonic", "points_monotonic_seconds": points,
            "points_since_injection_seconds": {key: value - anchor for key, value in points.items()} if anchor is not None else None,
            "adjacent_nonoverlapping_intervals": intervals, "missing_boundaries": missing,
            "errors": errors, "complete_ordered_path": not missing and not errors,
            "semantics": "Commit return is an upper bound just after commit; scheduler races may invalidate ordering. Admission-to-generation includes scheduler checks/FIFO/nearby reads, not a pure queue metric. HTTP return and generation child spans overlap the outer path and must not be added."}


def run_benchmark(*args, **kwargs):
    base = load_base()
    observer = Boundaries()
    original_loader = base.load_components
    instrumentation = {"schema": 1, "version": "boundary_observed_v4", "base_benchmark_sha256": BASE_SHA256,
        "observer_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "production_policy_changed": False, "observation_storage": "in_memory_then_private_run_artifacts"}

    def load_components(code_root):
        components, build = original_loader(code_root)
        return observed_components(components, observer), {**build, "instrumentation": instrumentation}

    base.load_components = load_components
    base.SimulatedServer = observed_server(base, observer)
    report = base.run_benchmark(*args, **kwargs)
    for case in report["cases"]:
        case["boundary_observations"] = case_observations(case, observer)
    report["boundary_observations"] = {**instrumentation, "operations": observer.operations,
        "dropped_operations": observer.dropped_operations,
        "all_paths_complete_ordered": all(row["boundary_observations"]["complete_ordered_path"] for row in report["cases"]),
        "correctness_or_timing_oracle_unchanged": True,
        "limitations": ["Observer work adds small uncalibrated in-process overhead; compare instrumented variants using the same wrapper hash.",
            "Commit timestamps are successful method-return observations, not SQLite internal fsync spans.",
            "The original correctness and latency verdicts are unchanged; observation completeness is separate.",
            "No native Rust, wire receive/decode, client journal, or phone observation is added."]}
    output = Path(args[1] if len(args) > 1 else kwargs["output"])
    base.private_json(output / "report.json", report)
    return report


def main(argv=None):
    base = load_base()
    base.run_benchmark = run_benchmark
    return base.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
