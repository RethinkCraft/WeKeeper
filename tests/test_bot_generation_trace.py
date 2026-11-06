"""Request-local trace clocks use only synthetic providers and temporary DBs."""
import asyncio
from datetime import datetime, timezone
import json

import pytest

from chatlog_keeper.bot.providers import ProviderError
from chatlog_keeper.bot.reply_generator import GenerationTrace, generation_metrics
from chatlog_keeper.bot.service import Service
from test_bot_reply_generator import DAY, NOW, Provider, generator, source, store
from test_bot_protocol_business import harness, wait_finished


def test_callbacks_keep_reported_duration_separate_from_observed_end_and_mcp(store, tmp_path):
    event = source("trace", "查询娱乐首单")
    request = store.begin_request(event)["request"]

    def complete(messages, instructions, tools, handler, metadata):
        observe = metadata["_on_event"]
        for stage, seconds in [("model_call", .02), ("model_call_1", .02), ("usage_start", .003),
                               ("model_call_2", float("nan")), ("usage_finish", float("inf")),
                               ("usage_init", True), ("usage_queue_wait", -1),
                               ("private source body", 1), (["not a stage"], 1)]:
            observe({"type": "timing", "stage": stage, "seconds": seconds})
        result = handler("query_offer_status", {"item": "fun", "date": DAY})
        assert result["ok"]
        return "未知"

    result = asyncio.run(generator(store, tmp_path, Provider(complete)).generate(event, request, []))
    trace = result.metrics["trace"]
    assert trace["events"]["generation_ended"]["outcome"] == "completed"
    assert datetime.fromisoformat(trace["host_utc_anchor"]).tzinfo is not None
    assert trace["offset_clock"] == "process_monotonic_seconds"
    spans = {row["name"]: row for row in trace["spans"]}
    for name in ("model_call", "model_call_1", "usage_start"):
        assert spans[name]["boundary"] == "callback_observed_end_only"
        assert spans[name]["start_offset_seconds"] is None
        assert spans[name]["end_offset_seconds"] >= 0
    assert spans["model_call"]["role"] == "aggregate"
    assert spans["model_call_1"]["role"] == "child"
    assert spans["mcp"]["boundary"] == "host_measured" and spans["mcp"]["index"] == 1
    assert spans["mcp"]["end_offset_seconds"] >= spans["mcp"]["start_offset_seconds"]
    assert "do_not_sum" in trace["aggregation"]
    assert {"model_call_2", "usage_finish", "usage_init", "usage_queue_wait"}.isdisjoint(result.metrics["timings"])
    assert "private source body" not in json.dumps(result.metrics)
    assert "arguments" not in json.dumps(result.metrics)
    json.dumps(result.metrics, allow_nan=False)


def test_failed_generator_retains_timing_without_exception_text(store, tmp_path):
    event = source("failed-trace")
    request = store.begin_request(event)["request"]
    progress = {}

    def complete(messages, instructions, tools, handler, metadata):
        metadata["_on_event"]({"type": "timing", "stage": "model_call_1", "seconds": .4})
        raise ProviderError("private exception and credentials must not become metrics")

    with pytest.raises(ProviderError):
        asyncio.run(generator(store, tmp_path, Provider(complete)).generate(event, request, [], progress))
    metrics = generation_metrics(progress)
    assert metrics["timings"]["model_call_1"] == .4
    assert metrics["trace"]["events"]["generation_ended"]["outcome"] == "failed"
    assert metrics["trace"]["events"]["reply_generator_ended"]["outcome"] == "failed"
    assert "private exception" not in json.dumps(metrics)


def test_trace_bounds_sanitizes_clock_observations_and_detaches_snapshots():
    now = [10.0]
    trace = GenerationTrace(monotonic=lambda: now[0],
                            utc_now=lambda: datetime(2026, 10, 29, tzinfo=timezone.utc))
    trace.mark("generation_queued")
    now[0] = 11.0
    trace.mark("generation_started")
    for _ in range(70):
        trace.span("model_call_1", .1, observed=True)
    trace.span("private model output", 1, observed=True)
    trace.wall_observation("inbox_batch_staging_before_commit", "secret invalid time")
    trace.wall_observation("arbitrary_body", "2026-10-29T00:00:00Z")
    snapshot = trace.snapshot()
    assert len(snapshot["spans"]) == 64 and snapshot["dropped_observations"] == 6
    assert snapshot["wall_observations"] == {}
    assert snapshot["events"]["generation_queued"]["offset_seconds"] == 0
    assert snapshot["events"]["generation_started"]["offset_seconds"] == 1
    snapshot["spans"].clear()
    assert len(trace.snapshot()["spans"]) == 64
    progress = {"_generation_trace": trace, "timings": {"mcp": .1, "secret": 2, "model_call": float("nan")},
                "event": {"text": "secret"}, "future": object()}
    assert generation_metrics(progress)["timings"] == {"mcp": .1}
    assert "secret" not in json.dumps(generation_metrics(progress), allow_nan=False)


def test_business_failure_freezes_same_request_trace_and_batch_staging_label(harness):
    h = harness
    key = h.inbox.add("10", "private request body")

    async def fail(event, request, nearby, progress=None):
        progress["timings"]["model_call_1"] = .25
        progress["_generation_trace"].span("model_call_1", .25, observed=True)
        raise RuntimeError("private provider exception")

    h.generator.generate = fail
    h.worker().run_once(block=True)
    metrics = h.store.get("protocol_metrics_" + key)
    trace = metrics["trace"]
    assert metrics["timings"]["model_call_1"] == .25
    assert trace["events"]["generation_ended"]["outcome"] == "failed"
    offsets = [trace["events"][name]["offset_seconds"] for name in
               ("generation_queued", "generation_started", "generation_ended", "final_freeze_begin")]
    assert offsets == sorted(offsets)
    assert set(trace["wall_observations"]) == {"inbox_batch_staging_before_commit", "business_admission_before_commit"}
    assert trace["missing"]["final_commit_end"] == "trace_frozen_before_transaction_commit"
    assert "private request body" not in json.dumps(metrics)
    assert "private provider exception" not in json.dumps(metrics)


def test_recovered_committed_report_does_not_invent_previous_generation_clocks(harness):
    h = harness
    key = h.inbox.add("10", "今日娱乐首单已无，请报备")
    worker = h.worker(); worker.admit_pending()
    assert Service(h.store, {"event_id": key, "writable": True}).call("report_offer_status",
        {"item": "fun", "status": "unavailable", "evidence": "今日娱乐首单已无，请报备"})["recorded"]
    worker.run_once(block=True)
    trace = h.store.get("protocol_metrics_" + key)["trace"]
    assert h.generator.calls == []
    assert "generation_started" not in trace["events"] and "generation_queued" not in trace["events"]
    assert trace["events"]["generation_ended"]["outcome"] == "recovered_without_generation"
    assert trace["spans"] == []
    assert trace["missing"]["crashed_prior_attempt"] == "unfrozen_in_memory_observations_are_unavailable"


def test_concurrent_requests_have_independent_attempts_and_trace_only_freezes_with_final(harness):
    h = harness
    keys = [h.inbox.add("10"), h.inbox.add("20", thread="c@chatroom")]
    worker = h.worker(); worker.run_once(); wait_finished(worker)
    assert all(h.store.get("protocol_metrics_" + key) is None for key in keys)
    assert all(h.store.reply(key)["status"] == "awaiting_generation" for key in keys)
    worker.drain_completed()
    traces = [h.store.get("protocol_metrics_" + key)["trace"] for key in keys]
    assert len({trace["attempt_id"] for trace in traces}) == 2
    assert len({trace["runtime_id"] for trace in traces}) == 1
    for key in keys:
        assert h.store.get("protocol_frozen_" + key) is not None
        assert h.store.reply(key)["status"] == "pending"

