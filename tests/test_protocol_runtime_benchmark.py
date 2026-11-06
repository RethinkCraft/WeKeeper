"""Calibration tests for the native full-runtime bench; never a live sender."""
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import threading

import pytest

from chatlog_keeper.bot.protocol_gateway import GatewayError, SendResult
from chatlog_keeper.bot.protocol_native_inbox import ProtocolNativeInbox
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config

spec = importlib.util.spec_from_file_location("runtime_benchmark", Path(__file__).parents[1] / "scripts/benchmarks/benchmark_protocol_runtime.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class Clock:
    def __init__(self):
        self.now, self.sleeps = 100.0, []

    def __call__(self):
        return self.now

    def sleep(self, duration):
        self.sleeps.append(duration)
        self.now += duration


def transport(accepted=.2, returned=.1):
    return bench.transport_configuration(overrides={
        "samples": {name: [0] for name in bench.OPERATIONS - {"accepted", "http_return"}},
        "send_samples": [{"accepted": accepted, "http_return": returned}]})


def peer(clock, accepted=.2, returned=.1):
    return bench.SimulatedServer("robot", "room@chatroom", transport(accepted, returned),
                                {"SendResult": SendResult, "GatewayError": GatewayError}, clock=clock, sleep=clock.sleep)


def submit(server, injection, key):
    source = injection["source"]
    return server.send_received_quote(thread_id=source["threadId"], received_id=source["receivedId"],
        server_id=source["serverId"], source_sha256=source["sourceSha256"], text="fixture final", idempotency_key=key)


@pytest.mark.parametrize("accepted,returned,outcome", [(.2, .1, "submitting"), (.1, .2, "accepted")])
def test_http_return_and_acceptance_share_start_instead_of_adding(accepted, returned, outcome):
    clock = Clock(); server = peer(clock, accepted, returned)
    source = server.inject({"text": "same body"}, "manager")
    result = submit(server, source, "reply-1")
    assert result.outcome == outcome
    assert clock.now == pytest.approx(100 + returned)
    assert server.submissions["reply-1"]["accepted_at"] == pytest.approx(100 + accepted)
    clock.sleep(max(0, 100 + accepted - clock.now))
    assert server.get_send_status("reply-1").outcome == "accepted"
    assert server.submissions["reply-1"]["post_count"] == 1
    assert result.phone_received is False


def test_same_account_gate_serializes_second_command_until_first_acceptance():
    clock = Clock(); server = peer(clock, .6, .1)
    first = server.inject({"text": "same body"}, "manager")
    second = server.inject({"text": "same body"}, "manager")
    submit(server, first, "r1"); submit(server, second, "r2")
    assert first["request_id"] != second["request_id"]
    row = server.submissions["r2"]
    assert row["submitted_at"] == pytest.approx(100.1)
    assert row["effective_command_at"] == pytest.approx(100.6)
    assert row["account_gate_wait_seconds"] == pytest.approx(.5)
    assert row["accepted_at"] == pytest.approx(101.2)
    assert clock.now == pytest.approx(100.7)


def test_activation_shares_send_gate_without_inflating_measured_operation_delay():
    clock = Clock(); server = peer(clock, .6, .1)
    server.config["samples"]["activation"] = [.01]
    first = server.inject({"text": "same body"}, "manager")
    second = server.inject({"text": "same body"}, "manager")
    submit(server, first, "r1")
    server.activate_native_consumer("consumer", own_username="robot")
    assert clock.now == pytest.approx(100.61)  # .5 existing send wait + unchanged .01 activation.
    assert server.operation_seconds["activation"] == [.01]
    submit(server, second, "r2")
    assert server.submissions["r2"]["effective_command_at"] == pytest.approx(100.61)


@pytest.mark.parametrize("legacy_whole_poll_lock", [False, True])
def test_activation_gate_wait_blocks_legacy_custody_lock_but_not_candidate(tmp_path, legacy_whole_poll_lock):
    # Negative control exactly restores the old whole-poll DB-lock boundary;
    # it is not an alternate runtime implementation or a benchmark result.
    class LegacyLockScope(ProtocolNativeInbox):
        def poll(self, **kwargs):
            with self._lock:
                return super().poll(**kwargs)
    clock = Clock(); server = peer(clock, .6, .1)
    source = server.inject({"text": "same body"}, "manager")
    klass = LegacyLockScope if legacy_whole_poll_lock else ProtocolNativeInbox
    with klass(bench.SimulatedGateway(server), path=tmp_path / "inbox.sqlite", own_username="robot", consumer="fixture") as inbox:
        inbox.activate(); submit(server, source, "r1")
        # Production revalidates activation sparsely. Exercise that explicit
        # branch here; an ordinary poll need not acquire the account gate.
        inbox._needs_activation = True
        entered, release = threading.Event(), threading.Event()
        def blocked_sleep(duration):
            if duration > .3:
                entered.set(); assert release.wait(3)
            clock.sleep(duration)
        server.sleep = blocked_sleep
        with ThreadPoolExecutor(3) as pool:
            receiving = pool.submit(inbox.poll)
            try:
                assert entered.wait(1)
                # Snapshot/preflight do not use the account gate, and the
                # simulator's state mutex is not held while activation waits.
                assert pool.submit(server.get_received_message, source["source"]["receivedId"]).result(timeout=1)["serverId"] == source["source"]["serverId"]
                pool.submit(server.require_account, "robot").result(timeout=1)
                short_read = pool.submit(inbox.pending_requests)
                if legacy_whole_poll_lock:
                    with pytest.raises(TimeoutError):
                        short_read.result(timeout=.05)
                else:
                    assert short_read.result(timeout=1) == []
            finally:
                release.set()
            assert receiving.result(timeout=1).new_requests == 1
            short_read.result(timeout=1)


def test_wrong_source_rejected_and_duplicate_submit_visible():
    clock = Clock(); server = peer(clock)
    source = server.inject({"text": "same body"}, "manager")
    wrong = deepcopy(source); wrong["source"]["serverId"] = "123"
    with pytest.raises(GatewayError, match="simulated_quote_source_mismatch"):
        submit(server, wrong, "r1")
    submit(server, source, "r1"); submit(server, source, "r1")
    assert server.submissions["r1"]["post_count"] == 2


def test_full_planned_denominator_keeps_missing_and_nearest_rank_calibrated():
    complete = bench.latency_summary(list(range(1, 11)))
    assert (complete["p50_seconds"], complete["p95_seconds"], complete["max_seconds"]) == (5, 10, 10)
    failed = bench.latency_summary([1] * 9 + [None])
    assert failed["count"] == 10 and failed["missing_count"] == failed["timeout_count"] == 1
    assert failed["p50_seconds"] == 1 and failed["p95_seconds"] == failed["max_seconds"] == "infinity"


def test_schedule_has_30_fixed_injections_repeated_text_burst_and_idle():
    rows = bench.validate_schedule(bench.default_schedule())
    assert all(sum(row["category"] == category for row in rows) == 10 for category in bench.CATEGORIES)
    assert len({row["text"] for row in rows if row["category"] == "query"}) == 1
    burst = [row for row in rows if row["scenario"] == "continuous_report_query"]
    assert burst[1]["at_seconds"] - burst[0]["at_seconds"] == pytest.approx(.1)
    idle = next(index for index, row in enumerate(rows) if row["scenario"] == "idle_first")
    assert rows[idle]["at_seconds"] - rows[idle - 1]["at_seconds"] == 30
    with pytest.raises(ValueError, match="thirty"):
        bench.validate_schedule(rows[:-1])


def test_read_calibration_uses_full_per_operation_samples_once_and_hashes(tmp_path):
    source = tmp_path / "reads.json"
    source.write_text(json.dumps({"samples": [{"operation": operation, "seconds": value}
        for operation in ("existing_activation", "required_consumer_event_read", "received_snapshot", "require_account")
        for value in (.01, .02)]}))
    configuration = bench.transport_configuration(source)
    assert configuration["samples"]["activation"] == [.01, .02]
    assert configuration["samples"]["snapshot"] == [.01, .02]
    assert len(configuration["provenance"]["read_file_sha256"]) == 64
    clock = Clock()
    server = bench.SimulatedServer("robot", "room", configuration, {}, clock=clock, sleep=clock.sleep)
    server.activate_native_consumer("consumer", own_username="robot")
    assert clock.now == pytest.approx(100.01)  # Not .01 activation + .01 preflight.
    assert configuration["send_samples"][0]["accepted"] == 1.774814  # Includes entry queue.


def valid_oracle_fixture():
    source = {"receivedId": "received1", "threadId": "room", "serverId": "18446744073709551614",
              "sourceSha256": "a" * 64}
    expected = {"business_id": "fixture", "day": "2026-10-30", "item_id": "fun", "status": "unavailable", "version": 1}
    receipt = {**expected, "ok": True, "recorded": True}
    evidence = {"request": {"event_id": "request1"}, "usage_run_id": "usage1", "report_facts": {"fun": receipt},
                "tool_results": [{"name": "report_offer_status", "result": receipt}]}
    final = "记好啦，2026-10-30，娱乐首单已无。"
    frozen = {"request_id": "request1", "reply_id": "reply1", "source": {**source, "id": "received1"},
              "text": final, "evidence": evidence, "state": "accepted"}
    command = {"thread_id": "room", "received_id": "received1", "server_id": source["serverId"],
               "source_sha256": source["sourceSha256"], "text": final, "idempotency_key": "reply1"}
    return {"case": {"category": "report"}, "injected": {"source": source, "request_id": "request1"},
        "frozen": frozen, "outbox_row": deepcopy(frozen), "submission": {"command": command, "post_count": 1},
        "expected": expected, "report_rows": [{"result": json.dumps(receipt)}],
        "audit_rows": [{"kind": "report"}], "item_name": "娱乐首单"}


@pytest.mark.parametrize("fault,expected_error", [
    ("wrong_final", "final_business_claim_mismatch"), ("wrong_quote", "submitted_quote_or_body_mismatch"),
    ("duplicate", "duplicate_submission"), ("missing_tool", "current_request_tool_missing_or_wrong"),
    ("stale_version", "current_request_fact_mismatch"), ("missing_audit", "report_or_audit_count_mismatch"),
    ("wrong_freeze", "frozen_identity_text_or_evidence_mismatch"), ("not_accepted", "local_outbox_not_accepted"),
])
def test_independent_oracle_rejects_false_success(fault, expected_error):
    value = valid_oracle_fixture()
    assert bench.inspect_case(**value) == []
    if fault == "wrong_final":
        value["frozen"]["text"] = "2026-10-30，娱乐首单可用。"
    elif fault == "wrong_quote":
        value["submission"]["command"]["server_id"] = "18446744073709551613"
    elif fault == "duplicate":
        value["submission"]["post_count"] = 2
    elif fault == "missing_tool":
        value["frozen"]["evidence"]["tool_results"] = []
    elif fault == "stale_version":
        value["frozen"]["evidence"]["report_facts"]["fun"]["version"] = 0
    elif fault == "missing_audit":
        value["audit_rows"] = []
    elif fault == "wrong_freeze":
        value["frozen"]["source"]["id"] = "wrong"
    elif fault == "not_accepted":
        value["outbox_row"]["state"] = "unknown"
    assert expected_error in bench.inspect_case(**value)


def test_chat_historical_business_claim_does_not_get_certified():
    value = valid_oracle_fixture(); value["case"]["category"] = "chat"
    value["report_rows"] = []; value["audit_rows"] = []
    value["frozen"]["text"] = "我在的，你说～今天娱乐首单已经报备过了，状态是已无。"
    assert "unsolicited_business_claim_in_chat" in bench.inspect_case(**value)
    value["frozen"]["text"] = "今天娱乐已无。"
    assert "unsolicited_business_claim_in_chat" in bench.inspect_case(**value, item_aliases=("娱乐",))


def test_extra_key_sending_same_source_fails_whole_submission_set():
    value = valid_oracle_fixture()
    sends = {"reply1": value["submission"]}
    assert bench.submission_set_errors(sends, ["reply1"], ["received1"]) == []
    sends["extra-reply"] = deepcopy(value["submission"])
    failures = bench.submission_set_errors(sends, ["reply1"], ["received1"])
    assert "unexpected_or_missing_submission_key" in failures
    assert "unexpected_or_duplicate_submitted_source" in failures


class FakeProvider:
    def complete(self, messages, instructions, tools, call, metadata):
        if metadata.get("_required_report"):
            requirement = metadata["_required_report"]
            call("report_offer_status", {"item": "娱乐首单", "status": requirement["items"][0]["status"],
                "evidence": messages[-1]["content"]})
        elif metadata.get("_required_read_tool"):
            call("query_offer_status", {"item": "娱乐首单", "date": metadata["_required_read_day"]})
        return "我在的，你说～"

    def close(self):
        pass


def test_actual_five_lane_runtime_real_mcp_fake_model_all_thirty(tmp_path):
    original = tmp_path / "original"
    with closing(BotStore(original)) as store:
        store.configure(config()); (original / "bot/secrets").mkdir(mode=0o700)
    schedule = bench.default_schedule()
    for index, row in enumerate(schedule):
        row["at_seconds"] = index * .01
        row["scenario"] = "test_compressed_burst_not_default_load"
    report = bench.run_benchmark(original, tmp_path / "run", group_ref="group_a", own="wxid_robot",
        thread="synthetic@chatroom", schedule=schedule, transport=transport(0, 0),
        provider_factory=lambda profile: FakeProvider(), timeout_seconds=45)
    assert report["correctness_passed"], [(row["case"], row["failures"]) for row in report["cases"] if not row["passed"]]
    assert set(report["runtime"]["lanes"]) == {"receive", "business", "projection", "send", "status"}
    assert all(value["state"] == "ready" for value in report["runtime"]["lanes"].values())
    assert len(report["cases"]) == report["provider_instances"] == 30
    assert report["wechat_sent"] is report["phone_received"] is report["phone_p95_verified"] is False
    assert report["original_ledger_preserved"] and report["final_fixture_ledger_matches"]
    assert all(row["simulated_submission"]["post_count"] == 1 for row in report["cases"])
    assert all(row["local_outbox_accepted_seconds"] >= row["simulated_remote_accepted_seconds"] for row in report["cases"])
    assert report["summary"]["all"]["local_outbox_accepted_seconds"]["missing_count"] == 0
    with closing(BotStore(original)) as store:
        assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 0


def test_default_preview_does_not_import_production_or_open_source(monkeypatch, capsys):
    monkeypatch.setattr(bench, "load_components", lambda _: pytest.fail("preview imported production"))
    monkeypatch.setattr(bench, "clone_state", lambda *_: pytest.fail("preview copied source"))
    assert bench.main([]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["model_calls"] == 0 and result["state_opened"] is False
    assert len(result["schedule"]) == 30
