"""Boundary calibration only: fake generation, SQLite/runtime, no MCP or network."""
import asyncio
from contextlib import ExitStack
import hashlib
import importlib.util
import json
from pathlib import Path
import time

import pytest

from chatlog_keeper.bot.reply_generator import GenerationResult
from test_bot_service import config

ROOT = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location("runtime_observed", ROOT / "scripts/benchmarks/benchmark_protocol_runtime_observed.py")
observed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observed)


def test_frozen_base_and_history_remain_identical():
    data = observed.BASE_PATH.read_bytes()
    assert hashlib.sha256(data).hexdigest() == observed.BASE_SHA256
    # The private archival copy is an operator artifact, not required in a
    # clean checkout. When present, it must match the public historical source.
    archive = ROOT / "private/protocol-gateway/runtime-benchmark-v3-source/benchmark_protocol_runtime.py"
    if archive.exists():
        assert archive.read_bytes() == data


def fixture_observations():
    observer = observed.Boundaries(clock=lambda: 100)
    points = {name: 100 + index for index, name in enumerate(observed.PATH)}
    for name, value in points.items():
        observer.point("request", "r", name, value)
    observer.trace_origins["r"] = (100, "attempt")
    trace = {"attempt_id": "attempt", "events": {name: {"offset_seconds": points[name] - 100}
             for name in ("generation_queued", "generation_started", "generation_ended")}}
    case = {"request_id": "r", "source": {"receivedId": "received"}, "metrics": {"trace": trace},
        "simulated_remote_accepted_seconds": points["remote_accepted"] - 100,
        "local_outbox_accepted_seconds": points["local_accepted"] - 100,
        "simulated_submission": {"command": {"idempotency_key": "reply"},
            "submitted_at": points["command_started"], "effective_command_at": points["account_gate_entered"],
            "accepted_at": points["remote_accepted"], "http_returned_at": points["account_gate_entered"] + .2}}
    return observer, case


def test_adjacent_intervals_cover_total_once_and_http_return_is_nested():
    observer, case = fixture_observations()
    result = observed.case_observations(case, observer)
    assert result["complete_ordered_path"]
    intervals = result["adjacent_nonoverlapping_intervals"]
    assert sum(row["seconds"] for row in intervals) == case["local_outbox_accepted_seconds"]
    assert all("http_returned" not in (row["start"], row["end"]) for row in intervals)
    assert result["points_since_injection_seconds"]["http_returned"] < case["simulated_remote_accepted_seconds"]


def test_clock_mismatch_and_reordered_return_boundaries_are_not_hidden():
    observer, case = fixture_observations()
    observer.points[("request", "r")]["outbox_freeze_commit_returned"] += 10
    case["metrics"]["trace"]["attempt_id"] = "different-attempt"
    result = observed.case_observations(case, observer)
    assert not result["complete_ordered_path"]
    assert "generation_trace_origin_missing_or_mismatched" in result["errors"]
    assert any(value.startswith("boundary_order_conflict:") for value in result["errors"])
    assert any(row["seconds"] < 0 for row in result["adjacent_nonoverlapping_intervals"])


def test_missing_send_retains_injection_and_missing_boundaries():
    observer = observed.Boundaries()
    observer.point("request", "r", "injected", 123)
    result = observed.case_observations({"request_id": "r", "source": {"receivedId": "x"}}, observer)
    assert result["points_monotonic_seconds"] == {"injected": 123}
    assert "remote_accepted" in result["missing_boundaries"]
    assert not result["complete_ordered_path"]


def test_operation_buffer_is_bounded():
    observer = observed.Boundaries(max_operations=1)
    observer.operation("receive", 1, 2)
    observer.operation("receive", 3, 4)
    assert len(observer.operations) == observer.dropped_operations == 1


@pytest.mark.parametrize("fails", [False, True])
def test_legacy_commit_staged_without_split_commit_hook(fails):
    now = [100.0]
    observer = observed.Boundaries(clock=lambda: now[0])

    class LegacyInbox:
        def _resolve(self, event):
            now[0] += .01
            return "request-1", {}, "content"

        def _commit_staged(self):
            self._resolve({})
            now[0] += .02
            if fails:
                raise RuntimeError("synthetic commit failure")
            return 1, 1

    components = {key: object for key in ("BotStore", "ProtocolBusinessWorker", "ProtocolOutbox")}
    inbox = observed.observed_components({**components, "ProtocolNativeInbox": LegacyInbox}, observer)["ProtocolNativeInbox"]()
    if fails:
        with pytest.raises(RuntimeError, match="synthetic commit failure"):
            inbox._commit_staged()
        assert ("request", "request-1") not in observer.points
    else:
        assert inbox._commit_staged() == (1, 1)
        points = observer.points[("request", "request-1")]
        assert points["inbox_resolution_batch_started"] == 100
        assert points["inbox_request_commit_returned"] == pytest.approx(100.03)
    assert not hasattr(inbox, "_observed_resolved")


class FakeGenerator:
    async def generate(self, event, request, nearby, progress):
        await asyncio.sleep(.002)
        return GenerationResult("我在的，你说～", {"request": {"event_id": request["event_id"]}}, {})


def test_two_requests_real_five_lane_sqlite_fake_generation_boundary_order(tmp_path):
    base = observed.load_base()
    original, _ = base.load_components(ROOT)
    observer = observed.Boundaries()
    components = observed.observed_components(original, observer)
    transport = base.transport_configuration(overrides={
        "samples": {name: [0] for name in base.OPERATIONS - {"accepted", "http_return"}},
        "send_samples": [{"accepted": .01, "http_return": .002}]})
    server = observed.observed_server(base, observer)("wxid_robot", "synthetic@chatroom", transport, components)
    clients = [base.SimulatedGateway(server) for _ in range(3)]
    with ExitStack() as stack:
        store = components["BotStore"](tmp_path / "state"); stack.callback(store.close)
        store.configure(config())
        inbox = components["ProtocolNativeInbox"](clients[0], path=tmp_path / "inbox.sqlite",
            own_username="wxid_robot", consumer="observed-test")
        stack.callback(inbox.close); inbox.activate()

        class AcceptedOutbox(components["ProtocolOutbox"]):
            def _save(self, reply_id, **kwargs):
                result = super()._save(reply_id, **kwargs)
                if result["state"] == "accepted":
                    accepted.setdefault(reply_id, time.monotonic())
                return result

        accepted = {}
        outbox = AcceptedOutbox(tmp_path / "outbox.sqlite", account_id=server.account_id, own_username="wxid_robot")
        stack.callback(outbox.close)
        worker = components["ProtocolBusinessWorker"](inbox=inbox, store=store, generator=FakeGenerator(), outbox=outbox,
            bindings={server.thread: "group_a"}, own_username="wxid_robot", account_id=server.account_id)
        runtime = components["ProtocolRuntime"](inbox=inbox, business=worker, outbox=outbox,
            receive_gateway=clients[0], send_gateway=clients[1], status_gateway=clients[2],
            interval=.005, status_interval=.005, close_callback=worker.close)
        stack.callback(runtime.close)
        runtime.start()
        plans = [server.inject({"text": "@机器人 在吗"}, "wxid_manager") for _ in range(2)]
        deadline = time.monotonic() + 10
        while len(accepted) < 2 and time.monotonic() < deadline:
            time.sleep(.005)
        runtime.close()
        assert len(accepted) == 2, runtime.status()
        for plan in plans:
            request_id = plan["request_id"]
            frozen = store.get("protocol_frozen_" + request_id)
            submission = server.submissions[frozen["reply_id"]]
            case = {"request_id": request_id, "source": plan["source"],
                "metrics": store.get("protocol_metrics_" + request_id), "simulated_submission": submission,
                "simulated_remote_accepted_seconds": submission["accepted_at"] - plan["injected"],
                "local_outbox_accepted_seconds": accepted[frozen["reply_id"]] - plan["injected"]}
            result = observed.case_observations(case, observer)
            assert result["complete_ordered_path"], result
            assert sum(row["seconds"] for row in result["adjacent_nonoverlapping_intervals"]) == pytest.approx(case["local_outbox_accepted_seconds"])
            points = result["points_monotonic_seconds"]
            assert points["inbox_raw_append_started"] <= points["inbox_raw_commit_returned"]
            assert points["business_admission_started"] <= points["business_admission_commit_returned"]
            assert points["business_final_commit_started"] <= points["business_final_commit_returned"]
            assert points["outbox_freeze_started"] <= points["outbox_freeze_commit_returned"]
            assert submission["post_count"] == 1


def test_preview_remains_no_state_no_model(capsys):
    assert observed.main([]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["model_calls"] == 0 and result["state_opened"] is False
