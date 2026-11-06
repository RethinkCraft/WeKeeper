"""Direct benchmark accounting is observed, not inferred from output flags."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from chatlog_keeper.bot.direct_plan import saved_native_plan
from chatlog_keeper.bot.protocol_gateway import GatewayError, SendResult
from chatlog_keeper.bot.protocol_business import ProtocolBusinessWorker
from chatlog_keeper.bot.protocol_runtime import ProviderResources
from chatlog_keeper.bot.reply_generator import ReplyGenerator
from chatlog_keeper.bot.service import Service
from chatlog_keeper.dashboard.usage import UsageLedger
from test_bot_protocol_business import ACCOUNT, GROUPS, OWN, harness
from test_protocol_runtime_phased import Clock


SCRIPT = Path(__file__).parents[1] / "scripts/benchmarks/benchmark_protocol_runtime_direct.py"
spec = importlib.util.spec_from_file_location("direct_runtime_benchmark", SCRIPT)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def wrapped_fake_components(adapter, tmp_path, *, explode=False):
    class Resources:
        def __init__(self, store=None, **kwargs):
            self.store, self.usage_dir = store, tmp_path
        def local_business_execution(self, profile=None):
            return "local lease"

    class Generator:
        def __init__(self, *args, provider_factory, **kwargs):
            self.provider_factory, self.options = provider_factory, kwargs
        async def generate(self):
            return await asyncio.to_thread(self.provider_factory, {"id": "fake"})

    class Worker:
        def __init__(self, generator):
            self.generator = generator
        def _generate(self, request, nearby, progress):
            return asyncio.run(self.generator.generate())

    original_calls = []
    def provider(profile):
        original_calls.append(profile)
        if explode:
            raise RuntimeError("fake constructor failed")
        return object()

    components = adapter.wrap_components({"ProviderResources": Resources, "ReplyGenerator": Generator,
                                          "ProtocolBusinessWorker": Worker})
    components["ProviderResources"]()
    generator = components["ReplyGenerator"](provider_factory=provider)
    return components["ProtocolBusinessWorker"](generator), generator, original_calls


@pytest.mark.parametrize("enabled", [False, True])
def test_explicit_opt_in_injects_only_real_captured_local_factory(tmp_path, enabled):
    adapter = bench.DirectAdapter(enabled=enabled)
    _, generator, original_calls = wrapped_fake_components(adapter, tmp_path)
    assert ("local_execution_factory" in generator.options) is enabled
    if enabled:
        assert generator.options["local_execution_factory"].__self__ is adapter.resources
        assert generator.options["local_execution_factory"]() == "local lease"
    assert original_calls == adapter.provider_constructions == []


def test_worker_request_context_survives_asyncio_to_thread_and_concurrent_workers(tmp_path):
    adapter = bench.DirectAdapter(enabled=True)
    worker, _, original_calls = wrapped_fake_components(adapter, tmp_path)
    with ThreadPoolExecutor(2) as pool:
        futures = [pool.submit(worker._generate, {"event_id": key}, [], {}) for key in ("a", "b")]
        assert all(future.result(timeout=3) is not None for future in futures)
    assert sorted(row["request_id"] for row in adapter.provider_constructions) == ["a", "b"]
    assert adapter.generated_requests == {"a", "b"}
    assert adapter.request_context.get() is None and len(original_calls) == 2


def test_failed_provider_construction_is_counted_before_exception(tmp_path):
    adapter = bench.DirectAdapter(enabled=True)
    worker, _, _ = wrapped_fake_components(adapter, tmp_path, explode=True)
    with pytest.raises(RuntimeError, match="fake constructor"):
        worker._generate({"event_id": "failed"}, [], {})
    assert [row["request_id"] for row in adapter.provider_constructions] == ["failed"]
    assert adapter.request_context.get() is None


def test_actual_worker_and_generator_preserve_constructor_request_context(harness, tmp_path):
    h = harness
    identifier = h.inbox.add("20", "在吗，接个话呀")
    h.generator.direct_plan_enabled = True
    admission_worker = h.worker()
    admission_worker.admit_pending()
    admission_worker.close()
    request = h.store.request(identifier)
    assert "_native_tool_plan" not in request["event"]
    adapter = bench.DirectAdapter(enabled=True)
    components = adapter.wrap_components({"ProviderResources": ProviderResources,
        "ReplyGenerator": ReplyGenerator, "ProtocolBusinessWorker": ProtocolBusinessWorker})

    class Host:
        tools = []
        def __init__(self, *args): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass

    class Provider:
        def complete(self, *args): return "我在的，你说～"
        def close(self): pass

    resources = components["ProviderResources"](h.store, state_dir=tmp_path,
        usage_dir=tmp_path / "usage", dashboard_dir=tmp_path / "dashboard")
    generator = components["ReplyGenerator"](h.store, state_dir=tmp_path,
        provider_factory=lambda _: Provider(), host_factory=Host)
    worker = components["ProtocolBusinessWorker"](inbox=h.inbox, store=h.store, generator=generator,
        outbox=h.outbox, bindings=GROUPS, own_username=OWN, account_id=ACCOUNT)
    try:
        result = worker._generate(request, [], worker._trace_progress(request))
        assert result.text == "我在的，你说～"
        assert result.evidence["usage_run_id"]
        assert [row["request_id"] for row in adapter.provider_constructions] == [identifier]
        assert resources._clients == {} and resources._usage is None
    finally:
        worker.close(); resources.close()


@pytest.fixture
def local_fixture(harness, tmp_path):
    h = harness
    identifier = h.inbox.add("10", "今天娱乐首单已无，请报备")
    # Real native inbox snapshots carry this documented custody alias.
    source = h.inbox.rows[-1]["message_json"]
    source["id"] = source["receivedId"]
    h.generator.direct_plan_enabled = True
    assert h.worker().admit_pending() == 1
    request = h.store.request(identifier)
    receipt = Service(h.store, {"event_id": identifier, "writable": True}).call(
        "report_offer_status", {"item": "fun", "status": "unavailable", "evidence": request["text"]})
    assert receipt["ok"] is receipt["recorded"] is True
    run_id = "local_" + "a" * 32
    usage_dir = tmp_path / "usage"
    with closing(UsageLedger(usage_dir)) as ledger:
        ledger.start_run(run_id=run_id, kind="local_business", model="local-business", provider="local-business",
            profile_id=request["profile_id"], group_ref=request["group_ref"], bot_request_id=identifier,
            model_invoked=False, execution_route=bench.ROUTE)
        ledger.finish_run(run_id)
    evidence = {"request": {key: deepcopy(request.get(key)) for key in bench.REQUEST_FIELDS},
        "usage_run_id": None, "execution_run_id": run_id, "execution_route": bench.ROUTE,
        "model_invoked": False, "model_calls": 0, "direct_plan": deepcopy(request["event"]["_native_tool_plan"]),
        "report_facts": {"fun": receipt}, "tool_results": [{"name": "report_offer_status", "result": receipt}]}
    evidence["request"]["source_event"] = deepcopy(request["event"])
    final = f"已记录：{receipt['day']}，娱乐首单已无。"
    frozen = {"request_id": identifier, "reply_id": "reply1", "source": deepcopy(source),
              "text": final, "evidence": evidence, "state": "accepted"}
    source_without_alias = {key: value for key, value in source.items() if key != "id"}
    command = {"thread_id": source["threadId"], "received_id": source["receivedId"], "server_id": source["serverId"],
               "source_sha256": source["sourceSha256"], "text": final, "idempotency_key": "reply1"}
    fixture = {"case": {"category": "report"},
        "injected": {"source": source_without_alias, "request_id": identifier},
        "frozen": frozen, "outbox_row": deepcopy(frozen), "submission": {"command": command, "post_count": 1},
        "expected": {key: receipt[key] for key in ("business_id", "day", "item_id", "status", "version")},
        "report_rows": [dict(row) for row in h.store.conn.execute("SELECT * FROM reports WHERE event_id=?", (identifier,))],
        "audit_rows": [dict(row) for row in h.store.conn.execute("SELECT * FROM audit_log WHERE event_id=?", (identifier,))],
        "item_name": "娱乐首单"}
    adapter = bench.DirectAdapter(enabled=True)
    adapter.resources = SimpleNamespace(store=h.store, usage_dir=usage_dir)
    adapter.plan_validator = saved_native_plan
    adapter.generated_requests.add(identifier)
    engine = bench.checked_phase_engine().engine
    return SimpleNamespace(adapter=adapter, fixture=fixture, engine=engine, store=h.store,
                           request=request, run_id=run_id, usage_dir=usage_dir)


def inspect(value):
    return value.adapter.inspect_case(value.engine.inspect_case, **value.fixture)


def test_only_verified_local_accounting_replaces_old_usage_requirement(local_fixture):
    value = local_fixture
    assert value.engine.inspect_case(**value.fixture) == ["current_request_evidence_missing"]
    before = deepcopy(value.fixture)
    assert inspect(value) == []
    assert value.fixture == before
    check = value.adapter.local_checks[value.request["event_id"]]
    assert check["passed"] and check["provider_constructions"] == 0


@pytest.mark.parametrize("fault,error", [
    ("provider", "local_route_constructed_model_provider"),
    ("unbound_provider", "provider_observation_not_bound"),
    ("unobserved_request", "provider_observation_not_bound"),
    ("no_opt_in", "direct_route_without_opt_in"),
    ("wrong_request", "local_request_evidence_mismatch"),
    ("forged_plan", "persisted_direct_plan_mismatch"),
    ("disabled_binding", "local_request_binding_mismatch"),
    ("bad_id", "invalid_local_execution_run_id"),
    ("missing_run", "local_execution_run_missing"),
    ("wrong_kind", "local_execution_run_mismatch"),
    ("failed_run", "local_execution_run_mismatch"),
    ("wrong_request_run", "local_execution_run_mismatch"),
    ("invalid_metadata", "local_execution_run_mismatch"),
    ("tokens", "local_execution_tokens_not_zero"),
    ("usage_call", "local_execution_has_model_calls"),
    ("extra_run", "local_request_has_extra_usage_runs"),
])
def test_flags_cannot_forge_zero_model_local_success(local_fixture, fault, error):
    value = local_fixture
    evidence = value.fixture["frozen"]["evidence"]
    if fault in {"provider", "unbound_provider"}:
        value.adapter.provider_constructions.append({"request_id": value.request["event_id"] if fault == "provider" else None})
    elif fault == "unobserved_request":
        value.adapter.generated_requests.clear()
    elif fault == "no_opt_in":
        value.adapter.enabled = False
    elif fault == "wrong_request":
        evidence["request"]["event_id"] = "different"
    elif fault == "forged_plan":
        evidence["direct_plan"]["day"] = "2000-01-01"
    elif fault == "disabled_binding":
        settings = value.store.config(); settings["groups"][0]["enabled"] = False
        value.store.configure(settings)
    elif fault == "bad_id":
        evidence["execution_run_id"] = "not-a-local-id"
    else:
        with sqlite3.connect(value.usage_dir / "usage.sqlite") as conn:
            if fault == "missing_run":
                conn.execute("DELETE FROM usage_runs")
            elif fault == "wrong_kind":
                conn.execute("UPDATE usage_runs SET kind='bot_chat'")
            elif fault == "failed_run":
                conn.execute("UPDATE usage_runs SET status='failed'")
            elif fault == "wrong_request_run":
                conn.execute("UPDATE usage_runs SET metadata=json_set(metadata,'$.bot_request_id','other')")
            elif fault == "invalid_metadata":
                conn.execute("UPDATE usage_runs SET metadata='null'")
            elif fault == "tokens":
                conn.execute("UPDATE usage_runs SET usage=json_set(usage,'$.input_tokens',1)")
            elif fault == "usage_call":
                conn.execute("INSERT INTO usage_calls VALUES(?,?,NULL,NULL,NULL,NULL,'completed',0)", ("bad", value.run_id))
            elif fault == "extra_run":
                conn.execute("INSERT INTO usage_runs SELECT 'extra',kind,user_id,turn_id,model,started_at,completed_at,status,usage,metadata,error_code,delivered FROM usage_runs")
    errors = inspect(value)
    assert error in errors
    assert "current_request_evidence_missing" in errors


@pytest.mark.parametrize("fault,error", [("duplicate", "duplicate_submission"),
    ("wrong_quote", "submitted_quote_or_body_mismatch"), ("missing_tool", "current_request_tool_missing_or_wrong"),
    ("missing_audit", "report_or_audit_count_mismatch"), ("wrong_fact", "current_request_fact_mismatch")])
def test_successful_local_accounting_never_removes_other_oracle_failures(local_fixture, fault, error):
    value = local_fixture
    if fault == "duplicate": value.fixture["submission"]["post_count"] = 2
    elif fault == "wrong_quote": value.fixture["submission"]["command"]["server_id"] = "wrong"
    elif fault == "missing_tool": value.fixture["frozen"]["evidence"]["tool_results"] = []
    elif fault == "missing_audit": value.fixture["audit_rows"] = []
    elif fault == "wrong_fact": value.fixture["frozen"]["evidence"]["report_facts"]["fun"]["version"] = 0
    errors = inspect(value)
    assert error in errors
    assert "current_request_evidence_missing" not in errors


def test_unknown_route_fails_and_model_route_keeps_original_oracle(local_fixture):
    value = local_fixture
    value.fixture["frozen"]["evidence"]["execution_route"] = "fake_direct"
    assert "unknown_local_execution_route" in inspect(value)
    evidence = value.fixture["frozen"]["evidence"]
    for key in ("execution_route", "execution_run_id", "direct_plan", "model_invoked", "model_calls"):
        evidence.pop(key)
    evidence["usage_run_id"] = "ordinary-model-run"
    assert inspect(value) == value.engine.inspect_case(**value.fixture)


def phase_document():
    return {"failed_candidate": False, "complete_phase_evidence": True, "source_tree": "a" * 40,
        "cases": [{"python_method_to_accepted_commit_seconds": .8,
            "command_to_accepted_commit_seconds": .6, "phases_seconds": {"queue_total": .1}, "submit_api_ms": 350}]}


@pytest.mark.parametrize("enabled", [False, True])
def test_preview_preserves_cli_without_opening_production_or_state(tmp_path, capsys, enabled):
    path = tmp_path / "phases.json"; path.write_text(json.dumps(phase_document()))
    assert bench.main(["--phase-calibration", str(path), *(["--direct-native-plan"] if enabled else [])]) == 0
    document = json.loads(capsys.readouterr().out)
    assert document["state_opened"] is False and document["model_calls"] == 0
    metadata = document["transport"]["provenance"]["direct_adapter"]
    assert metadata["direct_native_plan_enabled"] is enabled
    assert metadata["adapter_sha256"] == hashlib.sha256(SCRIPT.read_bytes()).hexdigest()
    assert metadata["adapter_is_production_source"] is False
    assert metadata["phase_calibration_is_production_optimization"] is False
    assert len(document["schedule"]) == 30


def test_changed_base_engine_is_rejected_before_import(tmp_path, monkeypatch):
    path = tmp_path / "changed.py"; path.write_text("raise AssertionError('must not execute')")
    monkeypatch.setattr(bench, "BASE_PATH", path)
    with pytest.raises(RuntimeError, match="engine_changed"):
        bench.checked_phase_engine()


def test_actual_component_source_hashes_remain_separate_from_adapter_metadata(tmp_path, monkeypatch):
    phased = bench.checked_phase_engine()
    original = {"ProviderResources": type("Resources", (), {}), "ReplyGenerator": lambda: None,
                "ProtocolBusinessWorker": type("Worker", (), {})}
    build = {"python_files": {"production.py": "production-hash"}, "python_tree_sha256": "tree-hash",
             "benchmark_sha256": bench.BASE_SHA}
    calls = []
    def load(root):
        calls.append(root)
        return original, deepcopy(build)
    monkeypatch.setattr(phased.engine, "load_components", load)
    adapter = bench.DirectAdapter(enabled=False)
    engine = bench.configure_engine(phased, adapter)
    wrapped, loaded_build = engine.load_components(tmp_path)
    assert calls == [tmp_path]
    assert loaded_build["python_files"] == build["python_files"]
    assert loaded_build["python_tree_sha256"] == build["python_tree_sha256"]
    assert loaded_build["benchmark_sha256"] == bench.BASE_SHA
    assert loaded_build["benchmark_adapter"]["adapter_is_production_source"] is False
    assert wrapped is not original and "benchmark_adapter" not in build


def test_report_serialization_captures_final_observations_separately(tmp_path):
    phased = bench.checked_phase_engine()
    adapter = bench.DirectAdapter(enabled=False)
    engine = bench.configure_engine(phased, adapter)
    adapter.provider_constructions.append({"request_id": "request1", "profile_id": "fake", "at": 1.})
    value = {"build": {"python_files": {"production.py": "hash"}}}
    output = tmp_path / "report.json"
    engine.private_json(output, value)
    saved = json.loads(output.read_text())
    assert saved["benchmark_adapter"]["provider_constructions"] == adapter.provider_constructions
    assert saved["build"]["python_files"] == {"production.py": "hash"}


def test_adapter_keeps_client_prelude_outside_gate_and_http_overlap_unchanged(tmp_path):
    phased = bench.checked_phase_engine()
    path = tmp_path / "phase.json"; path.write_text(json.dumps(phase_document()))
    phased.configure_engine(path)
    engine = bench.configure_engine(phased, bench.DirectAdapter(enabled=True))
    clock = Clock()
    config = engine.transport_configuration(overrides={
        "samples": {key: [0] for key in engine.OPERATIONS - {"accepted", "http_return"}}})
    server = engine.SimulatedServer("robot", "room", config,
        {"SendResult": SendResult, "GatewayError": GatewayError}, clock=clock, sleep=clock.sleep)
    def send(source, key):
        source = source["source"]
        return server.send_received_quote(thread_id=source["threadId"], received_id=source["receivedId"],
            server_id=source["serverId"], source_sha256=source["sourceSha256"], text="fixed", idempotency_key=key)
    a = server.inject({"text": "same"}, "sender"); b = server.inject({"text": "same"}, "sender")
    send(a, "a"); send(b, "b")
    row = server.submissions["b"]
    assert row["client_preparation_ended_at"] == pytest.approx(100.55)
    assert row["effective_command_at"] == pytest.approx(100.8)
    assert row["account_gate_wait_seconds"] == pytest.approx(.25)
    assert row["accepted_at"] == pytest.approx(101.4)
    assert clock.now == pytest.approx(100.95)
