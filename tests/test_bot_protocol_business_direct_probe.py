"""Independent Direct content oracle: fake providers, actual local MCP/ledger."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import json
import os
import sqlite3
from types import SimpleNamespace

import pytest

from scripts.diagnostics import check_protocol_business_direct as direct
from scripts.benchmarks.benchmark_protocol_runtime_direct import DirectAdapter, ROUTE
from chatlog_keeper.bot.reply_generator import ReplyGenerator
from chatlog_keeper.bot.store import BotStore
from test_bot_protocol_business_edges import Provider
from test_bot_service import config


@pytest.fixture
def original(tmp_path):
    path = tmp_path / "source"
    with closing(BotStore(path)) as store:
        store.configure(config())
        (path / "bot/secrets").mkdir(mode=0o700)
    return path


def fixture_cases():
    return [
        {"id": "report", "text": "今天娱乐首单已无，请报备", "actor": "manager", "category": "report",
         "expected_write": {"fun": "unavailable"}, "expected_read": []},
        {"id": "query", "text": "帮我查今天娱乐首单的状态", "actor": "peer2", "category": "query",
         "expected_write": {}, "expected_read": ["fun"]},
    ]


def run(original, output, **kwargs):
    return direct.run_content(original, output, group_ref="group_a", own="wxid_robot",
        thread="direct-content@chatroom", suite="edges", direct_native_plan=True,
        provider_factory=lambda _: pytest.fail("pure Direct must not construct a model"), **kwargs)


def test_true_direct_report_query_has_persisted_plan_local_ledger_and_zero_provider(original, tmp_path):
    prior = os.environ.get("WEKEEPER_BENCHMARK_CODE_ROOT")
    report = run(original, tmp_path / "direct", cases=fixture_cases())
    assert report["passed"] and report["mechanical_passed"]
    assert report["provider_instances"] == 0
    assert report["direct_adapter"]["provider_constructions"] == []
    assert report["execution_route_counts"][ROUTE] == 2
    checks = report["direct_adapter"]["local_execution_checks"]
    assert len(checks) == 2 and all(row["passed"] for row in checks.values())
    assert report["configuration_preserved"] == {"model_profile": True, "persona": True}
    assert report["original_ledger_preserved"] is True
    assert report["wechat_sent"] is False and report["phone_latency_tested"] is False
    assert os.environ.get("WEKEEPER_BENCHMARK_CODE_ROOT") == prior
    assert report["build"]["code_root"] == str(direct.ROOT)
    for row in report["cases"]:
        evidence = row["evidence"]
        assert evidence["model_invoked"] is False and evidence["model_calls"] == 0
        assert evidence["usage_run_id"] is None and evidence["execution_run_id"].startswith("local_")
        with closing(BotStore(tmp_path / "direct/state")) as store:
            assert store.request(row["request_id"])["event"]["_native_tool_plan"] == evidence["direct_plan"]
    with sqlite3.connect(tmp_path / "direct/usage/usage.sqlite") as db:
        assert db.execute("SELECT count(*) FROM usage_runs WHERE kind='local_business'").fetchone()[0] == 2
        assert db.execute("SELECT count(*) FROM usage_calls").fetchone()[0] == 0
    assert (tmp_path / "direct/report.json").stat().st_mode & 0o077 == 0
    with closing(BotStore(original)) as store:
        assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 0
        assert store.conn.execute("SELECT count(*) FROM requests").fetchone()[0] == 0


@pytest.mark.parametrize("mutation,failure", [
    ("local_id", "local_execution_run_missing"),
    ("plan", "persisted_direct_plan_mismatch"),
    ("fact", "current_request_facts_mismatch"),
    ("tool", "current_request_tools_missing_or_wrong"),
    ("final", "final_business_claim_mismatch"),
    ("unknown_route", "unknown_local_execution_route"),
])
def test_direct_flag_cannot_mask_missing_audit_or_corrupted_original_oracles(original, tmp_path, monkeypatch, mutation, failure):
    generate = ReplyGenerator.generate
    async def corrupted(self, *args, **kwargs):
        result = await generate(self, *args, **kwargs)
        evidence = deepcopy(result.evidence)
        if mutation == "local_id":
            evidence["execution_run_id"] = "local_" + "f" * 32
        elif mutation == "plan":
            evidence["direct_plan"]["item_ids"] = ["rank"]
        elif mutation == "fact":
            evidence["read_facts"]["fun"]["status"] = "available"
        elif mutation == "tool":
            evidence["tool_results"] = []
        elif mutation == "final":
            return replace(result, text="查询结果：" + evidence["required_read"]["day"] + "，娱乐首单可用。")
        elif mutation == "unknown_route":
            evidence["execution_route"] = "forged-direct"
        return replace(result, evidence=evidence)
    monkeypatch.setattr(ReplyGenerator, "generate", corrupted)
    report = run(original, tmp_path / mutation, cases=fixture_cases()[1:])
    assert report["passed"] is False and report["mechanical_passed"] is False
    assert failure in report["cases"][0]["failures"]


def test_corpus_subset_preserves_originals_and_observes_real_model_constructor_only_for_chat(original, tmp_path):
    report = direct.run_content(original, tmp_path / "subset", group_ref="group_a", own="wxid_robot",
        thread="direct-content@chatroom", direct_native_plan=True, case_ids=["synthetic_0001", "synthetic_0003", "synthetic_0014"],
        provider_factory=lambda _: Provider())
    assert report["passed"]
    assert report["corpus_manifest"]["selected_case_ids"] == ["synthetic_0001", "synthetic_0003", "synthetic_0014"]
    assert len(report["direct_adapter"]["provider_constructions"]) == report["provider_instances"]
    model_cases = [row for row in report["cases"] if row["evidence"].get("execution_route") is None]
    assert report["provider_instances"] == len(model_cases)
    assert all(row["category"] == "chat" for row in model_cases)
    assert report["execution"] == "sequential_content_only" and report["original_schedule_applied"] is False


@pytest.mark.parametrize("enabled", [False, True])
def test_opt_in_keeps_ambiguous_edges_on_model_and_pending_review(original, tmp_path, enabled):
    report = direct.run_content(original, tmp_path / "edges", group_ref="group_a", own="wxid_robot",
        thread="direct-content@chatroom", suite="edges", direct_native_plan=enabled, provider_factory=lambda _: Provider())
    assert report["mechanical_passed"] is True
    assert report["passed"] is False and report["status"] == "pending_manual_semantic_review"
    assert report["pending_manual_cases"] == ["edges_04_unauthorized", "edges_05_negated", "edges_06_quoted"]
    assert report["provider_instances"] == (4 if enabled else 8)
    checks = report["direct_adapter"]["local_execution_checks"]
    assert len(checks) == (4 if enabled else 0)
    assert all(row["passed"] for row in checks.values())
    assert report["direct_adapter"]["direct_native_plan_enabled"] is enabled


def test_library_requires_explicit_model_or_injected_factory_before_any_state_open(tmp_path):
    with pytest.raises(ValueError, match="real_model_opt_in_required"):
        direct.run_content(tmp_path / "missing", tmp_path / "out", group_ref="g", own="bot", thread="room")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("suite", ["corpus", "edges"])
def test_preview_never_opens_state_or_model(tmp_path, monkeypatch, capsys, suite):
    monkeypatch.setattr(direct, "run_content", lambda *args, **kwargs: pytest.fail("preview opened execution"))
    assert direct.main(["--suite", suite, "--direct-native-plan", "--source-state", str(tmp_path / "missing")]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["mode"] == "preview" and result["state_opened"] is False and result["model_calls"] == 0
    assert result["direct_native_plan_enabled"] is True and list(tmp_path.iterdir()) == []


def test_unknown_code_root_fails_before_clone_and_restores_mcp_environment(original, tmp_path, monkeypatch):
    monkeypatch.setenv("WEKEEPER_BENCHMARK_CODE_ROOT", "prior-root")
    with pytest.raises(ValueError, match="invalid_benchmark_code_root"):
        direct.run_content(original, tmp_path / "out", group_ref="g", own="bot", thread="room",
            code_root=tmp_path / "absent", provider_factory=lambda _: Provider())
    assert os.environ["WEKEEPER_BENCHMARK_CODE_ROOT"] == "prior-root"
    assert not (tmp_path / "out").exists()


def test_claimed_route_without_actual_observation_is_rejected():
    adapter = DirectAdapter(enabled=True)
    evidence = {"execution_route": ROUTE, "model_invoked": False, "model_calls": 0, "usage_run_id": None}
    errors = direct._execution_errors(adapter, evidence, "invented", {"source": {}})
    assert "provider_observation_not_bound" in errors
    assert "local_execution_resources_missing" in errors



def test_persisted_plan_cannot_be_hidden_by_a_claimed_model_result():
    adapter = DirectAdapter(enabled=True)
    # Isolate the route condition: an actually observed model constructor and
    # nonempty legacy inference ID must not erase the accepted Direct strategy.
    adapter.generated_requests.add("planned")
    adapter.provider_constructions.append({"request_id": "planned"})
    adapter.resources = SimpleNamespace(store=SimpleNamespace(request=lambda identifier: {
        "event": {"_native_tool_plan": {"version": 1, "kind": "query"}}}))
    evidence = {"usage_run_id": "pretend-inference"}
    assert direct._execution_errors(adapter, evidence, "planned", {"source": {}}) == [
        "persisted_direct_plan_changed_route"]



def test_changed_fixture_helper_is_rejected_before_clone(original, tmp_path, monkeypatch):
    from scripts.diagnostics import check_protocol_business_edges as edges
    fake_source = tmp_path / "unreviewed-edge.py"
    fake_source.write_text("# an altered fixture oracle\n")
    monkeypatch.setattr(edges, "__file__", str(fake_source))
    with pytest.raises(ValueError, match="content_fixture_helper_source_changed"):
        direct.run_content(original, tmp_path / "must-not-clone", group_ref="group_a", own="wxid_robot",
            thread="room", suite="edges", provider_factory=lambda _: Provider(), direct_native_plan=True)
    assert not (tmp_path / "must-not-clone").exists()
