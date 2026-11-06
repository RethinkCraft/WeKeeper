"""Original-corpus identity/selection and isolated fake-provider execution."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from scripts.diagnostics import check_protocol_business_corpus as corpus
from scripts.diagnostics.protocol_cases import deepseek_cases
from chatlog_keeper.bot.reply_generator import ReplyGenerator
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config


def test_manifest_preserves_all_original_content_and_rejects_timing_claims():
    original = deepseek_cases(20261029)
    selected = corpus.select_cases()
    assert selected == original and len(selected) == 100
    manifest = corpus.manifest(selected)
    # JSON comparison retains all original fields while normalizing tuples.
    assert json.dumps(manifest["cases"], sort_keys=True) == json.dumps([case.as_dict() for case in original], sort_keys=True)
    assert manifest["full_content_corpus_selected"] is True
    assert manifest["execution"] == "sequential_content_only"
    assert all(manifest[key] is False for key in ("original_schedule_applied", "pressure_tested",
               "idle_latency_tested", "phone_latency_tested", "wechat_sent", "phone_received"))
    for left, right in zip(selected[60:80:2], selected[61:80:2]):
        assert left.text == right.text and left.sender == "manager" and right.sender == "peer2"


def test_selected_ids_keep_original_order_without_implicit_preceding_cases():
    selected = corpus.select_cases(case_ids=["synthetic_0040", "synthetic_0018"])
    assert [case.id for case in selected] == ["synthetic_0018", "synthetic_0040"]
    assert corpus.select_cases(count=3) == deepseek_cases(20261029)[:3]
    assert corpus.manifest(selected)["full_content_corpus_selected"] is False


@pytest.mark.parametrize("arguments", [
    {"count": 0}, {"count": 101}, {"count": True}, {"count": 1.5},
    {"case_ids": []}, {"case_ids": "synthetic_0001"}, {"case_ids": ["missing"]},
    {"case_ids": ["synthetic_0001", "synthetic_0001"]},
    {"count": 1, "case_ids": ["synthetic_0001"]},
])
def test_invalid_selection_cannot_silently_expand_to_full_corpus(arguments):
    with pytest.raises(ValueError):
        corpus.select_cases(**arguments)


def test_adapter_maps_project_id_and_actor_without_rewriting_text():
    settings = config()
    binding = {"business": settings["businesses"][0]}
    before = deepcopy(binding)
    originals = corpus.select_cases()
    fixtures = corpus.native_cases(originals, binding)
    assert binding == before
    for original, fixture in zip(originals, fixtures):
        assert (fixture["id"], fixture["text"], fixture["actor"], fixture["category"]) == (
            original.id, original.text, original.sender, original.category)
        assert fixture["expected_write"] == ({"fun": original.expectation.write_status} if original.category == "report" else {})
        assert fixture["expected_read"] == (["fun"] if original.category == "query" else [])
    broken = deepcopy(binding)
    broken["business"]["items"].append({"id": "duplicate", "name": "娱乐首单"})
    with pytest.raises(ValueError, match="mapping"):
        corpus.native_cases(originals, broken)


def test_default_preview_does_not_open_state_clone_or_provider(tmp_path, monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("preview must not construct a model or copy/open state")
    monkeypatch.setattr(corpus, "run_corpus", forbidden)
    assert corpus.main(["--count", "2", "--source-state", str(tmp_path / "missing"),
                        "--output", str(tmp_path / "must-not-exist")]) == 0
    value = json.loads(capsys.readouterr().out)
    assert value["mode"] == "preview" and value["model_calls"] == 0 and value["state_opened"] is False
    assert value["corpus_manifest"]["selected_count"] == 2
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(SystemExit) as error:
        corpus.main(["--real-model", "--count", "1"])
    assert error.value.code == 2


def test_library_execution_requires_opt_in_before_touching_paths(tmp_path):
    with pytest.raises(ValueError, match="real_model_opt_in_required"):
        corpus.run_corpus(tmp_path / "missing", tmp_path / "output", group_ref="g", own="bot", thread="room")
    assert list(tmp_path.iterdir()) == []


class Provider:
    def complete(self, messages, instructions, tools, call, metadata):
        for item in (metadata.get("_required_report") or {}).get("items", []):
            result = call("report_offer_status", {"item": item["item_id"], "status": item["status"],
                          "evidence": messages[-1]["content"]})
            assert result["ok"]
        if metadata.get("_required_read_tool"):
            result = call("query_offer_status", {"item": "娱乐首单", "date": metadata["_required_read_day"]})
            assert result["ok"]
        return "收到啦。"

    def close(self):
        pass


@pytest.fixture
def original(tmp_path):
    path = tmp_path / "source"
    with closing(BotStore(path)) as store:
        store.configure(config())
        (path / "bot/secrets").mkdir(mode=0o700)
    return path


def test_selected_content_uses_independent_sql_oracle_and_preserves_configuration(original, tmp_path):
    requested = ["synthetic_0003", "synthetic_0014", "synthetic_0017", "synthetic_0061", "synthetic_0062"]
    profiles = []
    def provider(profile):
        profiles.append(deepcopy(profile))
        return Provider()
    result = corpus.run_corpus(original, tmp_path / "corpus", group_ref="group_a", own="wxid_robot",
                               thread="synthetic-corpus@chatroom", case_ids=requested, provider_factory=provider)
    assert result["passed"] and result["mechanical_passed"]
    assert [case["case"] for case in result["cases"]] == requested
    assert result["model_execution"] == "injected_fixture"
    assert result["cases"][0]["expected_facts"]["fun"]["version"] == 0
    assert result["cases"][2]["expected_facts"]["fun"]["status"] == "available"
    assert result["cases"][2]["expected_facts"]["fun"]["version"] == 1
    left, right = result["cases"][-2:]
    assert left["sender_id"] == result["actors"]["manager"]
    assert right["sender_id"] == result["actors"]["peer2"]
    assert left["sender_id"] != right["sender_id"] and left["request_id"] != right["request_id"]
    with closing(BotStore(original)) as store:
        assert all(profile == store.binding("group_a")["profile"] for profile in profiles)
        assert store.binding("group_a")["business_id"] == "shop"
        assert store.conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 0
        assert store.conn.execute("SELECT COUNT(*) FROM requests").fetchone()[0] == 0
    assert result["configuration_preserved"] == {"model_profile": True, "persona": True}
    assert result["wechat_sent"] is False and result["phone_received"] is False
    assert (tmp_path / "corpus/report.json").stat().st_mode & 0o077 == 0
    frozen = json.loads((tmp_path / "corpus/corpus-manifest.json").read_text())
    assert frozen["selected_case_ids"] == requested
    assert frozen["selected_content_sha256"] == result["corpus_manifest"]["selected_content_sha256"]


def test_corpus_cannot_pass_correct_evidence_with_contradictory_final(original, tmp_path, monkeypatch):
    generate = ReplyGenerator.generate
    async def incorrect(self, *args, **kwargs):
        result = await generate(self, *args, **kwargs)
        return replace(result, text="查询结果：2026-10-29，娱乐首单已无。")
    monkeypatch.setattr(ReplyGenerator, "generate", incorrect)
    result = corpus.run_corpus(original, tmp_path / "wrong", group_ref="group_a", own="wxid_robot",
        thread="synthetic-corpus@chatroom", case_ids=["synthetic_0003"], provider_factory=lambda _: Provider())
    assert result["passed"] is False
    assert "final_business_claim_mismatch" in result["cases"][0]["failures"]


def test_full_hundred_fake_content_run_keeps_original_case_identity_and_write_counts(original, tmp_path):
    calls = []
    def provider(profile):
        calls.append(profile["id"])
        return Provider()
    result = corpus.run_corpus(original, tmp_path / "hundred", group_ref="group_a", own="wxid_robot",
                               thread="synthetic-corpus@chatroom", provider_factory=provider)
    originals = deepseek_cases(20261029)
    assert result["passed"] is True and len(calls) == 100
    assert [case["case"] for case in result["cases"]] == [case.id for case in originals]
    assert [case["text"] for case in result["fixture_cases"]] == [case.text for case in originals]
    assert all(row["sender_id"] == result["actors"][case.sender] for row, case in zip(result["cases"], originals))
    assert result["corpus_manifest"]["full_content_corpus_selected"] is True
    expected_reports = sum(case.category == "report" for case in originals)
    with closing(BotStore(tmp_path / "hundred/state")) as store:
        assert store.conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == expected_reports
        assert store.conn.execute("SELECT COUNT(*) FROM audit_log WHERE kind='report'").fetchone()[0] == expected_reports
    assert result["cases"][-1]["expected_facts"]["fun"]["version"] == expected_reports
    assert result["model_execution"] == "injected_fixture"
    assert result["pressure_tested"] is False and result["idle_latency_tested"] is False

