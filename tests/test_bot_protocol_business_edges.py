"""Synthetic providers plus actual isolated MCP; never a live model or sender."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace

import pytest

from scripts.diagnostics import check_protocol_business_edges as edges
from chatlog_keeper.bot.protocol_outbox import ProtocolOutbox
from chatlog_keeper.bot.reply_generator import ReplyGenerator
from chatlog_keeper.bot.service import Service
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config


class Provider:
    def complete(self, messages, instructions, tools, call, metadata):
        report = metadata.get("_required_report")
        if report:
            for item in report["items"]:
                call("report_offer_status", {"item": item["item_id"], "status": item["status"],
                                            "evidence": messages[-1]["content"]})
        if metadata.get("_required_read_tool"):
            selected = metadata["_required_read_items"]
            args = {"date": metadata["_required_read_day"]}
            if len(selected) == 1:
                args["item"] = selected[0]
            call("query_offer_status", args)
        return "这条未登记。"

    def close(self):
        pass


def prepare(tmp_path):
    original = tmp_path / "original"
    with closing(BotStore(original)) as store:
        store.configure(config())
        (original / "bot/secrets").mkdir(mode=0o700)
        # Existing ledger content must survive fixture namespace creation.
        event = {"event_id": "original-report", "message_ref": "original-message", "group_ref": "group_a",
                 "sender_id": "wxid_manager", "text": "今天娱乐首单可用，请报备", "occurred_at": store.now()}
        store.begin_request(event)
        assert Service(store, {"event_id": event["event_id"], "writable": True}).call("report_offer_status", {
            "item": "fun", "status": "available", "evidence": event["text"]})["recorded"]
    output = tmp_path / "edges"
    state = edges.clone_state(original, output)
    return original, output, state


def run(state, output, **kwargs):
    return edges.run_cases(state, output, group_ref="group_a", own="wxid_robot", thread="synthetic@chatroom",
                           provider_factory=lambda _: Provider(), **kwargs)


def test_eight_edges_have_independent_facts_and_pending_refusal_review(tmp_path):
    original, output, state = prepare(tmp_path)
    result = run(state, output)
    assert result["mechanical_passed"] is True
    assert result["passed"] is False and result["status"] == "pending_manual_semantic_review"
    assert result["pending_manual_cases"] == ["edges_04_unauthorized", "edges_05_negated", "edges_06_quoted"]
    assert result["original_ledger_preserved"] is True
    assert result["original_ledger_before"]["reports"]["rows"] == 1
    assert result["wechat_sent"] is False and result["phone_received"] is False
    assert result["actors"]["manager"] == "wxid_manager"
    assert len(set(result["actors"].values())) == 3
    first, third = result["cases"][0], result["cases"][2]
    assert first["request_id"] != third["request_id"]
    assert {row["status"] for row in first["evidence"]["read_facts"].values()} == {"unknown"}
    assert {row["version"] for row in third["evidence"]["read_facts"].values()} == {1}
    assert result["cases"][-1]["evidence"]["read_facts"]["fun"]["version"] == 2
    with closing(BotStore(original)) as store:
        assert len(store.config()["businesses"]) == 2
        assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 1
    with closing(BotStore(state)) as store:
        assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 4


@pytest.mark.parametrize("mutation,expected_failure", [
    ("tool_fact", "current_request_facts_mismatch"),
    ("tool_result", "current_request_tools_missing_or_wrong"),
    ("final", "final_business_claim_mismatch"),
    ("quote", "frozen_identity_mismatch"),
])
def test_oracle_rejects_corrupt_facts_final_and_frozen_source(tmp_path, monkeypatch, mutation, expected_failure):
    _, output, state = prepare(tmp_path)
    generate = ReplyGenerator.generate
    async def corrupted(self, *args, **kwargs):
        result = await generate(self, *args, **kwargs)
        evidence = deepcopy(result.evidence)
        if mutation == "tool_fact":
            evidence["read_facts"]["fun"]["status"] = "available"
        if mutation == "tool_result":
            evidence["tool_results"][0]["result"]["items"][0]["status"] = "available"
        if mutation == "final":
            return replace(result, text="查询结果：" + evidence["required_read"]["day"] + "，娱乐首单可用。")
        return replace(result, evidence=evidence)
    monkeypatch.setattr(ReplyGenerator, "generate", corrupted)
    if mutation == "quote":
        get = ProtocolOutbox.get
        def wrong_source(self, *args, **kwargs):
            value = get(self, *args, **kwargs)
            value["source"]["serverId"] = "wrong-server-id"
            return value
        monkeypatch.setattr(ProtocolOutbox, "get", wrong_source)
    result = run(state, output, cases=[{"id": "query", "text": "查询娱乐首单状态", "actor": "manager",
        "category": "query", "expected_write": {}, "expected_read": ["fun"]}])
    assert result["mechanical_passed"] is False and result["passed"] is False
    assert expected_failure in result["cases"][0]["failures"]


def test_whole_final_oracle_rejects_missing_duplicate_and_contradictory_claims():
    facts = {"fun": {"status": "unknown"}, "rank": {"status": "available"}}
    names = {"fun": "娱乐首单", "rank": "竞技首单"}
    good = "查询结果：2026-10-29，娱乐首单未确认；竞技首单可用。"
    assert edges.dated_final_matches(good, day="2026-10-29", expected=facts, names=names, recorded=False)
    for bad in (good + "但娱乐首单其实可用。", good.replace("未确认", "可用"),
                good.replace("；竞技首单可用", ""), good.replace("竞技首单可用", "娱乐首单未确认")):
        assert not edges.dated_final_matches(bad, day="2026-10-29", expected=facts, names=names, recorded=False)


def test_default_cli_cannot_clone_or_open_models(tmp_path, monkeypatch):
    monkeypatch.setattr(edges, "clone_state", lambda *_: pytest.fail("default must not clone credentials"))
    with pytest.raises(SystemExit) as error:
        edges.main(["--source-state", str(tmp_path / "source"), "--output", str(tmp_path / "out"),
                    "--group-ref", "group_a", "--own", "wxid_robot", "--thread", "synthetic@chatroom"])
    assert error.value.code == 2 and not (tmp_path / "out").exists()
