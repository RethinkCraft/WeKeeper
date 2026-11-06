"""Small native business replay oracle, including deliberate false success."""
import importlib.util
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

from chatlog_keeper.bot.store import BotStore
from chatlog_keeper.bot.reply_generator import ReplyGenerator
from test_bot_service import config


spec = importlib.util.spec_from_file_location("native_probe", Path(__file__).parents[1] / "scripts/diagnostics/check_protocol_business.py")
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class Provider:
    def __init__(self, omit_report=False):
        self.omit_report = omit_report

    def complete(self, messages, instructions, tools, call, metadata):
        if metadata.get("_required_report") and not self.omit_report:
            call("report_offer_status", {"item": "娱乐首单", "status": "unavailable", "evidence": messages[-1]["content"]})
        if metadata.get("_required_read_tool"):
            call("query_offer_status", {"item": "娱乐首单", "date": metadata["_required_read_day"]})
        return "记好啦，娱乐首单已无。" if metadata.get("_required_report") else "收到啦"

    def close(self):
        pass


@pytest.mark.parametrize("omit_report", [False, True])
def test_real_native_custody_and_business_oracle(tmp_path, omit_report):
    original = tmp_path / "original"
    with closing(BotStore(original)) as store:
        store.configure(config())
        (original / "bot/secrets").mkdir(mode=0o700)
    output = tmp_path / "probe"
    state = probe.clone_state(original, output)
    report = probe.run_samples(state, output, group_ref="group_a", own="wxid_robot", thread="synthetic@chatroom",
                               provider_factory=lambda _: Provider(omit_report))
    assert report["passed"] is not omit_report
    assert report["wechat_sent"] is False and report["phone_received"] is False
    assert report["recovery"]["no_new_model"] is True
    if omit_report:
        assert "business_result_mismatch" in report["cases"][0]["failures"]
    else:
        assert report["recovery"]["report_rows_added"] == 1
        first, second = report["cases"][1:3]
        assert first["request_id"] != second["request_id"]
        assert first["evidence"]["read_facts"] == second["evidence"]["read_facts"]
    with closing(BotStore(original)) as store:
        assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 0
        assert store.conn.execute("SELECT count(*) FROM requests").fetchone()[0] == 0


def test_oracle_rejects_correct_facts_with_contradictory_final(tmp_path, monkeypatch):
    original = tmp_path / "original"
    with closing(BotStore(original)) as store:
        store.configure(config())
        (original / "bot/secrets").mkdir(mode=0o700)
    output = tmp_path / "probe"
    state = probe.clone_state(original, output)
    generate = ReplyGenerator.generate
    async def wrong_final(self, *args, **kwargs):
        result = await generate(self, *args, **kwargs)
        return replace(result, text="查询结果：2026-10-29，娱乐首单可用。")
    monkeypatch.setattr(ReplyGenerator, "generate", wrong_final)
    report = probe.run_samples(state, output, group_ref="group_a", own="wxid_robot", thread="synthetic@chatroom",
                               provider_factory=lambda _: Provider())
    assert report["passed"] is False
    assert report["recovery"]["report_rows_added"] == 1
    assert all("final_business_claim_mismatch" in row["failures"] for row in report["cases"][:3])
