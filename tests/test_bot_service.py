import asyncio
from datetime import datetime, timedelta, timezone
import os
import sys

import pytest

from chatlog_keeper.bot.persona import default_persona, persona_prompt
from chatlog_keeper.bot.service import Service, report_evidence
from chatlog_keeper.bot.store import BotStore


NOW = "2026-10-27T12:00:00+08:00"


def config():
    return {
        "enabled": True,
        "providers": [{"id": "gpt", "name": "GPT", "kind": "siwc", "model": "configured-model"}],
        "personas": [default_persona()],
        "businesses": [{"id": "shop", "name": "总店", "reporter_ids": ["wxid_manager"],
                        "items": [{"id": "fun", "name": "娱乐首单", "aliases": ["娱乐"]},
                                  {"id": "rank", "name": "竞技首单", "aliases": []}]},
                       {"id": "other", "name": "另店", "reporter_ids": ["wxid_other"],
                        "items": [{"id": "fun", "name": "娱乐首单", "aliases": []}]}],
        "groups": [{"group_ref": group, "business_id": business, "persona_id": "alpha", "provider_id": "gpt",
                    "bot_wxid": "wxid_robot", "enabled": True}
                   for group, business in (("group_a", "shop"), ("group_b", "shop"), ("group_c", "other"))],
    }


@pytest.fixture
def store(tmp_path):
    value = BotStore(tmp_path)
    value.configure(config())
    yield value
    value.close()


def request(store, key="one", *, group="group_a", sender="wxid_manager", text="今日娱乐首单已无",
            time="2026-10-27T11:00:00+08:00", order=1):
    event = {"message_ref": "msg_" + key, "group_ref": group, "sender_id": sender,
             "text": text, "occurred_at": time, "archive_id": order}
    return store.begin_request(event)


def service(store, event_id="msg_one", **overrides):
    return Service(store, {"event_id": event_id, "writable": True, "now": NOW, **overrides})


def report(svc, **overrides):
    return svc.call("report_offer_status", {"item": "娱乐首单", "status": "unavailable", "evidence": "今日娱乐首单已无", **overrides})


def test_shared_business_and_unknown_next_day(store):
    request(store)
    recorded = report(service(store))
    assert recorded["ok"] and recorded["recorded"]
    assert recorded["version"] == 1
    other_group = Service(store, {"group_ref": "group_b", "now": NOW})
    result = other_group.call("query_offer_status", {"item": "娱乐"})
    assert result["items"][0]["status"] == "unavailable"
    assert result["items"][0]["message_ref"] == "msg_one"
    assert other_group.call("query_offer_status", {"item": "娱乐", "date": "2026-10-28"})["items"][0]["status"] == "unknown"
    separate = Service(store, {"group_ref": "group_c", "now": NOW})
    assert separate.call("query_offer_status", {"item": "娱乐首单"})["items"][0]["status"] == "unknown"


def test_unauthorized_context_and_model_identity_cannot_write(store):
    request(store, sender="wxid_visitor")
    assert report(service(store))["error"] == "report_not_authorized"
    assert report(service(store), sender_id="wxid_manager")["error"] == "invalid_tool_arguments"
    assert report(service(store, sender_id="wxid_manager"))["error"] == "bot_context_mismatch"
    readonly = Service(store, {"group_ref": "group_a", "now": NOW})
    assert report(readonly)["error"] == "read_only_bot_context"
    assert len(readonly.tools) == 2
    assert store.offer_status("shop", "2026-10-27", "fun")["version"] == 0


@pytest.mark.parametrize("text", [
    "今日娱乐首单已无吗？", "今日娱乐首单已无？", "如果今日娱乐首单已无，帮忙报备",
    "听说今日娱乐首单已无", "不是今日娱乐首单已无", "不要记录今日娱乐首单已无",
    "张三说过今日娱乐首单已无", "比如今日娱乐首单已无", "“今日娱乐首单已无”",
    "‘今日娱乐首单已无’是什么意思", "'今日娱乐首单已无'是什么意思",
    "> 今日娱乐首单已无", "今日娱乐首单尚未用完", "今日娱乐首单已无，还是可用？",
])
def test_report_requires_affirmative_current_source(store, text):
    request(store, text=text)
    result = report(service(store))
    assert result["error"] == "explicit_report_evidence_required"
    assert store.offer_status("shop", "2026-10-27", "fun")["status"] == "unknown"


def test_invented_evidence_and_missing_information_ask_for_clarification(store):
    request(store, text="记录一下")
    assert report(service(store))["error"] == "explicit_report_evidence_required"
    assert report(service(store), item="不存在")["needs_clarification"]
    assert service(store).call("report_offer_status", {"status": "unavailable"})["error"] == "offer_item_required"
    assert service(store).call("report_offer_status", {"item": "娱乐"})["error"] == "offer_status_required"


def test_request_and_report_replay_does_not_duplicate_audit_or_status(store):
    assert request(store)["accepted"] is True
    first = report(service(store))
    assert request(store)["accepted"] is False
    second = report(service(store))
    assert second["replayed"] and first["version"] == second["version"] == 1
    assert len([row for row in store.audit() if row["kind"] == "report"]) == 1
    reply = store.reply("msg_one")
    assert reply["status"] == "awaiting_generation" and reply["text"] is None
    with pytest.raises(ValueError, match="identity_conflict"):
        request(store, sender="wxid_other")


def test_newer_correction_and_stale_report_order(store):
    request(store)
    assert report(service(store))["ok"]
    request(store, "correction", text="娱乐首单恢复可用", time="2026-10-27T11:10:00+08:00", order=3)
    correction = report(service(store, "msg_correction"), status="available", evidence="娱乐首单恢复可用")
    assert correction["status"] == "available" and correction["version"] == 2
    request(store, "late", time="2026-10-27T11:05:00+08:00", order=2)
    assert report(service(store, "msg_late"))["error"] == "stale_report"
    assert store.offer_status("shop", "2026-10-27", "fun")["status"] == "available"


def test_optimistic_revision_and_date_boundaries(store):
    revision = store.config()["revision"]
    with pytest.raises(ValueError, match="configuration_conflict"):
        store.configure(config(), expected_revision=revision - 1)
    assert store.configure(config(), expected_revision=revision) == revision + 1
    request(store, time="2026-10-26T16:01:00+00:00")
    assert report(service(store))["day"] == "2026-10-27"
    request(store, "version", text="娱乐首单恢复可用", time="2026-10-27T11:20:00+08:00")
    assert report(service(store, "msg_version"), status="available", evidence="娱乐首单恢复可用", expected_version=0)["error"] == "offer_version_conflict"
    request(store, "yesterday", time="2026-10-26T23:59:00+08:00")
    assert report(service(store, "msg_yesterday"))["error"] == "report_expired"
    assert report(service(store), date="2026-10-28")["error"] == "report_source_date_mismatch"


def test_cloud_configuration_and_command_outcome_commit_atomically(store):
    initial = store.config()["revision"]
    control = {"command_id": "cloud_config1", "revision": 4}
    applied = store.configure(config(), control=control)
    assert applied == initial + 1
    assert store.get("cloud_config_revision") == 4
    assert store.get("control_cloud_config1") == {"status": "applied", "config_revision": 4}
    assert store.configure(config(), control=control) == applied
    assert store.config()["revision"] == applied
    with pytest.raises(ValueError, match="revision_stale"):
        store.configure(config(), control={"command_id": "other_cloud_command", "revision": 4})
    changed = config()
    changed["enabled"] = False
    with pytest.raises(ValueError, match="control_identity_conflict"):
        store.configure(changed, control=control)
    assert store.config()["enabled"]


def test_configuration_failure_rolls_back_cloud_ack_and_revision(store, monkeypatch):
    original_revision = store.config()["revision"]

    def fail(*args, **kwargs):
        raise RuntimeError("simulated_configuration_failure")

    monkeypatch.setattr(store, "_audit", fail)
    with pytest.raises(RuntimeError, match="configuration_failure"):
        store.configure(config(), control={"command_id": "failed_control", "revision": 8})
    assert store.config()["revision"] == original_revision
    assert store.get("cloud_config_revision", 0) == 0
    assert store.get("control_failed_control") is None


def test_quotes_are_bound_to_trigger_and_unknown_send_is_not_retryable(store):
    request(store)
    with pytest.raises(ValueError, match="quote_target_mismatch"):
        store.queue_reply("msg_one", "记好啦", {"message_ref": "msg_other", "group_ref": "group_a"})
    reply = store.queue_reply("msg_one", "记好啦")
    assert reply["reply_to"]["message_ref"] == "msg_one"
    assert store.queue_reply("msg_one", "记好啦")["reply_id"] == reply["reply_id"]
    store.transition_reply(reply["reply_id"], "sending")
    store.transition_reply(reply["reply_id"], "unknown", error="confirmation_missing")
    assert store.pending_replies() == []
    with pytest.raises(ValueError, match="reply_transition"):
        store.transition_reply(reply["reply_id"], "pending")
    with pytest.raises(ValueError, match="reply_transition"):
        store.defer_unstarted_reply(reply["reply_id"], "native_ui_busy")


def test_unstarted_send_can_be_deferred_without_changing_generated_text(store):
    request(store)
    reply = store.queue_reply("msg_one", "记好啦")
    store.transition_reply(reply["reply_id"], "sending")
    result = store.defer_unstarted_reply(reply["reply_id"], "native_ui_busy")
    assert result["status"] == "pending" and result["text"] == "记好啦"
    assert len(store.pending_replies()) == 1
    with pytest.raises(ValueError, match="reply_transition"):
        store.defer_unstarted_reply(reply["reply_id"], "native_ui_busy")


def test_history_only_confirmed_and_group_persona_version_isolated(store):
    request(store)
    reply = store.queue_reply("msg_one", "我记住啦")
    assert store.history("group_a", "alpha", "1") == []
    store.transition_reply(reply["reply_id"], "sending")
    with pytest.raises(ValueError, match="confirmation_required"):
        store.transition_reply(reply["reply_id"], "sent")
    store.transition_reply(reply["reply_id"], "sent", confirmation_ref="msg_bot_reply")
    assert store.history("group_a", "alpha", "1") == [
        {"role": "user", "content": "今日娱乐首单已无"}, {"role": "assistant", "content": "我记住啦"}]
    assert store.history("group_b", "alpha", "1") == []
    assert store.history("group_a", "alpha", "2") == []


def test_disable_cancels_pending_and_revokes_inflight_report(store):
    request(store)
    value = config()
    value["enabled"] = False
    store.configure(value)
    assert store.reply("msg_one")["status"] == "cancelled"
    assert report(service(store))["error"] == "bot_group_disabled"
    assert store.pending_replies() == []


def test_private_database_and_configuration_validation(tmp_path):
    value = BotStore(tmp_path)
    if os.name != "nt":
        assert value.directory.stat().st_mode & 0o777 == 0o700
        assert value.path.stat().st_mode & 0o777 == 0o600
    bad = config()
    bad["businesses"][0]["items"][1]["aliases"] = ["娱乐"]
    with pytest.raises(ValueError, match="ambiguous"):
        value.configure(bad)
    bad = config()
    bad["providers"][0]["api_key"] = "not-a-real-key"
    with pytest.raises(ValueError, match="credentials_require_reference"):
        value.configure(bad)
    value.close()


def test_business_write_audit_and_outbox_rollback_together(store, monkeypatch):
    request(store)
    original = store._audit

    def fail(*args, **kwargs):
        raise RuntimeError("simulated_persistence_failure")

    monkeypatch.setattr(store, "_audit", fail)
    with pytest.raises(RuntimeError, match="persistence_failure"):
        report(service(store))
    monkeypatch.setattr(store, "_audit", original)
    assert store.offer_status("shop", "2026-10-27", "fun")["version"] == 0
    assert report(service(store))["version"] == 1


def test_receipt_survives_generation_failure_and_admin_adjustment_is_versioned(store):
    request(store)
    assert store.receipt("msg_one") is None
    assert report(service(store))["recorded"]
    receipt = store.receipt("msg_one")
    assert receipt["recorded"] and receipt["status"] == "unavailable"
    assert store.reply("msg_one")["status"] == "awaiting_generation"
    payload = {"business_id": "shop", "item_id": "fun", "day": "2026-10-27",
               "status": "available", "expected_revision": 1, "actor": "web_admin_1"}
    adjustment = store.adjust_offer(payload)
    assert adjustment["version"] == 2 and adjustment["status"] == "available"
    assert store.adjust_offer(payload)["error"] == "offer_version_conflict"
    assert any(row["kind"] == "admin_adjustment" for row in store.audit())


def test_admin_adjustment_command_is_idempotent_even_after_offer_changes(store):
    payload = {"business_id": "shop", "item_id": "fun", "day": "2026-10-27",
               "status": "available", "expected_revision": 0, "actor": "web_admin_1", "event_id": "admin_command1"}
    first = store.adjust_offer(payload)
    assert first["version"] == 1
    newer = {**payload, "event_id": "admin_command2", "status": "unavailable", "expected_revision": 1}
    assert store.adjust_offer(newer)["version"] == 2
    replay = store.adjust_offer(payload)
    assert replay["replayed"] and replay["version"] == 1
    assert store.offer_status("shop", "2026-10-27", "fun")["status"] == "unavailable"
    assert store.adjust_offer({**payload, "status": "unknown"})["error"] == "admin_command_identity_conflict"
    assert len([row for row in store.audit() if row["kind"] == "admin_adjustment"]) == 2


@pytest.mark.parametrize("text", ["昨天娱乐首单已无", "2026-10-26娱乐首单已无"])
def test_report_cannot_relabel_a_previous_days_statement_as_today(store, text):
    request(store, text=text)
    assert not report(service(store), evidence="娱乐首单已无")["ok"]
    assert store.offer_status("shop", "2026-10-27", "fun")["version"] == 0


def test_negative_availability_cannot_be_recorded_available(store):
    request(store, text="娱乐首单没有可用名额")
    result = report(service(store), evidence="娱乐首单没有可用名额", status="available")
    assert result["error"] == "explicit_report_evidence_required"


def test_persona_has_original_examples_and_no_live_business_facts():
    profile = default_persona()
    assert profile["markdown"].count("### 示例 ") == 3
    text = persona_prompt(profile)
    assert "不是当前业务事实" in text
    assert "没有联网" in text
    assert "业务工具" in text


def test_mcp_readonly_default_and_server_bound_write_context(store, tmp_path):
    pytest.importorskip("mcp")
    from chatlog_keeper.bot.mcp import create_server
    readonly = create_server(tmp_path, group_ref="group_a")
    bound = None
    try:
        names = {tool.name for tool in asyncio.run(readonly.list_tools())}
        assert names == {"list_offer_items", "query_offer_status"}
        now = datetime.now(timezone.utc).isoformat()
        request(store, time=now)
        bound = create_server(tmp_path, event_id="msg_one")
        names = {tool.name for tool in asyncio.run(bound.list_tools())}
        assert "report_offer_status" in names
        assert bound.bot_service.context["sender_id"] == "wxid_manager"
        result = bound.bot_service.call("report_offer_status", {"item": "娱乐", "status": "unavailable", "evidence": "今日娱乐首单已无"})
        assert result["ok"]
        with pytest.raises(ValueError, match="context_mismatch"):
            create_server(tmp_path, event_id="msg_one", group_ref="group_c")
    finally:
        readonly.bot_store.close()
        if bound:
            bound.bot_store.close()


def test_bot_stdio_round_trip_binds_real_host_context_with_synthetic_data(store, tmp_path):
    pytest.importorskip("mcp")
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    request(store, time=datetime.now(timezone.utc).isoformat())

    async def exercise():
        params = StdioServerParameters(command=sys.executable,
            args=["-m", "chatlog_keeper.bot.mcp", "--state-dir", str(tmp_path), "--event-id", "msg_one"],
            env=os.environ.copy())
        with (tmp_path / "bot-mcp-stderr.log").open("w") as errors:
            async with stdio_client(params, errlog=errors) as (reader, writer):
                async with ClientSession(reader, writer, read_timeout_seconds=timedelta(seconds=10)) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    assert len(tools.tools) == 3
                    assert sum(tool.annotations.readOnlyHint for tool in tools.tools) == 2
                    listed = await session.call_tool("list_offer_items", {})
                    assert not listed.isError and listed.structuredContent["business_id"] == "shop"
                    written = await session.call_tool("report_offer_status", {
                        "item": "娱乐", "status": "unavailable", "evidence": "今日娱乐首单已无"})
                    assert not written.isError and written.structuredContent["recorded"]
                    replay = await session.call_tool("report_offer_status", {
                        "item": "娱乐", "status": "unavailable", "evidence": "今日娱乐首单已无"})
                    assert replay.structuredContent["replayed"]
                    queried = await session.call_tool("query_offer_status", {"item": "娱乐"})
                    assert queried.structuredContent["items"][0]["status"] == "unavailable"

    asyncio.run(exercise())
