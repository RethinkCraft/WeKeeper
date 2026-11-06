"""Restricted native chat capacity; fixed barriers, real MCP/SQLite, no model API."""
import asyncio
from copy import deepcopy
import json
import importlib.util
import threading
import time

import pytest

from test_bot_protocol_business import harness, wait_finished
from chatlog_keeper.bot.memory_host import MemoryBusinessMCPHost
from chatlog_keeper.bot.protocol_runtime import ProviderResources
from chatlog_keeper.bot.providers import ProviderError
from chatlog_keeper.bot.reply_generator import GenerationResult, ReplyGenerator
from chatlog_keeper.bot.request_policy import (
    BUSINESS_CAPABLE, CHAT_ONLY, POLICY_KEY, classify_native_request, request_capability,
)
from chatlog_keeper.bot.service import Service
from chatlog_keeper.bot.store import BotStore

LAZY_HOSTS = [MemoryBusinessMCPHost]


def event_for(h, text, **changes):
    key = h.inbox.add(str(100 + len(h.inbox.rows)), text, **changes)
    worker = h.worker()
    event = worker._event(h.inbox.rows[-1], "group_a", h.store.binding("group_a"))
    worker.close()
    return key, event


def accept(h, text, *, optin=True, **changes):
    key, event = event_for(h, text, **changes)
    request = h.store.begin_request(event, native_chat_policy=optin)["request"]
    return key, request


@pytest.mark.parametrize("text", ["你好", "在吗，接个话呀", "@机器人\u2005在吗，接个话呀", "谢谢你！", "hi", "早上好！"])
def test_only_complete_native_social_messages_are_restricted(harness, text):
    h = harness; _, request = accept(h, text)
    assert request_capability(request) == CHAT_ONLY


@pytest.mark.parametrize("text", [
    "好的", "那现在呢", "在吗，娱乐首单什么状态", "在吗，今天娱乐首单已无请报备", "在吗 顺便记录一下",
    "不要报备，聊聊天", "‘你好’", "> 你好", "如果你好", "@机器人 @查询娱乐首单 你好", "@机器人 @其他人 在吗",
    "@机器人 查询娱乐首单，然后你好", "你好，顺便做件事", "你好，下次再报备", "娱乐", "query", "你好;查询库存",
    "@查询娱乐首单 你好", "@娱乐首单 在吗", "@fun 你好",
])
def test_ambiguous_mixed_quoted_or_multiple_mentions_keep_business_fifo(harness, text):
    _, request = accept(harness, text)
    assert request_capability(request) == BUSINESS_CAPABLE


@pytest.mark.parametrize("source_change", [
    {"type": True}, {"type": 49}, {"appMessageType": 57}, {"quote": {"embeddedText": "你好"}},
    {"direction": "outgoing"}, {"quoteEligible": False}, {"disposition": "quarantined"},
    {"mentions": {"state": "known", "userIds": ["wxid_robot", "another"]}},
    {"senderId": "other"}, {"threadId": "another@chatroom"},
])
def test_restricted_policy_requires_exact_native_source(harness, source_change):
    h = harness; _, event = event_for(h, "你好")
    event["native_source"].update(source_change)
    assert classify_native_request(event, h.store.binding("group_a")["business"]) == BUSINESS_CAPABLE


def test_configured_business_alias_is_never_treated_as_social(harness):
    h = harness; _, event = event_for(h, "你好")
    business = h.store.binding("group_a")["business"]
    business["items"][0]["aliases"].append("你好")
    assert classify_native_request(event, business) == BUSINESS_CAPABLE


def test_policy_is_atomic_strips_incoming_and_old_rows_are_not_replanned(harness):
    h = harness; key, event = event_for(h, "你好")
    event[POLICY_KEY] = {"version": 1, "capability": "invented"}
    with h.store.conn:
        h.store.conn.execute("CREATE TRIGGER fail_chat_accept BEFORE INSERT ON reply_outbox BEGIN SELECT RAISE(ABORT,'fixed failure'); END")
    try:
        with pytest.raises(Exception, match="fixed failure"):
            h.store.begin_request(event, native_chat_policy=True)
        assert h.store.request(key) is None
    finally:
        with h.store.conn: h.store.conn.execute("DROP TRIGGER fail_chat_accept")
    saved = h.store.begin_request(event, native_chat_policy=True)["request"]
    assert request_capability(saved) == CHAT_ONLY
    key2, old_event = event_for(h, "在吗")
    old_event[POLICY_KEY] = saved["event"][POLICY_KEY]
    old = h.store.begin_request(old_event)["request"]
    assert POLICY_KEY not in old["event"] and request_capability(old) == BUSINESS_CAPABLE
    assert h.store.begin_request(old_event, native_chat_policy=True)["request"] == old
    assert h.store.request(key2) == old


@pytest.mark.parametrize("host_class", LAZY_HOSTS)
def test_every_transport_rejects_chat_before_starting_mcp(harness, host_class):
    h = harness; key, _ = accept(h, "你好")
    async def run():
        host = host_class(h.store.directory.parent, key)
        async with host:
            with pytest.raises(ProviderError, match="capability_denied"):
                await host.prepare()
            with pytest.raises(ProviderError, match="capability_denied"):
                await host.call("query_offer_status", {})
            assert host.owner is None and host.host is None and host.tool_timings == []
    asyncio.run(run())


def test_chat_service_and_transaction_cannot_read_or_write_business(harness):
    h = harness; key, _ = accept(h, "你好")
    service = Service(h.store, {"event_id": key, "writable": True})
    assert service.tools == []
    for name, args in [("list_offer_items", {}), ("query_offer_status", {"item": "fun"}),
                       ("report_offer_status", {"item": "fun", "status": "unavailable", "evidence": "你好"})]:
        assert service.call(name, args)["error"] == "bot_business_capability_denied"
    assert h.store.record_offer(key, "fun", "unavailable")["error"] == "bot_business_capability_denied"
    assert h.store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 0
    assert h.store.conn.execute("SELECT count(*) FROM audit_log WHERE kind='report'").fetchone()[0] == 0


@pytest.mark.parametrize("mutation", ["remove_policy", "changed_capability", "changed_source", "changed_business"])
def test_caller_cannot_replace_accepted_chat_capability(harness, mutation):
    h = harness; _, request = accept(h, "你好")
    event = deepcopy(request["event"])
    if mutation == "remove_policy": event.pop(POLICY_KEY)
    elif mutation == "changed_capability": event[POLICY_KEY]["capability"] = BUSINESS_CAPABLE
    elif mutation == "changed_source": event["native_source"]["sourceSha256"] = "f" * 64
    else: event["native_business"]["id"] = "other"
    providers = []
    runner = ReplyGenerator(h.store, state_dir=h.store.directory.parent,
        provider_factory=lambda profile: providers.append(profile))
    with pytest.raises(ValueError): asyncio.run(runner.generate(event, request, []))
    assert providers == []


def test_chat_model_handler_cannot_acquire_business_authority(harness):
    h = harness; key, request = accept(h, "你好")
    calls = []
    class Provider:
        def complete(self, messages, instructions, tools, handler, metadata):
            assert tools == []
            for name in ("list_offer_items", "query_offer_status", "report_offer_status"):
                with pytest.raises(ProviderError, match="capability_denied"): handler(name, {})
            calls.append(metadata["bot_request_id"])
            return "你好呀。"
        def close(self): pass
    runner = ReplyGenerator(h.store, state_dir=h.store.directory.parent, provider_factory=lambda profile: Provider())
    result = asyncio.run(runner.generate(request["event"], request, []))
    assert calls == [key] and result.evidence["tool_results"] == []
    assert result.evidence["capability"] == CHAT_ONLY and result.metrics["used_mcp"] is False


@pytest.mark.parametrize("host_class", LAZY_HOSTS)
def test_stalled_chat_leaves_original_slot_for_real_report_then_query(harness, tmp_path, host_class):
    h = harness
    entered, release = threading.Event(), threading.Event()
    providers, active, maximum = [], 0, 0
    lock = threading.Lock()
    class Provider:
        def complete(self, messages, instructions, tools, handler, metadata):
            nonlocal active, maximum
            assert tools == []
            with lock:
                active += 1; maximum = max(maximum, active); providers.append(metadata["bot_request_id"])
            entered.set()
            try:
                assert release.wait(10), "fixed chat barrier was not released"
                return "在呢。"
            finally:
                with lock: active -= 1
        def close(self): pass
    resources = ProviderResources(h.store, state_dir=tmp_path, usage_dir=tmp_path/"usage", dashboard_dir=tmp_path/"dashboard")
    runner = ReplyGenerator(h.store, state_dir=tmp_path, provider_factory=lambda profile: Provider(),
        local_execution_factory=resources.local_business_execution, host_factory=host_class)
    keys = [h.inbox.add(str(n), text) for n, text in enumerate(
        ["在吗，接个话呀", "你好", "今天娱乐首单已无，请报备", "帮我查今天娱乐首单的状态"], 20)]
    worker = h.worker(); worker.generator = runner
    try:
        worker.run_once(); assert entered.wait(3)
        deadline = time.monotonic() + 8
        while any(h.store.reply(k)["status"] != "pending" for k in keys[2:]):
            worker.run_once()
            assert time.monotonic() < deadline, "business was blocked by the pure-chat barrier"
            time.sleep(.005)
        assert not release.is_set()
        assert providers == [keys[0]]
        assert [h.store.reply(k)["status"] for k in keys[:2]] == ["awaiting_generation"] * 2
        assert h.store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 1
        assert h.store.conn.execute("SELECT count(*) FROM audit_log WHERE kind='report'").fetchone()[0] == 1
        receipt = h.store.receipt(keys[2]); assert receipt["version"] == 1
        final = worker._frozen_record(h.store.request(keys[3]))
        assert final["evidence"]["read_facts"]["fun"]["event_id"] == keys[2]
        assert final["evidence"]["read_facts"]["fun"]["version"] == 1
        release.set()
        deadline = time.monotonic() + 5
        while any(h.store.reply(k)["status"] != "pending" for k in keys):
            worker.run_once(); assert time.monotonic() < deadline; time.sleep(.005)
        assert providers == keys[:2] and maximum == 1 and worker.pool._max_workers == 2
    finally:
        release.set(); worker.close(); resources.close()


def test_report_commit_does_not_release_fifo_before_final_freezes(harness, monkeypatch):
    h = harness; h.generator.chat_policy_enabled = True
    report = h.inbox.add("30", "今天娱乐首单已无，请报备")
    query = h.inbox.add("31", "查询娱乐首单")
    worker = h.worker(); worker.run_once(); wait_finished(worker)
    original = h.store.queue_reply
    def fail_final(event_id, *args, **kwargs):
        if event_id == report: raise OSError("fixed durable final failure")
        return original(event_id, *args, **kwargs)
    monkeypatch.setattr(h.store, "queue_reply", fail_final)
    with pytest.raises(OSError, match="durable final"): worker.run_once()
    assert h.store.receipt(report)["version"] == 1
    assert h.generator.calls == [report] and h.store.reply(query)["status"] == "awaiting_generation"
    monkeypatch.setattr(h.store, "queue_reply", original)
    worker.run_once(block=True)
    assert h.generator.calls == [report, query]
    assert h.store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 1


def test_restricted_policy_survives_reopen_and_configuration_revocation(harness):
    h = harness; key, request = accept(h, "你好")
    reopened = BotStore(h.store.directory.parent)
    try:
        assert reopened.request(key) == request
        assert request_capability(reopened.request(key)) == CHAT_ONLY
        configuration = h.store.config(); configuration["groups"][0]["enabled"] = False
        h.store.configure(configuration)
        saved = reopened.request(key)
        assert saved["event"][POLICY_KEY] == request["event"][POLICY_KEY]
        assert request_capability(saved) == CHAT_ONLY
        assert h.store.reply(key)["status"] == "cancelled"
        assert Service(reopened, {"event_id": key, "writable": True}).call("query_offer_status", {})["error"] == "bot_business_capability_denied"
    finally:
        reopened.close()


def test_changed_persisted_policy_cannot_grant_model_tools_or_freeze_as_original(harness):
    h = harness; key, request = accept(h, "你好")
    class Provider:
        def complete(self, messages, instructions, tools, handler, metadata):
            assert tools == []
            changed = deepcopy(request["event"]); changed[POLICY_KEY]["capability"] = BUSINESS_CAPABLE
            with h.store.lock, h.store.conn:
                h.store.conn.execute("UPDATE requests SET event=? WHERE event_id=?", (json.dumps(changed), key))
            with pytest.raises(ProviderError, match="capability_denied"): handler("query_offer_status", {})
            return "你好。"
        def close(self): pass
    runner = ReplyGenerator(h.store, state_dir=h.store.directory.parent, provider_factory=lambda profile: Provider())
    with pytest.raises(ProviderError, match="policy_changed"):
        asyncio.run(runner.generate(request["event"], request, []))
    assert h.store.reply(key)["text"] is None


def test_old_social_row_keeps_original_business_fifo(harness):
    h = harness; h.generator.chat_policy_enabled = True
    old, event = event_for(h, "你好")
    h.store.begin_request(event)  # Deliberately predates trusted admission opt-in.
    report = h.inbox.add("40", "今天娱乐首单已无，请报备")
    entered, release = threading.Event(), threading.Event()
    original = h.generator.generate
    async def generate(event, request, nearby, progress=None):
        if event["event_id"] == old:
            entered.set(); assert release.wait(3)
        return await original(event, request, nearby, progress)
    h.generator.generate = generate
    worker = h.worker()
    try:
        worker.run_once(); assert entered.wait(2)
        for _ in range(3): worker.run_once()
        assert list(worker.inflight) == [old]
        assert h.store.reply(report)["status"] == "awaiting_generation"
        assert request_capability(h.store.request(old)) == BUSINESS_CAPABLE
    finally:
        release.set(); worker.close()


@pytest.mark.parametrize("host_class", LAZY_HOSTS)
def test_business_response_loss_and_restart_recover_original_receipt_once(harness, tmp_path, host_class):
    h = harness; lost = []
    class LostResponse(host_class):
        async def call(self, name, arguments):
            result = await super().call(name, arguments)
            if name == "report_offer_status":
                lost.append(name)
                raise TimeoutError("fixed response loss after durable report")
            return result
    resources = ProviderResources(h.store, state_dir=tmp_path, usage_dir=tmp_path/"usage", dashboard_dir=tmp_path/"dashboard")
    providers = []
    runner = ReplyGenerator(h.store, state_dir=tmp_path, provider_factory=lambda profile: providers.append(profile),
        local_execution_factory=resources.local_business_execution, host_factory=LostResponse)
    report = h.inbox.add("50", "今天娱乐首单已无，请报备")
    query = h.inbox.add("51", "帮我查今天娱乐首单的状态")
    worker = h.worker(); worker.generator = runner
    try:
        worker.run_once(block=True)
        assert lost == ["report_offer_status"] and h.store.receipt(report)["version"] == 1
        assert worker._frozen_record(h.store.request(report))["evidence"]["recovery"] == "committed_report"
        assert h.store.reply(query)["status"] == "awaiting_generation"
        worker.close()
        restarted = h.worker(); restarted.generator = runner
        restarted.run_once(block=True)
        frozen = restarted._frozen_record(h.store.request(query))
        assert frozen["evidence"]["read_facts"]["fun"]["event_id"] == report
        assert frozen["evidence"]["read_facts"]["fun"]["version"] == 1
        assert providers == [] and lost == ["report_offer_status"]
        assert h.store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 1
        assert h.store.conn.execute("SELECT count(*) FROM audit_log WHERE kind='report'").fetchone()[0] == 1
    finally:
        worker.close(); resources.close()

