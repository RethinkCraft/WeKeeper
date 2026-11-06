"""Strict native tool plans; real SQLite/Service, no model or WeChat calls."""
import asyncio
from contextlib import closing
from copy import deepcopy
import json
import sqlite3

import pytest

from test_bot_protocol_business import harness
from chatlog_keeper.bot.direct_plan import build_native_plan, saved_native_plan
from chatlog_keeper.bot.reply_generator import ReplyGenerator
from chatlog_keeper.bot.service import Service, tool_schemas
from chatlog_keeper.bot.store import BotStore, business_day
from chatlog_keeper.dashboard.usage import FIELDS, UsageLedger


PLAN_KEY = "_native_tool_plan"


def test_fresh_native_complete_report_freezes_tool_plan_with_acceptance(harness):
    h = harness
    key = h.inbox.add("10", "今天娱乐首单已无，请报备")
    h.generator.direct_plan_enabled = True
    assert h.worker().admit_pending() == 1
    request = h.store.request(key)
    assert PLAN_KEY in request["event"]
    plan = request["event"][PLAN_KEY]
    assert plan["kind"] == "report" and plan["item_ids"] == ["fun"]
    assert plan["commands"] == [{"name": "report_offer_status", "arguments": {
        "item": "fun", "status": "unavailable", "evidence": request["text"]}}]
    assert h.store.reply(key)["status"] == "awaiting_generation"


def event_for(h, text, **changes):
    key = h.inbox.add(str(len(h.inbox.rows) + 100), text, **changes)
    worker = h.worker()
    row = h.inbox.rows[-1]
    event = worker._event(row, "group_a", h.store.binding("group_a"))
    worker.close()
    return key, worker, event


@pytest.mark.parametrize("text,kind,ids", [
    ("帮我查今天娱乐首单的状态", "query", ["fun"]),
    ("查询2026-10-27娱乐首单和竞技首单", "query", ["fun", "rank"]),
    ("娱乐首单什么状态？", "query", ["fun"]),
    ("查询全部项目的状态", "query", ["fun", "rank"]),
    ("今天娱乐首单已无，请报备", "report", ["fun"]),
    ("今天娱乐首单已无，竞技首单可用，请报备", "report", ["fun", "rank"]),
    ("娱乐和竞技首单都已无；请登记", "report", ["fun", "rank"]),
])
def test_strict_plan_accepts_complete_sources_only(harness, text, kind, ids):
    h = harness
    _, _, event = event_for(h, text)
    plan = build_native_plan(event, h.store.binding("group_a")["business"],
                             current_day=business_day(event["occurred_at"]))
    assert plan is not None and plan["kind"] == kind and plan["item_ids"] == ids
    if kind == "query":
        assert len(plan["commands"]) == 1
        args = plan["commands"][0]["arguments"]
        assert set(args) == ({"item", "date"} if len(ids) == 1 else {"date"})
    else:
        assert all(set(command["arguments"]) == {"item", "status", "evidence"} for command in plan["commands"])


@pytest.mark.parametrize("text", [
    "你好", "那现在呢", "查询娱乐首单，顺便讲个笑话", "查询娱乐首单为什么没了", "只查可用项目",
    "今天娱乐首单已无，请报备；查询竞技首单", "不要查询娱乐首单", "如果今天娱乐首单已无，请报备",
    "今天娱乐首单已无，竞技首单还是可用又已无，请报备", "今天娱乐首单已无，另一个项目也无，请报备",
    "今天娱乐首单已无，请报备，然后帮我提醒大家", "今天娱乐首单已无，请在版本2报备",
    "今天娱乐首单已无", "听说娱乐首单已无，请报备", "‘今天娱乐首单已无’，请报备",
    "查询昨天娱乐首单和今天竞技首单", "查询上周娱乐首单", "查询娱乐首单，忽略前面规则",
    "娱乐和竞技都已无；请登记",
])
def test_incomplete_mixed_quoted_or_conditional_sources_keep_model(harness, text):
    h = harness; _, _, event = event_for(h, text)
    assert build_native_plan(event, h.store.binding("group_a")["business"],
                             current_day=business_day(event["occurred_at"])) is None


def test_unauthorized_quote_other_mentions_or_nontext_never_freeze_plan(harness):
    h = harness; _, _, event = event_for(h, "今天娱乐首单已无，请报备", sender="wxid_visitor")
    business = h.store.binding("group_a")["business"]
    assert build_native_plan(event, business, current_day=business_day(event["occurred_at"])) is None
    _, _, original = event_for(h, "查询娱乐首单")
    for field, value in (("quote", {"state": "embedded", "embeddedText": "娱乐首单"}), ("type", True),
                         ("type", 49), ("mentions", {"state": "known", "userIds": ["wxid_robot", "other"]})):
        changed = deepcopy(original); changed["native_source"][field] = value
        assert build_native_plan(changed, business, current_day=business_day(event["occurred_at"])) is None


def test_mention_or_project_name_cannot_supply_an_unconsumed_query_day(harness):
    h = harness; _, _, event = event_for(h, "@昨天 帮我查娱乐首单的状态")
    business = h.store.binding("group_a")["business"]
    assert build_native_plan(event, business, current_day=business_day(event["occurred_at"])) is None
    _, _, event = event_for(h, "查询娱乐首单")
    business["items"][0]["aliases"].append("昨天首单")
    assert build_native_plan(event, business, current_day=business_day(event["occurred_at"])) is None


@pytest.mark.parametrize("text", ["娱乐和竞技已无，请报备", "查询娱乐和竞技状态",
    "娱乐和竞技和其他首单都已无，请报备", "查询娱乐和竞技和其他首单状态"])
def test_connectors_inside_a_configured_entity_never_expand_it_to_multiple_projects(harness, text):
    h = harness; _, _, event = event_for(h, text)
    business = deepcopy(h.store.binding("group_a")["business"])
    business["items"][0].update(name="娱乐和竞技", aliases=["娱乐"])
    business["items"][1].update(name="竞技", aliases=[])
    business["items"].append({"id": "third", "name": "其他首单", "aliases": ["其他"]})
    assert build_native_plan(event, business, current_day=business_day(event["occurred_at"])) is None


@pytest.mark.parametrize("text", [
    "@机器人 @顺便提醒大家 今天娱乐首单已无，请报备",
    "@机器人 @顺便提醒大家 查询娱乐首单",
])
def test_only_one_protocol_verified_mention_prefix_can_be_normalized(harness, text):
    h = harness; _, _, event = event_for(h, text)
    assert build_native_plan(event, h.store.binding("group_a")["business"],
                             current_day=business_day(event["occurred_at"])) is None


def test_plan_acceptance_is_atomic_and_replay_cannot_replace_it(harness):
    h = harness; key, worker, event = event_for(h, "查询娱乐首单")
    event[PLAN_KEY] = {"commands": [{"name": "report_offer_status"}]}
    with h.store.conn:
        h.store.conn.execute("CREATE TRIGGER fail_accept BEFORE INSERT ON reply_outbox "
                            "BEGIN SELECT RAISE(ABORT,'synthetic acceptance'); END")
    try:
        with pytest.raises(sqlite3.IntegrityError, match="synthetic acceptance"):
            h.store.begin_request(event, native_tool_plan=True)
        assert h.store.request(key) is None
    finally:
        with h.store.conn:
            h.store.conn.execute("DROP TRIGGER fail_accept")
    saved = h.store.begin_request(event, native_tool_plan=True)["request"]
    assert saved["event"][PLAN_KEY]["commands"][0]["name"] == "query_offer_status"
    replay = deepcopy(event); replay[PLAN_KEY] = {"version": 999}
    assert h.store.begin_request(replay)["request"] == saved
    second = BotStore(h.store.directory.parent)
    try:
        assert second.request(key)["event"][PLAN_KEY] == saved["event"][PLAN_KEY]
    finally:
        second.close()


def test_old_native_request_is_not_replanned_after_opt_in(harness):
    h = harness; key, worker, event = event_for(h, "查询娱乐首单")
    original = h.store.begin_request(event)["request"]
    assert PLAN_KEY not in original["event"]
    h.generator.direct_plan_enabled = True
    restarted = h.worker(); assert restarted.admit_pending() == 0
    assert h.store.request(key) == original


def make_runner(h, usage_dir, *, callback=None, provider=None, actual_mcp=False):
    calls, provider_calls, executions = [], [], []
    class Host:
        tools = [row["function"] for row in tool_schemas()]
        def __init__(self, directory, event_id):
            self.service = Service(h.store, {"event_id": event_id, "writable": True})
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        def call_sync(self, name, arguments):
            calls.append((name, deepcopy(arguments)))
            return callback(self.service, name, arguments) if callback else self.service.call(name, arguments)
    class Execution:
        def __init__(self): self.ledger = UsageLedger(usage_dir)
        def close(self): self.ledger.close()
    def local_factory(profile):
        executions.append(profile["id"])
        return Execution()
    def provider_factory(profile):
        provider_calls.append(profile["id"])
        if provider is None:
            pytest.fail("direct business constructed a model provider")
        return provider
    kwargs = {} if actual_mcp else {"host_factory": Host}
    runner = ReplyGenerator(h.store, state_dir=h.store.directory.parent, provider_factory=provider_factory,
                            local_execution_factory=local_factory, **kwargs)
    return runner, calls, provider_calls, executions


def admit_direct(h, text):
    key = h.inbox.add(str(len(h.inbox.rows) + 10), text)
    h.generator.direct_plan_enabled = True
    worker = h.worker(); worker.admit_pending()
    return key, worker, h.store.request(key)


@pytest.mark.parametrize("text,kind", [("今天娱乐首单已无，请报备", "report"),
                                       ("帮我查今天娱乐首单的状态", "query")])
def test_direct_executes_real_tool_and_zero_model_with_separate_local_accounting(harness, tmp_path, text, kind):
    h = harness; key, _, request = admit_direct(h, text)
    runner, calls, providers, executions = make_runner(h, tmp_path / "usage")
    result = asyncio.run(runner.generate(request["event"], request, []))
    assert providers == [] and len(executions) == 1 and len(calls) == 1
    assert result.evidence["usage_run_id"] is None and result.evidence["execution_run_id"].startswith("local_")
    assert result.evidence["verified_tool_final"]["kind"] == kind
    assert result.metrics["used_mcp"] is True and result.metrics["model_calls"] == 0
    assert "provider_init" not in result.metrics["timings"] and "model_call" not in result.metrics["timings"]
    assert business_day(request["occurred_at"]) in result.text
    with closing(UsageLedger(tmp_path / "usage")) as ledger:
        from chatlog_keeper.bot.deferred_usage import restore_deferred_usage
        assert ledger.list_runs() == []
        h.store.set("synthetic_frozen_" + key, result.evidence)
        restore_deferred_usage(h.store.get("synthetic_frozen_" + key)["deferred_usage"], ledger)
        rows = ledger.conn.execute("SELECT * FROM usage_runs").fetchall()
        assert len(rows) == 1 and rows[0]["kind"] == "local_business"
        wire = ledger.get(result.evidence["execution_run_id"])
        assert wire["model"] == wire["provider"] == "local-business" and wire["model_invoked"] is False
        assert wire["usage"] == {field: 0 for field in FIELDS}
        assert ledger.conn.execute("SELECT count(*) FROM usage_calls").fetchone()[0] == 0
    assert len([row for row in h.store.audit() if row["kind"] == "report"]) == int(kind == "report")


def test_multi_query_is_one_snapshot_and_renders_only_requested_subset(harness, tmp_path):
    from test_bot_query_snapshot import commit_after_first_offer_row
    h = harness
    settings = h.store.config(); settings["businesses"][0]["items"].append({"id": "other", "name": "其他首单", "aliases": ["其他"]})
    h.store.configure(settings)
    key, _, request = admit_direct(h, "查询2026-10-27娱乐首单和竞技首单")
    day = request["event"][PLAN_KEY]["day"]
    with h.store.conn:
        h.store.conn.executemany("INSERT INTO offers VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", [
            ("shop", day, item, "available", 1, "seed", 1, 1, "seed-message", "group_a", "wxid_manager", h.store.now())
            for item in ("fun", "rank", "other")])
    runner, calls, providers, _ = make_runner(h, tmp_path / "usage")
    with commit_after_first_offer_row(h.store):
        result = asyncio.run(runner.generate(request["event"], request, []))
    assert calls == [("query_offer_status", {"date": day})] and providers == []
    assert set(result.evidence["read_facts"]) == {"fun", "rank"}
    assert {row["version"] for row in result.evidence["read_facts"].values()} == {1}
    assert "其他" not in result.text
    assert h.store.offer_status("shop", day, "fun")["version"] == 2


@pytest.mark.parametrize("field,value", [("business_id", "other"), ("version", True), ("status", "made-up")])
def test_invalid_tool_facts_never_trigger_model_reinterpretation(harness, tmp_path, field, value):
    h = harness; _, _, request = admit_direct(h, "查询娱乐首单")
    def corrupt(service, name, arguments):
        output = deepcopy(service.call(name, arguments)); output["items"][0][field] = value
        return output
    runner, _, providers, _ = make_runner(h, tmp_path / "usage", callback=corrupt)
    progress = {}
    with pytest.raises(Exception, match="business_result_unverified"):
        asyncio.run(runner.generate(request["event"], request, [], progress=progress))
    assert providers == []
    with closing(UsageLedger(tmp_path / "usage")) as ledger:
        from chatlog_keeper.bot.deferred_usage import restore_deferred_usage
        assert ledger.list_runs() == []
        h.store.set("synthetic_failed_frozen", progress["_deferred_usage"])
        restore_deferred_usage(h.store.get("synthetic_failed_frozen"), ledger)
        assert ledger.list_runs()[0]["status"] == "failed"


@pytest.mark.parametrize("mutation", ["changed", "removed"])
def test_forged_in_memory_plan_and_changed_binding_fail_before_tools(harness, tmp_path, mutation):
    h = harness; key, _, request = admit_direct(h, "查询娱乐首单")
    runner, calls, providers, executions = make_runner(h, tmp_path / "usage")
    forged = deepcopy(request)
    if mutation == "removed":
        forged["event"].pop(PLAN_KEY)
    else:
        forged["event"][PLAN_KEY]["commands"][0]["arguments"]["item"] = "rank"
    with pytest.raises(ValueError, match="context_mismatch"):
        asyncio.run(runner.generate(forged["event"], forged, []))
    changed = deepcopy(h.store.binding("group_a")["business"]); changed["name"] = "changed"
    with pytest.raises(ValueError, match="direct_plan_invalid"):
        saved_native_plan(request, changed)
    assert calls == providers == executions == []


def test_direct_configuration_is_the_persisted_snapshot_not_caller_metadata(harness, tmp_path):
    h = harness; _, _, request = admit_direct(h, "查询娱乐首单")
    request["profile"]["id"] = "forged-profile"
    request["persona"]["id"] = "forged-persona"
    runner, _, providers, executions = make_runner(h, tmp_path / "usage")
    result = asyncio.run(runner.generate(request["event"], request, []))
    assert executions == ["gpt"] and providers == []
    with closing(UsageLedger(tmp_path / "usage")) as ledger:
        from chatlog_keeper.bot.deferred_usage import restore_deferred_usage
        h.store.set("synthetic_frozen_result", result.evidence)
        restore_deferred_usage(h.store.get("synthetic_frozen_result")["deferred_usage"], ledger)
        assert ledger.get(result.evidence["execution_run_id"])["profile_id"] == "gpt"


def test_response_loss_after_report_commit_uses_receipt_and_never_new_model_or_write(harness, tmp_path, monkeypatch):
    h = harness; key, worker, request = admit_direct(h, "今天娱乐首单已无，请报备")
    def lose_response(service, name, arguments):
        assert service.call(name, arguments)["recorded"]
        raise OSError("synthetic lost local tool response")
    runner, calls, providers, _ = make_runner(h, tmp_path / "usage", callback=lose_response)
    worker.generator = runner; worker.run_once(block=True)
    assert providers == [] and len(calls) == 1 and "已记录" in h.store.reply(key)["text"]
    assert h.store.receipt(key)["version"] == 1
    assert len([row for row in h.store.audit() if row["kind"] == "report"]) == 1
    frozen = deepcopy(h.store.get("protocol_frozen_" + key))
    worker.close()
    monkeypatch.setattr(h.generator, "generate", lambda *a, **k: pytest.fail("frozen reply regenerated"))
    restarted = h.worker(); restarted.run_once()
    assert h.store.get("protocol_frozen_" + key) == frozen
    assert providers == [] and len(calls) == 1


def test_partial_direct_report_failure_only_claims_committed_items(harness, tmp_path):
    h = harness; key, worker, request = admit_direct(h, "今天娱乐首单已无，竞技首单可用，请报备")
    def fail_second(service, name, arguments):
        if arguments["item"] == "rank":
            raise OSError("synthetic second tool unavailable")
        return service.call(name, arguments)
    runner, calls, providers, _ = make_runner(h, tmp_path / "usage", callback=fail_second)
    worker.generator = runner; worker.run_once(block=True)
    assert len(calls) == 2 and providers == []
    receipt = h.store.receipt(key)
    assert len(receipt["results"]) == 1 and receipt["results"][0]["item_id"] == "fun"
    text = h.store.reply(key)["text"]
    assert "娱乐首单已无" in text and "竞技首单可用" not in text and "其余项目未确认" in text
    assert len([row for row in h.store.audit() if row["kind"] == "report"]) == 1


@pytest.mark.parametrize("text", ["今天娱乐首单已无，请报备", "查询娱乐首单"])
def test_actual_memory_mcp_direct_plan_uses_no_model(harness, tmp_path, text):
    h = harness; _, _, request = admit_direct(h, text)
    runner, _, providers, _ = make_runner(h, tmp_path / "usage", actual_mcp=True)
    result = asyncio.run(runner.generate(request["event"], request, []))
    assert result.metrics["used_mcp"] and result.metrics["model_calls"] == 0
    assert len(result.evidence["tool_results"]) == 1 and providers == []


def test_ineligible_social_request_keeps_existing_model_route(harness, tmp_path):
    from test_bot_reply_generator import Provider
    h = harness; _, _, request = admit_direct(h, "在吗，接个话呀")
    assert PLAN_KEY not in request["event"]
    provider = Provider(lambda messages, instructions, tools, handler, metadata: "在呢。")
    runner, calls, providers, executions = make_runner(h, tmp_path / "usage", provider=provider)
    result = asyncio.run(runner.generate(request["event"], request, []))
    assert result.text == "在呢。" and len(providers) == 1 and executions == calls == []
    assert result.evidence["usage_run_id"] and "execution_run_id" not in result.evidence
    assert provider.closed


def test_local_accounting_cannot_contain_api_calls_or_fake_tokens(tmp_path):
    with closing(UsageLedger(tmp_path)) as ledger:
        key = ledger.start_run(kind="local_business", model="local-business", provider="local-business",
            group_ref="group", bot_request_id="request", model_invoked=False)
        with pytest.raises(ValueError, match="no_model_calls"):
            ledger.record_call(key, usage={"total_tokens": 1})
        with pytest.raises(ValueError, match="local_business_usage"):
            ledger.finish_run(key, usage={field: 1 for field in FIELDS})
        ledger.finish_run(key)
        assert ledger.get(key)["usage"] == {field: 0 for field in FIELDS}
        assert ledger.pending() == []


def test_local_resource_lease_opens_no_http_or_model_and_blocks_close_until_released(tmp_path):
    from test_bot_protocol_runtime_resources import Fakes, close_in_thread
    fake = Fakes(tmp_path)
    local = fake.resources.local_business_execution()
    assert fake.clients == [] and fake.providers == [] and len(fake.actors) == 1
    _, closed, errors = close_in_thread(fake.resources)
    assert not closed.wait(.02)
    local.close(); local.close()
    assert closed.wait(2) and not errors and fake.actors[0].close_calls == 1


def test_local_execution_audit_never_enters_cloud_inference_sync_or_starves_it(tmp_path):
    from chatlog_keeper.dashboard.sync import SyncService
    service = SyncService(tmp_path / "archive", tmp_path / "sync", {
        "server_url": "https://synthetic.invalid", "groups": ["group"], "machine_token": "synthetic"})
    with closing(UsageLedger(service.state.directory)) as ledger:
        local = ledger.start_run(kind="local_business", model="local-business", provider="local-business",
            group_ref="group", bot_request_id="request", model_invoked=False)
        ledger.finish_run(local)
        ledger.record_run(run_id="model-run", kind="summary", model="synthetic-model", usage={field: 0 for field in FIELDS})
        assert [row["run_id"] for row in ledger.pending()] == ["model-run"]
    posted = []
    service.post = lambda route, payload: posted.append((route, payload))
    try:
        service.flush_usage()
        assert [row["run_id"] for _, payload in posted for row in payload["runs"]] == ["model-run"]
        with closing(UsageLedger(service.state.directory)) as ledger:
            assert ledger.get(local)["kind"] == "local_business"
            assert ledger.conn.execute("SELECT delivered FROM usage_runs WHERE run_id=?", (local,)).fetchone()[0] == 0
    finally:
        service.close()
