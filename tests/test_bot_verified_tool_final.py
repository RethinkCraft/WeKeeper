"""One fewer model round only after current, complete MCP facts are verified.

Every transport is injected; Service and its temporary SQLite transactions are
real. These tests never open a model, gateway, production ledger or UI.
"""
import asyncio
import copy
from datetime import datetime, timezone
import json

import pytest

from chatlog_keeper.bot.providers import ChatGPTProvider, ProviderError
from chatlog_keeper.dashboard.usage import UsageLedger
from test_bot_reply_generator import DAY, NOW, generator, source, store
from test_bot_required_read import TOOLS, call, compatible, gate, query_result, response
from test_bot_protocol_business import harness
from test_bot_query_snapshot import commit_after_first_offer_row


def scripted(batches, *, final="这是一段需要宿主校验的最终表述。"):
    bodies = []
    def remote(request):
        bodies.append(json.loads(request.content))
        assert "_verified_tool_final" not in request.content.decode()
        if len(bodies) <= len(batches):
            return response({"tool_calls": batches[len(bodies) - 1]})
        return response({"content": final})
    return compatible(remote), bodies


@pytest.mark.parametrize("kind,multiple", [(kind, multiple) for kind in ("query", "report") for multiple in (False, True)])
def test_complete_current_mcp_facts_need_one_model_round(store, tmp_path, kind, multiple):
    text = ("查询娱乐首单和竞技首单" if multiple else "查询娱乐首单") if kind == "query" else (
        "今天娱乐首单已无；竞技首单可用；请报备" if multiple else "今天娱乐首单已无，请报备")
    event = source("verified", text); request = store.begin_request(event)["request"]
    items = ["fun", "rank"] if multiple else ["fun"]
    batch = [call(arguments={"date": DAY} if multiple else {"item": "fun", "date": DAY})] if kind == "query" else [
        call("report_offer_status", number=index + 1, arguments={"item": item,
            "status": "unavailable" if item == "fun" else "available", "evidence": text})
        for index, item in enumerate(items)]
    provider, bodies = scripted([batch])
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == 1
    assert result.evidence["verified_tool_final"]["kind"] == kind
    assert result.metrics["used_mcp"] is True and "model_call_2" not in result.metrics["timings"]
    facts = result.evidence["read_facts" if kind == "query" else "report_facts"]
    assert set(facts) == set(items)
    assert all(row["business_id"] == "shop" and row["day"] == DAY for row in facts.values())
    assert len(result.evidence["tool_results"]) == (len(items) if kind == "report" else 1)
    assert len([row for row in store.audit() if row["kind"] == "report"]) == (len(items) if kind == "report" else 0)
    assert all(row["version"] == (1 if kind == "report" else 0) for row in facts.values())
    expected = ("查询结果：" + DAY + "，娱乐首单未确认" + ("；竞技首单未确认" if multiple else "") + "。"
                if kind == "query" else "已记录：" + DAY + "，娱乐首单已无" + ("；竞技首单可用" if multiple else "") + "。")
    assert result.text == expected
    # Generation returns accounting operations with its frozen evidence. The
    # protocol projection, after a durable final, owns the actual ledger write.
    assert provider.ledger.finished is None
    store.set("synthetic_frozen_" + request["event_id"], result.evidence)
    from chatlog_keeper.bot.deferred_usage import restore_deferred_usage
    restore_deferred_usage(store.get("synthetic_frozen_" + request["event_id"])["deferred_usage"], provider.ledger)
    assert provider.ledger.finished["status"] == "completed"


def test_named_multi_item_reads_execute_complete_snapshots(store, tmp_path):
    event = source("split", "查询娱乐首单和竞技首单"); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call(item="fun"), call(item="rank", number=2)]])
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == 1 and result.evidence["verified_tool_final"]["kind"] == "query"
    assert [row["arguments"] for row in result.evidence["tool_results"]] == [{}, {}]
    assert all({item["item_id"] for item in row["result"]["items"]} == {"fun", "rank"}
               for row in result.evidence["tool_results"])


def test_incomplete_then_complete_query_ends_only_after_complete_batch(store, tmp_path):
    event = source("partial", "查询娱乐首单和竞技首单"); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call(item="fun")], [call(number=2, arguments={"date": DAY})]])
    attempted = []
    def first_failure(service, name, arguments):
        attempted.append(arguments)
        return ({"ok": False, "error": "synthetic incomplete read"} if len(attempted) == 1
                else service.call(name, arguments))
    result = asyncio.run(generator(store, tmp_path, provider, host_callback=first_failure).generate(event, request, []))
    assert len(bodies) == 2 and result.evidence["verified_tool_final"]["kind"] == "query"
    assert len(result.evidence["tool_results"]) == 1


def test_full_query_keeps_one_sqlite_snapshot_despite_concurrent_commit(store, tmp_path):
    day = "2026-10-27"
    with store.conn:
        store.conn.executemany("INSERT INTO offers VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            [("shop", day, item, "available", 1, "seed", 1, 1,
              "seed-message", "group_a", "wxid_manager", NOW) for item in ("fun", "rank")])
    event = source("coherent", "查询2026-10-27娱乐首单和竞技首单"); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call(arguments={"date": day})]])
    with commit_after_first_offer_row(store):
        result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == 1
    assert [(row["status"], row["version"]) for row in result.evidence["read_facts"].values()] == [
        ("available", 1), ("available", 1)]
    assert store.offer_status("shop", day, "fun")["version"] == 2
    assert result.text == "查询结果：2026-10-27，娱乐首单可用；竞技首单可用。"


def test_mixed_read_and_report_retains_original_loop(store, tmp_path):
    text = "今天娱乐首单已无，请报备；查竞技首单"
    event = source("mixed", text); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call(item="rank")], [call("report_offer_status", arguments={
        "item": "fun", "status": "unavailable", "evidence": text})]])
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == 3 and "verified_tool_final" not in result.evidence
    assert result.evidence["required_read"] and result.evidence["required_report"]
    assert store.receipt(event["event_id"])["version"] == 1


def test_read_with_refused_report_is_not_mistaken_for_pure_query(store, tmp_path):
    text = "今天娱乐首单已无，请报备；查询竞技首单"
    event = source("mixed-refused", text); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call(item="rank")], [call("report_offer_status", arguments={
        "item": "fun", "status": "unavailable", "evidence": text})]])
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == 3 and "verified_tool_final" not in result.evidence
    assert result.evidence["required_read"] and result.evidence["required_report"] is None
    assert store.receipt(event["event_id"]) is None


def test_later_failed_call_in_batch_cannot_reuse_earlier_complete_query(store, tmp_path):
    event = source("batch", "查询娱乐首单"); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call(), call(number=2, arguments={"item": "missing"})]])
    with pytest.raises(ProviderError, match="required_read_not_satisfied"):
        asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == 4


@pytest.mark.parametrize("field,value", [("business_id", "other"), ("version", True),
                                        ("version", -1), ("item_id", "other")])
def test_invalid_fact_shape_never_enables_early_final(store, tmp_path, field, value):
    event = source("invalid", "查询娱乐首单"); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call()]])
    def corrupt(service, name, arguments):
        output = copy.deepcopy(service.call(name, arguments))
        output["items"][0][field] = value
        return output
    try:
        result = asyncio.run(generator(store, tmp_path, provider, host_callback=corrupt).generate(event, request, []))
    except ProviderError:
        result = None
    assert len(bodies) > 1
    assert result is None or "verified_tool_final" not in result.evidence


@pytest.mark.parametrize("text,batches", [("你好", []), ("请列出项目", [[call()]]),
    ("不要报备娱乐首单已无", [[call("report_offer_status", arguments={
        "item": "fun", "status": "unavailable", "evidence": "娱乐首单已无"})]])])
def test_chat_optional_tools_and_nonaffirmative_report_keep_original_path(store, tmp_path, text, batches):
    event = source("original", text); request = store.begin_request(event)["request"]
    provider, bodies = scripted(batches, final="在呢。")
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == len(batches) + 1
    assert "verified_tool_final" not in result.evidence
    assert store.receipt(event["event_id"]) is None


def test_entire_report_batch_executes_before_finish_and_same_item_is_idempotent(store, tmp_path):
    text = "今天娱乐首单已无，请报备"
    event = source("replay", text); request = store.begin_request(event)["request"]
    arguments = {"item": "fun", "status": "unavailable", "evidence": text}
    provider, bodies = scripted([[call("report_offer_status", arguments=arguments),
                                  call("report_offer_status", number=2, arguments=arguments)]])
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert len(bodies) == 1 and len(result.evidence["tool_results"]) == 2
    assert result.evidence["tool_results"][1]["result"]["replayed"] is True
    assert store.receipt(event["event_id"])["version"] == 1
    assert len([row for row in store.audit() if row["kind"] == "report"]) == 1


def test_partial_report_cancellation_never_claims_all_items_successful(store, tmp_path):
    text = "今天娱乐首单已无；竞技首单可用；请报备"
    event = source("cancelled", text); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call("report_offer_status", number=i, arguments={
        "item": item, "status": status, "evidence": text})
        for i, item, status in [(1, "fun", "unavailable"), (2, "rank", "available")]]])
    def cancel(service, name, arguments):
        result = service.call(name, arguments)
        store.conn.execute("UPDATE requests SET status='cancelled' WHERE event_id=?", (event["event_id"],))
        store.conn.commit()
        return result
    with pytest.raises(ProviderError, match="required_report_not_satisfied"):
        asyncio.run(generator(store, tmp_path, provider, host_callback=cancel).generate(event, request, []))
    assert len(bodies) == 4
    assert [row["item_id"] for row in store.receipt(event["event_id"])["results"]] == ["fun"]


@pytest.mark.parametrize("field,value", [("version", 999), ("sender_id", "other"), ("message_ref", "other")])
def test_report_tool_output_must_match_persisted_receipt_before_early_final(store, tmp_path, field, value):
    text = "今天娱乐首单已无，请报备"
    event = source("receipt", text); request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call("report_offer_status", arguments={
        "item": "fun", "status": "unavailable", "evidence": text})]])
    def changed(service, name, arguments):
        output = service.call(name, arguments)
        return {**output, field: value}
    result = asyncio.run(generator(store, tmp_path, provider, host_callback=changed).generate(event, request, []))
    assert len(bodies) == 2 and "verified_tool_final" not in result.evidence
    assert store.receipt(event["event_id"])["version"] == 1
    assert len([row for row in store.audit() if row["kind"] == "report"]) == 1


def test_commit_then_lost_tool_response_recovers_receipt_without_second_model_or_write(harness, tmp_path):
    h = harness; text = "今天娱乐首单已无，请报备"; key = h.inbox.add("10", text)
    h.inbox.rows[0]["message_json"]["createdAt"] = datetime.fromtimestamp(NOW, timezone.utc).isoformat()
    provider, bodies = scripted([[call("report_offer_status", arguments={
        "item": "fun", "status": "unavailable", "evidence": text})]])
    def lost(service, name, arguments):
        assert service.call(name, arguments)["recorded"] is True
        raise TimeoutError("synthetic lost MCP response")
    worker = h.worker()
    worker.generator = generator(h.store, tmp_path, provider, host_callback=lost)
    worker.run_once(block=True)
    assert len(bodies) == 1
    frozen = h.store.get("protocol_frozen_" + key)
    assert frozen["evidence"]["recovery"] == "committed_report"
    assert "已记录" in frozen["text"]
    worker.run_once(block=True)
    assert len(bodies) == 1 and h.store.receipt(key)["version"] == 1
    assert len([row for row in h.store.audit() if row["kind"] == "report"]) == 1


def test_early_final_records_only_actual_model_call_and_finishes_usage(tmp_path):
    provider, bodies = scripted([[call()]])
    provider.ledger = UsageLedger(tmp_path); provider.own_ledger = True
    observed = []
    def handler(*_):
        observed.append(provider.ledger.conn.execute("SELECT COUNT(*) FROM usage_calls").fetchone()[0])
        return query_result()
    try:
        result = provider.complete([], "", TOOLS, handler, {**gate(), "run_id": "synthetic",
            "group_ref": "synthetic-group", "bot_request_id": "synthetic-request",
            "_verified_tool_final": lambda: "verified"})
        assert result == "verified" and len(bodies) == 1 and observed == [1]
        assert provider.ledger.conn.execute("SELECT COUNT(*) FROM usage_calls").fetchone()[0] == 1
        assert provider.ledger.get("synthetic")["status"] == "completed"
    finally:
        provider.close()


def test_siwc_consumes_callback_without_changing_its_existing_loop(tmp_path):
    class Client:
        state_dir = tmp_path
        def bot_chat(self, *args, **kwargs):
            assert not any(key.startswith("_") for key in kwargs["run_metadata"])
            return {"output_text": "原路径最终正文"}
    provider = ChatGPTProvider(tmp_path, {"kind": "siwc"}, client=Client())
    assert provider.complete([], "", TOOLS, lambda *_: None,
        {"_verified_tool_final": lambda: pytest.fail("SIWC must keep its existing loop")}) == "原路径最终正文"


def test_verified_completion_cannot_extend_existing_provider_deadline():
    now = [0]
    provider = compatible(lambda _: response({"tool_calls": [call()]}), clock=lambda: now[0])
    def final():
        now[0] = 61
        return "too late"
    try:
        with pytest.raises(ProviderError, match="bot_provider_timeout"):
            provider.complete([], "", TOOLS, lambda *_: query_result(),
                              {**gate(), "_verified_tool_final": final})
        assert provider.ledger.finished["status"] == "failed"
    finally:
        provider.close()
