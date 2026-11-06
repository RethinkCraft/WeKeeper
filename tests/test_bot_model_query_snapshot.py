"""A model's separate tool calls cannot manufacture a mixed business snapshot."""
import asyncio
from contextlib import closing
from copy import deepcopy
import sqlite3

import pytest

from chatlog_keeper.bot.providers import ProviderError
from test_bot_reply_generator import DAY, NOW, Provider, generator, source, store
from test_bot_required_read import call
from test_bot_verified_tool_final import scripted


def seed_snapshot(store):
    with store.conn:
        store.conn.executemany("INSERT INTO offers VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", [
            ("shop", DAY, item, "available", 1, "seed", 1, 1,
             "seed-message", "group_a", "wxid_manager", NOW)
            for item in ("fun", "rank")])


def test_model_item_batch_cannot_render_a_tuple_that_never_existed(store, tmp_path):
    seed_snapshot(store)
    event = source("model-snapshot", "查询娱乐首单和竞技首单")
    request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call(item="fun"), call(item="rank", number=2)]])
    outputs = []

    def update_between_calls(service, name, arguments):
        output = service.call(name, arguments)
        outputs.append(deepcopy(output))
        if len(outputs) == 1:
            with closing(sqlite3.connect(store.path, timeout=5)) as writer:
                writer.execute("UPDATE offers SET status='unavailable',version=2 "
                               "WHERE business_id='shop' AND day=?", (DAY,))
                writer.commit()
        return output

    result = asyncio.run(generator(store, tmp_path, provider, host_callback=update_between_calls)
                         .generate(event, request, []))
    facts = result.evidence["read_facts"]
    assert set(facts) == {"fun", "rank"}
    # Both projects changed in one transaction: (available v1, unavailable v2)
    # never existed and cannot be accepted even as an ordinary model final.
    assert {(row["status"], row["version"]) for row in facts.values()} in (
        {("available", 1)}, {("unavailable", 2)})
    assert result.text in (
        f"查询结果：{DAY}，娱乐首单可用；竞技首单可用。",
        f"查询结果：{DAY}，娱乐首单已无；竞技首单已无。")
    assert len(outputs) == 2
    assert all({row["item_id"] for row in output["items"]} == {"fun", "rank"} for output in outputs)
    assert [row["arguments"] for row in result.evidence["tool_results"]] == [{}, {}]
    assert [row["requested_arguments"] for row in result.evidence["tool_results"]] == [
        {"item": "fun"}, {"item": "rank"}]
    assert store.offer_status("shop", DAY, "fun")["version"] == 2
    assert store.offer_status("shop", DAY, "rank")["version"] == 2


def test_partial_mcp_results_cannot_accumulate_into_an_ordinary_model_final(store, tmp_path):
    seed_snapshot(store)
    event = source("incomplete-snapshots", "查询娱乐首单和竞技首单")
    request = store.begin_request(event)["request"]
    provider, bodies = scripted([[call(item="fun"), call(item="rank", number=2)]])
    returned = []

    def partial_result(service, name, arguments):
        output = service.call(name, arguments)
        item = "fun" if not returned else "rank"
        output["items"] = [row for row in output["items"] if row["item_id"] == item]
        returned.append(deepcopy(output))
        return output

    with pytest.raises(ProviderError, match="required_read_not_satisfied"):
        asyncio.run(generator(store, tmp_path, provider, host_callback=partial_result)
                    .generate(event, request, []))
    assert [{row["item_id"] for row in output["items"]} for output in returned] == [{"fun"}, {"rank"}]
    # The normal provider final is reached after its own permissive coverage
    # gate, but the host still refuses to publish a union of these two results.
    assert len(bodies) == 2


@pytest.mark.parametrize("arguments,error", [
    ({"item": "missing"}, "unknown_offer_item"),
    ({"item": ["fun", "rank"]}, "unknown_offer_item"),
    ({"item": "fun", "unexpected": True}, "invalid_tool_arguments"),
    ({"item": "fun", "date": "wrong"}, "invalid_offer_date"),
    ({"item": "fun", "date": False}, "invalid_offer_date"),
])
def test_snapshot_query_preserves_service_parameter_errors(store, tmp_path, arguments, error):
    event = source("invalid-snapshot", "查询娱乐首单和竞技首单")
    request = store.begin_request(event)["request"]
    observed = []

    def complete(messages, instructions, tools, handler, metadata):
        output = handler("query_offer_status", deepcopy(arguments))
        assert output["ok"] is False and output["error"] == error
        return "没有核实。"

    def actual_call(service, name, executed):
        observed.append(deepcopy(executed))
        return service.call(name, executed)

    with pytest.raises(ProviderError, match="required_read_not_satisfied"):
        asyncio.run(generator(store, tmp_path, Provider(complete), host_callback=actual_call)
                    .generate(event, request, []))
    assert len(observed) == 1
    assert observed[0].get("date") == arguments.get("date")
    if error != "invalid_offer_date":
        assert observed[0] == arguments
    else:
        assert "item" not in observed[0]


def test_model_snapshot_reads_all_but_only_renders_requested_projects(store, tmp_path):
    settings = store.config()
    settings["businesses"][0]["items"].append({"id": "other", "name": "其他首单", "aliases": ["其他"]})
    store.configure(settings)
    seed_snapshot(store)
    event = source("subset-snapshot", "查询娱乐首单和竞技首单")
    request = store.begin_request(event)["request"]
    provider, _ = scripted([[call(arguments={"item_id": "娱乐", "date": DAY})]])
    result = asyncio.run(generator(store, tmp_path, provider).generate(event, request, []))
    assert set(result.evidence["read_facts"]) == {"fun", "rank"}
    assert "其他" not in result.text
    tool = result.evidence["tool_results"][0]
    assert tool["arguments"] == {"date": DAY}
    assert tool["requested_arguments"] == {"item_id": "娱乐", "date": DAY}
    assert {row["item_id"] for row in tool["result"]["items"]} == {"fun", "rank", "other"}


def test_snapshot_query_does_not_swallow_sql_errors(store, tmp_path):
    event = source("sql-snapshot", "查询娱乐首单和竞技首单")
    request = store.begin_request(event)["request"]
    provider, _ = scripted([[call(item="fun")]])

    def drop_before_query(service, name, arguments):
        with store.conn:
            store.conn.execute("DROP TABLE offers")
        return service.call(name, arguments)

    progress = {}
    with pytest.raises(ProviderError, match="provider_execution_failed"):
        asyncio.run(generator(store, tmp_path, provider, host_callback=drop_before_query)
                    .generate(event, request, [], progress=progress))
    assert provider.ledger.finished is None
    from chatlog_keeper.bot.deferred_usage import restore_deferred_usage
    store.set("synthetic_failed_frozen", progress["_deferred_usage"])
    restore_deferred_usage(store.get("synthetic_failed_frozen"), provider.ledger)
    assert provider.ledger.finished["status"] == "failed"


def test_snapshot_query_preserves_service_binding_authorization(store, tmp_path):
    event = source("disabled-snapshot", "查询娱乐首单和竞技首单")
    request = store.begin_request(event)["request"]
    observed = []

    def complete(messages, instructions, tools, handler, metadata):
        output = handler("query_offer_status", {"item": "fun"})
        assert output["ok"] is False and output["error"] == "bot_group_disabled"
        return "未核实。"

    def disable_before_query(service, name, arguments):
        settings = store.config()
        settings["groups"][0]["enabled"] = False
        store.configure(settings)
        output = service.call(name, arguments)
        observed.append(output)
        return output

    with pytest.raises(ProviderError, match="required_read_not_satisfied"):
        asyncio.run(generator(store, tmp_path, Provider(complete), host_callback=disable_before_query)
                    .generate(event, request, []))
    assert len(observed) == 1 and store.receipt(event["event_id"]) is None
