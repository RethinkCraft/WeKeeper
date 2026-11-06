"""Native-only memory MCP lifecycle, real SDK authority and durable recovery."""
import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from chatlog_keeper.bot import memory_host as module
from chatlog_keeper.bot.memory_host import MemoryBusinessMCPHost
from chatlog_keeper.bot.providers import ProviderError
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config
from test_bot_query_snapshot import DAY, commit_after_first_offer_row


@pytest.fixture
def store(tmp_path):
    value = BotStore(tmp_path)
    value.configure(config())
    yield value
    value.close()


def admit(store, key, text, sender="wxid_manager"):
    return store.begin_request({"event_id": key, "message_ref": key, "group_ref": "group_a",
        "sender_id": sender, "text": text, "occurred_at": datetime.now(timezone.utc).isoformat()})["request"]


@pytest.fixture
def fake(tmp_path, monkeypatch):
    value = SimpleNamespace(entered=[], exited=[], calls=[], now=0, init_error=None,
        call_error=False, release=None, call_release=None, tools=None)
    host = MemoryBusinessMCPHost(tmp_path, "fixed-event")
    value.tools = host.tools
    class Session:
        async def list_tools(self):
            return SimpleNamespace(tools=[SimpleNamespace(name=t['name'], description=t['description'], inputSchema=t['parameters']) for t in value.tools])
        async def call_tool(self, name, arguments):
            value.calls.append((asyncio.current_task(), name, deepcopy(arguments)))
            if value.call_release is not None:
                value.call_entered.set()
                await value.call_release.wait()
            value.now += 3
            if value.call_error:
                value.call_error = False
                raise RuntimeError("injected tool failure")
            return SimpleNamespace(isError=False, structuredContent={"ok": True}, content=[])
    @asynccontextmanager
    async def connection(directory, event_id):
        assert directory == str(tmp_path.absolute()) and event_id == "fixed-event"
        value.entered.append(asyncio.current_task())
        try:
            if value.release is not None: await value.release.wait()
            value.now += 2
            if value.init_error: raise value.init_error
            yield object(), Session()
        finally:
            value.exited.append(asyncio.current_task())
    monkeypatch.setattr(module, "_memory_session", connection)
    # The fake session has no accepted request DB; real-policy guards have
    # separate tests through both SDK transports.
    monkeypatch.setattr(module, "_require_business_capability", lambda *_: None)
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: value.now))
    return host, value


def test_unused_host_starts_no_server_or_sdk(fake):
    host, value = fake
    async def run():
        async with host: assert host.timings == {} and host.tool_timings == []
    asyncio.run(run())
    assert value.entered == value.exited == value.calls == []


def test_concurrent_prepare_calls_initialize_once_and_execute_in_owner_order(fake):
    host, value = fake
    async def run():
        value.release = asyncio.Event()
        async with host:
            tasks = [asyncio.create_task(host.prepare()) for _ in range(2)]
            tasks += [asyncio.create_task(host.call("query_offer_status", {"item": str(i)})) for i in range(2)]
            await asyncio.sleep(0); await asyncio.sleep(0)
            assert len(value.entered) == 1 and not value.calls
            value.release.set()
            assert await asyncio.gather(*tasks) == [True, True, {"ok": True}, {"ok": True}]
            assert host.timings == {"mcp_init": 2, "mcp_tool": 6}
            assert [row["index"] for row in host.tool_timings] == [1, 2]
            snapshot = host.timings; snapshot["mcp_init"] = 999
            assert host.timings["mcp_init"] == 2
    asyncio.run(run())
    assert value.entered == value.exited
    assert all(row[0] is value.entered[0] for row in value.calls)
    assert [row[2]["item"] for row in value.calls] == ["0", "1"]


def test_cancelled_prepare_cannot_cancel_shared_ready(fake):
    host, value = fake
    async def run():
        value.release = asyncio.Event()
        async with host:
            first = asyncio.create_task(host.prepare()); second = asyncio.create_task(host.prepare())
            await asyncio.sleep(0); await asyncio.sleep(0)
            first.cancel()
            with pytest.raises(asyncio.CancelledError): await first
            assert not host.ready.cancelled() and not host.owner.cancelled()
            value.release.set()
            assert await second is True
    asyncio.run(run())
    assert len(value.entered) == len(value.exited) == 1


def test_close_during_prepare_rejects_unadmitted_tool(fake):
    host, value = fake
    async def run():
        value.release = asyncio.Event()
        await host.__aenter__()
        pending = asyncio.create_task(host.call("query_offer_status", {}))
        await asyncio.sleep(0); await asyncio.sleep(0)
        closing = asyncio.create_task(host.__aexit__(None, None, None))
        await asyncio.sleep(0); value.release.set()
        with pytest.raises(ProviderError, match="host_closed"): await pending
        await closing
    asyncio.run(run())
    assert not value.calls and value.entered == value.exited


@pytest.mark.parametrize("fault", ["init", "schema"])
def test_failed_initialization_closes_owner_and_fails_every_waiter(fake, fault):
    host, value = fake
    if fault == "init": value.init_error = RuntimeError("injected startup")
    else: value.tools = value.tools[:-1]
    async def run():
        await host.__aenter__()
        results = await asyncio.gather(host.prepare(), host.call("query_offer_status", {}), return_exceptions=True)
        assert all(isinstance(r, RuntimeError if fault == "init" else ProviderError) for r in results)
        with pytest.raises(RuntimeError if fault == "init" else ProviderError):
            await host.__aexit__(None, None, None)
        assert host.ready.done() and host.owner.done()
    asyncio.run(run())
    assert len(value.entered) == len(value.exited) == 1 and not value.calls


def test_cancelled_caller_and_closer_drain_admitted_tool_once(fake):
    host, value = fake
    async def run():
        value.call_release, value.call_entered = asyncio.Event(), asyncio.Event()
        await host.__aenter__()
        caller = asyncio.create_task(host.call("query_offer_status", {}))
        await value.call_entered.wait(); caller.cancel()
        with pytest.raises(asyncio.CancelledError): await caller
        closing = asyncio.create_task(host.__aexit__(None, None, None))
        await asyncio.sleep(0); closing.cancel(); await asyncio.sleep(0)
        assert not closing.done() and not host.owner.cancelled()
        value.call_release.set()
        with pytest.raises(asyncio.CancelledError): await closing
        assert host.owner.done()
    asyncio.run(run())
    assert len(value.calls) == 1 and value.entered == value.exited


def test_tool_error_does_not_restart_or_reexecute_owner(fake):
    host, value = fake
    async def run():
        async with host:
            value.call_error = True
            with pytest.raises(RuntimeError, match="tool failure"):
                await host.call("query_offer_status", {})
            assert (await host.call("query_offer_status", {}))["ok"]
    asyncio.run(run())
    assert len(value.entered) == 1 and len(value.calls) == 2


def test_queued_arguments_are_copied_and_closed_reentry_is_rejected(fake):
    host, value = fake
    async def run():
        value.call_release, value.call_entered = asyncio.Event(), asyncio.Event()
        async with host:
            first = asyncio.create_task(host.call("query_offer_status", {"item": "first"}))
            await value.call_entered.wait()
            args = {"item": "second"}
            second = asyncio.create_task(host.call("query_offer_status", args))
            await asyncio.sleep(0); args["item"] = "mutated"
            value.call_release.set(); await asyncio.gather(first, second)
        with pytest.raises(ProviderError, match="reuse"): await host.__aenter__()
        with pytest.raises(ProviderError, match="closed"): await host.call("query_offer_status", {})
        with pytest.raises(ProviderError, match="closed"): host.call_sync("query_offer_status", {})
    asyncio.run(run())
    assert value.calls[1][2] == {"item": "second"}


def real_host(store, request):
    return MemoryBusinessMCPHost(store.directory.parent, request["event_id"])


def test_real_sdk_schemas_and_fixed_report_query_match_stdio(store):
    report = admit(store, "report", "今天娱乐首单已无，请报备")
    query = admit(store, "query", "查询全部项目状态")
    async def run():
        async with real_host(store, report) as host:
            await host.prepare()
            actual = await host.session.list_tools()
            assert host.host.bot_store.conn.execute("PRAGMA synchronous").fetchone()[0] == 2
            output = await asyncio.to_thread(host.call_sync, "report_offer_status", {
                "item": "fun", "status": "unavailable", "evidence": report["text"]})
            assert output["recorded"] and output["event_id"] == report["event_id"]
        with pytest.raises(sqlite3.ProgrammingError): host.host.bot_store.conn.execute("SELECT 1")
        from chatlog_keeper.bot.mcp import create_server
        external = create_server(store.directory.parent, group_ref="group_a")
        try:
            expected_tools = [{"name": t.name, "description": t.description or "", "parameters": t.inputSchema}
                              for t in await external.list_tools()]
            expected_result = external.bot_service.call("query_offer_status", {})
        finally:
            external.bot_store.close()
        async with real_host(store, query) as memory:
            await memory.prepare()
            memory_tools = [{"name": t.name, "description": t.description or "", "parameters": t.inputSchema} for t in actual.tools]
            assert [t for t in memory_tools if t["name"] != "report_offer_status"] == expected_tools
            assert (await memory.call("query_offer_status", {})) == expected_result
    asyncio.run(run())
    assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 1


def test_real_sdk_parallel_authority_and_external_snapshot(store):
    with store.conn:
        store.conn.executemany("INSERT INTO offers VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", [
            ("shop", DAY, item, "available", 1, "seed", 1, 1, "seed", "group_a", "wxid_manager", DAY)
            for item in ("fun", "rank")])
    query = admit(store, "snapshot", "查询全部项目状态")
    visitor = admit(store, "visitor", "今天娱乐首单已无，请报备", "visitor")
    async def run():
        async with real_host(store, query) as reader:
            async with real_host(store, visitor) as writer:
                await asyncio.gather(reader.prepare(), writer.prepare())
                assert reader.host is not writer.host and reader.session is not writer.session
                with commit_after_first_offer_row(reader.host.bot_store):
                    output = await reader.call("query_offer_status", {"date": DAY})
                assert [(r["status"], r["version"]) for r in output["items"]] == [("available", 1), ("available", 1)]
                latest, bad = await asyncio.gather(reader.call("query_offer_status", {"date": DAY}),
                    writer.call("report_offer_status", {"item": "fun", "status": "unavailable", "evidence": visitor["text"], "event_id": query["event_id"], "sender_id": "wxid_manager"}))
                assert all(row["version"] == 2 for row in latest["items"])
                assert bad == {"ok": False, "error": "report_not_authorized"}
    asyncio.run(run())
    assert store.receipt(visitor["event_id"]) is None


def test_lost_sync_response_cannot_repeat_write_or_block_another_worker_loop(store, monkeypatch):
    report = admit(store, "lost-report", "今天娱乐首单已无，请报备")
    query = admit(store, "independent-query", "查询全部项目状态")
    reader_ready, entered, release, stop = [threading.Event() for _ in range(4)]
    holder, writes = {}, []
    original_submit = asyncio.run_coroutine_threadsafe
    submitted = []
    def short_wait(coro, loop):
        future = original_submit(coro, loop); submitted.append(future)
        class Waiter:
            def result(self, timeout):
                assert timeout == 20
                return future.result(timeout=.02)
        return Waiter()
    monkeypatch.setattr(module.asyncio, "run_coroutine_threadsafe", short_wait)
    def writer_loop():
        async def run():
            async with real_host(store, report) as host:
                await host.prepare()
                original = host.host.bot_service.call
                def blocked(name, args):
                    writes.append(name); entered.set()
                    assert release.wait(3), "controller did not release blocked Service"
                    return original(name, args)
                host.host.bot_service.call = blocked
                holder["host"] = host
                holder["loop"] = asyncio.get_running_loop()
                reader_ready.set()
                await asyncio.to_thread(stop.wait, 4)
            return host
        return asyncio.run(run())
    def query_loop():
        async def run():
            async with real_host(store, query) as host:
                return await host.call("query_offer_status", {})
        return asyncio.run(run())
    with ThreadPoolExecutor(3) as pool:
        writing = pool.submit(writer_loop)
        try:
            assert reader_ready.wait(3)
            calling = pool.submit(holder["host"].call_sync, "report_offer_status", {
                "item": "fun", "status": "unavailable", "evidence": report["text"]})
            assert entered.wait(3)
            with pytest.raises(FutureTimeout): calling.result(timeout=1)
            assert not submitted[0].done()
            # Different Worker-style asyncio.run/thread and a fresh server can
            # finish while the first request's synchronous tool is blocked.
            assert pool.submit(query_loop).result(timeout=2)["ok"]
            release.set()
            assert submitted[0].result(timeout=3)["recorded"] is True
        finally:
            release.set(); stop.set()
        closed = writing.result(timeout=3)
    assert closed.owner.done() and writes == ["report_offer_status"]
    assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 1
    async def recover():
        async with real_host(store, report) as host:
            result = await host.call("report_offer_status", {"item": "fun", "status": "unavailable", "evidence": report["text"]})
            assert result["replayed"] is True and result["version"] == 1
    asyncio.run(recover())
    assert store.conn.execute("SELECT count(*) FROM reports").fetchone()[0] == 1


def test_first_sdk_import_is_in_prepare_timer_and_constructor_opens_nothing(store, monkeypatch):
    import builtins
    request = admit(store, "cold", "查询娱乐首单")
    host = real_host(store, request)
    original = builtins.__import__
    observed = []
    def instrument(name, *args, **kwargs):
        if name == "mcp.shared.memory":
            observed.append(host.owner)
            assert host._timings == {} and host.owner is asyncio.current_task()
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", instrument)
    async def run():
        async with host:
            assert host.host is None and host.owner is None
            await host.prepare()
            assert host.timings["mcp_init"] > 0
    asyncio.run(run())
    assert len(observed) == 1


def test_dead_initialized_owner_refuses_a_new_command_immediately(store):
    request = admit(store, "dead-owner", "查询娱乐首单")
    host = real_host(store, request)
    async def run():
        await host.__aenter__()
        try:
            await host.prepare(); host.owner.cancel()
            with pytest.raises(asyncio.CancelledError): await host.owner
            with pytest.raises(ProviderError, match="host_closed"):
                await asyncio.wait_for(host.call("query_offer_status", {}), .05)
            assert host.queue.empty()
        finally:
            with pytest.raises(asyncio.CancelledError): await host.__aexit__(None, None, None)
    asyncio.run(run())


@pytest.mark.parametrize("stage", ["constructor", "registration"])
def test_server_factory_failure_closes_store_even_with_retained_traceback(store, monkeypatch, stage):
    import mcp.server.fastmcp
    from chatlog_keeper.bot import mcp as server_module
    from chatlog_keeper.bot.store import BotStore
    request = admit(store, "factory-error", "查询娱乐首单")
    captured, retained = [], []
    failure = RuntimeError("injected server factory failure")
    def opening(*args, **kwargs):
        value = BotStore(*args, **kwargs); captured.append(value); return value
    def broken(*args, **kwargs): raise failure
    monkeypatch.setattr(server_module, "BotStore", opening)
    if stage == "constructor":
        monkeypatch.setattr(mcp.server.fastmcp, "FastMCP", broken)
    else:
        monkeypatch.setattr(mcp.server.fastmcp.FastMCP, "tool", lambda *args, **kwargs: broken)
    host = real_host(store, request)
    async def run():
        await host.__aenter__()
        with pytest.raises(RuntimeError) as error: await host.prepare()
        assert error.value is failure
        retained.append(error.value.__traceback__)
        with pytest.raises(RuntimeError) as error: await host.__aexit__(None, None, None)
        assert error.value is failure
    asyncio.run(run())
    assert retained and captured and host.owner.done()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        captured[0].conn.execute("SELECT 1")


def test_native_runtime_wires_memory_but_other_default_stays_stdio(monkeypatch, tmp_path):
    import inspect
    import chatlog_keeper.bot.protocol_runtime as runtime
    from chatlog_keeper.bot.reply_generator import ReplyGenerator
    assert inspect.signature(ReplyGenerator).parameters["host_factory"].default is MemoryBusinessMCPHost
    # Exercise the existing real open_runtime wiring test with its lightweight
    # components; also inspect the actual constructor argument, not source text.
    import test_bot_protocol_runtime as fixture
    original = fixture.test_open_runtime_wires_three_gateways_same_journal_history_and_safe_close_order
    from test_bot_protocol_runtime import local_config
    config = local_config.__wrapped__(tmp_path)
    import chatlog_keeper.bot.memory_host as imported
    marker = object()
    monkeypatch.setattr(imported, "MemoryBusinessMCPHost", marker)
    # Existing helper's Generator accepts arbitrary kwargs. A factory wrapper
    # observes the argument while its normal gateway/close assertions still run.
    seen = []
    real_open = runtime.open_runtime
    def observed_open(value):
        import chatlog_keeper.bot.reply_generator as generators
        factory = generators.ReplyGenerator
        def observed(*args, **kwargs):
            seen.append(kwargs.get("host_factory"))
            return factory(*args, **kwargs)
        monkeypatch.setattr(generators, "ReplyGenerator", observed)
        return real_open(value)
    monkeypatch.setattr(runtime, "open_runtime", observed_open)
    original(config, monkeypatch, False)
    assert seen == [marker]
