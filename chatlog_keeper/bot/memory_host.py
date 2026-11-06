"""Request-scoped MCP SDK memory sessions for the native runtime.

This changes transport and process lifetime only. Each request still creates
the existing server-bound Service/SQLite store and invokes tools through the
SDK ClientSession's typed JSON-RPC messages, rather than calling Service here.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import copy
from datetime import timedelta
import json
from pathlib import Path
import time

from .providers import BUSINESS_TOOLS, ProviderError, validate_tools
from .request_policy import _require_business_capability


@asynccontextmanager
async def _memory_session(state_dir, event_id):
    # Intentionally inside the owner's measured prepare interval. Importing
    # this module or constructing a host must not hide the first SDK/server
    # import outside the first request's mcp_init observation.
    from mcp.shared.memory import create_connected_server_and_client_session
    from .mcp import create_server

    server = create_server(state_dir, event_id=event_id)
    try:
        async with create_connected_server_and_client_session(
                server, read_timeout_seconds=timedelta(seconds=10)) as session:
            yield server, session
    finally:
        # SDK helper has closed both sessions/streams and its server task group
        # before the request-owned SQLite connection can be closed.
        server.bot_store.close()


class MemoryBusinessMCPHost:
    """Lazy, one-shot event binding with one owner task and ordered tool calls.

Concurrent prepare/call callers share initialization. Cancellation of a caller
does not cancel the owner or re-execute an accepted command. Closing admission
drains already queued work once; a committed report remains in its receipt even
if its caller lost the response. A different request needs a new host instance.
"""
    def __init__(self, state_dir, event_id):
        from .service import tool_schemas
        self.state_dir = str(Path(state_dir).expanduser().absolute())
        self._event_id = str(event_id)
        self.tools = [{"name": tool["function"]["name"], "description": tool["function"].get("description", ""),
                       "parameters": tool["function"]["parameters"]} for tool in tool_schemas()]
        self.host, self.session = None, None
        self._timings, self._tool_timings = {}, []
        self._entered, self.closed = False, False

    @property
    def event_id(self):
        return self._event_id

    @property
    def timings(self):
        return dict(self._timings)

    @property
    def tool_timings(self):
        return [dict(value) for value in self._tool_timings]

    async def __aenter__(self):
        if self._entered or self.closed:
            raise ProviderError("bot_mcp_host_reuse")
        self._entered = True
        self.loop = asyncio.get_running_loop()
        self.queue, self.ready = asyncio.Queue(), self.loop.create_future()
        self.owner = None
        return self

    async def __aexit__(self, *args):
        self.closed = True
        if self.owner is not None:
            if not self.owner.done():
                self.queue.put_nowait(None)
            cancelled = False
            try:
                # Closing a cancelled controller must still let the owner
                # drain admitted work and exit its AnyIO contexts itself.
                while not self.owner.done():
                    try:
                        await asyncio.shield(self.owner)
                    except asyncio.CancelledError:
                        cancelled = True
                self.owner.result()
            finally:
                if self.ready.done() and not self.ready.cancelled():
                    self.ready.exception()
            if cancelled:
                raise asyncio.CancelledError

    async def prepare(self):
        if self.closed or not self._entered:
            raise ProviderError("bot_mcp_host_closed")
        if asyncio.get_running_loop() is not self.loop:
            raise ProviderError("bot_mcp_host_loop_mismatch")
        _require_business_capability(self.state_dir, self.event_id)
        if self.owner is None:
            self.owner = asyncio.create_task(self._serve())
        await asyncio.shield(self.ready)
        if self.closed:
            raise ProviderError("bot_mcp_host_closed")
        if self.owner.done():
            # Readiness describes initialization, not the owner's continued
            # lifetime. Never enqueue another command after SDK failure or
            # owner cancellation, even when the old ready future succeeded.
            if not self.owner.cancelled():
                error = self.owner.exception()
                if error is not None:
                    raise error
            raise ProviderError("bot_mcp_host_closed")
        return True

    async def _serve(self):
        # No SDK import, server construction or store open precedes this clock.
        started, initialized = time.monotonic(), False
        try:
            async with _memory_session(self.state_dir, self.event_id) as (server, session):
                self.host, self.session = server, session
                listed = await session.list_tools()
                actual = [{"name": tool.name, "description": tool.description or "", "parameters": tool.inputSchema}
                          for tool in listed.tools]
                validate_tools(actual)
                if {tool["name"] for tool in actual} != {tool["name"] for tool in self.tools}:
                    raise ProviderError("bot_tool_schema_changed")
                self._timings["mcp_init"] = max(0, time.monotonic() - started)
                initialized = True
                self.ready.set_result(True)
                while True:
                    job = await self.queue.get()
                    if job is None:
                        break
                    name, arguments, future = job
                    tool_started = time.monotonic()
                    try:
                        result = await session.call_tool(name, arguments)
                        if result.isError:
                            output = {"ok": False, "error": "bot_tool_rejected"}
                        elif result.structuredContent is not None:
                            output = result.structuredContent
                        else:
                            texts = [block.text for block in result.content if getattr(block, "type", None) == "text"]
                            try:
                                output = json.loads("\n".join(texts))
                            except ValueError:
                                output = {"ok": False, "error": "bot_invalid_tool_result"}
                        if not future.done():
                            future.set_result(copy.deepcopy(output))
                    except BaseException as exc:
                        if not future.done():
                            future.set_exception(exc)
                        if not isinstance(exc, Exception):
                            raise
                    finally:
                        elapsed = max(0, time.monotonic() - tool_started)
                        self._timings["mcp_tool"] = self._timings.get("mcp_tool", 0) + elapsed
                        self._tool_timings.append({"index": len(self._tool_timings) + 1, "name": name, "seconds": elapsed})
        except BaseException as exc:
            if not self.ready.done():
                self.ready.set_exception(exc)
            while not self.queue.empty():
                job = self.queue.get_nowait()
                if job is not None and not job[2].done():
                    job[2].set_exception(exc)
            raise
        finally:
            if not initialized:
                self._timings["mcp_init"] = max(0, time.monotonic() - started)

    async def call(self, name, arguments):
        if name not in BUSINESS_TOOLS:
            raise ProviderError("bot_unapproved_tool_call")
        await self.prepare()
        if self.closed:
            raise ProviderError("bot_mcp_host_closed")
        future = self.loop.create_future()
        self.queue.put_nowait((name, copy.deepcopy(arguments), future))
        return await future

    def call_sync(self, name, arguments):
        if self.closed or not self._entered:
            raise ProviderError("bot_mcp_host_closed")
        return asyncio.run_coroutine_threadsafe(self.call(name, arguments), self.loop).result(timeout=20)
