"""Real SDK client/server stdio round trip; all data is synthetic."""
import asyncio
import base64
from datetime import timedelta
import os
import sys

import pytest

pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


def test_stdio_tools_structured_data_and_image_content(tmp_path):
    script = r'''
import base64
from chatlog_keeper.mcp_server import create_server
class Store:
    def __init__(self, state, *, readonly):
        assert readonly
    def close(self): pass
    def get_status(self): return {"status":"ready","source":"synthetic"}
    def read_messages(self, **kwargs):
        return {"messages":[{"message_ref":"msg_synthetic","text":"stdio fixture"}],
                "coverage":{"complete":True},"next_cursor":None}
class Media:
    def __init__(self, store): pass
    def get_media(self, reference, quality="best"):
        return {"metadata":{"width":1,"height":1,"tier":"full"},"mime_type":"image/png",
                "data":base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")}
create_server("/tmp/never-read-private-data",store_factory=Store,media_factory=Media).run(transport="stdio")
'''

    async def exercise():
        parameters = StdioServerParameters(command=sys.executable, args=["-c", script], env=os.environ.copy())
        with (tmp_path / "mcp-stderr.log").open("w") as errors:
            async with stdio_client(parameters, errlog=errors) as (reader, writer):
                async with ClientSession(reader, writer, read_timeout_seconds=timedelta(seconds=15)) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    assert len(tools.tools) == 15
                    assert sum(tool.annotations.readOnlyHint for tool in tools.tools) == 14
                    request = next(tool for tool in tools.tools if tool.name == "request_voice_transcription")
                    assert request.annotations.readOnlyHint is False
                    assert request.annotations.openWorldHint is True
                    assert not any("send" in tool.name for tool in tools.tools)
                    status = await session.call_tool("get_status", {})
                    assert not status.isError and status.structuredContent["source"] == "synthetic"
                    page = await session.call_tool("read_messages", {
                        "group_ids": ["synthetic@chatroom"],
                        "since": "2026-10-24T00:00:00+08:00", "until": "2026-10-25T00:00:00+08:00",
                    })
                    assert page.structuredContent["messages"][0]["text"] == "stdio fixture"
                    image = await session.call_tool("get_media", {"reference": "media_synthetic"})
                    block = next(c for c in image.content if c.type == "image")
                    assert block.mimeType == "image/png" and base64.b64decode(block.data).startswith(b"\x89PNG")
                    assert image.structuredContent["width"] == 1
                    rejected = await session.call_tool("read_messages", {"group_ids": [], "since": "a", "until": "b"})
                    assert rejected.isError is True

    asyncio.run(exercise())
