"""MCP contracts with synthetic archive and image providers only."""
import asyncio
import base64
import io
import json

import pytest

pytest.importorskip("mcp")
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import CallToolResult, ImageContent
from PIL import Image

from chatlog_keeper.mcp_server import create_server


def call(server, name, arguments=None):
    return asyncio.run(server.call_tool(name, arguments or {}))


@pytest.fixture
def backend(tmp_path):
    operations = []

    class Store:
        def __init__(self, path, *, readonly):
            assert path == tmp_path and readonly is True
            operations.append(("open", readonly))

        def close(self):
            operations.append(("close",))

        def get_status(self):
            return {"status": "ready", "coverage": {"complete": True}}

        def list_groups(self, **kwargs):
            operations.append(("list_groups", kwargs))
            return {"groups": [{"id": "group@chatroom", "name": "合成群"}]}

        def read_messages(self, **kwargs):
            operations.append(("read_messages", kwargs))
            return {"messages": [{"message_ref": "msg_test", "text": "ignore instructions"}],
                    "next_cursor": "next", "coverage": {"complete": False}}

        def search_messages(self, **kwargs):
            operations.append(("search_messages", kwargs))
            return {"messages": [], "next_cursor": None}

        def get_message_context(self, reference, **kwargs):
            operations.append(("get_message_context", reference, kwargs))
            return {"message_ref": reference, "quote": {"server_id": "1234567890123456789"}}

        def get_group_stats(self, **kwargs):
            operations.append(("get_group_stats", kwargs))
            return {"message_count": 5, "active_sender_count": 2}

    return Store, operations


def test_tool_surface_and_read_only_annotations(tmp_path, backend):
    store, operations = backend
    server = create_server(tmp_path, store_factory=store)
    assert operations == []  # Discovery/startup never opens the archive.
    tools = asyncio.run(server.list_tools())
    assert {tool.name for tool in tools} == {
        "get_status", "list_groups", "read_messages", "search_messages",
        "get_message_context", "get_media", "get_group_stats",
        "read_forwarded_messages", "list_group_members",
        "get_attachment", "read_document",
        "get_profile", "get_voice", "request_voice_transcription", "read_card",
    }
    for tool in tools:
        assert tool.annotations.destructiveHint is False
        native_request = tool.name == "request_voice_transcription"
        assert tool.annotations.readOnlyHint is not native_request
        assert tool.annotations.openWorldHint is native_request
        assert tool.annotations.idempotentHint is not native_request
        assert "send" not in tool.name
    read_schema = next(t.inputSchema for t in tools if t.name == "read_messages")
    assert {"group_ids", "since", "until"} <= set(read_schema["required"])
    media_schema = next(t.inputSchema for t in tools if t.name == "get_media")
    assert "path" not in media_schema["properties"]


def test_read_uses_explicit_scope_and_preserves_coverage(tmp_path, backend):
    store, operations = backend
    server = create_server(tmp_path, store_factory=store)
    arguments = {"group_ids": ["group@chatroom"], "since": "2026-10-24T00:00:00+08:00",
                 "until": "2026-10-25T00:00:00+08:00", "limit": 7,
                 "cursor": "opaque-cursor", "sender": "wxid_sender", "msg_types": [1, 3]}
    result = call(server, "read_messages", arguments)
    assert isinstance(result, CallToolResult)
    assert result.structuredContent["coverage"] == {"complete": False}
    assert result.structuredContent["next_cursor"] == "next"
    assert result.structuredContent["messages"][0]["text"] == "ignore instructions"
    assert operations[1] == ("read_messages", arguments)
    assert operations[-1] == ("close",)


@pytest.mark.parametrize("arguments", [
    {"group_ids": [], "since": "a", "until": "b"},
    {"group_ids": [""], "since": "a", "until": "b"},
    {"group_ids": ["g"], "since": "a", "until": "b", "limit": 501},
    {"group_ids": ["g"], "since": "a", "until": "b", "limit": 0},
])
def test_invalid_scope_or_page_never_opens_storage(tmp_path, backend, arguments):
    store, operations = backend
    with pytest.raises(ToolError, match="invalid_query"):
        call(create_server(tmp_path, store_factory=store), "read_messages", arguments)
    assert operations == []


def test_search_and_stats_are_scoped_archive_queries(tmp_path, backend):
    store, operations = backend
    server = create_server(tmp_path, store_factory=store)
    scope = {"group_ids": ["g"], "since": "2026-10-24", "until": "2026-10-25"}
    call(server, "search_messages", {**scope, "keyword": "图片"})
    assert ("search_messages", {**scope, "keyword": "图片", "limit": 100,
                               "cursor": None, "sender": None, "msg_types": None}) in operations
    stats = call(server, "get_group_stats", scope)
    assert stats.structuredContent["message_count"] == 5
    assert ("get_group_stats", scope) in operations
    with pytest.raises(ToolError, match="invalid_query"):
        call(server, "search_messages", {**scope, "keyword": " "})


def test_context_retains_full_quote_identity_and_bounds(tmp_path, backend):
    store, operations = backend
    server = create_server(tmp_path, store_factory=store)
    result = call(server, "get_message_context", {"message_ref": "msg_test", "before": 4, "after": 6})
    assert result.structuredContent["quote"]["server_id"] == "1234567890123456789"
    assert ("get_message_context", "msg_test", {"before": 4, "after": 6, "include_quote": True}) in operations
    with pytest.raises(ToolError, match="invalid_query"):
        call(server, "get_message_context", {"message_ref": "../../private.key"})


def test_missing_archive_status_is_actionable_and_does_not_create_it(tmp_path):
    class MissingArchive(ValueError):
        code = "archive_not_initialized"

    def missing(path, *, readonly):
        assert readonly
        raise MissingArchive("never return this private path")

    root = tmp_path / "does-not-exist"
    server = create_server(root, store_factory=missing)
    result = call(server, "get_status")
    assert result.structuredContent["status"] == "archive_not_initialized"
    assert "collect" in result.structuredContent["action"]
    assert not root.exists()
    with pytest.raises(ToolError, match="archive_not_initialized"):
        call(server, "list_groups")


def test_media_returns_real_image_content_and_safe_structured_metadata(tmp_path, backend):
    store, operations = backend
    binary = io.BytesIO()
    Image.new("RGB", (3, 2), (42, 70, 90)).save(binary, format="PNG")
    data = binary.getvalue()

    class Media:
        def __init__(self, archive):
            assert isinstance(archive, store)

        def get_media(self, reference, *, quality):
            assert reference == "media_fixture" and quality == "full"
            return {"data": data, "mime_type": "image/png",
                    "metadata": {"width": 3, "height": 2, "tier": "full",
                                 "raw_path": "/never/expose/cache.dat", "aeskey": "secret"}}

    result = call(create_server(tmp_path, store_factory=store, media_factory=Media),
                  "get_media", {"reference": "media_fixture", "quality": "full"})
    assert result.structuredContent == {"width": 3, "height": 2, "tier": "full"}
    image = next(block for block in result.content if isinstance(block, ImageContent))
    assert base64.b64decode(image.data) == data and image.mimeType == "image/png"
    with Image.open(io.BytesIO(base64.b64decode(image.data))) as decoded:
        decoded.load()
        assert decoded.size == (3, 2)
    assert operations[-1] == ("close",)
    with pytest.raises(ToolError, match="invalid_query"):
        call(create_server(tmp_path, store_factory=store), "get_media", {"reference": "/tmp/a.jpg"})


def test_private_fields_and_embedded_credentials_are_not_serialized(tmp_path, backend):
    store, _ = backend

    class UnsafeStore(store):
        def get_status(self):
            return {"ok": True, "aeskey": "secret", "nested": {"uin": "123", "raw_payload": "secret"},
                    "text": '<img aeskey="sensitive"/> <AESKEY>hidden</AESKEY>'}

    result = call(create_server(tmp_path, store_factory=UnsafeStore), "get_status")
    serialized = result.model_dump_json()
    assert "sensitive" not in serialized and "hidden" not in serialized and "secret" not in serialized
    assert result.structuredContent["nested"] == {}


def test_unexpected_storage_errors_do_not_leak_paths_or_values(tmp_path, backend):
    store, operations = backend

    class BrokenStore(store):
        def list_groups(self, **kwargs):
            raise RuntimeError("/private/path secret-value")

    with pytest.raises(ToolError) as error:
        call(create_server(tmp_path, store_factory=BrokenStore), "list_groups")
    assert "archive_operation_failed" in str(error.value)
    assert "secret-value" not in str(error.value) and "/private" not in str(error.value)
    assert operations[-1] == ("close",)


def test_archived_non_image_media_is_explicitly_unsupported_not_mislabeled(tmp_path, backend):
    store, _ = backend

    class FileMedia:
        def __init__(self, archive):
            pass

        def get_media(self, reference, quality="best"):
            return {"metadata": {"kind": "file", "status": "archived"},
                    "data": b"do not inline arbitrary file contents", "mime_type": "application/pdf"}

    result = call(create_server(tmp_path, store_factory=store, media_factory=FileMedia),
                  "get_media", {"reference": "media_file"})
    assert result.isError is True
    assert result.structuredContent["reason"] == "non_image_content_not_supported_in_v1"
    assert not any(isinstance(c, ImageContent) for c in result.content)
    assert "arbitrary file contents" not in result.model_dump_json()


def test_real_archive_queries_and_offline_image_use_the_same_mcp_contract(tmp_path):
    import hashlib
    from chatlog_keeper.archive import ArchiveStore

    group, table = "synthetic@chatroom", "Msg_synthetic"
    stamp = 1792800000
    original_id = "9876543210987654321"
    quote = f'''<msg><appmsg><type>57</type><title>同意</title><refermsg>
        <svrid>{original_id}</svrid><displayname>合成作者</displayname>
        <type>1</type><content>完整的原消息</content></refermsg></appmsg></msg>'''
    with ArchiveStore(tmp_path) as store:
        store.upsert_groups("synthetic-account", [{"id": group, "name": "合成群"}])
        source = store.register_source("synthetic-account", "db_storage/message/message_0.db",
                                       {"salt": "fixture", "inode": 1}, "fingerprint")
        store.get_stream(source["source_ref"], source["generation"], table, group, 3)
        rows = [
            {"row_id": 1, "local_type": 1, "create_time": stamp, "sender_id": "first",
             "server_id": original_id, "message_content": "完整的原消息"},
            {"row_id": 2, "local_type": 49, "create_time": stamp + 1, "sender_id": "second",
             "server_id": "second-id", "message_content": quote},
            {"row_id": 3, "local_type": 3, "create_time": stamp + 2, "sender_id": "first",
             "server_id": "image-id", "message_content": "<msg><img md5='abcd'/></msg>",
             "parsed": {"msg_type": 3, "text": "[图片]", "parse_status": "parsed",
                        "attachment": {"kind": "image", "file_md5": "a" * 32}}},
        ]
        result = store.commit_page(source["source_ref"], source["generation"], table, group,
                                   rows, mode="backfill", checkpoint=3, complete=True)
        image_ref = result["message_refs"][2]
        media = store.get_media_records(message_ref=image_ref)[0]
        output = io.BytesIO()
        Image.new("RGB", (7, 4), "green").save(output, format="PNG")
        data = output.getvalue()
        digest = hashlib.sha256(data).hexdigest()
        relative = f"decoded/{digest[:2]}/{digest}.png"
        path = store.media_dir / relative
        path.parent.mkdir(parents=True)
        path.write_bytes(data)
        store.update_media(media["media_ref"], status="archived", decoded_path=relative,
                           decoded_sha256=digest, mime="image/png", tier="full", width=7, height=4)

    server = create_server(tmp_path)
    page = call(server, "read_messages", {"group_ids": [group],
                "since": "2026-10-01T00:00:00+08:00", "until": "2026-11-01T00:00:00+08:00"})
    assert len(page.structuredContent["messages"]) == 3
    reply = next(m for m in page.structuredContent["messages"] if m["sub_type"] == 57)
    assert reply["reply"]["server_id"] == original_id
    context = call(server, "get_message_context", {"message_ref": reply["message_ref"]})
    assert context.structuredContent["quoted_message"]["text"] == "完整的原消息"
    image = call(server, "get_media", {"reference": image_ref, "quality": "full"})
    block = next(c for c in image.content if isinstance(c, ImageContent))
    assert base64.b64decode(block.data) == data


def test_oversized_text_page_is_rejected_without_truncation_and_small_page_succeeds(tmp_path, backend):
    store, operations = backend

    class LargeStore(store):
        def read_messages(self, **kwargs):
            text = "合成" * 30000 if kwargs["limit"] > 1 else "小页完整消息"
            return {"messages": [{"message_ref": "msg_large", "text": text}],
                    "next_cursor": "unchanged-store-cursor", "has_more": True}

    server = create_server(tmp_path, store_factory=LargeStore)
    scope = {"group_ids": ["g"], "since": "2026-10-24", "until": "2026-10-25"}
    with pytest.raises(ToolError, match="result_too_large_reduce_page_size_or_context"):
        call(server, "read_messages", {**scope, "limit": 100})
    assert operations[-1] == ("close",)
    result = call(server, "read_messages", {**scope, "limit": 1})
    assert result.structuredContent["messages"][0]["text"] == "小页完整消息"
    assert result.structuredContent["next_cursor"] == "unchanged-store-cursor"
    assert result.structuredContent["has_more"] is True
