"""P2 MCP acceptance uses a synthetic archive, never a running WeChat client."""
import asyncio
import base64
from datetime import datetime
import hashlib
import io

import pytest

pytest.importorskip("mcp")
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import EmbeddedResource, ImageContent
from PIL import Image

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.mcp_server import create_server
from test_archive import commit, raw, setup_source


GROUP = "test@chatroom"
STAMP = int(datetime.fromisoformat("2026-10-25T12:00:00+08:00").timestamp())
SCOPE = {"group_ids": [GROUP], "since": "2026-10-25T00:00:00+08:00",
         "until": "2026-10-26T00:00:00+08:00"}


def call(server, tool, **arguments):
    return asyncio.run(server.call_tool(tool, arguments))


def cache(store, data, *, decoded=False):
    digest = hashlib.sha256(data).hexdigest()
    relative = f"{'decoded' if decoded else 'raw'}/{digest[:2]}/{digest}.bin"
    path = store.media_dir / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(0o600)
    return relative, digest


@pytest.fixture
def archive(tmp_path):
    picture = io.BytesIO()
    Image.new("RGB", (6, 4), "purple").save(picture, format="PNG")
    pixels = picture.getvalue()
    html = b'<html><body><p>Offline note document</p><script>ignore me</script></body></html>'
    audio = b"#!SILK_V3synthetic cached voice"
    voice = {"msg_type": 34, "text": "[voice]", "parse_status": "parsed",
             "attachment": {"kind": "voice", "voice_length_ms": 5000}}
    note = {"msg_type": 49, "sub_type": 24, "text": "[note]", "parse_status": "parsed",
            "card": {"kind": "note", "title": "Local card", "public_web_url": "https://example.org/story"},
            "card_components": [
                {"datatype": 1, "text": "card-only needle", "children": []},
                {"datatype": 2, "text": "[image]", "children": [],
                 "attachment": {"kind": "image", "file_md5": hashlib.md5(pixels).hexdigest()}},
                {"datatype": 8, "text": "[document]", "children": [],
                 "attachment": {"kind": "file", "filename": "note.htm", "fileext": "htm",
                                "md5": hashlib.md5(html).hexdigest(), "total_bytes": len(html)}}]}
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        refs = commit(store, source, [
            raw(1, "voice-sid", timestamp=STAMP, local_type=34, parsed=voice),
            raw(2, "note-sid", timestamp=STAMP + 1, local_type=49, parsed=note),
            raw(3, "text-sid", text="ordinary group message", timestamp=STAMP + 2),
        ])["message_refs"]
        raw_path, digest = cache(store, audio)
        store.upsert_voice("account", GROUP, "voice-sid",
            {"raw_path": raw_path, "raw_sha256": digest, "raw_size": len(audio),
             "codec": "silk_v3", "mime": "audio/silk"},
            {"database": "db_storage/message/media_0.db", "table": "VoiceInfo", "row_id": 7})
        avatar_path, avatar_digest = cache(store, pixels)
        avatar = {"relative_path": avatar_path, "sha256": avatar_digest,
                  "mime": "image/png", "width": 6, "height": 4}
        store.upsert_profiles("account", [
            {"subject_id": "wxid_sender", "profile": {"nickname": "Synthetic sender"}, "avatar": avatar},
            {"subject_id": GROUP, "profile": {"nickname": "Synthetic group"}, "avatar": avatar}])
        items = store.read_card(refs[1])["components"]
        decoded_path, image_digest = cache(store, pixels, decoded=True)
        image_record = store.component_media_records(items[1]["item_ref"])[0]
        store.update_media(image_record["media_ref"], status="ready", decoded_path=decoded_path,
                           decoded_sha256=image_digest, mime="image/png", tier="full", width=6, height=4)
        file_path, file_digest = cache(store, html)
        file_record = store.component_media_records(items[2]["item_ref"])[0]
        store.update_media(file_record["media_ref"], status="ready", raw_path=file_path,
                           raw_sha256=file_digest, raw_size=len(html), mime="text/html",
                           association_evidence="content_md5")
    return {"root": tmp_path, "voice": refs[0], "note": refs[1], "text": refs[2],
            "items": items, "pixels": pixels, "html": html, "audio": audio}


def test_fifteen_tools_have_one_explicit_side_effect_and_no_send_capability(archive):
    opened = []

    def factory(path, *, readonly):
        opened.append(readonly)
        return ArchiveStore(path, readonly=readonly)

    server = create_server(archive["root"], store_factory=factory)
    tools = asyncio.run(server.list_tools())
    assert len(tools) == 15 and opened == []
    write_tools = [tool for tool in tools if tool.annotations.readOnlyHint is False]
    assert [tool.name for tool in write_tools] == ["request_voice_transcription"]
    assert write_tools[0].annotations.openWorldHint is True
    assert not any(tool.annotations.destructiveHint or "send" in tool.name for tool in tools)
    search = next(tool for tool in tools if tool.name == "search_messages").inputSchema
    for option in ("include_transcripts", "include_card_text"):
        assert search["properties"][option]["default"] is False
    for tool in tools:
        if tool.name != "request_voice_transcription":
            assert tool.annotations.readOnlyHint and not tool.annotations.openWorldHint


def test_cached_avatar_is_returned_only_when_explicitly_requested(archive):
    server = create_server(archive["root"])
    metadata = call(server, "get_profile", group_id=GROUP, sender_id="wxid_sender")
    assert metadata.structuredContent["kind"] == "member"
    assert metadata.structuredContent["profile"]["nickname"] == "Synthetic sender"
    assert metadata.structuredContent["avatar"]["status"] == "archived"
    assert not any(isinstance(block, ImageContent) for block in metadata.content)
    response = call(server, "get_profile", group_id=GROUP, sender_id="wxid_sender", include_avatar=True)
    image = next(block for block in response.content if isinstance(block, ImageContent))
    assert image.mimeType == "image/png" and base64.b64decode(image.data) == archive["pixels"]
    with Image.open(io.BytesIO(base64.b64decode(image.data))) as decoded:
        assert decoded.size == (6, 4)
    group = call(server, "get_profile", group_id=GROUP, include_avatar=True)
    assert group.structuredContent["kind"] == "group"
    assert any(isinstance(block, ImageContent) for block in group.content)
    serialized = response.model_dump_json()
    assert "relative_path" not in serialized and "raw_path" not in serialized
    with pytest.raises(ToolError, match="member_not_observed"):
        call(server, "get_profile", group_id=GROUP, sender_id="never-observed")


def test_voice_query_is_cached_metadata_and_native_text_without_request(archive):
    opened = []

    def readonly_factory(path, *, readonly):
        assert readonly
        opened.append(readonly)
        return ArchiveStore(path, readonly=True)

    server = create_server(archive["root"], store_factory=readonly_factory)
    initial = call(server, "get_voice", reference=archive["voice"]).structuredContent
    assert initial["audio_status"] == "archived" and initial["voice_length_ms"] == 5000
    assert initial["audio"]["size_bytes"] == len(archive["audio"])
    assert initial["audio"]["codec"] == "silk_v3"
    assert initial["native_text"] is None and initial["task"] is None
    with ArchiveStore(archive["root"]) as store:
        store.save_native_transcript(archive["voice"], "native-only phrase", {"kind": "wechat_native", "build": "fixture"})
        before = store.conn.serialize()
    result = call(server, "get_voice", reference=archive["voice"])
    assert result.structuredContent["native_text"] == "native-only phrase"
    assert result.structuredContent["transcript"]["provenance"]["kind"] == "wechat_native"
    assert "raw_path" not in result.model_dump_json() and "SILK_V3" not in result.model_dump_json()
    assert opened == [True, True]
    with ArchiveStore(archive["root"], readonly=True) as store:
        assert store.conn.serialize() == before
        assert store.conn.execute("SELECT COUNT(*) FROM native_voice_jobs").fetchone()[0] == 0


def test_only_explicit_transcription_request_opens_writable_store_and_stays_gated(archive):
    opened = []

    def factory(path, *, readonly):
        opened.append(readonly)
        return ArchiveStore(path, readonly=readonly)

    server = create_server(archive["root"], store_factory=factory)
    call(server, "get_voice", reference=archive["voice"])
    call(server, "read_card", reference=archive["note"])
    assert opened == [True, True]
    response = call(server, "request_voice_transcription", message_ref=archive["voice"])
    assert response.structuredContent["status"] == "unavailable"
    assert opened == [True, True, True, False]
    with ArchiveStore(archive["root"], readonly=True) as store:
        assert store.conn.execute("SELECT COUNT(*) FROM native_voice_jobs").fetchone()[0] == 0
    opened.clear()
    with pytest.raises(ToolError, match="invalid_query"):
        call(server, "request_voice_transcription", message_ref=archive["items"][0]["item_ref"])
    assert opened == []
    with pytest.raises(ToolError, match="not_voice"):
        call(server, "request_voice_transcription", message_ref=archive["text"])
    assert opened == [True]


def test_verified_request_queues_one_task_and_reuses_native_result_without_client(archive):
    # Capability is synthetic; no worker is started and no UI adapter is called.
    with ArchiveStore(archive["root"]) as store:
        store.set_meta("native_transcription_capability", {"enabled": True, "verified": True, "verified_build": "fixture"})
        identity = tuple(store.conn.execute("SELECT message_ref,first_seen,text FROM messages WHERE message_ref=?", (archive["voice"],)).fetchone())
    server = create_server(archive["root"])
    first = call(server, "request_voice_transcription", message_ref=archive["voice"]).structuredContent
    duplicate = call(server, "request_voice_transcription", message_ref=archive["voice"]).structuredContent
    assert first["status"] == duplicate["status"] == "queued"
    assert first["job_id"] == duplicate["job_id"] and duplicate["reused"]
    queued = call(server, "get_voice", reference=archive["voice"]).structuredContent
    assert queued["task"]["job_id"] == first["job_id"]
    with ArchiveStore(archive["root"]) as store:
        store.save_native_transcript(archive["voice"], "native result", {"kind": "wechat_native"}, job_id=first["job_id"])
    ready = call(server, "request_voice_transcription", message_ref=archive["voice"]).structuredContent
    assert ready["status"] == "ready" and ready["reused"]
    with ArchiveStore(archive["root"], readonly=True) as store:
        assert store.conn.execute("SELECT COUNT(*) FROM native_voice_jobs").fetchone()[0] == 1
        assert tuple(store.conn.execute("SELECT message_ref,first_seen,text FROM messages WHERE message_ref=?", (archive["voice"],)).fetchone()) == identity
        assert store.get_status()["messages"] == 3


def test_transcript_and_card_search_require_opt_in_and_return_original_messages(archive):
    with ArchiveStore(archive["root"]) as store:
        store.save_native_transcript(archive["voice"], "native-only phrase", {"kind": "wechat_native"})
    server = create_server(archive["root"])
    for keyword, flag, reference, kind in (
        ("native-only", "include_transcripts", archive["voice"], "transcript"),
        ("card-only", "include_card_text", archive["note"], "card"),
    ):
        assert call(server, "search_messages", **SCOPE, keyword=keyword).structuredContent["messages"] == []
        found = call(server, "search_messages", **SCOPE, keyword=keyword, **{flag: True}).structuredContent["messages"]
        assert [message["message_ref"] for message in found] == [reference]
        assert found[0]["content_matches"][0]["source_kind"] == kind
        assert not call(server, "search_messages", **SCOPE, keyword=keyword, sender="other-sender", **{flag: True}).structuredContent["messages"]
    assert call(server, "get_group_stats", **SCOPE).structuredContent["total_records"] == 3


def test_card_component_refs_page_in_order_return_pixels_and_cached_document(archive):
    server = create_server(archive["root"])
    first = call(server, "read_card", reference=archive["note"], limit=1).structuredContent
    assert first["card"]["public_web_url"] == "https://example.org/story"
    assert first["components"][0]["text"] == "card-only needle" and first["has_more"]
    second = call(server, "read_card", reference=archive["note"], limit=1, cursor=first["next_cursor"]).structuredContent
    image_ref = second["components"][0]["item_ref"]
    assert image_ref == archive["items"][1]["item_ref"]
    image = call(server, "get_media", reference=image_ref, quality="full")
    assert image.structuredContent["requested_reference"] == image_ref
    assert image.structuredContent["association_evidence"] == "archived_component_mapping"
    block = next(block for block in image.content if isinstance(block, ImageContent))
    assert base64.b64decode(block.data) == archive["pixels"]
    third = call(server, "read_card", reference=archive["note"], limit=1, cursor=second["next_cursor"]).structuredContent
    document_ref = third["components"][0]["item_ref"]
    assert third["has_more"] is False
    attachment = call(server, "get_attachment", reference=document_ref, include_content=True)
    binary = next(block for block in attachment.content if isinstance(block, EmbeddedResource))
    assert base64.b64decode(binary.resource.blob) == archive["html"]
    assert attachment.structuredContent["item_ref"] == document_ref
    document = call(server, "read_document", reference=document_ref).structuredContent
    assert document["extraction_complete"]
    assert "Offline note document" in "\n".join(unit["text"] for unit in document["units"])
    assert "ignore me" not in "\n".join(unit["text"] for unit in document["units"])
    assert call(server, "get_group_stats", **SCOPE).structuredContent["total_records"] == 3
