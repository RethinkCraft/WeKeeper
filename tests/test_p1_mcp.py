"""Real archive through the in-process MCP server, with synthetic data only."""
import asyncio
import base64
from datetime import datetime
import hashlib
import html
import io

import pytest

pytest.importorskip("mcp")
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ImageContent
from PIL import Image

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.mcp_server import create_server

GROUP = "p1-mcp@chatroom"
OTHER = "p1-other@chatroom"
SINCE = "2026-10-25T00:00:00+08:00"
UNTIL = "2026-10-25T23:59:59+08:00"
STAMP = int(datetime.fromisoformat("2026-10-25T12:00:00+08:00").timestamp())
SCOPE = {"group_ids": [GROUP], "since": SINCE, "until": UNTIL}


def call(server, tool, **arguments):
    return asyncio.run(server.call_tool(tool, arguments))


def forward_xml():
    nested = ('<dataitem datatype="17"><sourcename>Embedded A</sourcename>'
              '<dataitemsource><fromusr>embedded_a</fromusr></dataitemsource><recordxml>'
              '<recordinfo><datalist><dataitem datatype="1"><datadesc>deep needle</datadesc>'
              '<dataitemsource><fromusr>embedded_b</fromusr></dataitemsource>'
              '<srcMsgCreateTime>1</srcMsgCreateTime></dataitem></datalist></recordinfo>'
              '</recordxml></dataitem>')
    record = ('<recordinfo><datalist><dataitem datatype="1"><datadesc>embedded-only needle</datadesc>'
              '<sourcename>Embedded A</sourcename><dataitemsource><fromusr>embedded_a</fromusr></dataitemsource>'
              '<srcMsgCreateTime>1</srcMsgCreateTime></dataitem>' + nested +
              '<dataitem datatype="2"><fullmd5>' + 'a' * 32 + '</fullmd5>'
              '<dataitemsource><fromusr>embedded_a</fromusr></dataitemsource>'
              '<key>private-inline-key</key></dataitem></datalist></recordinfo>')
    return '<msg><appmsg><type>19</type><title>Bundle title</title><recorditem>' + html.escape(record) + '</recorditem></appmsg></msg>'


def profile(name, sender, nickname):
    return (f'<link name="{name}" type="link_profile"><memberlist><member><username>{sender}</username>'
            f'<nickname>{nickname}</nickname></member></memberlist></link>')


def event(template, links):
    return ('<sysmsg type="sysmsgtemplate"><sysmsgtemplate><content_template><template>' + template +
            '</template><link_list>' + links + '</link_list></content_template></sysmsgtemplate></sysmsg>')


@pytest.fixture
def archive(tmp_path):
    paths, refs = {}, {}
    with ArchiveStore(tmp_path) as store:
        store.upsert_groups("account", [{"id": GROUP, "name": "Synthetic group"},
                                         {"id": OTHER, "name": "Other synthetic group"}])
        source = store.register_source("account", "db_storage/message/message_0.db", {"inode": 1}, "fp")
        table = "Msg_p1mcp"
        store.get_stream(source["source_ref"], 1, table, GROUP, 12)
        empty = "<msgsource/>"

        def row(number, text, kind=1, source_text=empty, sender="outer_a", timestamp=None):
            return {"row_id": number, "server_id": f"sid-{number}", "local_type": kind,
                    "create_time": timestamp or STAMP + number, "sender_id": sender,
                    "message_content": text, "message_source": source_text}

        quote = ('<msg><appmsg><type>57</type><title>reply needle</title><refermsg><type>1</type>'
                 '<svrid>sid-1</svrid><content>' + html.escape(
                     '<msgsource><atuserlist>notify@all</atuserlist></msgsource>') +
                 '</content></refermsg></appmsg></msg>')
        rows = [
            row(1, "tag needle", source_text="<msgsource><atuserlist>wxid_target</atuserlist></msgsource>"),
            row(2, "broadcast needle", sender="outer_b", source_text="<msgsource><atuserlist>notify@all</atuserlist></msgsource>"),
            row(3, "@所有人 fake mention needle"),
            row(4, "@所有人 legacy needle", source_text=None),
            row(5, quote, 49),
            row(6, forward_xml(), 49),
            row(7, event('"$username$"邀请你加入了群聊', profile("username", "inviter", "Inviter")), 10000),
            row(8, event('"$username$"修改群名为“$remark$”', profile("username", "renamer", "Renamer") +
                         '<link name="remark" type="link_plain"><plain>New synthetic name</plain></link>'), 10000),
            row(9, '<sysmsg type="revokemsg"><revokemsg><newmsgid>sid-1</newmsgid><session>' + GROUP +
                   '</session><replacemsg>撤回了一条消息</replacemsg></revokemsg></sysmsg>', 10000),
            row(10, '<msg><img md5="' + 'a' * 32 + '"/></msg>', 3),
            row(11, "invalid metadata needle", source_text="not-xml"),
            row(12, forward_xml(), 49, timestamp=STAMP - 2 * 86400),
        ]
        committed = store.commit_page(source["source_ref"], 1, table, GROUP, rows,
                                      mode="backfill", checkpoint=12, complete=True)
        refs = {i: ref for i, ref in enumerate(committed["message_refs"], 1)}
        store.get_stream(source["source_ref"], 1, "Msg_other", OTHER, 1)
        store.commit_page(source["source_ref"], 1, "Msg_other", OTHER,
                          [row(1, forward_xml(), 49)], mode="backfill", checkpoint=1, complete=True)
        store.update_group_directory("account", [{
            "group_id": GROUP, "name": "Synthetic group", "owner_id": "outer_a",
            "members": [{"sender_id": member, "remark": "", "nickname": member,
                         "alias": "", "group_nickname": "Group " + member,
                         "display_name": "Group " + member, "is_owner": member == "outer_a"}
                        for member in ("outer_a", "outer_b", "silent_member")],
            "coverage": {"status": "available", "scope": "local_cached_directory", "group_nickname_status": "parsed"},
        }])
        binary = io.BytesIO()
        Image.new("RGB", (4, 3), "blue").save(binary, format="PNG")
        data = binary.getvalue()
        digest = hashlib.sha256(data).hexdigest()
        relative = f"decoded/{digest[:2]}/{digest}.png"
        path = store.media_dir / relative
        path.parent.mkdir(parents=True)
        path.write_bytes(data)
        media = store.get_media_records(message_ref=refs[10])[0]
        store.update_media(media["media_ref"], status="ready", decoded_path=relative,
                           decoded_sha256=digest, mime="image/png", tier="full", width=4, height=3)
        paths["png"] = data
    return tmp_path, create_server(tmp_path), refs, paths


def messages(server, **filters):
    return call(server, "read_messages", **SCOPE, **filters).structuredContent["messages"]


def test_query_tools_are_readonly_and_do_not_mutate_archive(archive):
    root, _, refs, _ = archive
    opened = []

    def readonly_store(path, *, readonly):
        assert readonly is True
        store = ArchiveStore(path, readonly=readonly)
        assert store.conn.execute("PRAGMA query_only").fetchone()[0] == 1
        opened.append(store)
        return store

    server = create_server(root, store_factory=readonly_store)
    tools = asyncio.run(server.list_tools())
    assert len(tools) == 15
    for tool in tools:
        native_request = tool.name == "request_voice_transcription"
        assert tool.annotations.readOnlyHint is not native_request
        assert tool.annotations.idempotentHint is not native_request
        assert not tool.annotations.destructiveHint
        assert tool.annotations.openWorldHint is native_request
        assert "send" not in tool.name
    assert opened == []
    with ArchiveStore(root, readonly=True) as store:
        before = store.conn.serialize()
    assert call(server, "get_status").structuredContent["messages"] == 13
    assert len(call(server, "list_groups").structuredContent["groups"]) == 2
    assert len(messages(server)) == 11
    assert call(server, "search_messages", **SCOPE, keyword="tag").structuredContent["messages"]
    assert call(server, "get_message_context", message_ref=refs[5]).structuredContent["quoted_message"]["message_ref"] == refs[1]
    assert call(server, "get_media", reference=refs[10], quality="full").structuredContent["width"] == 4
    assert call(server, "get_group_stats", **SCOPE).structuredContent["total_records"] == 11
    assert call(server, "read_forwarded_messages", reference=refs[6]).structuredContent["children"]
    assert call(server, "list_group_members", group_id=GROUP).structuredContent["member_count"] == 3
    with ArchiveStore(root, readonly=True) as store:
        assert store.conn.serialize() == before


def test_verified_mentions_all_and_event_filters(archive):
    _, server, refs, _ = archive
    assert [m["message_ref"] for m in messages(server, mention_target="wxid_target")] == [refs[1]]
    assert [m["message_ref"] for m in messages(server, mention_all=True)] == [refs[2]]
    without_all = {m["message_ref"] for m in messages(server, mention_all=False)}
    assert refs[3] in without_all  # Plain text @所有人 carries no real mention.
    assert refs[5] in without_all  # The quoted source's mention belongs to its original.
    assert refs[4] not in without_all  # Missing metadata is unknown, not false.
    assert refs[11] not in without_all  # Malformed metadata is also unknown.
    for kind, ref in (("member_join", refs[7]), ("group_rename", refs[8]), ("message_revoke", refs[9])):
        page = messages(server, event_type=kind)
        assert [m["message_ref"] for m in page] == [ref]
        assert page[0]["event"]["event_type"] == kind
    revoked = messages(server, event_type="message_revoke")[0]["event"]
    assert revoked["target_ref"] == refs[1] and revoked["target_status"] == "archived"
    result = call(server, "search_messages", **SCOPE, keyword="needle", mention_target="wxid_target")
    assert [m["message_ref"] for m in result.structuredContent["messages"]] == [refs[1]]


def test_forwarded_keyword_search_is_opt_in_and_uses_outer_scope(archive):
    _, server, refs, _ = archive
    plain = call(server, "search_messages", **SCOPE, keyword="embedded-only")
    assert plain.structuredContent["messages"] == []
    expanded = call(server, "search_messages", **SCOPE, keyword="embedded-only", include_forwarded=True)
    page = expanded.structuredContent
    assert page["search_scope"] == "top_level_and_forwarded_children"
    assert [m["message_ref"] for m in page["messages"]] == [refs[6]]
    found = page["messages"][0]
    match = found["forwarded_matches"][0]
    assert match["path"] == "1" and match["forward_ref"].startswith("fwd_")
    child = call(server, "read_forwarded_messages", reference=match["forward_ref"]).structuredContent["item"]
    assert child["create_time"] == 1  # The embedded original is decades outside the group range.
    assert child["text"] == "embedded-only needle"
    assert call(server, "search_messages", **SCOPE, keyword="embedded-only", include_forwarded=True,
                sender="embedded_a").structuredContent["messages"] == []
    assert call(server, "search_messages", **SCOPE, keyword="embedded-only", include_forwarded=True,
                sender="outer_a", msg_types=[49]).structuredContent["messages"]


def test_forward_tree_pages_direct_children_and_verified_media_returns_pixels(archive):
    _, server, refs, paths = archive
    one = call(server, "read_forwarded_messages", reference=refs[6], limit=1).structuredContent
    assert one["item"] is None and one["has_more"]
    assert [row["path"] for row in one["children"]] == ["1"]
    two = call(server, "read_forwarded_messages", reference=refs[6], limit=1,
               cursor=one["next_cursor"]).structuredContent
    nested_ref = two["children"][0]["forward_ref"]
    nested = call(server, "read_forwarded_messages", reference=nested_ref).structuredContent
    assert nested["item"]["path"] == "2"
    assert [row["path"] for row in nested["children"]] == ["2.1"]
    three = call(server, "read_forwarded_messages", reference=refs[6], limit=1,
                 cursor=two["next_cursor"]).structuredContent
    assert not three["has_more"]
    image = three["children"][0]
    assert image["datatype"] == 2 and image["attachment"]["kind"] == "image"
    assert image["media_status"] == "archived"
    result = call(server, "get_media", reference=image["forward_ref"], quality="full")
    pixels = next(c for c in result.content if isinstance(c, ImageContent))
    assert base64.b64decode(pixels.data) == paths["png"]
    assert "private-inline-key" not in three.__str__()


def test_expanded_statistics_preserve_top_level_counts_and_member_meaning(archive):
    _, server, _, _ = archive
    stats = call(server, "get_group_stats", **SCOPE).structuredContent
    assert stats["total_records"] == 11
    assert stats["user_messages"] == 8
    assert stats["active_senders"] == 2
    assert stats["forwarded_parent_messages"] == 1
    assert stats["forwarded_items"] == 4
    assert stats["embedded_author_labels"] == 2
    assert stats["messages_with_mentions"] == 2
    assert stats["messages_mentioning_all"] == 1
    assert stats["events_by_type"] == {"member_join": 1, "group_rename": 1, "message_revoke": 1}
    assert stats["semantics"]["missing_mention_metadata"] == 1
    assert stats["semantics"]["mention_metadata_errors"] == 1
    first = call(server, "list_group_members", group_id=GROUP, limit=1).structuredContent
    assert first["member_count"] == 3 and first["has_more"]
    assert first["owner_id"] == "outer_a"
    assert first["directory_coverage"]["scope"] == "local_cached_directory"
    assert first["members"][0]["group_nickname"] == "Group outer_a"
    assert "not complete historical membership" in first["history_scope"]


def test_semantic_filters_are_bound_to_pagination_scope(archive):
    _, server, _, _ = archive
    first = call(server, "read_messages", **SCOPE, limit=1, mention_all=False).structuredContent
    assert first["has_more"]
    with pytest.raises(ToolError, match="cursor_scope_changed"):
        call(server, "read_messages", **SCOPE, limit=1, cursor=first["next_cursor"], mention_all=True)
    with pytest.raises(ToolError, match="invalid_event_type"):
        call(server, "read_messages", **SCOPE, event_type="guessed_member_leave")
    with pytest.raises(ToolError, match="invalid_query"):
        call(server, "read_forwarded_messages", reference="../../raw.xml")
