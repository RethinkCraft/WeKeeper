"""Archive semantics integration using synthetic data, never WeChat sends."""
import html
import sqlite3

import pytest

from chatlog_keeper.archive import ArchiveError, ArchiveStore
from chatlog_keeper.message_model import PARSER_VERSION


GROUP = "synthetic@chatroom"
TABLE = "Msg_synthetic"


def source(store, path="message_0.db", group=GROUP):
    registered = store.register_source("account", path, {"inode": 1, "salt": "synthetic"}, "fp")
    store.upsert_groups("account", [{"id": group, "name": "Synthetic"}])
    store.get_stream(registered["source_ref"], registered["generation"], TABLE, group, 20)
    return registered


def raw(row_id=1, server_id="server1", **extra):
    return {"row_id": row_id, "server_id": server_id, "create_time": 10,
            "local_type": 49, "sender_id": "wxid_outer", "group_id": GROUP,
            "message_content": "synthetic raw", **extra}


def commit(store, registered, records, group=GROUP):
    return store.commit_page(registered["source_ref"], registered["generation"], TABLE,
                             group, records, mode="backfill", checkpoint=max(r["row_id"] for r in records))


def child(text, children=None, attachment=None):
    return {"text": text, "datatype": 17 if children else 2 if attachment else 1,
            "sender_id": "wxid_embedded", "sender_name": "Embedded", "create_time": 1,
            "children": children or [], "attachment": attachment, "parse_status": "parsed"}


def parsed(children):
    return {"msg_type": 49, "sub_type": 19, "text": "聊天记录", "parse_status": "parsed",
            "forwarded": children, "forwarded_status": "parsed"}


def xml_record(text="正文"):
    inner = '<recordinfo><datalist><dataitem datatype="1"><datadesc>' + html.escape(text) + '</datadesc><sourcename>Embedded</sourcename></dataitem></datalist></recordinfo>'
    return '<msg><appmsg><type>19</type><title>聊天记录</title><recorditem>' + html.escape(inner) + '</recorditem></appmsg></msg>'


def test_direct_paging_nested_expansion_long_text_and_top_level_counts(tmp_path):
    with ArchiveStore(tmp_path) as store:
        registered = source(store)
        full = "长文本" * 1500
        children = [child(full), child("nested", [child("one"), child("two")]), child("last")]
        ref = commit(store, registered, [raw(parsed=parsed(children))])["message_refs"][0]
        first = store.read_forwarded_messages(ref, limit=1)
        assert first["item"] is None
        assert first["children"][0]["text"] == full
        assert first["has_more"]
        second = store.read_forwarded_messages(ref, limit=1, cursor=first["next_cursor"])
        assert second["children"][0]["child_count"] == 2
        nested_ref = second["children"][0]["forward_ref"]
        nested = store.read_forwarded_messages(nested_ref, limit=1)
        assert nested["item"]["text"] == "nested"
        assert [i["text"] for i in nested["children"]] == ["one"]
        nested_last = store.read_forwarded_messages(nested_ref, limit=1, cursor=nested["next_cursor"])
        assert [i["text"] for i in nested_last["children"]] == ["two"]
        assert not nested_last["has_more"]
        final = store.read_forwarded_messages(ref, limit=1, cursor=second["next_cursor"])
        assert [i["text"] for i in final["children"]] == ["last"]
        message = store.get_message_context(ref)["message"]
        assert message["forwarded_count"] == 5
        assert message["forwarded_direct_count"] == 3
        stats = store.get_group_stats([GROUP], 0, 100)
        assert stats["total_records"] == stats["user_messages"] == stats["active_senders"] == 1


def test_ack_parent_merge_keeps_old_forward_refs_resolvable(tmp_path):
    with ArchiveStore(tmp_path) as store:
        registered = source(store)
        payload = parsed([child("first", [child("nested")])])
        unacked = commit(store, registered, [raw(server_id="", parsed=payload)])["message_refs"][0]
        old_child = store.read_forwarded_messages(unacked)["children"][0]["forward_ref"]
        old_grandchild = store.read_forwarded_messages(old_child)["children"][0]["forward_ref"]
        other = source(store, "message_1.db")
        canonical = commit(store, other, [raw(server_id="server_ack", parsed=payload)])["message_refs"][0]
        canonical_child = store.read_forwarded_messages(canonical)["children"][0]["forward_ref"]
        commit(store, registered, [raw(server_id="server_ack", parsed=payload)])
        assert store.get_status()["messages"] == 1
        assert store.read_forwarded_messages(old_child)["message_ref"] == canonical
        assert store.read_forwarded_messages(old_child)["item"]["forward_ref"] == canonical_child
        assert store.read_forwarded_messages(old_grandchild)["item"]["text"] == "nested"
        assert len(store.read_forwarded_messages(unacked)["children"]) == 1


def test_reparse_preserves_refs_first_seen_counts_and_collection_progress(tmp_path):
    with ArchiveStore(tmp_path) as store:
        registered = source(store)
        ref = commit(store, registered, [raw(message_content=xml_record("完整原文"))])["message_refs"][0]
        before_child = store.read_forwarded_messages(ref)["children"][0]["forward_ref"]
        first_seen = store.conn.execute("SELECT first_seen FROM messages WHERE message_ref=?", (ref,)).fetchone()[0]
        before_stream = dict(store.conn.execute("SELECT * FROM streams").fetchone())
        before_origin = tuple(store.conn.execute("SELECT source_ref,generation,table_name,row_id,payload_hash FROM origins").fetchone())
        store.conn.execute("UPDATE messages SET parser_version=1,text='old summary' WHERE message_ref=?", (ref,))
        store.conn.commit()
        assert store.needs_reparse()
        assert store.reparse_batch(limit=1) == {"reparsed": 1, "pending": 0}
        result = store.read_forwarded_messages(ref)
        assert result["children"][0]["forward_ref"] == before_child
        assert result["children"][0]["text"] == "完整原文"
        assert store.conn.execute("SELECT first_seen FROM messages WHERE message_ref=?", (ref,)).fetchone()[0] == first_seen
        assert dict(store.conn.execute("SELECT * FROM streams").fetchone()) == before_stream
        assert tuple(store.conn.execute("SELECT source_ref,generation,table_name,row_id,payload_hash FROM origins").fetchone()) == before_origin
        assert store.get_status()["messages"] == 1
        assert store.get_message_context(ref)["message"]["parser_version"] == PARSER_VERSION
        assert store.reparse_batch() == {"reparsed": 0, "pending": 0}


def test_forward_media_requires_exact_archived_digest_in_same_scope(tmp_path):
    digest = "a" * 32
    with ArchiveStore(tmp_path) as store:
        registered = source(store)
        attachment = {"kind": "image", "file_md5": digest}
        ref = commit(store, registered, [raw(parsed=parsed([child("image", attachment=attachment)]))])["message_refs"][0]
        image = {"msg_type": 3, "text": "[图片]", "parse_status": "parsed", "attachment": attachment}
        commit(store, registered, [raw(row_id=2, server_id="image", local_type=3, parsed=image)])
        media = store.pending_media()[0]
        item = store.read_forwarded_messages(ref)["children"][0]
        assert item["media_status"] == "unresolved"
        with pytest.raises(ArchiveError, match="forwarded_media_not_archived"):
            store.forward_media_reference(item["forward_ref"])
        store.update_media(media["media_ref"], status="ready", tier="full")
        item = store.read_forwarded_messages(ref)["children"][0]
        assert item["media_status"] == "archived"
        assert [m["media_ref"] for m in item["media"]] == [media["media_ref"]]
        assert store.forward_media_reference(item["forward_ref"]) == media["media_ref"]
        store.conn.execute("UPDATE media SET group_id='other@chatroom'")
        store.conn.commit()
        assert store.read_forwarded_messages(ref)["children"][0]["media_status"] == "unresolved"
        store.conn.execute("UPDATE media SET group_id=?,file_key=?", (GROUP, 'b' * 32))
        store.conn.commit()
        assert store.read_forwarded_messages(ref)["children"][0]["media_status"] == "unresolved"


def test_crash_before_forward_insert_rolls_back_parent_origin_and_checkpoint(tmp_path):
    with ArchiveStore(tmp_path) as store:
        registered = source(store)
        before = dict(store.conn.execute("SELECT * FROM streams").fetchone())
        store.conn.executescript("CREATE TRIGGER fail_forward BEFORE INSERT ON forwarded BEGIN SELECT RAISE(ABORT,'simulated disk failure'); END;")
        record = raw(parsed=parsed([child("one")]))
        with pytest.raises(sqlite3.IntegrityError):
            commit(store, registered, [record])
        assert store.get_status()["messages"] == 0
        assert store.conn.execute("SELECT COUNT(*) FROM origins").fetchone()[0] == 0
        assert dict(store.conn.execute("SELECT * FROM streams").fetchone()) == before
        store.conn.execute("DROP TRIGGER fail_forward")
        ref = commit(store, registered, [record])["message_refs"][0]
        assert len(store.read_forwarded_messages(ref)["children"]) == 1
        commit(store, registered, [record])
        assert store.conn.execute("SELECT COUNT(*) FROM forwarded").fetchone()[0] == 1


def test_reparse_crash_rolls_back_then_resumes_without_new_refs(tmp_path):
    with ArchiveStore(tmp_path) as store:
        registered = source(store)
        ref = commit(store, registered, [raw(message_content=xml_record("after reparse"))])["message_refs"][0]
        original_forward = store.read_forwarded_messages(ref)["children"][0]["forward_ref"]
        store.conn.execute("UPDATE messages SET parser_version=1,text='before reparse'")
        store.conn.commit()
        store.conn.executescript("CREATE TRIGGER fail_reparse BEFORE INSERT ON forwarded BEGIN SELECT RAISE(ABORT,'simulated crash'); END;")
        with pytest.raises(sqlite3.IntegrityError):
            store.reparse_batch(limit=1)
        row = store.conn.execute("SELECT text,parser_version FROM messages").fetchone()
        assert tuple(row) == ("before reparse", 1)
        assert store.read_forwarded_messages(ref)["children"][0]["forward_ref"] == original_forward
        store.conn.execute("DROP TRIGGER fail_reparse")
        assert store.reparse_batch(limit=1) == {"reparsed": 1, "pending": 0}
        assert store.read_forwarded_messages(ref)["children"][0]["forward_ref"] == original_forward


def test_forward_cursor_rejects_changed_scope_and_reparsed_content(tmp_path):
    with ArchiveStore(tmp_path) as store:
        registered = source(store)
        ref = commit(store, registered, [raw(parsed=parsed([child("one"), child("two")]))])["message_refs"][0]
        page = store.read_forwarded_messages(ref, limit=1)
        with pytest.raises(ArchiveError, match="cursor_scope_changed"):
            store.read_forwarded_messages(page["children"][0]["forward_ref"], cursor=page["next_cursor"])
        commit(store, registered, [raw(parsed=parsed([child("changed"), child("two")]))])
        with pytest.raises(ArchiveError, match="cursor_snapshot_changed"):
            store.read_forwarded_messages(ref, cursor=page["next_cursor"])
