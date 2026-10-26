import json
import os
from pathlib import Path
import sqlite3

import pytest

from chatlog_keeper.archive import ArchiveError, ArchiveStore


def raw(row_id=1, server_id="s1", text="测试文字", timestamp=10, **extra):
    return {"row_id": row_id, "server_id": server_id, "create_time": timestamp,
            "local_type": 1, "sender_id": "wxid_sender", "sender_name": "Sender",
            "message_content": text, "packed_info_data": None,
            "parsed": {"msg_type": 1, "text": text, "parse_status": "parsed"}, **extra}


def setup_source(store, path="db_storage/message/message_0.db", group="test@chatroom", maximum=10):
    source = store.register_source("account", path, {"inode": 1, "salt": "salt"}, "fp")
    store.upsert_groups("account", [{"id": group, "name": "测试"}])
    store.get_stream(source["source_ref"], source["generation"], "Msg_test", group, maximum)
    return source


def commit(store, source, rows, checkpoint=None, mode="backfill", complete=True):
    return store.commit_page(source["source_ref"], source["generation"], "Msg_test", "test@chatroom", rows,
                             mode=mode, checkpoint=checkpoint if checkpoint is not None else max(r["row_id"] for r in rows), complete=complete)


def test_message_and_checkpoint_rollback_together_and_replay_is_idempotent(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        store.conn.executescript("CREATE TRIGGER fail_page BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'simulated disk failure'); END;")
        with pytest.raises(sqlite3.IntegrityError):
            commit(store, source, [raw()])
        assert store.get_status()["messages"] == 0
        assert store.get_stream(source["source_ref"], 1, "Msg_test", "test@chatroom", 10)["backfill_rowid"] == 0
        store.conn.execute("DROP TRIGGER fail_page")
        first = commit(store, source, [raw()])["message_refs"][0]
        assert commit(store, source, [raw()])["message_refs"] == [first]
        assert store.get_status()["messages"] == 1
        assert store.conn.execute("SELECT COUNT(*) FROM origins").fetchone()[0] == 1
    with ArchiveStore(tmp_path) as restarted:
        assert commit(restarted, source, [raw()])["message_refs"] == [first]
        assert restarted.get_status()["messages"] == 1


def test_server_acknowledgement_merges_without_losing_message_reference(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        first = commit(store, source, [raw(server_id="")])["message_refs"][0]
        other = setup_source(store, "db_storage/message/message_1.db")
        second = commit(store, other, [raw(server_id="12345678901234567890")])["message_refs"][0]
        commit(store, source, [raw(server_id="12345678901234567890")])
        assert store.get_status()["messages"] == 1
        assert store.get_message_context(first)["message"]["message_ref"] == second
        assert len(store.get_message_context(first)["message"]["origins"]) == 2


def test_no_server_id_messages_with_same_content_are_not_collapsed(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        commit(store, source, [raw(1, ""), raw(2, "")])
        assert store.get_status()["messages"] == 2


def test_replaced_source_gets_new_generation_without_forgetting_history(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        ref = commit(store, source, [raw()])["message_refs"][0]
        changed = store.register_source("account", "db_storage/message/message_0.db", {"inode": 2, "salt": "new"}, "fp2")
        assert changed["generation"] == 2
        store.get_stream(changed["source_ref"], 2, "Msg_test", "test@chatroom", 1)
        assert commit(store, changed, [raw()])["message_refs"] == [ref]
        assert store.get_status()["messages"] == 1
        assert store.conn.execute("SELECT COUNT(*) FROM origins").fetchone()[0] == 2
        with pytest.raises(ArchiveError, match="source_generation_changed"):
            commit(store, source, [raw()])


def test_pagination_freezes_new_records_and_requires_same_scope(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        commit(store, source, [raw(i, f"s{i}", timestamp=i * 10) for i in range(1, 4)])
        first = store.read_messages(["test@chatroom"], 0, 100, limit=1)
        commit(store, source, [raw(4, "late", timestamp=15)], mode="live")
        second = store.read_messages(["test@chatroom"], 0, 100, limit=1, cursor=first["next_cursor"])
        third = store.read_messages(["test@chatroom"], 0, 100, limit=1, cursor=second["next_cursor"])
        assert [p["messages"][0]["timestamp"] for p in [first, second, third]] == [10, 20, 30]
        assert third["has_more"] is False
        assert len(store.read_messages(["test@chatroom"], 0, 100)["messages"]) == 4
        with pytest.raises(ArchiveError, match="cursor_scope_changed"):
            store.read_messages(["test@chatroom"], 1, 100, cursor=first["next_cursor"])


def test_chinese_short_and_trigram_search_and_literal_wildcards(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        commit(store, source, [raw(1, "1", "这张耳机图片很清晰"), raw(2, "2", "100%已经完成"), raw(3, "3", "普通消息")])
        assert len(store.search_messages(["test@chatroom"], 0, 100, "耳机")["messages"]) == 1
        assert len(store.search_messages(["test@chatroom"], 0, 100, "耳机图片")["messages"]) == 1
        assert len(store.search_messages(["test@chatroom"], 0, 100, "%")["messages"]) == 1
        assert not store.search_messages(["test@chatroom"], 0, 100, '" OR injected')["messages"]


def test_full_quote_reference_resolves_and_missing_quote_stays_explicit(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        original = commit(store, source, [raw(1, "12345678901234567890")])["message_refs"][0]
        parsed = {"msg_type": 49, "sub_type": 57, "text": "回复", "parse_status": "parsed", "reply": {"server_id": "12345678901234567890", "text": "完整引用"}}
        reply = commit(store, source, [raw(2, "reply", parsed=parsed)])["message_refs"][0]
        context = store.get_message_context(reply)
        assert context["quoted_message"]["message_ref"] == original
        assert context["message"]["reply"]["server_id"] == "12345678901234567890"
        parsed["reply"]["server_id"] = "not-archived"
        commit(store, source, [raw(2, "reply", parsed=parsed)])
        missing = store.get_message_context(reply)
        assert missing["message"]["reply"]["target_status"] == "not_archived"
        assert missing["quoted_message"] is None


def test_raw_unknown_rows_retained_and_credentials_never_in_public_output(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        private = 'sensitive-aes-material'
        parsed = {"msg_type": 65000, "text": f'<img aeskey="{private}"/>', "parse_status": "unparsed", "attachment": {"kind": "image", "aeskey": private}}
        commit(store, source, [raw(local_type=65000, parsed=parsed, message_content=b'\x00raw-payload', packed_info_data=b'\xffpacked')])
        origin = store.conn.execute("SELECT raw_content,packed_info_data FROM origins").fetchone()
        assert tuple(origin) == (b'\x00raw-payload', b'\xffpacked')
        public = json.dumps(store.read_messages(["test@chatroom"], 0, 100))
        assert private not in public
        assert store.get_status()["unparsed_records"] == 1


def test_statistics_count_top_level_records_not_embedded_quotes(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        commit(store, source, [raw(1, "1"), raw(2, "2", parsed={"msg_type": 49, "text": "引用", "parse_status": "parsed", "reply": {"server_id": "1"}}), raw(3, "3", parsed={"msg_type": 10000, "text": "入群", "parse_status": "parsed"})])
        stats = store.get_group_stats(["test@chatroom"], 0, 100)
        assert stats["total_records"] == 3
        assert stats["user_messages"] == 2
        assert stats["active_senders"] == 1


def test_source_unavailable_or_stale_collector_does_not_claim_current_coverage(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store, maximum=1)
        commit(store, source, [raw()])
        assert store.list_groups()["groups"][0]["local_backfill_complete"]
        store.set_collector_status(last_error="account_not_found")
        assert not store.list_groups()["groups"][0]["local_backfill_complete"]
        store.set_collector_status(last_error=None, last_cycle_finished_at=1)
        result = store.read_messages(["test@chatroom"], 0, 100)
        assert result["coverage"]["collector_stale"]
        assert not result["coverage"]["groups"][0]["local_backfill_complete"]


def test_readonly_mcp_connection_does_not_create_archive_and_reads_committed_data(tmp_path):
    with pytest.raises(ArchiveError, match="archive_not_initialized"):
        ArchiveStore(tmp_path, readonly=True)
    assert not (tmp_path / "archive").exists()
    with ArchiveStore(tmp_path) as writer:
        source = setup_source(writer)
        with ArchiveStore(tmp_path, readonly=True) as reader:
            assert reader.get_status()["messages"] == 0
            commit(writer, source, [raw()])
            assert reader.get_status()["messages"] == 1
            from chatlog_keeper import __version__
            assert reader.get_status()["version"] == __version__
            assert reader.list_groups()["coverage"]["archive_updated_at"] is not None
            with pytest.raises(ArchiveError, match="read_only_archive"):
                reader.set_collector_status(running=True)
        assert writer.path.stat().st_mode & 0o777 == 0o600
        assert writer.archive_dir.stat().st_mode & 0o777 == 0o700


def test_pending_media_round_robin_and_extras_preserved(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        parsed = {"msg_type": 3, "text": "[图片]", "parse_status": "parsed", "attachment": {"kind": "image", "file_md5": "a" * 32}}
        commit(store, source, [raw(1, "1", parsed=parsed), raw(2, "2", parsed=parsed)])
        first = store.pending_media(1)[0]
        store.update_media(first["media_ref"], status="ready", variants=[{"tier": "thumbnail"}])
        assert store.pending_media(1)[0]["media_ref"] != first["media_ref"]
        assert store.get_media_records(media_ref=first["media_ref"])[0]["variants"] == [{"tier": "thumbnail"}]


def test_completed_source_can_release_plaintext_and_daily_reconcile_becomes_due(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store, maximum=1)
        assert store.source_needs_work(source["source_ref"], 1)
        commit(store, source, [raw()])
        assert not store.source_needs_work(source["source_ref"], 1)
        assert store.source_needs_work(source["source_ref"], 1, now=10**11)
        store.get_stream(source["source_ref"], 1, "Msg_test", "test@chatroom", 2)
        assert store.source_needs_work(source["source_ref"], 1)


def test_unknown_cross_generation_identity_is_retained_and_reported_not_content_deduped(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store, maximum=1)
        commit(store, source, [raw(server_id="")])
        changed = store.rotate_source(source["source_ref"], "source_rowid_reused")
        store.get_stream(changed["source_ref"], 2, "Msg_test", "test@chatroom", 1)
        commit(store, changed, [raw(server_id="")])
        store.set_source_status(changed["source_ref"], True, error=None, fingerprint="new")
        assert store.get_status()["messages"] == 2
        assert store.get_status()["source_resets"][0]["reason"] == "source_rowid_reused"
        assert store.get_status()["unresolved_cross_generation_records"] == 1
        records = store.read_messages(["test@chatroom"], 0, 100)["messages"]
        assert sum(r["possible_cross_generation_duplicate"] for r in records) == 1
        assert store.get_group_stats(["test@chatroom"], 0, 100)["unresolved_cross_generation_records"] == 1


def test_long_range_stats_bound_sender_breakdown_and_use_monthly_buckets(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store, maximum=101)
        commit(store, source, [raw(i, str(i), timestamp=1700000000, sender_id=f"sender-{i}") for i in range(1, 102)])
        result = store.get_group_stats(["test@chatroom"], 0, 1800000000)
        assert result["user_messages"] == 101 and result["active_senders"] == 101
        assert len(result["by_sender"]) == 100 and result["sender_breakdown_truncated"] is True
        assert result["date_granularity"] == "month"
