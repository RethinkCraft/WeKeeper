"""Current directory semantics are independent of actual group message authors."""
import copy
import json
import sqlite3

import pytest

from chatlog_keeper.archive import ArchiveError, ArchiveStore


def person(sender_id, group_nickname=""):
    return {"sender_id": sender_id, "remark": "", "nickname": sender_id + " nick",
            "alias": "", "group_nickname": group_nickname,
            "display_name": group_nickname or sender_id + " nick", "is_owner": sender_id == "a"}


def group(*people, native="directory@chatroom", status="available", nickname_status="parsed"):
    return {"group_id": native, "name": "Synthetic directory", "owner_id": "a",
            "members": [person(p) if isinstance(p, str) else p for p in people],
            "coverage": {"status": status, "scope": "local_cached_directory",
                         "member_count": len(people), "unresolved_member_count": int(status == "partial"),
                         "group_nickname_status": nickname_status}}


def current(store, native="directory@chatroom", **kwargs):
    return store.list_group_members(native, **kwargs)


def history(store):
    return [dict(r) for r in store.conn.execute("SELECT * FROM directory_history ORDER BY observation_id")]


def test_directory_persists_and_does_not_create_message_authors(tmp_path):
    with ArchiveStore(tmp_path) as store:
        store.update_group_directory("account", [group("a", "b", "c")])
        source = store.register_source("account", "db_storage/message/message_0.db", {"inode": 1}, "fp")
        store.get_stream(source["source_ref"], 1, "Msg_directory", "directory@chatroom", 1)
        store.commit_page(source["source_ref"], 1, "Msg_directory", "directory@chatroom", [{
            "row_id": 1, "server_id": "one", "local_type": 1, "create_time": 10,
            "sender_id": "a", "message_content": "Synthetic body",
        }], mode="backfill", checkpoint=1, complete=True)
        stats = store.get_group_stats(["directory@chatroom"], 0, 100)
        assert stats["total_records"] == 1
        assert stats["active_senders"] == 1
        assert current(store)["member_count"] == 3
        assert len(history(store)) == 1
    with ArchiveStore(tmp_path, readonly=True) as store:
        directory = current(store)
        assert [m["sender_id"] for m in directory["members"]] == ["a", "b", "c"]
        assert directory["owner_id"] == "a"
        assert directory["directory_coverage"]["scope"] == "local_cached_directory"
        assert "not complete historical membership" in directory["history_scope"]
        assert directory["directory_updated_at"] > 0
        with pytest.raises(ArchiveError):
            store.update_group_directory("account", [group("a")])


def test_member_pagination_and_changed_snapshot_cursor(tmp_path):
    with ArchiveStore(tmp_path) as store:
        snapshot = group("c", "a", "b")
        store.update_group_directory("account", [snapshot])
        first = current(store, limit=1)
        assert first["members"][0]["sender_id"] == "a"
        assert first["has_more"]
        second = current(store, limit=1, cursor=first["next_cursor"])
        third = current(store, limit=1, cursor=second["next_cursor"])
        assert second["members"][0]["sender_id"] == "b"
        assert third["members"][0]["sender_id"] == "c"
        assert not third["has_more"]
        store.update_group_directory("account", [copy.deepcopy(snapshot)])
        assert current(store, limit=1, cursor=first["next_cursor"])["members"][0]["sender_id"] == "b"
        assert len(history(store)) == 1
        snapshot["members"][1]["remark"] = "Observed new remark"
        store.update_group_directory("account", [snapshot])
        with pytest.raises(ArchiveError, match="cursor_snapshot_changed"):
            current(store, cursor=first["next_cursor"])
        assert len(history(store)) == 2


def test_member_cursor_is_bound_to_one_group(tmp_path):
    with ArchiveStore(tmp_path) as store:
        store.update_group_directory("account", [group("a", "b"), group("a", "c", native="other@chatroom")])
        first = current(store, limit=1)
        with pytest.raises(ArchiveError, match="cursor_scope_changed"):
            current(store, "other@chatroom", cursor=first["next_cursor"])
        store.update_group_directory("account", [group("d", native="other@chatroom")])
        assert current(store, cursor=first["next_cursor"])["members"][0]["sender_id"] == "b"


def test_partial_failures_preserve_observed_roster_and_recovery(tmp_path):
    with ArchiveStore(tmp_path) as store:
        store.update_group_directory("account", [group("a", "b", "c")])
        original = current(store)["members"]
        store.update_group_directory("account", [group("a", status="partial")])
        assert current(store)["members"] == original
        assert current(store)["directory_coverage"]["status"] == "partial"
        store.update_group_directory("account", [group(status="unsupported_schema")])
        assert current(store)["members"] == original
        assert current(store)["directory_coverage"]["status"] == "unsupported_schema"
        assert len(history(store)) == 1
        store.update_group_directory("account", [group("a", "d")])
        assert [m["sender_id"] for m in current(store)["members"]] == ["a", "d"]
        assert len(history(store)) == 2
        assert json.loads(history(store)[0]["payload"])["members"] == original
        assert store.get_group_stats(["directory@chatroom"], 0, 100)["total_records"] == 0


def test_explicit_complete_empty_roster_is_not_source_failure(tmp_path):
    with ArchiveStore(tmp_path) as store:
        store.update_group_directory("account", [group("a", "b")])
        store.update_group_directory("account", [])
        assert current(store)["member_count"] == 2
        store.update_group_directory("account", [group()])
        assert current(store)["member_count"] == 0
        assert current(store)["directory_coverage"]["status"] == "available"
        assert len(history(store)) == 2


def test_failed_group_nickname_parse_keeps_last_verified_name(tmp_path):
    with ArchiveStore(tmp_path) as store:
        store.update_group_directory("account", [group(person("a", "Verified group nickname"))])
        store.update_group_directory("account", [group(person("a"), nickname_status="parse_error")])
        directory = current(store)
        assert directory["members"][0]["group_nickname"] == "Verified group nickname"
        assert directory["directory_coverage"]["group_nickname_status"] == "parse_error"
        # Successful parsing can intentionally clear a group nickname.
        store.update_group_directory("account", [group(person("a"), nickname_status="parsed")])
        assert current(store)["members"][0]["group_nickname"] == ""


def test_directory_and_observation_roll_back_together(tmp_path):
    with ArchiveStore(tmp_path) as store:
        store.update_group_directory("account", [group("a", "b")])
        store.conn.executescript("""
          CREATE TRIGGER fail_directory_observation BEFORE INSERT ON directory_history
          BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;
        """)
        with pytest.raises(sqlite3.IntegrityError, match="synthetic failure"):
            store.update_group_directory("account", [group("a", "c")])
        assert [m["sender_id"] for m in current(store)["members"]] == ["a", "b"]
        assert len(history(store)) == 1
