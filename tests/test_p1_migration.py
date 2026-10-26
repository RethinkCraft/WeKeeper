"""P1 migration availability and directory recovery contracts."""
from chatlog_keeper.archive import ArchiveStore
from test_archive import setup_source, commit, raw


def test_metadata_pending_only_counts_reachable_current_generation(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        commit(store, source, [raw()])
        assert store.semantics_status()["metadata_backfill_pending"] == 1
        current = store.rotate_source(source["source_ref"], "synthetic_reset")
        status = store.semantics_status()
        assert status["metadata_backfill_pending"] == 0
        assert status["unavailable_metadata_origins"] == 1
        assert status["missing_mention_metadata"] == 1
        store.get_stream(current["source_ref"], current["generation"], "Msg_test", "test@chatroom", 2)
        commit(store, current, [raw(2, "new")])
        assert store.semantics_status()["metadata_backfill_pending"] == 1
        store.set_source_status(current["source_ref"], False, error="synthetic_unavailable")
        assert store.semantics_status()["metadata_backfill_pending"] == 0
        assert store.semantics_status()["unavailable_metadata_origins"] == 2


def test_identical_verified_directory_recovers_after_partial_read(tmp_path):
    with ArchiveStore(tmp_path) as store:
        group = {"group_id": "test@chatroom", "name": "Synthetic", "owner_id": "person",
                 "coverage": {"status": "available", "group_nickname_status": "parsed"},
                 "members": [{"sender_id": "person", "group_nickname": "Room name"}]}
        store.update_group_directory("account", [group])
        store.update_group_directory("account", [{**group, "members": [], "coverage": {"status": "partial"}}])
        assert store.list_group_members("test@chatroom")["directory_coverage"]["status"] == "partial"
        store.update_group_directory("account", [group])
        result = store.list_group_members("test@chatroom")
        assert result["directory_coverage"]["status"] == "available"
        assert result["member_count"] == 1
        assert store.conn.execute("SELECT COUNT(*) FROM directory_history").fetchone()[0] == 1
