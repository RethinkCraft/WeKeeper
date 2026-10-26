"""Independent regression checks for reconciliation against public queries."""
import pytest

from chatlog_keeper.archive import ArchiveError, ArchiveStore


def _source(store, maximum):
    group = "review@chatroom"
    store.upsert_groups("review-account", [{"id": group, "name": "Synthetic review"}])
    source = store.register_source("review-account", "db_storage/message/message_0.db",
                                   {"salt": "synthetic", "inode": 1}, "fingerprint")
    store.get_stream(source["source_ref"], source["generation"], "Msg_review", group, maximum)
    return source, group


def _row(row_id, server_id, text):
    return {"row_id": row_id, "local_type": 1, "create_time": row_id * 10,
            "real_sender_id": 1, "sender_id": "wxid_review",
            "server_id": server_id, "message_content": text}


def _commit(store, source, group, rows, mode="backfill", checkpoint=3):
    return store.commit_page(source["source_ref"], source["generation"], "Msg_review",
                             group, rows, mode=mode, checkpoint=checkpoint, complete=True)


def test_reconciled_edit_cannot_silently_change_an_inflight_search_snapshot(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source, group = _source(store, 3)
        _commit(store, source, group, [
            _row(1, "first", "needle one"),
            _row(2, "second", "needle two"),
            _row(3, "third", "needle three"),
        ])
        page = store.search_messages([group], 0, 100, "needle", limit=1)
        assert page["has_more"]
        _commit(store, source, group, [_row(2, "second", "changed after page one")],
                mode="reconcile", checkpoint=2)
        with pytest.raises(ArchiveError, match="snapshot|cursor"):
            store.search_messages([group], 0, 100, "needle", limit=1,
                                  cursor=page["next_cursor"])


def test_non_boundary_source_rowid_reuse_does_not_erase_the_archived_message(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source, group = _source(store, 3)
        _commit(store, source, group, [
            _row(1, "original-server-id", "original archive evidence"),
            _row(2, "second", "second"),
            _row(3, "unchanged-boundary", "boundary"),
        ])
        replacement = _row(1, "replacement-server-id", "a new message reused row one")
        try:
            _commit(store, source, group, [replacement], mode="reconcile", checkpoint=1)
        except ArchiveError:
            # Rejecting the page for a collector generation reset is safe too.
            pass
        records = store.read_messages([group], 0, 100)["messages"]
        assert any(m["server_id"] == "original-server-id"
                   and m["text"] == "original archive evidence" for m in records)


def test_source_identity_keeps_each_shards_time_after_server_id_deduplication(tmp_path):
    with ArchiveStore(tmp_path) as store:
        first, group = _source(store, 1)
        second = store.register_source("review-account", "db_storage/message/message_1.db",
                                       {"salt": "second-fixture", "inode": 2}, "second-fingerprint")
        store.get_stream(second["source_ref"], second["generation"], "Msg_review", group, 1)
        a = {**_row(1, "same-server-id", "same logical message"), "create_time": 10}
        b = {**a, "create_time": 11}
        _commit(store, first, group, [a], checkpoint=1)
        _commit(store, second, group, [b], checkpoint=1)
        assert store.get_status()["messages"] == 1
        assert store.get_origin_info(first["source_ref"], 1, "Msg_review", 1)["create_time"] == 10
        assert store.get_origin_info(second["source_ref"], 1, "Msg_review", 1)["create_time"] == 11
        # Same-source replays must compare the origin, not the first canonical time.
        _commit(store, first, group, [a], mode="reconcile", checkpoint=1)
        _commit(store, second, group, [b], mode="reconcile", checkpoint=1)
        assert store.get_status()["messages"] == 1
        assert all(source["generation"] == 1 for source in store.get_status()["sources"])
