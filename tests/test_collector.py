import hashlib
from pathlib import Path
import shutil
import sqlite3
import time
from types import SimpleNamespace

import pytest

from chatlog_keeper.collector import Collector, CollectorError
from chatlog_keeper.message_model import source_row_hash

GROUP = "test@chatroom"
TABLE = "Msg_" + hashlib.md5(GROUP.encode()).hexdigest()


class Store:
    """Transactional in-memory fixture; real atomicity is tested in archive tests."""
    def __init__(self, directory):
        self.state_dir = directory
        self.sources = {}
        self.streams = {}
        self.groups = {}
        self.records = {}
        self.commits = []
        self.status = {}
        self.fail_commit = False

    def register_source(self, account_id, relative_path, identity, fingerprint):
        ref = account_id + "/" + relative_path
        source = self.sources.get(ref)
        if source is None:
            source = self.sources[ref] = {"source_ref": ref, "generation": 1,
                                         "identity": identity, "fingerprint": None}
        elif source["identity"] != identity:
            source["generation"] += 1
            source["identity"] = identity
        return {**source, "changed": source["fingerprint"] != fingerprint}

    def upsert_groups(self, account_id, groups):
        self.groups.update({g["id"]: g for g in groups})

    def get_stream(self, ref, generation, table, group, maximum):
        key = ref, generation, table
        stream = self.streams.setdefault(key, {
            "group_id": group,
            "live_rowid": maximum, "backfill_rowid": 0, "backfill_end": maximum,
            "backfill_done": maximum == 0, "reconcile_rowid": 0,
            "reconcile_active": False, "last_reconciled_at": 0,
        })
        stream["observed_max_rowid"] = maximum
        return dict(stream)

    def source_needs_work(self, ref, generation, now=None):
        now = time.time() if now is None else now
        streams = [s for (source_ref, gen, _), s in self.streams.items()
                   if source_ref == ref and gen == generation]
        if not streams:
            return not self.sources[ref].get("fingerprint")
        return any(not s["backfill_done"] or s["reconcile_active"]
                   or s["live_rowid"] < s.get("observed_max_rowid", 0)
                   or s.get("last_reconciled_at", 0) < now - 86400 for s in streams)

    def commit_page(self, ref, generation, table, group, rows, mode, checkpoint, complete=False):
        if self.fail_commit:
            raise RuntimeError("simulated failure before transaction commit")
        stream = self.streams[ref, generation, table]
        for raw in rows:
            self.records[(raw["group_id"], raw["server_id"] or str(raw["row_id"]))] = raw
        stream[mode + "_rowid"] = checkpoint
        if mode == "backfill" and complete:
            stream["backfill_done"] = True
            stream["last_reconciled_at"] = time.time()
        if mode == "reconcile" and complete:
            stream.update(reconcile_active=False, last_reconciled_at=100000000000)
        if rows:
            stream["boundary_rowid"] = rows[-1]["row_id"]
            stream["boundary_hash"] = source_row_hash(rows[-1])
        self.commits.append((mode, [r["row_id"] for r in rows]))
        return {}

    def rotate_source(self, ref, reason):
        self.sources[ref]["generation"] += 1
        return dict(self.sources[ref])

    def get_origin_info(self, ref, generation, table, row_id):
        return next((r for r in self.records.values() if r["row_id"] == row_id), None)

    def start_reconcile(self, ref, generation, table, group, maximum):
        self.streams[ref, generation, table].update(
            reconcile_active=True, reconcile_rowid=0, reconcile_end=maximum)

    def set_source_status(self, ref, available, error=None, fingerprint=None):
        self.sources[ref].update(available=available, error=error)
        if fingerprint is not None:
            self.sources[ref]["fingerprint"] = fingerprint

    def mark_missing_sources(self, account_id, seen_refs):
        for ref, source in self.sources.items():
            if ref not in seen_refs:
                source["available"] = False

    def set_collector_status(self, **fields):
        self.status.update(fields)


@pytest.fixture
def environment(tmp_path, monkeypatch):
    root = tmp_path / "xwechat_files"
    account = root / "wxid_example"
    database = account / "db_storage/message/message_0.db"
    database.parent.mkdir(parents=True)
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE Name2Id(user_name TEXT)")
        connection.executemany("INSERT INTO Name2Id VALUES (?)", [(GROUP,), ("wxid_sender",)])
        connection.execute(f'''CREATE TABLE "{TABLE}"(local_type INTEGER, create_time INTEGER,
            real_sender_id INTEGER, message_content BLOB, server_id TEXT, packed_info_data BLOB)''')
        connection.executemany(f'INSERT INTO "{TABLE}" VALUES(?,?,?,?,?,?)', [
            (1, 100, 2, b"one", "101", None),
            (1, 100, 2, b"two", "102", None),
            (12345, 90, 2, b"unknown raw", "103", b"rawpacked"),
        ])
    flags = {"unlocked": True, "decryptions": 0}

    class Reader:
        def __init__(self, data_root, account_id, allow_live_key_extract):
            assert allow_live_key_extract is False
            self.enc_keys = {database: b"a" * 32} if flags["unlocked"] else {}

        def initialize(self):
            return True

        def _load_contacts(self):
            return SimpleNamespace(_wxid_to_display={GROUP: "测试", "wxid_sender": "成员"})

    def decrypt(source, key, destination):
        flags["decryptions"] += 1
        shutil.copyfile(source, destination)
        return True

    monkeypatch.setattr("chatlog_keeper.collector.wechat_db.WeChatDBReader", Reader)
    monkeypatch.setattr("chatlog_keeper.collector.wechat_db._decrypt_db_v4", decrypt)
    store = Store(tmp_path / "state")
    collector = Collector(store, data_root=root, page_size=2)
    collector._known_streams = lambda source: {
        table: stream["group_id"] for (ref, generation, table), stream in store.streams.items()
        if ref == source["source_ref"] and generation == source["generation"]
    }
    yield collector, store, database, flags
    collector.close()


def test_backfill_and_live_are_separate_and_old_timestamp_insert_is_not_lost(environment):
    collector, store, database, flags = environment
    collector.collect_once()
    assert set(store.records) == {(GROUP, "101"), (GROUP, "102")}
    with sqlite3.connect(database) as c:
        c.execute(f'INSERT INTO "{TABLE}" VALUES(?,?,?,?,?,?)', (1, 1, 2, b"late old time", "104", None))
    collector.collect_once()
    assert (GROUP, "104") in store.records
    assert (GROUP, "103") in store.records
    assert store.records[GROUP, "103"]["parsed"]["parse_status"] == "unsupported"
    assert store.records[GROUP, "103"]["message_content"] == b"unknown raw"
    assert store.commits[:3] == [("backfill", [1, 2]), ("live", [4]), ("backfill", [3])]


def test_unchanged_source_is_not_redecrypted_per_group_or_page(environment):
    collector, store, database, flags = environment
    collector.collect_once()
    pending = next(iter(collector._snapshots.values())).directory
    assert pending.exists()
    collector.collect_once()
    assert not pending.exists()
    assert not collector._snapshots
    collector.collect_once()
    assert flags["decryptions"] == 1
    assert len(store.records) == 3


def test_complete_source_discards_plaintext_and_skips_unchanged_scan_but_retries_media(environment):
    collector, store, database, flags = environment
    collector.page_size = 500
    calls = []
    collector.after_cycle = lambda *_: calls.append(True)
    collector.collect_once()
    assert not collector._snapshots
    collector.collect_once()
    assert not collector._snapshots
    assert flags["decryptions"] == 1
    assert len(calls) == 2


def test_pending_reconcile_reuses_snapshot_then_removes_it(environment):
    collector, store, database, flags = environment
    collector.page_size = 500
    collector.collect_once()
    collector.page_size = 2
    for stream in store.streams.values():
        stream["last_reconciled_at"] = 0
    collector.collect_once()
    pending = next(iter(collector._snapshots.values())).directory
    assert pending.exists()
    collector.collect_once()
    assert not pending.exists()
    assert not collector._snapshots
    assert flags["decryptions"] == 2


def test_close_removes_snapshot_for_unfinished_backfill(environment):
    collector, store, database, flags = environment
    collector.collect_once()
    directory = next(iter(collector._snapshots.values())).directory
    assert directory.exists()
    collector.close()
    collector.close()
    assert not directory.exists()
    assert not collector._snapshots


def test_missing_account_discards_pending_plaintext_and_marks_source_missing(environment):
    collector, store, database, flags = environment
    collector.collect_once()
    directory = next(iter(collector._snapshots.values())).directory
    assert directory.exists()
    shutil.rmtree(collector.account_dir)
    with pytest.raises(CollectorError, match="account_not_found"):
        collector.collect_once()
    assert not directory.exists()
    assert not collector._snapshots
    assert next(iter(store.sources.values()))["available"] is False


def test_failed_commit_replays_same_source_page_without_advancing(environment):
    collector, store, database, flags = environment
    store.fail_commit = True
    result = collector.collect_once()
    assert result["errors"]
    assert not store.records
    assert next(iter(store.streams.values()))["backfill_rowid"] == 0
    store.fail_commit = False
    collector.collect_once()
    assert set(store.records) == {(GROUP, "101"), (GROUP, "102")}


def test_missing_key_retains_source_and_does_not_start_live_extraction(environment):
    collector, store, database, flags = environment
    flags["unlocked"] = False
    result = collector.collect_once()
    assert result["errors"][0]["error"] == "cached_database_key_unavailable"
    assert not store.records
    assert flags["decryptions"] == 0
    assert next(iter(store.sources.values()))["available"] is False


def test_rowid_regression_rotates_source_and_replays_history(environment):
    collector, store, database, flags = environment
    collector.page_size = 500
    collector.collect_once()
    with sqlite3.connect(database) as c:
        c.execute(f'DELETE FROM "{TABLE}" WHERE rowid=3')
    collector.collect_once()
    assert next(iter(store.sources.values()))["generation"] == 2


def test_boundary_server_ack_does_not_rotate_source_generation(environment):
    collector, store, database, flags = environment
    collector.page_size = 500
    with sqlite3.connect(database) as c:
        c.execute(f'UPDATE "{TABLE}" SET server_id=NULL WHERE rowid=3')
    collector.collect_once()
    with sqlite3.connect(database) as c:
        c.execute(f'UPDATE "{TABLE}" SET server_id=? WHERE rowid=3', ("103",))
    collector.collect_once()
    assert next(iter(store.sources.values()))["generation"] == 1
    assert (GROUP, "103") in store.records  # Archive is not deleted with the source.


def test_changed_boundary_and_daily_reconcile_update_existing_generation(environment):
    collector, store, database, flags = environment
    collector.page_size = 500
    collector.collect_once()
    for stream in store.streams.values():
        stream["last_reconciled_at"] = 0
    with sqlite3.connect(database) as c:
        c.execute(f'UPDATE "{TABLE}" SET message_content=? WHERE rowid=1', (b"edited",))
    collector.collect_once()
    assert next(iter(store.sources.values()))["generation"] == 1
    assert store.records[GROUP, "101"]["parsed"]["text"] == "edited"
    with sqlite3.connect(database) as c:
        c.execute(f'UPDATE "{TABLE}" SET message_content=? WHERE rowid=3', (b"boundary edited",))
    collector.collect_once()
    assert next(iter(store.sources.values()))["generation"] == 1


@pytest.mark.parametrize("new_server_id", ["different", None])
def test_reused_boundary_row_with_different_server_id_rotates(environment, new_server_id):
    collector, store, database, flags = environment
    collector.page_size = 500
    collector.collect_once()
    with sqlite3.connect(database) as c:
        c.execute(f'UPDATE "{TABLE}" SET server_id=? WHERE rowid=3', (new_server_id,))
    collector.collect_once()
    assert next(iter(store.sources.values()))["generation"] == 2


def test_media_callback_runs_after_committed_text_and_when_source_unchanged(environment):
    collector, store, database, flags = environment
    seen = []
    collector.after_cycle = lambda owner, summary: seen.append(len(store.records))
    collector.collect_once()
    collector.collect_once()
    assert seen == [2, 3]


def test_single_instance_lock_rejects_second_collector(environment):
    collector, store, database, flags = environment
    other = Collector(store, data_root=collector.data_root)
    with collector._single_instance():
        store.set_collector_status(running=True)
        with pytest.raises(CollectorError, match="collector_already_running"):
            other.collect_once()
        other.close()
        assert store.status["running"] is True


def test_known_private_chat_is_excluded_and_unknown_table_marks_coverage_gap(environment):
    collector, store, database, flags = environment
    dm = "wxid_private"
    dm_table = "Msg_" + hashlib.md5(dm.encode()).hexdigest()
    unknown_table = "Msg_" + "a" * 32
    with sqlite3.connect(database) as c:
        c.execute("INSERT INTO Name2Id VALUES (?)", (dm,))
        c.execute(f'CREATE TABLE "{dm_table}" AS SELECT * FROM "{TABLE}"')
        c.execute(f'CREATE TABLE "{unknown_table}" AS SELECT * FROM "{TABLE}"')
    summary = collector.collect_once()
    assert summary["groups"] == 1
    assert summary["unresolved_tables"] == 1
    assert all(r["group_id"] == GROUP for r in store.records.values())
    assert next(iter(store.sources.values()))["error"] == "unresolved_conversation_tables"


def test_close_is_idempotent_even_when_no_cycle_has_run(environment):
    collector, store, database, flags = environment
    assert collector._status_owned is False
    collector.close()
    collector.close()
    assert not collector._snapshots
    collector.collect_once()
    collector.close()
    collector.close()
    assert store.status["running"] is False


def test_real_archive_commits_unknown_rows_and_late_insert_without_duplicates(environment, tmp_path):
    from chatlog_keeper.archive import ArchiveStore
    fixture, _, database, flags = environment
    with ArchiveStore(tmp_path / "real-state") as store:
        collector = Collector(store, data_root=fixture.data_root, page_size=2)
        try:
            assert not collector.collect_once()["errors"]
            with sqlite3.connect(database) as c:
                c.execute(f'INSERT INTO "{TABLE}" VALUES(?,?,?,?,?,?)',
                          (1, 1, 2, b"late old time", "104", None))
            assert not collector.collect_once()["errors"]
            assert not collector.collect_once()["errors"]
            assert store.conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 4
            unknown = store.conn.execute("SELECT raw_content FROM origins WHERE raw_type=12345").fetchone()
            assert bytes(unknown[0]) == b"unknown raw"
            stream = store.conn.execute("SELECT live_rowid,backfill_done FROM streams").fetchone()
            assert tuple(stream) == (4, 1)
        finally:
            collector.close()


def test_real_archive_skipped_source_preserves_last_scan_time(environment, tmp_path):
    from chatlog_keeper.archive import ArchiveStore
    fixture, _, database, flags = environment
    with ArchiveStore(tmp_path / "real-state") as store:
        collector = Collector(store, data_root=fixture.data_root, page_size=500)
        try:
            assert not collector.collect_once()["errors"]
            before = tuple(store.conn.execute("SELECT last_success,updated_at FROM sources").fetchone())
            assert not collector._snapshots
            assert not collector.collect_once()["errors"]
            after = tuple(store.conn.execute("SELECT last_success,updated_at FROM sources").fetchone())
            assert before == after
            assert flags["decryptions"] == 1
        finally:
            collector.close()


def test_real_source_replacement_with_same_family_fingerprint_never_reuses_old_snapshot(
    environment, tmp_path, monkeypatch,
):
    from chatlog_keeper.archive import ArchiveStore
    fixture, _, database, flags = environment
    with ArchiveStore(tmp_path / "real-state") as store:
        collector = Collector(store, data_root=fixture.data_root, page_size=2)
        # Models a restored DB preserving size/mtime and bounded header/tail bytes.
        monkeypatch.setattr(collector, "_fingerprint", lambda _: "same-family-fingerprint")
        try:
            assert not collector.collect_once()["errors"]
            original = next(iter(collector._snapshots.values())).directory
            replacement = tmp_path / "replacement.sqlite"
            shutil.copy2(database, replacement)
            with sqlite3.connect(replacement) as c:
                c.execute(f'UPDATE "{TABLE}" SET local_type=1,message_content=? WHERE rowid=3',
                          (b"replacement row",))
            replacement.replace(database)
            assert not collector.collect_once()["errors"]
            assert not original.exists()
            assert not collector.collect_once()["errors"]
            assert store.conn.execute("SELECT generation FROM sources").fetchone()[0] == 2
            assert store.conn.execute("SELECT text FROM messages WHERE server_id='103'").fetchone()[0] == "replacement row"
            assert flags["decryptions"] == 2
            assert not collector._snapshots
        finally:
            collector.close()


def test_real_missing_unfinished_group_table_rotates_and_releases_snapshot(environment, tmp_path):
    from chatlog_keeper.archive import ArchiveStore
    fixture, _, database, flags = environment
    second_group = "second@chatroom"
    second_table = "Msg_" + hashlib.md5(second_group.encode()).hexdigest()
    with sqlite3.connect(database) as c:
        c.execute("INSERT INTO Name2Id VALUES (?)", (second_group,))
        c.execute(f'CREATE TABLE "{second_table}" AS SELECT * FROM "{TABLE}" WHERE rowid=1')
    with ArchiveStore(tmp_path / "real-state") as store:
        collector = Collector(store, data_root=fixture.data_root, page_size=1)
        try:
            assert not collector.collect_once()["errors"]
            original = next(iter(collector._snapshots.values())).directory
            assert original.exists()
            assert store.conn.execute("SELECT backfill_done FROM streams WHERE table_name=?", (TABLE,)).fetchone()[0] == 0
            with sqlite3.connect(database) as c:
                c.execute(f'DROP TABLE "{TABLE}"')
            assert not collector.collect_once()["errors"]
            assert store.conn.execute("SELECT generation FROM sources").fetchone()[0] == 2
            assert store.conn.execute("SELECT count(*) FROM messages WHERE group_id=?", (GROUP,)).fetchone()[0] == 1
            assert store.conn.execute("SELECT count(*) FROM messages WHERE group_id=?", (second_group,)).fetchone()[0] == 1
            source = store.conn.execute("SELECT source_ref,generation FROM sources").fetchone()
            assert not store.source_needs_work(source[0], source[1])
            assert not original.exists()
            assert not collector._snapshots
            assert "source_tables_removed" in store.conn.execute(
                "SELECT value FROM meta WHERE key LIKE 'source_reset:%'").fetchone()[0]
        finally:
            collector.close()


def test_existing_group_table_uses_verified_archive_mapping_when_directory_entry_missing(environment):
    collector, store, database, flags = environment
    collector.collect_once()
    with sqlite3.connect(database) as c:
        c.execute("DELETE FROM Name2Id WHERE user_name=?", (GROUP,))
    collector._contact_names = {}
    summary = collector.collect_once()
    assert not summary["errors"]
    assert summary["unresolved_tables"] == 0
    assert next(iter(store.sources.values()))["generation"] == 1
    assert (GROUP, "103") in store.records
    assert not collector._snapshots


def test_real_archive_rollback_then_restart_replays_uncommitted_page(environment, tmp_path):
    from chatlog_keeper.archive import ArchiveStore
    fixture, _, database, flags = environment
    with ArchiveStore(tmp_path / "real-state") as store:
        collector = Collector(store, data_root=fixture.data_root, page_size=500)
        try:
            store.conn.execute('''CREATE TRIGGER fail_checkpoint BEFORE UPDATE OF backfill_rowid ON streams
                                  BEGIN SELECT RAISE(ABORT,'synthetic checkpoint failure'); END''')
            store.conn.commit()
            assert collector.collect_once()["errors"]
            assert store.conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 0
            assert store.conn.execute("SELECT backfill_rowid FROM streams").fetchone()[0] == 0
            store.conn.execute("DROP TRIGGER fail_checkpoint")
            store.conn.commit()
            assert not collector.collect_once()["errors"]
            assert store.conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 3
            assert store.conn.execute("SELECT backfill_done FROM streams").fetchone()[0] == 1
        finally:
            collector.close()


def test_real_archive_server_ack_preserves_ref_and_count(environment, tmp_path):
    from chatlog_keeper.archive import ArchiveStore
    fixture, _, database, flags = environment
    with sqlite3.connect(database) as c:
        c.execute(f'UPDATE "{TABLE}" SET server_id=NULL WHERE rowid=3')
    with ArchiveStore(tmp_path / "real-state") as store:
        collector = Collector(store, data_root=fixture.data_root, page_size=500)
        try:
            assert not collector.collect_once()["errors"]
            before = store.conn.execute("SELECT message_ref FROM messages WHERE server_id=''").fetchone()[0]
            with sqlite3.connect(database) as c:
                c.execute(f'UPDATE "{TABLE}" SET server_id=? WHERE rowid=3', ("103",))
            assert not collector.collect_once()["errors"]
            assert store.conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 3
            assert store.conn.execute("SELECT generation FROM sources").fetchone()[0] == 1
            assert store.conn.execute("SELECT message_ref FROM messages WHERE server_id='103'").fetchone()[0] == before
        finally:
            collector.close()


@pytest.mark.parametrize("new_server_id", ["999", None])
def test_real_reconcile_row_reuse_preserves_old_message_and_retries_new_generation(
    environment, tmp_path, new_server_id,
):
    from chatlog_keeper.archive import ArchiveStore
    fixture, _, database, flags = environment
    with ArchiveStore(tmp_path / "real-state") as store:
        collector = Collector(store, data_root=fixture.data_root, page_size=500)
        try:
            assert not collector.collect_once()["errors"]
            original = store.conn.execute(
                "SELECT message_ref,text FROM messages WHERE server_id='101'").fetchone()
            old_origin = store.conn.execute(
                "SELECT raw_content FROM origins WHERE row_id=1").fetchone()[0]
            # Row 3 remains unchanged, so source-boundary validation cannot see it.
            with sqlite3.connect(database) as c:
                c.execute(f'UPDATE "{TABLE}" SET server_id=?,message_content=? WHERE rowid=1',
                          (new_server_id, b"reused low row"))
            store.conn.execute("UPDATE streams SET last_reconciled=0")
            store.conn.commit()
            assert not collector.collect_once()["errors"]
            assert store.conn.execute("SELECT generation FROM sources").fetchone()[0] == 2
            assert store.conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 4
            preserved = store.conn.execute(
                "SELECT message_ref,text FROM messages WHERE server_id='101'").fetchone()
            assert tuple(preserved) == tuple(original)
            assert bytes(store.conn.execute(
                "SELECT raw_content FROM origins WHERE generation=1 AND row_id=1").fetchone()[0]) == bytes(old_origin)
            replacement = store.conn.execute(
                "SELECT text FROM messages WHERE message_ref<>? AND text='reused low row'",
                (original[0],)).fetchone()
            assert replacement is not None
            # An unchanged follow-up round does not keep rotating or duplicate again.
            assert not collector.collect_once()["errors"]
            assert store.conn.execute("SELECT generation FROM sources").fetchone()[0] == 2
            assert store.conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 4
        finally:
            collector.close()


def test_repeated_row_reuse_error_has_bounded_retry(environment, monkeypatch):
    from chatlog_keeper.archive import ArchiveError
    collector, store, database, flags = environment
    calls = []

    def reject(*args, **kwargs):
        calls.append(True)
        raise ArchiveError("source_rowid_reused")

    monkeypatch.setattr(store, "commit_page", reject)
    result = collector.collect_once()
    assert len(calls) == 2
    assert next(iter(store.sources.values()))["generation"] == 2
    assert result["errors"][0]["error"] == "source_rowid_reused"


def test_logged_out_client_is_checked_even_when_cached_sources_archive_successfully(environment):
    collector, store, _, flags = environment
    observed = []

    def monitor(current):
        observed.append(current.account_id)
        return {"state": "login_required", "reason": "official_login_window", "checked_at": 123}

    collector.health_monitor = monitor
    for _ in range(3):
        result = collector.collect_once()
        assert result["errors"] == []
        assert result["client_health"]["state"] == "login_required"
        assert store.status["last_error"] is None
        assert store.status["health_monitor_error"] is None
    assert len(observed) == 3
    assert all(account == "wxid_example" for account in observed)
    assert flags["decryptions"] == 1
    assert store.status["last_cycle_checked_at"] >= store.status["last_cycle_finished_at"]


def test_health_check_runs_after_reader_failure_without_overwriting_source_error(environment, monkeypatch):
    collector, store, _, _ = environment
    calls = []

    def unavailable():
        raise CollectorError("account_not_found")

    def monitor(current):
        calls.append(current)
        return {"state": "not_running", "reason": "official_process_absent", "checked_at": 123}

    monkeypatch.setattr(collector, "_reader", unavailable)
    collector.health_monitor = monitor
    with pytest.raises(CollectorError, match="account_not_found"):
        collector.collect_once()
    assert calls == [collector]
    assert store.status["last_error"] == "account_not_found"
    assert store.status["client_health"]["state"] == "not_running"
    assert store.status["last_cycle_checked_at"]
    assert "last_success_at" not in store.status


def test_monitor_failure_does_not_stop_archiving_or_publish_exception_details(environment, caplog):
    collector, store, _, _ = environment

    def broken(_current):
        raise RuntimeError("sensitive client path and credentials")

    collector.health_monitor = broken
    result = collector.collect_once()
    assert result["errors"] == []
    assert len(store.records) == 2
    assert store.status["client_health"]["state"] == "unknown"
    assert store.status["client_health"]["reason"] == "health_monitor_failed"
    assert store.status["health_monitor_error"] == "RuntimeError"
    assert "sensitive" not in caplog.text


def test_monitor_and_status_write_failures_preserve_original_collection_exception(environment, monkeypatch):
    collector, store, _, _ = environment
    original = OSError("original source failure")

    def fail_collect():
        raise original

    def fail_monitor(_collector):
        raise RuntimeError("monitor unavailable")

    def fail_status(**_fields):
        raise RuntimeError("status storage unavailable")

    monkeypatch.setattr(collector, "_collect", fail_collect)
    monkeypatch.setattr(store, "set_collector_status", fail_status)
    collector.health_monitor = fail_monitor
    with pytest.raises(OSError) as caught:
        collector.collect_once()
    assert caught.value is original


def test_second_collector_cannot_probe_or_change_client_health(environment):
    collector, store, _, _ = environment
    calls = []
    other = Collector(store, health_monitor=lambda _: calls.append(True))
    with collector._single_instance():
        with pytest.raises(CollectorError, match="collector_already_running"):
            other.collect_once()
    assert calls == []
    assert "client_health" not in store.status
