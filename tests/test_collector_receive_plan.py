"""Snapshot-local read plans and best-effort dirty-shard receive hints."""
from contextlib import contextmanager
import hashlib
from pathlib import Path
import shutil
import sqlite3
import time

import pytest

from chatlog_keeper import wechat_db
from chatlog_keeper.core._native_watch import NativeFileWatcher
from chatlog_keeper.collector import CollectorError
from test_collector import environment, GROUP, TABLE
from test_collector_source_batch import real_collector


def insert(database, sid, table=TABLE):
    with sqlite3.connect(database) as connection:
        connection.execute(f'INSERT INTO "{table}" VALUES(1,200,2,?,?,NULL)', (sid.encode(), sid))


def second_shard(database, monkeypatch):
    path = database.with_name('message_1.db')
    shutil.copyfile(database, path)
    original = wechat_db.WeChatDBReader
    class Reader(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.enc_keys[path] = b'a' * 32
    monkeypatch.setattr(wechat_db, 'WeChatDBReader', Reader)
    return path


def test_snapshot_plan_reuses_table_schema_maximum_and_live_stream(environment, monkeypatch):
    collector, store, database, _ = environment
    collector.bot_mode, collector.page_size = True, 500
    other = 'quiet@chatroom'
    other_table = 'Msg_' + hashlib.md5(other.encode()).hexdigest()
    with sqlite3.connect(database) as connection:
        connection.execute('INSERT INTO Name2Id VALUES (?)', (other,))
        connection.execute(f'CREATE TABLE "{other_table}" AS SELECT * FROM "{TABLE}"')
    collector.collect_once()
    collector.collect_once()  # Finish the second table's separate history page.
    insert(database, 'fresh-plan')
    statements, stream_reads = [], []
    original_connection = collector._snapshot_connection
    original_stream, original_commit = store.get_stream, collector._commit_live_source
    active = [False]
    @contextmanager
    def traced(path):
        with original_connection(path) as connection:
            connection.set_trace_callback(statements.append)
            yield connection
    def stream(*args):
        if active[0]:
            stream_reads.append(args[2])
        return original_stream(*args)
    def commit(*args):
        active[0] = True
        try:
            return original_commit(*args)
        finally:
            active[0] = False
    monkeypatch.setattr(collector, '_snapshot_connection', traced)
    monkeypatch.setattr(store, 'get_stream', stream)
    monkeypatch.setattr(collector, '_commit_live_source', commit)
    result = collector.collect_once()
    assert not result['errors'] and (GROUP, 'fresh-plan') in store.records
    assert sorted(stream_reads) == sorted([TABLE, other_table])
    assert sum('FROM SQLITE_MASTER' in sql.upper() for sql in statements) == 1
    for table in (TABLE, other_table):
        assert sum('MAX(ROWID)' in sql.upper() and table in sql for sql in statements) == 1
        assert sum('PRAGMA TABLE_INFO' in sql.upper() and table in sql for sql in statements) == 1
    # The quiet table is still boundary-validated, but not empty-polled again.
    assert any('ROWID = ' in sql.upper() and other_table in sql for sql in statements)
    assert not any('ROWID > ' in sql.upper() and other_table in sql for sql in statements)
    assert not collector._connection_plans


def test_snapshot_metadata_is_not_reused_after_source_schema_or_max_changes(environment):
    collector, store, database, _ = environment
    collector.bot_mode = True
    collector.collect_once()
    old = collector._snapshots[database]
    assert old.plan['maxima'][TABLE] == 3
    with sqlite3.connect(database) as connection:
        connection.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN message_source TEXT')
        connection.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"new-schema",NULL,?)',
                           (b'hello', '<msgsource><atuserlist>wxid_target</atuserlist></msgsource>'))
    result = collector.collect_once()
    assert not result['errors']
    current = collector._snapshots.get(database)
    if current is not None:
        assert current.plan is not old.plan and current.plan['maxima'][TABLE] == 4
        assert 'message_source' in current.plan['columns'][TABLE]
    assert not old.directory.exists()
    assert store.records[GROUP, 'new-schema']['parsed']['mentions'] == ['wxid_target']


def test_dirty_shard_scan_preserves_other_source_and_half_second_full_fence(environment, monkeypatch):
    collector, store, database, _ = environment
    other = second_shard(database, monkeypatch)
    collector.bot_mode, collector.page_size = True, 500
    collector.collect_once()
    registrations = []
    original = store.register_source
    monkeypatch.setattr(store, 'register_source', lambda *args: (registrations.append(args[1]), original(*args))[1])
    insert(database, 'hinted-shard')
    insert(other, 'missed-hint')
    collector._dirty_shards = {database}
    collector._last_signature_check = time.monotonic()
    collector._fast_signature = None
    result = collector.collect_once()
    assert result['partial_scan'] is True
    assert registrations == ['db_storage/message/message_0.db']
    assert (GROUP, 'hinted-shard') in store.records and (GROUP, 'missed-hint') not in store.records
    assert all(source['available'] for source in store.sources.values())
    # Even with another known hint, the complete family fence becomes due.
    registrations.clear()
    collector._dirty_shards = {database}
    collector._last_signature_check = time.monotonic() - .5
    result = collector.collect_once()
    assert result['partial_scan'] is False and len(registrations) == 2
    assert (GROUP, 'missed-hint') in store.records


def test_dirty_scan_continues_other_shards_live_backlog(environment, monkeypatch):
    collector, store, database, _ = environment
    other = second_shard(database, monkeypatch)
    collector.bot_mode, collector.page_size = True, 500
    collector.collect_once()
    collector.page_size = 2
    for index in range(5):
        insert(database, 'first-' + str(index))
        insert(other, 'second-' + str(index))
    collector.collect_once()
    assert collector._live_shards == {database, other}
    collector._dirty_shards = {database}
    collector._last_signature_check = time.monotonic()
    result = collector.collect_once()
    assert result['partial_scan'] and result['live_pending']
    assert (GROUP, 'second-3') in store.records
    assert {page['source_ref'] for page in result['live_batches']} == set(store.sources)


def test_successful_partial_backlog_scan_cannot_hide_another_source_failure(environment, monkeypatch):
    collector, store, database, _ = environment
    other = second_shard(database, monkeypatch)
    collector.bot_mode, collector.page_size = True, 500
    collector.collect_once()
    collector.collect_once()
    collector.page_size = 2
    for index in range(5):
        insert(database, 'backlog-' + str(index))
    insert(other, 'retry-failed-source')
    original = collector._get_snapshot
    failing = [True]
    def snapshot(path, *args):
        if path == other and failing[0]:
            raise CollectorError('synthetic_snapshot_unavailable')
        return original(path, *args)
    monkeypatch.setattr(collector, '_get_snapshot', snapshot)
    failed = collector.collect_once()
    assert failed['errors'] and failed['live_pending']
    assert collector._last_full_summary['errors']
    # Two immediate, no-new-hint passes drain only the healthy shard. Their
    # success cannot authorize the five-second idle shortcut for the whole DB.
    assert collector.collect_once()['partial_scan']
    assert collector.collect_once()['partial_scan']
    assert not collector._live_pending and not collector._last_summary['errors']
    failing[0] = False
    result = collector.collect_once()
    assert not result.get('idle') and not result['partial_scan']
    assert not result['errors'] and (GROUP, 'retry-failed-source') in store.records


def test_reader_rebuild_upgrades_a_narrow_hint_to_a_full_source_scan(environment, monkeypatch, tmp_path):
    collector, store, database, _ = environment
    other = second_shard(database, monkeypatch)
    cache = tmp_path / 'fake-key-cache'
    cache.write_bytes(b'metadata-v1')
    monkeypatch.setattr(wechat_db, '_wechat_account_key_cache_path', lambda _: cache)
    collector.bot_mode, collector.page_size = True, 500
    collector.collect_once()
    insert(other, 'key-fence-full-scan')
    cache.write_bytes(b'metadata-v2')
    collector._dirty_shards = {database}
    collector._last_signature_check = time.monotonic()
    result = collector.collect_once()
    assert not result['partial_scan'] and (GROUP, 'key-fence-full-scan') in store.records


def test_source_timings_are_bound_to_durable_live_pages(environment, tmp_path, monkeypatch):
    collector, store, database = real_collector(environment, tmp_path)
    try:
        collector.priority_groups = (GROUP,)
        insert(database, 'timed-real-commit')
        original = collector._get_snapshot
        def snapshot(*args):
            # Synthetic decryption wrapper exposes the same two measured
            # subphases as the real authenticated decrypter.
            started = time.perf_counter()
            result = original(*args)
            collector._add_timing('snapshot', .001)
            collector._add_timing('decrypt', .002)
            assert time.perf_counter() >= started
            return result
        monkeypatch.setattr(collector, '_get_snapshot', snapshot)
        result = collector.collect_once()
        page, = result['live_batches']
        trace = page['source_timings']
        assert {'source_registration', 'snapshot_and_decrypt', 'snapshot', 'decrypt', 'schema_discovery',
                'boundary_validation', 'priority_live_apply', 'source_status', 'live_commit', 'archive_writer_wait'} <= set(trace)
        assert trace['live_commit'] == page['commit_seconds']
        assert page['collection_started_at'] <= page['source_started_at'] <= page['commit_at'] <= page['source_ready_at']
        assert page['collection_to_source_seconds'] >= 0
        assert all(isinstance(value, (float, int)) and value >= 0 for value in trace.values())
        assert not collector._connection_plans and collector._source_timings is None
    finally:
        collector.close()
        store.close()


@pytest.mark.parametrize('name,mask,full', [
    (b'message_3.db-wal', 2, False),
    (b'message_3.db-shm', 2, False),
    (b'message_3.db', 2, False),
    (b'message_3.db', 0x80, True),
    (b'../message_3.db-wal', 2, True),
    (b'unknown.db', 2, True),
    (b'', 2, True),
    (b'message_3.db-wal', NativeFileWatcher.ISDIR, True),
])
def test_only_recognized_native_family_hints_can_narrow_work(tmp_path, name, mask, full):
    watcher = object.__new__(NativeFileWatcher)
    directory = tmp_path / 'db_storage/message'
    watcher._watches, watcher.dirty_paths, watcher.rescan_all = {1: (directory, (1, 2))}, set(), False
    watcher._record_hint(1, mask, name)
    assert watcher.rescan_all is full
    if not full:
        assert watcher.dirty_paths == {directory / 'message_3.db'}


def test_native_wal_dirty_path_is_exposed_and_replacement_requests_full_fences(tmp_path):
    directory = tmp_path / 'account/db_storage/message'
    directory.mkdir(parents=True)
    watcher = NativeFileWatcher()
    if not watcher.available:
        pytest.skip('native inotify unavailable')
    try:
        watcher.configure(tmp_path / 'account')
        (directory / 'message_0.db-wal').write_bytes(b'fake WAL hint')
        assert watcher.wait(.2)
        assert watcher.dirty_paths == {directory / 'message_0.db'} and not watcher.rescan_all
        (directory / 'message_0.db').write_bytes(b'new main identity')
        assert watcher.wait(.2) and watcher.rescan_all
        assert not watcher.wait(0) and not watcher.dirty_paths
    finally:
        watcher.close()
