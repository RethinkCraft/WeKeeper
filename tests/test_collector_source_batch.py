"""Durability/rollback boundaries of the schema-compatible source batch."""
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from chatlog_keeper.archive import ArchiveError, ArchiveStore
from chatlog_keeper.collector import Collector
from chatlog_keeper.core._store_batch import StoreBatchAborted, source_batch
from test_collector import environment, TABLE


def store_for(path, *, factory=sqlite3.Connection):
    connection = sqlite3.connect(path, factory=factory, check_same_thread=False)
    connection.execute('PRAGMA journal_mode=WAL')
    connection.execute('PRAGMA synchronous=FULL')
    connection.execute('CREATE TABLE data(value TEXT)')
    connection.commit()
    return SimpleNamespace(conn=connection, _lock=threading.RLock())


def test_nested_legacy_contexts_publish_once_and_only_after_outer_commit(tmp_path):
    path = tmp_path / 'archive.sqlite'
    store = store_for(path)
    raw = store.conn
    trace = []
    raw.set_trace_callback(trace.append)
    with sqlite3.connect(path) as observer:
        with source_batch(store, enabled=True) as batch:
            with store.conn:
                store.conn.execute('INSERT INTO data VALUES("message")')
            with store.conn:
                store.conn.execute('INSERT INTO data VALUES("cursor-and-source-ready")')
            assert observer.execute('SELECT COUNT(*) FROM data').fetchone()[0] == 0
            assert batch.committed_at is None
        assert observer.execute('SELECT COUNT(*) FROM data').fetchone()[0] == 2
    assert store.conn is raw and not raw.in_transaction
    assert trace.count('COMMIT') == 1 and batch.committed_at is not None
    assert raw.execute('PRAGMA synchronous').fetchone()[0] == 2
    raw.close()


def test_caught_nested_error_still_rolls_back_every_prior_page(tmp_path):
    store = store_for(tmp_path / 'archive.sqlite')
    raw = store.conn
    with pytest.raises(StoreBatchAborted, match='nested_operation_failed'):
        with source_batch(store, enabled=True):
            with store.conn:
                store.conn.execute('INSERT INTO data VALUES("earlier-live-page")')
            try:
                with store.conn:
                    store.conn.execute('INSERT INTO data VALUES("broken-later-page")')
                    raise ValueError('fixture-page-rejected')
            except ValueError:
                pass
    assert store.conn is raw and not raw.in_transaction
    assert raw.execute('SELECT COUNT(*) FROM data').fetchone()[0] == 0
    raw.close()


def test_commit_failure_rolls_back_and_restores_the_real_connection(tmp_path):
    class Connection(sqlite3.Connection):
        fail = False
        def commit(self):
            if self.fail:
                self.fail = False
                raise sqlite3.OperationalError('fixture-durable-commit-failed')
            return super().commit()
    store = store_for(tmp_path / 'archive.sqlite', factory=Connection)
    raw = store.conn
    raw.fail = True
    with pytest.raises(sqlite3.OperationalError, match='durable-commit-failed'):
        with source_batch(store, enabled=True) as batch:
            with store.conn:
                store.conn.execute('INSERT INTO data VALUES("uncommitted")')
    assert store.conn is raw and not raw.in_transaction
    assert raw.execute('SELECT COUNT(*) FROM data').fetchone()[0] == 0
    assert batch.committed_at is None
    raw.close()


def test_store_lock_prevents_other_threads_observing_the_proxy(tmp_path):
    store = store_for(tmp_path / 'archive.sqlite')
    raw = store.conn
    waiting, acquired = threading.Event(), threading.Event()
    seen = []
    def other_thread():
        waiting.set()
        with store._lock:
            seen.append(store.conn)
            acquired.set()
    with source_batch(store, enabled=True):
        thread = threading.Thread(target=other_thread)
        thread.start()
        assert waiting.wait(1) and not acquired.wait(.02)
    thread.join(1)
    assert acquired.is_set() and seen == [raw]
    raw.close()


@pytest.mark.parametrize('boundary', ['commit', 'rollback', 'close', 'executescript'])
def test_explicit_boundaries_cannot_escape_or_unpoison_the_batch(tmp_path, boundary):
    store = store_for(tmp_path / 'archive.sqlite')
    raw = store.conn
    with pytest.raises(StoreBatchAborted, match='nested_operation_failed'):
        with source_batch(store, enabled=True):
            store.conn.execute('INSERT INTO data VALUES("pending")')
            try:
                getattr(store.conn, boundary)()
            except StoreBatchAborted:
                pass
    assert raw.execute('SELECT COUNT(*) FROM data').fetchone()[0] == 0
    raw.close()


def test_non_bot_path_keeps_ordinary_context_commit_contract(tmp_path):
    store = store_for(tmp_path / 'archive.sqlite')
    raw = store.conn
    with source_batch(store, enabled=False) as batch:
        assert store.conn is raw and not batch.active
        with store.conn:
            store.conn.execute('INSERT INTO data VALUES("legacy")')
        assert not raw.in_transaction
    assert batch.committed_at is None
    raw.close()


def real_collector(environment, tmp_path):
    fixture, _, database, _ = environment
    store = ArchiveStore(tmp_path / 'real')
    collector = Collector(store, data_root=fixture.data_root, page_size=500, bot_mode=True, poll_interval=.5)
    collector.collect_once()
    return collector, store, database


def test_live_message_cursor_and_available_source_become_visible_together(environment, tmp_path, monkeypatch):
    collector, store, database = real_collector(environment, tmp_path)
    try:
        with sqlite3.connect(database) as native:
            native.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"new-atomic",NULL)', (b'new',))
        summary = collector.collect_once()
        with ArchiveStore(store.state_dir, readonly=True) as reader:
            source = reader.conn.execute('SELECT available,error FROM sources').fetchone()
            assert tuple(source) == (1, None)
            row = reader.conn.execute('SELECT COUNT(*) FROM messages WHERE server_id="new-atomic"').fetchone()[0]
            assert row == 1
            assert reader.conn.execute('SELECT live_rowid FROM streams').fetchone()[0] == 4
        page, = summary['live_batches']
        assert page['shared_source_commit'] is True
        assert page['commit_at'] >= page['collection_started_at']
        assert page['source_ready_at'] >= page['commit_at']
        assert not summary['errors']
    finally:
        collector.close()
        store.close()


def test_recovered_source_is_unavailable_until_whole_live_batch_is_durable(environment, tmp_path, monkeypatch):
    collector, store, database = real_collector(environment, tmp_path)
    original = store.commit_page
    source = store.conn.execute('SELECT source_ref FROM sources').fetchone()[0]
    store.set_source_status(source, False, error='fixture_previous_failure')
    during = []
    try:
        with sqlite3.connect(database) as native:
            native.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"recovered-live",NULL)', (b'new',))
        def commit(*args, **kwargs):
            result = original(*args, **kwargs)
            with ArchiveStore(store.state_dir, readonly=True) as observer:
                assert observer.conn.execute('SELECT available FROM sources').fetchone()[0] == 0
                assert observer.conn.execute('SELECT COUNT(*) FROM messages WHERE server_id="recovered-live"').fetchone()[0] == 0
            during.append(True)
            return result
        monkeypatch.setattr(store, 'commit_page', commit)
        summary = collector.collect_once()
        assert during and not summary['errors']
        assert store.conn.execute('SELECT available FROM sources').fetchone()[0] == 1
        assert store.conn.execute('SELECT COUNT(*) FROM messages WHERE server_id="recovered-live"').fetchone()[0] == 1
    finally:
        collector.close()
        store.close()


def test_failed_source_batch_exposes_no_page_or_cursor_and_can_retry(environment, tmp_path, monkeypatch):
    collector, store, database = real_collector(environment, tmp_path)
    original = store.commit_page
    try:
        with sqlite3.connect(database) as native:
            native.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"rollback-live",NULL)', (b'new',))
        def rejected(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError('fixture-after-page-before-source-ready')
        monkeypatch.setattr(store, 'commit_page', rejected)
        summary = collector.collect_once()
        assert summary['errors'] and summary['live_batches'] == []
        assert store.conn.execute('SELECT COUNT(*) FROM messages WHERE server_id="rollback-live"').fetchone()[0] == 0
        assert store.conn.execute('SELECT live_rowid FROM streams').fetchone()[0] == 3
        assert store.conn.execute('SELECT available FROM sources').fetchone()[0] == 0
        assert not collector._source_batch_active
        monkeypatch.setattr(store, 'commit_page', original)
        summary = collector.collect_once()
        assert not summary['errors']
        assert store.conn.execute('SELECT COUNT(*) FROM messages WHERE server_id="rollback-live"').fetchone()[0] == 1
        assert store.conn.execute('SELECT live_rowid FROM streams').fetchone()[0] == 4
    finally:
        collector.close()
        store.close()


def test_rowid_reuse_retries_only_after_rolling_back_previous_batch(environment, tmp_path, monkeypatch):
    collector, store, database = real_collector(environment, tmp_path)
    original = store.commit_page
    calls = []
    before = store.conn.execute('SELECT generation FROM sources').fetchone()[0]
    try:
        with sqlite3.connect(database) as native:
            native.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"reuse-retry",NULL)', (b'new',))
        def rejected_once(*args, **kwargs):
            result = original(*args, **kwargs)
            if kwargs.get('mode') == 'live' and not calls:
                calls.append(True)
                raise ArchiveError('source_rowid_reused')
            return result
        monkeypatch.setattr(store, 'commit_page', rejected_once)
        summary = collector.collect_once()
        assert not summary['errors']
        assert store.conn.execute('SELECT generation FROM sources').fetchone()[0] == before + 1
        assert all(p['generation'] == before + 1 for p in summary['live_batches'])
        assert not store.conn.execute("SELECT 1 FROM sqlite_master WHERE name='bot_events'").fetchone()
        assert not store.conn.in_transaction and not collector._source_batch_active
    finally:
        collector.close()
        store.close()
