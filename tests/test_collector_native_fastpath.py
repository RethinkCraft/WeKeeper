"""Native wakeups and fair, authenticated cache-only receive scheduling."""
from contextlib import contextmanager
import hashlib
import importlib.util
from pathlib import Path
import shutil
import sqlite3
import struct
import threading
import time

import pytest

from chatlog_keeper import cli, wechat_db
from chatlog_keeper.collector import CollectorError
from chatlog_keeper.core._native_watch import NativeFileWatcher
from test_collector import environment, GROUP, TABLE


def test_priority_groups_are_repeatable_and_native_only(environment):
    args = cli.parser().parse_args(['collect', '--bot-mode', '--priority-group', 'a@chatroom',
                                    '--priority-group', 'b@chatroom'])
    assert args.priority_group == ['a@chatroom', 'b@chatroom']
    collector, _, _, _ = environment
    with pytest.raises(ValueError, match='priority group'):
        type(collector)(collector.store, priority_groups=['display name'])


def test_reader_reuses_verified_keys_until_cache_or_file_identity_changes(environment, monkeypatch, tmp_path):
    collector, _, database, _ = environment
    collector.bot_mode = True
    cache = tmp_path / 'account.key'
    cache.write_bytes(b'not a key; only a metadata fixture')
    monkeypatch.setattr(wechat_db, '_wechat_account_key_cache_path', lambda _: cache)
    original = wechat_db.WeChatDBReader
    calls = []
    def reader(*args, **kwargs):
        calls.append(True)
        return original(*args, **kwargs)
    monkeypatch.setattr(wechat_db, 'WeChatDBReader', reader)
    first = collector._reader()
    with sqlite3.connect(database) as connection:
        connection.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"new",NULL)', (b'new',))
    assert collector._reader() is first  # ordinary DB/WAL writes do not rotate its salt/key
    cache.write_bytes(b'changed cache metadata')
    second = collector._reader()
    assert second is not first
    replacement = database.with_suffix('.replacement')
    shutil.copyfile(database, replacement)
    replacement.replace(database)
    assert collector._reader() is not second
    assert len(calls) == 3


def test_failed_authentication_discards_reader_and_rechecks_changed_cached_key(environment, monkeypatch):
    collector, _, database, _ = environment
    collector.bot_mode = True
    first = collector._reader()
    monkeypatch.setattr(wechat_db, '_decrypt_db_v4', lambda *args: False)
    with pytest.raises(CollectorError, match='decryption_failed'):
        collector._get_snapshot(database, b'a' * 32, 'new-fingerprint')
    assert collector._cached_reader is None
    assert collector._reader() is not first


def test_unchanged_directory_is_not_written_each_live_cycle(environment):
    collector, store, database, _ = environment
    collector.bot_mode = True
    writes = []
    original = store.upsert_groups
    store.upsert_groups = lambda *args: (writes.append(args), original(*args))[1]
    collector.collect_once()
    count = len(writes)
    with sqlite3.connect(database) as connection:
        connection.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"new",NULL)', (b'new',))
    collector.collect_once()
    assert len(writes) == count == 1


def test_heartbeat_throttles_but_errors_and_recovery_publish_immediately(environment, monkeypatch):
    collector, store, _, _ = environment
    collector.bot_mode = True
    tick = [10.]
    monkeypatch.setattr('chatlog_keeper.collector.time.monotonic', lambda: tick[0])
    calls = []
    original = store.set_collector_status
    store.set_collector_status = lambda **fields: (calls.append(fields), original(**fields))[1]
    collector._set_status(running=True, last_error=None, last_cycle_at=1)
    collector._set_status(last_cycle_at=2)
    assert len(calls) == 1
    collector._set_status(last_error='read_failed')
    collector._set_status(last_error=None)
    assert len(calls) == 3
    tick[0] += 1
    collector._set_status(last_cycle_at=3)
    assert len(calls) == 4 and calls[-1]['last_cycle_at'] == 3


def test_bot_round_robin_does_one_live_page_per_table_before_history(environment):
    collector, store, database, _ = environment
    collector.bot_mode = True
    other = 'busy@chatroom'
    other_table = 'Msg_' + hashlib.md5(other.encode()).hexdigest()
    with sqlite3.connect(database) as connection:
        connection.execute('INSERT INTO Name2Id VALUES (?)', (other,))
        connection.execute(f'CREATE TABLE "{other_table}" AS SELECT * FROM "{TABLE}" WHERE 0')
    collector.collect_once()  # baseline both live streams before adding real-time rows
    collector.priority_groups = (other,)
    with sqlite3.connect(database) as connection:
        for index in range(5):
            for table, prefix in [(TABLE, 'normal'), (other_table, 'priority')]:
                connection.execute(f'INSERT INTO "{table}" VALUES(1,200,2,?,?,NULL)',
                                   (f'{prefix}-{index}'.encode(), f'{prefix}-{index}'))
    store.commits.clear()
    result = collector.collect_once()
    assert result['live_pending'] is True
    batches = result['live_batches']
    assert [batch['table_name'] for batch in batches] == [other_table, TABLE]
    assert all(batch['last_rowid'] - batch['first_rowid'] == 1 for batch in batches)
    assert [mode for mode, _ in store.commits] == ['live', 'live']
    assert all(batch['source_ready_at'] >= batch['commit_at'] for batch in batches)
    assert all(batch['fingerprint'] and batch['generation'] == 1 for batch in batches)
    assert 'snapshot_and_decrypt' in result['timings']
    assert not collector.collect_once().get('idle')
    assert not collector.collect_once()['live_pending']
    assert all((other, f'priority-{index}') in store.records for index in range(5))


def test_native_hint_is_only_associated_with_its_actual_live_batch(environment):
    collector, _, database, _ = environment
    collector.bot_mode = True
    collector.collect_once()
    with sqlite3.connect(database) as connection:
        connection.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"hinted",NULL)', (b'new',))
    collector._native_observed_at = time.time()
    collector._native_observed_monotonic = time.monotonic()
    result = collector.collect_once()
    assert result['live_batches'][0]['native_event_observed_at'] is not None
    assert result['timings']['native_hint_to_collect'] >= 0
    assert collector._native_observed_at is None
    assert collector._native_observed_monotonic is None
    with sqlite3.connect(database) as connection:
        connection.execute(f'INSERT INTO "{TABLE}" VALUES(1,200,2,?,"poll-only",NULL)', (b'poll',))
    next_cycle = collector.collect_once()
    assert next_cycle['live_batches'][0]['native_event_observed_at'] is None
    assert 'native_hint_to_collect' not in next_cycle['timings']


@pytest.fixture
def watcher(tmp_path):
    account = tmp_path / 'account'
    directory = account / 'db_storage/message'
    directory.mkdir(parents=True)
    watch = NativeFileWatcher()
    if not watch.available:
        pytest.skip('inotify not available in this environment')
    watch.configure(account)
    yield watch, account, directory
    watch.close()


def test_native_wakeup_catches_wal_writes_and_atomic_replacement(watcher):
    watch, account, directory = watcher
    wal = directory / 'message_0.db-wal'
    wal.write_bytes(b'first')
    assert watch.wait(.2)
    assert watch.observed_at and watch.observed_monotonic
    assert not watch.wait(0)
    wal.write_bytes(b'second')
    assert watch.wait(.2)
    replacement = directory / 'new.db'
    replacement.write_bytes(b'new')
    replacement.replace(directory / 'message_0.db')
    assert watch.wait(.2)
    # Directory replacement does not leave the watcher bound to the old inode.
    directory.rename(directory.with_name('old-message'))
    directory.mkdir()
    assert watch.wait(.2)
    watch.configure(account)
    watch.wait(0)
    (directory / 'message_1.db-wal').write_bytes(b'recreated')
    assert watch.wait(.2)


def test_wakeup_coalescing_has_one_deadline_during_a_write_storm(watcher):
    watch, _, directory = watcher
    stop = threading.Event()
    path = directory / 'message_0.db-wal'
    def writer():
        while not stop.is_set():
            path.write_bytes(b'changes')
            stop.wait(.001)
    thread = threading.Thread(target=writer)
    thread.start()
    try:
        started = time.monotonic()
        assert watch.wait(.2)
        assert time.monotonic() - started < .15
    finally:
        stop.set()
        thread.join(timeout=1)


def test_wakeup_overflow_is_dirty_and_rebuilds_watches(watcher, monkeypatch):
    watch, account, directory = watcher
    original = __import__('os').read
    payload = [struct.pack('iIII', -1, watch.OVERFLOW, 0, 0)]
    def read(fd, size):
        if fd == watch.fd:
            if payload:
                return payload.pop()
            raise BlockingIOError
        return original(fd, size)
    with monkeypatch.context() as scoped:
        scoped.setattr('chatlog_keeper.core._native_watch.os.read', read)
        assert watch._drain()
    assert watch.overflow and not watch._watches
    watch.configure(account)
    watch.wait(0)
    (directory / 'message_0.db').write_bytes(b'after overflow')
    assert watch.wait(.2)


def test_native_watch_unavailable_keeps_polling_fallback(monkeypatch):
    def unavailable(*args, **kwargs):
        raise OSError('unavailable')
    monkeypatch.setattr('chatlog_keeper.core._native_watch.ctypes.CDLL', unavailable)
    watch = NativeFileWatcher()
    assert not watch.available and not watch.wait(0)
    watch.close()


def test_changes_queued_during_collection_force_immediate_rescan(environment, monkeypatch):
    collector, _, _, _ = environment
    collector.bot_mode = True
    collector.poll_interval = 30  # bot fallback is bounded independently
    collector.account_dir = Path(collector.data_root) / 'wxid_example'
    stop = threading.Event()
    calls, timeouts = [], []
    class Watcher:
        observed_at = 123.
        observed_monotonic = 12.
        def configure(self, account):
            assert account == collector.account_dir
        def wait(self, timeout):
            timeouts.append(timeout)
            return True
        def close(self):
            pass
    monkeypatch.setattr('chatlog_keeper.collector.NativeFileWatcher', Watcher)
    def collect():
        calls.append(True)
        if len(calls) == 1:
            collector._fast_signature = 'old-signature'
        else:
            assert collector._fast_signature is None
            assert collector._native_observed_at == 123.
            stop.set()
    collector.collect_once = collect
    collector.run(stop)
    assert len(calls) == 2 and all(0 <= timeout <= .5 for timeout in timeouts)


def test_snapshot_and_authenticated_decrypt_are_timed_without_bypassing_snapshot(monkeypatch, tmp_path):
    source = tmp_path / 'encrypted.db'
    target = tmp_path / 'output.db'
    events = []
    @contextmanager
    def snapshot(path):
        assert path == source
        events.append('consistent_snapshot')
        yield tmp_path / 'private-copy.db'
    def decrypt(path, key, output):
        assert path.name == 'private-copy.db' and key == b'key' and output == target
        events.append('authenticated_decrypt')
        return True
    monkeypatch.setattr('chatlog_keeper.core._snapshot.snapshot_db_family', snapshot)
    monkeypatch.setattr(wechat_db, '_decrypt_db_v4_snapshot', decrypt)
    timings = {}
    assert wechat_db._decrypt_db_v4(source, b'key', target, timings=timings)
    assert events == ['consistent_snapshot', 'authenticated_decrypt']
    assert timings.keys() == {'snapshot', 'decrypt'}
