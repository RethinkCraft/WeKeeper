"""Warm MCP stores retain FULL semantics without rewriting schema metadata."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import threading

import pytest

from chatlog_keeper.bot.store import BotStore
from test_bot_service import config, report, request, service


def readonly(path):
    return sqlite3.connect(path.absolute().as_uri() + '?mode=ro', uri=True)


def test_warm_reopen_changes_neither_observer_data_version_nor_wal(tmp_path):
    first = BotStore(tmp_path)
    observer = readonly(first.path)
    try:
        version = observer.execute('PRAGMA data_version').fetchone()[0]
        wal = first.path.with_name(first.path.name + '-wal')
        before = wal.read_bytes()
        expected_config, revision = first.config(), first.get('revision')
        for _ in range(4):
            reopened = BotStore(tmp_path)
            try:
                assert reopened.conn.execute('PRAGMA synchronous').fetchone()[0] == 2
                assert reopened.config() == expected_config
                assert reopened.get('revision') == revision
            finally:
                reopened.close()
        assert observer.execute('PRAGMA data_version').fetchone()[0] == version
        assert wal.read_bytes() == before
    finally:
        observer.close()
        first.close()


def test_first_initialization_is_full_and_visible_to_independent_reader(tmp_path):
    store = BotStore(tmp_path)
    try:
        assert store.conn.execute('PRAGMA synchronous').fetchone()[0] == 2
        with readonly(store.path) as observer:
            values = dict(observer.execute("SELECT key,value FROM meta WHERE key IN ('config','revision','schema')"))
            assert set(values) == {'config', 'revision', 'schema'}
            assert values['schema'] == '2' and values['revision'] == '0'
    finally:
        store.close()
    reopened = BotStore(tmp_path)
    try:
        assert reopened.get('schema') == 2 and reopened.config()['revision'] == 0
    finally:
        reopened.close()


@pytest.mark.parametrize('missing', ['config', 'revision', 'schema'])
def test_missing_initial_metadata_is_repaired_without_overwriting_other_values(tmp_path, missing):
    first = BotStore(tmp_path)
    first.configure(config())
    before = first.config()
    with first.conn:
        first.conn.execute('DELETE FROM meta WHERE key=?', (missing,))
    second = BotStore(tmp_path)
    try:
        assert second.get('schema') == 2
        assert second.get('revision') == (0 if missing == 'revision' else before['revision'])
        if missing != 'config':
            expected = {**before, 'revision': second.get('revision')}
            assert second.config() == expected
        else:
            assert second.config()['groups'] == []
        with readonly(first.path) as observer:
            assert observer.execute('SELECT COUNT(*) FROM meta WHERE key IN (\'config\',\'revision\',\'schema\')').fetchone()[0] == 3
    finally:
        second.close()
        first.close()


def test_concurrent_metadata_repair_is_atomic_and_does_not_replace_configuration(tmp_path):
    keeper = BotStore(tmp_path)
    keeper.configure(config())
    expected = keeper.config()
    with keeper.conn:
        keeper.conn.execute("UPDATE meta SET value='1' WHERE key='schema'")
        keeper.conn.execute("DELETE FROM meta WHERE key='revision'")
    started = threading.Barrier(3)

    def reopen(_):
        started.wait(timeout=5)
        value = BotStore(tmp_path)
        try:
            return value.get('schema'), value.config(), value.conn.execute('PRAGMA synchronous').fetchone()[0]
        finally:
            value.close()

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(reopen, range(3)))
        assert all(schema == 2 and current == {**expected, 'revision': 0} and synchronous == 2
                   for schema, current, synchronous in results)
        with readonly(keeper.path) as observer:
            assert dict(observer.execute("SELECT key,value FROM meta WHERE key IN ('schema','revision')")) == {'schema': '2', 'revision': '0'}
    finally:
        keeper.close()


def test_legacy_reply_schema_migration_is_durable_before_warm_reopen(tmp_path):
    directory = tmp_path / 'bot'
    directory.mkdir(mode=0o700)
    path = directory / 'bot.sqlite'
    with sqlite3.connect(path) as connection:
        connection.execute('''CREATE TABLE reply_outbox(
            reply_id TEXT PRIMARY KEY,event_id TEXT NOT NULL UNIQUE,group_ref TEXT NOT NULL,
            text TEXT,reply_to TEXT NOT NULL,status TEXT NOT NULL,error TEXT,confirmation_ref TEXT,
            created_at TEXT NOT NULL,updated_at TEXT NOT NULL)''')
        connection.execute('INSERT INTO reply_outbox VALUES(?,?,?,?,?,?,?,?,?,?)',
            ('old_reply', 'old_event', 'group_a', 'old final', '{}', 'unknown', 'unconfirmed', None, 'before', 'before'))
    first = BotStore(tmp_path)
    try:
        with readonly(path) as observer:
            assert observer.execute('SELECT status,kind,text FROM reply_outbox').fetchone() == ('unknown', 'final', 'old final')
            assert observer.execute("SELECT value FROM meta WHERE key='schema'").fetchone()[0] == '2'
        assert first.conn.execute('PRAGMA synchronous').fetchone()[0] == 2
        with readonly(path) as observer:
            version = observer.execute('PRAGMA data_version').fetchone()[0]
            second = BotStore(tmp_path)
            second.close()
            assert observer.execute('PRAGMA data_version').fetchone()[0] == version
    finally:
        first.close()


def test_reopened_mcp_business_report_remains_one_transaction_and_one_audit(tmp_path):
    first = BotStore(tmp_path)
    first.configure(config())
    request(first)
    recorded = report(service(first))
    assert recorded['ok'] and recorded['recorded'] and recorded['version'] == 1
    second = BotStore(tmp_path)
    try:
        replay = report(service(second))
        assert replay['replayed'] and replay['version'] == 1
        assert second.offer_status('shop', '2026-10-27', 'fun')['version'] == 1
        assert len([row for row in second.audit() if row['kind'] == 'report']) == 1
        assert second.conn.execute('SELECT COUNT(*) FROM reports').fetchone()[0] == 1
    finally:
        second.close()
        first.close()
