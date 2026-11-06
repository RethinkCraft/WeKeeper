"""Atomic final usage accounting on synthetic SQLite state, without API calls."""
import json
import sqlite3
import threading

import pytest

from chatlog_keeper.bot.usage_actor import UsageLedgerActor
from chatlog_keeper.dashboard.usage import UsageLedger


def test_last_call_and_terminal_run_share_one_full_atomic_commit(tmp_path):
    ledger = UsageLedger(tmp_path)
    ledger.start_run(run_id='run', model='synthetic')
    reader = sqlite3.connect(tmp_path / 'usage.sqlite')
    statements, observations = [], []
    def observe(statement):
        statements.append(statement)
        if statement.startswith('UPDATE usage_runs'):
            observations.append((reader.execute("SELECT status FROM usage_runs WHERE run_id='run'").fetchone()[0],
                                 reader.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0]))
    try:
        ledger.conn.set_trace_callback(observe)
        row = ledger.finish_with_call('run', call={'call_id': 'last', 'usage': {'total_tokens': 7}})
        ledger.conn.set_trace_callback(None)
        assert statements.count('COMMIT') == 1 and observations == [('running', 0)]
        assert row['status'] == 'completed' and row['usage']['total_tokens'] == 7
        assert reader.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
        assert ledger.conn.execute('PRAGMA synchronous').fetchone()[0] == 2
        ledger.finish_with_call('run', call={'call_id': 'last', 'usage': {'total_tokens': 999}})
        assert ledger.get('run') == row and ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
        ledger.acknowledge('run')
        ledger.finish_with_call('run', call={'call_id': 'last'}, status='failed', error_code='synthetic')
        assert ledger.get('run') == row
    finally:
        reader.close(); ledger.close()


def test_unknown_final_call_keeps_aggregate_unknown(tmp_path):
    ledger = UsageLedger(tmp_path)
    try:
        ledger.start_run(run_id='run', model='synthetic')
        ledger.record_call('run', usage={'total_tokens': 5})
        row = ledger.finish_with_call('run', call={'call_id': 'unknown', 'usage': None}, status='failed')
        assert row['usage'] is None and row['status'] == 'failed'
        assert ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 2
    finally:
        ledger.close()


@pytest.mark.parametrize('failure', ['missing_run', 'invalid_status', 'update_error'])
def test_failed_finalization_rolls_back_the_call_insert(tmp_path, failure):
    ledger = UsageLedger(tmp_path)
    try:
        if failure != 'missing_run':
            ledger.start_run(run_id='run', model='synthetic')
        if failure == 'update_error':
            ledger.conn.execute("CREATE TRIGGER reject_finish BEFORE UPDATE ON usage_runs BEGIN SELECT RAISE(ABORT,'synthetic rollback'); END")
            ledger.conn.commit()
        with pytest.raises(sqlite3.IntegrityError if failure == 'update_error' else ValueError):
            ledger.finish_with_call('run', call={'call_id': 'last', 'usage': {'total_tokens': 7}},
                                    status='invalid' if failure == 'invalid_status' else 'completed')
        assert ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 0
        assert failure == 'missing_run' or ledger.get('run')['status'] == 'running'
    finally:
        ledger.close()


def test_actor_finishes_call_on_owner_and_acknowledges_actual_full_commit(tmp_path):
    threads, events = [], []
    class Ledger(UsageLedger):
        def finish_with_call(self, *args, **kwargs):
            threads.append(threading.get_ident())
            return super().finish_with_call(*args, **kwargs)
    actor = UsageLedgerActor(tmp_path, ledger_factory=Ledger, timing_callback=events.append)
    try:
        actor.start_run(run_id='run', model='synthetic')
        row = actor.finish_with_call('run', call={'call_id': 'last', 'usage': {'total_tokens': 9}})
        with sqlite3.connect(tmp_path / 'usage.sqlite') as reader:
            stored = reader.execute("SELECT status,usage FROM usage_runs WHERE run_id='run'").fetchone()
            assert stored[0] == row['status'] == 'completed' and json.loads(stored[1])['total_tokens'] == 9
            assert reader.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
    finally:
        actor.close()
    assert threads == [actor._thread.ident] and actor._thread.ident != threading.get_ident()
    assert any(event['stage'] == 'usage_finish_with_call' for event in events)
