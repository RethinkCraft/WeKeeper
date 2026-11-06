"""Opt-in HTTP evidence on fake wire + real FULL SQLite, never external API."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import hashlib
import json
import sqlite3
import threading

import httpx
import pytest

from chatlog_keeper.bot import model_http_trace as trace
from chatlog_keeper.bot import providers
from chatlog_keeper.bot.providers import ProviderError
from chatlog_keeper.bot.usage_actor import UsageLedgerActor
from chatlog_keeper.dashboard.usage import UsageLedger
from test_bot_required_read import TOOLS, call, compatible, response


META = {'run_id': 'run', 'group_ref': 'group', 'bot_request_id': 'request'}


@pytest.fixture(autouse=True)
def disabled(monkeypatch):
    monkeypatch.delenv(trace.ENVIRONMENT, raising=False)


def configured(tmp_path, remote):
    provider = compatible(remote)
    provider.ledger, provider.own_ledger = UsageLedger(tmp_path), True
    return provider


def metadata(ledger, run='run'):
    return json.loads(ledger.conn.execute('SELECT metadata FROM usage_runs WHERE run_id=?', (run,)).fetchone()[0])


def trace_row(index=1):
    return {'schema': 1, 'call_index': index, 'body_sha256': 'a'*64,
        'utc_anchor': '2026-10-30T00:00:00.000000+00:00', 'monotonic_anchor': 1.0,
        'duration_seconds': 1.0, 'outcome': 'returned', 'events': [],
        'dropped_events': 0, 'observer_errors': 0}


def test_disabled_post_kwargs_path_does_not_add_extensions_or_encode_for_trace(tmp_path, monkeypatch):
    monkeypatch.setattr(trace, '_Trace', lambda *_: pytest.fail('disabled observer constructed'))
    provider = configured(tmp_path, lambda _: response({'content': 'ok'}))
    original, calls = provider.http.post, []
    def post(*args, **kwargs):
        calls.append(kwargs)
        return original(*args, **kwargs)
    provider.http.post = post
    try:
        assert provider.complete([], '', TOOLS, lambda *_: {}, META) == 'ok'
        assert set(calls[0]) == {'headers', 'json', 'timeout'}
        assert 'model_http_calls' not in metadata(provider.ledger)
    finally:
        provider.close()


def test_enabled_wire_bytes_headers_params_equal_disabled_and_trace_is_private(tmp_path, monkeypatch):
    wires = []
    def remote(request):
        wires.append((request.content, dict(request.headers)))
        callback = request.extensions.get('trace')
        if callback:
            # A poisoned info object must never be inspected or stringified.
            class Poison:
                def __getattribute__(self, name):
                    raise AssertionError('secret was accessed')
            callback('proxy.start_tls.started', Poison())
            callback('http2.receive_response_headers.complete', {'headers': 'secret-never-store'})
            callback('unapproved.secret.complete', {'body': 'secret-never-store'})
        return httpx.Response(200, json={'choices': [{'message': {'content': 'ok'}}],
            'usage': {'prompt_tokens': 7, 'completion_tokens': 2, 'total_tokens': 9}})
    for index in range(2):
        if index:
            monkeypatch.setenv(trace.ENVIRONMENT, '1')
        provider = configured(tmp_path / str(index), remote)
        try:
            assert provider.complete([{'role': 'user', 'content': '非ASCII\n消息'}], 'rules', TOOLS, lambda *_: {}, META) == 'ok'
            if index:
                stored = metadata(provider.ledger)['model_http_calls'][0]
                row = stored['trace']
                assert row['body_sha256'] == hashlib.sha256(wires[-1][0]).hexdigest()
                assert row['outcome'] == 'returned' and row['call_index'] == 1
                assert row['dropped_events'] == 1 and row['observer_errors'] == 0
                assert [e['name'] for e in row['events']] == ['proxy.start_tls.started', 'http2.receive_response_headers.complete']
                assert 'secret-never-store' not in json.dumps(metadata(provider.ledger))
                assert provider.ledger.conn.execute('SELECT call_id FROM usage_calls').fetchone()[0] == stored['call_id']
                for wire in (provider.ledger.get('run'), provider.ledger.pending(), provider.ledger.list_runs()):
                    assert 'model_http' not in json.dumps(wire)
        finally:
            provider.close()
    assert wires[0] == wires[1]


def test_trace_cap_and_callback_clock_error_do_not_raise(monkeypatch):
    monkeypatch.setenv(trace.ENVIRONMENT, '1')
    value = trace.begin_trace({'value': 'private'}, 1)
    original = trace.time.monotonic
    monkeypatch.setattr(trace.time, 'monotonic', lambda: (_ for _ in ()).throw(RuntimeError('private')))
    value.callback('connection.connect_tcp.started', {})
    monkeypatch.setattr(trace.time, 'monotonic', original)
    for _ in range(140):
        value.callback('http2.receive_response_body.complete', {'data': 'not stored'})
    row = value.finish(returned=True)
    assert len(row['events']) == 128 and row['dropped_events'] == 12 and row['observer_errors'] == 1
    assert 'private' not in json.dumps(row)


@pytest.mark.parametrize('broken', ['begin', 'finish'])
def test_observer_failure_does_not_change_successful_post_or_accounting(tmp_path, monkeypatch, broken):
    monkeypatch.setenv(trace.ENVIRONMENT, '1')
    if broken == 'begin':
        monkeypatch.setattr(providers, 'begin_trace', lambda *_: (_ for _ in ()).throw(RuntimeError('secret')))
    else:
        monkeypatch.setattr(trace._Trace, 'finish', lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('secret')))
    provider = configured(tmp_path, lambda _: response({'content': 'ok'}))
    try:
        assert provider.complete([], '', TOOLS, lambda *_: {}, META) == 'ok'
        assert provider.ledger.get('run')['status'] == 'completed'
        assert provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
        assert 'model_http_calls' not in metadata(provider.ledger)
    finally:
        provider.close()


def test_failed_http_trace_uses_terminal_run_transaction_without_creating_usage_call(tmp_path, monkeypatch):
    monkeypatch.setenv(trace.ENVIRONMENT, '1')
    def remote(request):
        callback = request.extensions.get('trace')
        if callback:
            callback('http2.receive_response_headers.failed', {'exception': 'secret'})
        raise httpx.ReadTimeout('secret')
    provider = configured(tmp_path, remote)
    statements = []
    provider.ledger.conn.set_trace_callback(statements.append)
    try:
        with pytest.raises(ProviderError, match='bot_provider_timeout'):
            provider.complete([], '', TOOLS, lambda *_: {}, META)
        row = provider.ledger.get('run')
        assert row['status'] == 'failed' and row['usage'] is None
        assert provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 0
        records = metadata(provider.ledger)['model_http_calls']
        assert len(records) == 1 and 'call_id' not in records[0]
        assert records[0]['trace']['outcome'] == 'unknown'
        assert statements.count('COMMIT') == 2  # unchanged start + terminal.
        assert 'secret' not in json.dumps(records)
    finally:
        provider.close()


def test_returned_tool_call_then_lost_second_http_keeps_one_actual_call_and_two_traces(tmp_path, monkeypatch):
    monkeypatch.setenv(trace.ENVIRONMENT, '1')
    attempts = []
    def remote(request):
        attempts.append(request)
        request.extensions['trace']('http2.receive_response_headers.started', {})
        if len(attempts) == 1:
            return response({'tool_calls': [call()]})
        raise httpx.ReadError('secret')
    provider = configured(tmp_path, remote)
    try:
        with pytest.raises(ProviderError, match='bot_provider_invalid_response'):
            provider.complete([], '', TOOLS, lambda *_: {'ok': True}, META)
        assert provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
        rows = metadata(provider.ledger)['model_http_calls']
        assert [r['trace']['call_index'] for r in rows] == [1, 2]
        assert [r['trace']['outcome'] for r in rows] == ['returned', 'unknown']
        assert 'call_id' in rows[0] and 'call_id' not in rows[1]
    finally:
        provider.close()


def test_diagnostics_and_final_call_are_atomic_in_existing_full_commit(tmp_path):
    ledger = UsageLedger(tmp_path)
    reader = sqlite3.connect(tmp_path / 'usage.sqlite')
    statements, observations = [], []
    try:
        ledger.start_run(kind='bot_chat', model='fixture', **META)
        def observe(statement):
            statements.append(statement)
            if statement.startswith('UPDATE usage_runs'):
                observations.append((reader.execute('SELECT status,metadata FROM usage_runs').fetchone(),
                                     reader.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0]))
        ledger.conn.set_trace_callback(observe)
        ledger.finish_with_call('run', call={'call_id': 'call', 'usage': {'total_tokens': 9}, 'model_http_trace': trace_row()})
        assert statements.count('COMMIT') == 1
        assert len(observations) == 2 and all(status[0] == 'running' and 'model_http' not in status[1] and count == 0 for status, count in observations)
        assert ledger.get('run')['usage']['total_tokens'] == 9
        assert ledger.conn.execute('PRAGMA synchronous').fetchone()[0] == 2
        assert [r[1] for r in ledger.conn.execute('PRAGMA table_info(usage_calls)')] == [
            'call_id', 'run_id', 'response_id', 'request_id', 'usage', 'raw_usage', 'status', 'created_at']
    finally:
        reader.close(); ledger.close()


def test_diag_sql_failure_is_best_effort_but_usage_still_commits(tmp_path):
    ledger = UsageLedger(tmp_path)
    try:
        ledger.start_run(kind='bot_chat', model='fixture', **META)
        ledger.conn.execute("CREATE TRIGGER reject_diag BEFORE UPDATE OF metadata ON usage_runs BEGIN SELECT RAISE(ABORT,'diagnostic only'); END")
        ledger.conn.commit()
        ledger.finish_with_call('run', call={'call_id': 'call', 'usage': {'total_tokens': 9}, 'model_http_trace': trace_row()})
        assert ledger.get('run')['status'] == 'completed' and ledger.get('run')['usage']['total_tokens'] == 9
        assert 'model_http_calls' not in metadata(ledger)
    finally:
        ledger.close()


@pytest.mark.parametrize('method', ['record_call', 'finish_with_call'])
def test_diagnostic_rollback_replays_same_accounting_call_without_false_completion(tmp_path, method):
    ledger = UsageLedger(tmp_path)
    try:
        ledger.start_run(kind='bot_chat', model='fixture', **META)
        ledger.conn.execute("CREATE TRIGGER reject_diag BEFORE UPDATE OF metadata ON usage_runs BEGIN SELECT RAISE(ROLLBACK,'diagnostic rollback'); END")
        ledger.conn.commit()
        call_args = {'call_id': 'same-real-call', 'usage': {'total_tokens': 9}, 'model_http_trace': trace_row()}
        if method == 'record_call':
            assert ledger.record_call('run', **call_args) == 'same-real-call'
            ledger.finish_run('run')
        else:
            ledger.finish_with_call('run', call=call_args)
        assert ledger.get('run')['status'] == 'completed'
        assert ledger.get('run')['usage']['total_tokens'] == 9
        assert ledger.conn.execute('SELECT call_id FROM usage_calls').fetchall()[0][0] == 'same-real-call'
        assert ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
        assert ledger.model_http_trace_recoveries == 1 and ledger.model_http_trace_disabled
        assert 'model_http_calls' not in metadata(ledger)
    finally:
        ledger.close()


def test_fifth_observation_increments_drop_count_without_saving_fifth_events(tmp_path, monkeypatch):
    monkeypatch.setenv(trace.ENVIRONMENT, '1')
    ledger = UsageLedger(tmp_path)
    try:
        ledger.start_run(kind='bot_chat', model='fixture', **META)
        for index in range(1, 6):
            value = trace.begin_trace({'body': index}, index)
            value.callback('http2.receive_response_body.complete', {})
            row = value.finish(returned=True)
            ledger.record_call('run', call_id=str(index), model_http_trace=row)
        stored = metadata(ledger)
        assert len(stored['model_http_calls']) == 4
        assert stored['model_http_dropped_calls'] == 1
        assert [r['trace']['call_index'] for r in stored['model_http_calls']] == [1, 2, 3, 4]
    finally:
        ledger.close()


def test_diagnostic_rollback_generated_call_id_is_reused_and_unrecoverable_accounting_raises(tmp_path, monkeypatch):
    ledger = UsageLedger(tmp_path)
    original = ledger._record_call
    attempted = []
    try:
        ledger.start_run(kind='bot_chat', model='fixture', **META)
        ledger.conn.execute("CREATE TRIGGER reject_diag BEFORE UPDATE OF metadata ON usage_runs BEGIN SELECT RAISE(ROLLBACK,'diagnostic rollback'); END")
        ledger.conn.commit()
        def record(*args, **kwargs):
            attempted.append(kwargs['call_id'])
            if len(attempted) == 2:
                raise sqlite3.OperationalError('accounting retry failed')
            return original(*args, **kwargs)
        monkeypatch.setattr(ledger, '_record_call', record)
        with pytest.raises(sqlite3.OperationalError, match='accounting retry failed'):
            ledger.finish_with_call('run', call={'usage': {'total_tokens': 9}, 'model_http_trace': trace_row()})
        assert len(attempted) == 2 and attempted[0] and attempted[0] == attempted[1]
        assert ledger.get('run')['status'] == 'running'
        assert ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 0
        assert ledger.model_http_trace_disabled and ledger.model_http_trace_recoveries == 0
    finally:
        ledger.close()


def test_diagnostic_rollback_does_not_guess_recovery_of_caller_owned_transaction(tmp_path):
    ledger = UsageLedger(tmp_path)
    try:
        ledger.start_run(kind='bot_chat', model='fixture', **META)
        ledger.conn.execute("CREATE TRIGGER reject_diag BEFORE UPDATE OF metadata ON usage_runs BEGIN SELECT RAISE(ROLLBACK,'diagnostic rollback'); END")
        ledger.conn.commit()
        ledger._record_call('run', call_id='caller-owned', usage={'total_tokens': 2})
        assert ledger.conn.in_transaction
        with pytest.raises(RuntimeError, match='usage_http_diagnostics_transaction_rolled_back'):
            ledger.finish_with_call('run', call={'call_id': 'current', 'model_http_trace': trace_row()})
        assert ledger.get('run')['status'] == 'running'
        assert ledger.model_http_trace_disabled and ledger.model_http_trace_recoveries == 0
    finally:
        ledger.close()


def test_reserved_metadata_cannot_be_spoofed_and_replay_cannot_replace_trace(tmp_path):
    ledger = UsageLedger(tmp_path)
    try:
        ledger.start_run(kind='bot_chat', model='fixture', **META, model_http_calls=['forged'], model_http_dropped_calls=999)
        assert 'model_http_calls' not in metadata(ledger)
        ledger.finish_with_call('run', call={'call_id': 'call', 'model_http_trace': trace_row()})
        first = deepcopy(metadata(ledger))
        changed = trace_row(); changed['body_sha256'] = 'b'*64
        ledger.finish_with_call('run', call={'call_id': 'call', 'model_http_trace': changed})
        assert metadata(ledger) == first
    finally:
        ledger.close()


def test_closed_schema_rejects_content_and_bounds_calls_without_failing_usage(tmp_path):
    ledger = UsageLedger(tmp_path)
    try:
        ledger.start_run(kind='bot_chat', model='fixture', **META)
        malicious = trace_row(); malicious['headers'] = 'secret'
        ledger.record_call('run', call_id='bad', model_http_trace=malicious)
        assert 'model_http_calls' not in metadata(ledger)
        for index in range(5):
            value = trace_row(index % 4 + 1); value['monotonic_anchor'] += index
            ledger.record_call('run', call_id=str(index), model_http_trace=value)
        stored = metadata(ledger)
        assert len(stored['model_http_calls']) == 4 and stored['model_http_dropped_calls'] == 1
        assert 'secret' not in json.dumps(stored)
    finally:
        ledger.close()


def test_concurrent_pooled_client_callbacks_and_actor_are_bound_to_each_run(tmp_path, monkeypatch):
    monkeypatch.setenv(trace.ENVIRONMENT, '1')
    barrier = threading.Barrier(2)
    bodies = {}
    def remote(request):
        text = json.loads(request.content)['messages'][-1]['content']
        bodies[text] = hashlib.sha256(request.content).hexdigest()
        callback = request.extensions['trace']
        callback('http2.send_request_body.complete', {})
        barrier.wait(timeout=5)
        callback('http2.receive_response_body.complete', {})
        return response({'content': 'ok'})
    client = httpx.Client(transport=httpx.MockTransport(remote))
    actor = UsageLedgerActor(tmp_path)
    def complete(name):
        provider = compatible(lambda _: pytest.fail('wrong client'))
        provider.http.close()
        provider.http, provider.owned, provider.ledger = client, False, actor
        try:
            return provider.complete([{'role': 'user', 'content': name}], '', TOOLS, lambda *_: {},
                {**META, 'run_id': name, 'bot_request_id': name})
        finally:
            provider.close()
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            assert list(pool.map(complete, ('one', 'two'))) == ['ok', 'ok']
    finally:
        actor.close(); client.close()
    with sqlite3.connect(tmp_path / 'usage.sqlite') as connection:
        for run, value in connection.execute('SELECT run_id,metadata FROM usage_runs'):
            rows = json.loads(value)['model_http_calls']
            assert len(rows) == 1 and rows[0]['trace']['body_sha256'] == bodies[run]
            assert len(rows[0]['trace']['events']) == 2
