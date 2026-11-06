"""Compatible transport accounting on fake HTTP responses and synthetic SQLite."""
import json

import httpx
import pytest

from chatlog_keeper.bot.providers import ProviderError
from chatlog_keeper.dashboard.usage import UsageLedger
from test_bot_required_read import TOOLS, call, compatible, response


META = {'run_id': 'run', 'group_ref': 'synthetic-group', 'bot_request_id': 'synthetic-request'}
TOKENS = {'prompt_tokens': 10, 'completion_tokens': 2, 'total_tokens': 12}


def configured(tmp_path, remote, **kwargs):
    provider = compatible(remote, **kwargs)
    provider.ledger = UsageLedger(tmp_path)
    provider.own_ledger = True
    statements = []
    provider.ledger.conn.set_trace_callback(statements.append)
    return provider, statements


def payload(message, usage=TOKENS):
    return httpx.Response(200, json={'id': 'response', 'choices': [{'message': message}], 'usage': usage},
                          headers={'x-request-id': 'request'})


def test_single_final_response_records_actual_call_and_finish_in_one_commit(tmp_path):
    provider, statements = configured(tmp_path, lambda _: payload({'content': 'done'}))
    try:
        provider.ledger.finish_run = lambda *_args, **_kwargs: pytest.fail('terminal response must use only fused finish')
        assert provider.complete([], '', TOOLS, lambda *_: pytest.fail('chat executes no tool'), META) == 'done'
        assert statements.count('COMMIT') == 2  # start, then actual call + finish.
        row = provider.ledger.get('run')
        assert row['status'] == 'completed' and row['usage']['total_tokens'] == 12
        assert provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
    finally:
        provider.close()


def test_intermediate_usage_is_full_committed_before_tools_final_usage_is_fused(tmp_path):
    requests, executions = [], []
    def remote(request):
        requests.append(json.loads(request.content))
        return payload({'tool_calls': [call()]}) if len(requests) == 1 else payload({'content': 'tool result reply'})
    provider, statements = configured(tmp_path, remote)
    def handler(name, args):
        executions.append(name)
        rows = provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0]
        assert rows == 1 and provider.ledger.get('run')['status'] == 'running'
        return {'ok': True}
    try:
        assert provider.complete([], '', TOOLS, handler, META) == 'tool result reply'
        assert executions == ['query_offer_status'] and len(requests) == 2
        assert statements.count('COMMIT') == 3  # start, intermediate, final call + finish.
        assert provider.ledger.get('run')['usage']['total_tokens'] == 24
        assert provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 2
    finally:
        provider.close()


@pytest.mark.parametrize('failure,error,known_tokens', [
    ('missing_choices', 'bot_provider_invalid_response', 12),
    ('invalid_json', 'bot_provider_invalid_response', None),
    ('output_limit', 'bot_provider_output_limit', None),
    ('http_error', 'bot_provider_rate_limit', 12),
    ('bad_tool', 'bot_unapproved_tool_call', 12),
    ('empty_final', 'bot_provider_empty_response', 12),
])
def test_terminal_failures_keep_the_returned_actual_or_unknown_usage(tmp_path, failure, error, known_tokens):
    def remote(_):
        if failure == 'missing_choices':
            return httpx.Response(200, json={'usage': TOKENS})
        if failure == 'invalid_json':
            return httpx.Response(200, text='{')
        if failure == 'output_limit':
            return httpx.Response(200, content=b'x' * (1024 * 1024 + 1))
        if failure == 'http_error':
            return httpx.Response(429, json={'usage': TOKENS})
        if failure == 'bad_tool':
            return payload({'tool_calls': [call('fetch_url')]})
        return payload({'content': ''})
    provider, statements = configured(tmp_path, remote)
    try:
        with pytest.raises(ProviderError, match=error):
            provider.complete([], '', TOOLS, lambda *_: pytest.fail('failed envelope cannot execute tool'), META)
        assert statements.count('COMMIT') == 2
        row = provider.ledger.get('run')
        assert row['status'] == 'failed'
        assert provider.ledger.conn.execute("SELECT error_code FROM usage_runs WHERE run_id='run'").fetchone()[0] == error
        assert (row['usage']['total_tokens'] if row['usage'] else None) == known_tokens
        assert provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
    finally:
        provider.close()


def test_tool_round_limit_fuses_fourth_actual_response_without_fourth_tool(tmp_path):
    executions = []
    provider, statements = configured(tmp_path, lambda _: payload({'tool_calls': [call()]}))
    try:
        with pytest.raises(ProviderError, match='bot_tool_round_limit'):
            provider.complete([], '', TOOLS, lambda *args: executions.append(args) or {'ok': True}, META)
        assert len(executions) == 3 and statements.count('COMMIT') == 5
        assert provider.ledger.get('run')['usage']['total_tokens'] == 48
        assert provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 4
    finally:
        provider.close()


def test_tool_failure_preserves_already_committed_usage_without_duplicate_call(tmp_path):
    provider, statements = configured(tmp_path, lambda _: payload({'tool_calls': [call()]}))
    def handler(*_):
        raise ValueError('synthetic tool execution error')
    try:
        with pytest.raises(ProviderError, match='bot_provider_invalid_response'):
            provider.complete([], '', TOOLS, handler, META)
        assert statements.count('COMMIT') == 3
        assert provider.ledger.get('run')['usage']['total_tokens'] == 12
        assert provider.ledger.conn.execute('SELECT COUNT(*) FROM usage_calls').fetchone()[0] == 1
    finally:
        provider.close()


def test_each_http_call_has_whitelisted_round_timing_and_aggregate(tmp_path):
    now, requests, events = [0], [], []
    def remote(request):
        requests.append(request)
        now[0] += len(requests)
        return payload({'tool_calls': [call()]}) if len(requests) == 1 else payload({'content': 'done'})
    provider, _ = configured(tmp_path, remote, clock=lambda: now[0])
    try:
        assert provider.complete([], '', TOOLS, lambda *_: {'ok': True}, {**META, '_on_event': events.append}) == 'done'
        model_events = [event for event in events if event.get('stage', '').startswith('model_call')]
        assert model_events == [
            {'type': 'timing', 'stage': 'model_call', 'seconds': 1},
            {'type': 'timing', 'stage': 'model_call_1', 'seconds': 1},
            {'type': 'timing', 'stage': 'model_call', 'seconds': 2},
            {'type': 'timing', 'stage': 'model_call_2', 'seconds': 2},
        ]
        assert any(event.get('stage') == 'usage_finish_with_call' for event in events)
    finally:
        provider.close()


def test_failed_http_attempt_also_emits_its_bounded_timing(tmp_path):
    now, events = [0], []
    def remote(request):
        now[0] = 4
        raise httpx.ReadTimeout('synthetic timeout', request=request)
    provider, _ = configured(tmp_path, remote, clock=lambda: now[0])
    try:
        with pytest.raises(ProviderError, match='bot_provider_timeout'):
            provider.complete([], '', TOOLS, lambda *_: None, {**META, '_on_event': events.append})
        assert [event for event in events if event.get('stage') == 'model_call_1'] == [
            {'type': 'timing', 'stage': 'model_call_1', 'seconds': 4}]
        assert provider.ledger.get('run')['status'] == 'failed'
    finally:
        provider.close()
