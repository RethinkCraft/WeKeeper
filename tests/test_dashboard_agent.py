import base64
import hashlib
import io
import json

import httpx
from PIL import Image
import pytest

from chatlog_keeper.dashboard.agent import AgentWorker, ScopedArchive
from chatlog_keeper.dashboard.avatars import avatar_projections
from chatlog_keeper.dashboard.local_state import LocalState, canonical_json
from chatlog_keeper.dashboard.quota import CodexQuotaReader, _bucket
from test_dashboard_sync import archive_fixture
from test_p2_archive import blob


def configured(path):
    state = LocalState(path)
    (path / 'config.json').write_text(json.dumps({'server_url': 'https://example.test', 'groups': ['group_a'], 'machine_token': 'x' * 40}))
    data = {'group': {'group_ref': 'group_a', 'name': 'A'}, 'day': '2026-10-26', 'coverage': {}, 'messages': [
        {'message_ref': 'msg_before', 'create_time': 9, 'sender_id': 'sender_a', 'sender_name': 'A', 'text': 'outside'},
        {'message_ref': 'msg_inside', 'create_time': 11, 'sender_id': 'sender_a', 'sender_name': 'A', 'text': 'inside'},
        {'message_ref': 'msg_after', 'create_time': 21, 'sender_id': 'sender_a', 'sender_name': 'A', 'text': 'outside'}]}
    state.queue(data)
    return state


def test_chat_tools_cannot_escape_frozen_scope_or_revoked_allowlist(tmp_path):
    state = configured(tmp_path)
    archive = ScopedArchive(state, {'group_refs': ['group_a'], 'since': 10, 'until': 20}, tmp_path / 'config.json')
    assert archive.call('get_statistics', {})['message_count'] == 1
    assert archive.call('search_messages', {'query': '', 'limit': 50})['messages'][0]['message_ref'] == 'msg_inside'
    assert archive.call('get_message_context', {'message_ref': 'msg_before'}) == {'error': 'message_outside_selection'}
    assert len(archive.call('get_message_context', {'message_ref': 'msg_inside', 'before': 10, 'after': 10})['messages']) == 1
    value = json.loads((tmp_path / 'config.json').read_text()); value['groups'] = ['group_other']
    (tmp_path / 'config.json').write_text(json.dumps(value))
    with pytest.raises(ValueError, match='not_allowed'):
        archive.call('search_messages', {})
    state.close()


def test_restart_reports_interrupted_without_reexecuting_model(tmp_path):
    state = configured(tmp_path); source = state.source_id; state.close()
    sent = []
    def transport(request):
        body = json.loads(request.content)
        if request.url.path.endswith('/events'):
            sent.extend(body['events'])
            return httpx.Response(200, json={'accepted': True, 'last_seq': body['events'][-1]['seq']})
        return httpx.Response(200, json={'job': None})
    class NoInference:
        def chat(self, *a, **kw):
            pytest.fail('an uncertain inference must never be replayed')
    worker = AgentWorker(tmp_path, client=httpx.Client(transport=httpx.MockTransport(transport)), siwc=NoInference())
    job = {'turn_id': 'turn_a', 'lease_token': 'lease', 'source_id': source}
    worker.conn.execute("INSERT INTO jobs VALUES(?,?,'running',1)", ('turn_a', canonical_json(job))); worker.conn.commit()
    assert not worker.run_once()
    assert sent == [{'seq': 1, 'type': 'interrupted', 'error': 'local_worker_restarted'}]
    worker.run_once()
    assert len(sent) == 1
    worker.close()


def test_restart_preserves_completed_frame_even_if_job_state_write_was_interrupted(tmp_path):
    state = configured(tmp_path); state.close()
    sent = []
    def transport(request):
        body = json.loads(request.content)
        sent.extend(body['events'])
        assert [e['type'] for e in body['events']] == ['completed']
        return httpx.Response(200, json={'accepted': True, 'last_seq': 1})
    worker = AgentWorker(tmp_path, client=httpx.Client(transport=httpx.MockTransport(transport)), siwc=object())
    job = {'turn_id': 'turn_a', 'lease_token': 'lease'}
    worker.conn.execute("INSERT INTO jobs VALUES(?,?,'running',1)", ('turn_a', canonical_json(job))); worker.conn.commit()
    worker.event('turn_a', 'completed', text='known answer', message_refs=[])
    # Simulate a prior release crashing between event and status persistence.
    worker.conn.execute("UPDATE jobs SET status='running'"); worker.conn.commit()
    worker.close()
    worker = AgentWorker(tmp_path, client=httpx.Client(transport=httpx.MockTransport(transport)), siwc=object())
    worker.recover()
    assert len(sent) == 1
    assert worker.conn.execute('SELECT status FROM jobs').fetchone()[0] == 'completed'
    worker.close()


def test_lost_ack_replays_identical_events_and_preserves_finished_result(tmp_path):
    state = configured(tmp_path); state.close()
    received = []
    def transport(request):
        body = json.loads(request.content); received.append(body)
        if len(received) == 1:
            raise httpx.ReadTimeout('lost acknowledgement')
        return httpx.Response(200, json={'accepted': True, 'last_seq': 1})
    worker = AgentWorker(tmp_path, client=httpx.Client(transport=httpx.MockTransport(transport)), siwc=object())
    job = {'turn_id': 'turn_a', 'lease_token': 'lease'}
    worker.conn.execute("INSERT INTO jobs VALUES(?,?,'completed',1)", ('turn_a', canonical_json(job))); worker.conn.commit()
    worker.event('turn_a', 'completed', text='answer', message_refs=[])
    with pytest.raises(httpx.ReadTimeout): worker.flush(job)
    worker.close()
    worker = AgentWorker(tmp_path, client=httpx.Client(transport=httpx.MockTransport(transport)), siwc=object())
    worker.recover()
    assert received[0] == received[1]
    assert worker.conn.execute('SELECT delivered FROM events').fetchone()[0] == 1
    worker.close()


def test_chat_worker_streams_validated_result_and_never_repeats_finished_turn(tmp_path):
    state = configured(tmp_path); state.close()
    events, claims = [], []
    job = {'turn_id': 'turn_a', 'conversation_id': 'conversation_a', 'user_id': 1,
           'scope': {'group_refs': ['group_a'], 'since': 10, 'until': 20},
           'history': [], 'prompt': 'Summarize this selection', 'lease_token': 'lease', 'next_seq': 1}
    def transport(request):
        body = json.loads(request.content)
        if request.url.path.endswith('/claim'):
            claims.append(body)
            return httpx.Response(200, json={'job': job if len(claims) == 1 else None})
        if request.url.path.endswith('/events'):
            events.extend(body['events'])
            return httpx.Response(200, json={'accepted': True, 'last_seq': body['events'][-1]['seq'], 'cancel_requested': False})
        return httpx.Response(200, json={'accepted': True})
    class Model:
        def __init__(self): self.calls = 0
        def chat(self, messages, **kw):
            self.calls += 1
            assert kw['tool_handler']('get_statistics', {})['message_count'] == 1
            assert kw['context']['recent_evidence']['messages'][0]['message_ref'] == 'msg_inside'
            kw['on_event']({'type': 'delta', 'text': 'Answer [[msg_inside]]'})
            return {'output_text': 'Answer [[msg_inside]]'}
    model = Model()
    worker = AgentWorker(tmp_path, client=httpx.Client(transport=httpx.MockTransport(transport)), siwc=model)
    assert worker.run_once()
    assert events[-1]['type'] == 'completed' and events[-1]['message_refs'] == ['msg_inside']
    assert not worker.run_once() and model.calls == 1
    assert not (tmp_path / 'chat-waiting').exists()
    worker.close()


def test_avatar_exports_correct_identity_and_only_reencoded_thumbnail(tmp_path):
    store, group = archive_fixture(tmp_path / 'archive')
    out = io.BytesIO(); Image.new('RGB', (300, 200), 'blue').save(out, format='PNG')
    cached = blob(store, out.getvalue())
    store.upsert_profiles('private_account', [{'subject_id': 'wxid_private', 'profile': {}, 'avatar': {
        'relative_path': cached['raw_path'], 'sha256': cached['raw_sha256'], 'mime': 'image/png', 'width': 300, 'height': 200}}])
    items = avatar_projections(tmp_path / 'archive', [group], 'ab' * 32, 'source')
    assert len(items) == 1
    item = items[0]; encoded = json.dumps(item)
    assert 'wxid_private' not in encoded and 'private_account' not in encoded and 'relative_path' not in encoded
    image_data = base64.b64decode(item['content_base64'])
    assert item['avatar_hash'] == hashlib.sha256(image_data).hexdigest()
    with Image.open(io.BytesIO(image_data)) as image:
        assert max(image.size) <= 128 and image.format == 'JPEG'
    (store.media_dir / cached['raw_path']).unlink()
    assert avatar_projections(tmp_path / 'archive', [group], 'ab' * 32, 'source') == []
    store.close()


def test_avatar_queue_late_ack_cannot_remove_updated_thumbnail(tmp_path):
    state = LocalState(tmp_path)
    state.queue_feature('avatar', 'group:sender', {'avatar_hash': 'old'}, 'old')
    state.queue_feature('avatar', 'group:sender', {'avatar_hash': 'new'}, 'new')
    state.acknowledge_feature('avatar', 'group:sender', 'old')
    assert state.features('avatar')[0]['data']['avatar_hash'] == 'new'
    state.acknowledge_feature('avatar', 'group:sender', 'new')
    assert not state.features('avatar')
    state.retry_feature('avatar', 'group:sender')
    assert state.features('avatar')
    state.close()


def test_permanently_rejected_delivery_does_not_block_other_jobs(tmp_path):
    state = configured(tmp_path); state.close()
    claims = []
    def transport(request):
        if request.url.path.endswith('/events'):
            return httpx.Response(422, json={'detail': 'invalid_request'})
        claims.append(True)
        return httpx.Response(200, json={'job': None})
    worker = AgentWorker(tmp_path, client=httpx.Client(transport=httpx.MockTransport(transport)), siwc=object())
    job = {'turn_id': 'bad_turn', 'lease_token': 'lease'}
    worker.conn.execute("INSERT INTO jobs VALUES(?,?,'completed',1)", ('bad_turn', canonical_json(job))); worker.conn.commit()
    worker.event('bad_turn', 'completed', text='retained rejected answer', message_refs=[])
    assert not worker.run_once() and claims == [True]
    assert worker.conn.execute('SELECT COUNT(*) FROM delivery_errors').fetchone()[0] == 1
    assert worker.conn.execute('SELECT delivered FROM events').fetchone()[0] == 0
    assert not worker.run_once() and len(claims) == 2
    worker.close()


def test_quota_exports_only_official_windows_and_marks_missing_login(tmp_path, monkeypatch):
    assert _bucket({'primary': {'usedPercent': 9, 'windowDurationMins': 10080, 'resetsAt': 123}, 'email': 'secret', 'token': 'secret'}) == {
        'limitId': None, 'limitName': None, 'planType': None, 'rateLimitReachedType': None,
        'primary': {'usedPercent': 9, 'windowDurationMins': 10080, 'resetsAt': 123}, 'secondary': None}
    reader = CodexQuotaReader(clock=lambda: 100)
    monkeypatch.setattr(reader, '_start', lambda: None)
    monkeypatch.setattr(reader, '_rpc', lambda *a: {'account': None})
    value = reader.snapshot('source')
    assert value['auth_state'] == 'unavailable' and value['error'] == 'codex_chatgpt_login_required'
    assert value['sampled_at'] == 100
