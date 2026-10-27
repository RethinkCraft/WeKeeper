"""Real cloud routes + durable local worker; all inference and chats synthetic."""
import json

from chatlog_keeper.dashboard.agent import AgentWorker
from chatlog_keeper.dashboard.usage import UsageLedger
from chatlog_keeper.dashboard.local_state import canonical_json
from test_dashboard_sync_integration import pipeline, TOKEN, STAMP


def test_cloud_queue_local_scoped_chat_usage_and_private_restore(pipeline):
    service, cloud, store, group, message = pipeline
    service.scan(); service.flush()
    config = service.state.directory / 'config.json'
    config.write_text(json.dumps(service.config))
    for name, role in [('viewer', 'viewer'), ('other', 'viewer'), ('admin', 'admin')]:
        store.create_user(name, 'synthetic-password-123', role)
    auth = cloud.post('/api/auth/login', json={'username': 'viewer', 'password': 'synthetic-password-123'}).json()
    cloud.headers['X-CSRF-Token'] = auth['csrf_token']
    conversation = cloud.post('/api/chat/conversations', json={'group_refs': [group], 'since': STAMP-100, 'until': STAMP+100}).json()['conversation']
    response = cloud.post('/api/chat/conversations/'+conversation['id']+'/turns', json={'content': 'Summarize this group', 'client_request_id': 'integration-turn'})
    assert response.status_code == 200
    turn = response.json()['turn']

    class Model:
        def __init__(self): self.calls = 0
        def chat(self, messages, **kwargs):
            self.calls += 1
            metadata = kwargs['run_metadata']
            assert metadata['defer_finalization'] is True
            ledger = UsageLedger(service.state.directory)
            try:
                run_id = ledger.start_run(model='synthetic-model', **{k: v for k, v in metadata.items() if k != 'defer_finalization'})
                ledger.record_call(run_id, usage={'input_tokens': 25, 'output_tokens': 5, 'total_tokens': 30}, response_id='synthetic-response')
            finally: ledger.close()
            assert kwargs['tool_handler']('get_statistics', {})['message_count'] == 1
            assert kwargs['tool_handler']('get_message_context', {'message_ref': message})['messages'][0]['message_ref'] == message
            kwargs['on_event']({'type': 'tool', 'name': 'get_statistics'})
            kwargs['on_event']({'type': 'delta', 'text': 'Synthetic analysis [['+message+']]'})
            return {'output_text': 'Synthetic analysis [['+message+']]'}

    model = Model()
    worker = AgentWorker(service.state.directory, client=service.client, siwc=model)
    try:
        assert worker.run_once()
        detail = cloud.get('/api/chat/conversations/'+conversation['id']).json()
        assert detail['turns'][0]['status'] == 'completed'
        assert detail['turns'][0]['message_refs'] == [message]
        usage = cloud.get('/api/usage', params={'since': 0, 'until': 2100000000}).json()
        assert usage['scope'] == 'self' and usage['totals']['total_tokens'] == 30
        assert usage['quota'] is None
        # Retry delivery and reconnect without a second inference.
        assert not worker.run_once() and model.calls == 1
        service.flush_usage()
        assert cloud.get('/api/usage', params={'since': 0, 'until': 2100000000}).json()['totals']['total_tokens'] == 30
        auth2 = cloud.post('/api/auth/login', json={'username': 'other', 'password': 'synthetic-password-123'}).json()
        cloud.headers['X-CSRF-Token'] = auth2['csrf_token']
        assert cloud.get('/api/chat/conversations/'+conversation['id']).status_code == 404
        assert cloud.get('/api/usage', params={'since': 0, 'until': 2100000000}).json()['totals']['requests'] == 0
    finally:
        worker.close()


def test_completed_local_answer_survives_restart_and_expired_cloud_lease(pipeline):
    service, cloud, store, group, message = pipeline
    service.scan(); service.flush()
    (service.state.directory / 'config.json').write_text(json.dumps(service.config))
    user = store.create_user('viewer', 'synthetic-password-123', 'viewer')
    conversation = store.create_conversation(user['id'], {'group_refs': [group], 'since': STAMP-100, 'until': STAMP+100})
    turn = store.create_turn(conversation['id'], user['id'], {'content': 'synthetic', 'client_request_id': 'late-answer'})
    worker = AgentWorker(service.state.directory, client=service.client, siwc=object())
    try:
        job = worker.post('/v1/agent/jobs/claim', {'source_id': service.state.source_id, 'worker_id': worker.worker_id, 'group_refs': [group]})['job']
        worker.conn.execute("INSERT INTO jobs VALUES(?,?,'running',?)", (job['turn_id'], canonical_json(job), job['next_seq'])); worker.conn.commit()
        ledger = UsageLedger(service.state.directory)
        run_id = ledger.start_run(kind='chat', user_id=user['id'], turn_id=turn['id'], model='synthetic-model')
        ledger.record_call(run_id, usage={'input_tokens': 10, 'output_tokens': 2, 'total_tokens': 12}); ledger.close()
        worker.event(turn['id'], 'delta', text='partial')
        worker.event(turn['id'], 'completed', text='known answer [['+message+']]', message_refs=[message])
        worker.close()
        current = store.clock()
        store.clock = lambda: current+95
        store.expire_chat_jobs()
        assert store.chat_turn(turn['id'], user['id'])['status'] == 'interrupted'
        worker = AgentWorker(service.state.directory, client=service.client, siwc=object())
        worker.recover()
        final = store.chat_turn(turn['id'], user['id'])
        assert final['status'] == 'completed' and final['response_text'] == 'known answer [['+message+']]'
        assert worker.conn.execute('SELECT COUNT(*) FROM events WHERE delivered=0').fetchone()[0] == 0
        ledger = UsageLedger(service.state.directory)
        assert ledger.get(run_id)['status'] == 'completed' and ledger.get(run_id)['usage']['total_tokens'] == 12
        ledger.close()
    finally:
        worker.close()
