"""Synthetic cloud feature contracts: identity, privacy, jobs and usage."""
import base64
from datetime import datetime
import hashlib
import io
import json

import pytest
from fastapi.testclient import TestClient
from PIL import Image

from chatlog_keeper.dashboard.api import create_app
from chatlog_keeper.dashboard.protocol import content_hash
from chatlog_keeper.dashboard.store import Conflict, DashboardStore

NOW = datetime.fromisoformat('2026-10-26T12:00:00+08:00').timestamp()
TOKEN = 'a' * 40


def partition(group='group_a', source='source_a', messages=None):
    rows = messages if messages is not None else [dict(message_ref='msg_a', create_time=int(NOW-10), sender_id='sender_a', sender_name='Old name', msg_type=1, text='synthetic fixture', reply=None)]
    data = dict(group=dict(group_ref=group,name='Synthetic group'), day='2026-10-26', messages=rows, coverage=dict(local_backfill_complete=True))
    return dict(source_id=source,group_ref=group,day=data['day'],revision=1,content_hash=content_hash(data),initial=True,data=data)


def scope():
    return dict(group_refs=['group_a'],since=NOW-3600,until=NOW+3600)


def avatar(color='red', **changes):
    stream=io.BytesIO()
    Image.new('RGB',(64,64),color).save(stream,format='JPEG')
    data=stream.getvalue()
    return dict(source_id='source_a',group_ref='group_a',sender_id='sender_a',avatar_hash=hashlib.sha256(data).hexdigest(),mime_type='image/jpeg',content_base64=base64.b64encode(data).decode(),observed_at=NOW,**changes)


@pytest.fixture
def store(tmp_path):
    clock=[NOW]
    store=DashboardStore(tmp_path,clock=lambda:clock[0])
    store.test_clock=clock
    store.admin=store.create_user('admin','long-admin-password')
    store.viewer=store.create_user('viewer','long-viewer-password','viewer')
    store.other=store.create_user('other','long-other-password','viewer')
    store.ingest_partition(partition())
    yield store
    store.close()


def turn(store, user=None, content='Synthetic question', request='request_1'):
    user=user or store.viewer
    conversation=store.create_conversation(user['id'],dict(title='Synthetic',**scope()))
    value=store.create_turn(conversation['id'],user['id'],dict(content=content,client_request_id=request))
    return conversation,value


def claim(store):
    return store.claim_job(dict(source_id='source_a',worker_id='worker_a',group_refs=['group_a']))['job']


def events(store,job,*items):
    return store.append_job_events(job['turn_id'],dict(source_id='source_a',lease_token=job['lease_token'],events=list(items)))


def test_private_avatar_identity_hash_and_newest_name(store):
    row=partition()['data']['messages'][0]
    old=dict(row,sender_name='zzz old',create_time=int(NOW-30))
    new=dict(row,message_ref='msg_new',sender_name='aaa current',create_time=int(NOW-5))
    value=partition(messages=[old,new]);value['revision']=2
    store.ingest_partition(value)
    assert store.group_stats('group_a')['senders'][0]['sender_name']=='aaa current'
    assert store.group_stats('group_a')['senders'][0]['avatar_url'] is None
    first=avatar()
    assert store.ingest_avatar(first)['applied']
    assert not store.ingest_avatar(first)['applied']
    assert store.group_stats('group_a')['senders'][0]['avatar_url'].endswith(first['avatar_hash'])
    assert store.messages('group_a',NOW-100,NOW)['messages'][0]['avatar_url']
    assert store.avatar('group_a','sender_a')['avatar_hash']==first['avatar_hash']
    with pytest.raises(KeyError):store.avatar('group_a','sender_b')
    with pytest.raises(KeyError):store.avatar('group_a','sender_a','0'*64)
    bad=dict(first,source_id='source_other')
    with pytest.raises(Conflict):store.ingest_avatar(bad)
    bad=dict(first,content_base64=base64.b64encode(b'not an image').decode(),avatar_hash=hashlib.sha256(b'not an image').hexdigest())
    with pytest.raises(ValueError,match='image'):store.ingest_avatar(bad)
    # An older cached observation cannot replace a newer avatar.
    newer=avatar('blue');newer['observed_at']=NOW+10
    store.ingest_avatar(newer)
    assert not store.ingest_avatar(first)['applied']
    assert store.avatar('group_a','sender_a')['avatar_hash']==newer['avatar_hash']
    heartbeat=dict(source_id='source_a',sent_at=NOW,health=dict(state='session_present'),collector=dict(running=True,last_cycle_at=NOW))
    assert store.ingest_heartbeat(heartbeat)['avatars']==[dict(group_ref='group_a',sender_id='sender_a',avatar_hash=newer['avatar_hash'])]


def test_chat_owner_isolation_and_one_active_turn_per_user(store):
    conversation,value=turn(store)
    assert value['status']=='queued'
    replay=store.create_turn(conversation['id'],store.viewer['id'],dict(content='Synthetic question',client_request_id='request_1'))
    assert replay['id']==value['id']
    assert store.conversations(store.other['id'])==[]
    assert store.conversations(store.admin['id'])==[]
    for user in (store.other,store.admin):
        with pytest.raises(KeyError):store.conversation(conversation['id'],user['id'])
        with pytest.raises(KeyError):store.chat_turn(value['id'],user['id'])
        with pytest.raises(KeyError):store.cancel_turn(value['id'],user['id'])
    another=store.create_conversation(store.viewer['id'],dict(**scope()))
    with pytest.raises(Conflict,match='in_progress'):
        store.create_turn(another['id'],store.viewer['id'],dict(content='again',client_request_id='request_2'))
    # Other users are not blocked by this user's active turn.
    turn(store,store.other,request='other_request')
    with pytest.raises(Conflict,match='request_id'):
        store.create_turn(conversation['id'],store.viewer['id'],dict(content='changed',client_request_id='request_1'))


def test_job_lease_sequence_replay_scope_and_completed_result(store):
    _,value=turn(store)
    job=claim(store)
    assert job['turn_id']==value['id'] and job['next_seq']==1
    assert claim(store) is None
    assert job['scope']==scope()
    assert 'lease_token' not in store.chat_turn(value['id'],store.viewer['id'])
    event=dict(seq=1,type='delta',text='partial')
    events(store,job,event)
    events(store,job,event)
    assert store.chat_turn(value['id'],store.viewer['id'])['response_text']=='partial'
    with pytest.raises(Conflict,match='sequence_conflict'):
        events(store,job,dict(seq=1,type='delta',text='different'))
    with pytest.raises(Conflict,match='sequence_gap'):
        events(store,job,dict(seq=3,type='heartbeat'))
    with pytest.raises(ValueError,match='outside_scope'):
        events(store,job,dict(seq=2,type='completed',text='bad',message_refs=['msg_unknown']))
    done=dict(seq=2,type='completed',text='Verified final',message_refs=['msg_a'])
    assert events(store,job,done)['status']=='completed'
    assert events(store,job,done)['last_seq']==2
    result=store.chat_turn(value['id'],store.viewer['id'])
    assert result['response_text']=='Verified final' and result['message_refs']==['msg_a']
    assert len(store.chat_events(value['id'],store.viewer['id']))==2
    with pytest.raises(Conflict,match='terminal'):
        events(store,job,dict(seq=3,type='delta',text='late'))
    assert claim(store) is None


def test_expired_claim_becomes_interrupted_never_automatically_reclaimed(store):
    _,value=turn(store)
    job=claim(store)
    events(store,job,dict(seq=1,type='delta',text='unfinished'))
    store.test_clock[0]+=91
    assert claim(store) is None
    result=store.chat_turn(value['id'],store.viewer['id'])
    assert result['status']=='interrupted' and result['error']=='worker_lease_expired'
    assert store.chat_events(value['id'],store.viewer['id'])[-1]['type']=='interrupted'
    assert store.chat_turn(value['id'],store.viewer['id'])['response_text']=='unfinished'
    with pytest.raises(Conflict):events(store,job,dict(seq=2,type='completed',text='late',message_refs=[]))
    turn(store,request='request_after_interrupt')


def test_queued_and_running_cancel_and_private_multiturn_history(store):
    conversation,value=turn(store)
    assert store.cancel_turn(value['id'],store.viewer['id'])['status']=='cancelled'
    assert claim(store) is None
    next_turn=store.create_turn(conversation['id'],store.viewer['id'],dict(content='second',client_request_id='request_2'))
    job=claim(store)
    assert job['history']==[]
    store.cancel_turn(next_turn['id'],store.viewer['id'])
    assert events(store,job,dict(seq=1,type='heartbeat'))['cancel_requested']
    events(store,job,dict(seq=2,type='cancelled'))
    third=store.create_turn(conversation['id'],store.viewer['id'],dict(content='third',client_request_id='request_3'))
    job=claim(store)
    events(store,job,dict(seq=1,type='completed',text='third answer',message_refs=[]))
    store.create_turn(conversation['id'],store.viewer['id'],dict(content='fourth',client_request_id='request_4'))
    job=claim(store)
    assert job['history']==[dict(role='user',content='third'),dict(role='assistant',content='third answer')]
    assert job['prompt']=='fourth' and job['user_id']==store.viewer['id']


def usage_run(**changes):
    value=dict(run_id='summary_1',kind='summary',user_id=None,turn_id=None,model='test-model',started_at=NOW-10,completed_at=NOW,status='completed',usage=dict(input_tokens=100,cached_input_tokens=20,output_tokens=30,reasoning_output_tokens=10,total_tokens=130))
    return dict(value,**changes)


def test_usage_immutable_idempotency_admin_and_viewer_scope(store):
    _,value=turn(store)
    job=claim(store)
    chat=usage_run(run_id='chat_1',kind='chat',user_id=store.viewer['id'],turn_id=value['id'])
    summary=usage_run()
    batch=dict(source_id='source_a',runs=[chat,summary])
    assert store.ingest_usage(batch)['applied']==2
    assert store.ingest_usage(batch)['applied']==0
    with pytest.raises(Conflict):store.ingest_usage(dict(source_id='source_a',runs=[dict(summary,usage=None)]))
    with pytest.raises(ValueError,match='owner'):store.ingest_usage(dict(source_id='source_a',runs=[dict(chat,run_id='forged',user_id=store.other['id'])]))
    with pytest.raises(ValueError,match='owner'):store.ingest_usage(dict(source_id='source_other',runs=[dict(chat,run_id='forged_source')]))
    own=store.usage(store.viewer['id'],False,NOW-100,NOW)
    assert own['scope']=='self' and own['totals']['requests']==1
    assert own['totals']['total_tokens']==130 and own['quota'] is None
    assert own['recent'][0]['kind']=='chat'
    assert store.usage(store.other['id'],False,NOW-100,NOW)['totals']['requests']==0
    admin=store.usage(store.admin['id'],True,NOW-100,NOW)
    assert admin['totals']['requests']==2 and admin['totals']['total_tokens']==260
    assert {row['kind'] for row in admin['by_kind']}=={'summary','chat'}


def test_unavailable_usage_is_not_fabricated_zero_and_batch_is_atomic(store):
    store.ingest_usage(dict(source_id='source_a',runs=[usage_run(usage=None)]))
    total=store.usage(store.admin['id'],True,NOW-100,NOW)['totals']
    assert total['requests']==1 and total['total_tokens'] is None
    assert total['usage_unavailable_requests']==1 and total['usage_available_requests']==0
    bad=usage_run(run_id='bad',kind='chat',turn_id='missing',user_id=store.viewer['id'])
    with pytest.raises(ValueError):store.ingest_usage(dict(source_id='source_a',runs=[usage_run(run_id='would_insert'),bad]))
    assert store.usage(store.admin['id'],True,NOW-100,NOW)['totals']['requests']==1


def quota(now=NOW, **changes):
    return dict(dict(source_id='source_a',provider='codex_app_server',sampled_at=now,auth_state='ready',rate_limits={'rateLimits':dict(primary=dict(usedPercent=25,windowDurationMins=10080,resetsAt=NOW+86400)),'rateLimitsByLimitId':None},error=None),**changes)


def test_official_quota_staleness_and_failed_poll_preserves_last_success(store):
    assert store.ingest_quota(quota())['applied']
    assert not store.ingest_quota(quota())['applied']
    def snapshot():return store.usage(store.admin['id'],True,NOW-100,NOW+1000)['quota']['snapshots'][0]
    assert snapshot()['stale'] is False
    store.test_clock[0]+=90
    assert not snapshot()['stale']
    store.test_clock[0]+=1
    assert snapshot()['stale']
    store.ingest_quota(quota(store.clock(),auth_state='unavailable',rate_limits=None,error='official_source_unavailable'))
    old=snapshot()
    assert old['stale'] and old['sampled_at']==NOW and old['checked_at']==NOW+91
    assert old['rate_limits']['rateLimits']['primary']['usedPercent']==25
    assert old['age_seconds']==91
    assert store.usage(store.viewer['id'],False,NOW-100,NOW+1000)['quota'] is None
    assert not store.ingest_quota(quota(NOW-1))['applied']
    with pytest.raises(ValueError,match='future'):store.ingest_quota(quota(NOW+1000))


def test_schema_migration_reopens_and_persists_private_features(tmp_path):
    first=DashboardStore(tmp_path,clock=lambda:NOW)
    user=first.create_user('viewer','long-viewer-password','viewer')
    first.ingest_partition(partition())
    conversation=first.create_conversation(user['id'],dict(**scope()))
    first.ingest_avatar(avatar())
    first.close()
    reopened=DashboardStore(tmp_path,clock=lambda:NOW)
    try:
        assert reopened.conn.execute('PRAGMA user_version').fetchone()[0]==2
        assert reopened.conversation(conversation['id'],user['id'])['conversation']['id']==conversation['id']
        assert reopened.avatar('group_a','sender_a')['mime_type']=='image/jpeg'
    finally:reopened.close()


@pytest.fixture
def client(tmp_path):
    app=create_app(tmp_path,TOKEN)
    app.state.store.clock=lambda:NOW
    for username,role in (('admin','admin'),('viewer','viewer'),('other','viewer')):
        app.state.store.create_user(username,f'long-{username}-password',role)
    app.state.store.ingest_partition(partition())
    with TestClient(app,base_url='https://testserver') as client:
        client.app_store=app.state.store
        yield client


def login(client, username='viewer'):
    response=client.post('/api/auth/login',json=dict(username=username,password=f'long-{username}-password'))
    assert response.status_code==200
    client.headers['X-CSRF-Token']=response.json()['csrf_token']
    return response.json()['user']


def test_api_avatar_requires_session_and_machine_upload(client):
    value=avatar()
    endpoint='/api/groups/group_a/senders/sender_a/avatar'
    assert client.get(endpoint).status_code==401
    assert client.post('/v1/ingest/avatars',json=value).status_code==401
    login(client)
    assert client.post('/v1/ingest/avatars',json=value).status_code==401
    assert client.post('/v1/ingest/avatars',json=value,headers={'Authorization':'Bearer '+TOKEN}).status_code==200
    response=client.get(endpoint)
    assert response.status_code==200 and response.headers['content-type']=='image/jpeg'
    assert 'no-store' in response.headers['cache-control']
    assert client.get(endpoint+'?hash='+'0'*64).status_code==404


def test_api_chat_private_ownership_csrf_and_finite_sse_replay(client):
    user=login(client)
    created=client.post('/api/chat/conversations',json=dict(**scope()))
    assert created.status_code==200
    conversation=created.json()['conversation']
    csrf=client.headers.pop('X-CSRF-Token')
    assert client.post('/api/chat/conversations/'+conversation['id']+'/turns',json=dict(content='q',client_request_id='a')).status_code==403
    client.headers['X-CSRF-Token']=csrf
    value=client.post('/api/chat/conversations/'+conversation['id']+'/turns',json=dict(content='q',client_request_id='a')).json()['turn']
    worker=client.post('/v1/agent/jobs/claim',json=dict(source_id='source_a',worker_id='w',group_refs=['group_a']),headers={'Authorization':'Bearer '+TOKEN}).json()['job']
    body=dict(source_id='source_a',lease_token=worker['lease_token'],events=[dict(seq=1,type='delta',text='prefix'),dict(seq=2,type='completed',text='final',message_refs=['msg_a'])])
    assert client.post('/v1/agent/jobs/'+value['id']+'/events',json=body,headers={'Authorization':'Bearer '+TOKEN}).status_code==200
    stream=client.get('/api/chat/turns/'+value['id']+'/events?after=1')
    assert stream.status_code==200 and 'event: completed' in stream.text
    assert 'event: delta' not in stream.text
    login(client,'other')
    assert client.get('/api/chat/conversations/'+conversation['id']).status_code==404
    assert client.get('/api/chat/turns/'+value['id']+'/events').status_code==404
    login(client,'admin')
    assert client.get('/api/chat/conversations/'+conversation['id']).status_code==404


def test_api_usage_permissions_and_quota_protocol(client):
    headers={'Authorization':'Bearer '+TOKEN}
    assert client.post('/v1/ingest/usage',json=dict(source_id='source_a',runs=[usage_run()]),headers=headers).status_code==200
    assert client.post('/v1/ingest/quota',json=quota(),headers=headers).status_code==200
    login(client)
    view=client.get('/api/usage').json()
    assert view['scope']=='self' and view['totals']['requests']==0 and view['quota'] is None
    login(client,'admin')
    view=client.get('/api/usage').json()
    assert view['scope']=='all' and view['totals']['requests']==1
    assert view['quota']['binding']=='unverified'
    assert view['quota']['snapshots'][0]['rate_limits']['rateLimits']['primary']['usedPercent']==25
    bad=quota();bad['provider']='guessed_from_siwwc'
    assert client.post('/v1/ingest/quota',json=bad,headers=headers).status_code==422


def test_quota_actual_reader_payload_keeps_reached_type_and_rejects_unapproved_data(client):
    from chatlog_keeper.dashboard.quota import _bucket
    raw=dict(limitId='codex',limitName='Codex',planType='pro',rateLimitReachedType=None,
             normalModelSlug='must_not_be_exported',primary=dict(usedPercent=9,windowDurationMins=10080,resetsAt=NOW+600),secondary=None)
    value=quota(rate_limits=dict(rateLimits=_bucket(raw),rateLimitsByLimitId={'codex':_bucket(raw)}))
    assert 'normalModelSlug' not in value['rate_limits']['rateLimits']
    response=client.post('/v1/ingest/quota',json=value,headers={'Authorization':'Bearer '+TOKEN})
    assert response.status_code==200
    login(client,'admin')
    snapshot=client.get('/api/usage').json()['quota']['snapshots'][0]
    assert snapshot['rate_limits']['rateLimits']['primary']['usedPercent']==9
    assert snapshot['rate_limits']['rateLimits']['rateLimitReachedType'] is None
    value['rate_limits']['rateLimits']['raw_access_token']='should_not_echo'
    invalid=client.post('/v1/ingest/quota',json=value,headers={'Authorization':'Bearer '+TOKEN})
    assert invalid.status_code==422 and 'should_not_echo' not in invalid.text


def test_disabling_user_cancels_queued_and_signals_running_jobs(store):
    _,queued=turn(store)
    store.update_user(store.viewer['id'],dict(disabled=True))
    assert store.chat_turn(queued['id'],store.viewer['id'])['status']=='cancelled'
    assert claim(store) is None
    _,running=turn(store,store.other,request='other_running')
    job=claim(store)
    store.update_user(store.other['id'],dict(disabled=True))
    result=events(store,job,dict(seq=1,type='heartbeat'))
    assert result['cancel_requested'] and result['status']=='cancelling'


def test_empty_official_quota_result_does_not_clear_previous_values_or_freshen(store):
    store.ingest_quota(quota())
    store.test_clock[0]+=30
    store.ingest_quota(quota(store.clock(),rate_limits={'rateLimits':None,'rateLimitsByLimitId':None}))
    value=store.usage(store.admin['id'],True,NOW-100,NOW+1000)['quota']['snapshots'][0]
    assert value['sampled_at']==NOW and value['checked_at']==NOW+30
    assert value['stale'] and value['rate_limits']['rateLimits']['primary']['usedPercent']==25


def test_completed_result_survives_95_second_disconnect_without_reexecution(store):
    _,value=turn(store)
    job=claim(store)
    events(store,job,dict(seq=1,type='delta',text='partial'))
    # The worker has a durable completion, but its delivery connection was lost.
    completed=dict(source_id='source_a',lease_token=job['lease_token'],text='durable completed answer',message_refs=['msg_a'])
    store.test_clock[0]+=95
    store.expire_chat_jobs()
    assert store.chat_turn(value['id'],store.viewer['id'])['status']=='interrupted'
    assert claim(store) is None  # No inference is ever automatically requeued.
    result=store.restore_job_result(value['id'],completed)
    assert result['status']=='completed' and result['last_seq']==3
    assert not store.restore_job_result(value['id'],completed)['applied']
    restored=store.chat_turn(value['id'],store.viewer['id'])
    assert restored['response_text']=='durable completed answer' and restored['error'] is None
    history=store.chat_events(value['id'],store.viewer['id'])
    assert [e['type'] for e in history]==['delta','interrupted','completed']
    assert history[-1]['message_refs']==['msg_a']
    with pytest.raises(Conflict,match='result_conflict'):
        store.restore_job_result(value['id'],dict(completed,text='different result'))


def test_result_recovery_preserves_cancel_ownership_and_scope(store):
    _,value=turn(store)
    job=claim(store)
    result=dict(source_id='source_a',lease_token=job['lease_token'],text='result',message_refs=[])
    with pytest.raises(Conflict,match='lease'):
        store.restore_job_result(value['id'],dict(result,lease_token='wrong'))
    with pytest.raises(Conflict,match='lease'):
        store.restore_job_result(value['id'],dict(result,source_id='other_source'))
    with pytest.raises(ValueError,match='outside_scope'):
        store.restore_job_result(value['id'],dict(result,message_refs=['msg_not_in_scope']))
    store.cancel_turn(value['id'],store.viewer['id'])
    store.test_clock[0]+=95
    with pytest.raises(Conflict,match='cancelled'):
        store.restore_job_result(value['id'],result)
    assert store.chat_turn(value['id'],store.viewer['id'])['status']=='interrupted'


def test_result_recovery_route_is_machine_only(client):
    login(client)
    conversation=client.post('/api/chat/conversations',json=scope()).json()['conversation']
    value=client.post('/api/chat/conversations/'+conversation['id']+'/turns',json=dict(content='q',client_request_id='recover_route')).json()['turn']
    job=client.post('/v1/agent/jobs/claim',json=dict(source_id='source_a',worker_id='w',group_refs=['group_a']),headers={'Authorization':'Bearer '+TOKEN}).json()['job']
    body=dict(source_id='source_a',lease_token=job['lease_token'],text='answer',message_refs=['msg_a'])
    path='/v1/agent/jobs/'+value['id']+'/result'
    assert client.post(path,json=body).status_code==401
    assert client.post(path,json=body,headers={'Authorization':'Bearer '+TOKEN}).status_code==200
    assert not client.post(path,json=body,headers={'Authorization':'Bearer '+TOKEN}).json()['applied']
