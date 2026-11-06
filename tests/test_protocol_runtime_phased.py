"""Calibrated client work must overlap rather than lengthen the account gate."""
import importlib.util
from pathlib import Path

import pytest
from chatlog_keeper.bot.protocol_gateway import GatewayError, SendResult

spec=importlib.util.spec_from_file_location('phased_bench',Path(__file__).parents[1]/'scripts/benchmarks/benchmark_protocol_runtime_phased.py')
bench=importlib.util.module_from_spec(spec);spec.loader.exec_module(bench)

class Clock:
    now=100.
    def __call__(self):return self.now
    def sleep(self,seconds):self.now+=seconds

def peer():
    clock=Clock();config=bench.engine.transport_configuration(overrides={
        'samples':{k:[0] for k in bench.engine.OPERATIONS-{'accepted','http_return'}}})
    config['native_send_phases']=[{'client_prelude':.2,'gateway_total':.6,
        'gateway_queue':.1,'client_postlude':.05}]
    server=bench.PhasedServer('robot','room@chatroom',config,
        {'SendResult':SendResult,'GatewayError':GatewayError},clock=clock,sleep=clock.sleep)
    return clock,server

def send(server,source,key):
    s=source['source']
    return server.send_received_quote(thread_id=s['threadId'],received_id=s['receivedId'],
        server_id=s['serverId'],source_sha256=s['sourceSha256'],text='fixed',idempotency_key=key)

def test_second_client_journal_overlaps_first_rpc_and_http_does_not_add_to_acceptance():
    clock,server=peer();a=server.inject({'text':'same'},'sender');b=server.inject({'text':'same'},'sender')
    send(server,a,'a');assert clock.now==pytest.approx(100.35)
    send(server,b,'b');row=server.submissions['b']
    assert row['client_preparation_ended_at']==pytest.approx(100.55)
    assert row['effective_command_at']==pytest.approx(100.8)
    assert row['account_gate_wait_seconds']==pytest.approx(.25)
    assert row['accepted_at']==pytest.approx(101.4)
    assert clock.now==pytest.approx(100.95)

def test_acceptance_does_not_add_client_postlude_and_duplicate_does_not_create_new_remote_command():
    clock,server=peer();source=server.inject({'text':'same'},'sender')
    send(server,source,'a');deadline=server.submissions['a']['accepted_at']
    send(server,source,'a')
    assert len(server.submissions)==1 and server.submissions['a']['post_count']==2
    assert server.submissions['a']['accepted_at']==deadline

def test_failed_or_negative_phase_data_is_rejected_without_clamping():
    document={'failed_candidate':False,'complete_phase_evidence':True,'source_tree':'a'*40,
      'cases':[{'python_method_to_accepted_commit_seconds':.1,'command_to_accepted_commit_seconds':.2,
        'phases_seconds':{'queue_total':.05},'submit_api_ms':100}]}
    with pytest.raises(ValueError):bench.phase_samples(document)
    document['failed_candidate']=True
    with pytest.raises(ValueError):bench.phase_samples(document)
