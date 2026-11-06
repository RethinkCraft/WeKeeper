#!/usr/bin/env python3
"""Native-runtime benchmark with measured client/gateway overlap; no live send."""
from copy import deepcopy
import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path

BASE_SHA = '542d3bcdc368e5b71cc0d9f85e56ac56fe652722c0774491ca5fbcbaabcfe544'
BASE_PATH=Path(__file__).with_name('benchmark_protocol_runtime.py')
spec=importlib.util.spec_from_file_location('_runtime_phase_engine',BASE_PATH)
engine=importlib.util.module_from_spec(spec)
spec.loader.exec_module(engine)

def phase_samples(document):
    if (document.get('failed_candidate') is not False or document.get('complete_phase_evidence') is not True
            or not isinstance(document.get('source_tree'),str) or len(document['source_tree'])!=40):
        raise ValueError('validated_phase_calibration_required')
    result=[]
    for row in document.get('cases',[]):
        total=row['python_method_to_accepted_commit_seconds']
        gateway=row['command_to_accepted_commit_seconds']
        queue=row['phases_seconds']['queue_total']
        returned=row['submit_api_ms']/1000
        prelude=total-gateway
        postlude=returned-prelude-queue
        values=(total,gateway,queue,returned,prelude,postlude)
        if any(type(v) not in (int,float) or not math.isfinite(v) or v<0 or v>60 for v in values) or gateway<queue:
            raise ValueError('invalid_or_unordered_phase_calibration')
        result.append({'client_prelude':prelude,'gateway_total':gateway,'gateway_queue':queue,
                       'client_postlude':postlude,'accepted':total,'http_return':returned})
    if not result:raise ValueError('missing_phase_samples')
    return result

class PhasedServer(engine.SimulatedServer):
    def send_received_quote(self, *, thread_id, received_id, server_id, source_sha256, text, idempotency_key):
        started=self.clock()
        with self.lock:
            source=self.messages[received_id]
            if (thread_id,server_id,source_sha256)!=(source['threadId'],source['serverId'],source['sourceSha256']):
                raise self.components['GatewayError']('simulated_quote_source_mismatch')
            command={'thread_id':thread_id,'received_id':received_id,'server_id':server_id,
                'source_sha256':source_sha256,'text':text,'idempotency_key':idempotency_key}
            index=int(source['firstReceivedSequence'])-1
            phase=self.config['native_send_phases'][index%len(self.config['native_send_phases'])]
        # The client journal and identity checks can overlap the preceding RPC.
        self.sleep(phase['client_prelude'])
        with self.lock:
            existing=self.submissions.get(idempotency_key)
            if existing:
                existing['post_count']+=1
                if command!=existing['command']:
                    raise self.components['GatewayError']('gateway_idempotency_conflict')
                return self._result(idempotency_key)
            entered=self.clock()
            effective=max(entered,self.account_gate_available_at)
            gateway_delay=self.delay('accepted',index,phase['gateway_total'])
            http_tail=self.delay('http_return',index,phase['gateway_queue']+phase['client_postlude'])
            accepted=effective+gateway_delay;http_deadline=effective+http_tail
            self.account_gate_available_at=accepted
            self.submissions[idempotency_key]={'sequence':source['firstReceivedSequence'],'command':command,
                'post_count':1,'submitted_at':started,'client_preparation_ended_at':entered,
                'client_prelude_seconds':phase['client_prelude'],'effective_command_at':effective,
                'account_gate_wait_seconds':effective-entered,'accepted_at':accepted,
                'http_return_deadline':http_deadline,'accepted_delay':gateway_delay,
                'http_delay':http_tail,'gateway_queue_seconds':phase['gateway_queue'],
                'client_postlude_seconds':phase['client_postlude']}
        self.sleep(max(0,http_deadline-self.clock()))
        with self.lock:
            self.submissions[idempotency_key]['http_returned_at']=self.clock()
            return self._result(idempotency_key)

def configure_engine(path):
    if hashlib.sha256(BASE_PATH.read_bytes()).hexdigest()!=BASE_SHA:
        raise RuntimeError('base_runtime_engine_changed')
    document=json.loads(path.read_text());samples=phase_samples(document)
    original=engine.transport_configuration
    def transport(read_calibration=None,overrides=None):
        overrides=deepcopy(overrides) if overrides is not None else {}
        if 'send_samples' in overrides:
            raise ValueError('phase_calibration_and_send_override_conflict')
        overrides['send_samples']=[{k:p[k] for k in ('accepted','http_return')} for p in samples]
        result=original(read_calibration,overrides)
        result['native_send_phases']=samples
        result['provenance'].update({'send':'same retained client and gateway phase observations',
            'send_calibration_count':len(samples),'default_send_calibration_count':4,
            'phase_file_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
            'native_source_tree':document['source_tree'],'runtime_engine_sha256':BASE_SHA,
            'phase_wrapper_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'account_gate_model':'v3: only measured gateway work holds account gate; client prelude/postlude overlap preceding or current RPC',
            'send_limitations':'small private test-group sample; UTC/monotonic correlation; no phone/remote-tail certification',
            'model_revision_is_not_production_optimization':True})
        return result
    engine.transport_configuration=transport
    engine.SimulatedServer=PhasedServer

def main(argv=None):
    parser=argparse.ArgumentParser(add_help=False)
    parser.add_argument('--phase-calibration',type=Path,required=True)
    args,remaining=parser.parse_known_args(argv)
    configure_engine(args.phase_calibration)
    return engine.main(remaining)

if __name__=='__main__':raise SystemExit(main())
