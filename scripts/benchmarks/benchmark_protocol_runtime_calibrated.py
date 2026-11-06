#!/usr/bin/env python3
"""Run the unchanged runtime engine with retained native send observations."""
from pathlib import Path
import hashlib
import json
import sys

import benchmark_protocol_runtime as baseline

ORIGINAL_TRANSPORT = baseline.transport_configuration
BASE_ENGINE_SHA256 = '542d3bcdc368e5b71cc0d9f85e56ac56fe652722c0774491ca5fbcbaabcfe544'

def calibrated_transport(read_calibration=None, overrides=None):
    result=ORIGINAL_TRANSPORT(read_calibration,overrides)
    if overrides is not None and 'send_samples' in overrides:
        result['provenance']['default_send_calibration_count']=result['provenance']['send_calibration_count']
        result['provenance']['send_calibration_count']=len(result['send_samples'])
        result['provenance']['send']='explicit retained native observations supplied with --transport-config; replaces default send samples'
        result['provenance']['send_limitations']='small sample; host UTC correlation checked against monotonic spans; no peer/phone or network-tail claim'
        result['provenance']['runtime_engine_sha256']=BASE_ENGINE_SHA256
        result['provenance']['calibration_wrapper_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    return result

if __name__=='__main__':
    if hashlib.sha256(Path(baseline.__file__).read_bytes()).hexdigest()!=BASE_ENGINE_SHA256:
        raise RuntimeError('base_runtime_engine_changed')
    baseline.transport_configuration=calibrated_transport
    raise SystemExit(baseline.main())
