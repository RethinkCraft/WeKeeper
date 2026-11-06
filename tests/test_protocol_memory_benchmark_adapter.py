import asyncio
from pathlib import Path

import pytest

from scripts.benchmarks.benchmark_protocol_runtime_memory import MemoryHostSelection


@pytest.mark.parametrize('record,evidence,errors', [
    (None, {}, ['memory_host_not_observed_closed']),
    ({'closed': False, 'session_started': True, 'tools': []}, {}, ['memory_host_not_observed_closed']),
    ({'closed': True, 'session_started': False, 'tools': []}, {}, []),
    ({'closed': True, 'session_started': False, 'tools': ['query_offer_status']},
     {'tool_results': [{'name': 'query_offer_status'}]}, ['memory_host_tools_not_bound_to_evidence']),
    ({'closed': True, 'session_started': True, 'tools': ['report_offer_status']},
     {'tool_results': [{'name': 'query_offer_status'}]}, ['memory_host_tools_not_bound_to_evidence']),
    ({'closed': True, 'session_started': True, 'tools': ['query_offer_status']},
     {'tool_results': [{'name': 'query_offer_status'}]}, []),
])
def test_a_flag_cannot_replace_closed_host_and_actual_tool_evidence(record, evidence, errors):
    choice = MemoryHostSelection(True)
    if record is not None:
        record['successful_tools'] = list(record['tools'])
        choice.records['request'] = record
    assert choice.execution_errors(evidence, 'request') == errors


def test_a_real_rejected_attempt_does_not_need_to_appear_as_successful_evidence():
    choice = MemoryHostSelection(True)
    choice.records['unauthorized'] = {'closed': True, 'session_started': True,
        'tools': ['report_offer_status'], 'successful_tools': []}
    assert choice.execution_errors({'tool_results': []}, 'unauthorized') == []
    assert choice.execution_errors({'tool_results': [{'name': 'report_offer_status'}]}, 'unauthorized') == [
        'memory_host_tools_not_bound_to_evidence']


def test_success_claim_without_an_actual_rpc_attempt_is_rejected():
    choice = MemoryHostSelection(True)
    choice.records['request'] = {'closed': True, 'session_started': True,
        'tools': [], 'successful_tools': ['query_offer_status']}
    assert choice.execution_errors({'tool_results': [{'name': 'query_offer_status'}]}, 'request') == [
        'memory_host_tools_not_bound_to_evidence']


def test_disabled_adapter_leaves_generator_and_oracle_unchanged():
    choice = MemoryHostSelection(False)
    original = {'ReplyGenerator': object()}
    assert choice.wrap_components(original, '/missing/source') is original
    assert choice.execution_errors({}, 'unobserved') == []


def test_explicit_wiring_uses_actual_loaded_memory_host_without_starting_sdk(tmp_path):
    import chatlog_keeper.bot.memory_host as host_module
    choice = MemoryHostSelection(True)
    captured = {}
    def generator(*args, **kwargs):
        captured.update(kwargs)
        return 'generator'
    components = choice.wrap_components({'ReplyGenerator': generator}, Path(__file__).resolve().parents[1])
    assert components['ReplyGenerator'](state_dir=tmp_path) == 'generator'
    host_class = captured['host_factory']
    assert issubclass(host_class, host_module.MemoryBusinessMCPHost)
    async def never_used():
        async with host_class(tmp_path, 'request') as host:
            assert host.owner is None and host.session is None
    asyncio.run(never_used())
    assert choice.execution_errors({}, 'request') == []
    assert choice.metadata()['source']['path'] == str(Path(host_module.__file__).resolve())


def test_loaded_host_from_wrong_code_root_is_rejected():
    with pytest.raises(ValueError, match='memory_host_code_root_mismatch'):
        MemoryHostSelection(True).wrap_components({'ReplyGenerator': object()}, '/wrong/source')
