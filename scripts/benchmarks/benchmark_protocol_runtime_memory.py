#!/usr/bin/env python3
"""One explicit MCP transport factor over the frozen Direct runtime engine."""
import argparse
from copy import deepcopy
import hashlib
import importlib
import importlib.util
from pathlib import Path
import threading

DIRECT_PATH = Path(__file__).with_name('benchmark_protocol_runtime_direct.py')
DIRECT_SHA = '32e87538434947f2a0aaf883efdc92f70153027e01ae1d0d50b9622084217ab3'


def checked_direct():
    if hashlib.sha256(DIRECT_PATH.read_bytes()).hexdigest() != DIRECT_SHA:
        raise RuntimeError('frozen_direct_engine_changed')
    spec = importlib.util.spec_from_file_location('_memory_direct_engine', DIRECT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class MemoryHostSelection:
    def __init__(self, enabled=False):
        if type(enabled) is not bool:
            raise ValueError('invalid_memory_host_option')
        self.enabled, self.records, self.source = enabled, {}, None
        self.lock = threading.Lock()

    def metadata(self):
        with self.lock:
            return {'memory_mcp_enabled': self.enabled,
                'adapter_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'source': self.source, 'hosts': deepcopy(self.records),
                'transport': 'SDK typed JSON-RPC memory streams' if self.enabled else 'code-root default',
                'does_not_test_memory_byte_encoding': True}

    def wrap_components(self, components, code_root):
        if not self.enabled:
            return components
        module = importlib.import_module('chatlog_keeper.bot.memory_host')
        path = Path(module.__file__).resolve()
        if not path.is_relative_to(Path(code_root).resolve()):
            raise ValueError('memory_host_code_root_mismatch')
        self.source = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        selection = self

        class ObservedMemoryHost(module.MemoryBusinessMCPHost):
            def __init__(self, state_dir, event_id):
                super().__init__(state_dir, event_id)
                self._observation_id = event_id
                self._observed_successful_tools = []
                with selection.lock:
                    if event_id in selection.records:
                        raise RuntimeError('multiple_hosts_for_request')
                    selection.records[event_id] = {'closed': False, 'session_started': False,
                        'tools': [], 'successful_tools': []}

            async def call(self, name, arguments):
                output = await super().call(name, arguments)
                # The generator retains only successful tool outputs. Observe
                # success before returning it, without saving argument/content.
                if isinstance(output, dict) and output.get('ok') is True:
                    self._observed_successful_tools.append(name)
                return output

            async def __aexit__(self, *args):
                try:
                    return await super().__aexit__(*args)
                finally:
                    with selection.lock:
                        selection.records[self._observation_id] = {
                            'closed': self.closed and (self.owner is None or self.owner.done()),
                            'session_started': self.host is not None and self.session is not None,
                            'tools': [row['name'] for row in self.tool_timings],
                            'successful_tools': list(self._observed_successful_tools), 'timings': self.timings}

        original = components['ReplyGenerator']
        def generator(*args, **kwargs):
            if 'host_factory' in kwargs:
                raise ValueError('multiple_mcp_host_selectors')
            return original(*args, host_factory=ObservedMemoryHost, **kwargs)
        return {**components, 'ReplyGenerator': generator}

    def execution_errors(self, evidence, identifier):
        if not self.enabled:
            return []
        with self.lock:
            row = deepcopy(self.records.get(identifier))
        if not row or not row['closed']:
            return ['memory_host_not_observed_closed']
        tools = [result['name'] for result in evidence.get('tool_results', [])]
        attempts = iter(row['tools'])
        successes_were_attempted = all(any(attempt == name for attempt in attempts)
                                      for name in row['successful_tools'])
        if (row['successful_tools'] != tools or not successes_were_attempted
                or ((row['tools'] or tools) and not row['session_started'])):
            return ['memory_host_tools_not_bound_to_evidence']
        return []


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--phase-calibration', type=Path, required=True)
    parser.add_argument('--direct-native-plan', action='store_true')
    parser.add_argument('--memory-mcp', action='store_true')
    args, remaining = parser.parse_known_args(argv)
    direct = checked_direct()
    selection = MemoryHostSelection(args.memory_mcp)

    class Adapter(direct.DirectAdapter):
        def metadata(self):
            return {**super().metadata(), 'mcp_host_adapter': selection.metadata()}

        def wrap_components(self, components):
            wrapped = super().wrap_components(components)
            # load_components already checked and inventoried the real package.
            root = Path(importlib.import_module('chatlog_keeper.bot.protocol_runtime').__file__).resolve().parents[2]
            return selection.wrap_components(wrapped, root)

        def inspect_case(self, original, *values, **kwargs):
            errors = super().inspect_case(original, *values, **kwargs)
            frozen = kwargs.get('frozen', values[2] if len(values) > 2 else None)
            injected = kwargs.get('injected', values[1] if len(values) > 1 else None)
            return errors + (selection.execution_errors(frozen['evidence'], injected['request_id']) if frozen else [])

    phased = direct.checked_phase_engine()
    phased.configure_engine(args.phase_calibration)
    engine = direct.configure_engine(phased, Adapter(enabled=args.direct_native_plan))
    return engine.main(remaining)


if __name__ == '__main__':
    raise SystemExit(main())
