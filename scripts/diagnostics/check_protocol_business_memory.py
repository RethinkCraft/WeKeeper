#!/usr/bin/env python3
"""The frozen content oracle with one explicit request-scoped MCP host choice."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.benchmarks.benchmark_protocol_runtime_memory import MemoryHostSelection

CONTENT_SHA = '8f8be0f62dfbcc905a3f82cfb681591bd0c76936d8ede966f2f65f1571398ec9'


def main(argv=None):
    path = Path(__file__).with_name('check_protocol_business_direct.py')
    if hashlib.sha256(path.read_bytes()).hexdigest() != CONTENT_SHA:
        raise RuntimeError('frozen_content_oracle_changed')
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--memory-mcp', action='store_true')
    args, remaining = parser.parse_known_args(argv)
    from scripts.diagnostics import check_protocol_business_direct as content
    from scripts.benchmarks import benchmark_protocol_runtime as base
    selection = MemoryHostSelection(args.memory_mcp)
    original_load, original_errors, original_run = base.load_components, content._execution_errors, content.run_content

    def load(code_root):
        components, build = original_load(code_root)
        components = selection.wrap_components(components, code_root)
        build['mcp_host_adapter'] = selection.metadata()
        return components, build

    def errors(adapter, evidence, identifier, frozen):
        return original_errors(adapter, evidence, identifier, frozen) + selection.execution_errors(evidence, identifier)

    def run(*values, **kwargs):
        report = original_run(*values, **kwargs)
        report['mcp_host_adapter'] = selection.metadata()
        report['memory_content_wrapper_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        output = Path(values[1] if len(values) > 1 else kwargs['output']) / 'report.json'
        with output.open('w') as stream:
            output.chmod(0o600)
            json.dump(report, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write('\n')
        return report

    base.load_components, content._execution_errors, content.run_content = load, errors, run
    try:
        return content.main(remaining)
    finally:
        base.load_components, content._execution_errors, content.run_content = original_load, original_errors, original_run


if __name__ == '__main__':
    raise SystemExit(main())
