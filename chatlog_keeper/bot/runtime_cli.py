"""Shared explicit protocol check/run commands for installed and source users."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal
import sys

from .protocol_runtime import RuntimeConfig, RuntimeStatusFile, check_configuration, error_code, open_runtime


def add_runtime_options(parser, *, shared=False):
    for name in ("state-dir", "usage-dir", "dashboard-dir"):
        parser.add_argument("--" + name, type=Path, default=argparse.SUPPRESS if shared else None)
    for name in ("inbox", "outbox", "journal", "token-file", "bindings"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("server", "account", "own", "consumer"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--gateway-timeout", type=float, default=10.0)
    parser.add_argument("--status-file", type=Path)
    parser.add_argument("--activate", action="store_true", help="Explicit first checkpoint activation; run only")
    parser.add_argument("--cloud-control", action="store_true", help="Enable previously bootstrapped cloud control")
    parser.add_argument("--archive-state", type=Path, help="Existing archive used for cloud member references")


def run_command(args, *, opener=None):
    if args.activate and args.command != "run":
        raise ValueError("protocol_activation_requires_run")
    state = Path(args.state_dir)
    dashboard = Path(args.dashboard_dir or state / "dashboard")
    config = RuntimeConfig(state_dir=state, usage_dir=Path(args.usage_dir or dashboard),
        inbox_path=args.inbox, outbox_path=args.outbox, journal_path=args.journal,
        token_file=args.token_file, bindings_file=args.bindings, account_id=args.account,
        own_username=args.own, consumer=args.consumer, server=args.server,
        dashboard_dir=dashboard, activate=args.activate, gateway_timeout=args.gateway_timeout,
        status_file=args.status_file, cloud_control=args.cloud_control, archive_state=args.archive_state)
    runtime, observer, handlers, exit_code = None, None, {}, 0
    try:
        if args.command == "check":
            result = check_configuration(config)
            result["binding_count"] = len(result.pop("bindings"))
            print(json.dumps(result, ensure_ascii=False))
        else:
            observer = RuntimeStatusFile.for_config(config)
            if observer is not None:
                observer.write({"state": "starting"})
            runtime = (opener or open_runtime)(config)
            for signum in (signal.SIGINT, signal.SIGTERM):
                handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, lambda *_: runtime.request_stop())
            runtime.run_forever()
    except KeyboardInterrupt:
        exit_code = 130
    except Exception as error:
        print(json.dumps({"mode": args.command, "error": error_code(error)}), file=sys.stderr)
        try:
            if runtime is not None:
                runtime.record_failure(error)
            elif observer is not None:
                observer.write({"state": "failed", "error": error_code(error)})
        except Exception as status_error:
            print(json.dumps({"mode": "status", "error": error_code(status_error)}), file=sys.stderr)
        exit_code = 1
    finally:
        try:
            if runtime is not None:
                runtime.close()
        except Exception as error:
            print(json.dumps({"mode": "shutdown", "error": error_code(error)}), file=sys.stderr)
            exit_code = 1
        for signum, handler in handlers.items():
            signal.signal(signum, handler)
    return exit_code


def source_main(argv=None, *, opener=None):
    parser = argparse.ArgumentParser(description="Check explicit native protocol state; --run starts the bot")
    add_runtime_options(parser)
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args(argv)
    args.command = "run" if args.run else "check"
    if not args.state_dir:
        parser.error("--state-dir is required")
    if args.activate and not args.run:
        parser.error("--activate requires --run")
    return run_command(args, opener=opener)
