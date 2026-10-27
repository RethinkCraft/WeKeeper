"""Operator commands for the opt-in self-hosted dashboard."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import fcntl
import getpass
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import sys
import threading
import time
from zoneinfo import ZoneInfo


def parser():
    p = argparse.ArgumentParser(prog="wekeeper-dashboard")
    p.add_argument("--archive-state", default=os.environ.get("CHATLOG_KEEPER_DATA_DIR", str(Path.home() / ".local/share/wekeeper")))
    p.add_argument("--local-state", help="Private dashboard state; defaults to ARCHIVE_STATE/dashboard")
    commands = p.add_subparsers(dest="command", required=True)
    commands.add_parser("groups", help="List local group references without exporting messages")
    configure = commands.add_parser("configure", help="Configure explicit text-only sync")
    configure.add_argument("--server-url", required=True)
    configure.add_argument("--group", action="append", required=True, dest="groups")
    configure.add_argument("--token-file", help="Read machine token from protected file; otherwise prompt")
    commands.add_parser("status")
    sync = commands.add_parser("sync")
    sync.add_argument("--once", action="store_true")
    agent = commands.add_parser("agent", help="Run the outbound-only web chat worker")
    agent.add_argument("--once", action="store_true")
    avatars = commands.add_parser("avatar-backfill", help="Fetch missing selected-group avatars from saved contact URLs")
    avatars.add_argument("--once", action="store_true")
    summarize = commands.add_parser("summarize")
    summarize.add_argument("--once", action="store_true")
    summarize.add_argument("--history", action="store_true", help="Explicitly analyze older days, consuming plan usage")
    summarize.add_argument("--resume", action="store_true", help="Resume after resolving a quota or authorization pause")
    auth = commands.add_parser("auth")
    auths = auth.add_subparsers(dest="auth_command", required=True)
    login = auths.add_parser("login")
    login.add_argument("--label")
    login.add_argument("--no-browser", action="store_true")
    login.add_argument("--new-account", action="store_true")
    login.add_argument("--port", type=int, default=0, help="Loopback callback port, useful with SSH local forwarding")
    account = auths.add_parser("account")
    account.add_argument("account_id")
    for name in ("status", "models", "logout", "resume"):
        auths.add_parser(name)
    model = auths.add_parser("model")
    model.add_argument("slug")
    serve = commands.add_parser("serve")
    serve.add_argument("--data-dir", default=os.environ.get("WEKEEPER_DATA_DIR", "./data/dashboard"))
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--static-dir", default=os.environ.get("WEKEEPER_STATIC_DIR"))
    user = commands.add_parser("user-add")
    user.add_argument("username")
    user.add_argument("--role", choices=("admin", "viewer"), default="viewer")
    user.add_argument("--data-dir", default=os.environ.get("WEKEEPER_DATA_DIR", "./data/dashboard"))
    user.add_argument("--password-stdin", action="store_true")
    backup = commands.add_parser("backup")
    backup.add_argument("destination")
    backup.add_argument("--data-dir", default=os.environ.get("WEKEEPER_DATA_DIR", "./data/dashboard"))
    return p


def _print(value):
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _summary_wait(stop, directory, seconds=30):
    """A finished chat wakes unfinished batches without changing the cadence."""
    until = time.monotonic() + seconds
    while not stop.is_set() and time.monotonic() < until:
        marker = directory / "summary-wake"
        if marker.exists():
            marker.unlink(missing_ok=True)
            return
        stop.wait(min(1, max(0, until - time.monotonic())))


def _configure(args, local_dir):
    from .local_state import LocalState
    from .projection import list_local_groups
    from .sync import read_config
    known = {g["group_ref"] for g in list_local_groups(args.archive_state)}
    if not set(args.groups).issubset(known):
        raise ValueError("unknown_group_reference")
    token = Path(args.token_file).read_text().strip() if args.token_file else getpass.getpass("Machine token: ")
    config = {"server_url": args.server_url.rstrip("/"), "groups": list(dict.fromkeys(args.groups)), "machine_token": token}
    state = LocalState(local_dir)
    target = local_dir / "config.json"
    temporary = local_dir / "config.json.new"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(config, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        read_config(temporary)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
        state.close()
    _print({"configured": True, "server_url": config["server_url"], "groups": len(config["groups"]), "config": str(target)})


def main(argv=None):
    args = parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    local_dir = Path(args.local_state or Path(args.archive_state) / "dashboard").expanduser()
    try:
        if args.command == "groups":
            from .projection import list_local_groups
            _print(list_local_groups(args.archive_state))
        elif args.command == "configure":
            _configure(args, local_dir)
        elif args.command == "auth":
            from .siwc import SIWCClient
            client = SIWCClient(local_dir)
            try:
                if args.auth_command == "login":
                    _print(client.login(account_label=args.label, open_browser=not args.no_browser, url_callback=print, new_account=args.new_account, callback_port=args.port))
                elif args.auth_command == "models":
                    _print(client.models())
                elif args.auth_command == "model":
                    _print(client.select_model(args.slug))
                elif args.auth_command == "account":
                    _print(client.select_account(args.account_id))
                elif args.auth_command == "logout":
                    _print(client.logout())
                elif args.auth_command == "resume":
                    from .local_state import LocalState
                    from .summaries import SummaryWorker
                    state = LocalState(local_dir)
                    worker = SummaryWorker(local_dir, state.source_id)
                    try:
                        _print(worker.resume())
                    finally:
                        worker.close()
                        state.close()
                else:
                    _print(client.status())
            finally:
                client.close()
        elif args.command == "serve":
            import uvicorn
            from .api import create_app
            token = os.environ.get("WEKEEPER_MACHINE_TOKEN")
            if not token or len(token) < 32:
                raise ValueError("machine_token_minimum_32_characters")
            uvicorn.run(create_app(args.data_dir, token, static_dir=args.static_dir), host=args.host, port=args.port, proxy_headers=True, forwarded_allow_ips="127.0.0.1")
        elif args.command == "user-add":
            from .store import DashboardStore
            password = sys.stdin.readline().rstrip("\r\n") if args.password_stdin else getpass.getpass("New password (at least 12 characters): ")
            store = DashboardStore(args.data_dir)
            try:
                _print(store.create_user(args.username, password, role=args.role))
            finally:
                store.close()
        elif args.command == "backup":
            target = Path(args.destination)
            if target.exists():
                raise ValueError("backup_destination_exists")
            source = Path(args.data_dir) / "dashboard.sqlite"
            if not source.is_file() or source.is_symlink():
                raise ValueError("dashboard_not_initialized")
            old = os.umask(0o077)
            try:
                with sqlite3.connect(source.absolute().as_uri() + "?mode=ro", uri=True) as src, sqlite3.connect(target) as dst:
                    src.backup(dst)
                    if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                        raise ValueError("backup_integrity_failed")
            finally:
                os.umask(old)
            _print({"backup": str(target), "integrity": "ok"})
        elif args.command == "status":
            from .local_state import LocalState
            from .summaries import SummaryWorker
            state = LocalState(local_dir)
            worker = SummaryWorker(local_dir, state.source_id)
            try:
                _print({"source_id": state.source_id, "pending_partitions": len(state.partitions(pending_only=True)), "sync": state.get("sync_status"), "summary": worker.status()})
            finally:
                worker.close()
                state.close()
        elif args.command == "sync":
            from .sync import SyncService, read_config
            service = SyncService(args.archive_state, local_dir, read_config(local_dir / "config.json"), config_path=local_dir / "config.json")
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: service.stop.set())
            try:
                if args.once:
                    service.send_heartbeat()
                    service.scan()
                    _print({"partitions_sent": service.flush(), "source_id": service.state.source_id})
                    service.flush_summaries()
                    service.scan_avatars()
                    service.flush_avatars()
                    service.flush_usage()
                else:
                    service.run()
            finally:
                service.close()
        elif args.command == "agent":
            from .agent import AgentWorker
            worker = AgentWorker(local_dir)
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: worker.stop.set())
            try:
                if args.once:
                    _print({"processed": worker.run_once()})
                else:
                    worker.run()
            finally:
                worker.close()
        elif args.command == "avatar-backfill":
            from .avatar_backfill import AvatarBackfill
            worker = AvatarBackfill(args.archive_state, local_dir)
            stop = threading.Event()
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: stop.set())
            try:
                if args.once:
                    worker.scan()
                    worker.flush(stop=stop)
                    _print(worker.status())
                else:
                    worker.run(stop=stop)
            finally:
                worker.close()
        elif args.command == "summarize":
            from .local_state import LocalState
            from .summaries import SummaryWorker
            state = LocalState(local_dir)
            worker = SummaryWorker(local_dir, state.source_id)
            from .sync import read_config
            allowed_groups = set(read_config(local_dir / "config.json")["groups"])
            stop = threading.Event()
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, lambda *_: stop.set())
            lockfile = (local_dir / "summary-scheduler.lock").open("a")
            try:
                if args.resume:
                    worker.resume()
                fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
                last_cycle = 0.0
                printed = None
                while not stop.is_set():
                    allowed_groups = set(read_config(local_dir / "config.json")["groups"])
                    if not args.once and worker.status().get("state") == "ready" and time.monotonic() - last_cycle < 1800:
                        _summary_wait(stop, local_dir)
                        continue
                    partitions = [p for p in state.partitions() if p["group_ref"] in allowed_groups]
                    today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
                    recent = None if args.history else {today.isoformat(), (today - timedelta(days=1)).isoformat()}
                    result = worker.run_once(partitions, baseline_days=recent)
                    if result.get("state") == "ready":
                        last_cycle = time.monotonic()
                    serialized = json.dumps(result, sort_keys=True)
                    if serialized != printed:
                        _print(result)
                        printed = serialized
                    if args.once:
                        break
                    _summary_wait(stop, local_dir)
            finally:
                lockfile.close()
                worker.close()
                state.close()
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        # Exceptions can contain HTTP credentials/URLs: expose only safe codes.
        code = getattr(exc, "code", None)
        if not isinstance(code, str) or not code.replace("_", "").isalnum():
            code = type(exc).__name__
        print("wekeeper-dashboard: " + code, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
