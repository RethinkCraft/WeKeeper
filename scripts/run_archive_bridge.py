#!/usr/bin/env python3
"""Copy retained protocol snapshots into the existing archive.

This process only reads unregistered gateway events and received snapshots.
It does not register a consumer, ack, send, or open an official WeChat database.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import signal
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from chatlog_keeper.bot.archive_bridge import (
    ArchiveBridge, BridgeError, BridgePaused, archive_account, bot_lanes_hold,
)
from chatlog_keeper.bot.protocol_gateway import GatewayError, ProtocolGateway

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-state", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--catchup-floor", type=_catchup_floor,
                        help="Initial inclusive ISO timestamp with timezone; persisted for subsequent restarts")
    parser.add_argument("--bot-status", type=Path, help="Pause the bridge if this bot status leaves ready")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--pace", type=float, default=0.15)
    parser.add_argument("--once", action="store_true", help="Stop after the retained pages are copied")
    args = parser.parse_args(argv)
    if args.run and args.once:
        parser.error("--once and --run are separate")
    if not args.run and not args.once:
        _check(args)
        return 0
    return _run(args)


def _check(args):
    account, version = archive_account(args.archive_state)
    path = args.archive_state / "archive" / "archive.sqlite"
    con = sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True)
    try:
        latest = con.execute("SELECT MAX(create_time) FROM messages").fetchone()[0]
    finally:
        con.close()
    print(json.dumps({"archive_account": account, "archive_version": version, "latest_create_time": latest,
                      "bot_status_present": bool(args.bot_status and args.bot_status.is_file())},
                     sort_keys=True))


def _catchup_floor(value):
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("catchup floor must be an ISO timestamp with timezone") from None
    if parsed.tzinfo is None or not 0 <= parsed.timestamp() <= 253402214400:
        raise argparse.ArgumentTypeError("catchup floor must be an ISO timestamp with timezone")
    return parsed


def _run(args):
    account, _version = archive_account(args.archive_state)
    token = args.token_file.read_text().strip()
    gateway = ProtocolGateway(token=token, account_id=args.account, journal_path=args.journal,
                              base_url=args.server, timeout=15)
    try:
        bridge = ArchiveBridge(gateway=gateway, archive_dir=args.archive_state, state_path=args.state_dir / "bridge.sqlite",
                               archive_account=account, floor=args.catchup_floor,
                               limit=args.limit, pace=args.pace,
                               before_page=(lambda: _lanes(args.bot_status)) if args.bot_status else None)
    except BaseException:
        gateway.close()
        raise
    # The retained cursor is stored in the bridge database. Do not open the
    # gateway's live control database: a reader-side -shm file makes the
    # gateway refuse to start.
    stop = False

    def _stop(_signum, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    exit_code = 0
    pauses = 0
    transport_errors = 0
    pages = 0
    try:
        while not stop:
            try:
                result = bridge.poll_once()
                pauses = 0
                transport_errors = 0
            except BridgePaused:
                pauses += 1
                print(json.dumps({"paused": True, "reason": "bot_lanes", "streak": pauses}, sort_keys=True), flush=True)
                # Stay up without reading the gateway until the bot lanes return.
                stop = _sleep(stop, 20)
                continue
            except GatewayError as error:
                if error.code != "gateway_transport_unavailable":
                    raise
                transport_errors += 1
                print(json.dumps({"backoff": True, "reason": error.code, "streak": transport_errors}, sort_keys=True), flush=True)
                stop = _sleep(stop, 60 if transport_errors >= 3 else 15)
                continue
            print(json.dumps({k: result[k] for k in ("fetched", "inserted", "skipped", "gaps", "caught_up")},
                             sort_keys=True), flush=True)
            if args.once and result.get("caught_up"):
                break
            if result.get("caught_up"):
                stop = _sleep(stop, 2)
                continue
            pages += 1
            if pages % 15 == 0:
                stop = _sleep(stop, 25)
    finally:
        bridge.close()
        gateway.close()
    return exit_code


def _lanes(path):
    for _ in range(2):
        try:
            status = json.loads(Path(path).read_text())
        except (OSError, ValueError):
            time.sleep(0.2)
            continue
        if bot_lanes_hold(status):
            return True
        time.sleep(0.3)
    return False


def _sleep(stop, seconds):
    deadline = time.monotonic() + seconds
    while not stop and time.monotonic() < deadline:
        time.sleep(0.2)
    return stop


if __name__ == "__main__":
    raise SystemExit(main())
