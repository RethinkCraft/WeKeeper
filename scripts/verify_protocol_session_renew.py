#!/usr/bin/env python3
"""Check whether a -13 rejection was followed by session renew within a window.

Reads the per-account gateway control.sqlite events table. Exit 0 when a renewed
outcome is present after the latest -13 auth_required, exit 2 when logout
happened without renew, exit 1 on usage/IO errors. Does not log in or send
messages.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


def events(path):
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """SELECT sequence AS id, kind, data_json, created_at FROM events
               ORDER BY sequence"""
        ).fetchall()
    finally:
        connection.close()
    result = []
    for row in rows:
        try:
            data = json.loads(row["data_json"])
        except ValueError:
            data = {}
        result.append({"id": row["id"], "kind": row["kind"], "data": data,
                       "created_at": row["created_at"]})
    return result


def evaluate(rows):
    last_reject = None
    for row in rows:
        if row["kind"] != "account.connection":
            continue
        data = row["data"] if isinstance(row["data"], dict) else {}
        if data.get("serviceCode") == -13 or data.get("state") in {"auth_required", "renewed"}:
            last_reject = row
    if last_reject is None:
        return {"status": "no_reject_seen", "detail": "no account.connection with -13/auth_required/renewed"}
    data = last_reject["data"]
    if data.get("authentication") == "renewed" or data.get("state") == "renewed":
        return {"status": "renewed", "event": last_reject}
    if data.get("authentication") == "logged_out" or data.get("state") == "auth_required":
        for row in rows:
            if row["id"] <= last_reject["id"]:
                continue
            payload = row["data"] if isinstance(row["data"], dict) else {}
            if row["kind"] == "account.connection" and (
                    payload.get("authentication") == "renewed" or payload.get("state") == "renewed"):
                return {"status": "renewed", "event": row, "after_logout_marker": last_reject}
        return {"status": "logged_out_without_renew", "event": last_reject}
    return {"status": "unknown_connection_event", "event": last_reject}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events-db", type=Path, required=True,
                        help="Per-account control.sqlite (events live here)")
    parser.add_argument("--account", help="Informational only; DB is already account-scoped")
    parser.add_argument("--since", help="ISO timestamp; ignore older connection events")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if not args.events_db.is_file():
        print(json.dumps({"error": "events_db_missing", "path": str(args.events_db)}), file=sys.stderr)
        return 1
    rows = events(args.events_db)
    if args.since:
        rows = [row for row in rows if str(row.get('created_at') or '') >= args.since]
    result = evaluate(rows)
    if args.account:
        result["account"] = args.account
    if args.json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print(result["status"])
        if "event" in result:
            print(json.dumps(result["event"], ensure_ascii=False, sort_keys=True))
    status = result["status"]
    if status == "renewed":
        return 0
    if status == "logged_out_without_renew":
        return 2
    if status == "no_reject_seen":
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
