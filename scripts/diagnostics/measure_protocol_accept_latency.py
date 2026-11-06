#!/usr/bin/env python3
"""Measure message.received → send accepted latency from gateway control events.

Primary metric for the takeover plan: robot-controlled path only. Emits one
JSON summary with median/p90 and optional per-message JSONL. Does not inject
or send traffic; pair with an A/A harness or live phone messages.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


def parse_time(value):
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value:
        return None
    text = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


def load_events(path):
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
        result.append({
            "id": row["id"],
            "kind": row["kind"],
            "data": data if isinstance(data, dict) else {},
            "created_at": row["created_at"],
            "created_ts": parse_time(row["created_at"]),
        })
    return result


def pairs(rows):
    """Match each message.received to a later accepted send sharing thread/quote."""
    pending_by_received = {}
    pending_by_thread = {}
    matched = []
    for row in rows:
        data = row["data"]
        kind = row["kind"]
        if kind == "message.received":
            thread = data.get("threadId") or data.get("thread_id")
            received_id = data.get("receivedId") or data.get("received_id")
            if received_id is not None:
                pending_by_received[str(received_id)] = row
            if thread is not None:
                pending_by_thread.setdefault(str(thread), []).append(row)
            continue
        accepted = False
        if kind in {"message.accepted", "send.accepted"}:
            accepted = True
        elif kind == "send.updated" and data.get("state") == "accepted":
            accepted = True
        elif kind == "operation.updated" and data.get("state") == "accepted":
            accepted = True
        if not accepted:
            continue
        thread = data.get("threadId") or data.get("thread_id") or data.get("destination")
        quote = data.get("quoteReceivedId") or data.get("quote_received_id") or data.get("replyToReceivedId")
        source = None
        if quote is not None:
            source = pending_by_received.pop(str(quote), None)
        if source is None and thread is not None:
            bucket = pending_by_thread.get(str(thread)) or []
            if bucket:
                source = bucket.pop(0)
        if source is None or source["created_ts"] is None or row["created_ts"] is None:
            continue
        matched.append({
            "received_event_id": source["id"],
            "accepted_event_id": row["id"],
            "thread_id": thread,
            "seconds": max(0.0, row["created_ts"] - source["created_ts"]),
            "received_at": source["created_at"],
            "accepted_at": row["created_at"],
        })
    return matched


def summarize(samples):
    values = [row["seconds"] for row in samples]
    if not values:
        return {"count": 0}
    values = sorted(values)

    def pct(p):
        if len(values) == 1:
            return values[0]
        rank = (len(values) - 1) * p
        low = int(rank)
        high = min(low + 1, len(values) - 1)
        frac = rank - low
        return values[low] * (1 - frac) + values[high] * frac

    return {
        "count": len(values),
        "median_seconds": statistics.median(values),
        "p90_seconds": pct(0.9),
        "mean_seconds": statistics.mean(values),
        "min_seconds": values[0],
        "max_seconds": values[-1],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events-db", type=Path, required=True,
                        help="Per-account control.sqlite")
    parser.add_argument("--account", help="Informational label only")
    parser.add_argument("--jsonl", type=Path, help="optional per-message latency dump")
    parser.add_argument("--since-id", type=int, default=0)
    args = parser.parse_args(argv)
    if not args.events_db.is_file():
        print(json.dumps({"error": "events_db_missing", "path": str(args.events_db)}), file=sys.stderr)
        return 1
    rows = [row for row in load_events(args.events_db) if row["id"] >= args.since_id]
    samples = pairs(rows)
    report = {"account": args.account, "summary": summarize(samples), "samples": samples}
    if args.jsonl:
        args.jsonl.parent.mkdir(parents=True, exist_ok=True)
        with args.jsonl.open("w", encoding="utf-8") as handle:
            for sample in samples:
                handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
