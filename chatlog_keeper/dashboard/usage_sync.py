"""Upload an existing usage ledger using its owning dashboard source identity."""
from __future__ import annotations

import hashlib
from pathlib import Path
import time

import httpx

from .usage import UsageLedger

LEGACY_USAGE_FIELDS = ("run_id", "kind", "user_id", "turn_id", "model", "started_at", "completed_at", "status", "usage")
BOT_USAGE_FIELDS = ("provider", "profile_id", "group_ref", "bot_request_id")


def usage_projection(run):
    """Keep existing SIWC summary/chat uploads readable by pre-bot servers.

    New metadata is required for bot runs and other providers. Ordinary SIWC
    records retain their established wire shape so updating a local reader does
    not require updating the separately deployed cloud API at the same time.
    """
    legacy = (run.get("kind") in {"chat", "summary"}
              and run.get("provider", "siwc") in {None, "siwc"}
              and not any(run.get(key) for key in BOT_USAGE_FIELDS if key != "provider"))
    keys = LEGACY_USAGE_FIELDS if legacy else (*LEGACY_USAGE_FIELDS, *BOT_USAGE_FIELDS)
    return {key: run.get(key) for key in keys}


def _reconcile_key(directory, state):
    # The main ledger retains its existing key, including heartbeat-triggered
    # restore recovery. Independent bot ledgers must not share its timestamp.
    if directory == Path(state.directory).expanduser().resolve():
        return "last_usage_reconcile"
    return "last_usage_reconcile_" + hashlib.sha256(str(directory).encode()).hexdigest()[:24]


def upload_usage(usage_dir, *, state, post, clock=time.time):
    """Upload without copying a ledger or creating another synchronization source.

    `state` is the owning dashboard's existing LocalState. `post(route, payload)`
    uses its machine authorization and accepts a full /v1 path. A connection is
    opened and closed in this caller's thread, independently of the bot's writer.
    """
    directory = Path(usage_dir).expanduser()
    ledger = UsageLedger(directory)
    try:
        key = _reconcile_key(directory.resolve(), state)
        reconcile = clock() - state.get(key, 0) >= 86400
        rows = ledger.list_runs() if reconcile else ledger.pending()
        rows = [row for row in rows if row.get("kind") != "local_business"]
        errors, uploaded = [], 0
        for start in range(0, len(rows), 100):
            runs = [usage_projection(row) for row in rows[start:start + 100] if row.get("completed_at") is not None]
            if not runs:
                continue
            try:
                post("/v1/ingest/usage", {"source_id": state.source_id, "runs": runs})
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in {400, 409, 422}:
                    raise
                # An orphaned restored chat must not starve unrelated accounting.
                for run in runs:
                    try:
                        post("/v1/ingest/usage", {"source_id": state.source_id, "runs": [run]})
                        ledger.acknowledge(run["run_id"])
                        uploaded += 1
                    except httpx.HTTPStatusError as item_error:
                        errors.append(item_error)
            else:
                for run in runs:
                    ledger.acknowledge(run["run_id"])
                    uploaded += 1
        if errors:
            raise errors[0]
        if reconcile:
            state.set(key, clock())
        return uploaded
    finally:
        ledger.close()
