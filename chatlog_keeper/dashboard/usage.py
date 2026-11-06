"""Local durable inference accounting; unknown provider usage stays unknown."""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid

from chatlog_keeper.bot.model_http_trace import MAX_CALLS, validated_trace

FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens")
TERMINAL = frozenset({"completed", "failed", "cancelled", "interrupted"})


class _HTTPDiagnosticsRolledBack(RuntimeError):
    """The optional diagnostic write invalidated its accounting transaction."""


def normalize_usage(value):
    if not isinstance(value, dict):
        return None
    def number(item):
        return item if type(item) is int and item >= 0 else None
    incoming = value.get("input_tokens_details") or value.get("prompt_tokens_details") or {}
    outgoing = value.get("output_tokens_details") or value.get("completion_tokens_details") or {}
    normalized = {
        "input_tokens": number(value.get("input_tokens", value.get("prompt_tokens"))),
        "cached_input_tokens": number(value.get("cached_input_tokens", value.get("prompt_cache_hit_tokens", incoming.get("cached_tokens") if isinstance(incoming, dict) else None))),
        "output_tokens": number(value.get("output_tokens", value.get("completion_tokens"))),
        "reasoning_output_tokens": number(value.get("reasoning_output_tokens", outgoing.get("reasoning_tokens") if isinstance(outgoing, dict) else None)),
        "total_tokens": number(value.get("total_tokens")),
    }
    return normalized if any(item is not None for item in normalized.values()) else None


class UsageLedger:
    def __init__(self, state_dir, *, clock=time.time):
        root = Path(state_dir).expanduser()
        if root.is_symlink() or (root / "usage.sqlite").is_symlink():
            raise ValueError("unsafe_usage_state")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.clock = clock
        self.model_http_trace_disabled = False
        self.model_http_trace_recoveries = 0
        self.model_http_trace_failures = 0
        old = os.umask(0o077)
        try:
            self.conn = sqlite3.connect(root / "usage.sqlite", timeout=15)
        finally:
            os.umask(old)
        os.chmod(root / "usage.sqlite", 0o600)
        self.conn.row_factory = sqlite3.Row
        initialization = os.open(root / "usage-init.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(initialization, fcntl.LOCK_EX)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.executescript("""
              CREATE TABLE IF NOT EXISTS usage_runs(
                run_id TEXT PRIMARY KEY,kind TEXT NOT NULL,user_id INTEGER,turn_id TEXT,
                model TEXT NOT NULL,started_at REAL NOT NULL,completed_at REAL,status TEXT NOT NULL,
                usage TEXT,metadata TEXT NOT NULL,error_code TEXT,delivered INTEGER NOT NULL DEFAULT 0);
              CREATE TABLE IF NOT EXISTS usage_calls(
                call_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,response_id TEXT,request_id TEXT,
                usage TEXT,raw_usage TEXT,status TEXT NOT NULL,created_at REAL NOT NULL);
              CREATE INDEX IF NOT EXISTS usage_call_runs ON usage_calls(run_id);
            """)
            self.conn.commit()
        finally:
            fcntl.flock(initialization, fcntl.LOCK_UN)
            os.close(initialization)

    def close(self):
        self.conn.close()

    def start_run(self, *, run_id=None, kind="chat", model="", user_id=None, turn_id=None, started_at=None, **metadata):
        if kind not in {"chat", "summary", "bot_chat", "local_business"}:
            raise ValueError("invalid_run_kind")
        if user_id is not None and (type(user_id) is not int or user_id < 1):
            raise ValueError("invalid_run_user")
        if kind in {"bot_chat", "local_business"} and (user_id is not None or turn_id is not None or not metadata.get("group_ref") or not metadata.get("bot_request_id")):
            raise ValueError("bot_usage_requires_local_request")
        if kind == "local_business" and (model != "local-business" or metadata.get("provider") != "local-business"
                                         or metadata.get("model_invoked") is not False):
            raise ValueError("invalid_local_business_accounting")
        run_id = run_id or str(uuid.uuid4())
        # Prompts, credentials and chat content never belong in an accounting row.
        allowed = {key: value for key, value in metadata.items() if key in {
            "conversation_id", "source_id", "group_refs", "input_hash", "prompt_version",
            "parent_memory_version", "start_seq", "end_seq", "input_characters", "executor",
            "provider", "profile_id", "group_ref", "bot_request_id",
            "model_invoked", "execution_route",
        }}
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO usage_runs VALUES(?,?,?,?,?,?,NULL,'running',NULL,?,NULL,0)",
                              (run_id, kind, user_id, turn_id, model, self.clock() if started_at is None else started_at, json.dumps(allowed, separators=(",", ":"))))
        return run_id

    def record_call(self, run_id, *, call_id=None, response_id=None, request_id=None,
                    usage=None, status="completed", model_http_trace=None):
        if model_http_trace is not None and not self.model_http_trace_disabled:
            arguments = {"call_id": call_id or str(uuid.uuid4()), "response_id": response_id,
                         "request_id": request_id, "usage": usage, "status": status}
            return self._with_http_diagnostics(
                lambda: self._record_call(run_id, **arguments, model_http_trace=model_http_trace),
                lambda: self._record_call(run_id, **arguments))
        with self.conn:
            return self._record_call(run_id, call_id=call_id, response_id=response_id,
                                     request_id=request_id, usage=usage, status=status)

    def _with_http_diagnostics(self, observed, accounting):
        had_transaction = self.conn.in_transaction
        try:
            with self.conn:
                return observed()
        except _HTTPDiagnosticsRolledBack:
            # Only the operation owned here can be reconstructed. Unknown
            # caller-owned writes in a pre-existing transaction must fail.
            if had_transaction:
                raise
            with self.conn:
                result = accounting()
            self.model_http_trace_recoveries += 1
            return result

    def _record_call(self, run_id, *, call_id=None, response_id=None, request_id=None,
                     usage=None, status="completed", model_http_trace=None):
        """Insert without opening a nested connection transaction."""
        kind = self.conn.execute("SELECT kind FROM usage_runs WHERE run_id=?", (run_id,)).fetchone()
        if kind is not None and kind[0] == "local_business":
            raise ValueError("local_business_has_no_model_calls")
        call_id = call_id or str(uuid.uuid4())
        normalized = normalize_usage(usage)
        inserted = self.conn.execute("INSERT OR IGNORE INTO usage_calls VALUES(?,?,?,?,?,?,?,?)",
                          (call_id, run_id, response_id, request_id,
                           json.dumps(normalized, separators=(",", ":")) if normalized is not None else None,
                           json.dumps(usage, separators=(",", ":")) if isinstance(usage, dict) else None,
                           status, self.clock()))
        if inserted.rowcount and model_http_trace is not None:
            self._record_model_http_trace(run_id, model_http_trace, call_id=call_id)
        return call_id

    def _record_model_http_trace(self, run_id, value, *, call_id=None):
        """Private evidence in the caller's existing transaction; never usage.

        A failed HTTP attempt has no accounting call_id. Its trace is stored on
        finish_run only, explicitly retaining unknown HTTP outcome. Invalid or
        failed diagnostics cannot discard an otherwise valid accounting write.
        """
        if self.model_http_trace_disabled:
            return
        had_transaction = self.conn.in_transaction
        try:
            trace = validated_trace(value)
            if trace is None:
                return
            row = self.conn.execute("SELECT kind,metadata FROM usage_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None or row[0] != "bot_chat":
                return
            metadata = json.loads(row[1])
            records = metadata.get("model_http_calls", [])
            if type(metadata) is not dict or type(records) is not list:
                return
            record = {"trace": trace}
            if call_id is not None:
                record["call_id"] = call_id
            if any((call_id is not None and existing.get("call_id") == call_id)
                   or existing == record for existing in records):
                return
            if len(records) >= MAX_CALLS or trace["call_index"] > MAX_CALLS:
                dropped = metadata.get("model_http_dropped_calls", 0)
                metadata["model_http_dropped_calls"] = min(2**63-1, dropped + 1)
            else:
                metadata["model_http_calls"] = [*records, record]
            self.conn.execute("UPDATE usage_runs SET metadata=? WHERE run_id=?",
                (json.dumps(metadata, separators=(",", ":")), run_id))
        except BaseException:
            self.model_http_trace_disabled = True
            self.model_http_trace_failures += 1
            if had_transaction and not self.conn.in_transaction:
                # Swallowing RAISE(ROLLBACK) here would silently lose the actual
                # call INSERT and could falsely complete its run with no usage.
                raise _HTTPDiagnosticsRolledBack("usage_http_diagnostics_transaction_rolled_back") from None

    def _aggregate(self, run_id):
        rows = self.conn.execute("SELECT usage FROM usage_calls WHERE run_id=?", (run_id,)).fetchall()
        if not rows:
            return None
        values = [json.loads(row[0]) if row[0] else None for row in rows]
        output = {}
        for key in FIELDS:
            output[key] = sum(item[key] for item in values) if all(item is not None and item.get(key) is not None for item in values) else None
        return output if any(value is not None for value in output.values()) else None

    def finish_run(self, run_id, *, status="completed", error_code=None, usage=None, completed_at=None,
                   model_http_trace=None):
        if model_http_trace is not None and not self.model_http_trace_disabled:
            def accounting():
                return self._finish_run(run_id, status=status, error_code=error_code, usage=usage, completed_at=completed_at)
            def observed():
                self._record_model_http_trace(run_id, model_http_trace)
                return accounting()
            self._with_http_diagnostics(observed, accounting)
            return self.get(run_id)
        with self.conn:
            self._finish_run(run_id, status=status, error_code=error_code, usage=usage, completed_at=completed_at)
        return self.get(run_id)

    def _finish_run(self, run_id, *, status="completed", error_code=None, usage=None, completed_at=None):
        if status not in TERMINAL:
            raise ValueError("invalid_terminal_run_status")
        value = normalize_usage(usage) if usage is not None else self._aggregate(run_id)
        row = self.conn.execute("SELECT status,delivered,kind FROM usage_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise ValueError("usage_run_not_started")
        if row[2] == "local_business":
            if usage is not None and normalize_usage(usage) != {field: 0 for field in FIELDS}:
                raise ValueError("invalid_local_business_usage")
            value = {field: 0 for field in FIELDS}
        if row[0] not in TERMINAL or (not row[1] and row[0] == "completed" and status == "failed"):
            self.conn.execute("UPDATE usage_runs SET status=?,completed_at=?,usage=?,error_code=? WHERE run_id=?",
                              (status, completed_at if completed_at is not None else self.clock(),
                               json.dumps(value, separators=(",", ":")) if value is not None else None,
                               error_code, run_id))

    def finish_with_call(self, run_id, *, call, status="completed", error_code=None, usage=None, completed_at=None):
        """Commit an actual final response and its terminal run in one FULL write."""
        if not isinstance(call, dict) or set(call) - {"call_id", "response_id", "request_id", "usage", "status", "model_http_trace"}:
            raise ValueError("invalid_usage_call")
        if call.get("model_http_trace") is not None and not self.model_http_trace_disabled:
            fixed_call = {**call, "call_id": call.get("call_id") or str(uuid.uuid4())}
            accounting_call = {key: value for key, value in fixed_call.items() if key != "model_http_trace"}
            def complete(value):
                self._record_call(run_id, **value)
                self._finish_run(run_id, status=status, error_code=error_code, usage=usage, completed_at=completed_at)
            self._with_http_diagnostics(lambda: complete(fixed_call), lambda: complete(accounting_call))
            return self.get(run_id)
        with self.conn:
            self._record_call(run_id, **call)
            self._finish_run(run_id, status=status, error_code=error_code, usage=usage, completed_at=completed_at)
        return self.get(run_id)

    def record_run(self, *, run_id=None, kind="chat", model="", status="completed", usage=None,
                   user_id=None, turn_id=None, completed_at=None, error_code=None, **metadata):
        run_id = self.start_run(run_id=run_id, kind=kind, model=model, user_id=user_id, turn_id=turn_id, **metadata)
        return self.finish_run(run_id, status=status, usage=usage, completed_at=completed_at, error_code=error_code)

    @staticmethod
    def _wire(row):
        metadata = json.loads(row["metadata"])
        result = {"run_id": row["run_id"], "kind": row["kind"], "user_id": row["user_id"],
                "turn_id": row["turn_id"], "model": row["model"], "started_at": row["started_at"],
                "completed_at": row["completed_at"], "status": row["status"],
                "usage": json.loads(row["usage"]) if row["usage"] is not None else None,
                "provider": metadata.get("provider") or "siwc", "profile_id": metadata.get("profile_id"),
                "group_ref": metadata.get("group_ref"), "bot_request_id": metadata.get("bot_request_id")}
        if row["kind"] == "local_business":
            result.update(model_invoked=False, execution_route=metadata.get("execution_route"))
        return result

    def get(self, run_id):
        row = self.conn.execute("SELECT * FROM usage_runs WHERE run_id=?", (run_id,)).fetchone()
        return self._wire(row) if row else None

    def list_runs(self, limit=None, after=None):
        query = "SELECT * FROM usage_runs WHERE status<>'running'"
        params = []
        if after is not None:
            query += " AND run_id>?"
            params.append(after)
        query += " ORDER BY run_id"
        if limit is not None:
            query += " LIMIT ?"
            params.append(max(1, min(int(limit), 10000)))
        return [self._wire(row) for row in self.conn.execute(query, params)]

    def pending(self, limit=1000):
        # Local execution audits never enter the model-usage upload protocol.
        return [self._wire(row) for row in self.conn.execute("SELECT * FROM usage_runs WHERE status<>'running' AND kind<>'local_business' AND delivered=0 ORDER BY started_at,run_id LIMIT ?", (max(1, min(int(limit), 10000)),))]

    def list_running(self, turn_id=None):
        query, args = "SELECT * FROM usage_runs WHERE status='running'", []
        if turn_id is not None:
            query += " AND turn_id=?"
            args.append(turn_id)
        return [self._wire(row) for row in self.conn.execute(query + " ORDER BY started_at,run_id", args)]

    def interrupt_turn(self, turn_id):
        for run in self.list_running(turn_id):
            self.finish_run(run["run_id"], status="interrupted", error_code="executor_interrupted")

    def acknowledge(self, run_id):
        with self.conn:
            self.conn.execute("UPDATE usage_runs SET delivered=1 WHERE run_id=? AND status<>'running'", (run_id,))

    def totals(self):
        runs = self.list_runs()
        return {"runs": len(runs), "known_usage_runs": sum(run["usage"] is not None for run in runs),
                "unknown_usage_runs": sum(run["usage"] is None for run in runs),
                "reported_tokens": {key: sum(run["usage"][key] for run in runs if run["usage"] is not None and run["usage"].get(key) is not None) for key in FIELDS}}
