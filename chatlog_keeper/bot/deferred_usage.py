"""Buffer usage ledger writes so reply generation is not blocked by FULL commits.

Ops stay in memory until flush(), which applies them in order to the real ledger
with the same FULL durability. Crash before flush loses only accounting, never
the frozen reply. Flush failures after outbox projection are recorded but do not
undo delivery.
"""
from __future__ import annotations

import copy
import threading
import uuid


class DeferredUsageLedger:
    """Drop-in ledger facade that records ops without waiting on SQLite."""

    def __init__(self, underlying=None):
        self._underlying = underlying
        self._ops = []
        self._lock = threading.Lock()
        self._flushed = False

    def bind(self, underlying):
        if underlying is None:
            raise ValueError("deferred_usage_underlying_missing")
        self._underlying = underlying
        return self

    def ops(self):
        # JSON freeze round-trips tuples to lists; keep a JSON-stable shape so
        # protocol_final_not_frozen comparisons succeed after queue_reply.
        with self._lock:
            return [[method, list(args), copy.deepcopy(kwargs)] for method, args, kwargs in self._ops]

    def start_run(self, **kwargs):
        run_id = kwargs.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            run_id = "bot_" + uuid.uuid4().hex
        payload = dict(kwargs)
        payload["run_id"] = run_id
        with self._lock:
            if self._flushed:
                raise RuntimeError("deferred_usage_already_flushed")
            self._ops.append(("start_run", (), payload))
        return run_id

    def record_call(self, run_id, **kwargs):
        with self._lock:
            if self._flushed:
                raise RuntimeError("deferred_usage_already_flushed")
            self._ops.append(("record_call", (run_id,), dict(kwargs)))

    def finish_run(self, run_id, **kwargs):
        with self._lock:
            if self._flushed:
                raise RuntimeError("deferred_usage_already_flushed")
            self._ops.append(("finish_run", (run_id,), dict(kwargs)))

    def finish_with_call(self, run_id, **kwargs):
        with self._lock:
            if self._flushed:
                raise RuntimeError("deferred_usage_already_flushed")
            self._ops.append(("finish_with_call", (run_id,), dict(kwargs)))

    def flush(self, underlying=None):
        target = underlying if underlying is not None else self._underlying
        if target is None:
            raise ValueError("deferred_usage_underlying_missing")
        with self._lock:
            if self._flushed:
                return {"flushed": 0, "already": True}
            ops = list(self._ops)
            self._ops.clear()
            self._flushed = True
        for method, args, kwargs in ops:
            getattr(target, method)(*args, **kwargs)
        return {"flushed": len(ops), "already": False}


def restore_deferred_usage(ops, underlying):
    """Replay serialized ops captured in frozen evidence/metrics."""
    buffer = DeferredUsageLedger(underlying)
    if not isinstance(ops, list):
        raise ValueError("deferred_usage_ops_invalid")
    for entry in ops:
        if not isinstance(entry, (list, tuple)) or len(entry) != 3:
            raise ValueError("deferred_usage_op_invalid")
        method, args, kwargs = entry
        if method == "start_run":
            buffer.start_run(**kwargs)
        elif method == "record_call":
            buffer.record_call(args[0], **kwargs)
        elif method == "finish_run":
            buffer.finish_run(args[0], **kwargs)
        elif method == "finish_with_call":
            buffer.finish_with_call(args[0], **kwargs)
        else:
            raise ValueError("deferred_usage_method_unknown")
    return buffer.flush()
