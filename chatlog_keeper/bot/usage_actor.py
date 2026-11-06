"""A persistent usage connection with one owner and synchronous durable replies."""
from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass
import queue
import threading
import time

from chatlog_keeper.dashboard.usage import UsageLedger


@dataclass
class _Command:
    method: str
    args: tuple
    kwargs: dict
    result: Future
    queued_at: float


_STAGES = {"start_run": "usage_start", "record_call": "usage_record_call",
           "finish_run": "usage_finish", "finish_with_call": "usage_finish_with_call", "close": "usage_close"}


class UsageLedgerActor:
    """Serialize accounting on a persistent, thread-owned UsageLedger.

    Construction starts initialization in the background. Each method waits for
    its own result, including the ledger's existing FULL commit; no accounting
    write is acknowledged early. ``close`` stops admission, drains accepted work,
    closes the connection on its owner, and joins that thread.

    The optional timing callback receives only fixed stage names and durations.
    It runs on the owner thread and must not call back into this actor.
    """

    def __init__(self, state_dir, *, ledger_factory=None, timing_callback=None):
        self._queue = queue.Queue()
        self._lock = threading.Lock()
        self._closed = False
        self._startup_error = None
        self._timing_callback = timing_callback
        self.ready = Future()
        self.ready.set_running_or_notify_cancel()
        self._close_result = Future()
        self._close_result.set_running_or_notify_cancel()
        factory = UsageLedger if ledger_factory is None else ledger_factory
        self._thread = threading.Thread(target=self._run, args=(state_dir, factory),
                                        name="bot-usage-ledger", daemon=True)
        self._thread.start()

    def _observe(self, stage, started):
        if self._timing_callback is not None:
            try:
                self._timing_callback({"type": "timing", "stage": stage,
                                       "seconds": max(0.0, time.monotonic() - started)})
            except BaseException:
                # Diagnostics cannot discard a committed accounting result.
                pass

    def _run(self, state_dir, factory):
        started = time.monotonic()
        try:
            ledger = factory(state_dir)
        except BaseException as exc:
            # Admission and the failure transition share a lock, so every job
            # queued during initialization either fails here or sees this error.
            with self._lock:
                self._startup_error = exc
                pending = []
                while not self._queue.empty():
                    pending.append(self._queue.get_nowait())
            self.ready.set_exception(exc)
            for command in pending:
                if command.result is not self._close_result:
                    command.result.set_exception(exc)
            self._close_result.set_exception(exc)
            return
        self._observe("usage_init", started)
        self.ready.set_result(None)

        while True:
            command = self._queue.get()
            self._observe("usage_queue_wait", command.queued_at)
            started = time.monotonic()
            try:
                result = getattr(ledger, command.method)(*command.args, **command.kwargs)
            except BaseException as exc:
                command.result.set_exception(exc)
            else:
                command.result.set_result(result)
            finally:
                self._observe(_STAGES[command.method], started)
            if command.method == "close":
                return

    def _check_owner(self):
        if threading.current_thread() is self._thread:
            raise RuntimeError("usage_actor_reentrant_call")

    def _call(self, method, *args, **kwargs):
        self._check_owner()
        result = Future()
        with self._lock:
            if self._closed:
                raise RuntimeError("usage_actor_closed")
            if self._startup_error is not None:
                raise self._startup_error
            self._queue.put(_Command(method, args, kwargs, result, time.monotonic()))
        return result.result()

    def start_run(self, **kwargs):
        return self._call("start_run", **kwargs)

    def record_call(self, run_id, **kwargs):
        return self._call("record_call", run_id, **kwargs)

    def finish_run(self, run_id, **kwargs):
        return self._call("finish_run", run_id, **kwargs)

    def finish_with_call(self, run_id, **kwargs):
        return self._call("finish_with_call", run_id, **kwargs)

    def close(self):
        self._check_owner()
        with self._lock:
            if not self._closed:
                self._closed = True
                if self._startup_error is None:
                    self._queue.put(_Command("close", (), {}, self._close_result,
                                             time.monotonic()))
        try:
            return self._close_result.result()
        finally:
            self._thread.join()
