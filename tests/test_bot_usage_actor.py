"""Durability and owner-thread lifecycle checks; no model or production state."""
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import threading
import time

import pytest

from chatlog_keeper.bot.usage_actor import UsageLedgerActor
from chatlog_keeper.dashboard.usage import UsageLedger


def wait_until(predicate):
    deadline = time.monotonic() + 5
    while not predicate():
        if time.monotonic() >= deadline:
            pytest.fail("actor synchronization timed out")
        time.sleep(.001)


def test_real_usage_commits_are_visible_before_return_and_connection_has_one_owner(tmp_path):
    calls = []

    class TrackedLedger(UsageLedger):
        def __init__(self, directory):
            super().__init__(directory)
            calls.append(("init", threading.get_ident(), self.conn.execute("PRAGMA synchronous").fetchone()[0]))

        def start_run(self, **kwargs):
            calls.append(("start", threading.get_ident()))
            return super().start_run(**kwargs)

        def record_call(self, *args, **kwargs):
            calls.append(("call", threading.get_ident()))
            return super().record_call(*args, **kwargs)

        def finish_run(self, *args, **kwargs):
            calls.append(("finish", threading.get_ident()))
            return super().finish_run(*args, **kwargs)

        def close(self):
            calls.append(("close", threading.get_ident()))
            return super().close()

    actor = UsageLedgerActor(tmp_path, ledger_factory=TrackedLedger)
    try:
        assert actor.ready.result(timeout=5) is None
        run = actor.start_run(run_id="synthetic", kind="bot_chat", model="synthetic-model",
                              group_ref="test-group", bot_request_id="test-request")
        with sqlite3.connect(f"file:{tmp_path / 'usage.sqlite'}?mode=ro", uri=True) as reader:
            assert reader.execute("SELECT status FROM usage_runs WHERE run_id=?", (run,)).fetchone() == ("running",)
            actor.record_call(run, call_id="first", usage={"total_tokens": 17})
            actor.record_call(run, call_id="second", usage=None)
            actor.record_call(run, call_id="first", usage={"total_tokens": 999})
            rows = reader.execute("SELECT usage FROM usage_calls ORDER BY call_id").fetchall()
            assert len(rows) == 2
            assert json.loads(rows[0][0])["total_tokens"] == 17
            assert rows[1][0] is None
            finished = actor.finish_run(run)
            assert finished["status"] == "completed" and finished["usage"] is None
            assert reader.execute("SELECT status,usage FROM usage_runs WHERE run_id=?", (run,)).fetchone() == ("completed", None)
    finally:
        actor.close()
    assert calls[0][2] == 2  # FULL remains enabled on the actual owner connection.
    assert {call[1] for call in calls} == {actor._thread.ident}
    assert actor._thread.ident != threading.get_ident()
    assert calls[-1][0] == "close" and not actor._thread.is_alive()


def test_constructor_does_not_wait_for_initialization_and_early_calls_wait(tmp_path):
    loading, release = threading.Event(), threading.Event()

    def factory(directory):
        loading.set()
        assert release.wait(5)
        return UsageLedger(directory)

    actor = UsageLedgerActor(tmp_path, ledger_factory=factory)
    try:
        assert loading.wait(5)
        assert not actor.ready.done()
        with ThreadPoolExecutor(max_workers=1) as callers:
            future = callers.submit(actor.start_run, run_id="early", model="synthetic")
            wait_until(lambda: actor._queue.qsize() == 1)
            assert not future.done()
            release.set()
            assert future.result(timeout=5) == "early"
    finally:
        release.set()
        actor.close()


def test_concurrent_clients_share_one_connection_and_all_calls_are_retained(tmp_path):
    actor = UsageLedgerActor(tmp_path)
    try:
        def account(number):
            run = actor.start_run(run_id=str(number), model="synthetic")
            for index in range(3):
                actor.record_call(run, call_id=f"{number}_{index}", usage={"total_tokens": index + 1})
            return actor.finish_run(run)

        with ThreadPoolExecutor(max_workers=2) as callers:
            results = list(callers.map(account, range(4)))
        assert all(row["usage"]["total_tokens"] == 6 for row in results)
        with sqlite3.connect(f"file:{tmp_path / 'usage.sqlite'}?mode=ro", uri=True) as reader:
            assert reader.execute("SELECT COUNT(*) FROM usage_calls").fetchone()[0] == 12
            assert reader.execute("SELECT COUNT(*) FROM usage_runs WHERE status='completed'").fetchone()[0] == 4
    finally:
        actor.close()


def test_close_rejects_new_work_and_drains_previously_accepted_calls(tmp_path):
    entered, release = threading.Event(), threading.Event()
    operations = []

    class Ledger:
        def __init__(self, directory):
            self.owner = threading.get_ident()

        def start_run(self, **kwargs):
            assert threading.get_ident() == self.owner
            entered.set()
            assert release.wait(5)
            operations.append("start")
            return "run"

        def record_call(self, run_id, **kwargs):
            assert threading.get_ident() == self.owner
            operations.append("call")
            return "call"

        def close(self):
            assert threading.get_ident() == self.owner
            operations.append("close")

    actor = UsageLedgerActor(tmp_path, ledger_factory=Ledger)
    with ThreadPoolExecutor(max_workers=3) as callers:
        first = callers.submit(actor.start_run)
        try:
            assert entered.wait(5)
            second = callers.submit(actor.record_call, "run")
            wait_until(lambda: actor._queue.qsize() == 1)
            closing = callers.submit(actor.close)
            wait_until(lambda: actor._closed)
            with pytest.raises(RuntimeError, match="usage_actor_closed"):
                actor.start_run()
            assert not closing.done()
            release.set()
            assert first.result(timeout=5) == "run"
            assert second.result(timeout=5) == "call"
            assert closing.result(timeout=5) is None
        finally:
            release.set()
            actor.close()
    actor.close()
    assert operations == ["start", "call", "close"]
    assert not actor._thread.is_alive()


def test_initialization_failure_reaches_ready_pending_calls_and_close(tmp_path):
    release = threading.Event()

    def factory(directory):
        assert release.wait(5)
        raise ValueError("synthetic_initialization_failure")

    actor = UsageLedgerActor(tmp_path, ledger_factory=factory)
    with ThreadPoolExecutor(max_workers=1) as callers:
        pending = callers.submit(actor.start_run)
        wait_until(lambda: actor._queue.qsize() == 1)
        release.set()
        with pytest.raises(ValueError, match="synthetic_initialization_failure"):
            actor.ready.result(timeout=5)
        with pytest.raises(ValueError, match="synthetic_initialization_failure"):
            pending.result(timeout=5)
    with pytest.raises(ValueError, match="synthetic_initialization_failure"):
        actor.start_run()
    with pytest.raises(ValueError, match="synthetic_initialization_failure"):
        actor.close()
    assert not actor._thread.is_alive()


@pytest.mark.parametrize("initialization_fails", [False, True])
def test_close_during_initialization_finishes_every_accepted_result(tmp_path, initialization_fails):
    release = threading.Event()
    operations = []

    class Ledger:
        def __init__(self, directory):
            assert release.wait(5)
            if initialization_fails:
                raise ValueError("synthetic_initialization_failure")

        def start_run(self, **kwargs):
            operations.append("start")
            return "run"

        def close(self):
            operations.append("close")

    actor = UsageLedgerActor(tmp_path, ledger_factory=Ledger)
    with ThreadPoolExecutor(max_workers=2) as callers:
        pending = callers.submit(actor.start_run)
        wait_until(lambda: actor._queue.qsize() == 1)
        closing = callers.submit(actor.close)
        wait_until(lambda: actor._closed)
        assert not closing.done()
        with pytest.raises(RuntimeError, match="usage_actor_closed"):
            actor.start_run()
        release.set()
        if initialization_fails:
            for result in (pending, closing):
                with pytest.raises(ValueError, match="synthetic_initialization_failure"):
                    result.result(timeout=5)
        else:
            assert pending.result(timeout=5) == "run"
            assert closing.result(timeout=5) is None
            assert operations == ["start", "close"]
    assert not actor._thread.is_alive()


def test_method_errors_reach_caller_without_losing_following_results(tmp_path):
    class Ledger:
        def __init__(self, directory):
            pass

        def record_call(self, *args, **kwargs):
            raise ValueError("synthetic_call_failure")

        def finish_run(self, run_id, **kwargs):
            return {"run_id": run_id}

        def close(self):
            pass

    actor = UsageLedgerActor(tmp_path, ledger_factory=Ledger)
    try:
        with pytest.raises(ValueError, match="synthetic_call_failure"):
            actor.record_call("run")
        assert actor.finish_run("run") == {"run_id": "run"}
    finally:
        actor.close()


def test_owner_reentry_is_rejected_instead_of_deadlocking(tmp_path):
    owner = {}

    class Ledger:
        def __init__(self, directory):
            pass

        def start_run(self, **kwargs):
            return owner["actor"].start_run()

        def close(self):
            pass

    actor = owner["actor"] = UsageLedgerActor(tmp_path, ledger_factory=Ledger)
    try:
        with pytest.raises(RuntimeError, match="usage_actor_reentrant_call"):
            actor.start_run()
    finally:
        actor.close()


def test_close_failure_is_shared_by_repeat_close_and_thread_stops(tmp_path):
    calls = []

    class Ledger:
        def __init__(self, directory):
            pass

        def close(self):
            calls.append(threading.get_ident())
            raise ValueError("synthetic_close_failure")

    actor = UsageLedgerActor(tmp_path, ledger_factory=Ledger)
    for _ in range(2):
        with pytest.raises(ValueError, match="synthetic_close_failure"):
            actor.close()
    assert calls == [actor._thread.ident]
    assert not actor._thread.is_alive()
    with pytest.raises(RuntimeError, match="usage_actor_closed"):
        actor.start_run()


def test_timing_payloads_are_fixed_and_callback_failures_cannot_discard_commits(tmp_path):
    events = []

    def observe(event):
        events.append(event)
        raise RuntimeError("synthetic_observer_failure")

    actor = UsageLedgerActor(tmp_path, timing_callback=observe)
    run = actor.start_run(run_id="synthetic-private-id", model="synthetic-private-model")
    actor.record_call(run, usage={"total_tokens": 3})
    assert actor.finish_run(run)["usage"]["total_tokens"] == 3
    actor.close()
    assert {event["stage"] for event in events} == {
        "usage_init", "usage_queue_wait", "usage_start", "usage_record_call", "usage_finish", "usage_close"}
    assert all(set(event) == {"type", "stage", "seconds"} and event["seconds"] >= 0 for event in events)
    assert "synthetic-private" not in repr(events)
