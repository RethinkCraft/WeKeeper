"""Slow client probes never hold the bot collector's live reception loop."""
import sqlite3
import threading

import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.collector import Collector
from test_collector import environment, TABLE


def test_blocked_probe_does_not_delay_new_source_and_is_single_flight(environment, monkeypatch):
    collector, store, database, _ = environment
    collector.bot_mode = True
    now = [10.]
    monkeypatch.setattr("chatlog_keeper.collector.time.monotonic", lambda: now[0])
    entered, release = threading.Event(), threading.Event()
    calls = []
    def monitor(current):
        calls.append(threading.get_ident())
        entered.set()
        assert release.wait(3)
        return {"state": "session_present", "checked_at": 123}
    collector.health_monitor = monitor
    try:
        first = collector.collect_once()
        assert entered.wait(3)
        future = collector._health_future
        assert "client_health" not in first and "client_health" not in store.status
        assert collector._last_health is None
        now[0] = 30.  # Even after multiple throttle periods, never queue another.
        with sqlite3.connect(database) as connection:
            connection.execute(f'INSERT INTO "{TABLE}" VALUES (1,200,2,?,"health-live",NULL)', (b"new",))
        result = collector.collect_once()
        assert any(sid == "health-live" for _, sid in store.records)
        assert result["errors"] == [] and "client_health" not in result
        for _ in range(3):
            collector._check_client_health()
        assert collector._health_future is future and not future.done()
        assert len(calls) == 1 and calls[0] != threading.get_ident()
        release.set()
        future.result(timeout=3)
        # Consume the finished evidence before scheduling the next due probe.
        assert collector._check_client_health()["state"] == "session_present"
        assert store.status["client_health"]["state"] == "session_present"
        assert store.status["last_health_probe_seconds"] == 20
    finally:
        release.set()


def test_completed_probes_publish_once_and_obey_five_second_start_cadence(environment, monkeypatch):
    collector, store, _, _ = environment
    collector.bot_mode = True
    now = [20.]
    monkeypatch.setattr("chatlog_keeper.collector.time.monotonic", lambda: now[0])
    writes, probes = [], []
    original = store.set_collector_status
    def write(**fields):
        writes.append(fields)
        original(**fields)
    monkeypatch.setattr(store, "set_collector_status", write)
    collector.health_monitor = lambda _: (probes.append(now[0]), {"state": "login_required"})[1]
    assert collector._check_client_health() is None
    collector._health_future.result(timeout=3)
    assert collector._check_client_health()["state"] == "login_required"
    assert len(writes) == 1 and "last_health_probe_seconds" in writes[0]
    for tick in (20.1, 22, 24.999):
        now[0] = tick
        assert collector._check_client_health()["state"] == "login_required"
    assert probes == [20.] and len(writes) == 1
    now[0] = 25
    collector._check_client_health()
    collector._health_future.result(timeout=3)
    collector._check_client_health()
    assert probes == [20., 25.] and len(writes) == 2


@pytest.mark.parametrize("failure", ["exception", "malformed"])
def test_background_failure_is_unknown_without_exception_details(environment, caplog, failure):
    collector, store, _, _ = environment
    collector.bot_mode = True
    def monitor(_):
        if failure == "exception":
            raise RuntimeError("sensitive path and credential")
        return {"state": 1, "detail": "sensitive path and credential"}
    collector.health_monitor = monitor
    assert collector._check_client_health() is None
    collector._health_future.result(timeout=3)
    health = collector._check_client_health()
    assert health["state"] == "unknown" and health["reason"] == "health_monitor_failed"
    assert store.status["health_monitor_error"] == ("RuntimeError" if failure == "exception" else "ValueError")
    assert "sensitive" not in repr(store.status) + caplog.text
    assert store.status["last_health_probe_seconds"] >= 0


def test_normal_mode_keeps_synchronous_health_semantics(environment):
    collector, store, _, _ = environment
    caller = threading.get_ident()
    probes = []
    collector.health_monitor = lambda _: (probes.append(threading.get_ident()), {"state": "not_running"})[1]
    first = collector._check_client_health()
    second = collector._check_client_health()
    assert first == second == {"state": "not_running"}
    assert probes == [caller, caller] and collector._health_pool is None
    assert store.status["health_monitor_error"] is None
    collector.close()
    assert collector._check_client_health() == {"state": "not_running"}
    assert probes == [caller, caller, caller]  # Existing normal-mode reuse stays synchronous.


def test_close_joins_probe_before_store_closes_and_cannot_schedule_again(tmp_path, monkeypatch):
    with ArchiveStore(tmp_path) as store:
        entered, release, joining, finished = (threading.Event() for _ in range(4))
        def monitor(_):
            entered.set()
            assert release.wait(3)
            assert store.get_meta("probe-marker", "still-open") == "still-open"
            return {"state": "session_present"}
        collector = Collector(store, health_monitor=monitor, bot_mode=True)
        assert collector._check_client_health() is None
        assert entered.wait(3)
        pool = collector._health_pool
        shutdown = pool.shutdown
        def join(**kwargs):
            joining.set()
            return shutdown(**kwargs)
        monkeypatch.setattr(pool, "shutdown", join)
        closer = threading.Thread(target=lambda: (collector.close(), finished.set()))
        closer.start()
        try:
            assert joining.wait(3)
            assert not finished.is_set()
            release.set()
            closer.join(timeout=3)
            assert finished.is_set() and not closer.is_alive()
            assert collector._health_pool is collector._health_future is None
            assert collector._check_client_health() is None
            collector.close()
        finally:
            release.set()
            closer.join(timeout=3)
            collector.close()
