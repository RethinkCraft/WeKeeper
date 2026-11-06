"""Subsecond scans preserve archive safety while avoiding idle work."""
import sqlite3
import threading

import pytest

from chatlog_keeper import cli
from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.collector import Collector
from test_collector import environment, TABLE


@pytest.mark.parametrize("interval", [".1", ".5", "1.25"])
def test_subsecond_collection_cli(interval):
    args = cli.parser().parse_args(["collect", "--bot-mode", "--interval", interval])
    assert args.interval == float(interval)


@pytest.mark.parametrize("interval", ["nan", "inf", "-inf", "0", ".09"])
def test_nonfinite_or_too_short_collection_cli(interval):
    with pytest.raises(SystemExit):
        cli.parser().parse_args(["collect", "--interval", interval])


@pytest.mark.parametrize("interval", [float("nan"), float("inf"), .09])
def test_constructor_rejects_unbounded_cadence(interval, tmp_path):
    with pytest.raises(ValueError, match="invalid collector limits"):
        Collector(type("Store", (), {"state_dir": tmp_path})(), poll_interval=interval)


def test_idle_fast_scans_skip_key_validation_sql_work_and_reparse(environment, monkeypatch):
    collector, store, database, flags = environment
    collector.bot_mode = True
    collector.poll_interval = .5
    now = [10.]
    monkeypatch.setattr("chatlog_keeper.collector.time.monotonic", lambda: now[0])
    reparsed = []
    store.reparse_batch = lambda **kwargs: reparsed.append(kwargs)
    collector.collect_once()
    collector.collect_once()
    called = []
    original = collector._reader
    collector._reader = lambda: (called.append(True), original())[1]
    assert collector.collect_once()["idle"]
    assert called == [] and reparsed == []
    with sqlite3.connect(database) as connection:
        connection.execute(f'INSERT INTO "{TABLE}" VALUES (1,200,2,?,"fast-new",NULL)', (b"new",))
    assert not collector.collect_once().get("idle")
    assert called == [True]
    assert any(sid == "fast-new" for _, sid in store.records)
    now[0] += 5
    collector.collect_once()
    assert called == [True, True]


def test_fast_health_probes_are_throttled_but_new_messages_are_not(environment, monkeypatch):
    collector, store, database, _ = environment
    collector.bot_mode = True
    now = [20.]
    monkeypatch.setattr("chatlog_keeper.collector.time.monotonic", lambda: now[0])
    probes = []
    started = [threading.Event(), threading.Event()]
    def monitor(_):
        probes.append(now[0])
        started[len(probes) - 1].set()
        return {"state": "session_present"}
    collector.health_monitor = monitor
    collector.collect_once()
    assert started[0].wait(3)
    collector._health_future.result(timeout=3)
    collector.collect_once()
    collector.collect_once()
    assert probes == [20.]
    now[0] += 5
    collector.collect_once()
    assert started[1].wait(3)
    collector._health_future.result(timeout=3)
    collector.collect_once()
    assert probes == [20., 25.]


def test_fast_collection_with_real_archive_keeps_schema_and_live_cursor(environment, tmp_path):
    fixture, _, database, _ = environment
    state = tmp_path / "real-archive"
    with ArchiveStore(state) as store:
        schema = store.conn.execute("PRAGMA user_version").fetchone()[0]
        collector = Collector(store, data_root=fixture.data_root, page_size=2,
                              poll_interval=.5, bot_mode=True)
        try:
            collector.collect_once()
            with sqlite3.connect(database) as connection:
                connection.execute(f'INSERT INTO "{TABLE}" VALUES (1,200,2,?,"new-live",NULL)', (b"new",))
            collector.collect_once()
            assert store.conn.execute("PRAGMA user_version").fetchone()[0] == schema
            assert store.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 4
            assert store.conn.execute("SELECT live_rowid FROM streams").fetchone()[0] == 4
            with ArchiveStore(state, readonly=True) as reader:
                assert reader.get_status()["schema_version"] == schema
                assert reader.get_status()["messages"] == 4
        finally:
            collector.close()
