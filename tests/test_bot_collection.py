"""Live collection scheduling and separate enrichment use synthetic sources."""
import hashlib
import sqlite3

import pytest

from chatlog_keeper import cli
from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.collector import CollectorError, EnrichmentWorker
from test_collector import environment, TABLE


def another_shard(database):
    other = database.with_name("message_1.db")
    group = "second@chatroom"
    table = "Msg_" + hashlib.md5(group.encode()).hexdigest()
    with sqlite3.connect(other) as connection:
        connection.execute("CREATE TABLE Name2Id(user_name TEXT)")
        connection.executemany("INSERT INTO Name2Id VALUES (?)", [(group,), ("wxid_sender",)])
        connection.execute(f'CREATE TABLE "{table}"(local_type INTEGER,create_time INTEGER,real_sender_id INTEGER,message_content BLOB,server_id TEXT,packed_info_data BLOB)')
        connection.execute(f'INSERT INTO "{table}" VALUES (1,100,2,?,"second-history",NULL)', (b"history",))
    return other, table


def enable_shard(monkeypatch, other):
    from chatlog_keeper import collector as module
    original = module.wechat_db.WeChatDBReader
    def reader(*args, **kwargs):
        instance = original(*args, **kwargs)
        instance.enc_keys[other] = b"a" * 32
        return instance
    monkeypatch.setattr(module.wechat_db, "WeChatDBReader", reader)


def test_every_shard_live_page_precedes_any_historical_page(environment, monkeypatch):
    collector, store, database, _ = environment
    other, table = another_shard(database)
    enable_shard(monkeypatch, other)
    collector.collect_once()
    with sqlite3.connect(database) as connection:
        connection.execute(f'INSERT INTO "{TABLE}" VALUES (1,200,2,?,"first-live",NULL)', (b"new",))
    with sqlite3.connect(other) as connection:
        connection.execute(f'INSERT INTO "{table}" VALUES (1,200,2,?,"second-live",NULL)', (b"new",))
    store.commits.clear()
    collector.collect_once()
    assert [mode for mode, _ in store.commits[:2]] == ["live", "live"]
    assert any(mode == "backfill" for mode, _ in store.commits[2:])


def test_bot_mode_has_one_global_history_page_and_no_inline_p2(environment, monkeypatch):
    collector, store, database, _ = environment
    other, _ = another_shard(database)
    enable_shard(monkeypatch, other)
    collector.bot_mode = True
    collector.poll_interval = 3
    store.upsert_voice = store.upsert_profiles = lambda *_: None
    monkeypatch.setattr("chatlog_keeper.p2_sources.P2Sources.collect",
                        lambda *_args, **_kwargs: pytest.fail("P2 ran on live collection thread"))
    collector.collect_once()
    assert len([entry for entry in store.commits if entry[0] != "live"]) == 1
    first_group = next(stream for stream in store.streams.values() if stream["backfill_rowid"])
    store.commits.clear()
    collector.collect_once()
    assert len([entry for entry in store.commits if entry[0] != "live"]) <= 1
    assert any(stream["backfill_done"] for stream in store.streams.values() if stream is not first_group)


def test_separate_enrichment_cannot_collect_messages_and_has_its_own_singleton(tmp_path, monkeypatch):
    with ArchiveStore(tmp_path) as store:
        worker = EnrichmentWorker(store, data_root=tmp_path / "data", account="account")
        worker.context.account_dir = tmp_path / "data/account"
        worker.context._reader = lambda: object()
        worker.context._names = lambda reader: {"group@chatroom": "Group"}
        worker._p2.collect = lambda *_args, **_kwargs: {"voices_linked": 1}
        monkeypatch.setattr("chatlog_keeper.archive_media.MediaArchiver.process_pending",
                            lambda *_args, **_kwargs: {"processed": 0})
        second = EnrichmentWorker(store)
        try:
            with worker.context._single_instance():
                assert worker.run_once()["p2_sources"]["voices_linked"] == 1
            with worker._single_instance():
                with pytest.raises(CollectorError, match="enrichment_already_running"):
                    second.run_once()
            assert store.conn.execute("SELECT COUNT(*) FROM streams").fetchone()[0] == 0
            assert store.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
            assert not store.conn.execute("SELECT 1 FROM sqlite_master WHERE name='bot_events'").fetchone()
        finally:
            worker.close()
            second.close()


@pytest.mark.parametrize("extra,expected", [([], 3), (["--interval", "7"], 7)])
def test_bot_cli_selects_live_cadence_and_removes_media_callback(tmp_path, monkeypatch, extra, expected):
    from chatlog_keeper import collector as module
    captured = {}
    class FakeCollector:
        def __init__(self, store, **options):
            captured.update(options)
        def collect_once(self):
            return {"errors": []}
        def close(self):
            pass
    monkeypatch.setattr(module, "Collector", FakeCollector)
    cli.collect(cli.parser().parse_args(["collect", "--bot-mode", "--once", "--state-dir", str(tmp_path), *extra]))
    assert captured["poll_interval"] == expected
    assert captured["bot_mode"] is True and captured["after_cycle"] is None
