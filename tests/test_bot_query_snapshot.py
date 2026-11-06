"""Business query snapshots must remain coherent across another writer's commit."""
from contextlib import closing, contextmanager
import sqlite3
import threading

import pytest

from chatlog_keeper.bot.service import Service
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config, NOW


DAY = "2026-10-27"


@pytest.fixture
def store(tmp_path):
    value = BotStore(tmp_path)
    value.configure(config())
    with value.conn:
        value.conn.executemany(
            "INSERT INTO offers VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            [(business, day, item, "available", version, "seed", 1, 1,
              "seed-message", "group_a", "wxid_manager", NOW)
             for business, day, item, version in (
                 ("shop", DAY, "fun", 1), ("shop", DAY, "rank", 1),
                 ("other", DAY, "fun", 9), ("shop", "2026-10-28", "fun", 9))],
        )
    yield value
    value.close()


@contextmanager
def commit_after_first_offer_row(store):
    """Commit through another WAL connection while the reader holds a row.

    No timing sleeps or shared BotStore lock: this reproduces a separate MCP
    process or administrator committing between the old per-item SELECTs.
    """
    first_row = threading.Event()
    committed = threading.Event()
    errors = []
    factory = store.conn.row_factory

    def write():
        try:
            assert first_row.wait(5), "query did not read an offer row"
            with closing(sqlite3.connect(store.path, timeout=5)) as writer:
                writer.execute(
                    "UPDATE offers SET status='unavailable',version=2 "
                    "WHERE business_id='shop' AND day=?", (DAY,))
                writer.commit()
        except BaseException as error:
            errors.append(error)
        finally:
            committed.set()

    def on_row(cursor, values):
        row = factory(cursor, values)
        columns = [column[0] for column in cursor.description]
        if columns[:3] == ["business_id", "day", "item_id"] and not first_row.is_set():
            first_row.set()
            assert committed.wait(5), "independent writer was blocked by the query"
            assert not errors
        return row

    writer = threading.Thread(target=write, daemon=True)
    store.conn.row_factory = on_row
    writer.start()
    try:
        yield
    finally:
        store.conn.row_factory = factory
        writer.join(timeout=6)
        assert not writer.is_alive()
        assert committed.is_set() and not errors


def test_multi_item_query_keeps_one_snapshot_during_another_connection_commit(store):
    with commit_after_first_offer_row(store):
        result = Service(store, {"group_ref": "group_a", "now": NOW}).call("query_offer_status")

    assert result["ok"]
    assert [row["item_id"] for row in result["items"]] == ["fun", "rank"]
    assert [(row["status"], row["version"]) for row in result["items"]] == [
        ("available", 1), ("available", 1)]
    # The writer really committed before the query finished; a later read sees it.
    assert store.offer_status("shop", DAY, "fun")["version"] == 2
    assert store.offer_status("shop", DAY, "rank")["version"] == 2


def test_snapshot_preserves_selection_order_scope_and_unknown_defaults(store):
    rows = store.offer_statuses("shop", DAY, ["rank", "missing", "fun"])
    assert [row["item_id"] for row in rows] == ["rank", "missing", "fun"]
    assert rows[0] == store.offer_status("shop", DAY, "rank")
    assert rows[2] == store.offer_status("shop", DAY, "fun")
    assert rows[1] == {"business_id": "shop", "day": DAY, "item_id": "missing",
                       "status": "unknown", "version": 0, "message_ref": None,
                       "updated_at": None, "sender_id": None, "group_ref": None}
    assert store.offer_statuses("shop", DAY, []) == []


def test_single_item_and_unreported_day_keep_existing_service_results(store):
    service = Service(store, {"group_ref": "group_a", "now": NOW})
    current = service.call("query_offer_status", {"item": "娱乐"})
    assert current["items"] == [{**store.offer_status("shop", DAY, "fun"),
                                 "item_name": "娱乐首单"}]
    missing = service.call("query_offer_status", {"item": "娱乐", "date": "2026-10-29"})
    assert missing["items"] == [{**store.offer_status("shop", "2026-10-29", "fun"),
                                 "item_name": "娱乐首单"}]
