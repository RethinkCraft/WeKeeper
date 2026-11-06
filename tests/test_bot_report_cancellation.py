"""Cancellation after Service authorization must still close the write race."""
import sqlite3
import json

import pytest

from chatlog_keeper.bot.service import Service
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config


NOW = "2026-10-29T10:00:00+08:00"
DAY = "2026-10-29"
EVENT = "synthetic-cancel-race"
TEXT = "今天娱乐首单已无；竞技首单可用"


@pytest.fixture
def ledger(tmp_path):
    store = BotStore(tmp_path)
    store.configure(config())
    store.begin_request({"event_id": EVENT, "message_ref": "msg_cancel_race",
        "group_ref": "group_a", "sender_id": "wxid_manager", "text": TEXT,
        "occurred_at": NOW})
    # A distinct connection models cancellation by the custody coordinator
    # while the event-bound MCP process is already handling a tool call.
    peer = sqlite3.connect(store.path)
    try:
        yield store, peer
    finally:
        peer.close()
        store.close()


def cancel(peer):
    with peer:
        assert peer.execute("UPDATE requests SET status='cancelled' WHERE event_id=?", (EVENT,)).rowcount == 1


def service(store):
    return Service(store, {"event_id": EVENT, "writable": True, "now": NOW})


def report(store):
    return service(store).call("report_offer_status", {
        "item": "fun", "status": "unavailable", "evidence": TEXT})


def test_service_processing_check_cannot_authorize_a_later_cancelled_transaction(ledger, monkeypatch):
    store, peer = ledger
    original = store.record_offer
    passed_front_check = []
    before_changes, before_audit = store.conn.total_changes, store.audit()
    before_reply = store.reply(EVENT)

    def cancel_before_transaction(*args, **kwargs):
        # Service.call reaches this method only after reading processing and
        # validating permission, date and affirmative source evidence.
        assert store.request(EVENT)["status"] == "processing"
        assert not store.conn.in_transaction
        passed_front_check.append(True)
        cancel(peer)
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "record_offer", cancel_before_transaction)
    assert report(store) == {"ok": False, "error": "bot_request_closed"}
    assert passed_front_check == [True]
    assert store.request(EVENT)["status"] == "cancelled"
    assert store.offer_status("shop", DAY, "fun")["version"] == 0
    assert store.receipt(EVENT) is None
    assert store.audit() == before_audit
    assert store.reply(EVENT) == before_reply
    assert store.conn.total_changes == before_changes


def test_committed_receipt_survives_cancellation_but_another_item_cannot_write(ledger):
    store, peer = ledger
    committed = report(store)
    assert committed["ok"] and committed["recorded"] and committed["version"] == 1
    before_receipt, before_audit = store.receipt(EVENT), store.audit()
    before_reply, before_changes = store.reply(EVENT), store.conn.total_changes
    cancel(peer)

    replay = store.record_offer(EVENT, "fun", "unavailable", expected_version=0, evidence=TEXT)
    assert replay == {**committed, "replayed": True}
    assert store.record_offer(EVENT, "fun", "available", evidence=TEXT) == {
        "ok": False, "error": "event_already_reported"}
    assert store.record_offer(EVENT, "rank", "available", evidence=TEXT) == {
        "ok": False, "error": "bot_request_closed"}
    assert store.request(EVENT)["status"] == "cancelled"
    assert store.receipt(EVENT) == before_receipt
    assert store.offer_status("shop", DAY, "fun")["version"] == 1
    assert store.offer_status("shop", DAY, "rank")["version"] == 0
    assert store.conn.execute("SELECT COUNT(*) FROM reports WHERE event_id=?", (EVENT,)).fetchone()[0] == 1
    assert store.audit() == before_audit and store.reply(EVENT) == before_reply
    assert store.conn.total_changes == before_changes


@pytest.mark.parametrize("state", ["generated", "completed", "failed", "cancelled"])
def test_closed_request_rejects_new_report_items_inside_transaction(ledger, state):
    store, _ = ledger
    store.set_request_status(EVENT, state)
    before_changes = store.conn.total_changes
    assert store.record_offer(EVENT, "fun", "unavailable", evidence=TEXT) == {
        "ok": False, "error": "bot_request_closed"}
    assert store.receipt(EVENT) is None
    assert store.offer_status("shop", DAY, "fun")["version"] == 0
    assert store.conn.total_changes == before_changes


@pytest.mark.parametrize("state", ["processing", "interrupted"])
def test_existing_authorized_recovery_states_remain_writable(ledger, state):
    store, _ = ledger
    store.set_request_status(EVENT, state)
    result = report(store)
    assert result["ok"] and result["recorded"] and result["version"] == 1
    assert store.receipt(EVENT)["recorded"] is True


def test_legacy_archive_account_is_not_interpreted_as_native_username(ledger):
    store, _ = ledger
    event = store.request(EVENT)["event"]
    event["account_id"] = "opaque_archive_account_id"
    with store.conn:
        store.conn.execute("UPDATE requests SET event=? WHERE event_id=?", (json.dumps(event), EVENT))
    settings = store.config(); settings["groups"][0]["bot_wxid"] = "wxid_other_bot"
    store.configure(settings)
    result = report(store)
    assert result["ok"] and result["recorded"] and result["version"] == 1
