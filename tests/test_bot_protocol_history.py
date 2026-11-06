"""Local conversation context without changing delivery facts or using a gateway."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
from types import SimpleNamespace

import pytest

from chatlog_keeper.bot.protocol_gateway import _json
from chatlog_keeper.bot.protocol_history import ProtocolHistoryStore
from chatlog_keeper.bot.protocol_outbox import OutboxError, ProtocolOutbox
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config


OWN, ACCOUNT = "wxid_robot", "native-test-account"
BASE = datetime(2026, 10, 29, 10, tzinfo=timezone.utc)


@pytest.fixture
def h(tmp_path):
    store = BotStore(tmp_path)
    store.configure(config())
    outbox = ProtocolOutbox(tmp_path / "native.sqlite", account_id=ACCOUNT, own_username=OWN)
    history = ProtocolHistoryStore(store, outbox, own_username=OWN, account_id=ACCOUNT)
    yield SimpleNamespace(store=store, outbox=outbox, history=history)
    outbox.close()
    store.close()


def legacy(h, name, *, second, order=1, bot=OWN, status="sent", group="group_a"):
    event = {"event_id": name, "message_ref": "legacy-" + name, "group_ref": group,
             "sender_id": "peer", "text": name, "occurred_at": BASE + timedelta(seconds=second),
             "archive_id": order, "account_id": "opaque-archive-account",
             "reply_to": {"message_ref": "legacy-" + name, "group_ref": group, "bot_wxid": bot}}
    h.store.begin_request(event)
    reply = h.store.queue_reply(name, "answer " + name)
    if status != "pending":
        h.store.transition_reply(reply["reply_id"], "sending")
        if status == "sent":
            h.store.transition_reply(reply["reply_id"], "sent", confirmation_ref="confirmation-" + name)
        else:
            h.store.transition_reply(reply["reply_id"], status)
    return name


def native(h, sid, *, second, order=1, state="accepted", group="group_a",
           project=True, freeze=True, final=True):
    thread = group + "@chatroom"
    digest = hashlib.sha256(_json([OWN, thread, sid]).encode()).hexdigest()
    request_id, message_ref = "protocol:" + digest, "native:" + digest
    text, answer = "native " + sid, "answer native " + sid
    when = BASE + timedelta(seconds=second)
    source = {"receivedId": "recv-" + sid, "firstReceivedSequence": str(order),
              "accountId": ACCOUNT, "ownUsername": OWN, "sourceSha256": "a" * 64,
              "disposition": "ready", "reason": None, "threadId": thread, "serverId": sid,
              "senderId": "peer", "direction": "incoming", "type": 1, "appMessageType": None,
              "createdAt": when.isoformat(), "body": text, "mentions": {"state": "known", "userIds": [OWN]},
              "quote": None, "quoteEligible": True}
    quote = {"transport": "native_protocol", "message_ref": message_ref, "group_ref": group,
             "account_id": ACCOUNT, "own_username": OWN, "thread_id": thread,
             "received_id": source["receivedId"], "server_id": sid, "source_sha256": source["sourceSha256"]}
    event = {"event_id": request_id, "message_ref": message_ref, "group_ref": group,
             "sender_id": "peer", "text": text, "occurred_at": when, "archive_id": order,
             "transport": "native_protocol", "account_id": OWN, "gateway_account_id": ACCOUNT,
             "group_id": thread, "server_id": sid, "native_source": source, "reply_to": quote}
    h.store.begin_request(event)
    reply = h.store.reply(request_id)
    frozen = {"request_id": request_id, "reply_id": reply["reply_id"], "source": source,
              "text": answer, "evidence": {"request": {"event_id": request_id}, "facts": []}}
    if final:
        metadata = {"protocol_frozen_" + request_id: frozen} if freeze else {}
        h.store.queue_reply(request_id, answer, reply_to=quote, metadata=metadata)
    if project:
        h.outbox.freeze(**frozen)
        if state != "queued":
            # Fixture delivery state only; history never runs a gateway action.
            h.outbox._save(reply["reply_id"], state=state)
    return request_id


def users(messages):
    return [row["content"] for row in messages if row["role"] == "user"]


def update_event(h, request_id, change):
    event = h.store.request(request_id)["event"]
    change(event)
    with h.store.conn:
        h.store.conn.execute("UPDATE requests SET event=? WHERE event_id=?", (_json(event), request_id))


def test_legacy_and_native_mix_by_source_time_and_order_without_state_changes(h):
    legacy(h, "legacy-old", second=1)
    native(h, "10", second=2, order=1)
    legacy(h, "legacy-later-order", second=2, order=2)
    recent = native(h, "20", second=3)
    before = (list(h.store.conn.iterdump()), list(h.outbox._db.iterdump()))
    assert users(h.history.history("group_a", "alpha", 1)) == [
        "legacy-old", "native 10", "legacy-later-order", "native 20"]
    assert users(h.history.history("group_a", "alpha", "1", limit=2)) == [
        "legacy-later-order", "native 20"]
    assert h.store.request(recent)["status"] == "generated"
    assert h.store.reply(recent)["status"] == "pending"
    assert h.outbox.get(h.store.reply(recent)["reply_id"])["phone_received"] is False
    assert (list(h.store.conn.iterdump()), list(h.outbox._db.iterdump())) == before
    assert h.history.directory == h.store.directory and h.history.path == h.store.path


@pytest.mark.parametrize("state", ["queued", "submitting", "unknown", "rejected"])
def test_unaccepted_native_and_unconfirmed_legacy_turns_are_not_history(h, state):
    native(h, "10", second=1, state=state)
    legacy(h, "pending", second=2, status="pending")
    legacy(h, "unknown", second=3, status="unknown")
    assert h.history.history("group_a", "alpha", 1) == []


@pytest.mark.parametrize("changes", [{"project": False}, {"freeze": False}, {"final": False}])
def test_missing_transport_or_business_final_is_not_history(h, changes):
    native(h, "10", second=1, **changes)
    assert h.history.history("group_a", "alpha", 1) == []


@pytest.mark.parametrize("target,field,value", [
    ("event", "account_id", "other-real-account"),
    ("event", "gateway_account_id", "other-gateway"),
    ("source", "ownUsername", "other-real-account"),
    ("source", "accountId", "other-gateway"),
    ("source", "serverId", "99"),
    ("source", "senderId", "other-peer"),
    ("source", "body", "changed source"),
    ("quote", "received_id", "other-source"),
])
def test_native_identity_or_quote_mismatch_is_excluded(h, target, field, value):
    key = native(h, "10", second=1)
    def change(event):
        document = event if target == "event" else event["native_source" if target == "source" else "reply_to"]
        document[field] = value
    update_event(h, key, change)
    assert h.history.history("group_a", "alpha", 1) == []


def test_wrong_group_persona_version_business_and_explicit_legacy_bot_are_excluded(h):
    native(h, "10", second=1, group="group_b")
    persona = native(h, "20", second=2)
    version = native(h, "30", second=3)
    business = native(h, "40", second=4)
    legacy(h, "wrong-bot", second=5, bot="other-bot")
    with h.store.conn:
        h.store.conn.execute("UPDATE requests SET persona_id='other' WHERE event_id=?", (persona,))
        h.store.conn.execute("UPDATE requests SET persona_version='2' WHERE event_id=?", (version,))
        h.store.conn.execute("UPDATE requests SET business_id='other' WHERE event_id=?", (business,))
    assert h.history.history("group_a", "alpha", 1) == []
    assert users(h.history.history("group_b", "alpha", 1)) == ["native 10"]


def test_native_record_cannot_fall_back_to_legacy_sent_flags(h):
    key = native(h, "10", second=1, state="unknown")
    update_event(h, key, lambda event: event.pop("transport"))
    reply = h.store.reply(key)
    h.store.transition_reply(reply["reply_id"], "sending")
    h.store.transition_reply(reply["reply_id"], "sent", confirmation_ref="synthetic")
    assert h.history.history("group_a", "alpha", 1) == []


def test_recent_unaccepted_rows_do_not_hide_an_older_accepted_turn(h):
    native(h, "1", second=1)
    for n in range(2, 36):
        native(h, str(n), second=n, state="unknown")
    assert users(h.history.history("group_a", "alpha", 1, limit=1)) == ["native 1"]


def test_outbox_reads_never_hold_business_store_lock(h, monkeypatch):
    native(h, "10", second=1)
    get = h.outbox.get
    def check(reply_id):
        assert not h.store.lock._is_owned()
        return get(reply_id)
    monkeypatch.setattr(h.outbox, "get", check)
    assert users(h.history.history("group_a", "alpha", 1)) == ["native 10"]


@pytest.mark.parametrize("field", ["source", "text", "evidence"])
def test_accepted_transport_must_match_the_frozen_source_answer_and_facts(h, monkeypatch, field):
    key = native(h, "10", second=1)
    record = deepcopy(h.outbox.get(h.store.reply(key)["reply_id"]))
    if field == "source":
        record[field]["serverId"] = "99"
    elif field == "text":
        record[field] = "a different answer"
    else:
        record[field] = {"different": "facts"}
    monkeypatch.setattr(h.outbox, "get", lambda reply_id: record)
    assert h.history.history("group_a", "alpha", 1) == []


def test_only_missing_outbox_records_are_ignored(h, monkeypatch):
    native(h, "10", second=1)
    monkeypatch.setattr(h.outbox, "get", lambda key: None)
    assert h.history.history("group_a", "alpha", 1) == []
    def failure(key):
        raise OutboxError("unexpected_storage_error")
    monkeypatch.setattr(h.outbox, "get", failure)
    with pytest.raises(OutboxError, match="unexpected_storage_error"):
        h.history.history("group_a", "alpha", 1)


@pytest.mark.parametrize("limit", [0, 101, -1, True, "1", 1.5])
def test_history_limit_is_a_bounded_number_of_turns(h, limit):
    with pytest.raises(ValueError, match="invalid_bot_history_limit"):
        h.history.history("group_a", "alpha", 1, limit=limit)


@pytest.mark.parametrize("own,account", [("other", ACCOUNT), (OWN, "other")])
def test_facade_rejects_outbox_for_another_account(h, own, account):
    with pytest.raises(ValueError, match="protocol_history_account_mismatch"):
        ProtocolHistoryStore(h.store, h.outbox, own_username=own, account_id=account)
