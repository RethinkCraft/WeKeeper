"""Native custody -> business -> frozen final, with no archive, model or UI."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

from chatlog_keeper.bot.protocol_business import ProtocolBusinessError, ProtocolBusinessWorker
from chatlog_keeper.bot.protocol_gateway import _json
from chatlog_keeper.bot.protocol_outbox import ProtocolOutbox
from chatlog_keeper.bot.reply_generator import GenerationResult
from chatlog_keeper.bot.service import Service
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config


OWN, ACCOUNT = "wxid_robot", "native-test-account"
GROUPS = {"a@chatroom": "group_a", "b@chatroom": "group_b", "c@chatroom": "group_c"}


class Inbox:
    def __init__(self):
        self.own_username = OWN
        self.gateway = SimpleNamespace(account_id=ACCOUNT)
        self.rows, self.states, self.blocked = [], {}, {}
        self.context, self.context_calls = [], []

    def add(self, sid, text="你好", *, thread="a@chatroom", sender="wxid_manager", mentions=True,
            created_at=None):
        n = len(self.rows) + 1
        request_id = "protocol:" + hashlib.sha256(_json([OWN, thread, sid]).encode()).hexdigest()
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        source = {"receivedId": "recv_" + sid, "firstReceivedSequence": str(n),
                  "accountId": ACCOUNT, "ownUsername": OWN, "sourceSha256": hashlib.sha256((sid + text).encode()).hexdigest(),
                  "threadId": thread, "serverId": sid, "senderId": sender, "direction": "incoming",
                  "disposition": "ready", "reason": None, "type": 1, "appMessageType": None,
                  "createdAt": created_at or now, "body": text,
                  "mentions": {"state": "known", "userIds": [OWN] if mentions else []},
                  "quote": None, "quoteEligible": True}
        row = {"request_id": request_id, "own_username": OWN, "account_id": ACCOUNT,
               "thread_id": thread, "server_id": sid, "message_id": source["receivedId"],
               "event_cursor": str(n), "received_at": now, "message_json": source}
        self.rows.append(row); self.states[request_id] = "pending"
        return request_id

    def pending_requests(self, *, limit=500):
        return deepcopy([r for r in self.rows if self.states[r["request_id"]] == "pending"][:limit])

    def complete(self, request_id):
        self.states[request_id] = "completed"

    def request_state(self, request_id):
        return self.states[request_id]

    def source_quarantine_reason(self, received_id):
        return self.blocked.get(received_id)

    def nearby_context(self, request_id, *, limit=5):
        self.context_calls.append((request_id, limit))
        return deepcopy(self.context[-limit:])

    def quarantine(self, request_id):
        row = next(r for r in self.rows if r["request_id"] == request_id)
        self.blocked[row["message_id"]] = "message_recalled"
        if self.states[request_id] == "pending":
            self.states[request_id] = "quarantined"


class Generator:
    def __init__(self, store):
        self.store, self.calls = store, []
        self.pause, self.entered = None, threading.Event()

    async def generate(self, event, request, nearby, progress=None):
        self.calls.append(event["event_id"])
        service = Service(self.store, {"event_id": event["event_id"], "writable": True})
        result = None
        if "报备" in event["text"]:
            result = service.call("report_offer_status", {"item": "娱乐首单", "status": "unavailable", "evidence": event["text"]})
            self.entered.set()
            if self.pause:
                assert self.pause.wait(3), "synthetic generation release missing"
        elif "查询" in event["text"]:
            result = service.call("query_offer_status", {"item": "娱乐首单"})
        return GenerationResult("合成的已验证回复", {"request": {"event_id": event["event_id"]}, "result": result}, {})


@pytest.fixture
def harness(tmp_path):
    inbox, store = Inbox(), BotStore(tmp_path)
    store.configure(config())
    outbox = ProtocolOutbox(tmp_path / "transport.sqlite", account_id=ACCOUNT, own_username=OWN)
    generator = Generator(store)
    workers = []
    def worker(**changes):
        value = ProtocolBusinessWorker(inbox=inbox, store=store, generator=generator, outbox=outbox,
                                       bindings=GROUPS, own_username=OWN, account_id=ACCOUNT, **changes)
        workers.append(value)
        return value
    yield SimpleNamespace(inbox=inbox, store=store, outbox=outbox, generator=generator, worker=worker)
    if generator.pause:
        generator.pause.set()
    for value in workers:
        value.close()
    outbox.close(); store.close()


def wait_finished(worker):
    for value in list(worker.inflight.values()):
        value["future"].result(timeout=3)


@pytest.mark.parametrize("configured,expected", [(None, 5), (0, 0), (2, 2), (9, 5), (-1, 0)])
def test_generation_uses_configured_native_nearby_context(harness, configured, expected):
    h = harness
    settings = config()
    if configured is not None:
        settings["nearby_messages"] = configured
    h.store.configure(settings)
    request_id = h.inbox.add("10")
    h.inbox.context = [{"message_ref": "native:" + str(n), "sender_name": "synthetic-sender",
                        "text": "same body", "create_time": n} for n in range(5)]
    seen = []

    async def generate(event, request, nearby, progress=None):
        seen.append(deepcopy(nearby))
        return GenerationResult("synthetic final", {"request": {"event_id": event["event_id"]}}, {})

    h.generator.generate = generate
    h.worker().run_once(block=True)
    assert seen == [h.inbox.context[-expected:] if expected else []]
    assert h.inbox.context_calls == ([(request_id, expected)] if expected else [])


def test_same_identity_replay_and_same_body_distinct_ids(harness):
    h = harness
    a = h.inbox.add("18446744073709551615")
    b = h.inbox.add("18446744073709551614")
    worker = h.worker()
    assert worker.admit_pending() == 2
    assert worker.admit_pending() == 0
    assert h.store.request(a)["message_ref"] != h.store.request(b)["message_ref"]
    worker.run_once(block=True); worker.run_once(block=True)
    assert len(h.generator.calls) == 2
    assert worker.project_outbox()["projected"] == 2
    assert len(h.outbox.pending()) == 2


@pytest.mark.parametrize("field,value", [("createdAt", "2025-01-01T00:00:00+00:00"),
                                        ("senderId", "wxid_other"), ("sourceSha256", "f" * 64)])
def test_admission_replay_rejects_changed_native_identity(harness, field, value):
    h = harness; h.inbox.add("10"); worker = h.worker(); worker.admit_pending()
    h.inbox.rows[0]["message_json"][field] = value
    with pytest.raises(ProtocolBusinessError, match="replay_conflict"):
        worker.admit_pending()
    assert not h.generator.calls


def test_recovery_after_business_admission_is_idempotent(harness):
    h = harness; key = h.inbox.add("10"); worker = h.worker()
    worker.admit_pending(); worker.close()
    restarted = h.worker(); restarted.run_once(block=True)
    assert h.generator.calls == [key]
    assert restarted.project_outbox()["projected"] == 1


def test_frozen_final_restart_does_not_generate_or_read_business_again(harness, monkeypatch):
    h = harness; key = h.inbox.add("10"); worker = h.worker()
    worker.run_once(block=True); worker.close()
    monkeypatch.setattr(h.generator, "generate", lambda *a, **k: pytest.fail("frozen final must not regenerate"))
    monkeypatch.setattr(h.store, "receipt", lambda *a: pytest.fail("frozen final must not reread business"))
    restarted = h.worker(); restarted.run_once()
    assert restarted.project_outbox() == {"projected": 1, "errors": []}
    frozen = h.store.get("protocol_frozen_" + key)
    item = h.outbox.pending()[0]
    assert (item["text"], item["evidence"], item["source"]) == (frozen["text"], frozen["evidence"], frozen["source"])


def test_same_business_fifo_releases_after_freeze_not_transport(harness, monkeypatch):
    h = harness
    report = h.inbox.add("10", "今日娱乐首单已无，请报备")
    query = h.inbox.add("20", "查询娱乐首单", thread="b@chatroom")
    other = h.inbox.add("30", "你好", thread="c@chatroom")
    h.generator.pause = threading.Event(); worker = h.worker()
    worker.run_once()
    assert h.generator.entered.wait(2)
    assert query not in h.generator.calls
    assert set(worker.inflight) == {report, other}
    h.generator.pause.set(); wait_finished(worker)
    worker.run_once(block=True)
    assert h.generator.calls.index(report) < h.generator.calls.index(query)
    query_facts = h.store.get("protocol_frozen_" + query)["evidence"]["result"]["items"][0]
    assert query_facts["version"] == 1 and query_facts["status"] == "unavailable"
    monkeypatch.setattr(h.outbox, "freeze", lambda **k: (_ for _ in ()).throw(OSError("synthetic transport disk failure")))
    assert len(worker.project_outbox()["errors"]) == 3
    fourth = h.inbox.add("40", "查询娱乐首单")
    worker.run_once(block=True)
    assert fourth in h.generator.calls
    assert h.store.get("protocol_frozen_" + fourth) is not None


def test_failed_final_transaction_preserves_same_business_barrier(harness):
    h = harness; report = h.inbox.add("10", "今日娱乐首单已无，请报备"); query = h.inbox.add("20", "查询娱乐首单")
    worker = h.worker()
    h.store.conn.execute("CREATE TRIGGER fail_final BEFORE INSERT ON meta WHEN NEW.key LIKE 'protocol_frozen_%' BEGIN SELECT RAISE(ABORT,'synthetic final disk fault'); END;")
    with pytest.raises(sqlite3.DatabaseError, match="synthetic final"):
        worker.run_once(block=True)
    assert h.generator.calls == [report] and report in worker.inflight
    assert h.store.reply(report)["status"] == "awaiting_generation"
    assert h.store.receipt(report)["version"] == 1
    h.store.conn.execute("DROP TRIGGER fail_final")
    worker.run_once(block=True)
    assert h.generator.calls == [report, query]
    assert len([r for r in h.store.audit() if r["kind"] == "report"]) == 1


def test_committed_report_recovery_uses_receipt_without_generator(harness, monkeypatch):
    h = harness; key = h.inbox.add("10", "今日娱乐首单已无，请报备"); worker = h.worker(); worker.admit_pending()
    event = h.store.request(key)["event"]
    receipt = Service(h.store, {"event_id": key, "writable": True}).call("report_offer_status", {
        "item": "娱乐首单", "status": "unavailable", "evidence": event["text"]})
    assert receipt["recorded"]
    monkeypatch.setattr(h.generator, "generate", lambda *a, **k: pytest.fail("committed write must not rerun"))
    worker.run_once()
    assert "已记录" in h.store.reply(key)["text"]
    assert len([r for r in h.store.audit() if r["kind"] == "report"]) == 1


def test_quarantine_after_freeze_before_projection_is_never_queued(harness):
    h = harness; key = h.inbox.add("10"); worker = h.worker(); worker.run_once(block=True)
    h.inbox.quarantine(key)
    assert h.inbox.request_state(key) == "completed"
    assert worker.project_outbox()["projected"] == 1
    assert h.outbox.pending() == []
    assert h.outbox.get(h.store.reply(key)["reply_id"])["state"] == "rejected"


def test_quarantine_during_generation_revokes_unperformed_report(harness):
    h = harness; key = h.inbox.add("10", "今日娱乐首单已无，请报备")
    entered, release = threading.Event(), threading.Event()
    async def generate(event, request, nearby, progress=None):
        entered.set(); assert release.wait(3)
        result = Service(h.store, {"event_id": key, "writable": True}).call("report_offer_status", {
            "item": "娱乐首单", "status": "unavailable", "evidence": event["text"]})
        assert result["error"] == "bot_request_closed"
        return GenerationResult("没有写入", {"request": {"event_id": key}}, {})
    h.generator.generate = generate
    worker = h.worker(); worker.run_once(); assert entered.wait(2)
    h.inbox.quarantine(key); worker.run_once()
    release.set(); wait_finished(worker); worker.run_once()
    assert h.store.receipt(key) is None and h.outbox.pending() == []
    assert h.store.get("protocol_blocked_" + key)["delivery_blocked"] is True


def test_late_configuration_disable_is_explicitly_cancelled(harness, monkeypatch):
    h = harness; key = h.inbox.add("10"); worker = h.worker()
    original = h.store.queue_reply
    def disable(*args, **kwargs):
        value = h.store.config(); value["groups"][0]["enabled"] = False; h.store.configure(value)
        return original(*args, **kwargs)
    monkeypatch.setattr(h.store, "queue_reply", disable)
    worker.run_once(block=True)
    assert h.store.request(key)["status"] == "cancelled"
    assert h.inbox.request_state(key) == "completed"
    assert h.store.get("protocol_frozen_" + key) is None


def test_wrong_owner_and_second_worker_refused(harness):
    h = harness; worker = h.worker()
    with pytest.raises(ProtocolBusinessError, match="already_running"):
        h.worker()
    worker.close(); h.inbox.own_username = "different-account"
    with pytest.raises(ProtocolBusinessError, match="account_mismatch"):
        h.worker()


def test_other_native_account_rows_are_not_recovered(harness):
    h = harness
    h.store.begin_request({"event_id": "protocol:other-account", "message_ref": "native:other-account",
        "group_ref": "group_a", "sender_id": "wxid_manager", "text": "other account pending",
        "occurred_at": datetime.now(timezone.utc).isoformat(), "transport": "native_protocol",
        "account_id": "other-own", "gateway_account_id": "other-alias"})
    key = h.inbox.add("10"); worker = h.worker(); worker.run_once(block=True)
    assert h.generator.calls == [key]
    assert worker.project_outbox()["projected"] == 1
    assert h.store.reply("protocol:other-account")["status"] == "awaiting_generation"


def test_same_text_without_frozen_evidence_cannot_release_business_lane(harness, monkeypatch):
    h = harness; key = h.inbox.add("10"); h.inbox.add("20"); worker = h.worker()
    original = h.store.queue_reply
    def lose_metadata(*args, **kwargs):
        kwargs["metadata"] = {}
        return original(*args, **kwargs)
    monkeypatch.setattr(h.store, "queue_reply", lose_metadata)
    with pytest.raises(ProtocolBusinessError, match="final_not_frozen"):
        worker.run_once(block=True)
    assert h.generator.calls == [key] and key in worker.inflight
    assert h.inbox.request_state(key) == "pending" and h.outbox.pending() == []
    # Model a process crash after the invalid store implementation lost facts.
    # Durable detection, rather than another model run, must survive restart.
    worker.inflight.clear(); worker.close()
    restarted = h.worker()
    with pytest.raises(ProtocolBusinessError, match="final_not_frozen"):
        restarted.run_once()
    assert h.generator.calls == [key]


def test_quarantine_preserves_already_committed_report_and_marks_blocked(harness):
    h = harness; key = h.inbox.add("10", "今日娱乐首单已无，请报备")
    h.generator.pause = threading.Event(); worker = h.worker(); worker.run_once()
    assert h.generator.entered.wait(2)
    h.inbox.quarantine(key); worker.run_once()
    h.generator.pause.set(); wait_finished(worker); worker.run_once()
    assert h.store.request(key)["status"] == "cancelled"
    assert h.store.receipt(key)["version"] == 1
    assert h.store.get("protocol_blocked_" + key)["business_receipt"]["recorded"] is True
    assert len([r for r in h.store.audit() if r["kind"] == "report"]) == 1
    assert h.outbox.pending() == []


def test_shutdown_freezes_completed_result_without_starting_next_request(harness):
    h = harness
    first = h.inbox.add("10", "查询娱乐首单")
    second = h.inbox.add("20", "查询娱乐首单")
    worker = h.worker(); worker.run_once(); worker.close()
    assert h.generator.calls == [first]
    assert h.store.get("protocol_frozen_" + first) is not None
    assert h.store.reply(second)["status"] == "awaiting_generation"


def test_disable_after_freeze_blocks_projection_without_erasing_final(harness):
    h = harness; key = h.inbox.add("10"); worker = h.worker(); worker.run_once(block=True)
    original = deepcopy(h.store.get("protocol_frozen_" + key))
    settings = h.store.config(); settings["groups"][0]["enabled"] = False; h.store.configure(settings)
    assert worker.project_outbox()["projected"] == 1
    reply = h.outbox.get(h.store.reply(key)["reply_id"])
    assert reply["state"] == "rejected" and reply["source_blocked"] == "configuration_changed"
    assert h.store.get("protocol_frozen_" + key) == original


def test_omitted_persona_preserves_default_persona_and_reply(harness):
    h = harness
    settings = h.store.config(); settings["groups"][0].pop("persona_id")
    h.store.configure(settings)
    key = h.inbox.add("10"); worker = h.worker()
    worker.run_once(block=True)
    assert h.generator.calls == [key]
    assert h.store.reply(key)["status"] == "pending"
    assert worker.project_outbox() == {"projected": 1, "errors": []}
    assert len(h.outbox.pending()) == 1


@pytest.mark.parametrize("identity_scope", ["group", "global"])
@pytest.mark.parametrize("already_frozen", [False, True])
def test_changed_bot_identity_blocks_old_account_work(harness, identity_scope, already_frozen):
    h = harness
    if identity_scope == "global":
        settings = h.store.config(); settings["groups"][0].pop("bot_wxid")
        h.store.configure(settings); h.store.set("bot_wxid", OWN)
    key = h.inbox.add("10"); worker = h.worker()
    if already_frozen:
        worker.run_once(block=True)
    else:
        worker.admit_pending()
    if identity_scope == "group":
        settings = h.store.config(); settings["groups"][0]["bot_wxid"] = "wxid_other_bot"
        h.store.configure(settings)
    else:
        h.store.set("bot_wxid", "wxid_other_bot")
    assert worker.delivery_block_reason({"request_id": key}) == "configuration_changed"
    if already_frozen:
        original = deepcopy(h.store.get("protocol_frozen_" + key))
        assert worker.project_outbox() == {"projected": 1, "errors": []}
        assert h.outbox.get(h.store.reply(key)["reply_id"])["source_blocked"] == "configuration_changed"
        assert h.store.get("protocol_frozen_" + key) == original
    else:
        worker.run_once()
        assert h.generator.calls == []
        assert h.store.request(key)["status"] == "cancelled"
        assert h.inbox.request_state(key) == "completed"
    assert h.outbox.pending() == []


def test_disabled_admitted_reply_closes_request_and_inbox(harness):
    h = harness; key = h.inbox.add("10"); worker = h.worker()
    worker.admit_pending()
    settings = h.store.config(); settings["groups"][0]["enabled"] = False; h.store.configure(settings)
    assert h.store.reply(key)["status"] == "cancelled"
    worker.run_once()
    assert h.store.request(key)["status"] == "cancelled"
    assert h.inbox.request_state(key) == "completed"
    assert h.store.get("protocol_blocked_" + key)["delivery_blocked"] is True
    assert h.generator.calls == [] and h.outbox.pending() == []


def test_cancelled_reply_cannot_write_if_reenabled_before_scheduler_tick(harness):
    h = harness; key = h.inbox.add("10", "今日娱乐首单已无，请报备"); worker = h.worker()
    worker.admit_pending()
    settings = h.store.config(); settings["groups"][0]["enabled"] = False; h.store.configure(settings)
    settings["groups"][0]["enabled"] = True; h.store.configure(settings)
    assert h.store.request(key)["status"] == "processing"
    result = Service(h.store, {"event_id": key, "writable": True}).call("report_offer_status", {
        "item": "娱乐首单", "status": "unavailable", "evidence": "今日娱乐首单已无，请报备"})
    assert result == {"ok": False, "error": "bot_request_closed"}
    assert h.store.receipt(key) is None
    assert not [row for row in h.store.audit() if row["kind"] == "report"]
    worker.run_once()
    assert h.store.request(key)["status"] == "cancelled" and h.inbox.request_state(key) == "completed"
    assert h.generator.calls == []


def test_cancelled_reply_retains_receipt_replay_but_cannot_write_another_item(harness):
    h = harness; key = h.inbox.add("10", "今日娱乐首单已无，请报备"); worker = h.worker()
    worker.admit_pending()
    service = Service(h.store, {"event_id": key, "writable": True})
    original = service.call("report_offer_status", {
        "item": "娱乐首单", "status": "unavailable", "evidence": "今日娱乐首单已无，请报备"})
    assert original["recorded"] is True
    settings = h.store.config(); settings["groups"][0]["enabled"] = False; h.store.configure(settings)
    settings["groups"][0]["enabled"] = True; h.store.configure(settings)
    replayed = h.store.record_offer(key, "fun", "unavailable", evidence="今日娱乐首单已无，请报备")
    assert replayed == {**original, "replayed": True}
    assert h.store.record_offer(key, "rank", "unavailable", evidence="old model continuation") == {
        "ok": False, "error": "bot_request_closed"}
    worker.run_once()
    assert h.store.request(key)["status"] == "cancelled"
    assert h.store.get("protocol_blocked_" + key)["business_receipt"]["version"] == 1
    assert len([row for row in h.store.audit() if row["kind"] == "report"]) == 1
    assert h.outbox.pending() == []


@pytest.mark.parametrize("mutation", ["group_account", "global_account", "group_disabled"])
def test_native_binding_change_after_service_authorization_cannot_write(harness, monkeypatch, mutation):
    h = harness
    if mutation == "global_account":
        settings = h.store.config(); settings["groups"][0].pop("bot_wxid")
        h.store.configure(settings); h.store.set("bot_wxid", OWN)
    key = h.inbox.add("10", "今日娱乐首单已无，请报备"); worker = h.worker(); worker.admit_pending()
    original = h.store.record_offer
    passed = []
    def change_before_transaction(*args, **kwargs):
        assert not h.store.conn.in_transaction
        passed.append(True)
        # An independent configuration connection changes authorization after
        # Service checked it but before BEGIN IMMEDIATE in record_offer.
        peer = BotStore(h.store.directory.parent)
        try:
            if mutation == "global_account":
                peer.set("bot_wxid", "wxid_other_bot")
            else:
                settings = peer.config()
                if mutation == "group_account":
                    settings["groups"][0]["bot_wxid"] = "wxid_other_bot"
                else:
                    settings["groups"][0]["enabled"] = False
                peer.configure(settings)
        finally:
            peer.close()
        return original(*args, **kwargs)
    monkeypatch.setattr(h.store, "record_offer", change_before_transaction)
    result = Service(h.store, {"event_id": key, "writable": True}).call("report_offer_status", {
        "item": "娱乐首单", "status": "unavailable", "evidence": "今日娱乐首单已无，请报备"})
    assert passed == [True]
    assert result == {"ok": False, "error": "bot_request_closed" if mutation == "group_disabled" else "bot_binding_changed"}
    assert h.store.receipt(key) is None
    assert not [row for row in h.store.audit() if row["kind"] == "report"]
    worker.run_once()
    assert h.store.request(key)["status"] == "cancelled" and h.inbox.request_state(key) == "completed"
    assert h.generator.calls == []


@pytest.mark.parametrize("identity_scope", ["group", "global"])
def test_tick_revokes_inflight_generation_after_account_change(harness, identity_scope):
    h = harness
    if identity_scope == "global":
        settings = h.store.config(); settings["groups"][0].pop("bot_wxid")
        h.store.configure(settings); h.store.set("bot_wxid", OWN)
    key = h.inbox.add("10", "今日娱乐首单已无，请报备")
    entered, release = threading.Event(), threading.Event()
    async def paused_generation(event, request, nearby, progress=None):
        entered.set(); assert release.wait(3)
        result = Service(h.store, {"event_id": key, "writable": True}).call("report_offer_status", {
            "item": "娱乐首单", "status": "unavailable", "evidence": event["text"]})
        assert result == {"ok": False, "error": "bot_request_closed"}
        return GenerationResult("任务已取消", {"request": {"event_id": key}}, {})
    h.generator.generate = paused_generation
    worker = h.worker(); worker.run_once(); assert entered.wait(2)
    try:
        if identity_scope == "global":
            h.store.set("bot_wxid", "wxid_other_bot")
        else:
            settings = h.store.config(); settings["groups"][0]["bot_wxid"] = "wxid_other_bot"
            h.store.configure(settings)
        worker.run_once()
        assert key in worker.inflight and h.store.request(key)["status"] == "cancelled"
        assert h.store.reply(key)["status"] == "cancelled" and h.inbox.request_state(key) == "completed"
    finally:
        release.set()
    wait_finished(worker); worker.run_once()
    assert h.store.receipt(key) is None and h.outbox.pending() == []


@pytest.mark.parametrize("identity_scope", ["group", "global"])
def test_native_account_change_preserves_receipt_replay_without_new_item(harness, identity_scope):
    h = harness
    if identity_scope == "global":
        settings = h.store.config(); settings["groups"][0].pop("bot_wxid")
        h.store.configure(settings); h.store.set("bot_wxid", OWN)
    key = h.inbox.add("10", "今日娱乐首单已无，请报备"); worker = h.worker(); worker.admit_pending()
    original = Service(h.store, {"event_id": key, "writable": True}).call("report_offer_status", {
        "item": "娱乐首单", "status": "unavailable", "evidence": "今日娱乐首单已无，请报备"})
    if identity_scope == "global":
        h.store.set("bot_wxid", "wxid_other_bot")
    else:
        settings = h.store.config(); settings["groups"][0]["bot_wxid"] = "wxid_other_bot"
        h.store.configure(settings)
    before_audit = h.store.audit()
    assert h.store.record_offer(key, "fun", "unavailable", evidence="old model continuation") == {**original, "replayed": True}
    assert h.store.record_offer(key, "rank", "unavailable", evidence="old model continuation") == {
        "ok": False, "error": "bot_binding_changed"}
    assert h.store.audit() == before_audit
    worker.run_once()
    assert h.store.get("protocol_blocked_" + key)["business_receipt"]["version"] == 1
    assert h.store.request(key)["status"] == "cancelled" and h.outbox.pending() == []


def _stale_created_at(seconds_ago=3600):
    return datetime.now(timezone.utc).replace(microsecond=0).timestamp() - seconds_ago


def test_stale_chat_and_query_expire_without_generation(harness):
    h = harness
    old = datetime.fromtimestamp(_stale_created_at(), timezone.utc).isoformat()
    chat_key = h.inbox.add("901", "你好", created_at=old)
    query_key = h.inbox.add("902", "查询娱乐首单", created_at=old)
    worker = h.worker(stale_after_seconds=600)
    result = worker.run_once(block=True)
    assert h.generator.calls == []
    assert h.store.request(chat_key)["status"] == "cancelled"
    assert h.store.request(query_key)["status"] == "cancelled"
    assert h.store.get("protocol_expired_" + chat_key)["reason"] == "expired"
    assert h.store.get("protocol_expired_" + query_key)["reason"] == "expired"
    assert h.inbox.request_state(chat_key) == "completed"
    assert h.inbox.request_state(query_key) == "completed"
    assert h.outbox.pending() == []
    kinds = {entry["kind"] for entry in result["stale_events"]}
    assert kinds == {"expired"}
    assert worker.project_outbox() == {"projected": 0, "errors": []}


def test_stale_report_bookkeeps_but_does_not_deliver(harness):
    h = harness
    from zoneinfo import ZoneInfo
    midday = datetime.now(ZoneInfo("Asia/Shanghai")).replace(hour=12, minute=0, second=0, microsecond=0)
    old = midday.isoformat()
    text = "今日娱乐首单已无，请报备"
    key = h.inbox.add("903", text, created_at=old)
    worker = h.worker(stale_after_seconds=600, clock=lambda: midday.timestamp() + 3600)
    worker.run_once(block=True)
    assert key in h.generator.calls
    assert h.store.receipt(key) is not None
    assert h.store.get("protocol_delivery_suppress_" + key) == "suppressed_stale"
    frozen = h.store.get("protocol_frozen_" + key)
    assert frozen["evidence"]["delivery_suppressed"] == "suppressed_stale"
    assert worker.delivery_block_reason(frozen) == "suppressed_stale"
    projected = worker.project_outbox()
    assert projected["projected"] == 1 and projected["errors"] == []
    row = h.outbox.get(frozen["reply_id"])
    assert row["state"] == "rejected" and row["source_blocked"] == "suppressed_stale"
    assert h.outbox.pending() == []
    assert any(entry["kind"] == "suppressed_stale" for entry in worker.stale_events)
    assert h.store.get("protocol_stale_log")[0]["kind"] == "suppressed_stale"


def test_fresh_report_still_delivers(harness):
    h = harness
    key = h.inbox.add("904", "今日娱乐首单已无，请报备")
    worker = h.worker(stale_after_seconds=600)
    worker.run_once(block=True)
    assert h.store.get("protocol_delivery_suppress_" + key) is None
    assert worker.project_outbox() == {"projected": 1, "errors": []}
    assert h.outbox.pending()
