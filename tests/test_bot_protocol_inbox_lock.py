"""HTTP lock boundaries and durable safety controls, using only MockTransport."""
from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import threading

import pytest

from chatlog_keeper.bot.protocol_gateway import GatewayError
from chatlog_keeper.bot.protocol_inbox import InboxError
from test_bot_protocol_inbox import THREAD
from test_bot_protocol_native_inbox import direct, downgrade, request_for


@pytest.mark.parametrize("method", ["activate_native_consumer", "receive", "get_received_message", "ack"])
def test_every_http_phase_releases_db_mutex_for_other_thread(direct, method, monkeypatch):
    original_source = direct.add("10")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox, ThreadPoolExecutor(2) as pool:
        inbox.activate(); inbox.poll()
        request = request_for(inbox, "10")
        direct.add("20")
        entered, release = threading.Event(), threading.Event()
        operation = getattr(gateway, method)
        def blocked(*args, **kwargs):
            entered.set()
            assert release.wait(3)
            return operation(*args, **kwargs)
        monkeypatch.setattr(gateway, method, blocked)
        if method == "activate_native_consumer":
            inbox._needs_activation = True  # Explicit sparse-activation phase under test.
        receiving = pool.submit(inbox.poll)
        try:
            assert entered.wait(1)
            def local():
                assert inbox.pending_requests()
                assert inbox.source_quarantine_reason(original_source) is None
                assert inbox.nearby_context(request) == []
                inbox.complete(request)
                return inbox.request_state(request)
            # A different thread is essential: the old RLock is reentrant.
            assert pool.submit(local).result(timeout=1) == "completed"
        finally:
            release.set()
        assert receiving.result(timeout=2).new_requests == 1
        assert direct.acknowledged == "2"
        assert inbox.request_state(request) == "completed"


def test_poll_serializer_deduplicates_concurrent_polls_without_blocking_reads(direct, monkeypatch):
    direct.add("10")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox, ThreadPoolExecutor(3) as pool:
        inbox.activate()
        entered, release = threading.Event(), threading.Event()
        calls = []
        operation = gateway.activate_native_consumer
        def blocked(*args, **kwargs):
            calls.append(1); entered.set()
            assert release.wait(3)
            return operation(*args, **kwargs)
        monkeypatch.setattr(gateway, "activate_native_consumer", blocked)
        inbox._needs_activation = True  # Ordinary idle polls intentionally skip activation.
        first = pool.submit(inbox.poll)
        try:
            assert entered.wait(1)
            second = pool.submit(inbox.poll)
            assert pool.submit(inbox.pending_requests).result(timeout=1) == []
            assert calls == [1]
        finally:
            release.set()
        assert first.result(timeout=2).new_requests + second.result(timeout=2).new_requests == 1
        assert len(inbox.pending_requests()) == 1 and direct.acknowledged == "1"


@pytest.mark.parametrize("change", ["cursor", "checkpoint"])
def test_http_return_rechecks_local_checkpoint_and_cursor(direct, change, monkeypatch):
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox, ThreadPoolExecutor(1) as pool:
        inbox.activate()
        entered, release = threading.Event(), threading.Event()
        operation = gateway.receive
        def blocked(*args, **kwargs):
            entered.set(); assert release.wait(3)
            return operation(*args, **kwargs)
        monkeypatch.setattr(gateway, "receive", blocked)
        receiving = pool.submit(inbox.poll)
        try:
            assert entered.wait(1)
            with inbox._lock:
                if change == "cursor":
                    inbox._db.execute("UPDATE inbox_meta SET committed_cursor='1'")
                else:
                    checkpoint = inbox._checkpoint(); checkpoint["receivedBoundary"] = "99"
                    inbox._db.execute("UPDATE native_checkpoint SET checkpoint_json=?", (json.dumps(checkpoint),))
        finally:
            release.set()
        with pytest.raises(InboxError, match="inbox_local_state_changed"):
            receiving.result(timeout=2)
        assert direct.acknowledged == "0"


@pytest.mark.parametrize("change", ["raw", "extra", "page"])
def test_snapshot_apply_checks_entire_staged_page_before_ack(direct, change, monkeypatch):
    direct.add("10")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox, ThreadPoolExecutor(1) as pool:
        inbox.activate()
        entered, release = threading.Event(), threading.Event()
        operation = gateway.get_received_message
        def blocked(*args, **kwargs):
            entered.set(); assert release.wait(3)
            return operation(*args, **kwargs)
        monkeypatch.setattr(gateway, "get_received_message", blocked)
        receiving = pool.submit(inbox.poll)
        try:
            assert entered.wait(1)
            with inbox._lock:
                if change == "raw":
                    inbox._db.execute("UPDATE inbox_events SET raw_json='{}'")
                elif change == "page":
                    inbox._db.execute("UPDATE inbox_page SET next_cursor='2'")
                else:
                    inbox._db.execute("INSERT INTO inbox_events(cursor,raw_json,received_at)VALUES('2','{}','synthetic')")
        finally:
            release.set()
        with pytest.raises(InboxError, match="inbox_staged_page_changed"):
            receiving.result(timeout=2)
        assert not direct.acks and inbox.pending_requests() == []


def recall(direct, *, thread=THREAD, server="10"):
    direct.events.append({"cursor": str(len(direct.events) + 1), "kind": "message.recalled",
                          "data": {"threadId": thread, "serverId": server}})


@pytest.mark.parametrize("kind", ["raw_quarantine", "raw_recall", "snapshot_downgrade"])
@pytest.mark.parametrize("completed", [False, True])
def test_known_block_is_local_before_rest_of_page_resolves(direct, kind, completed, monkeypatch):
    original = direct.add("10")
    direct.add("10", threadId="other@chatroom")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox, ThreadPoolExecutor(1) as pool:
        inbox.activate(); inbox.poll()
        request = next(r["request_id"] for r in inbox.pending_requests() if r["message_id"] == original)
        other = next(r["request_id"] for r in inbox.pending_requests() if r["thread_id"] == "other@chatroom")
        trigger = request_for(inbox, "20")
        if completed:
            inbox.complete(request)
        if kind == "raw_recall":
            recall(direct)
        else:
            downgrade(direct, original)
            if kind == "snapshot_downgrade":
                direct.events[-1]["data"]["disposition"] = "ready"
        later = direct.add("30")
        entered, release = threading.Event(), threading.Event()
        operation = gateway.get_received_message
        def blocked(identifier):
            if identifier == later:
                entered.set(); assert release.wait(3)
                raise GatewayError("synthetic_snapshot_unavailable")
            return operation(identifier)
        monkeypatch.setattr(gateway, "get_received_message", blocked)
        receiving = pool.submit(inbox.poll)
        try:
            assert entered.wait(1)
            assert inbox.source_quarantine_reason(original)
            assert inbox.request_state(request) == ("completed" if completed else "quarantined")
            if not completed:
                with pytest.raises(InboxError, match="quarantined"):
                    inbox.complete(request)
            assert inbox.request_state(other) == "pending"
            assert inbox.nearby_context(trigger) == []
            assert direct.acknowledged == "3"
        finally:
            release.set()
        with pytest.raises(GatewayError, match="synthetic_snapshot_unavailable"):
            receiving.result(timeout=2)
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        assert inbox.source_quarantine_reason(original)
        assert inbox.request_state(request) == ("completed" if completed else "quarantined")
        if kind == "snapshot_downgrade":
            direct.messages[original].update(disposition="ready", reason=None, quoteEligible=True)
            with pytest.raises(InboxError, match="inbox_received_event_conflict"):
                inbox.poll()
            assert direct.acknowledged == "3"


@pytest.mark.parametrize("kind", ["quarantine", "recall"])
def test_same_page_block_before_new_request_identity_is_still_effective(direct, kind):
    original = direct.add("10")
    if kind == "quarantine":
        downgrade(direct, original)
    else:
        recall(direct)
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        assert [row["server_id"] for row in inbox.pending_requests()] == ["20"]
        assert inbox.source_quarantine_reason(original)
        assert direct.acknowledged == "3"


def test_quarantine_save_failure_retains_negative_guard_without_allowing_completion(direct):
    original = direct.add("10")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        request, trigger = request_for(inbox, "10"), request_for(inbox, "20")
        downgrade(direct, original)
        direct.events[-1]["data"]["disposition"] = "ready"
        inbox._db.execute("""CREATE TRIGGER fail_quarantine BEFORE UPDATE OF projection_json ON inbox_events
            WHEN NEW.classification='quarantined' BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END""")
        with pytest.raises(sqlite3.DatabaseError, match="synthetic disk failure"):
            inbox.poll()
        assert inbox.source_quarantine_reason(original) == "message_recalled"
        assert inbox.nearby_context(trigger) == []
        with pytest.raises(InboxError, match="quarantined"):
            inbox.complete(request)
        assert direct.acknowledged == "2"
        inbox._db.execute("DROP TRIGGER fail_quarantine")
        inbox.poll()
        assert inbox.request_state(request) == "quarantined" and not inbox._denied_sources
        assert direct.acknowledged == "3"


@pytest.mark.parametrize("completed", [False, True])
def test_failed_negative_proof_is_identity_scoped_and_blocks_context_for_completed_source(direct, completed):
    original = direct.add("10")
    alias = direct.add("10")
    other = direct.add("10", threadId="different@chatroom")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        request, trigger = request_for(inbox, "10"), request_for(inbox, "20")
        if completed:
            inbox.complete(request)
        downgrade(direct, alias)
        direct.events[-1]["data"]["disposition"] = "ready"
        inbox._db.execute("""CREATE TRIGGER fail_quarantine BEFORE UPDATE OF projection_json ON inbox_events
            WHEN NEW.classification='quarantined' BEGIN SELECT RAISE(ABORT,'synthetic disk failure'); END""")
        with pytest.raises(sqlite3.DatabaseError):
            inbox.poll()
        assert inbox.source_quarantine_reason(original) == "message_recalled"
        assert inbox.source_quarantine_reason(other) is None
        assert inbox.nearby_context(trigger) == []
        if completed:
            inbox.complete(request)  # A no-op cannot erase the negative proof.
            assert inbox.request_state(request) == "completed"
            assert inbox.source_quarantine_reason(original)
        else:
            with pytest.raises(InboxError, match="quarantined"):
                inbox.complete(request)
        inbox._db.execute("DROP TRIGGER fail_quarantine")
        inbox.poll()
        assert inbox.request_state(request) == ("completed" if completed else "quarantined")
        assert not inbox._denied_sources


def test_close_waits_for_poll_without_seizing_database_mutex(direct, monkeypatch):
    with direct.gateway() as gateway, ThreadPoolExecutor(3) as pool:
        inbox = direct.inbox(gateway); inbox.activate()
        entered, release = threading.Event(), threading.Event()
        operation = gateway.receive
        def blocked(*args, **kwargs):
            entered.set(); assert release.wait(3)
            return operation(*args, **kwargs)
        monkeypatch.setattr(gateway, "receive", blocked)
        receiving = pool.submit(inbox.poll)
        try:
            assert entered.wait(1)
            closing = pool.submit(inbox.close)
            assert pool.submit(inbox.pending_requests).result(timeout=1) == []
            assert not closing.done()
        finally:
            release.set()
        receiving.result(timeout=2); closing.result(timeout=2)
        assert inbox._db is None
        inbox.close()


def test_remote_ack_success_then_local_view_change_reconciles_without_duplicate(direct, monkeypatch):
    direct.add("10")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox, ThreadPoolExecutor(1) as pool:
        inbox.activate()
        entered, release = threading.Event(), threading.Event()
        operation = gateway.ack
        def blocked(*args, **kwargs):
            result = operation(*args, **kwargs)
            entered.set(); assert release.wait(3)
            return result
        monkeypatch.setattr(gateway, "ack", blocked)
        receiving = pool.submit(inbox.poll)
        try:
            assert entered.wait(1) and direct.acknowledged == "1"
            with inbox._lock:
                inbox._db.execute("UPDATE inbox_meta SET acknowledged_cursor='1'")
        finally:
            release.set()
        with pytest.raises(InboxError, match="inbox_local_state_changed"):
            receiving.result(timeout=2)
        requests = inbox.pending_requests()
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        assert inbox.poll().new_requests == 0
        assert inbox.pending_requests() == requests and len(requests) == 1


@pytest.mark.parametrize("unrelated_count", [1000, 10000])
def test_source_guard_does_not_scan_unrelated_event_or_alias_history(direct, unrelated_count):
    source = direct.add("10")
    direct.add("20")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        other = request_for(inbox, "20")
        with inbox._transaction():
            inbox._db.executemany("""INSERT INTO inbox_events
                (cursor,raw_json,received_at,committed,message_id,classification,projection_json)
                VALUES(?,?,?,1,?,?,?)""", [
                (str(i + 3), json.dumps({"kind": "message.recalled" if i % 2 else "message.received",
                    "data": {"threadId": "unrelated@chatroom", "serverId": str(i),
                             "disposition": "quarantined"}}), "synthetic", "unrelated-" + str(i),
                 "quarantined", json.dumps({"accountId": gateway.account_id,
                     "ownUsername": inbox.own_username, "threadId": "unrelated@chatroom", "serverId": str(i)}))
                for i in range(unrelated_count)])
            inbox._db.executemany("INSERT INTO inbox_message_ids VALUES(?,?)",
                                 [("unrelated-alias-" + str(i), other) for i in range(unrelated_count)])
        steps = []
        inbox._db.set_progress_handler(lambda: steps.append(1) or 0, 100)
        try:
            assert inbox.source_quarantine_reason(source) is None
        finally:
            inbox._db.set_progress_handler(None, 0)
        # A VM work bound, not a flaky wall-clock deadline. Scanning either
        # history table exceeds this even for the smaller fixture.
        assert len(steps) < 20
        statements = []
        inbox._db.set_trace_callback(statements.append)
        try:
            assert len(inbox.nearby_context(other)) == 1
        finally:
            inbox._db.set_trace_callback(None)
        query = next(sql for sql in statements if sql.startswith("WITH eligible"))
        plan = [row["detail"] for row in inbox._db.execute("EXPLAIN QUERY PLAN " + query)]
        assert not any(detail.startswith("SCAN q") for detail in plan)
        assert sum(detail.startswith("SEARCH q USING INDEX") for detail in plan) == 3


def test_guard_indexes_backfill_existing_raw_recall_without_projection(direct):
    source = direct.add("10")
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        inbox.activate(); inbox.poll()
        recall(direct)
        page = gateway.receive(consumer=inbox.consumer, after="1", limit=100)
        with inbox._lock:
            inbox._stage(page)
        for name in ("native_blocked_received", "native_blocked_identity", "native_recalled_identity", "native_request_aliases"):
            inbox._db.execute("DROP INDEX " + name)
    with direct.gateway() as gateway, direct.inbox(gateway) as inbox:
        assert inbox.source_quarantine_reason(source) == "message_recalled"
        assert direct.acknowledged == "1"
        assert inbox.poll().acknowledged_cursor == "2"
        assert inbox.source_quarantine_reason(source) == "message_recalled"
