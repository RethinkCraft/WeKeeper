import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from chatlog_keeper.dashboard.protocol import content_hash
from chatlog_keeper.dashboard.store import Conflict, DashboardStore, LoginThrottled

ZONE = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 10, 26, 12, tzinfo=ZONE).timestamp()


def message(ref="msg_a", day="2026-10-26", sender="person_a", text="hello"):
    timestamp = int(datetime.fromisoformat(day + "T10:00:00+08:00").timestamp())
    return {"message_ref": ref, "create_time": timestamp, "sender_id": sender, "sender_name": "A", "msg_type": 1, "text": text, "reply": None}


def partition(messages=None, revision=1, day="2026-10-26", initial=False):
    data = {"group": {"group_ref": "group_a", "name": "测试群"}, "day": day, "messages": messages if messages is not None else [message(day=day)], "coverage": {"local_backfill_complete": True}}
    return {"source_id": "source_a", "group_ref": "group_a", "day": day, "revision": revision, "content_hash": content_hash(data), "initial": initial, "data": data}


@pytest.fixture
def store(tmp_path):
    clock = [NOW]
    value = DashboardStore(tmp_path, clock=lambda: clock[0])
    value.test_clock = clock
    yield value
    value.close()


def test_atomic_partition_replay_old_revision_deletion_and_hash(store):
    first = partition([message(), message("msg_b")])
    assert store.ingest_partition(first)["applied"]
    assert not store.ingest_partition(first)["applied"]
    assert store.group_stats("group_a")["messages"] == 2
    newer = partition([message("msg_b", text="changed")], revision=2)
    store.ingest_partition(newer)
    assert store.group_stats("group_a")["messages"] == 1
    assert not store.ingest_partition(first)["applied"]
    conflict = partition([message("msg_b", text="wrong")], revision=2)
    with pytest.raises(Conflict):
        store.ingest_partition(conflict)
    corrupt = partition(revision=3)
    corrupt["data"]["messages"][0]["text"] = "not hashed"
    with pytest.raises(ValueError, match="hash"):
        store.ingest_partition(corrupt)
    assert store.messages("group_a")["messages"][0]["text"] == "changed"


def test_transaction_rolls_back_when_insert_fails(store):
    store.ingest_partition(partition())
    store.conn.execute("CREATE TRIGGER fail_message BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'failed'); END")
    with pytest.raises(Exception):
        store.ingest_partition(partition([message("msg_b")], revision=2))
    assert store.messages("group_a")["messages"][0]["message_ref"] == "msg_a"
    assert store.conn.execute("SELECT revision FROM partitions").fetchone()[0] == 1


def test_cross_day_people_count_not_sum_and_late_message(store):
    store.ingest_partition(partition(day="2026-10-25"))
    store.ingest_partition(partition([message("msg_today")]))
    stats = store.group_stats("group_a", "2026-10-25T00:00:00+08:00", NOW)
    assert stats["messages"] == 2
    assert stats["active_senders"] == 1
    late = partition([message(day="2026-10-25"), message("msg_late", "2026-10-25", "person_b")], revision=2, day="2026-10-25")
    store.ingest_partition(late)
    assert store.group_stats("group_a", "2026-10-25T00:00:00+08:00", NOW)["active_senders"] == 2


def test_summary_citations_and_invalidation(store):
    initial = partition()
    store.ingest_partition(initial)
    summary = {"source_id": "source_a", "group_ref": "group_a", "day": "2026-10-26", "revision": 1, "input_hash": initial["content_hash"], "model": "test", "generated_at": NOW, "covered_until": NOW, "summary": {"topics": [{"text": "topic", "message_refs": ["msg_a"]}], "decisions": [], "todos": []}}
    assert store.ingest_summary(summary)["applied"]
    assert not store.ingest_summary(summary)["applied"]
    bad = {**summary, "revision": 2, "summary": {"topics": [{"text": "invented", "message_refs": ["msg_unknown"]}]}}
    with pytest.raises(ValueError, match="reference"):
        store.ingest_summary(bad)
    store.ingest_partition(partition([message(text="corrected")], revision=2))
    assert store.summaries("group_a")[0]["stale"]
    with pytest.raises(Conflict, match="stale"):
        store.ingest_summary(summary)


def test_keywords_initial_backfill_replay_and_individual_ack(store):
    a = store.create_user("admin", "long-admin-password")
    b = store.create_user("viewer", "long-viewer-password", "viewer")
    store.save_rule({"name": "urgent", "keyword": "WARNING", "enabled": True})
    initial = partition([message(text="warning historical")], initial=True)
    store.ingest_partition(initial)
    assert store.alerts(a["id"]) == []
    # Only future message timestamps match a newly enabled rule, including outage catch-up.
    newer_msg = message("msg_future", text="Warning urgent")
    newer_msg["create_time"] = int(NOW + 1)
    changed = partition([message(), newer_msg], revision=2)
    store.ingest_partition(changed)
    store.ingest_partition(changed)
    alerts = store.alerts(a["id"])
    assert len(alerts) == 1
    store.mark_alert(alerts[0]["id"], a["id"], acknowledge=True)
    assert store.alerts(a["id"])[0]["acknowledged_at"]
    assert store.alerts(b["id"])[0]["acknowledged_at"] is None
    assert store.alerts(a["id"])[0]["resolved_at"] is None


def heartbeat(now, state="session_present", **extra):
    return {"source_id": "source_a", "health": {"state": state, "reason": "test"}, "collector": {"running": True, "last_cycle_at": now, "last_error": None}, "last_message_at": now - 36000, "summary": {"state": "not_configured", "error": None, "last_success_at": None}, "sent_at": now, **extra}


def test_quiet_groups_heartbeat_timeout_and_confirmed_recovery(store):
    store.ingest_heartbeat(heartbeat(NOW))
    store.test_clock[0] += 40
    store.ingest_heartbeat(heartbeat(store.test_clock[0]))
    store.check_stale_sources()
    assert not store.alerts(1)
    store.test_clock[0] += 91
    store.check_stale_sources()
    assert store.alerts(1)[0]["kind"] == "sync_interrupted"
    store.ingest_heartbeat(heartbeat(store.test_clock[0]))
    assert store.alerts(1)[0]["resolved_at"] is None
    store.test_clock[0] += 30
    store.ingest_heartbeat(heartbeat(store.test_clock[0]))
    assert store.alerts(1)[0]["resolved_at"] is not None


def test_confirmed_fault_does_not_require_desktop_delivery(store):
    store.ingest_heartbeat(heartbeat(NOW, "login_required"))
    store.test_clock[0] += 30
    store.ingest_heartbeat(heartbeat(store.test_clock[0], "login_required"))
    assert len(store.alerts(1)) == 1
    store.test_clock[0] += 30
    store.ingest_heartbeat(heartbeat(store.test_clock[0], "login_required"))
    assert len(store.alerts(1)) == 1


def test_auth_hash_session_revocation_lockout_and_last_admin(store):
    admin = store.create_user("admin", "long-admin-password")
    assert "long-admin-password" not in json.dumps(dict(store.conn.execute("SELECT * FROM users").fetchone()))
    auth = store.login("admin", "long-admin-password")
    assert store.session(auth["token"])["user"]["role"] == "admin"
    with pytest.raises(ValueError, match="last_admin"):
        store.update_user(admin["id"], {"disabled": True})
    store.update_user(admin["id"], {"password": "changed-admin-password"})
    assert not store.session(auth["token"])
    for _ in range(8):
        assert store.login("admin", "wrong-password", "ip") is None
    with pytest.raises(LoginThrottled):
        store.login("admin", "changed-admin-password", "ip")


def test_backup_restores_content_and_persistent_sessions(store, tmp_path):
    user = store.create_user("admin", "long-admin-password")
    token = store.login("admin", "long-admin-password")["token"]
    store.ingest_partition(partition())
    path = store.backup(tmp_path / "restore" / "dashboard.sqlite")
    assert path.stat().st_mode & 0o777 == 0o600
    with_store = DashboardStore(path.parent, clock=lambda: NOW)
    try:
        assert with_store.session(token)["user"]["id"] == user["id"]
        assert with_store.group_stats("group_a")["messages"] == 1
    finally:
        with_store.close()


def test_message_cursor_binds_scope_and_invalidates_after_message_change(store):
    store.ingest_partition(partition([message("msg_a"), message("msg_b")]))
    first = store.messages("group_a", limit=1)
    cursor = first["next_cursor"]
    assert store.messages("group_a", limit=1, before=cursor)["messages"][0]["message_ref"] == "msg_a"
    # Heartbeats refresh the page but do not invalidate message pagination.
    store.ingest_heartbeat(heartbeat(NOW))
    store.messages("group_a", limit=1, before=cursor)
    with pytest.raises(ValueError, match="cursor"):
        store.messages("group_a", limit=1, before=cursor, q="changed query")
    store.ingest_partition(partition([message("msg_a"), message("msg_b", text="corrected")], revision=2))
    with pytest.raises(Conflict, match="cursor_snapshot_changed"):
        store.messages("group_a", limit=1, before=cursor)


def test_unknown_session_does_not_claim_confirmed_login_recovery(store):
    store.ingest_heartbeat(heartbeat(NOW, "login_required"))
    store.test_clock[0] += 30
    store.ingest_heartbeat(heartbeat(store.test_clock[0], "login_required"))
    for _ in range(3):
        store.test_clock[0] += 30
        store.ingest_heartbeat(heartbeat(store.test_clock[0], "unknown"))
    assert store.alerts(1)[0]["resolved_at"] is None


def test_safe_database_permissions_and_backup_destination(store):
    for name in ("dashboard.sqlite", "dashboard.sqlite-wal", "dashboard.sqlite-shm"):
        path = store.root / name
        if path.exists():
            assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(ValueError, match="backup_destination"):
        store.backup(store.root / "dashboard.sqlite")


def test_waiting_summary_is_not_an_incident_and_pause_is(store):
    for index in range(2):
        store.test_clock[0] = NOW + index * 30
        store.ingest_heartbeat(heartbeat(store.test_clock[0], summary={"state": "waiting"}))
    assert not store.alerts(1)
    for index in range(2):
        store.test_clock[0] += 30
        store.ingest_heartbeat(heartbeat(store.test_clock[0], summary={"state": "paused", "error": "usage_limit"}))
    assert store.alerts(1)[0]["kind"] == "summary"


def test_heartbeat_online_does_not_hide_failed_partition_upload(store):
    for index in range(2):
        store.test_clock[0] = NOW + index * 30
        store.ingest_heartbeat(heartbeat(store.test_clock[0], sync={"state": "error", "error": "partition_upload_failed", "last_success_at": NOW - 3600, "pending_partitions": 7}))
    source = store.overview(1)["sources"][0]
    assert not source["stale"]
    assert source["sync"]["pending_partitions"] == 7
    assert any(a["kind"] == "sync_error" and a["resolved_at"] is None for a in store.alerts(1))
    for _ in range(2):
        store.test_clock[0] += 30
        store.ingest_heartbeat(heartbeat(store.test_clock[0], sync={"state": "ready", "error": None, "last_success_at": store.test_clock[0], "pending_partitions": 0}))
    assert next(a for a in store.alerts(1) if a["kind"] == "sync_error")["resolved_at"] is not None


def test_heartbeat_reconciliation_manifest_is_source_scoped_and_metadata_only(store):
    first = partition()
    store.ingest_partition(first)
    other = partition([message("msg_other")])
    other["source_id"] = "source_other"
    other["group_ref"] = "group_other"
    other["data"]["group"]["group_ref"] = "group_other"
    other["content_hash"] = content_hash(other["data"])
    store.ingest_partition(other)
    summary = {"source_id": "source_a", "group_ref": "group_a", "day": "2026-10-26", "revision": 1, "input_hash": first["content_hash"], "model": "test", "generated_at": NOW, "covered_until": NOW, "summary": {"topics": [{"text": "topic", "message_refs": ["msg_a"]}], "decisions": [], "todos": []}}
    store.ingest_summary(summary)
    ack = store.ingest_heartbeat(heartbeat(NOW))
    assert ack["partitions"] == [{"group_ref": "group_a", "day": "2026-10-26", "revision": 1, "content_hash": first["content_hash"]}]
    assert ack["summaries"] == [{"group_ref": "group_a", "day": "2026-10-26", "revision": 1, "input_hash": first["content_hash"]}]
    assert "text" not in json.dumps(ack) and "group_other" not in json.dumps(ack)
    store.ingest_partition(partition([message(text="changed")], revision=2))
    assert store.ingest_heartbeat(heartbeat(NOW))["summaries"] == []


def test_exact_valid_summary_replay_clears_stale_after_input_returns(store):
    first = partition()
    store.ingest_partition(first)
    summary = {"source_id": "source_a", "group_ref": "group_a", "day": "2026-10-26", "revision": 1, "input_hash": first["content_hash"], "model": "test", "generated_at": NOW, "covered_until": NOW, "summary": {"topics": [{"text": "topic", "message_refs": ["msg_a"]}], "decisions": [], "todos": []}}
    store.ingest_summary(summary)
    store.ingest_partition(partition([message(text="temporary correction")], revision=2))
    assert store.summaries("group_a")[0]["stale"]
    first["revision"] = 3
    store.ingest_partition(first)
    assert store.ingest_summary(summary)["applied"]
    assert not store.summaries("group_a")[0]["stale"]
    assert store.ingest_heartbeat(heartbeat(NOW))["summaries"]


def test_group_details_preserve_coverage_gaps_and_current_source_freshness(store):
    value = partition()
    value["data"]["coverage"].update(source_available=False, gaps=["source_unavailable"])
    value["content_hash"] = content_hash(value["data"])
    store.ingest_partition(value)
    stats = store.group_stats("group_a")
    assert stats["coverage"]["local_backfill_complete"]
    assert stats["coverage"]["source_available"] is False
    assert stats["coverage"]["gaps"] == ["source_unavailable"]
    assert stats["source"] is None
    store.ingest_heartbeat(heartbeat(NOW, sync={"state": "error", "pending_partitions": 4, "error": "partition_upload_failed", "last_success_at": NOW - 100}))
    source = store.group_stats("group_a")["source"]
    assert source["source_id"] == "source_a"
    assert not source["stale"] and source["received_at"] == NOW
    assert source["sync"]["state"] == "error" and source["sync"]["pending_partitions"] == 4
    store.test_clock[0] += 91
    assert store.group_stats("group_a")["source"]["stale"]
