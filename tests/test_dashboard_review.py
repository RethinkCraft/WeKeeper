"""Independent regression cases from the dashboard integration review."""
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import httpx
import pytest

from chatlog_keeper.dashboard.local_state import LocalState
from chatlog_keeper.dashboard.store import Conflict, DashboardStore
from chatlog_keeper.dashboard.sync import SyncService

ZONE = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 10, 26, 12, tzinfo=ZONE).timestamp()
TOKEN = "a" * 40


def data(group="group_removed", day="2026-10-26"):
    return {"group": {"group_ref": group, "name": group}, "day": day,
            "messages": [{"message_ref": "msg_evidence", "create_time": int(datetime.fromisoformat(day + "T10:00:00+08:00").timestamp()), "sender_id": "sender_opaque", "sender_name": "A", "msg_type": 1, "text": "private old group content", "reply": None}],
            "coverage": {"local_backfill_complete": True}}


def config(groups):
    return {"server_url": "https://cloud.invalid", "machine_token": TOKEN, "groups": groups}


def test_removed_group_is_not_sent_from_preexisting_queue(tmp_path):
    """Revoking the allowlist while offline must apply to already queued plaintext."""
    sent = []
    def transport(request):
        body = json.loads(request.content)
        sent.append(body["group_ref"])
        return httpx.Response(200, json={"applied": True, "revision": body["revision"]})
    client = httpx.Client(transport=httpx.MockTransport(transport))
    service = SyncService(tmp_path / "archive", tmp_path / "state", config(["group_retained"]), client=client)
    try:
        service.state.queue(data())
        service.flush()
        assert sent == [], "A group removed from the current allowlist was still uploaded"
    finally:
        service.close()
        client.close()


def heartbeat(now, state):
    return {"source_id": "source_a", "health": {"state": state, "reason": "test"},
            "collector": {"running": True, "last_cycle_at": now, "last_error": None},
            "last_message_at": now - 1000, "sent_at": now}


@pytest.mark.parametrize("fault,kind", [("network_unavailable", "network"), ("source_unavailable", "source")])
def test_unknown_does_not_resolve_known_runtime_fault(tmp_path, fault, kind):
    clock = [NOW]
    cloud = DashboardStore(tmp_path, clock=lambda: clock[0])
    try:
        for _ in range(2):
            cloud.ingest_heartbeat(heartbeat(clock[0], fault))
            clock[0] += 30
        assert next(a for a in cloud.alerts(1) if a["kind"] == kind)["resolved_at"] is None
        for _ in range(3):
            cloud.ingest_heartbeat(heartbeat(clock[0], "unknown"))
            clock[0] += 30
        assert next(a for a in cloud.alerts(1) if a["kind"] == kind)["resolved_at"] is None, "Unknown probe state was incorrectly treated as verified recovery"
    finally:
        cloud.close()


@pytest.mark.parametrize("fault,kind", [("network_unavailable", "network"), ("source_unavailable", "source"), ("login_required", "wechat_session")])
def test_verified_idle_session_can_recover_fault_without_new_messages(tmp_path, fault, kind):
    clock = [NOW]
    cloud = DashboardStore(tmp_path, clock=lambda: clock[0])
    try:
        for _ in range(2):
            cloud.ingest_heartbeat(heartbeat(clock[0], fault))
            clock[0] += 30
        assert next(a for a in cloud.alerts(1) if a["kind"] == kind)["resolved_at"] is None
        for _ in range(2):
            cloud.ingest_heartbeat(heartbeat(clock[0], "sync_unverified"))
            clock[0] += 30
        assert next(a for a in cloud.alerts(1) if a["kind"] == kind)["resolved_at"] is not None
        assert json.loads(cloud.conn.execute("SELECT payload FROM sources").fetchone()[0])["health"]["state"] == "sync_unverified"
    finally:
        cloud.close()


def test_cloud_restore_reconciles_locally_acknowledged_partitions(tmp_path, monkeypatch):
    """A cloud database restored before an ACKed upload must recover quiet history."""
    import chatlog_keeper.dashboard.sync as sync_module
    cloud = DashboardStore(tmp_path / "cloud")
    payload_data = data()
    def transport(request):
        body = json.loads(request.content)
        if request.url.path.endswith("/heartbeat"):
            return httpx.Response(200, json=cloud.ingest_heartbeat(body))
        return httpx.Response(200, json=cloud.ingest_partition(body))
    client = httpx.Client(transport=httpx.MockTransport(transport))
    service = SyncService(tmp_path / "archive", tmp_path / "state", config(["group_removed"]), client=client)
    try:
        service.state.queue(payload_data)
        queued = service.state.partitions()[0]
        service.state.acknowledge(queued["group_ref"], queued["day"], queued["revision"])
        monkeypatch.setattr(sync_module, "projections", lambda *args: [payload_data])
        # Simulate restoring cloud data before this accepted partition existed.
        assert cloud.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
        service.send_heartbeat()
        service.scan()
        service.flush()
        assert cloud.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 1, "Unchanged ACKed history remained permanently absent after cloud restore"
    finally:
        service.close()
        client.close()
        cloud.close()
