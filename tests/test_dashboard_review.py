"""Independent regression cases from the dashboard integration review."""
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import httpx
import pytest

from chatlog_keeper.dashboard.local_state import LocalState
from chatlog_keeper.dashboard.store import Conflict, DashboardStore
from chatlog_keeper.dashboard.summaries import SummaryWorker
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


def test_summary_scheduler_respects_current_allowlist(tmp_path, monkeypatch):
    from chatlog_keeper.dashboard.cli import main
    import chatlog_keeper.dashboard.summaries as summaries_module
    state_dir = tmp_path / "state"
    state = LocalState(state_dir)
    state.queue(data(day=datetime.now(ZONE).date().isoformat()))
    state.close()
    (state_dir / "config.json").write_text(json.dumps(config(["group_retained"])))
    captured = []

    class Worker:
        def __init__(self, *args, **kwargs):
            pass
        def run_once(self, partitions, **kwargs):
            captured.extend(partitions)
            return {"state": "ready"}
        def close(self):
            pass

    monkeypatch.setattr(summaries_module, "SummaryWorker", Worker)
    assert main(["--archive-state", str(tmp_path / "archive"), "--local-state", str(state_dir), "summarize", "--once"]) == 0
    assert captured == [], "A no-longer-allowed group was still submitted for model analysis"


class Model:
    def status(self):
        return {"state": "ready", "model": "test-model"}
    def summarize(self, messages, **kwargs):
        return {"topics": [{"text": "source-based", "message_refs": [messages[0]["message_ref"]]}], "decisions": [], "todos": []}


def test_summary_waits_for_its_unuploaded_source_partition(tmp_path):
    """Missing cloud input is not evidence that the local summary is obsolete."""
    cloud = DashboardStore(tmp_path / "cloud")
    def transport(request):
        body = json.loads(request.content)
        try:
            result = cloud.ingest_summary(body)
            return httpx.Response(200, json=result)
        except Conflict as exc:
            return httpx.Response(409, json={"detail": str(exc)})
    client = httpx.Client(transport=httpx.MockTransport(transport))
    service = SyncService(tmp_path / "archive", tmp_path / "state", config(["group_removed"]), client=client)
    worker = SummaryWorker(service.state.directory, service.state.source_id, client=Model())
    try:
        service.state.queue(data())
        worker.run_once(service.state.partitions())
        assert worker.pending()
        service.flush_summaries()
        assert worker.pending(), "A valid summary was permanently ACKed before its input reached the cloud"
    finally:
        worker.close()
        service.close()
        client.close()
        cloud.close()


def heartbeat(now, state):
    return {"source_id": "source_a", "health": {"state": state, "reason": "test"},
            "collector": {"running": True, "last_cycle_at": now, "last_error": None},
            "last_message_at": now - 1000, "summary": {"state": "not_configured"}, "sent_at": now}


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


def test_default_summary_scheduler_finishes_yesterday_after_midnight(tmp_path, monkeypatch):
    import chatlog_keeper.dashboard.cli as cli
    import chatlog_keeper.dashboard.summaries as summaries_module
    state_dir = tmp_path / "state"
    state = LocalState(state_dir)
    previous = data()
    previous["messages"][0]["create_time"] = int(datetime.fromisoformat("2026-10-26T23:59:00+08:00").timestamp())
    state.queue(previous)
    state.close()
    (state_dir / "config.json").write_text(json.dumps(config(["group_removed"])))
    captured = []

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = datetime(2026, 10, 27, 0, 10, tzinfo=ZONE)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)

    class Worker:
        def __init__(self, *args, **kwargs):
            pass
        def run_once(self, partitions, **kwargs):
            captured.extend(partitions)
            return {"state": "ready"}
        def close(self):
            pass

    monkeypatch.setattr(cli, "datetime", Clock)
    monkeypatch.setattr(summaries_module, "SummaryWorker", Worker)
    assert cli.main(["--archive-state", str(tmp_path / "archive"), "--local-state", str(state_dir), "summarize", "--once"]) == 0
    assert [p["day"] for p in captured] == ["2026-10-26"], "The final minutes of yesterday never receive another summary run"


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
