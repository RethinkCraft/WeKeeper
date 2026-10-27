"""Cross-component contracts using real archive/cloud stores, without sockets."""
from datetime import date, datetime, timedelta
import json
from zoneinfo import ZoneInfo

import httpx
from fastapi.testclient import TestClient
import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.dashboard.api import create_app
from chatlog_keeper.dashboard.local_state import LocalState
from chatlog_keeper.dashboard.projection import projections
from chatlog_keeper.dashboard.protocol import Summary
from chatlog_keeper.dashboard.summaries import SummaryWorker
from chatlog_keeper.dashboard.sync import SyncService

TOKEN = "machine-token-longer-than-thirty-two-characters"
DAY = "2026-10-26"
STAMP = int(datetime(2026, 10, 26, 10, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())


def archive(directory):
    with ArchiveStore(directory) as store:
        source = store.register_source("private-account", "db_storage/message/message_0.db", {"inode": 1, "salt": "secret"}, "fp")
        store.upsert_groups("private-account", [{"id": "test@chatroom", "name": "Test group"}])
        store.get_stream(source["source_ref"], source["generation"], "Msg_test", "test@chatroom", 1)
        store.commit_page(source["source_ref"], source["generation"], "Msg_test", "test@chatroom", [
            {"row_id": 1, "server_id": "one", "create_time": STAMP, "local_type": 1,
             "sender_id": "private-sender", "sender_name": "Sender", "message_content": "讨论安排",
             "packed_info_data": b"private-payload", "parsed": {"msg_type": 1, "text": "讨论安排", "parse_status": "parsed"}}],
             mode="backfill", checkpoint=1, complete=True)
        group = store.conn.execute("SELECT group_ref FROM groups").fetchone()[0]
        message = store.conn.execute("SELECT message_ref FROM messages").fetchone()[0]
    return group, message


class Model:
    model = "test-model"

    def status(self):
        return {"state": "ready", "model": self.model}

    def summarize(self, messages, **kwargs):
        return {"topics": [{"text": "安排讨论", "message_refs": [messages[0]["message_ref"]]}], "decisions": [], "todos": []}


@pytest.fixture
def pipeline(tmp_path):
    group, message = archive(tmp_path / "archive")
    application = create_app(tmp_path / "cloud", TOKEN)
    with TestClient(application, base_url="https://cloud.invalid") as cloud:
        def forward(request):
            response = cloud.request(request.method, request.url.path, content=request.content,
                                     headers=dict(request.headers))
            return httpx.Response(response.status_code, content=response.content, headers=dict(response.headers))
        http = httpx.Client(transport=httpx.MockTransport(forward))
        service = SyncService(tmp_path / "archive", tmp_path / "local", {"server_url": "https://cloud.invalid", "groups": [group], "machine_token": TOKEN}, client=http)
        try:
            yield service, cloud, application.state.store, group, message
        finally:
            service.close()
            http.close()


def test_actual_projection_sync_summary_and_model_change_contract(pipeline):
    service, cloud, store, group, message = pipeline
    service.scan()
    outbound = service.state.partitions()[0]
    serialized = json.dumps(outbound, ensure_ascii=False)
    assert "private-account" not in serialized and "private-sender" not in serialized
    assert "private-payload" not in serialized and "origins" not in serialized
    assert service.flush() == 1
    model = Model()
    worker = SummaryWorker(service.state.directory, service.state.source_id, client=model)
    try:
        assert worker.run_once(service.state.partitions())["state"] == "ready"
        first = worker.pending()[0]
        Summary.model_validate(first)
        service.flush_summaries()
        assert not worker.pending()
        assert store.summaries(group)[0]["summary"]["topics"][0]["message_refs"] == [message]
        model.model = "another-valid-model"
        worker.run_once(service.state.partitions())
        second = worker.pending()[0]
        assert second["revision"] > first["revision"]
        assert second["input_hash"] == first["input_hash"]
        service.flush_summaries()
        assert not worker.pending()
        assert store.summaries(group)[0]["model"] == "another-valid-model"
    finally:
        worker.close()


def test_lost_partition_ack_can_replay_after_successful_cloud_commit(pipeline):
    service, cloud, store, group, _ = pipeline
    service.scan()
    normal_post = service.post
    lost = [True]

    def flaky(route, payload):
        response = normal_post(route, payload)
        if route.endswith("/partitions") and lost[0]:
            lost[0] = False
            raise httpx.ReadTimeout("simulated lost ACK")
        return response
    service.post = flaky
    with pytest.raises(httpx.ReadTimeout):
        service.flush()
    assert len(service.state.partitions(pending_only=True)) == 1
    assert store.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 1
    assert service.flush() == 1
    assert not service.state.partitions(pending_only=True)
    assert store.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 1


def test_heartbeat_converts_structured_local_error_to_wire_code(pipeline):
    service, cloud, store, _, _ = pipeline
    worker = SummaryWorker(service.state.directory, service.state.source_id, client=Model())
    try:
        worker._set_state(state="paused", error={"code": "subscription_sharing_usage_limit_exceeded", "http_status": 429})
        response = service.send_heartbeat()
        assert response.get("received_at") or response.get("accepted") or response.get("applied")
        saved = json.loads(store.conn.execute("SELECT payload FROM sources").fetchone()[0])
        assert saved["summary"]["error"] == "subscription_sharing_usage_limit_exceeded"
    finally:
        worker.close()


def test_stale_summary_is_removed_after_source_partition_changed(pipeline):
    service, _, store, _, _ = pipeline
    service.scan()
    service.flush()
    worker = SummaryWorker(service.state.directory, service.state.source_id, client=Model())
    try:
        worker.run_once(service.state.partitions())
        current = service.state.partitions()[0]
        current["data"]["messages"][0]["text"] = "更新后的原文"
        service.state.queue(current["data"])
        service.flush()
        assert worker.pending()
        service.flush_summaries()
        assert not worker.pending()  # Must be regenerated against the newer partition.
    finally:
        worker.close()


def test_revision_conflict_is_not_mistaken_for_stale_input(pipeline):
    service, _, _, _, _ = pipeline
    service.scan()
    service.flush()  # A summary is eligible only after its evidence was ACKed.
    worker = SummaryWorker(service.state.directory, service.state.source_id, client=Model())
    try:
        worker.run_once(service.state.partitions())

        def conflict(route, payload):
            request = httpx.Request("POST", "https://cloud.invalid" + route)
            response = httpx.Response(409, json={"detail": "summary_revision_conflict"}, request=request)
            response.raise_for_status()
        service.post = conflict
        with pytest.raises(httpx.HTTPStatusError):
            service.flush_summaries()
        assert len(worker.pending()) == 1
    finally:
        worker.close()


def test_removed_group_is_not_uploaded_or_summarized_from_old_outbox(pipeline):
    service, _, _, _, _ = pipeline
    service.scan()
    worker = SummaryWorker(service.state.directory, service.state.source_id, client=Model())
    try:
        worker.run_once(service.state.partitions())
        assert worker.pending()
        # Simulate an operator reducing the allowlist before queued data was sent.
        service.config["groups"] = ["group_still_allowed"]
        called = []
        service.post = lambda route, payload: called.append((route, payload))
        assert service.flush() == 0
        service.flush_summaries()
        assert not called
        assert service.state.partitions(pending_only=True)
        assert worker.pending()
    finally:
        worker.close()


def test_more_than_one_upload_batch_keeps_summaries_until_evidence_ack(pipeline):
    service, _, store, group, _ = pipeline
    first = date(2026, 7, 1)
    for index in range(101):
        day = first + timedelta(days=index)
        timestamp = int(datetime.combine(day, datetime.min.time(), tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()) + 36000
        service.state.queue({"group": {"group_ref": group, "name": "Test group"}, "day": day.isoformat(),
                             "messages": [{"message_ref": f"msg_day_{index}", "create_time": timestamp,
                                           "sender_id": "sender", "sender_name": "Sender", "msg_type": 1,
                                           "text": "讨论安排", "reply": None}],
                             "coverage": {"local_backfill_complete": True}}, initial=True)
    worker = SummaryWorker(service.state.directory, service.state.source_id, client=Model())
    try:
        for _ in range(20):
            result = worker.run_once(service.state.partitions())
            if result["state"] == "ready":
                break
        assert result["state"] == "ready"
        assert len(worker.pending(limit=500)) == 101
        service.flush_summaries()
        assert len(worker.pending(limit=500)) == 101
        assert store.conn.execute("SELECT COUNT(*) FROM summaries").fetchone()[0] == 0
        assert service.flush(limit=100) == 100
        service.flush_summaries()
        assert len(worker.pending(limit=500)) == 1
        assert store.conn.execute("SELECT COUNT(*) FROM summaries").fetchone()[0] == 100
        assert service.flush(limit=100) == 1
        service.flush_summaries()
        assert not worker.pending()
        assert store.conn.execute("SELECT COUNT(*) FROM summaries").fetchone()[0] == 101
    finally:
        worker.close()


def test_cloud_restore_requeues_missing_evidence_without_losing_pending_summary(pipeline):
    service, _, store, group, _ = pipeline
    service.scan()
    service.flush()
    worker = SummaryWorker(service.state.directory, service.state.source_id, client=Model())
    try:
        worker.run_once(service.state.partitions())
        expected = worker.pending()[0]
        assert not service.state.partitions(pending_only=True)
        # Simulate restoring a cloud backup from before this partition arrived.
        with store.transaction():
            store.conn.execute("DELETE FROM partitions")
            store.conn.execute("DELETE FROM messages")
        service.flush_summaries()
        assert worker.pending() == [expected]
        assert len(service.state.partitions(pending_only=True)) == 1
        service.flush()
        service.flush_summaries()
        assert not worker.pending()
        assert store.summaries(group)[0]["input_hash"] == expected["input_hash"]
    finally:
        worker.close()
