"""Cross-component contracts using real archive/cloud stores, without sockets."""
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import httpx
from fastapi.testclient import TestClient
import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.dashboard.api import create_app
from chatlog_keeper.dashboard.sync import SyncService

TOKEN = "machine-token-longer-than-thirty-two-characters"
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


def test_projection_heartbeat_and_sync_do_not_create_summary_state(pipeline):
    service, cloud, store, group, message = pipeline
    service.scan()
    serialized = json.dumps(service.state.partitions()[0], ensure_ascii=False)
    assert "private-account" not in serialized and "private-sender" not in serialized
    assert "private-payload" not in serialized and "origins" not in serialized
    assert service.flush() == 1
    assert store.conn.execute("SELECT message_ref FROM messages").fetchone()[0] == message
    ack = service.send_heartbeat()
    assert "summaries" not in ack
    saved = json.loads(store.conn.execute("SELECT payload FROM sources").fetchone()[0])
    assert "summary" not in saved
    assert not (service.state.directory / "summary.sqlite").exists()
