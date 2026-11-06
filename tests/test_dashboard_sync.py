from datetime import datetime
import json
from zoneinfo import ZoneInfo

import httpx
import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.dashboard.local_state import LocalState, content_hash
from chatlog_keeper.dashboard.projection import projections
from chatlog_keeper.dashboard.sync import SyncService, read_config


def archive_fixture(path):
    store = ArchiveStore(path)
    store.upsert_groups("private_account", [{"id": "private_group@chatroom", "name": "Example"}, {"id": "other@chatroom", "name": "Other"}])
    source = store.register_source("private_account", "db_storage/private.db", {"inode": 1}, "fp")
    store.get_stream(source["source_ref"], 1, "Msg_test", "private_group@chatroom", 3)
    stamp = int(datetime(2026, 10, 26, 12, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())
    for i in (1, 2):
        row = {"row_id": i, "server_id": f"s{i}", "create_time": stamp,
               "local_type": 1, "sender_id": "wxid_private", "sender_name": "Example sender",
               "message_content": f"text{i}", "packed_info_data": None,
               "parsed": {"msg_type": 1, "text": f"text{i}", "parse_status": "parsed"}}
        store.commit_page(source["source_ref"], 1, "Msg_test", "private_group@chatroom", [row], mode="live", checkpoint=i, complete=True)
    ref = store.conn.execute("SELECT group_ref FROM groups WHERE group_id='private_group@chatroom'").fetchone()[0]
    return store, ref


def test_projection_is_scoped_text_only_and_stable(tmp_path):
    archive, ref = archive_fixture(tmp_path / "archive-state")
    try:
        first = projections(tmp_path / "archive-state", [ref], "ab" * 32)
        assert len(first) == 1
        assert len(first[0]["messages"]) == 2
        assert first[0]["messages"][0]["sender_id"] == first[0]["messages"][1]["sender_id"]
        assert first[0]["messages"][0]["sender_id"].startswith("sender_")
        encoded = json.dumps(first)
        assert "wxid_private" not in encoded and "private_account" not in encoded
        assert "private.db" not in encoded and "private_group@chatroom" not in encoded
        assert set(first[0]["messages"][0]) == {"message_ref", "create_time", "sender_id", "sender_name", "msg_type", "text", "reply"}
        archive.conn.execute("UPDATE messages SET updated_at=updated_at+99")
        archive.conn.commit()
        assert first == projections(tmp_path / "archive-state", [ref], "ab" * 32)
        with pytest.raises(ValueError, match="allowlist"):
            projections(tmp_path / "archive-state", [], "ab" * 32)
    finally:
        archive.close()


def test_durable_queue_coalesces_and_late_ack_preserves_newer_content(tmp_path):
    state = LocalState(tmp_path)
    data = {"group": {"group_ref": "group_test", "name": "Test"}, "day": "2026-10-26", "messages": [], "coverage": {}}
    assert state.queue(data, initial=True)
    old = state.partitions(pending_only=True)[0]
    source = state.source_id
    assert not state.queue(data)
    state.close()
    state = LocalState(tmp_path)
    assert state.source_id == source
    changed = dict(data, messages=[{"text": "corrected"}])
    assert state.queue(changed)
    newer = state.partitions(pending_only=True)[0]
    assert newer["initial"] and newer["revision"] == 2
    state.acknowledge("group_test", "2026-10-26", old["revision"])
    assert state.partitions(pending_only=True)
    state.acknowledge("group_test", "2026-10-26", newer["revision"])
    assert not state.partitions(pending_only=True)
    state.close()


def test_lost_ack_replays_same_partition_and_old_deletion_is_exported(tmp_path):
    archive, ref = archive_fixture(tmp_path / "archive")
    calls = []
    def transport(request):
        body = json.loads(request.content)
        calls.append(body)
        if len(calls) == 1:
            raise httpx.ReadTimeout("lost ACK")
        return httpx.Response(200, json={"applied": False, "revision": body["revision"]})
    client = httpx.Client(transport=httpx.MockTransport(transport))
    service = SyncService(tmp_path / "archive", tmp_path / "local", {"groups": [ref], "server_url": "https://192.0.2.1", "machine_token": "a" * 40}, client=client)
    try:
        service.scan()
        with pytest.raises(httpx.ReadTimeout):
            service.flush()
        assert len(service.state.partitions(pending_only=True)) == 1
        service.flush()
        assert calls[0] == calls[1]
        assert not service.state.partitions(pending_only=True)
        archive.conn.execute("DELETE FROM messages")
        archive.conn.commit()
        service.scan()
        pending = service.state.partitions(pending_only=True)
        assert pending[0]["data"]["messages"] == []
        assert pending[0]["revision"] == 2
        assert not pending[0]["initial"]
    finally:
        service.close()
        archive.close()
        client.close()


@pytest.mark.parametrize("url", ["http://example.com", "https://user:secret@example.com", "https://example.com?token=secret", "https://example.com#secret"])
def test_sync_refuses_insecure_or_credential_urls(tmp_path, url):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"server_url": url, "groups": ["group_test"], "machine_token": "a" * 40}))
    with pytest.raises(ValueError):
        read_config(path)
