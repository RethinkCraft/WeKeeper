"""Side-path archive bridge. Synthetic HTTP only; no official database."""
import json
from datetime import datetime, timedelta, timezone
import sqlite3

import httpx
import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.bot.archive_bridge import (
    ArchiveBridge, BridgeError, SOURCE_PATH, SOURCE_IDENTITY, SOURCE_FINGERPRINT, TABLE_PREFIX, bot_lanes_hold,
)
from chatlog_keeper.bot.protocol_gateway import GatewayError, ProtocolGateway


ACCOUNT = "synthetic-account"
OWN = "synthetic-bot"
THREAD = "synthetic-group@chatroom"
ARCHIVE_ACCOUNT = "synthetic-archive-account"
FLOOR = datetime(2026, 1, 1, tzinfo=timezone.utc)


class Peer:
    def __init__(self):
        self.events = []
        self.messages = {}
        self.calls = []
        self.earliest = 0

    def add(self, cursor, received_id, *, created_at, server_id="10", disposition="ready", body="synthetic text"):
        self.events.append({"cursor": str(cursor), "kind": "message.received",
                            "data": {"receivedId": received_id, "firstReceivedSequence": str(cursor),
                                     "disposition": disposition},
                            "createdAt": created_at})
        self.messages[received_id] = {
            "receivedId": received_id, "accountId": ACCOUNT, "ownUsername": OWN,
            "firstReceivedSequence": str(cursor), "sourceSha256": "ab" * 32,
            "disposition": disposition, "quoteEligible": disposition == "ready",
            "threadId": THREAD, "senderId": "synthetic-sender", "serverId": server_id,
            "direction": "incoming", "type": 1, "body": body, "createdAt": created_at, "quote": None,
        }

    def handle(self, request):
        self.calls.append((request.method, request.url.path))
        assert request.method == "GET"
        assert "/consumers/" not in request.url.path
        assert not request.url.path.endswith("/ack")
        profile = "/messages/" in request.url.path and "/received/messages/" not in request.url.path
        assert not profile
        if request.url.path == "/v1/status":
            return httpx.Response(200, json={"serverId": "synthetic-server", "ownershipGeneration": "synthetic"})
        path = request.url.path.removeprefix("/v1/accounts/" + ACCOUNT)
        if path == "":
            return httpx.Response(200, json={"id": ACCOUNT, "ownUsername": OWN})
        if path == "/events":
            after = int(request.url.params["after"])
            if after < self.earliest:
                return httpx.Response(400, json={"error": {"code": "event_cursor_expired"}})
            items = [event for event in self.events if int(event["cursor"]) > after][: int(request.url.params["limit"])]
            nxt = items[-1]["cursor"] if items else str(after)
            return httpx.Response(200, json={"items": items, "nextCursor": nxt})
        if path.startswith("/received/messages/"):
            message = self.messages.get(path.removeprefix("/received/messages/"))
            if message is None:
                return httpx.Response(404, json={"error": {"code": "not_found"}})
            return httpx.Response(200, json=message)
        raise AssertionError(path)


def bridge(tmp_path, peer, *, floor=None, before_page=None):
    gateway = ProtocolGateway(token="synthetic-token", account_id=ACCOUNT,
                              journal_path=tmp_path / "journal.sqlite", base_url="http://127.0.0.1:6175",
                              transport=httpx.MockTransport(peer.handle))
    return gateway, ArchiveBridge(gateway=gateway, archive_dir=tmp_path, state_path=tmp_path / "bridge.sqlite",
                                  archive_account=ARCHIVE_ACCOUNT, floor=floor or FLOOR,
                                  before_page=before_page)


def test_pages_snapshots_without_registering_or_acking(tmp_path):
    peer = Peer()
    created = (FLOOR + timedelta(minutes=5)).isoformat()
    peer.add(11, "recv-new", created_at=created, server_id="11")
    peer.add(12, "recv-old", created_at=(FLOOR - timedelta(days=1)).isoformat(), server_id="12")
    peer.add(13, "recv-gone", created_at=created, server_id="13")
    del peer.messages["recv-gone"]
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        first = reader.poll_once()
        assert first["inserted"] == 1
        assert first["gaps"] == 1
        second = reader.poll_once()
        assert second["caught_up"] is True
        with ArchiveStore(tmp_path, readonly=True) as archive:
            rows = [tuple(row) for row in archive.conn.execute("SELECT server_id,text FROM messages")]
            assert rows == [("11", "synthetic text")]
            assert not archive.conn.execute("SELECT 1 FROM sqlite_master WHERE name='bot_events'").fetchone()
    assert not any(method != "GET" or "/consumers/" in path or path.endswith("/ack") for method, path in peer.calls)


def test_pruned_cursor_is_recorded_and_replay_does_not_duplicate(tmp_path):
    peer = Peer()
    peer.earliest = 10
    created = (FLOOR + timedelta(minutes=5)).isoformat()
    peer.add(11, "recv-new", created_at=created, server_id="11")
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        reader.poll_once()
        assert reader.gap_count() == 1
        reason = reader.conn.execute("SELECT reason,detail FROM gaps").fetchone()
        assert reason[0] == "events_pruned"
        assert json.loads(reason[1])["resumed_after"] == "10"
        reader.conn.execute("INSERT INTO staged(id,after_cursor,next_cursor,items_json) VALUES(1,'10','11',?)",
                            (json.dumps(peer.events),))
        reader.conn.commit()
        again = reader.poll_once()
        assert again["inserted"] == 0
        with ArchiveStore(tmp_path, readonly=True) as archive:
            assert archive.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 1


def test_same_server_id_keeps_one_message_ref(tmp_path):
    created = (FLOOR + timedelta(minutes=5)).isoformat()
    with ArchiveStore(tmp_path) as archive:
        source = archive.register_source(ARCHIVE_ACCOUNT, "existing", {"kind": "existing"}, "existing")
        archive.get_stream(source["source_ref"], source["generation"], "Msg", THREAD, 0)
        archive.commit_page(source["source_ref"], source["generation"], "Msg", THREAD, [{
            "row_id": 1, "server_id": "11", "create_time": int(FLOOR.timestamp()) + 300,
            "sender_id": "synthetic-sender", "real_sender_id": "synthetic-sender", "local_type": 1,
            "message_content": "synthetic text",
            "parsed": {"msg_type": 1, "text": "synthetic text", "parse_status": "parsed", "mentions": []},
        }], mode="backfill", checkpoint=1, complete=True)
        kept = archive.conn.execute("SELECT message_ref FROM messages").fetchone()[0]
    peer = Peer()
    peer.add(11, "recv-new", created_at=created, server_id="11", body="synthetic text")
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        reader.poll_once()
        with ArchiveStore(tmp_path, readonly=True) as archive:
            refs = [row[0] for row in archive.conn.execute("SELECT message_ref FROM messages")]
    assert refs == [kept]


def test_pause_when_bot_lanes_leave_ready(tmp_path):
    peer = Peer()
    gateway, reader = bridge(tmp_path, peer, before_page=lambda: False)
    with gateway, reader:
        with pytest.raises(Exception, match="bridge_paused"):
            reader.poll_once()
        assert reader.cursor() == "0"
    fresh = datetime.now(timezone.utc).isoformat()
    ready = {"updated_at": fresh, "lanes": {"receive": {"state": "ready"}, "send": {"state": "ready"}}}
    assert bot_lanes_hold({**ready, "account": {"authenticated": True}})
    assert bot_lanes_hold(ready)
    assert not bot_lanes_hold({**ready, "account": {"authenticated": False}})
    recent = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    blocked = {"updated_at": fresh, "account": {"authenticated": True},
               "lanes": {"receive": {"state": "ready", "last_error": "gateway_transport_unavailable",
                                     "last_failure_at": recent},
                         "send": {"state": "ready"}}}
    assert not bot_lanes_hold(blocked)


@pytest.mark.parametrize("version", [4, 5])
def test_bridge_uses_public_store_and_replays_committed_page_after_interruption(tmp_path, version, monkeypatch):
    with ArchiveStore(tmp_path) as archive:
        archive.conn.execute(f"PRAGMA user_version={version}")
    peer = Peer()
    peer.add(11, "recv-one", created_at=(FLOOR + timedelta(minutes=5)).isoformat(), server_id="11")
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        original = reader._write_items
        def crash_after_archive(items):
            original(items)
            raise RuntimeError("synthetic crash after durable archive commit")
        monkeypatch.setattr(reader, "_write_items", crash_after_archive)
        with pytest.raises(RuntimeError, match="synthetic crash"):
            reader.poll_once()
        assert reader.cursor() == "0"
        assert reader.conn.execute("SELECT COUNT(*) FROM staged").fetchone()[0] == 1
        assignments = [tuple(row) for row in reader.conn.execute("SELECT * FROM assignments")]
        with ArchiveStore(tmp_path, readonly=True) as archive:
            reference = archive.conn.execute("SELECT message_ref FROM messages").fetchone()[0]
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        assert reader.poll_once()["inserted"] == 0
        assert reader.cursor() == "11"
        assert [tuple(row) for row in reader.conn.execute("SELECT * FROM assignments")] == assignments
    with ArchiveStore(tmp_path, readonly=True) as archive:
        assert archive.get_status()["schema_version"] == version
        assert archive.conn.execute("SELECT message_ref FROM messages").fetchone()[0] == reference
        source = archive.conn.execute("SELECT account_id,relative_path,identity,generation FROM sources").fetchone()
        assert tuple(source) == (ARCHIVE_ACCOUNT, SOURCE_PATH, json.dumps(SOURCE_IDENTITY, separators=(",", ":"), sort_keys=True), 1)
        assert archive.conn.execute("SELECT table_name,backfill_rowid FROM streams").fetchone()[:] == (TABLE_PREFIX + THREAD, 1)
        assert not archive.conn.execute("SELECT 1 FROM sqlite_master WHERE name='bot_events'").fetchone()


def test_transport_failure_retains_page_and_cursor_until_snapshot_is_available(tmp_path, monkeypatch):
    peer = Peer()
    peer.add(11, "recv-one", created_at=(FLOOR + timedelta(minutes=5)).isoformat(), server_id="11")
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        original = gateway.get_received_message
        def unavailable(_received_id):
            raise GatewayError("gateway_transport_unavailable")
        monkeypatch.setattr(gateway, "get_received_message", unavailable)
        with pytest.raises(GatewayError, match="gateway_transport_unavailable"):
            reader.poll_once()
        assert reader.cursor() == "0"
        assert reader.conn.execute("SELECT COUNT(*) FROM staged").fetchone()[0] == 1
        calls_before = sum(path.endswith("/events") for _method, path in peer.calls)
        monkeypatch.setattr(gateway, "get_received_message", original)
        assert reader.poll_once()["inserted"] == 1
        assert sum(path.endswith("/events") for _method, path in peer.calls) == calls_before


def test_explicit_floor_is_persisted_and_cannot_change_existing_cursor(tmp_path):
    peer = Peer()
    gateway = object()
    settings = dict(gateway=gateway, archive_dir=tmp_path, state_path=tmp_path / "bridge.sqlite",
                    archive_account=ARCHIVE_ACCOUNT)
    with pytest.raises(BridgeError, match="catchup_floor_required"):
        ArchiveBridge(**settings)
    with ArchiveBridge(**settings, floor=FLOOR) as reader:
        reader.conn.execute("INSERT INTO meta VALUES('cursor','100')")
        reader.conn.commit()
    with ArchiveBridge(**settings) as resumed:
        assert resumed.floor == int(FLOOR.timestamp()) and resumed.cursor() == "100"
    with pytest.raises(BridgeError, match="catchup_floor_changed"):
        ArchiveBridge(**settings, floor=FLOOR + timedelta(days=1))
    with ArchiveBridge(**settings) as resumed:
        assert resumed.cursor() == "100"


def test_legacy_bridge_adds_explicit_floor_without_resetting_cursor_assignments_or_staged_page(tmp_path):
    settings = dict(gateway=object(), archive_dir=tmp_path, state_path=tmp_path / "bridge.sqlite",
                    archive_account=ARCHIVE_ACCOUNT)
    with ArchiveBridge(**settings, floor=FLOOR) as initial:
        initial.conn.execute("DELETE FROM meta WHERE key='catchup_floor'")
        initial.conn.execute("INSERT INTO meta VALUES('cursor','100')")
        initial.conn.execute("INSERT INTO assignments VALUES('existing-received',7)")
        initial.conn.execute("INSERT INTO staged VALUES(1,'100','101','[]')")
        initial.conn.commit()
    with pytest.raises(BridgeError, match="catchup_floor_required"):
        ArchiveBridge(**settings)
    with ArchiveBridge(**settings, floor=FLOOR) as upgraded:
        assert upgraded.cursor() == "100"
        assert upgraded.conn.execute("SELECT row_id FROM assignments").fetchone()[0] == 7
        assert tuple(upgraded.conn.execute("SELECT * FROM staged").fetchone()) == (1, "100", "101", "[]")


@pytest.mark.parametrize("version", [4, 5])
def test_sparse_overlap_keeps_names_verified_mentions_and_all_rich_semantics(tmp_path, version):
    created = FLOOR + timedelta(minutes=5)
    with ArchiveStore(tmp_path) as archive:
        source = archive.register_source(ARCHIVE_ACCOUNT, "native-cache", {"kind": "native"}, "native")
        archive.get_stream(source["source_ref"], 1, "Msg", THREAD, 0)
        parsed = {"msg_type": 49, "sub_type": 24, "text": "完整卡片正文", "parse_status": "parsed",
                  "mentions": ["known-member"], "mentions_status": "verified", "mention_all": False,
                  "attachment": {"kind": "file", "filename": "known.txt", "md5": "a" * 32},
                  "card": {"kind": "note", "title": "完整卡片标题"},
                  "card_components": [{"datatype": 1, "text": "保留卡片内容", "children": []}],
                  "forwarded_status": "parsed", "forwarded": [{"text": "保留转发内容", "sender_id": "known-forwarder", "children": []}]}
        archive.commit_page(source["source_ref"], 1, "Msg", THREAD, [{
            "row_id": 1, "server_id": "11", "create_time": int(created.timestamp()),
            "sender_id": "synthetic-sender", "real_sender_id": "synthetic-sender", "sender_name": "KnownSender",
            "local_type": 49, "message_content": "original rich payload", "parsed": parsed,
        }], mode="backfill", checkpoint=1, complete=True)
        archive.conn.execute(f"PRAGMA user_version={version}")
        protected = {table: [tuple(row) for row in archive.conn.execute(f"SELECT * FROM {table}")]
                     for table in ("messages", "forwarded", "card_components", "media", "p2_text_search")}
    peer = Peer()
    peer.add(11, "recv-overlap", created_at=created.isoformat(), server_id="11", body="稀疏展示文本")
    peer.messages["recv-overlap"]["type"] = 49
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        assert reader.poll_once()["gaps"] == 0
        reader.conn.execute("INSERT INTO staged VALUES(1,'0','11',?)", (json.dumps(peer.events),))
        reader.conn.commit()
        assert reader.poll_once()["inserted"] == 0
    with ArchiveStore(tmp_path, readonly=True) as archive:
        assert archive.get_status()["schema_version"] == version
        for table, before in protected.items():
            assert [tuple(row) for row in archive.conn.execute(f"SELECT * FROM {table}")] == before
        assert archive.conn.execute("SELECT COUNT(*) FROM origins").fetchone()[0] == 2


def test_plain_text_correction_is_quarantined_instead_of_overwriting_archived_evidence(tmp_path):
    created = FLOOR + timedelta(minutes=5)
    with ArchiveStore(tmp_path) as archive:
        source = archive.register_source(ARCHIVE_ACCOUNT, "native-cache", {}, "native")
        archive.get_stream(source["source_ref"], 1, "Msg", THREAD, 0)
        archive.commit_page(source["source_ref"], 1, "Msg", THREAD, [{
            "row_id": 1, "server_id": "11", "create_time": int(created.timestamp()),
            "sender_id": "synthetic-sender", "local_type": 1, "message_content": "已归档原文",
            "parsed": {"msg_type": 1, "text": "已归档原文", "parse_status": "parsed"},
        }], mode="backfill", checkpoint=1, complete=True)
    peer = Peer()
    peer.add(11, "recv-conflict", created_at=created.isoformat(), server_id="11", body="另一份修订正文")
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        result = reader.poll_once()
        assert result["gaps"] == 1 and reader.cursor() == "11"
        gap = reader.conn.execute("SELECT reason,detail FROM gaps").fetchone()
        assert gap[0] == "snapshot_conflict"
        assert json.loads(gap[1])["snapshot"]["message_content"] == "另一份修订正文"
    with ArchiveStore(tmp_path, readonly=True) as archive:
        assert archive.conn.execute("SELECT text FROM messages").fetchone()[0] == "已归档原文"
        assert archive.conn.execute("SELECT COUNT(*) FROM origins").fetchone()[0] == 1
        assert archive.conn.execute("SELECT backfill_rowid FROM streams WHERE table_name=?", (TABLE_PREFIX + THREAD,)).fetchone()[0] == 1


def test_explicit_revoke_is_archived_as_separate_evidence_without_restoring_target(tmp_path):
    peer = Peer()
    created = (FLOOR + timedelta(minutes=5)).isoformat()
    peer.add(11, "original", created_at=created, server_id="11", body="原消息")
    revoke = f"<sysmsg type='revokemsg'><revokemsg><session>{THREAD}</session><newmsgid>11</newmsgid><replacemsg>消息已撤回</replacemsg></revokemsg></sysmsg>"
    peer.add(12, "revoke", created_at=created, server_id="12", disposition="system", body=revoke)
    peer.messages["revoke"]["type"] = 10000
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        result = reader.poll_once()
        assert result["inserted"] == 2 and result["gaps"] == 0
    with ArchiveStore(tmp_path, readonly=True) as archive:
        original = archive.conn.execute("SELECT message_ref,text FROM messages WHERE server_id='11'").fetchone()
        event = json.loads(archive.conn.execute("SELECT event FROM messages WHERE server_id='12'").fetchone()[0])
        assert original["text"] == "原消息"
        assert event["event_type"] == "message_revoke" and event["target_server_id"] == "11"


def test_projection_fills_missing_plain_text_without_replacing_existing_semantics(tmp_path):
    created = FLOOR + timedelta(minutes=5)
    with ArchiveStore(tmp_path) as archive:
        source = archive.register_source(ARCHIVE_ACCOUNT, "native-cache", {}, "native")
        archive.get_stream(source["source_ref"], 1, "Msg", THREAD, 0)
        archive.commit_page(source["source_ref"], 1, "Msg", THREAD, [{
            "row_id": 1, "server_id": "11", "create_time": int(created.timestamp()),
            "sender_id": "synthetic-sender", "local_type": 1, "message_content": "",
            "parsed": {"msg_type": 1, "text": "", "parse_status": "unparsed", "mentions": ["known"], "mentions_status": "verified"},
        }], mode="backfill", checkpoint=1, complete=True)
    peer = Peer()
    peer.add(11, "fill", created_at=created.isoformat(), server_id="11", body="补充缺失正文")
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        assert reader.poll_once()["gaps"] == 0
    with ArchiveStore(tmp_path, readonly=True) as archive:
        row = archive.conn.execute("SELECT text,mentions,semantic FROM messages").fetchone()
        assert row["text"] == "补充缺失正文" and json.loads(row["mentions"]) == ["known"]
        assert json.loads(row["semantic"])["mentions_status"] == "verified"


def test_conflicting_same_server_id_in_one_page_is_also_quarantined(tmp_path):
    peer = Peer()
    created = (FLOOR + timedelta(minutes=5)).isoformat()
    peer.add(11, "first", created_at=created, server_id="11", body="原消息")
    peer.add(12, "conflicting", created_at=created, server_id="11", body="不同正文")
    gateway, reader = bridge(tmp_path, peer)
    with gateway, reader:
        result = reader.poll_once()
        assert result["inserted"] == 1 and result["gaps"] == 1 and reader.cursor() == "12"
    with ArchiveStore(tmp_path, readonly=True) as archive:
        assert archive.conn.execute("SELECT text FROM messages").fetchone()[0] == "原消息"
        assert archive.conn.execute("SELECT COUNT(*) FROM origins").fetchone()[0] == 1
