"""Public schema-4/5 compatibility, without a private historical checkout."""
import io
import json
import sqlite3

from PIL import Image
import pytest

from chatlog_keeper.archive import ArchiveError, ArchiveStore
from chatlog_keeper.dashboard.avatars import avatar_projections
from chatlog_keeper.dashboard.projection import projections
from test_archive import commit, raw, setup_source
from test_p2_archive import blob


@pytest.mark.parametrize("version", [4, 5])
def test_existing_versions_keep_search_projection_avatars_and_checkpoints_on_restart(tmp_path, version):
    with ArchiveStore(tmp_path) as seed:
        source = setup_source(seed)
        reference = commit(seed, source, [raw(text="兼容归档检索")])["message_refs"][0]
        image = io.BytesIO()
        Image.new("RGB", (4, 5), "blue").save(image, format="PNG")
        asset = blob(seed, image.getvalue())
        seed.upsert_profiles("account", [{"subject_id": "wxid_sender", "profile": {"nickname": "Sender"},
            "avatar": {"relative_path": asset["raw_path"], "sha256": asset["raw_sha256"], "mime": "image/png"}}])
        group = seed.conn.execute("SELECT group_ref FROM groups").fetchone()[0]
        seed.conn.execute(f"PRAGMA user_version={version}")
    for readonly in (True, False, True):
        with ArchiveStore(tmp_path, readonly=readonly) as archive:
            assert archive.get_status()["schema_version"] == version
            assert archive.conn.execute("PRAGMA user_version").fetchone()[0] == version
            assert archive.search_messages([group], 0, 100, "归档")["messages"][0]["message_ref"] == reference
            assert archive.get_profile(group, "wxid_sender", True)["avatar"]["status"] == "archived"
            if not readonly:
                commit(archive, source, [raw(2, "new", text="新增兼容文字", timestamp=20)], mode="live")
            assert archive.conn.execute("SELECT backfill_rowid FROM streams").fetchone()[0] == 1
            assert not archive.conn.execute("SELECT 1 FROM sqlite_master WHERE name='bot_events'").fetchone()
    exported = projections(tmp_path, [group], "ab" * 32)
    assert [message["text"] for message in exported[0]["messages"]] == ["兼容归档检索", "新增兼容文字"]
    avatars = avatar_projections(tmp_path, [group], "ab" * 32, "synthetic-source")
    assert len(avatars) == 1 and avatars[0]["mime_type"] == "image/jpeg"
    assert not (tmp_path / "archive/backups").exists()


def test_new_archive_defaults_to_five_without_retired_bot_queue(tmp_path):
    with ArchiveStore(tmp_path) as archive:
        assert archive.get_status()["schema_version"] == 5
        assert not archive.conn.execute("SELECT 1 FROM sqlite_master WHERE name='bot_events'").fetchone()


@pytest.mark.parametrize("readonly", [False, True])
def test_unknown_archive_version_is_rejected_without_rewriting_database(tmp_path, readonly):
    with ArchiveStore(tmp_path) as seed:
        seed.conn.execute("PRAGMA user_version=99")
    path = tmp_path / "archive/archive.sqlite"
    before = path.read_bytes()
    with pytest.raises(ArchiveError, match="unsupported_archive_version"):
        ArchiveStore(tmp_path, readonly=readonly)
    assert path.read_bytes() == before
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 99


def test_retired_bot_history_is_unchanged_by_live_inserts_and_message_merges(tmp_path):
    with ArchiveStore(tmp_path) as archive:
        source = setup_source(archive)
        provisional = commit(archive, source, [raw(server_id="")])["message_refs"][0]
        other = setup_source(archive, "db_storage/message/message_1.db")
        canonical = commit(archive, other, [raw(server_id="ack")])["message_refs"][0]
        archive.conn.execute("""CREATE TABLE bot_events(
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,message_ref TEXT NOT NULL UNIQUE,
            source_ref TEXT NOT NULL,generation INTEGER NOT NULL,table_name TEXT NOT NULL,
            row_id INTEGER NOT NULL,observed_at REAL NOT NULL,payload TEXT NOT NULL)""")
        for reference in (provisional, canonical):
            archive.conn.execute("INSERT INTO bot_events VALUES(NULL,?,?,1,'Msg_test',1,1,?)",
                                 (reference, source["source_ref"], json.dumps({"message_ref": reference})))
        archive.conn.commit()
        history = [tuple(row) for row in archive.conn.execute("SELECT * FROM bot_events ORDER BY event_id")]
        commit(archive, source, [raw(server_id="ack")], mode="live")
        commit(archive, source, [raw(2, "next")], mode="live")
        assert archive.get_message_context(provisional)["message"]["message_ref"] == canonical
        assert [tuple(row) for row in archive.conn.execute("SELECT * FROM bot_events ORDER BY event_id")] == history
    with ArchiveStore(tmp_path) as restarted:
        assert [tuple(row) for row in restarted.conn.execute("SELECT * FROM bot_events ORDER BY event_id")] == history
