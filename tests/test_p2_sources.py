"""Synthetic local auxiliary caches only; never launch or operate WeChat."""
import hashlib
import io
from pathlib import Path
import shutil
import sqlite3
from types import SimpleNamespace

from PIL import Image
import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.archive_media import _read_blob
from chatlog_keeper.collector import Collector
from chatlog_keeper import p2_sources
from chatlog_keeper.p2_sources import P2Sources
from test_archive import setup_source, commit, raw

GROUP = "test@chatroom"
VOICE = b"\x02#!SILK_V3" + b"synthetic-audio"


def png(color="blue"):
    out = io.BytesIO()
    Image.new("RGB", (16, 12), color).save(out, format="PNG")
    return out.getvalue()


def voice_database(path, rows=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE Name2Id(user_name TEXT)")
        db.executemany("INSERT INTO Name2Id VALUES(?)", [(GROUP,), ("private-person",)])
        db.execute("CREATE TABLE VoiceInfo(chat_name_id INTEGER,create_time INTEGER,local_id INTEGER,svr_id TEXT,voice_data BLOB,data_index INTEGER)")
        db.executemany("INSERT INTO VoiceInfo VALUES(?,?,?,?,?,?)", rows or [(1, 100, 7, "101", VOICE, 0)])


def head_database(path, body=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE head_image(username TEXT,md5 TEXT,image_buffer BLOB,update_time INTEGER)")
        db.execute("INSERT INTO head_image VALUES(?,?,?,?)", (GROUP, "native-avatar-version", body or png(), 100))


@pytest.fixture
def sources(tmp_path, monkeypatch):
    store = ArchiveStore(tmp_path / "state")
    source = setup_source(store)
    ref = commit(store, source, [raw(server_id="101", local_type=34, timestamp=100,
        parsed={"msg_type": 34, "text": "[语音]", "parse_status": "parsed",
                "attachment": {"kind": "voice", "voice_length_ms": 1234}})])["message_refs"][0]
    account = tmp_path / "data/account"
    voice = account / "db_storage/message/media_0.db"
    head = account / "db_storage/head_image/head_image.db"
    voice_database(voice)
    head_database(head)
    collector = Collector(store, data_root=account.parent, account="account")
    collector.account_id, collector.account_dir = "account", account
    decryptions = []
    def decrypt(path, key, destination):
        assert key == b"a" * 32
        decryptions.append(path.name)
        shutil.copyfile(path, destination)
        return True
    monkeypatch.setattr("chatlog_keeper.p2_sources.wechat_db._verify_key_v4", lambda key, first: key == b"a" * 32)
    monkeypatch.setattr("chatlog_keeper.collector.wechat_db._decrypt_db_v4", decrypt)
    reader = SimpleNamespace(enc_keys={voice: b"a" * 32})
    enrichment = P2Sources(collector)
    yield SimpleNamespace(store=store, ref=ref, collector=collector, source=enrichment,
                          voice=voice, head=head, reader=reader, account=account,
                          decryptions=decryptions, names={GROUP: "Synthetic group", "wxid_sender": "Sender"})
    collector.close()
    store.close()


def collect(sources, **kwargs):
    return sources.source.collect(sources.reader, sources.names, **kwargs)


def test_voice_and_avatar_archive_once_without_changing_message_identity_or_health_evidence(sources):
    s = sources
    before = tuple(s.store.conn.execute("SELECT message_ref,first_seen,create_time FROM messages").fetchone())
    streams = [tuple(r) for r in s.store.conn.execute("SELECT * FROM streams")]
    origins = [tuple(r) for r in s.store.conn.execute("SELECT * FROM origins")]
    result = collect(s)
    assert not result["errors"]
    assert result["voices_linked"] == 1 and result["avatars_saved"] == 1
    assert len(s.decryptions) == 2 and not s.collector._snapshots
    voice = s.store.get_voice(s.ref)
    assert voice["audio_status"] == "archived" and voice["audio"]["codec"] == "silk_v3"
    assert voice["native_text"] is None
    assert voice["audio"]["sha256"] == hashlib.sha256(VOICE).hexdigest()
    profile = s.store.get_profile(GROUP, include_avatar=True)
    assert profile["avatar"]["width"] == 16 and profile["avatar"]["height"] == 12
    assert s.store.get_profile_avatar(profile["avatar"]["avatar_ref"])["data"] == png()
    assert tuple(s.store.conn.execute("SELECT message_ref,first_seen,create_time FROM messages").fetchone()) == before
    assert [tuple(r) for r in s.store.conn.execute("SELECT * FROM streams")] == streams
    assert [tuple(r) for r in s.store.conn.execute("SELECT * FROM origins")] == origins
    again = collect(s)
    assert again["sources_unchanged"] == 2 and again["voice_rows_checked"] == 0
    assert len(s.decryptions) == 2
    assert s.store.conn.execute("SELECT COUNT(*) FROM voice_assets").fetchone()[0] == 1
    assert s.store.conn.execute("SELECT COUNT(*) FROM avatar_assets").fetchone()[0] == 1


def test_copies_stay_readable_after_source_caches_are_deleted(sources):
    s = sources
    collect(s)
    voice = s.store.conn.execute("SELECT raw_path,raw_sha256 FROM media WHERE kind='voice'").fetchone()
    profile = s.store.get_profile(GROUP)
    shutil.rmtree(s.account)
    assert _read_blob(s.store.media_dir, voice[0], voice[1]) == VOICE
    assert s.store.get_profile_avatar(profile["avatar"]["avatar_ref"])["data"] == png()
    assert s.store.get_voice(s.ref)["audio_status"] == "archived"
    report = collect(s)
    assert {r["error"] for r in report["errors"]} == {"auxiliary_source_unavailable"}


def test_changed_avatar_and_voice_keep_old_content_versions(sources):
    s = sources
    collect(s)
    old_profile = s.store.get_profile(GROUP)
    with sqlite3.connect(s.head) as db:
        db.execute("UPDATE head_image SET image_buffer=?,update_time=200", (png("red"),))
    with sqlite3.connect(s.voice) as db:
        db.execute("UPDATE VoiceInfo SET voice_data=?", (VOICE + b"new",))
    collect(s)
    new_profile = s.store.get_profile(GROUP)
    assert old_profile["avatar"]["avatar_ref"] != new_profile["avatar"]["avatar_ref"]
    assert s.store.get_profile_avatar(old_profile["avatar"]["avatar_ref"])["data"] == png()
    assert s.store.get_profile_avatar(new_profile["avatar"]["avatar_ref"])["data"] == png("red")
    assert s.store.conn.execute("SELECT COUNT(*) FROM voice_assets").fetchone()[0] == 2
    assert s.store.conn.execute("SELECT COUNT(*) FROM avatar_assets").fetchone()[0] == 2


def test_observed_group_sender_avatar_does_not_require_contact_entry(sources):
    s = sources
    sender = s.store.conn.execute("SELECT sender_id FROM messages LIMIT 1").fetchone()[0]
    with sqlite3.connect(s.head) as db:
        db.execute("INSERT INTO head_image VALUES(?,?,?,?)", (sender, "sender-version", png('red'), 100))
    s.names.pop(sender, None)
    result = collect(s)
    assert not result['errors']
    assert s.store.conn.execute("SELECT avatar_ref FROM profiles WHERE account_id=? AND subject_id=?", ('account', sender)).fetchone()[0]
    assert s.store.conn.execute("SELECT COUNT(*) FROM avatar_assets").fetchone()[0] == 2
    assert s.store.get_status()["messages"] == 1


def test_unmatched_audio_is_saved_and_can_link_after_source_disappears(sources):
    s = sources
    with sqlite3.connect(s.voice) as db:
        db.execute("INSERT INTO VoiceInfo VALUES(?,?,?,?,?,?)", (1, 101, 8, "102", VOICE + b"late", 0))
    result = collect(s)
    assert result["voices_linked"] == 1
    shutil.rmtree(s.account)
    source = setup_source(s.store)
    ref = commit(s.store, source, [raw(2, "102", local_type=34, timestamp=101,
        parsed={"msg_type": 34, "text": "[语音]", "parse_status": "parsed"})])["message_refs"][0]
    collect(s)
    assert s.store.get_voice(ref)["audio_status"] == "archived"
    assert s.store.get_voice(ref)["audio"]["sha256"] == hashlib.sha256(VOICE + b"late").hexdigest()


def test_private_chat_audio_is_not_copied_and_duplicate_group_sid_is_not_mislinked(sources):
    s = sources
    with sqlite3.connect(s.voice) as db:
        db.execute("INSERT INTO VoiceInfo VALUES(?,?,?,?,?,?)", (2, 101, 8, "private", b"#!SILK_V3-private", 0))
        db.execute("INSERT INTO VoiceInfo VALUES(?,?,?,?,?,?)", (1, 100, 9, "101", VOICE + b"ambiguous", 1))
    result = collect(s)
    assert result["voice_ambiguous"] == 2
    assert s.store.get_voice(s.ref)["audio_status"] == "not_archived"
    assert len(list((s.store.media_dir / "raw").glob("*/*.bin"))) == 3  # two ambiguous group payloads and avatar
    assert not any(b"-private" in path.read_bytes() for path in (s.store.media_dir / "raw").glob("*/*.bin"))


def test_invalid_oversize_and_unknown_formats_are_explicit_and_do_not_drop_messages(sources, monkeypatch):
    s = sources
    monkeypatch.setattr(p2_sources, "MAX_AVATAR_BYTES", 5)
    monkeypatch.setattr(p2_sources, "MAX_VOICE_BYTES", 5)
    result = collect(s)
    assert result["voice_over_limit"] == 1 and result["avatar_invalid"] == 1
    assert s.store.get_status()["messages"] == 1
    monkeypatch.undo()
    with sqlite3.connect(s.voice) as db:
        db.execute("UPDATE VoiceInfo SET voice_data=?", (b"unrecognized future codec",))
    # Restore the synthetic decryption fixture after monkeypatch.undo().
    monkeypatch.setattr("chatlog_keeper.p2_sources.wechat_db._verify_key_v4", lambda key, first: True)
    monkeypatch.setattr("chatlog_keeper.collector.wechat_db._decrypt_db_v4", lambda path, key, target: bool(shutil.copyfile(path, target)))
    result = collect(s)
    assert result["voice_unknown_format"] == 1
    assert s.store.get_voice(s.ref)["audio"]["codec"] == "unknown"


def test_auxiliary_key_failure_does_not_extract_or_modify_sources(sources, monkeypatch):
    s = sources
    before = {path: path.read_bytes() for path in [s.voice, s.head]}
    monkeypatch.setattr("chatlog_keeper.p2_sources.wechat_db._verify_key_v4", lambda *args: False)
    result = collect(s)
    assert {r["error"] for r in result["errors"]} == {"cached_auxiliary_key_unavailable"}
    assert s.decryptions == []
    assert all(path.read_bytes() == body for path, body in before.items())
    assert s.store.get_status()["messages"] == 1


def test_row_batches_resume_and_zero_budget_is_nonblocking(sources, monkeypatch):
    s = sources
    with sqlite3.connect(s.voice) as db:
        db.executemany("INSERT INTO VoiceInfo VALUES(?,?,?,?,?,?)", [(1, n, n, str(n), VOICE + str(n).encode(), 0) for n in range(102, 108)])
    monkeypatch.setattr(p2_sources, "MAX_ROWS", 2)
    assert collect(s, time_budget=0)["budget_exhausted"]
    for _ in range(20):
        result = collect(s)
        states = [s.source._get_state(path) for path in [s.voice, s.head]]
        if all(x.get("complete") for x in states):
            break
    assert all(x["complete"] for x in states)
    assert s.store.get_voice(s.ref)["audio_status"] == "archived"
    assert not s.collector._snapshots
    # Each unchanged family decrypted only once across all bounded batches.
    assert s.decryptions.count("media_0.db") == 1


def test_source_enumeration_does_not_follow_symlinks(sources):
    s = sources
    other = s.account.parent / "other.db"
    other.write_bytes(b"outside")
    link = s.voice.with_name("media_1.db")
    link.symlink_to(other)
    assert link not in p2_sources.database_paths(s.account)


def test_failed_avatar_publication_does_not_advance_checkpoint_or_lose_retry(sources, monkeypatch):
    s = sources
    original = s.source._put
    def fail_avatar(payload):
        if payload == png():
            raise OSError("synthetic disk error")
        return original(payload)
    monkeypatch.setattr(s.source, "_put", fail_avatar)
    first = collect(s)
    assert first["errors"] == [{"source": "head_image.db", "error": "auxiliary_source_failed"}]
    assert s.source._get_state(s.head).get("after", 0) == 0
    assert s.store.get_profile(GROUP)["avatar"]["status"] == "not_cached"
    monkeypatch.setattr(s.source, "_put", original)
    second = collect(s)
    assert not second["errors"] and second["avatars_saved"] == 1
    assert s.store.get_profile(GROUP)["avatar"]["status"] == "archived"


def test_auxiliary_source_errors_do_not_replace_successful_main_collection(environment, monkeypatch):
    collector, store, database, flags = environment
    account = database.parent.parent.parent
    bad = account / "db_storage/message/media_0.db"
    with sqlite3.connect(bad) as db:
        db.execute("CREATE TABLE unrelated(x TEXT)")
    store.upsert_voice = lambda *args: {"matched": False}
    store.upsert_profiles = lambda *args: {"profiles": 0, "avatars": 0}
    store.media_dir = Path(store.state_dir) / "archive/media"
    monkeypatch.setattr("chatlog_keeper.p2_sources.wechat_db._verify_key_v4", lambda *args: True)
    result = collector.collect_once()
    assert result["p2_sources"]["errors"] == [{"source": "media_0.db", "error": "unsupported_voice_source_schema"}]
    assert store.status["last_error"] is None
    assert len(store.records) == 2


from test_collector import environment  # noqa: E402,F401
