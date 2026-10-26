"""The native cache gate uses synthetic existing data and never runs UI."""
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys

import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.native_cache_probe import probe, REASONS, ProbeInterrupted
from test_archive import setup_source, commit, raw

VOICE = b"\x02#!SILK_V3synthetic-cached-audio"
GROUP = "test@chatroom"


@pytest.fixture
def cached_voice(tmp_path, monkeypatch):
    state, root = tmp_path / "state", tmp_path / "client"
    account = root / "account"
    database = account / "db_storage/message/media_0.db"
    database.parent.mkdir(parents=True)
    with sqlite3.connect(database) as db:
        db.execute("CREATE TABLE Name2Id(user_name TEXT)")
        db.execute("INSERT INTO Name2Id VALUES(?)", (GROUP,))
        db.execute("CREATE TABLE VoiceInfo(chat_name_id INTEGER,create_time INTEGER,svr_id TEXT,voice_data BLOB)")
        db.execute("INSERT INTO VoiceInfo VALUES(1,100,'101',?)", (VOICE,))
    with ArchiveStore(state) as store:
        source = setup_source(store)
        reference = commit(store, source, [raw(server_id="101", local_type=34, timestamp=100,
            parsed={"msg_type": 34, "text": "[语音]", "parse_status": "parsed"})])["message_refs"][0]
        sha = hashlib.sha256(VOICE).hexdigest()
        target = store.media_dir / f"raw/{sha[:2]}/{sha}.bin"
        target.parent.mkdir(parents=True)
        target.write_bytes(VOICE)
        store.upsert_voice("account", GROUP, "101", {"raw_path": str(target.relative_to(store.media_dir)),
            "raw_sha256": sha, "raw_size": len(VOICE), "codec": "silk_v3"}, {})
        store.set_meta("collector", {"account_id": "account", "data_root": str(root)})
    key = state / "secrets/wechat_accounts" / (hashlib.sha256(b"account").hexdigest() + ".key")
    key.parent.mkdir(parents=True)
    key.parent.chmod(0o700)
    key.write_text("61" * 32)
    key.chmod(0o600)
    monkeypatch.setattr("chatlog_keeper.p2_sources.wechat_db._verify_key_v4", lambda key, first: key == b"a" * 32)
    monkeypatch.setattr("chatlog_keeper.collector.wechat_db._decrypt_db_v4", lambda source, key, target: bool(shutil.copyfile(source, target)))
    job = {"state_dir": str(state), "message_ref": reference, "account_id": "account", "group_id": GROUP,
           "server_id": "101", "create_time": 100}
    return job, database, key


def update(database, statement, parameters=()):
    with sqlite3.connect(database) as db:
        db.execute(statement, parameters)


def test_exact_current_audio_is_read_only_and_matches_archive(cached_voice):
    job, database, _ = cached_voice
    before = database.read_bytes()
    archive = Path(job["state_dir"]) / "archive/archive.sqlite"
    archive_before = hashlib.sha256(archive.read_bytes()).hexdigest()
    result = probe(job)
    assert result["status"] == "cached" and result["reason"] == "native_voice_cache_verified"
    assert result["proof"]["sha256"] == hashlib.sha256(VOICE).hexdigest()
    assert result["proof"]["matched_rows"] == 1
    assert database.read_bytes() == before
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == archive_before
    assert not any(key in json.dumps(result) for key in ("voice_data", "raw_path", "source_path"))


@pytest.mark.parametrize("statement,parameters,reason", [
    ("DELETE FROM VoiceInfo", (), "native_voice_cache_missing"),
    ("UPDATE VoiceInfo SET voice_data=NULL", (), "native_voice_cache_empty"),
    ("UPDATE VoiceInfo SET voice_data=?", (b"",), "native_voice_cache_empty"),
    ("UPDATE VoiceInfo SET voice_data=?", (VOICE + b"changed",), "native_voice_cache_mismatch"),
    ("UPDATE VoiceInfo SET create_time=101", (), "native_voice_cache_identity_mismatch"),
    ("INSERT INTO VoiceInfo SELECT * FROM VoiceInfo", (), "native_voice_cache_ambiguous"),
])
def test_missing_cleared_mismatch_and_ambiguity_fail_closed(cached_voice, statement, parameters, reason):
    job, database, _ = cached_voice
    update(database, statement, parameters)
    result = probe(job)
    assert result == {"status": "unavailable", "reason": reason, "proof": {}}


def test_duplicate_across_shards_is_not_mistaken_for_unique(cached_voice):
    job, database, _ = cached_voice
    shutil.copyfile(database, database.with_name("media_1.db"))
    assert probe(job)["reason"] == "native_voice_cache_ambiguous"


def test_missing_key_does_not_acquire_material_or_change_archive(cached_voice):
    job, database, key = cached_voice
    key.unlink()
    assert probe(job)["reason"] == "native_voice_cache_key_unavailable"
    assert not key.exists()


def test_job_and_collector_account_must_match(cached_voice):
    job, _, _ = cached_voice
    assert probe({**job, "account_id": "other"})["reason"] == "native_voice_cache_identity_mismatch"
    with ArchiveStore(job["state_dir"]) as store:
        store.set_meta("collector", {"account_id": "other", "data_root": "/unused"})
    assert probe(job)["reason"] == "native_voice_cache_account_mismatch"


def test_timeout_unwinds_and_removes_decrypted_snapshot(cached_voice, monkeypatch):
    from chatlog_keeper.collector import Collector
    job, _, _ = cached_voice
    snapshots = []
    original = Collector._get_snapshot
    def snapshot(self, *args):
        value = original(self, *args)
        snapshots.append(value.parent)
        return value
    def expire(*args):
        raise ProbeInterrupted("native_voice_cache_timeout")
    monkeypatch.setattr(Collector, "_get_snapshot", snapshot)
    monkeypatch.setattr("chatlog_keeper.native_cache_probe._matching_voice", expire)
    assert probe(job)["reason"] == "native_voice_cache_timeout"
    assert snapshots and not any(path.exists() for path in snapshots)


def test_child_rejects_oversized_input_without_private_exception_output():
    result = subprocess.run([sys.executable, "-B", "-m", "chatlog_keeper.native_cache_probe"],
        input=b"x" * 17000, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=12)
    value = json.loads(result.stdout)
    assert value["status"] == "unavailable" and value["reason"] in REASONS
    assert value["reason"] == "native_voice_cache_invalid_job"
    assert b"Traceback" not in result.stderr
