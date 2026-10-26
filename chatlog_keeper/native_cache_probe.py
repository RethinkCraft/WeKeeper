"""Prove an exact archived voice is still in the authenticated client cache.

This short-lived child never operates WeChat, acquires keys, downloads media or
writes the archive. A saved archive alone is deliberately insufficient proof.
"""
from __future__ import annotations

from contextlib import closing, redirect_stdout
import ctypes
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import signal
import sqlite3
import sys
import time
from types import SimpleNamespace

MAX_JOB_BYTES = 16 * 1024
TIMEOUT_SECONDS = 10
MAX_MEDIA_DATABASES = 16
REASONS = frozenset({
    "native_voice_cache_verified", "native_voice_cache_invalid_job",
    "native_voice_cache_archive_unavailable", "native_voice_cache_identity_mismatch",
    "native_voice_cache_archive_audio_unavailable", "native_voice_cache_account_mismatch",
    "native_voice_cache_source_unavailable", "native_voice_cache_key_unavailable",
    "native_voice_cache_source_busy", "native_voice_cache_schema_unverified",
    "native_voice_cache_missing", "native_voice_cache_ambiguous", "native_voice_cache_empty",
    "native_voice_cache_mismatch", "native_voice_cache_over_limit",
    "native_voice_cache_timeout", "native_voice_cache_parent_gone", "native_voice_cache_failed",
})


class CacheUnavailable(Exception):
    def __init__(self, reason):
        self.reason = reason if reason in REASONS else "native_voice_cache_failed"
        super().__init__(self.reason)


class ProbeInterrupted(BaseException):
    """Cannot be swallowed by low-level crypto/schema exception fallbacks."""
    def __init__(self, reason):
        self.reason = reason


def _failure(reason):
    return {"status": "unavailable", "reason": reason if reason in REASONS else "native_voice_cache_failed", "proof": {}}


def _job(value):
    if not isinstance(value, dict):
        raise CacheUnavailable("native_voice_cache_invalid_job")
    for name, maximum in (("state_dir", 4096), ("message_ref", 204),
                          ("account_id", 512), ("group_id", 512), ("server_id", 512)):
        text = value.get(name)
        if not isinstance(text, str) or not 0 < len(text) <= maximum or any(ord(c) < 32 for c in text):
            raise CacheUnavailable("native_voice_cache_invalid_job")
    if (not Path(value["state_dir"]).is_absolute() or
            not re.fullmatch(r"msg_[A-Za-z0-9_-]{1,200}", value["message_ref"]) or
            not value["group_id"].endswith("@chatroom") or
            any(c in value["account_id"] for c in "/\\") or
            value["account_id"] in {".", ".."} or
            type(value.get("create_time")) is not int or value["create_time"] < 0 or
            ("parent_pid" in value and (type(value["parent_pid"]) is not int or value["parent_pid"] <= 1))):
        raise CacheUnavailable("native_voice_cache_invalid_job")
    return value


def _matching_voice(connection, job):
    tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"VoiceInfo", "Name2Id"} <= tables:
        raise CacheUnavailable("native_voice_cache_schema_unverified")
    voice_columns = {r[1] for r in connection.execute('PRAGMA table_info("VoiceInfo")')}
    names = {r[1] for r in connection.execute('PRAGMA table_info("Name2Id")')}
    if not {"chat_name_id", "create_time", "svr_id", "voice_data"} <= voice_columns or "user_name" not in names:
        raise CacheUnavailable("native_voice_cache_schema_unverified")
    from chatlog_keeper.p2_sources import MAX_VOICE_BYTES
    # Two rows suffice to refuse ambiguity; do not read an unbounded BLOB.
    return connection.execute(
        "SELECT v.create_time,length(v.voice_data),"
        "CASE WHEN length(v.voice_data)<=? THEN v.voice_data ELSE NULL END "
        "FROM VoiceInfo v JOIN Name2Id n ON n.rowid=v.chat_name_id "
        "WHERE n.user_name=? AND CAST(v.svr_id AS TEXT)=? LIMIT 2",
        (MAX_VOICE_BYTES, job["group_id"], job["server_id"])).fetchall()


def _probe(job):
    from chatlog_keeper.archive import ArchiveStore
    from chatlog_keeper.collector import Collector
    from chatlog_keeper.p2_sources import P2Sources, SourceUnavailable, database_paths, MAX_VOICE_BYTES

    try:
        store = ArchiveStore(Path(job["state_dir"]), readonly=True)
    except Exception:
        raise CacheUnavailable("native_voice_cache_archive_unavailable") from None
    with store:
        with store._read_transaction():
            row = store.conn.execute("SELECT * FROM messages WHERE message_ref=?", (job["message_ref"],)).fetchone()
            if row is None or row["msg_type"] != 34 or any(row[key] != job[key] for key in ("account_id", "group_id", "server_id", "create_time")):
                raise CacheUnavailable("native_voice_cache_identity_mismatch")
            for optional in ("sender_id",):
                if optional in job and row[optional] != job[optional]:
                    raise CacheUnavailable("native_voice_cache_identity_mismatch")
            configuration = store.get_meta("collector", {})
            if configuration.get("account_id") != row["account_id"]:
                raise CacheUnavailable("native_voice_cache_account_mismatch")
            data_root = configuration.get("data_root")
            if not isinstance(data_root, str) or not Path(data_root).is_absolute():
                raise CacheUnavailable("native_voice_cache_source_unavailable")
            root = Path(data_root)
            if root.is_symlink():
                raise CacheUnavailable("native_voice_cache_source_unavailable")
            try:
                accounts = [p for p in root.iterdir() if p.name == row["account_id"] and p.is_dir() and not p.is_symlink()]
            except OSError:
                raise CacheUnavailable("native_voice_cache_source_unavailable") from None
            if len(accounts) != 1:
                raise CacheUnavailable("native_voice_cache_account_mismatch")
            asset = store.conn.execute("SELECT a.* FROM voice_current c JOIN voice_assets a USING(asset_ref) WHERE c.message_ref=?", (row["message_ref"],)).fetchone()
            if store._verify_voice_asset(asset)[0] != "archived":
                raise CacheUnavailable("native_voice_cache_archive_audio_unavailable")
            expected = asset["sha256"]
            collector = Collector(store, data_root=root, account=row["account_id"])
            collector.account_dir = accounts[0]
            helper = P2Sources(collector)
            reader = SimpleNamespace(enc_keys={}, enc_key=None)
            matches = []
            try:
                paths = [p for p in database_paths(collector.account_dir) if re.fullmatch(r"media_[0-9]+\.db", p.name)]
                if not paths or len(paths) > MAX_MEDIA_DATABASES:
                    raise CacheUnavailable("native_voice_cache_source_unavailable")
                for source in paths:
                    try:
                        helper._safe_source(source)
                        key = helper._key(reader, source)
                        snapshot = collector._get_snapshot(source, key, "native-cache:" + collector._fingerprint(source))
                    except SourceUnavailable as exc:
                        code = {"cached_auxiliary_key_unavailable": "native_voice_cache_key_unavailable",
                                "auxiliary_source_busy": "native_voice_cache_source_busy",
                                "auxiliary_snapshot_size_limit": "native_voice_cache_over_limit"}.get(exc.code, "native_voice_cache_source_unavailable")
                        raise CacheUnavailable(code) from None
                    with closing(sqlite3.connect(snapshot.as_uri() + "?mode=ro", uri=True)) as connection:
                        connection.execute("PRAGMA query_only=ON")
                        matches.extend(_matching_voice(connection, job))
                    collector._discard_snapshot(source)
                    if len(matches) > 1:
                        raise CacheUnavailable("native_voice_cache_ambiguous")
                if not matches:
                    raise CacheUnavailable("native_voice_cache_missing")
                created, size, payload = matches[0]
                if created != job["create_time"]:
                    raise CacheUnavailable("native_voice_cache_identity_mismatch")
                if not size:
                    raise CacheUnavailable("native_voice_cache_empty")
                if size > MAX_VOICE_BYTES or payload is None:
                    raise CacheUnavailable("native_voice_cache_over_limit")
                if hashlib.sha256(bytes(payload)).hexdigest() != expected:
                    raise CacheUnavailable("native_voice_cache_mismatch")
                return {"status": "cached", "reason": "native_voice_cache_verified", "proof": {
                    **{key: job[key] for key in ("message_ref", "account_id", "group_id", "server_id", "create_time")},
                    "sha256": expected, "size_bytes": size, "matched_rows": 1, "checked_at": time.time()}}
            finally:
                collector.close()


def probe(job):
    """Pure read-only probe entry point; process limits are installed by main."""
    try:
        return _probe(_job(job))
    except (CacheUnavailable, ProbeInterrupted) as exc:
        return _failure(exc.reason)
    except BaseException:
        return _failure("native_voice_cache_failed")


def _child_limits():
    import resource
    parent = os.getppid()
    def expired(signum, frame):
        raise ProbeInterrupted("native_voice_cache_timeout" if signum in (signal.SIGALRM, signal.SIGXCPU) else "native_voice_cache_parent_gone")
    signal.signal(signal.SIGALRM, expired)
    signal.signal(signal.SIGTERM, expired)
    signal.signal(signal.SIGXCPU, expired)
    # SIGTERM unwinds finally blocks and removes decrypted temporary snapshots.
    if ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
        raise CacheUnavailable("native_voice_cache_parent_gone")
    if os.getppid() != parent or parent <= 1:
        raise CacheUnavailable("native_voice_cache_parent_gone")
    signal.alarm(TIMEOUT_SECONDS)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_CPU, (9, 10))
    resource.setrlimit(resource.RLIMIT_AS, (768 * 1024 * 1024, 768 * 1024 * 1024))
    return parent


def main():
    os.umask(0o077)
    logging.disable(logging.CRITICAL)
    try:
        parent = _child_limits()
        value = sys.stdin.buffer.read(MAX_JOB_BYTES + 1)
        if len(value) > MAX_JOB_BYTES:
            raise CacheUnavailable("native_voice_cache_invalid_job")
        job = _job(json.loads(value))
        if "parent_pid" in job and job["parent_pid"] != parent:
            raise CacheUnavailable("native_voice_cache_parent_gone")
        with redirect_stdout(sys.stderr):
            result = probe(job)
    except (CacheUnavailable, ProbeInterrupted) as exc:
        result = _failure(exc.reason)
    except BaseException:
        result = _failure("native_voice_cache_failed")
    finally:
        signal.alarm(0)
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
