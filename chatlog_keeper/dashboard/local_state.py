"""Durable local projection queue. This never writes to the WeChat archive."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import threading
import uuid


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def content_hash(value):
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class LocalState:
    def __init__(self, directory):
        self.directory = Path(directory)
        if self.directory.is_symlink():
            raise ValueError("unsafe_dashboard_state")
        self.directory.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.directory.chmod(0o700)
        self.path = self.directory / "sync.sqlite"
        if self.path.is_symlink():
            raise ValueError("unsafe_dashboard_state")
        old = os.umask(0o077)
        try:
            self.conn = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        finally:
            os.umask(old)
        self.path.chmod(0o600)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.lock = threading.RLock()
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS partitions(
            group_ref TEXT NOT NULL,day TEXT NOT NULL,revision INTEGER NOT NULL,
            content_hash TEXT NOT NULL,data TEXT NOT NULL,initial INTEGER NOT NULL,
            pending INTEGER NOT NULL DEFAULT 1,PRIMARY KEY(group_ref,day));
        CREATE TABLE IF NOT EXISTS feature_outbox(
            kind TEXT NOT NULL,item_key TEXT NOT NULL,digest TEXT NOT NULL,
            data TEXT NOT NULL,pending INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY(kind,item_key));
        """)
        with self.lock, self.conn:
            for key, value in (("source_id", str(uuid.uuid4())), ("sender_salt", secrets.token_hex(32))):
                self.conn.execute("INSERT OR IGNORE INTO meta VALUES(?,?)", (key, canonical_json(value)))

    @property
    def source_id(self):
        return self.get("source_id")

    def get(self, key, default=None):
        with self.lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else default

    def set(self, key, value):
        with self.lock, self.conn:
            self.conn.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, canonical_json(value)))

    def queue(self, data, *, initial=False):
        group_ref, day = data["group"]["group_ref"], data["day"]
        digest = content_hash(data)
        with self.lock, self.conn:
            row = self.conn.execute("SELECT * FROM partitions WHERE group_ref=? AND day=?", (group_ref, day)).fetchone()
            if row and row["content_hash"] == digest:
                return False
            revision = row["revision"] + 1 if row else 1
            # An unsent initial snapshot remains historical when it is coalesced.
            historical = bool(initial or (row and row["pending"] and row["initial"]))
            self.conn.execute("""INSERT INTO partitions VALUES(?,?,?,?,?,?,1)
                ON CONFLICT(group_ref,day) DO UPDATE SET revision=excluded.revision,
                content_hash=excluded.content_hash,data=excluded.data,initial=excluded.initial,pending=1""",
                (group_ref, day, revision, digest, canonical_json(data), int(historical)))
        return True

    def partitions(self, *, pending_only=False, limit=None):
        query = "SELECT * FROM partitions" + (" WHERE pending=1" if pending_only else "") + " ORDER BY day,group_ref"
        params = ()
        if limit is not None:
            query += " LIMIT ?"
            params = (limit,)
        with self.lock:
            rows = self.conn.execute(query, params).fetchall()
        return [{"source_id": self.source_id, "group_ref": r["group_ref"], "day": r["day"],
                 "revision": r["revision"], "content_hash": r["content_hash"],
                 "initial": bool(r["initial"]), "data": json.loads(r["data"])} for r in rows]

    def acknowledge(self, group_ref, day, revision):
        # A late ACK cannot remove a newer coalesced partition.
        with self.lock, self.conn:
            self.conn.execute("UPDATE partitions SET pending=0,initial=0 WHERE group_ref=? AND day=? AND revision=?", (group_ref, day, revision))

    def partition_status(self, group_ref, day):
        with self.lock:
            row = self.conn.execute("SELECT content_hash,pending FROM partitions WHERE group_ref=? AND day=?", (group_ref, day)).fetchone()
            return dict(row) if row else None

    def retry_partition(self, group_ref, day):
        with self.lock, self.conn:
            self.conn.execute("UPDATE partitions SET pending=1 WHERE group_ref=? AND day=?", (group_ref, day))

    def queue_feature(self, kind, key, payload, digest):
        with self.lock, self.conn:
            self.conn.execute("""INSERT INTO feature_outbox VALUES(?,?,?,?,1)
                ON CONFLICT(kind,item_key) DO UPDATE SET digest=excluded.digest,data=excluded.data,pending=1
                WHERE feature_outbox.digest<>excluded.digest""", (kind, key, digest, canonical_json(payload)))

    def features(self, kind, *, pending_only=True):
        with self.lock:
            rows = self.conn.execute("SELECT * FROM feature_outbox WHERE kind=?" + (" AND pending=1" if pending_only else "") + " ORDER BY item_key", (kind,)).fetchall()
        return [{**dict(row), "data": json.loads(row["data"])} for row in rows]

    def acknowledge_feature(self, kind, key, digest):
        with self.lock, self.conn:
            self.conn.execute("UPDATE feature_outbox SET pending=0 WHERE kind=? AND item_key=? AND digest=?", (kind, key, digest))

    def retry_feature(self, kind, key):
        with self.lock, self.conn:
            self.conn.execute("UPDATE feature_outbox SET pending=1 WHERE kind=? AND item_key=?", (kind, key))

    def close(self):
        self.conn.close()
