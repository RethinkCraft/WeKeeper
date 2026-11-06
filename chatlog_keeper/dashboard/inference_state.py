"""Private authorization/quota pause state shared by interactive SIWC workers."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import time


class InferenceState:
    def __init__(self, directory, *, clock=time.time):
        self.root = Path(directory).expanduser()
        self.path = self.root / "inference.sqlite"
        if self.root.is_symlink() or self.path.is_symlink():
            raise ValueError("unsafe_inference_state")
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.clock = clock
        old = os.umask(0o077)
        try:
            self.conn = sqlite3.connect(self.path, timeout=10)
        finally:
            os.umask(old)
        self.path.chmod(0o600)
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
        self.conn.commit()
        self._import_legacy_pause()

    def close(self):
        self.conn.close()

    def _import_legacy_pause(self):
        # Commit the marker together with the imported pause. A later resume
        # must never resurrect the old pause if cleanup was interrupted.
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            if not self.conn.execute("SELECT 1 FROM meta WHERE key='legacy_pause_imported'").fetchone():
                path = self.root / "summary.sqlite"
                if any((self.root / ("summary.sqlite" + suffix)).is_symlink() for suffix in ("", "-wal", "-shm", "-journal")):
                    raise ValueError("unsafe_legacy_pause_state")
                if path.exists():
                    with sqlite3.connect(path.absolute().as_uri() + "?mode=ro", uri=True) as legacy:
                        if legacy.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='summary_meta'").fetchone():
                            row = legacy.execute("SELECT value FROM summary_meta WHERE key='status'").fetchone()
                            value = json.loads(row[0]) if row else {}
                            if not isinstance(value, dict):
                                raise ValueError("invalid_legacy_pause_state")
                            if value.get("paused"):
                                state = {"state": "paused", "paused": True, "error": value.get("error"), "checked_at": self.clock()}
                                self.conn.execute("INSERT OR IGNORE INTO meta VALUES('status',?)", (json.dumps(state),))
                self.conn.execute("INSERT INTO meta VALUES('legacy_pause_imported','true')")
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            self.close()
            raise

    def status(self):
        row = self.conn.execute("SELECT value FROM meta WHERE key='status'").fetchone()
        return json.loads(row[0]) if row else {"state": "ready", "paused": False, "error": None}

    def _update(self, *, resume=False, **changes):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            previous = self.status()
            if previous.get("paused") and not resume:
                changes.update(state="paused", paused=True, error=previous.get("error"))
            value = {**previous, **changes, "checked_at": self.clock()}
            self.conn.execute("INSERT INTO meta VALUES('status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(value),))
            self.conn.commit()
            return value
        except BaseException:
            self.conn.rollback()
            raise

    def record_error(self, error):
        return self._update(state="paused" if error.paused else "error", paused=error.paused, error=error.public())

    def resume(self):
        return self._update(resume=True, state="ready", paused=False, error=None)
