"""Bounded avatar backfill from cached, authenticated contact URL metadata.

This worker never acquires keys, operates WeChat, or sends messages. Only its
private queue retains source URLs; archive profiles contain re-encoded images.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
import fcntl
import hashlib
import logging
import io
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx
from PIL import Image, ImageOps

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.collector import Collector
from chatlog_keeper.p2_sources import P2Sources

MAX_BYTES = 4 * 1024 * 1024
MAX_PIXELS = 4_000_000
REQUEST_SECONDS = 20
SCAN_SECONDS = 300
MAX_ATTEMPTS = 5
RETRY_COOLDOWN_SECONDS = 6 * 3600


class AvatarError(ValueError):
    def __init__(self, code, *, permanent=False):
        self.code, self.permanent = code, permanent
        super().__init__(code)


def allowed_url(value):
    """Accept only the observed avatar CDN, never a caller-selected host."""
    if not isinstance(value, str) or not value or len(value) > 4096 or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        return False
    try:
        parsed = urlsplit(value)
        return (parsed.scheme == "https" and parsed.hostname == "wx.qlogo.cn"
                and parsed.port in (None, 443) and parsed.netloc.lower() in {"wx.qlogo.cn", "wx.qlogo.cn:443"}
                and parsed.username is None and parsed.password is None and "#" not in value and "\\" not in value)
    except ValueError:
        return False


def _image_bytes(body):
    if len(body) > MAX_BYTES:
        raise AvatarError("avatar_too_large", permanent=True)
    try:
        with Image.open(io.BytesIO(body)) as original:
            if original.format not in {"PNG", "JPEG", "WEBP"} or not 0 < original.width * original.height <= MAX_PIXELS:
                raise AvatarError("invalid_avatar_image", permanent=True)
            original.verify()
        with Image.open(io.BytesIO(body)) as original:
            image = ImageOps.exif_transpose(original).convert("RGB")
            width, height = image.size
            # New pixels and an explicit format prevent carrying EXIF/ICC/text.
            clean = Image.new("RGB", image.size)
            clean.paste(image)
            output = io.BytesIO()
            clean.save(output, format="JPEG", quality=90, optimize=True)
        result = output.getvalue()
        if len(result) > MAX_BYTES:
            raise AvatarError("avatar_too_large", permanent=True)
        return result, width, height
    except AvatarError:
        raise
    except (OSError, ValueError, SyntaxError, Image.DecompressionBombError):
        raise AvatarError("invalid_avatar_image", permanent=True) from None


def _fetch(client, url):
    if not allowed_url(url):
        raise AvatarError("avatar_url_not_allowed", permanent=True)
    started = time.monotonic()
    try:
        with client.stream("GET", url, follow_redirects=False,
                           timeout=httpx.Timeout(5, connect=5),
                           headers={"Accept": "image/png,image/jpeg,image/webp"}) as response:
            status = response.status_code
            if 300 <= status < 400:
                raise AvatarError("avatar_redirect_rejected", permanent=True)
            if status in (401, 403, 404, 410):
                raise AvatarError("avatar_url_unavailable", permanent=True)
            if status != 200:
                raise AvatarError("avatar_http_error", permanent=400 <= status < 500 and status not in (408, 429))
            length = response.headers.get("content-length")
            if length and (not length.isdigit() or int(length) > MAX_BYTES):
                raise AvatarError("avatar_too_large", permanent=True)
            body = bytearray()
            for chunk in response.iter_bytes(64 * 1024):
                if time.monotonic() - started > REQUEST_SECONDS:
                    raise AvatarError("avatar_deadline_exceeded")
                body.extend(chunk)
                if len(body) > MAX_BYTES:
                    raise AvatarError("avatar_too_large", permanent=True)
        return _image_bytes(bytes(body))
    except httpx.HTTPError:
        raise AvatarError("avatar_network_error") from None


def _download_child(connection, url):
    """A killable process bounds DNS, network waits, and image decoders."""
    try:
        # HTTP debug logs include the private source URL. This child has no
        # application logging responsibility.
        logging.getLogger("httpx").disabled = True
        logging.getLogger("httpcore").disabled = True
        with httpx.Client(follow_redirects=False, trust_env=False) as client:
            body, width, height = _fetch(client, url)
        connection.send(("ok", body, width, height))
    except AvatarError as exc:
        connection.send(("error", exc.code, exc.permanent))
    except Exception:
        connection.send(("error", "avatar_download_failed", False))
    finally:
        connection.close()


class AvatarBackfill:
    def __init__(self, archive_dir, state_dir, *, client=None, clock=time.time, config_path=None):
        self.archive_dir, self.directory = Path(archive_dir), Path(state_dir)
        if self.directory.is_symlink():
            raise ValueError("unsafe_avatar_state")
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.directory.chmod(0o700)
        self.config_path = Path(config_path) if config_path else self.directory / "config.json"
        self.client, self.clock = client, clock
        self.lock, self.work_lock = threading.RLock(), threading.Lock()
        self.stop = threading.Event()
        path = self.directory / "avatar-backfill.sqlite"
        if path.is_symlink():
            raise ValueError("unsafe_avatar_state")
        old = os.umask(0o077)
        try:
            self.conn = sqlite3.connect(path, timeout=10, check_same_thread=False)
            path.chmod(0o600)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=FULL")
        finally:
            os.umask(old)
        self.lock_fd = os.open(self.directory / "avatar-backfill.lock",
                               os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        os.fchmod(self.lock_fd, 0o600)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs(
                account_id TEXT NOT NULL,subject_id TEXT NOT NULL,version TEXT NOT NULL,
                url TEXT NOT NULL,display_name TEXT NOT NULL,status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,next_attempt REAL NOT NULL DEFAULT 0,
                error TEXT,updated_at REAL NOT NULL,PRIMARY KEY(account_id,subject_id));
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        """)
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(jobs)")}
        if "retryable" not in columns:
            self.conn.execute("ALTER TABLE jobs ADD COLUMN retryable INTEGER NOT NULL DEFAULT 0")
            self.conn.commit()

    @contextmanager
    def _exclusive(self):
        with self.work_lock:
            try:
                fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise AvatarError("avatar_worker_already_running") from None
            try:
                yield
            finally:
                fcntl.flock(self.lock_fd, fcntl.LOCK_UN)

    def _groups(self):
        if self.config_path.is_symlink() or self.config_path.stat().st_size > 64 * 1024:
            raise ValueError("unsafe_avatar_config")
        value = json.loads(self.config_path.read_text()).get("groups", [])
        if not isinstance(value, list) or len(value) > 100 or any(not isinstance(g, str) or not g for g in value):
            raise ValueError("invalid_group_allowlist")
        return list(dict.fromkeys(value))

    def _scope(self):
        groups = self._groups()
        if not groups:
            return {}
        result = {}
        with ArchiveStore(self.archive_dir, readonly=True) as store:
            with store._lock, store._read_transaction():
                refs = store._group_scope(groups)
                for group in refs:
                    members = store.conn.execute("""SELECT g.account_id,m.sender_id,m.payload
                        FROM group_members m JOIN groups g USING(group_ref) WHERE m.group_ref=?""", (group,))
                    for row in members:
                        payload = json.loads(row["payload"])
                        result[(row["account_id"], row["sender_id"])] = str(payload.get("display_name") or "")[:120]
                    for row in store.conn.execute("""SELECT account_id,sender_id,MAX(sender_name) name
                            FROM messages WHERE group_ref=? AND sender_id<>'' GROUP BY account_id,sender_id""", (group,)):
                        result.setdefault((row["account_id"], row["sender_id"]), row["name"] or "")
        return {key: name for key, name in result.items() if key[1] and len(key[1]) <= 512 and not any(ord(c) < 32 for c in key[1])}

    def _contacts(self, account, subjects):
        """Read only selected identities, using cached key HMAC validation."""
        if Path(account).name != account or account in {".", ".."}:
            raise AvatarError("avatar_account_unavailable")
        with ArchiveStore(self.archive_dir, readonly=True) as store:
            meta = store.get_meta("collector", {})
            root = meta.get("data_root")
            if not root or meta.get("account_id") != account:
                raise AvatarError("avatar_source_unavailable")
            collector = Collector(store, data_root=root, account=account)
            collector.account_dir = Path(root) / account
            source = P2Sources(collector)
            database = collector.account_dir / "db_storage/contact/contact.db"
            try:
                source._safe_source(database)
                key = source._key(SimpleNamespace(enc_keys={}, enc_key=None), database)
                plain = collector._get_snapshot(database, key, collector._fingerprint(database))
                with closing(sqlite3.connect(plain.as_uri() + "?mode=ro", uri=True)) as connection:
                    connection.execute("PRAGMA query_only=ON")
                    columns = {r[1] for r in connection.execute("PRAGMA table_info(contact)")}
                    if not {"username", "small_head_url", "big_head_url", "head_img_md5"} <= columns:
                        raise AvatarError("unsupported_avatar_contact_schema")
                    result = {}
                    subjects = sorted(subjects)
                    for start in range(0, len(subjects), 400):
                        batch = subjects[start:start + 400]
                        for row in connection.execute("SELECT username,small_head_url,big_head_url,head_img_md5 FROM contact WHERE username IN (" + ",".join("?" for _ in batch) + ")", batch):
                            result[row[0]] = {"small": row[1] or "", "big": row[2] or "", "version": row[3] or ""}
                    return result
            except AvatarError:
                raise
            except Exception:
                raise AvatarError("avatar_source_unavailable") from None
            finally:
                collector.close()

    @staticmethod
    def _cached(store, account, subject):
        row = store.conn.execute("SELECT avatar_ref FROM profiles WHERE account_id=? AND subject_id=?", (account, subject)).fetchone()
        if row and row[0]:
            try:
                _image_bytes(store.get_profile_avatar(row[0])["data"])
                return True
            except Exception:
                pass
        return False

    def _meta(self, key, value):
        self.conn.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (key, json.dumps(value)))

    def scan(self):
        with self._exclusive():
            scope, now = self._scope(), self.clock()
            error = None
            with ArchiveStore(self.archive_dir, readonly=True) as store:
                missing = {key: name for key, name in scope.items() if not self._cached(store, *key)}
            contacts = {}
            failed_accounts = set()
            for account in {key[0] for key in missing}:
                try:
                    contacts[account] = self._contacts(account, {key[1] for key in missing if key[0] == account})
                except AvatarError as exc:
                    error = exc.code
                    failed_accounts.add(account)
            # Revocation can happen while a contact snapshot is being read.
            latest = self._scope()
            scope = {key: name for key, name in scope.items() if key in latest}
            with self.lock, self.conn:
                for old in self.conn.execute("SELECT account_id,subject_id FROM jobs").fetchall():
                    if (old[0], old[1]) not in scope:
                        self.conn.execute("DELETE FROM jobs WHERE account_id=? AND subject_id=?", tuple(old))
                for (account, subject), name in scope.items():
                    old = self.conn.execute("SELECT * FROM jobs WHERE account_id=? AND subject_id=?", (account, subject)).fetchone()
                    if (account, subject) not in missing:
                        self.conn.execute("""INSERT INTO jobs(account_id,subject_id,version,url,display_name,status,attempts,next_attempt,error,updated_at) VALUES(?,?,?,'',?,'ready',0,0,NULL,?)
                            ON CONFLICT(account_id,subject_id) DO UPDATE SET status='ready',url='',error=NULL,updated_at=excluded.updated_at""", (account, subject, "cached", name, now))
                        continue
                    if account in failed_accounts:
                        continue
                    entry = contacts.get(account, {}).get(subject, {})
                    small, big = entry.get("small", ""), entry.get("big", "")
                    url = next((u for u in (small, big) if allowed_url(u)), "")
                    version = hashlib.sha256(json.dumps([small, big, entry.get("version", "")], ensure_ascii=False).encode()).hexdigest()
                    if old and old["version"] == version and old["status"] != "ready":
                        if old["status"] == "unavailable" and old["retryable"] and old["next_attempt"] <= now:
                            # A long outage exhausts one bounded retry burst,
                            # then gets a fresh burst after the cooldown even
                            # when the cached avatar URL has not changed.
                            self.conn.execute("""UPDATE jobs SET status='pending',attempts=0,
                                next_attempt=0,error=NULL,retryable=0,updated_at=?
                                WHERE account_id=? AND subject_id=?""", (now, account, subject))
                        continue
                    status = "pending" if url else "missing_url"
                    self.conn.execute("""INSERT INTO jobs(account_id,subject_id,version,url,display_name,status,attempts,next_attempt,error,updated_at) VALUES(?,?,?,?,?,?,0,0,NULL,?)
                        ON CONFLICT(account_id,subject_id) DO UPDATE SET version=excluded.version,url=excluded.url,
                        display_name=excluded.display_name,status=excluded.status,attempts=0,next_attempt=0,error=NULL,retryable=0,updated_at=excluded.updated_at""", (account, subject, version, url, name, status, now))
                self._meta("last_scan_at", now)
                self._meta("last_error", error)
            return self.status()

    def _download(self, url, stop):
        if self.client is not None:
            return _fetch(self.client, url)
        context = multiprocessing.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(target=_download_child, args=(sender, url), daemon=True)
        process.start()
        sender.close()
        deadline = time.monotonic() + REQUEST_SECONDS
        try:
            while time.monotonic() < deadline:
                if stop.is_set():
                    raise AvatarError("avatar_cancelled")
                if receiver.poll(min(0.2, max(0, deadline - time.monotonic()))):
                    try:
                        result = receiver.recv()
                    except EOFError:
                        raise AvatarError("avatar_download_failed") from None
                    if result[0] == "ok":
                        return result[1:]
                    raise AvatarError(result[1], permanent=result[2])
                if not process.is_alive():
                    raise AvatarError("avatar_download_failed")
            raise AvatarError("avatar_deadline_exceeded")
        finally:
            receiver.close()
            if process.is_alive():
                process.terminate()
            process.join(timeout=1)
            if process.is_alive():
                process.kill()
                process.join(timeout=1)
            process.close()

    def flush(self, limit=10, stop=None):
        stop = stop or self.stop
        with self._exclusive():
            # Fill the visible active-member lists first, then prepare members
            # who have not spoken. This priority does not expand the allowlist.
            groups = self._groups()
            activity = []
            if groups:
                with ArchiveStore(self.archive_dir, readonly=True) as store:
                    activity = store.conn.execute("SELECT account_id,sender_id,COUNT(*) FROM messages WHERE group_ref IN (" + ",".join("?" for _ in groups) + ") AND create_time>=? AND create_time<=? GROUP BY account_id,sender_id", (*groups, self.clock() - 7 * 86400, self.clock())).fetchall()
            with self.lock, self.conn:
                self.conn.execute("CREATE TEMP TABLE IF NOT EXISTS activity_priority(account_id TEXT,subject_id TEXT,weight INTEGER,PRIMARY KEY(account_id,subject_id))")
                self.conn.execute("DELETE FROM activity_priority")
                self.conn.executemany("INSERT INTO activity_priority VALUES(?,?,?)", [tuple(row) for row in activity])
                rows = self.conn.execute("SELECT j.* FROM jobs j LEFT JOIN activity_priority a USING(account_id,subject_id) WHERE j.status IN ('pending','retry') AND j.next_attempt<=? ORDER BY COALESCE(a.weight,0) DESC,j.next_attempt,j.updated_at,j.account_id,j.subject_id LIMIT ?", (self.clock(), max(0, min(100, int(limit))))).fetchall()
            for row in rows:
                if stop.is_set():
                    break
                key = (row["account_id"], row["subject_id"])
                if key not in self._scope():
                    with self.lock, self.conn:
                        self.conn.execute("DELETE FROM jobs WHERE account_id=? AND subject_id=?", key)
                    continue
                with ArchiveStore(self.archive_dir, readonly=True) as store:
                    cached = self._cached(store, *key)
                try:
                    if not cached:
                        with self.lock:
                            tick = self.conn.execute("SELECT value FROM meta WHERE key='last_download_at'").fetchone()
                        delay = max(0, 1 - (self.clock() - json.loads(tick[0]))) if tick else 0
                        if delay and stop.wait(delay):
                            break
                        # Recheck the allowlist immediately before the request.
                        if key not in self._scope():
                            continue
                        with self.lock, self.conn:
                            self._meta("last_download_at", self.clock())
                        body, width, height = self._download(row["url"], stop)
                        if key not in self._scope() or stop.is_set():
                            continue
                        with ArchiveStore(self.archive_dir) as store:
                            # Another collector may have supplied a good image.
                            if not self._cached(store, *key):
                                collector = SimpleNamespace(store=store)
                                relative, digest = P2Sources(collector)._put(body)
                                existing = store.conn.execute("SELECT payload FROM profiles WHERE account_id=? AND subject_id=?", key).fetchone()
                                profile = json.loads(existing[0]) if existing else {"display_name": row["display_name"]}
                                store.upsert_profiles(key[0], [{"subject_id": key[1], "profile": profile,
                                    "avatar": {"relative_path": relative, "sha256": digest, "mime": "image/jpeg",
                                               "width": width, "height": height, "observed_at": self.clock(),
                                               "source": {"kind": "contact_avatar_backfill", "version": row["version"]}}}])
                    with self.lock, self.conn:
                        self.conn.execute("UPDATE jobs SET status='ready',error=NULL,updated_at=? WHERE account_id=? AND subject_id=? AND version=?", (self.clock(), *key, row["version"]))
                except Exception as exc:
                    failure = exc if isinstance(exc, AvatarError) else AvatarError("avatar_download_failed")
                    attempts = row["attempts"] + 1
                    status = "unavailable" if failure.permanent or attempts >= MAX_ATTEMPTS else "retry"
                    cooldown = RETRY_COOLDOWN_SECONDS if status == "unavailable" and not failure.permanent else min(3600, 60 * 2 ** (attempts - 1))
                    with self.lock, self.conn:
                        self.conn.execute("UPDATE jobs SET status=?,attempts=?,error=?,retryable=?,next_attempt=?,updated_at=? WHERE account_id=? AND subject_id=? AND version=?", (status, attempts, failure.code, int(not failure.permanent), self.clock() + cooldown, self.clock(), *key, row["version"]))
            return self.status()

    def status(self):
        with self.lock:
            counts = {r[0]: r[1] for r in self.conn.execute("SELECT status,COUNT(*) FROM jobs GROUP BY status")}
            meta = {r[0]: json.loads(r[1]) for r in self.conn.execute("SELECT key,value FROM meta WHERE key IN ('last_scan_at','last_error')")}
        return {"total": sum(counts.values()), "counts": counts, "scan_interval_seconds": SCAN_SECONDS, **meta}

    def run(self, stop=None):
        stop = stop or self.stop
        next_scan = 0
        while not stop.is_set():
            try:
                if self.clock() >= next_scan:
                    self.scan()
                    next_scan = self.clock() + SCAN_SECONDS
                self.flush(stop=stop)
            except Exception:
                with self.lock, self.conn:
                    self._meta("last_error", "avatar_worker_unavailable")
                next_scan = self.clock() + 30
            stop.wait(1)

    def close(self):
        self.stop.set()
        with self.work_lock, self.lock:
            self.conn.close()
            os.close(self.lock_fd)
