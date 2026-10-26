"""Cache-only voice/profile enrichment, separate from message collection watermarks.

Only the selected account's authenticated local databases are read.  Blobs are
content addressed in the existing private archive; no client UI, network,
credential acquisition, decoder, or transcription action is performed here.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import io
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from PIL import Image

from chatlog_keeper import wechat_db
from chatlog_keeper.archive_media import _blob_path, _private_directory, _read_blob
from chatlog_keeper.core._secrets import private_binary_writer, read_secret_text

_MEDIA = re.compile(r"media_[0-9]+\.db\Z")
MAX_VOICE_BYTES = 8 * 1024 * 1024
MAX_AVATAR_BYTES = 4 * 1024 * 1024
MAX_AVATAR_PIXELS = 4_000_000
MAX_SOURCE_BYTES = 256 * 1024 * 1024
MAX_ROWS = 500
MAX_CYCLE_BLOB_BYTES = 32 * 1024 * 1024
MAX_PENDING = 500


class SourceUnavailable(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def database_paths(account_dir):
    """Enumerate only the two validated auxiliary source families."""
    account_dir = Path(account_dir)
    message = account_dir / "db_storage/message"
    paths = []
    if message.is_dir() and not message.is_symlink():
        paths.extend(p for p in message.iterdir() if _MEDIA.fullmatch(p.name) and p.is_file() and not p.is_symlink())
    head = account_dir / "db_storage/head_image/head_image.db"
    if head.is_file() and not head.is_symlink():
        paths.append(head)
    return sorted(paths)


def _subject(value):
    return isinstance(value, str) and 0 < len(value) <= 512 and not any(ord(c) < 32 for c in value)


class P2Sources:
    """One bounded enrichment pass after all current message shards are saved."""

    def __init__(self, collector):
        self.collector = collector
        self.store = collector.store
        self._local_state = {}

    def _meta_key(self, path):
        relative = str(Path(path).relative_to(self.collector.account_dir))
        return "p2_aux:" + hashlib.sha256((self.collector.account_id + "\0" + relative).encode()).hexdigest()

    def _get_state(self, path):
        key = self._meta_key(path)
        getter = getattr(self.store, "get_meta", None)
        value = getter(key, {}) if callable(getter) else self._local_state.get(key, {})
        return dict(value) if isinstance(value, dict) else {}

    def _save_state(self, path, value):
        key = self._meta_key(path)
        setter = getattr(self.store, "set_meta", None)
        if callable(setter):
            setter(key, value)
        else:
            self._local_state[key] = value

    def _put(self, payload):
        digest = hashlib.sha256(payload).hexdigest()
        relative = f"raw/{digest[:2]}/{digest}.bin"
        root = Path(self.store.media_dir)
        target = _blob_path(root, relative)
        _private_directory(target.parent.parent)
        _private_directory(target.parent)
        if target.exists():
            try:
                if _read_blob(root, relative, digest, len(payload)) == payload:
                    return relative, digest
            except Exception:
                pass
        with private_binary_writer(target, secure_parent=True) as out:
            out.write(payload)
        return relative, digest

    def _key(self, reader, database):
        """Try only cached selected-account material and HMAC-check this DB."""
        account = self.collector.account_id
        path = Path(self.store.state_dir) / "secrets/wechat_accounts" / (hashlib.sha256(account.encode()).hexdigest() + ".key")
        candidates = []
        text = read_secret_text(path)
        if text:
            parsed = wechat_db._parse_cached_wechat_key_text(text)
            if parsed:
                candidates.append(parsed)
        candidates.extend(getattr(reader, "enc_keys", {}).values())
        candidates.append(getattr(reader, "enc_key", None))
        first = wechat_db._read_stable_page1(database)
        if first is None:
            raise SourceUnavailable("auxiliary_source_busy")
        seen = set()
        for value in candidates:
            if not isinstance(value, (bytes, bytearray)) or len(value) != 32 or bytes(value) in seen:
                continue
            candidate = bytes(value)
            seen.add(candidate)
            if wechat_db._verify_key_v4(candidate, first):
                return candidate
        raise SourceUnavailable("cached_auxiliary_key_unavailable")

    def _safe_source(self, path):
        try:
            relative = path.relative_to(self.collector.account_dir)
            for depth in range(1, len(relative.parts) + 1):
                if (self.collector.account_dir / Path(*relative.parts[:depth])).is_symlink():
                    raise SourceUnavailable("unsafe_auxiliary_source")
            value = path.lstat()
            if not stat.S_ISREG(value.st_mode) or value.st_uid != os.geteuid():
                raise SourceUnavailable("unsafe_auxiliary_source")
            total = value.st_size
            for suffix in ("-wal",):
                sidecar = path.with_name(path.name + suffix)
                if sidecar.exists():
                    info = sidecar.lstat()
                    if sidecar.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
                        raise SourceUnavailable("unsafe_auxiliary_source")
                    total += info.st_size
            if total > MAX_SOURCE_BYTES:
                raise SourceUnavailable("auxiliary_snapshot_size_limit")
        except SourceUnavailable:
            raise
        except OSError:
            raise SourceUnavailable("auxiliary_source_unavailable") from None

    def _pending(self, state, summary, deadline):
        remaining = dict(state.get("pending") or {})
        for row, record in list((state.get("pending") or {}).items())[:MAX_PENDING]:
            if time.monotonic() >= deadline:
                break
            result = self.store.upsert_voice(self.collector.account_id, record["group_id"], record["server_id"], record["blob"], record["source"])
            if result.get("matched"):
                summary["voices_linked"] += 1
                remaining.pop(row, None)
        state["pending"] = remaining

    def _voice(self, connection, path, state, summary, deadline):
        tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"VoiceInfo", "Name2Id"} <= tables:
            raise SourceUnavailable("unsupported_voice_source_schema")
        columns = {r[1] for r in connection.execute('PRAGMA table_info("VoiceInfo")')}
        if not {"chat_name_id", "create_time", "local_id", "svr_id", "voice_data", "data_index"} <= columns:
            raise SourceUnavailable("unsupported_voice_source_schema")
        name_columns = {r[1] for r in connection.execute('PRAGMA table_info("Name2Id")')}
        if "user_name" not in name_columns:
            raise SourceUnavailable("unsupported_voice_source_schema")
        maximum = connection.execute("SELECT COALESCE(MAX(rowid),0) FROM VoiceInfo").fetchone()[0]
        ambiguous = {(row[0], str(row[1])) for row in connection.execute(
            "SELECT n.user_name,v.svr_id FROM VoiceInfo v JOIN Name2Id n ON n.rowid=v.chat_name_id "
            "WHERE n.user_name LIKE '%@chatroom' GROUP BY n.user_name,v.svr_id HAVING COUNT(*)>1")}
        after = int(state.get("after") or 0)
        if maximum < after:
            after = 0
        # Save newly appended audio first; the lower scan remains independently
        # resumable so audio already present before the first run is not skipped.
        previous_maximum = int(state.get("maximum") or 0)
        if previous_maximum > maximum:
            previous_maximum = 0
        order = "CASE WHEN v.rowid>? THEN 0 ELSE 1 END,v.rowid"
        sql = ("SELECT v.rowid,n.user_name,v.create_time,v.local_id,v.svr_id,"
               "CASE WHEN length(v.voice_data)<=? THEN v.voice_data ELSE NULL END,"
               "length(v.voice_data),v.data_index FROM VoiceInfo v "
               "LEFT JOIN Name2Id n ON n.rowid=v.chat_name_id "
               "WHERE v.rowid>? OR v.rowid>? ORDER BY " + order + " LIMIT ?")
        rows = connection.execute(sql, (MAX_VOICE_BYTES, previous_maximum, after, previous_maximum, MAX_ROWS))
        processed_ids, scan_ids = [], []
        complete = True
        pending = dict(state.get("pending") or {})
        state["pending"] = pending
        for row_id, group_id, created, local_id, server_id, payload, size, data_index in rows:
            if time.monotonic() >= deadline or self._cycle_blob_bytes >= MAX_CYCLE_BLOB_BYTES:
                complete = False
                break
            processed_ids.append(row_id)
            if row_id <= previous_maximum:
                scan_ids.append(row_id)
            summary["voice_rows_checked"] += 1
            if not _subject(group_id) or not group_id.endswith("@chatroom") or server_id in (None, 0, "", "0"):
                continue
            if not size:
                summary["voice_empty"] += 1
                continue
            if payload is None or size > MAX_VOICE_BYTES:
                summary["voice_over_limit"] += 1
                continue
            payload = bytes(payload)
            if self._cycle_blob_bytes + len(payload) > MAX_CYCLE_BLOB_BYTES:
                complete = False
                processed_ids.pop()
                if row_id in scan_ids:
                    scan_ids.remove(row_id)
                break
            relative, digest = self._put(payload)
            self._cycle_blob_bytes += len(payload)
            silk = payload.startswith(b"#!SILK_V3") or payload.startswith(b"\x02#!SILK_V3")
            blob = {"raw_path": relative, "raw_sha256": digest, "raw_size": len(payload),
                    "codec": "silk_v3" if silk else "unknown",
                    "mime": "audio/silk" if silk else "application/octet-stream",
                    "format_status": "recognized" if silk else "unrecognized_voice_header"}
            source = {"database": str(path.relative_to(self.collector.account_dir)), "table": "VoiceInfo",
                      "row_id": row_id, "local_id": local_id, "create_time": created,
                      "data_index": data_index, "association_evidence": "account_group_server_id"}
            if (group_id, str(server_id)) in ambiguous:
                unresolved = state.setdefault("ambiguous_audio", {})
                if len(unresolved) < MAX_PENDING or str(row_id) in unresolved:
                    unresolved[str(row_id)] = {"group_id": group_id, "server_id": str(server_id),
                                               "blob": blob, "source": source}
                else:
                    raise SourceUnavailable("voice_ambiguous_link_limit")
                summary["voice_ambiguous"] += 1
                continue
            result = self.store.upsert_voice(self.collector.account_id, group_id, str(server_id), blob, source)
            if result.get("matched"):
                summary["voices_linked"] += 1
                pending.pop(str(row_id), None)
            elif len(pending) < MAX_PENDING or str(row_id) in pending:
                pending[str(row_id)] = {"group_id": group_id, "server_id": str(server_id), "blob": blob, "source": source}
            else:
                raise SourceUnavailable("voice_pending_link_limit")
            if not silk:
                summary["voice_unknown_format"] += 1
        if len(processed_ids) >= MAX_ROWS:
            complete = False
        # Advancing to maximum is safe only after the complete selected set was
        # handled. An interrupted pass retries its unvisited rows next cycle.
        if complete:
            state.update(after=maximum, maximum=maximum, complete=True)
        elif processed_ids:
            new = [value for value in processed_ids if value > previous_maximum]
            if new:
                state["maximum"] = max(new)
            if scan_ids:
                state["after"] = max(scan_ids)
            state["complete"] = False
        state["pending"] = pending

    def _profiles(self, connection, path, state, contact_names, summary, deadline):
        tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "head_image" not in tables:
            raise SourceUnavailable("unsupported_avatar_source_schema")
        columns = {r[1] for r in connection.execute('PRAGMA table_info("head_image")')}
        if not {"username", "md5", "image_buffer", "update_time"} <= columns:
            raise SourceUnavailable("unsupported_avatar_source_schema")
        after = int(state.get("after") or 0)
        rows = connection.execute("SELECT rowid,username,md5,CASE WHEN length(image_buffer)<=? THEN image_buffer ELSE NULL END,update_time FROM head_image WHERE rowid>? ORDER BY rowid LIMIT ?", (MAX_AVATAR_BYTES, after, MAX_ROWS))
        profiles, checked, last_row = [], 0, after
        interrupted = False
        for row_id, subject, source_md5, body, updated in rows:
            if time.monotonic() >= deadline or self._cycle_blob_bytes >= MAX_CYCLE_BLOB_BYTES:
                interrupted = True
                break
            if body and self._cycle_blob_bytes + len(body) > MAX_CYCLE_BLOB_BYTES:
                interrupted = True
                break
            last_row = row_id
            checked += 1
            summary["avatar_rows_checked"] += 1
            if not _subject(subject) or subject not in contact_names:
                summary["avatar_unmapped"] += 1
                continue
            if not body:
                summary["avatar_invalid"] += 1
                continue
            body = bytes(body)
            try:
                with Image.open(io.BytesIO(body)) as image:
                    fmt, width, height = image.format, *image.size
                    if fmt not in {"PNG", "JPEG"} or width < 1 or height < 1 or width * height > MAX_AVATAR_PIXELS:
                        raise ValueError("invalid_avatar")
                    image.verify()
                with Image.open(io.BytesIO(body)) as image:
                    image.load()
            except (OSError, ValueError, Image.DecompressionBombError):
                summary["avatar_invalid"] += 1
                continue
            relative, digest = self._put(body)
            self._cycle_blob_bytes += len(body)
            profile = {"username": subject, "display_name": contact_names.get(subject, subject)}
            avatar = {"relative_path": relative, "sha256": digest, "mime": Image.MIME[fmt],
                      "width": width, "height": height, "source_md5": str(source_md5 or ""),
                      "observed_at": updated, "source": {"database": str(path.relative_to(self.collector.account_dir)),
                                                             "table": "head_image", "row_id": row_id}}
            profiles.append({"subject_id": subject, "profile": profile, "avatar": avatar})
        if profiles:
            result = self.store.upsert_profiles(self.collector.account_id, profiles)
            summary["profiles_saved"] += int(result.get("profiles", len(profiles)))
            summary["avatars_saved"] += int(result.get("avatars", len(profiles)))
        state["after"] = last_row
        state["complete"] = not interrupted and checked < MAX_ROWS

    def collect(self, reader, contact_names, *, contacts_changed=False, time_budget=5.0):
        deadline = time.monotonic() + time_budget
        summary = {"voice_rows_checked": 0, "voices_linked": 0, "voice_empty": 0,
                   "voice_over_limit": 0, "voice_unknown_format": 0, "voice_ambiguous": 0, "avatar_rows_checked": 0,
                   "avatar_unmapped": 0, "avatar_invalid": 0, "profiles_saved": 0,
                   "avatars_saved": 0, "sources_checked": 0, "sources_unchanged": 0,
                   "errors": [], "budget_exhausted": False}
        self._cycle_blob_bytes = 0
        current = database_paths(self.collector.account_dir)
        manifest_key = "p2_aux_manifest:" + hashlib.sha256(self.collector.account_id.encode()).hexdigest()
        getter, setter = getattr(self.store, "get_meta", None), getattr(self.store, "set_meta", None)
        manifest = getter(manifest_key, []) if callable(getter) else self._local_state.get(manifest_key, [])
        known = {str(path.relative_to(self.collector.account_dir)) for path in current}
        for relative in manifest if isinstance(manifest, list) else []:
            if (isinstance(relative, str) and
                    (relative == "db_storage/head_image/head_image.db" or
                     re.fullmatch(r"db_storage/message/media_[0-9]+\.db", relative))):
                known.add(relative)
        if callable(setter):
            setter(manifest_key, sorted(known))
        else:
            self._local_state[manifest_key] = sorted(known)
        for path in sorted(self.collector.account_dir / relative for relative in known):
            if time.monotonic() >= deadline:
                summary["budget_exhausted"] = True
                break
            state = self._get_state(path)
            try:
                self._pending(state, summary, deadline)
                if time.monotonic() >= deadline:
                    self._save_state(path, state)
                    summary["budget_exhausted"] = True
                    break
                self._safe_source(path)
                fingerprint = self.collector._fingerprint(path)
                is_avatar = path.name == "head_image.db"
                changed = state.get("fingerprint") != fingerprint
                if changed or (is_avatar and contacts_changed):
                    state.update(after=0, complete=False, fingerprint=fingerprint)
                if not changed and state.get("complete") and not (is_avatar and contacts_changed):
                    self._save_state(path, state)
                    summary["sources_unchanged"] += 1
                    self.collector._discard_snapshot(path)
                    continue
                key = self._key(reader, path)
                snapshot = self.collector._get_snapshot(path, key, "p2:" + fingerprint)
                with closing(sqlite3.connect(snapshot.as_uri() + "?mode=ro", uri=True)) as connection:
                    connection.execute("PRAGMA query_only=ON")
                    if is_avatar:
                        self._profiles(connection, path, state, contact_names, summary, deadline)
                    else:
                        self._voice(connection, path, state, summary, deadline)
                state.update(last_success_at=time.time(), error=None)
                self._save_state(path, state)
                summary["sources_checked"] += 1
                if state.get("complete"):
                    self.collector._discard_snapshot(path)
            except Exception as exc:
                code = exc.code if isinstance(exc, SourceUnavailable) else "auxiliary_source_failed"
                state["error"] = code
                self._save_state(path, state)
                summary["errors"].append({"source": path.name, "error": code})
                self.collector._discard_snapshot(path)
        return summary
