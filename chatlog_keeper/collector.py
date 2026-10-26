"""One cache-only Linux WeChat collector with transactional archive progress."""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import signal
import sqlite3
import threading
import time

from chatlog_keeper import wechat_db
from chatlog_keeper.core._private_temp import (
    cleanup_private_temp_dir, create_private_temp_dir, scavenge_private_temp_dirs,
)
from chatlog_keeper.core._snapshot import _family_signature
from chatlog_keeper.message_model import parse_record, source_row_hash

LOG = logging.getLogger(__name__)
_TABLE = re.compile(r"Msg_[0-9a-fA-F]{32}\Z")
_DAY = 86400
_MAX_LIVE_PAGES = 10
_HISTORY_BATCH = 500


class CollectorError(RuntimeError):
    """A short, path-free collector state."""


@dataclass
class _Snapshot:
    fingerprint: str
    directory: Path
    database: Path


class Collector:
    def __init__(self, store, data_root=None, account=None, poll_interval=30,
                 page_size=500, after_cycle=None, health_monitor=None):
        if not 1 <= int(page_size) <= 1000 or float(poll_interval) < 1:
            raise ValueError("invalid collector limits")
        self.store = store
        self.data_root = Path(data_root).expanduser() if data_root else None
        self.account_id = str(account) if account else None
        self.account_dir = None
        self.poll_interval = float(poll_interval)
        self.page_size = int(page_size)
        self.after_cycle = after_cycle
        self.health_monitor = health_monitor
        self._snapshots = {}
        self._lock_fd = None
        self._status_owned = False
        self._contacts_signature = None
        self._contact_names = {}
        self._metadata_budget = 0
        self._p2_sources = None
        self._native_worker = None

    @contextmanager
    def _single_instance(self):
        if self._lock_fd is not None:
            yield
            return
        directory = Path(self.store.state_dir)
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(directory / "collector.lock", os.O_CREAT | os.O_RDWR |
                     os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(fd, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CollectorError("collector_already_running") from None
            self._lock_fd = fd
            yield
        finally:
            self._lock_fd = None
            os.close(fd)

    def _reader(self):
        root = self.data_root or wechat_db.find_weixin_data_root()
        if root is None:
            raise CollectorError("wechat_data_not_found")
        accounts = wechat_db.find_wxid_dirs(root)
        selected = [p for p in accounts if self.account_id is None or p.name == self.account_id]
        if not selected:
            raise CollectorError("account_not_found")
        if len(selected) != 1:
            raise CollectorError("multiple_accounts_require_explicit_account")
        self.data_root = Path(root)
        self.account_dir = selected[0]
        self.account_id = self.account_dir.name
        reader = wechat_db.WeChatDBReader(root, self.account_id, allow_live_key_extract=False)
        if not reader.initialize():
            raise CollectorError("wechat_reader_unavailable")
        return reader

    def _names(self, reader):
        contact = self.account_dir / "db_storage/contact/contact.db"
        signature = _family_signature(contact)
        if signature != self._contacts_signature:
            try:
                resolver = reader._load_contacts()
                names = dict(resolver._wxid_to_display)
                directory = getattr(resolver, "directory_snapshot", None)
                update_directory = getattr(self.store, "update_group_directory", None)
                if names and callable(directory) and callable(update_directory):
                    update_directory(self.account_id, directory())
            except Exception:
                names = {}
            # Retry a temporarily unavailable contact DB on the following cycle.
            if names:
                self._contact_names = names
                self._contacts_signature = signature
        return self._contact_names

    def _get_snapshot(self, database, key, fingerprint):
        previous = self._snapshots.get(database)
        if previous is not None and previous.fingerprint == fingerprint:
            return previous.database
        directory = create_private_temp_dir("wechat_archive_")
        plain = directory / "snapshot.db"
        try:
            if not wechat_db._decrypt_db_v4(database, key, plain):
                raise CollectorError("source_snapshot_or_decryption_failed")
            plain.chmod(0o600)
        except BaseException:
            cleanup_private_temp_dir(directory)
            raise
        if previous is not None:
            try:
                cleanup_private_temp_dir(previous.directory)
            except BaseException:
                cleanup_private_temp_dir(directory)
                raise
        self._snapshots[database] = _Snapshot(fingerprint, directory, plain)
        return plain

    def _discard_snapshot(self, database):
        cached = self._snapshots.get(database)
        if cached is not None:
            cleanup_private_temp_dir(cached.directory)
            self._snapshots.pop(database, None)

    def _known_streams(self, source):
        """Read only archive routing metadata, never a private source payload."""
        rows = self.store.conn.execute(
            "SELECT table_name,group_id FROM streams WHERE source_ref=? AND generation=?",
            (source["source_ref"], source["generation"]),
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    @staticmethod
    def _identity(database):
        info = database.stat()
        with database.open("rb") as handle:
            salt = handle.read(16)
        if len(salt) != 16:
            raise CollectorError("source_database_busy")
        return {"device": info.st_dev, "inode": info.st_ino, "salt": salt.hex()}

    @staticmethod
    def _fingerprint(database):
        signature = _family_signature(database)
        return hashlib.sha256(json.dumps(signature, separators=(",", ":")).encode()).hexdigest()

    @staticmethod
    def _groups(connection, contact_names):
        name_map = wechat_db._load_name_map(connection)
        known = {str(v) for v in name_map.values() if v}
        known.update(contact_names)
        lookup = {"Msg_" + hashlib.md5(name.encode()).hexdigest(): name for name in known}
        tables = [r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
                  if _TABLE.fullmatch(r[0])]
        groups, unresolved = [], 0
        for table in tables:
            native = lookup.get(table) or lookup.get("Msg_" + table[4:].lower())
            if native is None:
                unresolved += 1
            elif native.endswith("@chatroom"):
                groups.append((table, native, contact_names.get(native, native)))
        return groups, name_map, unresolved

    def _rows(self, connection, table, group_id, group_name, name_map, after,
              end=None, limit=None, exact=False, row_ids=None):
        if not _TABLE.fullmatch(table):
            raise CollectorError("unsupported_message_table")
        columns = {r[1] for r in connection.execute(f'PRAGMA table_info("{table}")')}
        required = {"local_type", "create_time", "real_sender_id", "message_content", "server_id"}
        if not required <= columns:
            raise CollectorError("unsupported_message_schema")
        packed = "packed_info_data" if "packed_info_data" in columns else "NULL"
        source = next((c for c in ("source", "message_source", "msgsource") if c in columns), None)
        source_sql = f'"{source}"' if source else "NULL"
        compression = '"WCDB_CT_source"' if "WCDB_CT_source" in columns else "NULL"
        if row_ids is not None:
            row_ids = list(dict.fromkeys(int(value) for value in row_ids))
            if not row_ids:
                return []
            if len(row_ids) > _HISTORY_BATCH:
                raise CollectorError("invalid_metadata_batch")
            parameters = row_ids[:]
            condition = "rowid IN (" + ",".join("?" for _ in row_ids) + ")"
        else:
            parameters = [after]
            condition = "rowid = ?" if exact else "rowid > ?"
        if end is not None:
            condition += " AND rowid <= ?"
            parameters.append(end)
        parameters.append(limit or self.page_size)
        values = connection.execute(
            f'SELECT rowid, local_type, create_time, real_sender_id, message_content, '
            f'server_id, {packed}, {source_sql}, {compression} FROM "{table}" WHERE {condition} '
            'ORDER BY rowid ASC LIMIT ?', parameters,
        ).fetchall()
        rows = []
        for (row_id, kind, timestamp, numeric_sender, content, server_id,
             packed_info, msgsource, source_compression) in values:
            sender = name_map.get(numeric_sender)
            if not sender:
                # Shard-scoped unmapped sender IDs must not be confused with real wxids.
                sender = f"unresolved:{self._source_ref}:{numeric_sender}"
            record = {
                "account_id": self.account_id, "group_id": group_id, "group_name": group_name,
                "row_id": int(row_id), "local_type": int(kind or 0),
                "create_time": int(timestamp or 0), "real_sender_id": numeric_sender,
                "sender_id": str(sender), "sender_name": self._contact_names.get(sender, ""),
                "server_id": str(server_id) if server_id not in (None, 0, "") else "",
                "message_content": content, "packed_info_data": packed_info,
                "message_source": msgsource,
                "source_compression": source_compression,
                "source_metadata_checked": True,
            }
            record["parsed"] = parse_record(record)
            rows.append(record)
        return rows

    def _validate_streams(self, connection, groups, name_map, source):
        reconciles = []
        for table, group_id, name in groups:
            maximum = int(connection.execute(f'SELECT COALESCE(MAX(rowid),0) FROM "{table}"').fetchone()[0])
            stream = self.store.get_stream(source["source_ref"], source["generation"], table, group_id, maximum)
            if maximum < max(int(stream.get("live_rowid", 0)), int(stream.get("backfill_rowid", 0))):
                return "source_rowid_regressed", []
            boundary = int(stream.get("boundary_rowid") or 0)
            expected = stream.get("boundary_hash")
            if boundary and expected:
                rows = self._rows(connection, table, group_id, name, name_map, boundary,
                                  limit=1, exact=True)
                if not rows:
                    return "source_boundary_missing", []
                if source_row_hash(rows[0]) != expected:
                    previous = self.store.get_origin_info(
                        source["source_ref"], source["generation"], table, boundary)
                    current = rows[0]
                    if previous is None:
                        return "source_boundary_unverifiable", []
                    # P0 did not read Linux's `source` column. Adding that
                    # metadata must not trigger a whole-table reconcile or a
                    # new source generation for an otherwise unchanged row.
                    if previous.get("message_source") is None and current.get("message_source") is not None:
                        legacy = dict(current, message_source=None)
                        if source_row_hash(legacy) == expected:
                            continue
                    old_sid = str(previous.get("server_id") or "")
                    new_sid = str(current.get("server_id") or "")
                    reused = bool(old_sid and old_sid != new_sid)
                    for field in ("create_time", "local_type", "real_sender_id"):
                        if str(previous.get(field)) != str(current.get(field)):
                            reused = True
                    if reused:
                        return "source_rowid_reused", []
                    # ACKs, edits and later attachment metadata update one origin.
                    # Reconcile in the same generation, preserving its message_ref.
                    if not stream.get("reconcile_active"):
                        reconciles.append((table, group_id, maximum))
        return None, reconciles

    def _metadata_needs_work(self, source):
        check = getattr(self.store, "metadata_backfill_needed", None)
        return bool(callable(check) and check(source["source_ref"], source["generation"]))

    def _backfill_metadata(self, connection, groups, name_map, source):
        """Refresh old origins from source without moving message watermarks.

        Only known origins are requested, so source gaps remain explicit and a
        reset database cannot create new historical messages through this path.
        A shared budget bounds this work across all shards in a collection cycle.
        """
        pending = getattr(self.store, "metadata_backfill_rows", None)
        if self._metadata_budget <= 0 or not callable(pending):
            return 0
        refreshed = 0
        for table, group_id, name in groups:
            if self._metadata_budget <= 0:
                break
            requested = pending(source["source_ref"], source["generation"], table,
                                limit=self._metadata_budget)
            row_ids = [int(item["row_id"] if isinstance(item, dict) else item)
                       for item in requested]
            if not row_ids:
                continue
            rows = self._rows(connection, table, group_id, name, name_map, 0,
                              limit=len(row_ids), row_ids=row_ids)
            self.store.backfill_source_metadata(source["source_ref"], source["generation"],
                                                table, rows)
            missing = set(row_ids) - {row["row_id"] for row in rows}
            if missing:
                self.store.mark_metadata_checked(source["source_ref"], source["generation"],
                                                 table, sorted(missing))
            self._metadata_budget -= len(row_ids)
            refreshed += len(rows)
        return refreshed

    def _process_table(self, connection, table, group_id, name, name_map, source, now):
        maximum = int(connection.execute(f'SELECT COALESCE(MAX(rowid),0) FROM "{table}"').fetchone()[0])
        ref, generation = source["source_ref"], source["generation"]
        stream = self.store.get_stream(ref, generation, table, group_id, maximum)
        count = 0
        live = int(stream["live_rowid"])
        for _ in range(_MAX_LIVE_PAGES):
            rows = self._rows(connection, table, group_id, name, name_map, live)
            if not rows:
                break
            live = rows[-1]["row_id"]
            self.store.commit_page(ref, generation, table, group_id, rows,
                                   mode="live", checkpoint=live, complete=len(rows) < self.page_size)
            count += len(rows)
            if len(rows) < self.page_size:
                break
        if not stream["backfill_done"]:
            rows = self._rows(connection, table, group_id, name, name_map,
                              int(stream["backfill_rowid"]), end=int(stream["backfill_end"]))
            checkpoint = rows[-1]["row_id"] if rows else int(stream["backfill_end"])
            complete = len(rows) < self.page_size or checkpoint >= int(stream["backfill_end"])
            self.store.commit_page(ref, generation, table, group_id, rows,
                                   mode="backfill", checkpoint=checkpoint, complete=complete)
            count += len(rows)
        elif not stream.get("reconcile_active") and now - float(
            stream.get("last_reconciled_at", stream.get("last_reconciled")) or 0
        ) >= _DAY:
            self.store.start_reconcile(ref, generation, table, group_id, maximum)
            stream = self.store.get_stream(ref, generation, table, group_id, maximum)
        if stream.get("reconcile_active"):
            end = int(stream.get("reconcile_end", maximum))
            rows = self._rows(connection, table, group_id, name, name_map,
                              int(stream.get("reconcile_rowid") or 0), end=end)
            checkpoint = rows[-1]["row_id"] if rows else end
            self.store.commit_page(ref, generation, table, group_id, rows,
                                   mode="reconcile", checkpoint=checkpoint,
                                   complete=len(rows) < self.page_size or checkpoint >= end)
            count += len(rows)
        return count

    def _collect(self):
        started = time.time()
        summary = {"sources": 0, "readable_sources": 0, "groups": 0,
                   "rows_processed": 0, "metadata_rows_refreshed": 0,
                   "historical_rows_reparsed": 0, "unresolved_tables": 0, "errors": []}
        self._metadata_budget = _HISTORY_BATCH
        self.store.set_collector_status(running=True, pid=os.getpid(), last_started_at=started,
                                        poll_interval=self.poll_interval)
        self._status_owned = True
        try:
            reader = self._reader()
        except BaseException as exc:
            # A missing account/data root can fail before individual shard discovery.
            # No previously decrypted whole-database copy remains useful then.
            for old in list(self._snapshots):
                self._discard_snapshot(old)
            if self.account_id and isinstance(exc, CollectorError) and str(exc) in {
                "account_not_found", "wechat_data_not_found",
            }:
                self.store.mark_missing_sources(self.account_id, [])
            raise
        old_contacts = self._contacts_signature
        contact_names = self._names(reader)
        contacts_changed = old_contacts is not None and old_contacts != self._contacts_signature
        initial_contacts = old_contacts is None and self._contacts_signature is not None
        self.store.upsert_groups(self.account_id, [
            {"id": group, "name": name} for group, name in contact_names.items()
            if group.endswith("@chatroom")
        ])
        databases = wechat_db.find_msg_databases(self.account_dir)
        auxiliary = []
        p2_enabled = callable(getattr(self.store, "upsert_voice", None)) and callable(getattr(self.store, "upsert_profiles", None))
        if p2_enabled:
            from chatlog_keeper.p2_sources import database_paths
            auxiliary = database_paths(self.account_dir)
        for old in set(self._snapshots) - set(databases) - set(auxiliary):
            self._discard_snapshot(old)
        seen = []
        metadata_tasks = []
        for database in databases:
            source = None
            try:
                identity = self._identity(database)
                fingerprint = self._fingerprint(database)
                source = self.store.register_source(self.account_id,
                    str(database.relative_to(self.account_dir)), identity, fingerprint)
                ref = source["source_ref"]
                self._source_ref = ref
                seen.append(ref)
                summary["sources"] += 1
                key = reader.enc_keys.get(database)
                if key is None:
                    self._discard_snapshot(database)
                    raise CollectorError("cached_database_key_unavailable")
                if (not source["changed"] and not contacts_changed
                        and source.get("available", True)
                        and not (initial_contacts and source.get("error") == "unresolved_conversation_tables")
                        and not self.store.source_needs_work(ref, source["generation"], now=started)
                        and not self._metadata_needs_work(source)):
                    # No plaintext DB is needed for a fully archived, unchanged shard.
                    self._discard_snapshot(database)
                    issue = source.get("error")
                    if issue == "unresolved_conversation_tables":
                        summary["unresolved_tables"] += 1
                    summary["readable_sources"] += 1
                    continue
                snapshot_key = fingerprint + ":" + str(source["generation"]) + ":" + hashlib.sha256(
                    json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                snapshot = self._get_snapshot(database, key, snapshot_key)
                with closing(sqlite3.connect(snapshot.as_uri() + "?mode=ro", uri=True)) as connection:
                    groups, names, unresolved = self._groups(connection, contact_names)
                    existing_tables = {r[0] for r in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'") if _TABLE.fullmatch(r[0])}
                    known_streams = self._known_streams(source)
                    tables_removed = bool(set(known_streams) - existing_tables)
                    # A temporarily missing directory entry does not erase a mapping
                    # already verified for this exact source generation and table hash.
                    mapped = {table for table, _, _ in groups}
                    for table, native in known_streams.items():
                        expected = "Msg_" + hashlib.md5(native.encode()).hexdigest()
                        if (table in existing_tables and table not in mapped
                                and native.endswith("@chatroom") and expected.lower() == table.lower()):
                            groups.append((table, native, contact_names.get(native, native)))
                            unresolved = max(0, unresolved - 1)
                    summary["unresolved_tables"] += unresolved
                    self.store.upsert_groups(self.account_id,
                        [{"id": group, "name": name} for _, group, name in groups])
                    reset_reason, reconciles = (
                        ("source_tables_removed", []) if tables_removed else
                        self._validate_streams(connection, groups, names, source))
                    if reset_reason:
                        source = self.store.rotate_source(ref, reset_reason)
                    else:
                        for table, group, maximum in reconciles:
                            self.store.start_reconcile(ref, source["generation"], table, group, maximum)
                    rotated = bool(reset_reason)
                    while True:
                        try:
                            for table, group, name in groups:
                                summary["rows_processed"] += self._process_table(
                                    connection, table, group, name, names, source, started)
                            break
                        except Exception as exc:
                            if getattr(exc, "code", None) != "source_rowid_reused" or rotated:
                                raise
                            # The page transaction was rejected and rolled back.
                            # Retry this stable source copy under a new generation,
                            # keeping old logical messages and their origins intact.
                            source = self.store.rotate_source(ref, "source_rowid_reused")
                            rotated = True
                    summary["groups"] += len(groups)
                    if groups and self._metadata_needs_work(source):
                        metadata_tasks.append((database, snapshot, groups, names, dict(source)))
                self.store.set_source_status(
                    ref, available=True,
                    error="unresolved_conversation_tables" if unresolved else None,
                    fingerprint=fingerprint)
                summary["readable_sources"] += 1
                if not groups or (not self.store.source_needs_work(ref, source["generation"], now=started)
                                  and not self._metadata_needs_work(source)):
                    self._discard_snapshot(database)
            except Exception as exc:
                code = getattr(exc, "code", None) or (
                    str(exc) if isinstance(exc, CollectorError) else type(exc).__name__)
                summary["errors"].append({"source_ref": source["source_ref"] if source else None,
                                           "error": code})
                if source is not None:
                    self.store.set_source_status(source["source_ref"], available=False, error=code)
        self.store.mark_missing_sources(self.account_id, seen)
        # New rows from every shard take priority over the one-time source
        # metadata upgrade. Reopening an existing private snapshot does not
        # decrypt the source again, and at most 500 origins are attempted here.
        for database, snapshot, groups, names, source in metadata_tasks:
            if self._metadata_budget <= 0:
                break
            try:
                with closing(sqlite3.connect(snapshot.as_uri() + "?mode=ro", uri=True)) as connection:
                    summary["metadata_rows_refreshed"] += self._backfill_metadata(
                        connection, groups, names, source)
                if (not self.store.source_needs_work(source["source_ref"], source["generation"], now=started)
                        and not self._metadata_needs_work(source)):
                    self._discard_snapshot(database)
            except Exception as exc:
                code = getattr(exc, "code", None) or (
                    str(exc) if isinstance(exc, CollectorError) else type(exc).__name__)
                summary["errors"].append({"source_ref": source["source_ref"], "error": code})
                self.store.set_source_status(source["source_ref"], available=False, error=code)
        reparse = getattr(self.store, "reparse_batch", None)
        if callable(reparse):
            reparsed = reparse(limit=_HISTORY_BATCH)
            summary["historical_rows_reparsed"] = (
                int(reparsed.get("reparsed", 0)) if isinstance(reparsed, dict) else int(reparsed))
        # Auxiliary cached media enriches existing messages only. Its failures
        # must not erase successful message collection or falsify client health.
        if p2_enabled:
            try:
                from chatlog_keeper.p2_sources import P2Sources
                if self._p2_sources is None:
                    self._p2_sources = P2Sources(self)
                summary["p2_sources"] = self._p2_sources.collect(
                    reader, contact_names, contacts_changed=contacts_changed or initial_contacts)
            except Exception:
                summary["p2_sources"] = {"errors": [{"error": "auxiliary_source_failed"}]}
        elapsed = time.time() - started
        status = dict(
            account_id=self.account_id, data_root=str(self.data_root),
            last_cycle_finished_at=time.time(), last_cycle_at=time.time(),
            last_cycle_seconds=round(elapsed, 3),
            last_error=summary["errors"][0]["error"] if summary["errors"] else
                       "unresolved_conversation_tables" if summary["unresolved_tables"] else None,
            sources_total=summary["sources"], sources_readable=summary["readable_sources"],
            unresolved_tables=summary["unresolved_tables"],
        )
        if "p2_sources" in summary:
            status["p2_sources"] = summary["p2_sources"]
        if not summary["errors"]:
            status["last_success_at"] = time.time()
        self.store.set_collector_status(**status)
        if self.after_cycle is not None:
            self.after_cycle(self, summary)
        return summary

    def _check_client_health(self):
        """Keep client availability separate from readable archival source data.

        A detector/desktop-notification failure must never discard a collected
        page or mask the original database error. Only the lock owner calls this.
        """
        fields = {"last_cycle_checked_at": time.time()}
        health = None
        if self.health_monitor is not None:
            try:
                health = self.health_monitor(self)
                if not isinstance(health, dict) or not isinstance(health.get("state"), str):
                    raise ValueError("invalid client health result")
                fields.update(client_health=health, health_monitor_error=None)
            except Exception as exc:
                health = {"state": "unknown", "reason": "health_monitor_failed",
                          "checked_at": time.time(), "process_running": None,
                          "display_available": None}
                fields.update(client_health=health, health_monitor_error=type(exc).__name__)
                LOG.error("client_health_monitor_failed: %s", type(exc).__name__)
        try:
            self.store.set_collector_status(**fields)
        except Exception as exc:
            LOG.error("client_health_status_write_failed: %s", type(exc).__name__)
        return health

    def _pump_native_transcription(self):
        """Poll an isolated optional UI task; never wait for conversion here."""
        try:
            from chatlog_keeper.native_transcription import NativeTranscriptionWorker
            if self._native_worker is None:
                self._native_worker = NativeTranscriptionWorker()
            self._native_worker.pump(self.store)
        except Exception as exc:
            # Auxiliary work does not mask source errors or receiving evidence.
            LOG.error("native_transcription_worker_failed: %s", type(exc).__name__)

    def collect_once(self):
        with self._single_instance():
            summary = None
            try:
                summary = self._collect()
                return summary
            except Exception as exc:
                try:
                    self.store.set_collector_status(last_error=str(exc) if isinstance(exc, CollectorError)
                                                    else type(exc).__name__)
                except Exception as status_exc:
                    LOG.error("collector_error_status_write_failed: %s", type(status_exc).__name__)
                raise
            finally:
                health = self._check_client_health()
                self._pump_native_transcription()
                if summary is not None and health is not None:
                    summary["client_health"] = health

    def run(self, stop_event=None):
        stop = stop_event or threading.Event()
        old_handlers = {}
        if threading.current_thread() is threading.main_thread():
            for number in (signal.SIGTERM, signal.SIGINT):
                old_handlers[number] = signal.signal(number, lambda *_: stop.set())
        try:
            with self._single_instance():
                scavenge_private_temp_dirs(("wechat_archive_", "wechat_contacts_"))
                while not stop.is_set():
                    started = time.monotonic()
                    try:
                        self.collect_once()
                    except Exception as exc:
                        LOG.error("collector_cycle_failed: %s", type(exc).__name__)
                    stop.wait(max(0.0, self.poll_interval - (time.monotonic() - started)))
        finally:
            for number, previous in old_handlers.items():
                signal.signal(number, previous)
            self.close()

    def close(self):
        if self._native_worker is not None:
            self._native_worker.close(self.store)
            self._native_worker = None
        for database in list(self._snapshots):
            self._discard_snapshot(database)
        if self._status_owned:
            try:
                with self._single_instance():
                    self.store.set_collector_status(running=False)
            except CollectorError:
                # Another collector may have started since this instance stopped.
                pass
            self._status_owned = False
