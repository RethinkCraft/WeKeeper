"""One cache-only Linux WeChat collector with transactional archive progress."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
import fcntl
import hashlib
import inspect
import json
import logging
import math
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
from chatlog_keeper.core._native_watch import NativeFileWatcher
from chatlog_keeper.core._store_batch import source_batch
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
    plan: dict = field(default_factory=dict)


class Collector:
    def __init__(self, store, data_root=None, account=None, poll_interval=30,
                 page_size=500, after_cycle=None, health_monitor=None, bot_mode=False,
                 priority_groups=()):
        if (not 1 <= int(page_size) <= 1000 or not math.isfinite(float(poll_interval))
                or float(poll_interval) < .1):
            raise ValueError("invalid collector limits")
        self.store = store
        self.data_root = Path(data_root).expanduser() if data_root else None
        self.account_id = str(account) if account else None
        self.account_dir = None
        self.poll_interval = float(poll_interval)
        self.page_size = int(page_size)
        self.after_cycle = after_cycle
        self.health_monitor = health_monitor
        self.bot_mode = bool(bot_mode)
        self.priority_groups = tuple(dict.fromkeys(priority_groups))
        if any(not isinstance(group, str) or not group.endswith('@chatroom') for group in self.priority_groups):
            raise ValueError('invalid priority group')
        self._maintenance_cursor = 0
        self._maintenance_pages = 0
        self._snapshots = {}
        self._lock_fd = None
        self._status_owned = False
        self._contacts_signature = None
        self._contact_names = {}
        self._metadata_budget = 0
        self._p2_sources = None
        self._native_worker = None
        self._last_health_at = None
        self._last_health = None
        self._health_pool = None
        self._health_future = None
        self._health_closed = False
        self._fast_signature = None
        self._last_full_scan = None
        self._last_summary = None
        self._last_full_summary = None
        self._cached_reader = None
        self._cached_reader_stamp = None
        self._group_names = {}
        self._status_pending = {}
        self._status_critical = None
        self._status_written_at = None
        self._timings = {}
        self._live_pending = False
        self._priority_shards = set()
        self._watcher = None
        self._native_observed_at = None
        self._native_observed_monotonic = None
        self._live_batches = []
        self._recent_live_batches = []
        self._source_batch_active = False
        self._connection_plans = {}
        self._source_timings = None
        self._source_live_pending = False
        self._live_shards = set()
        self._dirty_shards = None
        self._scan_candidates = None
        self._last_signature_check = None

    @contextmanager
    def _timed(self, name):
        started = time.perf_counter()
        try:
            yield
        finally:
            self._add_timing(name, time.perf_counter() - started)

    def _add_timing(self, name, seconds):
        self._timings[name] = self._timings.get(name, 0) + seconds
        trace = getattr(self, '_source_timings', None)
        if trace is not None:
            trace[name] = trace.get(name, 0) + seconds

    def _set_status(self, **fields):
        if not self.bot_mode:
            self.store.set_collector_status(**fields)
            return
        self._status_pending.update(fields)
        # Cycle timestamps/timings are heartbeat data, not state transitions.
        critical = {key: value for key, value in self._status_pending.items()
                    if not key.startswith('last_') or key == 'last_error'}
        now = time.monotonic()
        if (critical != self._status_critical or self._status_written_at is None
                or now - self._status_written_at >= 1):
            self.store.set_collector_status(**self._status_pending)
            self._status_critical = dict(critical)
            self._status_written_at = now

    def _upsert_groups(self, groups):
        changed = [group for group in groups if not self.bot_mode
                   or self._group_names.get(group['id']) != group['name']]
        if changed:
            with self._timed('group_directory'):
                self.store.upsert_groups(self.account_id, changed)
            for group in changed:
                self._group_names[group['id']] = group['name']

    def _key_cache_stamp(self):
        paths = []
        for name, arguments in (('_wechat_account_key_cache_path', (self.account_id,)),
                                ('_persistent_wechat_key_cache_path', ()),
                                ('_legacy_wechat_key_cache_path', ())):
            resolver = getattr(wechat_db, name, None)
            path = resolver(*arguments) if callable(resolver) else None
            if path is not None:
                paths.append(Path(path))
        result = []
        for path in dict.fromkeys(paths):
            try:
                stat = path.stat()
                identity = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)
            except FileNotFoundError:
                identity = None
            result.append((str(path), identity))
        return tuple(result)

    def _fast_source_signature(self):
        """Cheap change fence; never retain a decrypted snapshot for idle scans."""
        if self.account_dir is None:
            return None
        files = list(wechat_db.find_msg_databases(self.account_dir))
        files.append(self.account_dir / "db_storage/contact/contact.db")
        values = []
        for path in files:
            try:
                info = path.stat()
                identity = (info.st_dev, info.st_ino)
            except FileNotFoundError:
                identity = None
            values.append((str(path), identity, _family_signature(path)))
        return tuple(values), self._key_cache_stamp()

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
        if self._watcher is not None:
            # Install watches before the initial snapshot, so changes during
            # that first scan also cause an immediate follow-up scan.
            self._watcher.configure(self.account_dir)
        stamp = (str(self.account_dir.resolve()),
                 tuple((str(path), tuple(sorted(self._identity(path).items())))
                       for path in wechat_db.find_msg_databases(self.account_dir)),
                 self._key_cache_stamp()) if self.bot_mode else None
        if self.bot_mode and self._cached_reader is not None and stamp == self._cached_reader_stamp:
            return self._cached_reader
        # A changed account/file/key fence cannot be narrowed by a WAL hint.
        self._scan_candidates = None
        self._cached_reader = None
        self._cached_reader_stamp = None
        reader = wechat_db.WeChatDBReader(root, self.account_id, allow_live_key_extract=False)
        if not reader.initialize():
            raise CollectorError("wechat_reader_unavailable")
        if self.bot_mode:
            self._cached_reader, self._cached_reader_stamp = reader, stamp
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
            timings = {}
            with self._timed('snapshot_and_decrypt'):
                decrypt = wechat_db._decrypt_db_v4
                if 'timings' in inspect.signature(decrypt).parameters:
                    valid = decrypt(database, key, plain, timings=timings)
                else:
                    valid = decrypt(database, key, plain)
            for name, elapsed in timings.items():
                self._add_timing(name, elapsed)
            if not valid:
                self._cached_reader = self._cached_reader_stamp = None
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

    @contextmanager
    def _snapshot_connection(self, snapshot):
        """Reuse metadata only for one immutable authenticated private copy."""
        with closing(sqlite3.connect(snapshot.as_uri() + '?mode=ro', uri=True)) as connection:
            item = next((item for item in self._snapshots.values() if item.database == snapshot), None)
            self._connection_plans[connection] = item.plan if item is not None else {}
            try:
                yield connection
            finally:
                self._connection_plans.pop(connection, None)

    def _snapshot_metadata(self, connection):
        # Standalone row-reader tests and enrichment adapters may use their own
        # connection; never cache those mutable databases by accident.
        return getattr(self, '_connection_plans', {}).get(connection)

    def _table_names(self, connection):
        plan = self._snapshot_metadata(connection)
        if plan is None or 'tables' not in plan:
            tables = tuple(r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
                           if _TABLE.fullmatch(r[0]))
            if plan is not None:
                plan['tables'] = tables
            return tables
        return plan['tables']

    def _table_columns(self, connection, table):
        plan = self._snapshot_metadata(connection)
        columns = plan.setdefault('columns', {}) if plan is not None else {}
        if table not in columns:
            columns[table] = frozenset(r[1] for r in connection.execute(f'PRAGMA table_info("{table}")'))
        return columns[table]

    def _table_maximum(self, connection, table):
        plan = self._snapshot_metadata(connection)
        maxima = plan.setdefault('maxima', {}) if plan is not None else {}
        if table not in maxima:
            maxima[table] = int(connection.execute(f'SELECT COALESCE(MAX(rowid),0) FROM "{table}"').fetchone()[0])
        return maxima[table]

    def _table_state(self, connection, table, group, source, read_plan=None):
        key = (source['source_ref'], source['generation'], table)
        if read_plan is None or key not in read_plan:
            maximum = self._table_maximum(connection, table)
            state = (maximum, self.store.get_stream(source['source_ref'], source['generation'], table, group, maximum))
            if read_plan is not None:
                read_plan[key] = state
            return state
        return read_plan[key]

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

    def _groups(self, connection, contact_names):
        plan = self._snapshot_metadata(connection)
        if plan is not None and 'name_map' in plan:
            name_map = plan['name_map']
        else:
            name_map = wechat_db._load_name_map(connection)
            if plan is not None:
                plan['name_map'] = name_map
        known = {str(v) for v in name_map.values() if v}
        known.update(contact_names)
        lookup = {"Msg_" + hashlib.md5(name.encode()).hexdigest(): name for name in known}
        tables = self._table_names(connection)
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
        columns = self._table_columns(connection, table)
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

    def _validate_streams(self, connection, groups, name_map, source, read_plan=None):
        reconciles = []
        for table, group_id, name in groups:
            maximum, stream = self._table_state(connection, table, group_id, source, read_plan)
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

    def _process_table(self, connection, table, group_id, name, name_map, source, now,
                       *, live_only=False, maintenance_only=False, read_plan=None):
        maximum, stream = self._table_state(connection, table, group_id, source, read_plan)
        ref, generation = source["source_ref"], source["generation"]
        count = 0
        live = int(stream["live_rowid"])
        for _ in range(0 if maintenance_only else 1 if self.bot_mode else _MAX_LIVE_PAGES):
            if live >= maximum:
                break  # The immutable snapshot cannot produce another live row.
            rows = self._rows(connection, table, group_id, name, name_map, live)
            if not rows:
                break
            live = rows[-1]["row_id"]
            with self._timed('live_apply' if self._source_batch_active else 'live_commit'):
                commit_started = time.perf_counter()
                self.store.commit_page(ref, generation, table, group_id, rows,
                                       mode="live", checkpoint=live, complete=len(rows) < self.page_size)
            if self.bot_mode:
                self._live_batches.append({
                    'source_ref': ref, 'generation': generation, 'table_name': table,
                    'first_rowid': rows[0]['row_id'], 'last_rowid': rows[-1]['row_id'],
                    'commit_at': None if self._source_batch_active else time.time(),
                    'commit_seconds': 0 if self._source_batch_active else round(time.perf_counter() - commit_started, 6),
                    'page_apply_seconds': round(time.perf_counter() - commit_started, 6),
                    'collection_started_at': self._collection_started_at,
                    # This is when a filesystem hint was observed, not a
                    # claimed native/phone message arrival timestamp.
                    'native_event_observed_at': self._native_observed_at,
                })
            count += len(rows)
            if len(rows) < self.page_size:
                break
        if self.bot_mode and not maintenance_only and live < maximum:
            self._live_pending = True
            self._source_live_pending = True
        if live_only:
            return count
        if not stream["backfill_done"]:
            rows = self._rows(connection, table, group_id, name, name_map,
                              int(stream["backfill_rowid"]), end=int(stream["backfill_end"]))
            checkpoint = rows[-1]["row_id"] if rows else int(stream["backfill_end"])
            complete = len(rows) < self.page_size or checkpoint >= int(stream["backfill_end"])
            self.store.commit_page(ref, generation, table, group_id, rows,
                                   mode="backfill", checkpoint=checkpoint, complete=complete)
            self._maintenance_pages += 1
            count += len(rows)
            if self.bot_mode:
                return count
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
            self._maintenance_pages += 1
            count += len(rows)
        return count

    def _commit_live_source(self, connection, groups, names, source, started,
                            fingerprint, unresolved, tables_removed):
        """One durable real-time source boundary after snapshot decryption."""
        rotate_for_reuse = False
        for attempt in range(2):
            batch_start = len(self._live_batches)
            prior_pending, prior_names = self._live_pending, dict(self._group_names)
            prior_source_pending = self._source_live_pending
            read_plan = {}
            try:
                with source_batch(self.store, enabled=self.bot_mode) as batch:
                    self._source_batch_active = batch.active
                    if rotate_for_reuse:
                        source = self.store.rotate_source(source['source_ref'], 'source_rowid_reused')
                    self._upsert_groups([{'id': group, 'name': name} for _, group, name in groups])
                    with self._timed('boundary_validation'):
                        reset_reason, reconciles = (
                            ('source_tables_removed', []) if tables_removed else
                            self._validate_streams(connection, groups, names, source, read_plan))
                    if reset_reason:
                        source = self.store.rotate_source(source['source_ref'], reset_reason)
                    else:
                        for table, group, maximum in reconciles:
                            self.store.start_reconcile(source['source_ref'], source['generation'], table, group, maximum)
                    count = 0
                    for table, group, name in groups:
                        with self._timed('priority_live_apply' if group in self.priority_groups else 'other_live_apply'):
                            count += self._process_table(connection, table, group, name, names, source,
                                started, live_only=True, read_plan=read_plan)
                    with self._timed('source_status'):
                        self.store.set_source_status(source['source_ref'], available=True,
                            error='unresolved_conversation_tables' if unresolved else None,
                            fingerprint=fingerprint)
                if batch.active:
                    self._add_timing('live_commit', batch.commit_seconds)
                    self._add_timing('archive_writer_wait', batch.writer_wait_seconds)
                    for page in self._live_batches[batch_start:]:
                        # Legacy commit_page returned before the owner fsync.
                        # Only the successful outer COMMIT proves durability.
                        page.update(commit_at=batch.committed_at,
                                    commit_seconds=round(batch.commit_seconds, 6),
                                    shared_source_commit=True)
                return source, count
            except BaseException as exc:
                # Rolled-back rows, cursors and directory changes must not
                # survive in metrics/cache and affect a subsequent retry.
                del self._live_batches[batch_start:]
                self._live_pending, self._group_names = prior_pending, prior_names
                self._source_live_pending = prior_source_pending
                if getattr(exc, 'code', None) == 'source_rowid_reused' and attempt == 0:
                    rotate_for_reuse = True
                    continue
                raise
            finally:
                self._source_batch_active = False
        raise CollectorError('source_live_retry_exhausted')

    def _collect(self):
        started = time.time()
        self._collection_started_at = started
        self._timings = {}
        self._live_batches = []
        if self._native_observed_monotonic is not None:
            self._timings['native_hint_to_collect'] = time.monotonic() - self._native_observed_monotonic
        self._live_pending = False
        maintenance_deadline = time.monotonic() + self.poll_interval
        self._maintenance_pages = 0
        summary = {"sources": 0, "readable_sources": 0, "groups": 0,
                   "rows_processed": 0, "metadata_rows_refreshed": 0,
                   "historical_rows_reparsed": 0, "unresolved_tables": 0, "errors": []}
        self._metadata_budget = _HISTORY_BATCH
        self._set_status(running=True, pid=os.getpid(), last_started_at=started,
                         poll_interval=self.poll_interval, bot_mode=self.bot_mode)
        self._status_owned = True
        try:
            with self._timed('reader'):
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
        with self._timed('contacts'):
            contact_names = self._names(reader)
        contacts_changed = old_contacts is not None and old_contacts != self._contacts_signature
        initial_contacts = old_contacts is None and self._contacts_signature is not None
        self._upsert_groups([
            {"id": group, "name": name} for group, name in contact_names.items()
            if group.endswith("@chatroom")
        ])
        databases = wechat_db.find_msg_databases(self.account_dir)
        present_databases = set(databases)
        self._live_shards.intersection_update(present_databases)
        candidates = self._scan_candidates
        if candidates is not None:
            candidates = set(candidates) | self._live_shards
        if self.bot_mode and self.priority_groups:
            connection = getattr(self.store, 'conn', None)
            if connection is not None:
                placeholders = ','.join('?' for _ in self.priority_groups)
                rows = connection.execute(
                    'SELECT DISTINCT s.relative_path FROM sources s JOIN streams t USING(source_ref) '
                    'WHERE s.account_id=? AND t.generation=s.generation AND t.group_id IN (' + placeholders + ')',
                    (self.account_id, *self.priority_groups)).fetchall()
                self._priority_shards = {self.account_dir / row[0] for row in rows}
            databases = sorted(databases, key=lambda path: path not in self._priority_shards)
        if candidates is not None:
            # Directory enumeration still notices a new shard. Reader rebuilds
            # already force a full scan when any account/key/file identity
            # changes, so only known unchanged families can be omitted here.
            databases = [path for path in databases if path in candidates]
        auxiliary = []
        p2_enabled = callable(getattr(self.store, "upsert_voice", None)) and callable(getattr(self.store, "upsert_profiles", None))
        if p2_enabled:
            from chatlog_keeper.p2_sources import database_paths
            auxiliary = database_paths(self.account_dir)
        for old in set(self._snapshots) - present_databases - set(auxiliary):
            self._discard_snapshot(old)
        seen = []
        metadata_tasks = []
        history_tasks = []
        for database in databases:
            source = None
            source_started_at = time.time()
            self._source_timings = {}
            self._source_live_pending = False
            try:
                with self._timed('source_registration'):
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
                    self._live_shards.discard(database)
                    continue
                snapshot_key = fingerprint + ":" + str(source["generation"]) + ":" + hashlib.sha256(
                    json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                snapshot = self._get_snapshot(database, key, snapshot_key)
                with self._snapshot_connection(snapshot) as connection:
                    with self._timed('schema_discovery'):
                        groups, names, unresolved = self._groups(connection, contact_names)
                        existing_tables = set(self._table_names(connection))
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
                    if self.bot_mode and self.priority_groups:
                        groups.sort(key=lambda group: group[1] not in self.priority_groups)
                        if any(group in self.priority_groups for _, group, _ in groups):
                            self._priority_shards.add(database)
                    source, processed = self._commit_live_source(connection, groups, names, source,
                        started, fingerprint, unresolved, tables_removed)
                    if self._source_live_pending:
                        self._live_shards.add(database)
                    else:
                        self._live_shards.discard(database)
                    summary['rows_processed'] += processed
                    summary["groups"] += len(groups)
                    history_tasks.append((database, snapshot, groups, names, dict(source)))
                    if groups and self._metadata_needs_work(source):
                        metadata_tasks.append((database, snapshot, groups, names, dict(source)))
                # The live checkpoint AND the source availability/generation
                # are visible before waking the schema-4 pilot mirror. Waking
                # from inside commit_page could expose a recovered source as
                # unavailable and incorrectly advance the mirror's baseline.
                if self.bot_mode:
                    ready_at = time.time()
                    for batch in self._live_batches:
                        if batch['source_ref'] == ref and batch['generation'] == source['generation']:
                            batch.update(source_ready_at=ready_at, fingerprint=fingerprint,
                                source_started_at=source_started_at,
                                collection_to_source_seconds=max(0, source_started_at - self._collection_started_at),
                                source_timings={key: round(value, 6) for key, value in self._source_timings.items()})
                summary["readable_sources"] += 1
                if not groups:
                    self._discard_snapshot(database)
            except Exception as exc:
                code = getattr(exc, "code", None) or (
                    str(exc) if isinstance(exc, CollectorError) else type(exc).__name__)
                summary["errors"].append({"source_ref": source["source_ref"] if source else None,
                                           "error": code})
                if code in {'cached_database_key_unavailable', 'source_snapshot_or_decryption_failed'}:
                    self._cached_reader = self._cached_reader_stamp = None
                if source is not None:
                    self.store.set_source_status(source["source_ref"], available=False, error=code)
            finally:
                self._source_timings = None
        self._live_pending = self._live_pending or bool(self._live_shards)
        if candidates is None:
            # A narrow hint scan must never mark an omitted healthy shard as
            # missing. Every full-fence pass still checks the whole directory.
            self.store.mark_missing_sources(self.account_id, seen)
        # Every shard's live pages are committed before any historical work.
        # In bot mode one rotating history page shares the remaining cycle
        # budget; a busy early group cannot starve another group's live input.
        maintenance = [(database, snapshot, group, names, source)
                       for database, snapshot, groups, names, source in history_tasks
                       for group in groups]
        if self.bot_mode and maintenance:
            offset = self._maintenance_cursor % len(maintenance)
            maintenance = maintenance[offset:] + maintenance[:offset]
        for database, snapshot, group, names, source in maintenance:
            if self.bot_mode and (self._live_pending or self._maintenance_pages or time.monotonic() >= maintenance_deadline):
                break
            try:
                with self._snapshot_connection(snapshot) as connection:
                    try:
                        summary["rows_processed"] += self._process_table(
                            connection, *group, names, source, started, maintenance_only=True)
                    except Exception as exc:
                        if getattr(exc, "code", None) != "source_rowid_reused":
                            raise
                        replacement = self.store.rotate_source(source["source_ref"], "source_rowid_reused")
                        source.update(replacement)
                        # Other pending groups share this source dict. Metadata
                        # jobs have their own copies and must follow the reset.
                        for task in metadata_tasks:
                            if task[4]["source_ref"] == source["source_ref"]:
                                task[4].update(replacement)
                        summary["rows_processed"] += self._process_table(
                            connection, *group, names, source, started, maintenance_only=True)
                        self.store.set_source_status(source["source_ref"], available=True, error=None)
            except Exception as exc:
                code = getattr(exc, "code", None) or (str(exc) if isinstance(exc, CollectorError) else type(exc).__name__)
                summary["errors"].append({"source_ref": source["source_ref"], "error": code})
                self.store.set_source_status(source["source_ref"], available=False, error=code)
            finally:
                if self.bot_mode:
                    self._maintenance_cursor += 1
        # Historical processing may have finished a source after the live pass.
        for database, _, _, _, source in history_tasks:
            if (not self.store.source_needs_work(source["source_ref"], source["generation"], now=started)
                    and not self._metadata_needs_work(source)):
                self._discard_snapshot(database)
        # New rows from every shard take priority over the one-time source
        # metadata upgrade. Reopening an existing private snapshot does not
        # decrypt the source again, and at most 500 origins are attempted here.
        for database, snapshot, groups, names, source in metadata_tasks:
            if self._metadata_budget <= 0 or (self.bot_mode and (
                    self._live_pending or self._maintenance_pages or time.monotonic() >= maintenance_deadline)):
                break
            try:
                with self._snapshot_connection(snapshot) as connection:
                    summary["metadata_rows_refreshed"] += self._backfill_metadata(
                        connection, groups, names, source)
                if self.bot_mode:
                    self._maintenance_pages += 1
                if (not self.store.source_needs_work(source["source_ref"], source["generation"], now=started)
                        and not self._metadata_needs_work(source)):
                    self._discard_snapshot(database)
            except Exception as exc:
                code = getattr(exc, "code", None) or (
                    str(exc) if isinstance(exc, CollectorError) else type(exc).__name__)
                summary["errors"].append({"source_ref": source["source_ref"], "error": code})
                self.store.set_source_status(source["source_ref"], available=False, error=code)
        reparse = getattr(self.store, "reparse_batch", None)
        if callable(reparse) and not self.bot_mode:
            reparsed = reparse(limit=_HISTORY_BATCH)
            summary["historical_rows_reparsed"] = (
                int(reparsed.get("reparsed", 0)) if isinstance(reparsed, dict) else int(reparsed))
        # Auxiliary cached media enriches existing messages only. Its failures
        # must not erase successful message collection or falsify client health.
        if p2_enabled and not self.bot_mode:
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
            last_scan_partial=candidates is not None,
        )
        summary['partial_scan'] = candidates is not None
        if candidates is None:
            status.update(sources_total=summary['sources'], sources_readable=summary['readable_sources'],
                          unresolved_tables=summary['unresolved_tables'])
        elif not summary['errors'] and not summary['unresolved_tables']:
            # A successful hinted shard cannot clear another shard's error.
            status.pop('last_error', None)
        if "p2_sources" in summary:
            status["p2_sources"] = summary["p2_sources"]
        if not summary["errors"] and candidates is None:
            status["last_success_at"] = time.time()
        summary['timings'] = {key: round(value, 6) for key, value in self._timings.items()}
        summary['live_pending'] = self._live_pending
        summary['live_batches'] = self._live_batches
        self._recent_live_batches = (self._recent_live_batches + self._live_batches)[-64:]
        status['last_cycle_timings'] = summary['timings']
        status['last_live_batches'] = self._recent_live_batches
        status['last_native_event_observed_at'] = self._native_observed_at
        self._set_status(**status)
        if self.after_cycle is not None:
            self.after_cycle(self, summary)
        return summary

    def _probe_client_health(self):
        """The entire optional callback may block on local subprocesses."""
        started = time.monotonic()
        try:
            health = self.health_monitor(self)
            error = None
        except Exception as exc:
            health, error = None, type(exc).__name__
        return health, error, max(0.0, time.monotonic() - started)

    def _publish_client_health(self, result=None):
        fields = {"last_cycle_checked_at": time.time()}
        health = None
        if result is not None:
            health, error, elapsed = result
            if error is None and (not isinstance(health, dict) or not isinstance(health.get("state"), str)):
                error = "ValueError"
            if error:
                health = {"state": "unknown", "reason": "health_monitor_failed",
                          "checked_at": time.time(), "process_running": None,
                          "display_available": None}
                LOG.error("client_health_monitor_failed: %s", error)
            fields.update(client_health=health, health_monitor_error=error,
                          last_health_probe_seconds=round(elapsed, 3))
        try:
            self._set_status(**fields)
        except Exception as exc:
            LOG.error("client_health_status_write_failed: %s", type(exc).__name__)
        self._last_health = health
        return health

    def _check_client_health(self):
        """Publish completed evidence without blocking real-time collection.

        Only the collector lock owner schedules or drains the single background
        probe. No result yet means no health evidence; a readable cache cannot
        stand in for a healthy receiving client. Normal mode stays synchronous.
        """
        if not self.bot_mode:
            return self._publish_client_health(self._probe_client_health() if self.health_monitor is not None else None)
        if self._health_closed:
            return self._last_health
        if self._health_future is not None:
            if not self._health_future.done():
                return self._last_health
            result = self._health_future.result()  # Already done; never waits.
            self._health_future = None
            self._publish_client_health(result)
        checked = time.monotonic()
        if self._last_health_at is not None and checked - self._last_health_at < 5:
            return self._last_health
        self._last_health_at = checked
        if self.health_monitor is None:
            return self._publish_client_health()
        if self._health_pool is None:
            self._health_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="archive-health")
        self._health_future = self._health_pool.submit(self._probe_client_health)
        return self._last_health

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
                now = time.monotonic()
                full_fence = (not self.bot_mode or self._dirty_shards is None
                    or self._last_signature_check is None or now - self._last_signature_check >= .5)
                self._scan_candidates = None if full_fence else set(self._dirty_shards)
                signature = self._fast_source_signature() if self.bot_mode and full_fence else None
                if self.bot_mode and full_fence:
                    self._last_signature_check = now
                if (self.bot_mode and full_fence and signature is not None and signature == self._fast_signature
                        and not self._live_pending
                        and self._last_full_scan is not None and now - self._last_full_scan < 5
                        and self._last_full_summary is not None and not self._last_full_summary.get("errors")):
                    # Keep a bounded maintenance/health cadence while avoiding
                    # repeated key validation, contacts, SQL writes and P2 work
                    # when no native message DB family has changed.
                    summary = {**self._last_full_summary, "rows_processed": 0, "idle": True}
                    return summary
                summary = self._collect()
                self._native_observed_monotonic = None
                self._native_observed_at = None
                if self.bot_mode:
                    # Capture the pre-scan fence: a change DURING a scan must
                    # cause another scan, never be swallowed by an after-fence.
                    if full_fence:
                        self._fast_signature = signature
                        self._last_full_scan = now
                        self._last_full_summary = dict(summary)
                    self._last_summary = dict(summary)
                return summary
            except Exception as exc:
                try:
                    self._set_status(last_error=str(exc) if isinstance(exc, CollectorError)
                                     else type(exc).__name__)
                except Exception as status_exc:
                    LOG.error("collector_error_status_write_failed: %s", type(status_exc).__name__)
                raise
            finally:
                self._scan_candidates = None
                self._dirty_shards = set() if self._live_pending else None
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
                if self.bot_mode:
                    self._watcher = NativeFileWatcher()
                while not stop.is_set():
                    started = time.monotonic()
                    try:
                        self.collect_once()
                    except Exception as exc:
                        LOG.error("collector_cycle_failed: %s", type(exc).__name__)
                    if self.bot_mode:
                        self._watcher.configure(self.account_dir)
                        # Immediate fair continuation while live cursors lag;
                        # native file events queued during a scan are drained
                        # here and invalidate the pre-scan idle fence.
                        timeout = 0 if self._live_pending else max(
                            0.0, min(.5, self.poll_interval) - (time.monotonic() - started))
                        if self._watcher.wait(timeout):
                            self._fast_signature = None
                            self._native_observed_at = self._watcher.observed_at
                            self._native_observed_monotonic = self._watcher.observed_monotonic
                            self._dirty_shards = (None if getattr(self._watcher, 'rescan_all', True)
                                                  else set(getattr(self._watcher, 'dirty_paths', ())))
                    else:
                        stop.wait(max(0.0, self.poll_interval - (time.monotonic() - started)))
        finally:
            for number, previous in old_handlers.items():
                signal.signal(number, previous)
            self.close()

    def close(self):
        # The callback can read/write health metadata. Join it before the caller
        # closes ArchiveStore, and never let a completed shutdown schedule more.
        self._health_closed = True
        if self._watcher is not None:
            self._watcher.close()
            self._watcher = None
        self._cached_reader = self._cached_reader_stamp = None
        if self._health_pool is not None:
            self._health_pool.shutdown(wait=True)
            self._health_pool = None
            self._health_future = None
        if self._native_worker is not None:
            self._native_worker.close(self.store)
            self._native_worker = None
        for database in list(self._snapshots):
            self._discard_snapshot(database)
        if self._status_owned:
            try:
                with self._single_instance():
                    self._set_status(running=False)
            except CollectorError:
                # Another collector may have started since this instance stopped.
                pass
            self._status_owned = False


class EnrichmentWorker:
    """One cache-only auxiliary worker, with no message/checkpoint collection.

    It owns separate database connections, private snapshots and a separate
    lock. Slow P2 decryption, image decoding and file copies cannot hold up the
    collector's live scan. It never starts or drives the WeChat UI.
    """

    def __init__(self, store, data_root=None, account=None, poll_interval=30):
        if not math.isfinite(float(poll_interval)) or float(poll_interval) < 1:
            raise ValueError("invalid enrichment interval")
        self.store = store
        self.poll_interval = float(poll_interval)
        self.context = Collector(store, data_root=data_root, account=account)
        self._lock_fd = None
        from chatlog_keeper.p2_sources import P2Sources
        self._p2 = P2Sources(self.context)

    @contextmanager
    def _single_instance(self):
        if self._lock_fd is not None:
            yield
            return
        fd = os.open(Path(self.store.state_dir) / "enrichment.lock",
                     os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(fd, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CollectorError("enrichment_already_running") from None
            self._lock_fd = fd
            yield
        finally:
            self._lock_fd = None
            os.close(fd)

    def run_once(self):
        from chatlog_keeper.archive_media import MediaArchiver
        with self._single_instance():
            reader = self.context._reader()
            previous = self.context._contacts_signature
            names = self.context._names(reader)
            summary = {"p2_sources": self._p2.collect(
                reader, names, contacts_changed=previous != self.context._contacts_signature)}
            summary["media"] = MediaArchiver(self.store, self.context.data_root,
                                              self.context.account_dir).process_pending(limit=20, time_budget=10.0)
            reparse = getattr(self.store, "reparse_batch", None)
            if callable(reparse):
                summary["historical_rows_reparsed"] = reparse(limit=_HISTORY_BATCH)
            self.store.set_meta("enrichment_status", {"last_success_at": time.time(), "error": None})
            return summary

    def run(self, stop_event=None):
        stop = stop_event or threading.Event()
        handlers = {}
        if threading.current_thread() is threading.main_thread():
            for number in (signal.SIGTERM, signal.SIGINT):
                handlers[number] = signal.signal(number, lambda *_: stop.set())
        try:
            with self._single_instance():
                while not stop.is_set():
                    started = time.monotonic()
                    try:
                        self.run_once()
                    except Exception as exc:
                        self.store.set_meta("enrichment_status", {"error": type(exc).__name__})
                        LOG.error("enrichment_failed: %s", type(exc).__name__)
                    stop.wait(max(0, self.poll_interval - (time.monotonic() - started)))
        finally:
            for number, previous in handlers.items():
                signal.signal(number, previous)
            self.close()

    def close(self):
        self.context.close()
