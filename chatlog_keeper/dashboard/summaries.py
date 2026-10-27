"""Durable single-worker summary jobs, independent from collection and sync."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time

from .siwc import SIWCClient, SIWCError, validate_summary
from .usage import UsageLedger


def canonical_hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


PROMPT_VERSION = "incremental-summary-v2"
MAX_BATCH = 500
MAX_INPUT_CHARACTERS = 20000


def semantic(message):
    return {key: message.get(key) for key in ("message_ref", "create_time", "sender_id", "msg_type", "text", "reply")}


def slices(message):
    text = message.get("text", "")
    if not isinstance(text, str):
        raise SIWCError("invalid_summary_input")
    pieces = [text[index:index + 8000] for index in range(0, len(text), 8000)] or [""]
    for index, piece in enumerate(pieces):
        value = dict(message, text=piece)
        if len(pieces) > 1:
            value["text_segment"] = {"index": index + 1, "total": len(pieces)}
        yield value


class SummaryWorker:
    """Persistent observed-change queue; calendar dates never advance a cursor."""

    def __init__(self, state_dir, source_id, client=None, *, clock=time.time):
        self.root = Path(state_dir).expanduser()
        if self.root.is_symlink() or (self.root / "summary.sqlite").is_symlink():
            raise SIWCError("unsafe_summary_state")
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        self.source_id, self.clock = source_id, clock
        self.client, self._own_client = client or SIWCClient(self.root), client is None
        old = os.umask(0o077)
        try:
            self.conn = sqlite3.connect(self.root / "summary.sqlite", timeout=15)
        finally:
            os.umask(old)
        os.chmod(self.root / "summary.sqlite", 0o600)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript("""
          CREATE TABLE IF NOT EXISTS summary_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS summary_jobs(group_ref TEXT NOT NULL,day TEXT NOT NULL,input_hash TEXT NOT NULL,revision INTEGER NOT NULL,model TEXT NOT NULL,envelope TEXT NOT NULL,delivered INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(group_ref,day));
          CREATE TABLE IF NOT EXISTS summary_chunks(input_hash TEXT NOT NULL,model TEXT NOT NULL,result TEXT NOT NULL,PRIMARY KEY(input_hash,model));
          CREATE TABLE IF NOT EXISTS summary_v2_groups(group_ref TEXT PRIMARY KEY);
          CREATE TABLE IF NOT EXISTS summary_v2_sources(group_ref TEXT NOT NULL,day TEXT NOT NULL,data TEXT NOT NULL,baseline_queued INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(group_ref,day));
          CREATE TABLE IF NOT EXISTS summary_v2_messages(group_ref TEXT NOT NULL,message_ref TEXT NOT NULL,day TEXT NOT NULL,input_hash TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(group_ref,message_ref));
          CREATE TABLE IF NOT EXISTS summary_v2_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,group_ref TEXT NOT NULL,day TEXT NOT NULL,message_ref TEXT NOT NULL,operation TEXT NOT NULL,input_hash TEXT NOT NULL,processed INTEGER NOT NULL DEFAULT 0,superseded_by INTEGER,created_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS summary_v2_parts(seq INTEGER NOT NULL,part_index INTEGER NOT NULL,payload TEXT NOT NULL,processed INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(seq,part_index));
          CREATE TABLE IF NOT EXISTS summary_v2_segments(segment_id TEXT PRIMARY KEY,group_ref TEXT NOT NULL,day TEXT NOT NULL,start_seq INTEGER NOT NULL,end_seq INTEGER NOT NULL,input_refs TEXT NOT NULL,dependencies TEXT NOT NULL,result TEXT NOT NULL,model TEXT NOT NULL,input_hash TEXT NOT NULL,stale INTEGER NOT NULL DEFAULT 0,rebuild INTEGER NOT NULL DEFAULT 0);
          CREATE INDEX IF NOT EXISTS summary_v2_segments_scope ON summary_v2_segments(group_ref,day);
          CREATE TABLE IF NOT EXISTS summary_v2_attempts(input_hash TEXT PRIMARY KEY,attempts INTEGER NOT NULL,next_attempt_at REAL NOT NULL,error TEXT,exhausted INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS summary_v2_memory(group_ref TEXT PRIMARY KEY,version INTEGER NOT NULL,input_hash TEXT NOT NULL,body TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS summary_v2_legacy(group_ref TEXT NOT NULL,day TEXT NOT NULL,body TEXT NOT NULL,dirty_refs TEXT NOT NULL DEFAULT '[]',PRIMARY KEY(group_ref,day));
        """)
        self.conn.commit()

    def close(self):
        self.conn.close()
        if self._own_client:
            self.client.close()

    @contextmanager
    def _single_worker(self):
        fd = os.open(self.root / "summary-worker.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise SIWCError("summary_worker_already_running") from None
            yield
        finally:
            os.close(fd)

    def _get_state(self):
        row = self.conn.execute("SELECT value FROM summary_meta WHERE key='status'").fetchone()
        return json.loads(row[0]) if row else {"state": "not_configured", "last_success_at": None, "error": None}

    def _set_state(self, *, clear_pause=False, **changes):
        # Separate worker processes share this row. A normal completion must
        # never clear an authorization/quota pause written by another worker.
        owns_transaction = not self.conn.in_transaction
        if owns_transaction:
            self.conn.execute("BEGIN IMMEDIATE")
        try:
            previous = self._get_state()
            if previous.get("paused") and not clear_pause:
                changes.update(paused=True, state="paused", error=previous.get("error"))
            value = {**previous, **changes, "checked_at": self.clock()}
            self.conn.execute("INSERT INTO summary_meta VALUES('status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(value, separators=(",", ":")),))
            if owns_transaction:
                self.conn.commit()
        except BaseException:
            if owns_transaction:
                self.conn.rollback()
            raise
        return value

    def status(self):
        state = self._get_state()
        state["pending"] = self.conn.execute("SELECT COUNT(*) FROM summary_jobs WHERE delivered=0").fetchone()[0]
        state["pending_events"] = self.conn.execute("SELECT COUNT(*) FROM summary_v2_events WHERE processed=0").fetchone()[0]
        first = self.conn.execute("SELECT MIN(seq) FROM summary_v2_events WHERE processed=0").fetchone()[0]
        state["checkpoint_seq"] = first - 1 if first else self.conn.execute("SELECT COALESCE(MAX(seq),0) FROM summary_v2_events").fetchone()[0]
        return state

    def record_error(self, exc):
        return self._set_state(state="paused" if exc.paused else "error", paused=exc.paused, error=exc.public())

    def resume(self):
        with self.conn:
            self.conn.execute("DELETE FROM summary_v2_attempts")
        return self._set_state(clear_pause=True, state="waiting", paused=False, error=None)

    def pending(self, limit=100, group_refs=None):
        query, params = "SELECT envelope FROM summary_jobs WHERE delivered=0", []
        if group_refs is not None:
            if isinstance(group_refs, str):
                raise SIWCError("invalid_group_allowlist")
            refs = list(dict.fromkeys(group_refs))
            if not refs:
                return []
            if len(refs) > 100 or any(not isinstance(ref, str) for ref in refs):
                raise SIWCError("invalid_group_allowlist")
            query += " AND group_ref IN (" + ",".join("?" for _ in refs) + ")"
            params.extend(refs)
        query += " ORDER BY day,group_ref LIMIT ?"
        return [json.loads(row[0]) for row in self.conn.execute(query, [*params, max(1, min(limit, 500))])]

    def acknowledge(self, group_ref, day, revision):
        with self.conn:
            self.conn.execute("UPDATE summary_jobs SET delivered=1 WHERE group_ref=? AND day=? AND revision=?", (group_ref, day, revision))

    def _invalidate(self, group, ref):
        affected = []
        for row in self.conn.execute("SELECT segment_id,dependencies FROM summary_v2_segments WHERE group_ref=? AND stale=0", (group,)).fetchall():
            if ref in json.loads(row["dependencies"]):
                affected.append(row["segment_id"])
        if affected:
            first = min(self.conn.execute("SELECT start_seq FROM summary_v2_segments WHERE segment_id=?", (identity,)).fetchone()[0] for identity in affected)
            # Memory is derived from earlier segments. Conservative suffix
            # invalidation preserves transitive A→B→C dependencies even when a
            # later answer cites only its own new messages.
            self.conn.execute("UPDATE summary_v2_segments SET stale=1,rebuild=1 WHERE group_ref=? AND end_seq>=? AND stale=0", (group, first))
        for row in self.conn.execute("SELECT day,dirty_refs FROM summary_v2_legacy WHERE group_ref=?", (group,)).fetchall():
            dirty = set(json.loads(row["dirty_refs"])) | {ref}
            self.conn.execute("UPDATE summary_v2_legacy SET dirty_refs=? WHERE group_ref=? AND day=?", (json.dumps(sorted(dirty)), group, row["day"]))

    def _event(self, group, day, message, operation):
        ref = message["message_ref"]
        digest = canonical_hash(semantic(message))
        result = self.conn.execute("INSERT INTO summary_v2_events(group_ref,day,message_ref,operation,input_hash,created_at) VALUES(?,?,?,?,?,?)", (group, day, ref, operation, digest, self.clock()))
        seq = result.lastrowid
        # Retain superseded observations for auditing without paying to summarize
        # obsolete intermediate edits. Distinct messages are never dropped.
        old = [row[0] for row in self.conn.execute("SELECT seq FROM summary_v2_events WHERE group_ref=? AND message_ref=? AND processed=0 AND seq<?", (group, ref, seq))]
        for prior in old:
            self.conn.execute("UPDATE summary_v2_parts SET processed=1 WHERE seq=?", (prior,))
            self.conn.execute("UPDATE summary_v2_events SET processed=1,superseded_by=? WHERE seq=?", (seq, prior))
        if operation == "delete":
            if not any(ref in json.loads(row[0]) for row in self.conn.execute("SELECT dependencies FROM summary_v2_segments WHERE group_ref=? AND rebuild=1", (group,))):
                self.conn.execute("UPDATE summary_v2_events SET processed=1 WHERE seq=?", (seq,))
        else:
            for index, part in enumerate(slices(message)):
                self.conn.execute("INSERT INTO summary_v2_parts VALUES(?,?,?,0)", (seq, index, json.dumps(part, ensure_ascii=False, separators=(",", ":"))))

    def _observe(self, partitions, baseline_days):
        allowed = {part["group_ref"] for part in partitions}
        new_groups = allowed - {row[0] for row in self.conn.execute("SELECT group_ref FROM summary_v2_groups")}
        incoming = {}
        with self.conn:
            for part in partitions:
                data, group, day = part["data"], part["group_ref"], part["day"]
                if canonical_hash(data) != part["content_hash"] or data["group"]["group_ref"] != group or data["day"] != day:
                    raise SIWCError("invalid_summary_partition")
                old_source = self.conn.execute("SELECT baseline_queued FROM summary_v2_sources WHERE group_ref=? AND day=?", (group, day)).fetchone()
                baseline = baseline_days is None or day in baseline_days
                queue_baseline = baseline and not (old_source and old_source[0])
                if old_source is None:
                    legacy = self.conn.execute("SELECT envelope FROM summary_jobs WHERE group_ref=? AND day=?", (group, day)).fetchone()
                    if legacy:
                        self.conn.execute("INSERT OR IGNORE INTO summary_v2_legacy VALUES(?,?,?,'[]')", (group, day, json.dumps(json.loads(legacy[0])["summary"], ensure_ascii=False)))
                self.conn.execute("INSERT INTO summary_v2_sources VALUES(?,?,?,?) ON CONFLICT(group_ref,day) DO UPDATE SET data=excluded.data,baseline_queued=MAX(summary_v2_sources.baseline_queued,excluded.baseline_queued)", (group, day, json.dumps(part, ensure_ascii=False, separators=(",", ":")), int(queue_baseline)))
                for message in data["messages"]:
                    key = group, message["message_ref"]
                    incoming[key] = day, message
                    old = self.conn.execute("SELECT * FROM summary_v2_messages WHERE group_ref=? AND message_ref=?", key).fetchone()
                    digest = canonical_hash(semantic(message))
                    changed = old is not None and (old["input_hash"] != digest or old["day"] != day)
                    if changed:
                        self._invalidate(group, message["message_ref"])
                    if queue_baseline or changed or (old is None and group not in new_groups):
                        self._event(group, day, message, "upsert")
                    self.conn.execute("INSERT INTO summary_v2_messages VALUES(?,?,?,?,?) ON CONFLICT(group_ref,message_ref) DO UPDATE SET day=excluded.day,input_hash=excluded.input_hash,payload=excluded.payload", (group, message["message_ref"], day, digest, json.dumps(message, ensure_ascii=False, separators=(",", ":"))))
            scoped = {(part["group_ref"], part["day"]) for part in partitions}
            for row in self.conn.execute("SELECT * FROM summary_v2_messages").fetchall():
                if (row["group_ref"], row["day"]) in scoped and (row["group_ref"], row["message_ref"]) not in incoming:
                    self._invalidate(row["group_ref"], row["message_ref"])
                    self._event(row["group_ref"], row["day"], json.loads(row["payload"]), "delete")
                    self.conn.execute("DELETE FROM summary_v2_messages WHERE group_ref=? AND message_ref=?", (row["group_ref"], row["message_ref"]))
            self.conn.executemany("INSERT OR IGNORE INTO summary_v2_groups VALUES(?)", [(group,) for group in allowed])
        return allowed

    def _memory(self, group):
        body = {"topics": [], "decisions": [], "todos": []}
        for row in self.conn.execute("SELECT result FROM summary_v2_segments WHERE group_ref=? AND stale=0 ORDER BY end_seq DESC LIMIT 20", (group,)):
            result = json.loads(row[0])
            for kind in body:
                for item in result[kind]:
                    candidate = {**body, kind: body[kind] + [item]}
                    if len(body[kind]) < 15 and len(json.dumps(candidate, ensure_ascii=False)) <= 4000:
                        body[kind].append(item)
        digest = canonical_hash(body)
        old = self.conn.execute("SELECT * FROM summary_v2_memory WHERE group_ref=?", (group,)).fetchone()
        version = old["version"] + 1 if old and old["input_hash"] != digest else old["version"] if old else 1
        with self.conn:
            self.conn.execute("INSERT INTO summary_v2_memory VALUES(?,?,?,?) ON CONFLICT(group_ref) DO UPDATE SET version=excluded.version,input_hash=excluded.input_hash,body=excluded.body", (group, version, digest, json.dumps(body, ensure_ascii=False)))
        return body, version

    def _context(self, group, day, targets, memory_overlay=None):
        memory, version = self._memory(group)
        if memory_overlay:
            combined = self._merge([memory, *memory_overlay])
            memory = {"topics": [], "decisions": [], "todos": []}
            for kind in memory:
                for item in reversed(combined[kind]):
                    candidate = {**memory, kind: memory[kind] + [item]}
                    if len(memory[kind]) < 15 and len(json.dumps(candidate, ensure_ascii=False)) <= 4000:
                        memory[kind].append(item)
        target_refs = {message["message_ref"] for message in targets}
        before = min((message["create_time"] for message in targets), default=0)
        background = []
        rows = self.conn.execute("SELECT payload FROM summary_v2_messages WHERE group_ref=? ORDER BY json_extract(payload,'$.create_time') DESC", (group,))
        for row in rows:
            message = json.loads(row[0])
            if message["message_ref"] not in target_refs and message["create_time"] <= before:
                message["text"] = message["text"][-1000:]
                background.append(message)
                if len(background) == 50:
                    break
        context = {"mode": "incremental", "group_ref": group, "day": day, "memory": memory, "background": list(reversed(background))}
        while len(json.dumps({"messages": targets, "context": context}, ensure_ascii=False, separators=(",", ":"))) > MAX_INPUT_CHARACTERS:
            if context["background"]:
                context["background"].pop(0)
            elif any(context["memory"].values()):
                for kind in ("topics", "decisions", "todos"):
                    if context["memory"][kind]:
                        context["memory"][kind].pop()
                        break
            else:
                raise SIWCError("summary_batch_too_large")
        dependencies = set(target_refs) | {item["message_ref"] for item in context["background"]}
        dependencies |= {ref for values in context["memory"].values() for item in values for ref in item["message_refs"]}
        hashes = {row["message_ref"]: row["input_hash"] for row in self.conn.execute("SELECT message_ref,input_hash FROM summary_v2_messages WHERE group_ref=?", (group,)) if row["message_ref"] in dependencies}
        return context, version, hashes

    @staticmethod
    def _batch_parts(rows):
        batches, batch, size = [], [], 0
        for row in rows:
            message = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
            length = len(json.dumps(message, ensure_ascii=False, separators=(",", ":")))
            if batch and (len(batch) >= MAX_BATCH or size + length > 12000):
                batches.append(batch)
                batch, size = [], 0
            batch.append(row)
            size += length
        if batch:
            batches.append(batch)
        return batches

    def _infer(self, group, day, targets, start, end, model, memory_overlay=None):
        context, memory_version, dependencies = self._context(group, day, targets, memory_overlay)
        digest = canonical_hash({"messages": [semantic(item) | ({"text_segment": item["text_segment"]} if "text_segment" in item else {}) for item in targets], "context": context, "dependencies": dependencies, "model": model, "prompt_version": PROMPT_VERSION})
        row = self.conn.execute("SELECT result FROM summary_chunks WHERE input_hash=? AND model=?", (digest, model)).fetchone()
        if row:
            return json.loads(row[0]), digest, dependencies
        if self._get_state().get("paused"):
            raise SIWCError("siwc_shared_pause", paused=True)
        if (self.root / "chat-waiting").exists():
            raise SIWCError("summary_chat_waiting")
        if self._provider_calls >= self._provider_budget:
            raise SIWCError("summary_batch_budget")
        attempt = self.conn.execute("SELECT * FROM summary_v2_attempts WHERE input_hash=?", (digest,)).fetchone()
        if attempt and (attempt["exhausted"] or self.clock() < attempt["next_attempt_at"]):
            raise SIWCError("summary_retries_exhausted" if attempt["exhausted"] else "summary_retry_wait")
        count = (attempt["attempts"] if attempt else 0) + 1
        with self.conn:
            self.conn.execute("INSERT INTO summary_v2_attempts VALUES(?,?,0,NULL,0) ON CONFLICT(input_hash) DO UPDATE SET attempts=excluded.attempts", (digest, count))
        try:
            self._provider_calls += 1
            result = self.client.summarize(targets, model=model, context=context, run_metadata={
                "run_id": str(__import__("uuid").uuid4()), "kind": "summary", "source_id": self.source_id,
                "group_refs": [group], "input_hash": digest, "prompt_version": PROMPT_VERSION,
                "parent_memory_version": memory_version, "start_seq": start, "end_seq": end,
                "input_characters": len(json.dumps({"messages": targets, "context": context}, ensure_ascii=False))})
            result = validate_summary(result, {message["message_ref"] for message in targets})
        except SIWCError as exc:
            with self.conn:
                self.conn.execute("UPDATE summary_v2_attempts SET next_attempt_at=?,error=?,exhausted=? WHERE input_hash=?", (self.clock() + (60 if count == 1 else 300), exc.code, int(count >= 3), digest))
            raise
        with self.conn:
            self.conn.execute("INSERT OR REPLACE INTO summary_chunks VALUES(?,?,?)", (digest, model, json.dumps(result, ensure_ascii=False, separators=(",", ":"))))
        return result, digest, dependencies

    def _segment(self, group, day, targets, start, end, model, result, digest, dependencies):
        identity = canonical_hash({"group": group, "day": day, "input": digest, "start": start, "end": end})
        self.conn.execute("INSERT OR REPLACE INTO summary_v2_segments VALUES(?,?,?,?,?,?,?,?,?,?,0,0)", (identity, group, day, start, end, json.dumps(list(dict.fromkeys(message["message_ref"] for message in targets))), json.dumps(dependencies, sort_keys=True), json.dumps(result, ensure_ascii=False), model, digest))

    def _mark_parts(self, rows):
        for row in rows:
            self.conn.execute("UPDATE summary_v2_parts SET processed=1 WHERE seq=? AND part_index=?", (row["seq"], row["part_index"]))
            self.conn.execute("UPDATE summary_v2_events SET processed=1 WHERE seq=? AND NOT EXISTS(SELECT 1 FROM summary_v2_parts WHERE seq=? AND processed=0)", (row["seq"], row["seq"]))

    def _rebuild(self, group, day, model):
        segments = self.conn.execute("SELECT * FROM summary_v2_segments WHERE group_ref=? AND day=? AND rebuild=1", (group, day)).fetchall()
        refs = set(ref for segment in segments for ref in json.loads(segment["input_refs"]))
        messages = [json.loads(row["payload"]) for row in self.conn.execute("SELECT message_ref,payload FROM summary_v2_messages WHERE group_ref=? AND day=? ORDER BY json_extract(payload,'$.create_time'),message_ref", (group, day)) if row["message_ref"] in refs]
        rows = [{"payload": part} for message in messages for part in slices(message)]
        replacements = []
        for batch in self._batch_parts(rows):
            if (self.root / "chat-waiting").exists():
                return False
            targets = [row["payload"] for row in batch]
            start, end = min(segment["start_seq"] for segment in segments), max(segment["end_seq"] for segment in segments)
            result, digest, dependencies = self._infer(group, day, targets, start, end, model,
                                                       memory_overlay=[item[3] for item in replacements])
            replacements.append((targets, start, end, result, digest, dependencies))
        with self.conn:
            self.conn.executemany("UPDATE summary_v2_segments SET rebuild=0 WHERE segment_id=?", [(segment["segment_id"],) for segment in segments])
            for targets, start, end, result, digest, dependencies in replacements:
                self._segment(group, day, targets, start, end, model, result, digest, dependencies)
            for row in self.conn.execute("SELECT e.seq,e.message_ref,e.input_hash,m.input_hash AS current_hash FROM summary_v2_events e LEFT JOIN summary_v2_messages m ON m.group_ref=e.group_ref AND m.message_ref=e.message_ref WHERE e.group_ref=? AND e.day=? AND e.processed=0", (group, day)).fetchall():
                if row["message_ref"] in refs and row["input_hash"] == row["current_hash"]:
                    self.conn.execute("UPDATE summary_v2_parts SET processed=1 WHERE seq=?", (row["seq"],))
                    self.conn.execute("UPDATE summary_v2_events SET processed=1 WHERE seq=?", (row["seq"],))
        return True

    def _settle_deletions(self):
        with self.conn:
            for event in self.conn.execute("SELECT seq,group_ref,message_ref FROM summary_v2_events WHERE operation='delete' AND processed=0").fetchall():
                pending = any(event["message_ref"] in json.loads(row[0]) for row in self.conn.execute("SELECT dependencies FROM summary_v2_segments WHERE group_ref=? AND rebuild=1", (event["group_ref"],)))
                if not pending:
                    self.conn.execute("UPDATE summary_v2_events SET processed=1 WHERE seq=?", (event["seq"],))

    @staticmethod
    def _merge(bodies):
        merged = {"topics": [], "decisions": [], "todos": []}
        for body in bodies:
            for kind in merged:
                merged[kind].extend(body.get(kind, []))
        for kind, items in merged.items():
            unique = {}
            for item in items:
                key = item["text"].casefold()
                unique[key] = {"text": item["text"], "message_refs": list(dict.fromkeys((unique.get(key) or {}).get("message_refs", []) + item["message_refs"]))[:100]}
            merged[kind] = list(unique.values())[-100:]
        return merged

    def _publish(self, allowed, model):
        self._settle_deletions()
        completed = 0
        for source in self.conn.execute("SELECT * FROM summary_v2_sources").fetchall():
            group, day = source["group_ref"], source["day"]
            if group not in allowed or self.conn.execute("SELECT 1 FROM summary_v2_events WHERE group_ref=? AND day=? AND processed=0 LIMIT 1", (group, day)).fetchone() or self.conn.execute("SELECT 1 FROM summary_v2_segments WHERE group_ref=? AND day=? AND rebuild=1 LIMIT 1", (group, day)).fetchone():
                continue
            part = json.loads(source["data"])
            segments = self.conn.execute("SELECT * FROM summary_v2_segments WHERE group_ref=? AND day=? AND stale=0 ORDER BY end_seq,segment_id", (group, day)).fetchall()
            old = self.conn.execute("SELECT * FROM summary_jobs WHERE group_ref=? AND day=?", (group, day)).fetchone()
            if not segments and not source["baseline_queued"] and not old:
                continue
            live = {message["message_ref"] for message in part["data"]["messages"]}
            represented = {ref for segment in segments for ref in json.loads(segment["input_refs"])} & live
            baseline_complete = bool(source["baseline_queued"] and represented >= live)
            bodies = []
            legacy = self.conn.execute("SELECT * FROM summary_v2_legacy WHERE group_ref=? AND day=?", (group, day)).fetchone()
            if legacy and not baseline_complete:
                dirty = set(json.loads(legacy["dirty_refs"]))
                bodies.append({kind: [item for item in values if not dirty.intersection(item["message_refs"]) and set(item["message_refs"]).issubset(live)] for kind, values in json.loads(legacy["body"]).items()})
            bodies.extend(json.loads(segment["result"]) for segment in segments)
            body = validate_summary(self._merge(bodies), live)
            processing = {"mode": "incremental", "checkpoint_seq": self.status()["checkpoint_seq"], "baseline_complete": baseline_complete, "processed_messages": len(represented), "pending_events": 0}
            if old:
                previous = json.loads(old["envelope"])
                if old["input_hash"] == part["content_hash"] and old["model"] == model and previous.get("summary") == body and previous.get("processing", {}).get("baseline_complete") == baseline_complete:
                    continue
            envelope = {"source_id": self.source_id, "group_ref": group, "day": day,
                        "revision": max(part["revision"], old["revision"] + 1 if old else 1),
                        "input_hash": part["content_hash"], "model": model, "generated_at": self.clock(),
                        "covered_until": max((message["create_time"] for message in part["data"]["messages"] if message["message_ref"] in represented), default=0),
                        "summary": body, "processing": processing}
            with self.conn:
                self.conn.execute("INSERT INTO summary_jobs VALUES(?,?,?,?,?,?,0) ON CONFLICT(group_ref,day) DO UPDATE SET input_hash=excluded.input_hash,revision=excluded.revision,model=excluded.model,envelope=excluded.envelope,delivered=0", (group, day, part["content_hash"], envelope["revision"], model, json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))))
            completed += 1
        return completed

    def run_once(self, partitions, *, baseline_days=None, max_batches=8):
        try:
            with self._single_worker():
                ledger = UsageLedger(self.root, clock=self.clock)
                try:
                    for run in ledger.list_running():
                        if run["kind"] == "summary":
                            ledger.finish_run(run["run_id"], status="interrupted", error_code="summary_worker_restarted")
                finally:
                    ledger.close()
                self._provider_calls, self._provider_budget = 0, max_batches
                partitions = list(partitions)
                allowed = self._observe(partitions, baseline_days)
                if self._get_state().get("paused"):
                    return self.status()
                identity = self.client.status()
                if identity.get("state") != "ready" or not identity.get("model"):
                    raise SIWCError("siwc_" + identity.get("state", "login_required"), paused=True)
                model = identity["model"]
                previous_model = self.conn.execute("SELECT value FROM summary_meta WHERE key='incremental_model'").fetchone()
                if previous_model and json.loads(previous_model[0]) != model:
                    with self.conn:
                        for group in allowed:
                            self.conn.execute("UPDATE summary_v2_segments SET stale=1,rebuild=1 WHERE group_ref=?", (group,))
                with self.conn:
                    self.conn.execute("INSERT INTO summary_meta VALUES('incremental_model',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(model),))
                blocked, errors, batches = set(), [], 0
                while batches < max_batches:
                    if self._get_state().get("paused"):
                        return self.status()
                    if (self.root / "chat-waiting").exists():
                        return self._set_state(state="partial", paused=False, yielded=True, completed=self._publish(allowed, model), error=None)
                    rebuild = next((row for row in self.conn.execute("SELECT DISTINCT group_ref,day FROM summary_v2_segments WHERE rebuild=1 ORDER BY start_seq") if row["group_ref"] in allowed - blocked), None)
                    if rebuild:
                        try:
                            if not self._rebuild(rebuild["group_ref"], rebuild["day"], model):
                                return self._set_state(state="partial", yielded=True, error=None)
                            batches += 1
                            continue
                        except SIWCError as exc:
                            if exc.code in {"summary_chat_waiting", "summary_batch_budget"}:
                                return self._set_state(state="partial", yielded=exc.code == "summary_chat_waiting", error=None)
                            if exc.paused:
                                return self.record_error(exc)
                            blocked.add(rebuild["group_ref"])
                            errors.append(exc.code)
                            continue
                    rows = self.conn.execute("SELECT p.*,e.group_ref,e.day,e.message_ref FROM summary_v2_parts p JOIN summary_v2_events e USING(seq) WHERE p.processed=0 ORDER BY p.seq,p.part_index").fetchall()
                    first = next((row for row in rows if row["group_ref"] in allowed - blocked), None)
                    if first is None:
                        break
                    candidates = [row for row in rows if row["group_ref"] == first["group_ref"] and row["day"] == first["day"]]
                    batch = self._batch_parts(candidates)[0]
                    targets = [json.loads(row["payload"]) for row in batch]
                    group, day = first["group_ref"], first["day"]
                    try:
                        result, digest, dependencies = self._infer(group, day, targets, batch[0]["seq"], batch[-1]["seq"], model)
                    except SIWCError as exc:
                        if exc.code in {"summary_chat_waiting", "summary_batch_budget"}:
                            return self._set_state(state="partial", yielded=exc.code == "summary_chat_waiting", error=None)
                        if exc.paused:
                            return self.record_error(exc)
                        blocked.add(group)
                        errors.append(exc.code)
                        continue
                    with self.conn:
                        self._segment(group, day, targets, batch[0]["seq"], batch[-1]["seq"], model, result, digest, dependencies)
                        self._mark_parts(batch)
                    batches += 1
                    self._set_state(last_success_at=self.clock())
                completed = self._publish(allowed, model)
                pending = self.conn.execute("SELECT COUNT(*) FROM summary_v2_events WHERE processed=0").fetchone()[0]
                rebuilds = self.conn.execute("SELECT COUNT(*) FROM summary_v2_segments WHERE rebuild=1").fetchone()[0]
                return self._set_state(state="error" if errors else "partial" if pending or rebuilds else "ready", paused=False, yielded=False,
                                       error={"code": errors[0]} if errors else None, completed=completed,
                                       skipped=max(0, len(partitions) - completed), processed_batches=batches)
        except SIWCError as exc:
            if exc.code == "summary_worker_already_running":
                return {**self.status(), "busy": True}
            return self.record_error(exc)
