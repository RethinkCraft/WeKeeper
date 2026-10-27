"""Cloud-only private avatars, per-user chat jobs and auditable usage metadata."""
import base64
from datetime import datetime, timedelta, time as day_time
import hashlib
import hmac
import io
import json
import sqlite3
import uuid
from zoneinfo import ZoneInfo

from .protocol import ChatScope, canonical_json
from .security import opaque_token, token_hash

ZONE = ZoneInfo("Asia/Shanghai")
TERMINAL = frozenset({"completed", "failed", "interrupted", "cancelled"})
TOKEN_FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "total_tokens")


class Conflict(ValueError):
    pass


FEATURE_SCHEMA = """
CREATE TABLE IF NOT EXISTS avatar_blobs(avatar_hash TEXT PRIMARY KEY,mime_type TEXT NOT NULL,width INTEGER NOT NULL,height INTEGER NOT NULL,data BLOB NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sender_avatars(source_id TEXT NOT NULL,group_ref TEXT NOT NULL,sender_id TEXT NOT NULL,avatar_hash TEXT NOT NULL REFERENCES avatar_blobs(avatar_hash),observed_at REAL NOT NULL,display_name TEXT,PRIMARY KEY(source_id,group_ref,sender_id));
CREATE TABLE IF NOT EXISTS chat_conversations(id TEXT PRIMARY KEY,user_id INTEGER NOT NULL REFERENCES users(id),title TEXT NOT NULL,scope TEXT NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS chat_conversations_owner ON chat_conversations(user_id,updated_at);
CREATE TABLE IF NOT EXISTS chat_turns(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL REFERENCES chat_conversations(id),user_id INTEGER NOT NULL REFERENCES users(id),client_request_id TEXT NOT NULL,content TEXT NOT NULL,scope TEXT NOT NULL,status TEXT NOT NULL,created_at REAL NOT NULL,started_at REAL,completed_at REAL,response_text TEXT NOT NULL DEFAULT '',message_refs TEXT NOT NULL DEFAULT '[]',last_seq INTEGER NOT NULL DEFAULT 0,cancel_requested INTEGER NOT NULL DEFAULT 0,error TEXT,source_id TEXT,worker_id TEXT,lease_hash TEXT,lease_until REAL,UNIQUE(user_id,client_request_id));
CREATE UNIQUE INDEX IF NOT EXISTS chat_one_active_per_user ON chat_turns(user_id) WHERE status IN ('queued','running','cancelling');
CREATE TABLE IF NOT EXISTS chat_events(turn_id TEXT NOT NULL REFERENCES chat_turns(id),seq INTEGER NOT NULL,payload TEXT NOT NULL,created_at REAL NOT NULL,PRIMARY KEY(turn_id,seq));
CREATE TABLE IF NOT EXISTS usage_runs(source_id TEXT NOT NULL,run_id TEXT NOT NULL,kind TEXT NOT NULL,user_id INTEGER,turn_id TEXT,model TEXT NOT NULL,started_at REAL NOT NULL,completed_at REAL NOT NULL,status TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(source_id,run_id));
CREATE INDEX IF NOT EXISTS usage_runs_scope ON usage_runs(user_id,completed_at);
CREATE TABLE IF NOT EXISTS quota_snapshots(source_id TEXT PRIMARY KEY,sampled_at REAL NOT NULL,received_at REAL NOT NULL,payload TEXT NOT NULL);
"""


class CloudFeaturesMixin:
    def _migrate_features(self):
        self.conn.executescript(FEATURE_SCHEMA)
        columns = {row[1] for row in self.conn.execute("PRAGMA table_info(quota_snapshots)")}
        for column, kind in (("last_success_at", "REAL"), ("last_success_payload", "TEXT")):
            if column not in columns:
                self.conn.execute(f"ALTER TABLE quota_snapshots ADD COLUMN {column} {kind}")
        self.conn.execute("UPDATE quota_snapshots SET last_success_at=sampled_at,last_success_payload=payload WHERE last_success_at IS NULL AND json_extract(payload,'$.auth_state')='ready' AND json_extract(payload,'$.error') IS NULL AND EXISTS(SELECT 1 FROM json_tree(payload,'$.rate_limits') WHERE key='usedPercent' AND type IN ('integer','real'))")

    @staticmethod
    def _quota_has_values(value):
        limits = value.get("rate_limits") or {}
        buckets = [limits.get("rateLimits"), *(limits.get("rateLimitsByLimitId") or {}).values()]
        return any(isinstance(bucket, dict) and any(isinstance(bucket.get(key), dict) and type(bucket[key].get("usedPercent")) in (int, float) for key in ("primary", "secondary")) for bucket in buckets)

    def _cancel_user_turns(self, user_id):
        # Called inside the account-update transaction. A disabled account must
        # not leave queued inference behind or continue a running call unchecked.
        rows = self.conn.execute("SELECT * FROM chat_turns WHERE user_id=? AND status IN ('queued','running','cancelling')", (user_id,)).fetchall()
        for row in rows:
            if row["status"] == "queued":
                self._server_chat_event(row, "cancelled", "user_disabled")
            else:
                self.conn.execute("UPDATE chat_turns SET cancel_requested=1,status='cancelling' WHERE id=?", (row["id"],))
                self._bump()

    def ingest_avatar(self, value):
        from PIL import Image
        try:
            data = base64.b64decode(value["content_base64"], validate=True)
        except (ValueError, TypeError):
            raise ValueError("invalid_avatar_encoding") from None
        if not data or len(data) > 128 * 1024 or hashlib.sha256(data).hexdigest() != value["avatar_hash"]:
            raise ValueError("invalid_avatar_content_hash_or_size")
        try:
            with Image.open(io.BytesIO(data)) as image:
                width, height, fmt = *image.size, image.format
                if fmt not in {"PNG", "JPEG"} or not 1 <= width <= 256 or not 1 <= height <= 256 or getattr(image, "n_frames", 1) != 1:
                    raise ValueError()
                image.verify()
            with Image.open(io.BytesIO(data)) as image:
                image.load()
        except (OSError, ValueError, Image.DecompressionBombError):
            raise ValueError("invalid_avatar_image") from None
        mime = "image/png" if fmt == "PNG" else "image/jpeg"
        if mime != value["mime_type"]:
            raise ValueError("avatar_mime_mismatch")
        sid, group, sender = value["source_id"], value["group_ref"], value["sender_id"]
        with self.transaction():
            if not self.conn.execute("SELECT 1 FROM groups WHERE group_ref=? AND source_id=?", (group, sid)).fetchone():
                raise Conflict("avatar_source_group_mismatch")
            if not self.conn.execute("SELECT 1 FROM messages WHERE source_id=? AND group_ref=? AND sender_id=?", (sid, group, sender)).fetchone():
                raise ValueError("avatar_sender_not_observed")
            old = self.conn.execute("SELECT * FROM sender_avatars WHERE source_id=? AND group_ref=? AND sender_id=?", (sid, group, sender)).fetchone()
            if old and old["observed_at"] > value["observed_at"]:
                return {"applied": False, "avatar_hash": old["avatar_hash"]}
            if old and old["avatar_hash"] == value["avatar_hash"] and old["observed_at"] == value["observed_at"]:
                return {"applied": False, "avatar_hash": old["avatar_hash"]}
            self.conn.execute("INSERT OR IGNORE INTO avatar_blobs VALUES(?,?,?,?,?,?)", (value["avatar_hash"], mime, width, height, data, self.clock()))
            self.conn.execute("INSERT INTO sender_avatars VALUES(?,?,?,?,?,?) ON CONFLICT(source_id,group_ref,sender_id) DO UPDATE SET avatar_hash=excluded.avatar_hash,observed_at=excluded.observed_at,display_name=excluded.display_name", (sid, group, sender, value["avatar_hash"], value["observed_at"], value.get("display_name")))
            self._bump()
        return {"applied": True, "avatar_hash": value["avatar_hash"]}

    def _avatar_url(self, group, sender):
        row = self.conn.execute("SELECT a.avatar_hash FROM sender_avatars a JOIN groups g ON g.group_ref=a.group_ref AND g.source_id=a.source_id WHERE a.group_ref=? AND a.sender_id=?", (group, sender)).fetchone()
        return f"/api/groups/{group}/senders/{sender}/avatar?hash={row[0]}" if row else None

    def avatar(self, group, sender, expected_hash=None):
        with self._lock:
            row = self.conn.execute("SELECT b.* FROM sender_avatars a JOIN avatar_blobs b USING(avatar_hash) JOIN groups g ON g.group_ref=a.group_ref AND g.source_id=a.source_id WHERE a.group_ref=? AND a.sender_id=? AND EXISTS(SELECT 1 FROM messages m WHERE m.group_ref=a.group_ref AND m.source_id=a.source_id AND m.sender_id=a.sender_id)", (group, sender)).fetchone()
            if not row or expected_hash and row["avatar_hash"] != expected_hash:
                raise KeyError("avatar_not_found")
            return {"data": bytes(row["data"]), "mime_type": row["mime_type"], "avatar_hash": row["avatar_hash"]}

    def _chat_scope(self, scope):
        value = ChatScope.model_validate(scope).model_dump(mode="json")
        refs = value["group_refs"]
        rows = self.conn.execute("SELECT group_ref,source_id FROM groups WHERE group_ref IN (" + ",".join("?" for _ in refs) + ")", refs).fetchall()
        if len(rows) != len(refs):
            raise ValueError("unknown_chat_group")
        if len({row["source_id"] for row in rows}) != 1:
            raise ValueError("chat_groups_require_one_source")
        return value

    @staticmethod
    def _public_turn(row):
        keys = ("id", "conversation_id", "user_id", "content", "status", "created_at", "started_at", "completed_at", "response_text", "last_seq", "error")
        return {**{key: row[key] for key in keys}, "scope": json.loads(row["scope"]), "message_refs": json.loads(row["message_refs"]), "cancel_requested": bool(row["cancel_requested"])}

    def _conversation(self, row):
        latest = self.conn.execute("SELECT status FROM chat_turns WHERE conversation_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1", (row["id"],)).fetchone()
        return {"id": row["id"], "title": row["title"], "scope": json.loads(row["scope"]), "created_at": row["created_at"], "updated_at": row["updated_at"], "last_turn_status": latest[0] if latest else None}

    def _owned_conversation(self, conversation_id, user_id):
        row = self.conn.execute("SELECT * FROM chat_conversations WHERE id=? AND user_id=?", (conversation_id, user_id)).fetchone()
        if not row:
            raise KeyError("conversation_not_found")
        return row

    def create_conversation(self, user_id, value):
        with self.transaction():
            scope = self._chat_scope({key: value[key] for key in ("group_refs", "since", "until")})
            conversation_id, now = uuid.uuid4().hex, self.clock()
            self.conn.execute("INSERT INTO chat_conversations VALUES(?,?,?,?,?,?)", (conversation_id, user_id, value.get("title", "新的群聊对话"), canonical_json(scope), now, now))
            self._bump()
            return self._conversation(self.conn.execute("SELECT * FROM chat_conversations WHERE id=?", (conversation_id,)).fetchone())

    def conversations(self, user_id):
        self.expire_chat_jobs()
        with self._lock:
            return [self._conversation(row) for row in self.conn.execute("SELECT * FROM chat_conversations WHERE user_id=? ORDER BY updated_at DESC,id DESC LIMIT 100", (user_id,))]

    def conversation(self, conversation_id, user_id):
        self.expire_chat_jobs()
        with self._lock:
            row = self._owned_conversation(conversation_id, user_id)
            turns = self.conn.execute("SELECT * FROM chat_turns WHERE conversation_id=? ORDER BY created_at,rowid", (conversation_id,)).fetchall()
            return {"conversation": self._conversation(row), "turns": [self._public_turn(turn) for turn in turns]}

    def create_turn(self, conversation_id, user_id, value):
        self.expire_chat_jobs()
        with self.transaction():
            conversation = self._owned_conversation(conversation_id, user_id)
            scope = json.loads(conversation["scope"])
            for key in ("group_refs", "since", "until"):
                if value.get(key) is not None:
                    scope[key] = value[key]
            scope = self._chat_scope(scope)
            content = value["content"].strip()
            if not content:
                raise ValueError("empty_chat_message")
            prior = self.conn.execute("SELECT * FROM chat_turns WHERE user_id=? AND client_request_id=?", (user_id, value["client_request_id"])).fetchone()
            if prior:
                if prior["conversation_id"] != conversation_id or prior["content"] != content or json.loads(prior["scope"]) != scope:
                    raise Conflict("chat_request_id_conflict")
                return self._public_turn(prior)
            if self.conn.execute("SELECT 1 FROM chat_turns WHERE user_id=? AND status IN ('queued','running','cancelling')", (user_id,)).fetchone():
                raise Conflict("user_turn_in_progress")
            turn_id, now = uuid.uuid4().hex, self.clock()
            self.conn.execute("INSERT INTO chat_turns(id,conversation_id,user_id,client_request_id,content,scope,status,created_at) VALUES(?,?,?,?,?,?,'queued',?)", (turn_id, conversation_id, user_id, value["client_request_id"], content, canonical_json(scope), now))
            self.conn.execute("UPDATE chat_conversations SET scope=?,updated_at=? WHERE id=?", (canonical_json(scope), now, conversation_id))
            self._bump()
            return self._public_turn(self.conn.execute("SELECT * FROM chat_turns WHERE id=?", (turn_id,)).fetchone())

    def chat_turn(self, turn_id, user_id):
        self.expire_chat_jobs()
        with self._lock:
            row = self.conn.execute("SELECT * FROM chat_turns WHERE id=? AND user_id=?", (turn_id, user_id)).fetchone()
            if not row:
                raise KeyError("turn_not_found")
            return self._public_turn(row)

    def _server_chat_event(self, row, kind, error=None):
        seq = row["last_seq"] + 1
        event = {"seq": seq, "type": kind}
        if error:
            event["error"] = error
        now = self.clock()
        self.conn.execute("INSERT INTO chat_events VALUES(?,?,?,?)", (row["id"], seq, canonical_json(event), now))
        self.conn.execute("UPDATE chat_turns SET status=?,last_seq=?,error=?,completed_at=? WHERE id=?", (kind, seq, error, now, row["id"]))
        self.conn.execute("UPDATE chat_conversations SET updated_at=? WHERE id=?", (now, row["conversation_id"]))
        self._bump()

    def expire_chat_jobs(self):
        with self.transaction():
            rows = self.conn.execute("SELECT * FROM chat_turns WHERE status IN ('running','cancelling') AND lease_until<?", (self.clock(),)).fetchall()
            for row in rows:
                self._server_chat_event(row, "interrupted", "worker_lease_expired")

    def cancel_turn(self, turn_id, user_id):
        self.expire_chat_jobs()
        with self.transaction():
            row = self.conn.execute("SELECT * FROM chat_turns WHERE id=? AND user_id=?", (turn_id, user_id)).fetchone()
            if not row:
                raise KeyError("turn_not_found")
            if row["status"] not in TERMINAL:
                self.conn.execute("UPDATE chat_turns SET cancel_requested=1 WHERE id=?", (turn_id,))
                if row["status"] == "queued":
                    self._server_chat_event(row, "cancelled")
                else:
                    self.conn.execute("UPDATE chat_turns SET status='cancelling' WHERE id=?", (turn_id,))
                    self._bump()
            return self._public_turn(self.conn.execute("SELECT * FROM chat_turns WHERE id=?", (turn_id,)).fetchone())

    def claim_job(self, value):
        self.expire_chat_jobs()
        sid, groups = value["source_id"], set(value["group_refs"])
        with self.transaction():
            owned = {row[0] for row in self.conn.execute("SELECT group_ref FROM groups WHERE source_id=?", (sid,))}
            if not groups.issubset(owned):
                raise ValueError("claim_group_source_mismatch")
            job = None
            for row in self.conn.execute("SELECT t.* FROM chat_turns t JOIN users u ON u.id=t.user_id WHERE t.status='queued' AND u.disabled=0 ORDER BY t.created_at,t.rowid").fetchall():
                if set(json.loads(row["scope"])["group_refs"]).issubset(groups):
                    job = row
                    break
            if not job:
                return {"job": None}
            token, now = opaque_token(), self.clock()
            self.conn.execute("UPDATE chat_turns SET status='running',source_id=?,worker_id=?,lease_hash=?,lease_until=?,started_at=? WHERE id=? AND status='queued'", (sid, value["worker_id"], token_hash(token), now + 90, now, job["id"]))
            previous = self.conn.execute("SELECT content,response_text FROM chat_turns WHERE conversation_id=? AND status='completed' AND created_at<=? AND id<>? ORDER BY created_at DESC,rowid DESC LIMIT 10", (job["conversation_id"], job["created_at"], job["id"])).fetchall()
            history = []
            for old in reversed(previous):
                history.extend(({"role": "user", "content": old["content"]}, {"role": "assistant", "content": old["response_text"][:16000]}))
            self._bump()
            return {"job": {"turn_id": job["id"], "conversation_id": job["conversation_id"], "user_id": job["user_id"], "scope": json.loads(job["scope"]), "history": history, "prompt": job["content"], "lease_token": token, "next_seq": job["last_seq"] + 1}}

    def append_job_events(self, turn_id, value):
        from .protocol import JobEvent
        self.expire_chat_jobs()
        with self.transaction():
            row = self.conn.execute("SELECT * FROM chat_turns WHERE id=?", (turn_id,)).fetchone()
            if not row or row["source_id"] != value["source_id"] or not row["lease_hash"] or not hmac.compare_digest(row["lease_hash"], token_hash(value["lease_token"])):
                raise Conflict("invalid_job_lease")
            last_seq, status = row["last_seq"], row["status"]
            for incoming in value["events"]:
                event = JobEvent.model_validate(incoming).model_dump(exclude_none=True)
                seq, kind = event["seq"], event["type"]
                encoded = canonical_json(event)
                old = self.conn.execute("SELECT payload FROM chat_events WHERE turn_id=? AND seq=?", (turn_id, seq)).fetchone()
                if old:
                    if old[0] != encoded:
                        raise Conflict("job_event_sequence_conflict")
                    continue
                if seq != last_seq + 1:
                    raise Conflict("job_event_sequence_gap")
                if status in TERMINAL and kind != "heartbeat":
                    raise Conflict("job_already_terminal")
                now = self.clock()
                if kind == "completed":
                    scope = json.loads(row["scope"])
                    for ref in set(event["message_refs"]):
                        if not self.conn.execute("SELECT 1 FROM messages WHERE source_id=? AND message_ref=? AND group_ref IN (" + ",".join("?" for _ in scope["group_refs"]) + ") AND create_time>=? AND create_time<=?", (value["source_id"], ref, *scope["group_refs"], scope["since"], scope["until"])).fetchone():
                            raise ValueError("chat_reference_outside_scope")
                    self.conn.execute("UPDATE chat_turns SET response_text=?,message_refs=? WHERE id=?", (event["text"], canonical_json(list(dict.fromkeys(event["message_refs"]))), turn_id))
                elif kind == "delta":
                    length = self.conn.execute("SELECT length(response_text) FROM chat_turns WHERE id=?", (turn_id,)).fetchone()[0]
                    if length + len(event["text"]) > 262144:
                        raise ValueError("chat_response_too_large")
                    self.conn.execute("UPDATE chat_turns SET response_text=response_text||? WHERE id=?", (event["text"], turn_id))
                if kind in TERMINAL:
                    status = kind
                    self.conn.execute("UPDATE chat_turns SET status=?,completed_at=?,error=? WHERE id=?", (kind, now, event.get("error"), turn_id))
                self.conn.execute("INSERT INTO chat_events VALUES(?,?,?,?)", (turn_id, seq, encoded, now))
                self.conn.execute("UPDATE chat_turns SET last_seq=?,lease_until=? WHERE id=?", (seq, now + 90, turn_id))
                self.conn.execute("UPDATE chat_conversations SET updated_at=? WHERE id=?", (now, row["conversation_id"]))
                last_seq = seq
                self._bump()
            current = self.conn.execute("SELECT status,cancel_requested FROM chat_turns WHERE id=?", (turn_id,)).fetchone()
            return {"accepted": True, "last_seq": last_seq, "cancel_requested": bool(current["cancel_requested"]), "status": current["status"]}

    def restore_job_result(self, turn_id, value):
        """Deliver a previously completed result; never schedule inference again."""
        self.expire_chat_jobs()
        with self.transaction():
            row = self.conn.execute("SELECT * FROM chat_turns WHERE id=?", (turn_id,)).fetchone()
            if not row or row["source_id"] != value["source_id"] or not row["lease_hash"] or not hmac.compare_digest(row["lease_hash"], token_hash(value["lease_token"])):
                raise Conflict("invalid_job_lease")
            refs = list(dict.fromkeys(value["message_refs"]))
            if row["status"] == "completed":
                if row["response_text"] != value["text"] or json.loads(row["message_refs"]) != refs:
                    raise Conflict("chat_result_conflict")
                return {"accepted": True, "applied": False, "last_seq": row["last_seq"], "cancel_requested": bool(row["cancel_requested"]), "status": "completed"}
            if row["cancel_requested"] or row["status"] == "cancelled":
                raise Conflict("chat_result_cancelled")
            allowed = row["status"] in {"running", "cancelling"} or row["status"] == "interrupted" and row["error"] in {"worker_lease_expired", "local_worker_restarted"}
            if not allowed:
                raise Conflict("chat_result_not_recoverable")
            scope = json.loads(row["scope"])
            for ref in refs:
                if not self.conn.execute("SELECT 1 FROM messages WHERE source_id=? AND message_ref=? AND group_ref IN (" + ",".join("?" for _ in scope["group_refs"]) + ") AND create_time>=? AND create_time<=?", (value["source_id"], ref, *scope["group_refs"], scope["since"], scope["until"])).fetchone():
                    raise ValueError("chat_reference_outside_scope")
            now, seq = self.clock(), row["last_seq"] + 1
            event = {"seq": seq, "type": "completed", "text": value["text"], "message_refs": refs}
            self.conn.execute("INSERT INTO chat_events VALUES(?,?,?,?)", (turn_id, seq, canonical_json(event), now))
            self.conn.execute("UPDATE chat_turns SET status='completed',response_text=?,message_refs=?,last_seq=?,completed_at=?,error=NULL WHERE id=?", (value["text"], canonical_json(refs), seq, now, turn_id))
            self.conn.execute("UPDATE chat_conversations SET updated_at=? WHERE id=?", (now, row["conversation_id"]))
            self._bump()
            return {"accepted": True, "applied": True, "last_seq": seq, "cancel_requested": False, "status": "completed"}

    def chat_events(self, turn_id, user_id, after=0):
        self.chat_turn(turn_id, user_id)
        with self._lock:
            return [json.loads(row[0]) for row in self.conn.execute("SELECT payload FROM chat_events WHERE turn_id=? AND seq>? ORDER BY seq LIMIT 200", (turn_id, after))]

    def ingest_usage(self, value):
        applied = 0
        with self.transaction():
            for run in value["runs"]:
                if run["kind"] == "chat":
                    turn = self.conn.execute("SELECT user_id,source_id FROM chat_turns WHERE id=?", (run.get("turn_id"),)).fetchone()
                    if not turn or turn["user_id"] != run.get("user_id") or turn["source_id"] != value["source_id"]:
                        raise ValueError("usage_turn_owner_mismatch")
                elif run.get("user_id") is not None or run.get("turn_id") is not None:
                    raise ValueError("summary_usage_cannot_impersonate_user")
                encoded = canonical_json(run)
                previous = self.conn.execute("SELECT payload FROM usage_runs WHERE source_id=? AND run_id=?", (value["source_id"], run["run_id"])).fetchone()
                if previous:
                    if previous[0] != encoded:
                        raise Conflict("usage_run_conflict")
                    continue
                self.conn.execute("INSERT INTO usage_runs VALUES(?,?,?,?,?,?,?,?,?,?)", (value["source_id"], run["run_id"], run["kind"], run.get("user_id"), run.get("turn_id"), run["model"], run["started_at"], run["completed_at"], run["status"], encoded))
                applied += 1
            if applied:
                self._bump()
        return {"accepted": True, "applied": applied}

    def ingest_quota(self, value):
        now = self.clock()
        if value["sampled_at"] > now + 30:
            raise ValueError("quota_sample_time_in_future")
        with self.transaction():
            current = self.conn.execute("SELECT sampled_at,payload FROM quota_snapshots WHERE source_id=?", (value["source_id"],)).fetchone()
            if current and current[0] > value["sampled_at"]:
                return {"applied": False}
            encoded = canonical_json(value)
            if current and current[0] == value["sampled_at"]:
                if current[1] != encoded:
                    raise Conflict("quota_snapshot_conflict")
                return {"applied": False}
            successful = value["auth_state"] == "ready" and not value.get("error") and self._quota_has_values(value)
            if successful:
                self.conn.execute("INSERT INTO quota_snapshots(source_id,sampled_at,received_at,payload,last_success_at,last_success_payload) VALUES(?,?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET sampled_at=excluded.sampled_at,received_at=excluded.received_at,payload=excluded.payload,last_success_at=excluded.last_success_at,last_success_payload=excluded.last_success_payload", (value["source_id"], value["sampled_at"], now, encoded, value["sampled_at"], encoded))
            else:
                self.conn.execute("INSERT INTO quota_snapshots(source_id,sampled_at,received_at,payload) VALUES(?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET sampled_at=excluded.sampled_at,received_at=excluded.received_at,payload=excluded.payload", (value["source_id"], value["sampled_at"], now, encoded))
            self._bump()
        return {"applied": True}

    def usage(self, user_id, is_admin, since=None, until=None):
        if since is None:
            today = datetime.fromtimestamp(self.clock(), ZONE).date()
            since = datetime.combine(today - timedelta(days=6), day_time.min, ZONE).timestamp()
        start, end = self.time_range(since, until)
        with self._lock:
            if is_admin:
                rows = self.conn.execute("SELECT * FROM usage_runs WHERE completed_at>=? AND completed_at<=? ORDER BY completed_at DESC,run_id", (start, end)).fetchall()
            else:
                rows = self.conn.execute("SELECT * FROM usage_runs WHERE user_id=? AND completed_at>=? AND completed_at<=? ORDER BY completed_at DESC,run_id", (user_id, start, end)).fetchall()
            values = [json.loads(row["payload"]) for row in rows]
            def aggregate(items):
                known = sum(bool(item.get("usage") and any(item["usage"].get(k) is not None for k in TOKEN_FIELDS)) for item in items)
                result = {"requests": len(items), "usage_available_requests": known, "usage_unavailable_requests": len(items) - known}
                for key in TOKEN_FIELDS:
                    present = [item["usage"][key] for item in items if item.get("usage") and item["usage"].get(key) is not None]
                    result[key] = sum(present) if present else None
                return result
            by_kind, daily = {}, {}
            for run in values:
                by_kind.setdefault(run["kind"], []).append(run)
                day = datetime.fromtimestamp(run["completed_at"], ZONE).date().isoformat()
                daily.setdefault(day, []).append(run)
            quota = None
            if is_admin:
                snapshots = []
                for row in self.conn.execute("SELECT * FROM quota_snapshots ORDER BY source_id"):
                    item = json.loads(row["payload"])
                    current_available = self._quota_has_values(item)
                    last_success = row["last_success_at"]
                    age = max(0, self.clock() - last_success) if last_success is not None else None
                    if row["last_success_payload"]:
                        item["rate_limits"] = json.loads(row["last_success_payload"])["rate_limits"]
                    snapshots.append({**item, "checked_at": row["sampled_at"], "sampled_at": last_success, "last_success_at": last_success, "received_at": row["received_at"], "age_seconds": age, "stale": age is None or age > 90 or item["auth_state"] != "ready" or bool(item.get("error")) or not current_available})
                quota = {"provider": "codex_app_server", "binding": "unverified", "snapshots": snapshots}
            return {"scope": "all" if is_admin else "self", "totals": aggregate(values), "daily": [{"day": day, **aggregate(items)} for day, items in sorted(daily.items())], "by_kind": [{"kind": kind, **aggregate(items)} for kind, items in sorted(by_kind.items())], "recent": values[:100], "quota": quota}
