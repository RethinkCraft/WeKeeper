"""Durable cloud read model, independent from the private WeChat archive."""
from contextlib import contextmanager
from datetime import datetime, time as day_time
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import threading
import time
from zoneinfo import ZoneInfo

from .protocol import canonical_json, content_hash
from .security import opaque_token, password_hash, password_verify, token_hash
from .cloud_features import CloudFeaturesMixin, Conflict

ZONE = ZoneInfo("Asia/Shanghai")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL);
INSERT OR IGNORE INTO meta VALUES('revision',0);
INSERT OR IGNORE INTO meta VALUES('message_revision',0);
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,username TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,role TEXT NOT NULL CHECK(role IN ('admin','viewer')),disabled INTEGER NOT NULL DEFAULT 0,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY,user_id INTEGER NOT NULL REFERENCES users(id),csrf_token TEXT NOT NULL,expires_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS login_attempts(key TEXT PRIMARY KEY,count INTEGER NOT NULL,window_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS sources(source_id TEXT PRIMARY KEY,received_at REAL NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS groups(group_ref TEXT PRIMARY KEY,source_id TEXT NOT NULL,name TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS partitions(source_id TEXT NOT NULL,group_ref TEXT NOT NULL,day TEXT NOT NULL,revision INTEGER NOT NULL,content_hash TEXT NOT NULL,coverage TEXT NOT NULL,PRIMARY KEY(source_id,group_ref,day));
CREATE TABLE IF NOT EXISTS messages(source_id TEXT NOT NULL,message_ref TEXT NOT NULL,group_ref TEXT NOT NULL,day TEXT NOT NULL,create_time INTEGER NOT NULL,sender_id TEXT NOT NULL,sender_name TEXT NOT NULL,msg_type INTEGER NOT NULL,text TEXT NOT NULL,reply TEXT,PRIMARY KEY(source_id,message_ref));
CREATE INDEX IF NOT EXISTS messages_scope ON messages(group_ref,create_time,message_ref);
CREATE TABLE IF NOT EXISTS summaries(source_id TEXT NOT NULL,group_ref TEXT NOT NULL,day TEXT NOT NULL,revision INTEGER NOT NULL,input_hash TEXT NOT NULL,payload TEXT NOT NULL,stale INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(source_id,group_ref,day));
CREATE TABLE IF NOT EXISTS rules(id INTEGER PRIMARY KEY,name TEXT NOT NULL,group_ref TEXT,keyword TEXT NOT NULL,enabled INTEGER NOT NULL,effective_from REAL NOT NULL);
CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY,fingerprint TEXT UNIQUE NOT NULL,source_id TEXT,kind TEXT NOT NULL,severity TEXT NOT NULL,title TEXT NOT NULL,body TEXT NOT NULL,group_ref TEXT,message_ref TEXT,opened_at REAL NOT NULL,resolved_at REAL,updated_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS alerts_open ON alerts(resolved_at,opened_at);
CREATE TABLE IF NOT EXISTS alert_events(id INTEGER PRIMARY KEY,alert_id INTEGER NOT NULL REFERENCES alerts(id),kind TEXT NOT NULL,created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS alert_user_state(alert_id INTEGER NOT NULL REFERENCES alerts(id),user_id INTEGER NOT NULL REFERENCES users(id),read_at REAL,acknowledged_at REAL,PRIMARY KEY(alert_id,user_id));
CREATE TABLE IF NOT EXISTS candidates(key TEXT PRIMARY KEY,active INTEGER NOT NULL,count INTEGER NOT NULL,since REAL NOT NULL);
"""


class LoginThrottled(ValueError):
    pass


class DashboardStore(CloudFeaturesMixin):
    def __init__(self, data_dir, *, clock=time.time):
        self.root = Path(data_dir).expanduser()
        if self.root.is_symlink() or (self.root / "dashboard.sqlite").is_symlink():
            raise ValueError("unsafe_dashboard_state")
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.clock = clock
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.root / "dashboard.sqlite", isolation_level=None, check_same_thread=False, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=10000")
        if self.conn.execute("PRAGMA user_version").fetchone()[0] > 2:
            self.conn.close()
            raise ValueError("unsupported_dashboard_schema")
        self.conn.executescript(_SCHEMA)
        self._migrate_features()
        self.conn.execute("PRAGMA user_version=2")
        self._dummy_password = password_hash("not-a-user-password-constant")
        for filename in ("dashboard.sqlite", "dashboard.sqlite-wal", "dashboard.sqlite-shm"):
            path = self.root / filename
            if path.exists():
                os.chmod(path, 0o600)

    def close(self):
        with self._lock:
            self.conn.close()

    @contextmanager
    def transaction(self):
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def _bump(self):
        self.conn.execute("UPDATE meta SET value=value+1 WHERE key='revision'")

    def revision(self):
        with self._lock:
            return self.conn.execute("SELECT value FROM meta WHERE key='revision'").fetchone()[0]

    @staticmethod
    def _user(row):
        return {k: row[k] for k in ("id", "username", "role", "disabled", "created_at")}

    def create_user(self, username, password, role="admin"):
        if not isinstance(username, str) or not re.fullmatch(r"[A-Za-z0-9_.@-]{1,80}", username):
            raise ValueError("invalid_username")
        if role not in {"admin", "viewer"}:
            raise ValueError("invalid_role")
        hashed = password_hash(password)
        with self.transaction():
            try:
                result = self.conn.execute("INSERT INTO users(username,password_hash,role,created_at) VALUES(?,?,?,?)", (username, hashed, role, self.clock()))
            except sqlite3.IntegrityError:
                raise ValueError("username_exists") from None
            self._bump()
            return self._user(self.conn.execute("SELECT * FROM users WHERE id=?", (result.lastrowid,)).fetchone())

    def list_users(self):
        with self._lock:
            return [self._user(r) for r in self.conn.execute("SELECT * FROM users ORDER BY id")]

    def update_user(self, user_id, changes):
        values = {}
        if "password" in changes:
            values["password_hash"] = password_hash(changes["password"])
        if "role" in changes:
            if changes["role"] not in {"admin", "viewer"}:
                raise ValueError("invalid_role")
            values["role"] = changes["role"]
        if "disabled" in changes:
            values["disabled"] = int(changes["disabled"])
        with self.transaction():
            row = self.conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
            if not row:
                raise KeyError("user_not_found")
            if row["role"] == "admin" and not row["disabled"] and (values.get("role") == "viewer" or values.get("disabled")):
                if self.conn.execute("SELECT COUNT(*) FROM users WHERE role='admin' AND disabled=0").fetchone()[0] <= 1:
                    raise ValueError("last_admin_cannot_be_disabled")
            for key, value in values.items():
                self.conn.execute(f"UPDATE users SET {key}=? WHERE id=?", (value, user_id))
            if values:
                self.conn.execute("DELETE FROM sessions WHERE user_id=?", (user_id,))
                if values.get("disabled"):
                    self._cancel_user_turns(user_id)
                self._bump()
            return self._user(self.conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone())

    def login(self, username, password, remote=""):
        now = self.clock()
        attempt_key = hashlib.sha256((remote + "\0" + username.casefold()).encode()).hexdigest()
        with self.transaction():
            attempt = self.conn.execute("SELECT * FROM login_attempts WHERE key=?", (attempt_key,)).fetchone()
            if attempt and now - attempt["window_at"] < 900 and attempt["count"] >= 8:
                raise LoginThrottled("too_many_login_attempts")
            user = self.conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
            valid = password_verify(password, user["password_hash"] if user else self._dummy_password)
            if not user or user["disabled"] or not valid:
                count = attempt["count"] + 1 if attempt and now - attempt["window_at"] < 900 else 1
                start = attempt["window_at"] if count > 1 else now
                self.conn.execute("INSERT OR REPLACE INTO login_attempts VALUES(?,?,?)", (attempt_key, count, start))
                return None
            self.conn.execute("DELETE FROM login_attempts WHERE key=?", (attempt_key,))
            self.conn.execute("DELETE FROM sessions WHERE expires_at<?", (now,))
            token, csrf = opaque_token(), opaque_token()
            self.conn.execute("INSERT INTO sessions VALUES(?,?,?,?)", (token_hash(token), user["id"], csrf, now + 7 * 86400))
            return {"token": token, "csrf_token": csrf, "user": self._user(user)}

    def session(self, token):
        if not token:
            return None
        with self._lock:
            row = self.conn.execute("SELECT u.*,s.csrf_token FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=? AND s.expires_at>? AND u.disabled=0", (token_hash(token), self.clock())).fetchone()
            return {"user": self._user(row), "csrf_token": row["csrf_token"]} if row else None

    def logout(self, token):
        with self.transaction():
            self.conn.execute("DELETE FROM sessions WHERE token_hash=?", (token_hash(token),))

    def ingest_partition(self, payload):
        data = payload["data"]
        if content_hash(data) != payload["content_hash"]:
            raise ValueError("content_hash_mismatch")
        sid, group, day = payload["source_id"], payload["group_ref"], payload["day"]
        revision = payload["revision"]
        with self.transaction():
            current = self.conn.execute("SELECT * FROM partitions WHERE source_id=? AND group_ref=? AND day=?", (sid, group, day)).fetchone()
            if current and revision <= current["revision"]:
                if revision == current["revision"] and current["content_hash"] != payload["content_hash"]:
                    raise Conflict("partition_revision_conflict")
                return {"applied": False, "revision": current["revision"]}
            owner = self.conn.execute("SELECT source_id FROM groups WHERE group_ref=?", (group,)).fetchone()
            if owner and owner[0] != sid:
                raise Conflict("group_source_conflict")
            previous = {r[0] for r in self.conn.execute("SELECT message_ref FROM messages WHERE source_id=? AND group_ref=? AND day=?", (sid, group, day))}
            self.conn.execute("INSERT INTO groups VALUES(?,?,?) ON CONFLICT(group_ref) DO UPDATE SET name=excluded.name", (group, sid, data["group"]["name"]))
            self.conn.execute("DELETE FROM messages WHERE source_id=? AND group_ref=? AND day=?", (sid, group, day))
            for message in data["messages"]:
                existing = self.conn.execute("SELECT group_ref FROM messages WHERE source_id=? AND message_ref=?", (sid, message["message_ref"])).fetchone()
                if existing and existing[0] != group:
                    raise Conflict("message_group_conflict")
                self.conn.execute("INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_id,message_ref) DO UPDATE SET day=excluded.day,create_time=excluded.create_time,sender_id=excluded.sender_id,sender_name=excluded.sender_name,msg_type=excluded.msg_type,text=excluded.text,reply=excluded.reply", (sid, message["message_ref"], group, day, message["create_time"], message["sender_id"], message.get("sender_name", ""), message["msg_type"], message["text"], canonical_json(message["reply"]) if message.get("reply") else None))
                if not payload.get("initial") and message["message_ref"] not in previous:
                    self._keyword_alerts(sid, group, message)
            self.conn.execute("INSERT INTO partitions VALUES(?,?,?,?,?,?) ON CONFLICT(source_id,group_ref,day) DO UPDATE SET revision=excluded.revision,content_hash=excluded.content_hash,coverage=excluded.coverage", (sid, group, day, revision, payload["content_hash"], canonical_json(data.get("coverage", {}))))
            self.conn.execute("UPDATE summaries SET stale=1 WHERE source_id=? AND group_ref=? AND day=? AND input_hash<>?", (sid, group, day, payload["content_hash"]))
            self.conn.execute("UPDATE meta SET value=value+1 WHERE key='message_revision'")
            self._bump()
            return {"applied": True, "revision": revision}

    def ingest_summary(self, payload):
        sid, group, day = payload["source_id"], payload["group_ref"], payload["day"]
        with self.transaction():
            part = self.conn.execute("SELECT * FROM partitions WHERE source_id=? AND group_ref=? AND day=?", (sid, group, day)).fetchone()
            if not part or part["content_hash"] != payload["input_hash"]:
                raise Conflict("summary_input_stale")
            known = {r[0] for r in self.conn.execute("SELECT message_ref FROM messages WHERE source_id=? AND group_ref=? AND day=?", (sid, group, day))}
            for kind in ("topics", "decisions", "todos"):
                for item in payload["summary"].get(kind, []):
                    if not item["message_refs"] or not set(item["message_refs"]).issubset(known):
                        raise ValueError("summary_reference_not_in_partition")
            current = self.conn.execute("SELECT * FROM summaries WHERE source_id=? AND group_ref=? AND day=?", (sid, group, day)).fetchone()
            encoded = canonical_json(payload)
            if current and payload["revision"] <= current["revision"]:
                if payload["revision"] == current["revision"] and current["payload"] != encoded:
                    raise Conflict("summary_revision_conflict")
                if payload["revision"] == current["revision"] and current["stale"]:
                    # A source correction can return to the previous exact input.
                    # The citation/input checks above prove this replay is valid.
                    self.conn.execute("UPDATE summaries SET stale=0 WHERE source_id=? AND group_ref=? AND day=?", (sid, group, day))
                    self._bump()
                    return {"applied": True, "revision": current["revision"]}
                return {"applied": False, "revision": current["revision"]}
            self.conn.execute("INSERT INTO summaries VALUES(?,?,?,?,?,?,0) ON CONFLICT(source_id,group_ref,day) DO UPDATE SET revision=excluded.revision,input_hash=excluded.input_hash,payload=excluded.payload,stale=0", (sid, group, day, payload["revision"], payload["input_hash"], encoded))
            self._bump()
            return {"applied": True, "revision": payload["revision"]}

    def _incident(self, fingerprint, active, *, source_id=None, kind="runtime", severity="warning", title="", body="", group_ref=None, message_ref=None):
        now = self.clock()
        row = self.conn.execute("SELECT * FROM alerts WHERE fingerprint=?", (fingerprint,)).fetchone()
        if active and not row:
            result = self.conn.execute("INSERT INTO alerts(fingerprint,source_id,kind,severity,title,body,group_ref,message_ref,opened_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", (fingerprint, source_id, kind, severity, title, body, group_ref, message_ref, now, now))
            self.conn.execute("INSERT INTO alert_events(alert_id,kind,created_at) VALUES(?,'opened',?)", (result.lastrowid, now))
            self._bump()
        elif active and row and row["resolved_at"] is not None:
            self.conn.execute("UPDATE alerts SET opened_at=?,resolved_at=NULL,updated_at=?,title=?,body=? WHERE id=?", (now, now, title, body, row["id"]))
            self.conn.execute("DELETE FROM alert_user_state WHERE alert_id=?", (row["id"],))
            self.conn.execute("INSERT INTO alert_events(alert_id,kind,created_at) VALUES(?,'reopened',?)", (row["id"], now))
            self._bump()
        elif not active and row and row["resolved_at"] is None:
            self.conn.execute("UPDATE alerts SET resolved_at=?,updated_at=? WHERE id=?", (now, now, row["id"]))
            self.conn.execute("INSERT INTO alert_events(alert_id,kind,created_at) VALUES(?,'resolved',?)", (row["id"], now))
            self._bump()

    def _observe(self, sid, kind, active, *, title, body, uncertain=False):
        key, now = f"{sid}:{kind}", self.clock()
        row = self.conn.execute("SELECT * FROM candidates WHERE key=?", (key,)).fetchone()
        count = row["count"] + 1 if row and row["active"] == int(active) else 1
        since = row["since"] if count > 1 else now
        self.conn.execute("INSERT OR REPLACE INTO candidates VALUES(?,?,?,?)", (key, int(active), count, since))
        delay = 300 if active and uncertain else 25
        if count >= 2 and now - since >= delay:
            self._incident(key, active, source_id=sid, kind=kind, title=title, body=body)

    def ingest_heartbeat(self, payload):
        sid, now = payload["source_id"], self.clock()
        state = payload["health"]["state"]
        collector, summary = payload["collector"], payload.get("summary", {})
        with self.transaction():
            self.conn.execute("INSERT INTO sources VALUES(?,?,?) ON CONFLICT(source_id) DO UPDATE SET received_at=excluded.received_at,payload=excluded.payload", (sid, now, canonical_json(payload)))
            self._observe(sid, "sync_interrupted", False, title="本机同步已中断", body="云端超过 90 秒没有收到本机心跳，当前数据可能已过期。")
            # Primary health states can hide another probe's missing evidence.
            # Only a verified session/source state proves recovery; an unknown
            # reading or a different fault must not clear an existing incident.
            verified = state in {"receiving", "session_present", "sync_unverified"}
            if verified or state in {"login_required", "not_running"}:
                self._observe(sid, "wechat_session", state in {"login_required", "not_running"}, title="微信需要处理", body="微信未运行或需要重新登录，请检查采集机器。")
            if verified or state == "network_unavailable":
                self._observe(sid, "network", state == "network_unavailable", title="本机网络不可用", body="采集机器报告网络不可用。")
            if verified or state == "source_unavailable":
                self._observe(sid, "source", state == "source_unavailable", title="归档来源不可用", body="采集器无法读取微信来源。")
            cycle = collector.get("last_cycle_at")
            # Collector age is computed using the source's reported send time, not the cloud clock.
            stalled = isinstance(cycle, (int, float)) and payload["sent_at"] - cycle > 90
            self._observe(sid, "collector", not collector.get("running") or bool(collector.get("last_error")) or state == "collector_error" or stalled, title="采集异常", body="采集器已停止、长时间未完成采集或报告错误。")
            self._observe(sid, "unverified", state in {"unknown", "sync_unverified"}, title="消息同步需要核实", body="目前无法确认消息收取状态；安静群不代表掉线。", uncertain=True)
            self._observe(sid, "summary", summary.get("state") in {"error", "paused", "authorization_required", "policy_blocked", "quota_exceeded"}, title="自动摘要需要处理", body="摘要授权、额度或运行出现异常，请查看本机摘要状态。")
            sync = payload.get("sync") or {}
            self._observe(sid, "sync_error", sync.get("state") == "error", title="数据同步异常", body="本机心跳正常，但数据扫描或上传失败；云端统计可能尚未更新，请检查同步状态和待传分区。")
            self._bump()
            # Metadata-only reconciliation lets the source recover a cloud
            # restore without exporting another group's data or rerunning AI.
            partitions = [dict(row) for row in self.conn.execute(
                "SELECT group_ref,day,revision,content_hash FROM partitions WHERE source_id=? ORDER BY group_ref,day", (sid,))]
            summaries = [dict(row) for row in self.conn.execute(
                "SELECT group_ref,day,revision,input_hash FROM summaries WHERE source_id=? AND stale=0 ORDER BY group_ref,day", (sid,))]
            avatars = [dict(row) for row in self.conn.execute(
                "SELECT group_ref,sender_id,avatar_hash FROM sender_avatars WHERE source_id=? ORDER BY group_ref,sender_id", (sid,))]
            usage_count = self.conn.execute("SELECT COUNT(*) FROM usage_runs WHERE source_id=?", (sid,)).fetchone()[0]
        return {"accepted": True, "received_at": now, "partitions": partitions, "summaries": summaries, "avatars": avatars, "usage_count": usage_count}

    def check_stale_sources(self):
        now = self.clock()
        with self.transaction():
            for row in self.conn.execute("SELECT source_id,received_at FROM sources").fetchall():
                if now - row["received_at"] > 90:
                    self._incident(f"{row['source_id']}:sync_interrupted", True, source_id=row["source_id"], kind="sync_interrupted", title="本机同步已中断", body="云端超过 90 秒没有收到本机心跳，当前数据可能已过期。")
                    key = f"{row['source_id']}:sync_interrupted"
                    candidate = self.conn.execute("SELECT active FROM candidates WHERE key=?", (key,)).fetchone()
                    if not candidate or not candidate[0]:
                        self.conn.execute("INSERT OR REPLACE INTO candidates VALUES(?,1,1,?)", (key, now))

    def _keyword_alerts(self, sid, group, message):
        for rule in self.conn.execute("SELECT * FROM rules WHERE enabled=1 AND (group_ref IS NULL OR group_ref=?)", (group,)):
            if message["create_time"] < rule["effective_from"] or rule["keyword"].casefold() not in message["text"].casefold():
                continue
            self._incident(f"keyword:{rule['id']}:{message['message_ref']}", True, source_id=sid, kind="keyword", title=rule["name"], body=message["text"][:1000], group_ref=group, message_ref=message["message_ref"])

    def list_rules(self):
        with self._lock:
            return [dict(row) for row in self.conn.execute("SELECT * FROM rules ORDER BY id")]

    def save_rule(self, data, rule_id=None):
        with self.transaction():
            prior = self.conn.execute("SELECT * FROM rules WHERE id=?", (rule_id,)).fetchone() if rule_id is not None else None
            if rule_id is not None and not prior:
                raise KeyError("rule_not_found")
            merged = {**(dict(prior) if prior else {}), **data}
            name, keyword = merged.get("name", ""), merged.get("keyword", "")
            if not isinstance(name, str) or not 1 <= len(name.strip()) <= 200 or not isinstance(keyword, str) or not 1 <= len(keyword.strip()) <= 200:
                raise ValueError("invalid_keyword_rule")
            if merged.get("enabled", True) is None:
                raise ValueError("invalid_keyword_rule")
            group = merged.get("group_ref") or None
            if group and not self.conn.execute("SELECT 1 FROM groups WHERE group_ref=?", (group,)).fetchone():
                raise ValueError("unknown_group")
            if rule_id is None:
                result = self.conn.execute("INSERT INTO rules(name,group_ref,keyword,enabled,effective_from) VALUES(?,?,?,?,?)", (name.strip(), group, keyword, int(merged.get("enabled", True)), self.clock()))
                rule_id = result.lastrowid
            else:
                self.conn.execute("UPDATE rules SET name=?,group_ref=?,keyword=?,enabled=?,effective_from=? WHERE id=?", (name.strip(), group, keyword, int(merged.get("enabled", True)), self.clock(), rule_id))
            self._bump()
            return dict(self.conn.execute("SELECT * FROM rules WHERE id=?", (rule_id,)).fetchone())

    def delete_rule(self, rule_id):
        with self.transaction():
            if not self.conn.execute("DELETE FROM rules WHERE id=?", (rule_id,)).rowcount:
                raise KeyError("rule_not_found")
            self._bump()

    def alerts(self, user_id, state="all"):
        if state not in {"all", "open", "resolved"}:
            raise ValueError("invalid_alert_state")
        condition = "" if state == "all" else " WHERE a.resolved_at IS " + ("NULL" if state == "open" else "NOT NULL")
        with self._lock:
            return [dict(row) for row in self.conn.execute("SELECT a.id,a.source_id,a.kind,a.severity,a.title,a.body,a.group_ref,a.message_ref,a.opened_at,a.resolved_at,a.updated_at,u.read_at,u.acknowledged_at FROM alerts a LEFT JOIN alert_user_state u ON u.alert_id=a.id AND u.user_id=?" + condition + " ORDER BY (a.resolved_at IS NULL) DESC,a.opened_at DESC LIMIT 500", (user_id,))]

    def mark_alert(self, alert_id, user_id, *, acknowledge=False):
        with self.transaction():
            if not self.conn.execute("SELECT 1 FROM alerts WHERE id=?", (alert_id,)).fetchone():
                raise KeyError("alert_not_found")
            self.conn.execute("INSERT INTO alert_user_state(alert_id,user_id,read_at,acknowledged_at) VALUES(?,?,?,?) ON CONFLICT(alert_id,user_id) DO UPDATE SET read_at=COALESCE(alert_user_state.read_at,excluded.read_at),acknowledged_at=COALESCE(alert_user_state.acknowledged_at,excluded.acknowledged_at)", (alert_id, user_id, self.clock(), self.clock() if acknowledge else None))
            self._bump()
        return {"ok": True}

    def time_range(self, since=None, until=None):
        def parse(value, default):
            if value is None or value == "":
                return default
            try:
                return float(value)
            except (TypeError, ValueError):
                try:
                    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=ZONE)
                    return dt.timestamp()
                except (ValueError, OverflowError):
                    raise ValueError("invalid_time_range") from None
        now = self.clock()
        day = datetime.fromtimestamp(now, ZONE).date()
        start = parse(since, datetime.combine(day, day_time.min, ZONE).timestamp())
        end = parse(until, now)
        if not (0 <= start <= end <= 253402214400):
            raise ValueError("invalid_time_range")
        return start, end

    def groups(self, since=None, until=None):
        start, end = self.time_range(since, until)
        with self._lock:
            return [dict(row) for row in self.conn.execute("SELECT g.group_ref,g.name,g.source_id,COUNT(m.message_ref) messages,COUNT(DISTINCT NULLIF(m.sender_id,'')) active_senders,MAX(m.create_time) last_message_at FROM groups g LEFT JOIN messages m ON m.group_ref=g.group_ref AND m.create_time>=? AND m.create_time<=? GROUP BY g.group_ref ORDER BY messages DESC,g.name", (start, end))]

    def _coverage(self, group):
        parts = self.conn.execute("SELECT day,coverage FROM partitions WHERE group_ref=? ORDER BY day", (group,)).fetchall()
        coverage = [json.loads(p["coverage"]) for p in parts]
        available = [c.get("source_available") for c in coverage]
        source_available = False if False in available else True if available and all(value is True for value in available) else None
        return {"local_backfill_complete": bool(parts) and all(c.get("local_backfill_complete", False) for c in coverage), "source_available": source_available, "gaps": sorted({g for c in coverage for g in c.get("gaps", [])}), "days": [p["day"] for p in parts]}

    def _source(self, source_id):
        row = self.conn.execute("SELECT * FROM sources WHERE source_id=?", (source_id,)).fetchone()
        if not row:
            return None
        return {**json.loads(row["payload"]), "received_at": row["received_at"], "stale": self.clock() - row["received_at"] > 90}

    def group_stats(self, group, since=None, until=None):
        start, end = self.time_range(since, until)
        with self._lock:
            info = self.conn.execute("SELECT name,source_id FROM groups WHERE group_ref=?", (group,)).fetchone()
            if not info:
                raise KeyError("group_not_found")
            scope = (group, start, end)
            totals = dict(self.conn.execute("SELECT COUNT(*) messages,COUNT(DISTINCT NULLIF(sender_id,'')) active_senders FROM messages WHERE group_ref=? AND create_time>=? AND create_time<=?", scope).fetchone())
            trend = [dict(row) for row in self.conn.execute("SELECT day,COUNT(*) messages FROM messages WHERE group_ref=? AND create_time>=? AND create_time<=? GROUP BY day ORDER BY day", scope)]
            types = [dict(row) for row in self.conn.execute("SELECT msg_type,COUNT(*) messages FROM messages WHERE group_ref=? AND create_time>=? AND create_time<=? GROUP BY msg_type ORDER BY messages DESC", scope)]
            senders = [dict(row) for row in self.conn.execute("""WITH scoped AS (
                SELECT sender_id,sender_name,create_time,message_ref FROM messages
                WHERE group_ref=? AND create_time>=? AND create_time<=? AND sender_id<>''),
                latest AS (SELECT sender_id,sender_name,ROW_NUMBER() OVER(PARTITION BY sender_id ORDER BY create_time DESC,message_ref DESC) position FROM scoped),
                counts AS (SELECT sender_id,COUNT(*) messages FROM scoped GROUP BY sender_id)
                SELECT c.sender_id,n.sender_name,c.messages FROM counts c JOIN latest n ON n.sender_id=c.sender_id AND n.position=1
                ORDER BY c.messages DESC,c.sender_id LIMIT 50""", scope)]
            for sender in senders:
                sender["avatar_url"] = self._avatar_url(group, sender["sender_id"])
            return {"group_ref": group, "name": info[0], **totals, "trend": trend, "types": types, "senders": senders, "coverage": self._coverage(group), "source": self._source(info["source_id"])}

    def _message(self, row):
        return {"avatar_url": self._avatar_url(row["group_ref"], row["sender_id"]), "message_ref": row["message_ref"], "group_ref": row["group_ref"], "create_time": row["create_time"], "sender_id": row["sender_id"], "sender_name": row["sender_name"], "msg_type": row["msg_type"], "text": row["text"], "reply": json.loads(row["reply"]) if row["reply"] else None}

    def messages(self, group, since=None, until=None, q=None, limit=50, before=None):
        import base64
        start, end = self.time_range(since, until)
        if not 1 <= limit <= 200 or q and len(q) > 512:
            raise ValueError("invalid_message_query")
        cursor_data = None
        if before:
            if len(before) > 3000:
                raise ValueError("invalid_cursor")
            try:
                cursor_data = json.loads(base64.urlsafe_b64decode(before + "=" * (-len(before) % 4)))
                if (not isinstance(cursor_data, dict) or type(cursor_data["stamp"]) is not int
                        or not isinstance(cursor_data["ref"], str) or len(cursor_data["ref"]) > 256
                        or cursor_data["group"] != group or cursor_data["q"] != q
                        or cursor_data["start"] != start
                        or until is not None and cursor_data["end"] != end):
                    raise ValueError()
                end = cursor_data["end"]
            except Exception:
                raise ValueError("invalid_cursor") from None
        args = [group, start, end]
        extra = ""
        if q:
            extra += " AND instr(lower(text),lower(?))>0"
            args.append(q)
        if cursor_data:
            extra += " AND (create_time<? OR (create_time=? AND message_ref<?))"
            args.extend((cursor_data["stamp"], cursor_data["stamp"], cursor_data["ref"]))
        args.append(limit + 1)
        with self._lock:
            if not self.conn.execute("SELECT 1 FROM groups WHERE group_ref=?", (group,)).fetchone():
                raise KeyError("group_not_found")
            message_revision = self.conn.execute("SELECT value FROM meta WHERE key='message_revision'").fetchone()[0]
            if cursor_data and cursor_data.get("revision") != message_revision:
                raise Conflict("cursor_snapshot_changed")
            rows = self.conn.execute("SELECT * FROM messages WHERE group_ref=? AND create_time>=? AND create_time<=?" + extra + " ORDER BY create_time DESC,message_ref DESC LIMIT ?", args).fetchall()
            page = rows[:limit]
            cursor = None
            if len(rows) > limit:
                value = {"stamp": page[-1]["create_time"], "ref": page[-1]["message_ref"], "group": group, "start": start, "end": end, "q": q, "revision": message_revision}
                cursor = base64.urlsafe_b64encode(canonical_json(value).encode()).decode().rstrip("=")
            return {"messages": [self._message(r) for r in page], "next_cursor": cursor, "revision": message_revision, "coverage": self._coverage(group)}

    def message_context(self, ref):
        with self._lock:
            rows = self.conn.execute("SELECT * FROM messages WHERE message_ref=? LIMIT 2", (ref,)).fetchall()
            if len(rows) != 1:
                raise KeyError("message_not_found")
            row = rows[0]
            before = self.conn.execute("SELECT * FROM messages WHERE group_ref=? AND (create_time<? OR (create_time=? AND message_ref<?)) ORDER BY create_time DESC,message_ref DESC LIMIT 5", (row["group_ref"], row["create_time"], row["create_time"], ref)).fetchall()
            after = self.conn.execute("SELECT * FROM messages WHERE group_ref=? AND (create_time>? OR (create_time=? AND message_ref>?)) ORDER BY create_time,message_ref LIMIT 5", (row["group_ref"], row["create_time"], row["create_time"], ref)).fetchall()
            message = self._message(row)
            target = None
            if message["reply"] and message["reply"].get("target_ref"):
                item = self.conn.execute("SELECT * FROM messages WHERE source_id=? AND group_ref=? AND message_ref=?", (row["source_id"], row["group_ref"], message["reply"]["target_ref"])).fetchone()
                target = self._message(item) if item else None
            return {"message": message, "context": [self._message(r) for r in reversed(before)] + [message] + [self._message(r) for r in after], "target": target}

    def summaries(self, group):
        with self._lock:
            return [{**json.loads(row["payload"]), "stale": bool(row["stale"])} for row in self.conn.execute("SELECT payload,stale FROM summaries WHERE group_ref=? ORDER BY day DESC LIMIT 100", (group,))]

    def overview(self, user_id, since=None, until=None):
        start, end = self.time_range(since, until)
        self.check_stale_sources()
        with self._lock:
            totals = dict(self.conn.execute("SELECT COUNT(*) messages,COUNT(DISTINCT NULLIF(sender_id,'')) active_senders FROM messages WHERE create_time>=? AND create_time<=?", (start, end)).fetchone())
            totals["groups"] = self.conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
            totals["open_alerts"] = self.conn.execute("SELECT COUNT(*) FROM alerts WHERE resolved_at IS NULL").fetchone()[0]
            trend = [dict(row) for row in self.conn.execute("SELECT day,COUNT(*) messages FROM messages WHERE create_time>=? AND create_time<=? GROUP BY day ORDER BY day", (start, end))]
            sources = [{**json.loads(row["payload"]), "received_at": row["received_at"], "stale": self.clock() - row["received_at"] > 90} for row in self.conn.execute("SELECT * FROM sources ORDER BY source_id")]
            return {"revision": self.revision(), "totals": totals, "groups": self.groups(start, end), "sources": sources, "trend": trend, "alerts": self.alerts(user_id, "open")[:10]}

    def backup(self, destination):
        path = Path(destination)
        if path.is_symlink() or path.resolve() == (self.root / "dashboard.sqlite").resolve():
            raise ValueError("unsafe_backup_destination")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self._lock:
            target = sqlite3.connect(path)
            try:
                self.conn.backup(target)
            finally:
                target.close()
        os.chmod(path, 0o600)
        return path


Store = DashboardStore
