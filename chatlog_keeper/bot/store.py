"""Private robot state. The WeChat archive is never opened for writing here."""
from __future__ import annotations

import copy
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import threading
from zoneinfo import ZoneInfo

from .persona import default_persona, normalize_persona
from .request_policy import CHAT_ONLY, POLICY_KEY, build_native_policy, request_capability


SHANGHAI = ZoneInfo("Asia/Shanghai")
REPLY_STATES = frozenset({"awaiting_generation", "pending", "sending", "sent", "unknown", "failed", "cancelled"})


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_time(value) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, (float, int)) and not isinstance(value, bool):
        result = datetime.fromtimestamp(value, timezone.utc)
    elif isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError("invalid_event_time")
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("event_timezone_required")
    return result


def business_day(value) -> str:
    return parse_time(value).astimezone(SHANGHAI).date().isoformat()


def _identifier(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValueError("invalid_bot_identifier")
    return value.strip()


def _unique_rows(values, key="id"):
    if not isinstance(values, list) or len(values) > 1000:
        raise ValueError("invalid_bot_configuration")
    seen = set()
    for row in values:
        if not isinstance(row, dict):
            raise ValueError("invalid_bot_configuration")
        row[key] = _identifier(row.get(key))
        if row[key] in seen:
            raise ValueError("duplicate_bot_identifier")
        seen.add(row[key])
    return seen


def normalize_config(config):
    if not isinstance(config, dict):
        raise ValueError("invalid_bot_configuration")
    config = copy.deepcopy(config)
    config.pop("revision", None)
    config.setdefault("enabled", False)
    if not isinstance(config["enabled"], bool):
        raise ValueError("invalid_bot_configuration")
    config.setdefault("providers", config.pop("profiles", []))
    config.setdefault("personas", [default_persona()])
    config.setdefault("businesses", [])
    config.setdefault("groups", [])
    providers = _unique_rows(config["providers"])
    personas = _unique_rows(config["personas"])
    businesses = _unique_rows(config["businesses"])
    _unique_rows(config["groups"], "group_ref")
    for profile in config["providers"]:
        profile.setdefault("kind", profile.pop("provider", "compatible"))
        if profile["kind"] not in {"siwc", "deepseek", "compatible"}:
            raise ValueError("unsupported_bot_provider")
        if any(key in profile for key in ("api_key", "access_token", "refresh_token", "password")):
            raise ValueError("bot_credentials_require_reference")
    config["personas"] = [normalize_persona(persona) for persona in config["personas"]]
    for business in config["businesses"]:
        business.setdefault("name", business["id"])
        business.setdefault("reporter_ids", business.pop("reporters", []))
        if not isinstance(business["reporter_ids"], list):
            raise ValueError("invalid_bot_reporters")
        business["reporter_ids"] = list(dict.fromkeys(_identifier(v) for v in business["reporter_ids"]))
        business.setdefault("items", [])
        _unique_rows(business["items"])
        aliases = {}
        for item in business["items"]:
            item["name"] = _identifier(item.get("name", item["id"]))
            if not isinstance(item.get("aliases", []), list):
                raise ValueError("invalid_bot_item_alias")
            item["aliases"] = list(dict.fromkeys(_identifier(v) for v in item.get("aliases", [])))
            for alias in (item["id"], item["name"], *item["aliases"]):
                if alias.casefold() in aliases and aliases[alias.casefold()] != item["id"]:
                    raise ValueError("ambiguous_bot_item_alias")
                aliases[alias.casefold()] = item["id"]
    for group in config["groups"]:
        group.setdefault("enabled", False)
        group.setdefault("provider_id", group.pop("profile_id", None))
        if not isinstance(group["enabled"], bool) or group.get("business_id") not in businesses:
            raise ValueError("invalid_bot_group_binding")
        if group.get("provider_id") is not None and group["provider_id"] not in providers:
            raise ValueError("invalid_bot_provider_binding")
        if group.get("persona_id") is not None and group["persona_id"] not in personas:
            raise ValueError("invalid_bot_persona_binding")
        if group.get("bot_wxid"):
            group["bot_wxid"] = _identifier(group["bot_wxid"])
    return config


class BotStore:
    def __init__(self, state_dir):
        self.directory = Path(state_dir).expanduser() / "bot"
        if self.directory.is_symlink():
            raise ValueError("unsafe_bot_state")
        self.directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = self.directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
            raise ValueError("unsafe_bot_state")
        self.directory.chmod(0o700)
        self.path = self.directory / "bot.sqlite"
        for target in (self.path, Path(str(self.path) + "-wal"), Path(str(self.path) + "-shm")):
            if target.is_symlink():
                raise ValueError("unsafe_bot_state")
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
        CREATE TABLE IF NOT EXISTS requests(
          event_id TEXT PRIMARY KEY,message_ref TEXT NOT NULL UNIQUE,group_ref TEXT NOT NULL,
          sender_id TEXT NOT NULL,text TEXT NOT NULL,occurred_at TEXT NOT NULL,event_ts REAL NOT NULL,
          event_order INTEGER NOT NULL,event TEXT NOT NULL,business_id TEXT NOT NULL,
          profile_id TEXT,persona_id TEXT,persona_version TEXT,profile TEXT,persona TEXT,
          status TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS offers(
          business_id TEXT NOT NULL,day TEXT NOT NULL,item_id TEXT NOT NULL,status TEXT NOT NULL,
          version INTEGER NOT NULL,event_id TEXT NOT NULL,event_ts REAL NOT NULL,event_order INTEGER NOT NULL,
          message_ref TEXT NOT NULL,group_ref TEXT NOT NULL,sender_id TEXT NOT NULL,updated_at TEXT NOT NULL,
          PRIMARY KEY(business_id,day,item_id));
        CREATE TABLE IF NOT EXISTS reports(
          event_id TEXT NOT NULL,item_id TEXT NOT NULL,status TEXT NOT NULL,result TEXT NOT NULL,
          PRIMARY KEY(event_id,item_id));
        CREATE TABLE IF NOT EXISTS admin_commands(
          event_id TEXT PRIMARY KEY,payload_hash TEXT NOT NULL,result TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit_log(
          id INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT NOT NULL,event_id TEXT,
          business_id TEXT,day TEXT,item_id TEXT,payload TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS reply_outbox(
          reply_id TEXT PRIMARY KEY,event_id TEXT NOT NULL UNIQUE,group_ref TEXT NOT NULL,
          text TEXT,reply_to TEXT NOT NULL,status TEXT NOT NULL,error TEXT,confirmation_ref TEXT,
          created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS bot_replies_status ON reply_outbox(status,created_at);
        CREATE INDEX IF NOT EXISTS bot_history ON requests(group_ref,persona_id,persona_version,event_ts);
        """)
        self._migrate_replies()
        with self.lock:
            metadata = dict(self.conn.execute(
                "SELECT key,value FROM meta WHERE key IN ('config','revision','schema')"))
            if (not {'config', 'revision', 'schema'} <= metadata.keys()
                    or metadata['schema'] != '2'):
                # Warm MCP processes must not FULL-commit a constant schema
                # marker. Keep first initialization/repair atomic and race
                # safe: another process can finish after this read.
                with self.conn:
                    self.conn.execute("INSERT OR IGNORE INTO meta VALUES('config',?)", (canonical_json(normalize_config({})),))
                    self.conn.execute("INSERT OR IGNORE INTO meta VALUES('revision','0')")
                    self.conn.execute("INSERT INTO meta VALUES('schema','2') ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE meta.value IS NOT excluded.value")

    def _migrate_replies(self):
        if "kind" in {row[1] for row in self.conn.execute("PRAGMA table_info(reply_outbox)")}:
            return
        # Preserve all uncertain/sent records before changing the unique key.
        if self.conn.execute("SELECT 1 FROM reply_outbox LIMIT 1").fetchone():
            backup = self.directory / "bot-schema-1.sqlite"
            if backup.is_symlink():
                raise ValueError("unsafe_bot_backup")
            if not backup.exists():
                with sqlite3.connect(backup) as destination:
                    self.conn.backup(destination)
                backup.chmod(0o600)
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            self.conn.execute("ALTER TABLE reply_outbox RENAME TO reply_outbox_v1")
            self.conn.execute("""CREATE TABLE reply_outbox(
              reply_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,group_ref TEXT NOT NULL,
              text TEXT,reply_to TEXT NOT NULL,status TEXT NOT NULL,error TEXT,confirmation_ref TEXT,
              created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
              kind TEXT NOT NULL DEFAULT 'final' CHECK(kind IN ('waiting','final')),
              UNIQUE(event_id,kind))""")
            self.conn.execute("INSERT INTO reply_outbox SELECT *, 'final' FROM reply_outbox_v1")
            self.conn.execute("DROP TABLE reply_outbox_v1")
            self.conn.execute("CREATE INDEX bot_replies_status ON reply_outbox(status,created_at)")

    @staticmethod
    def now():
        return datetime.now(timezone.utc).isoformat()

    def get(self, key, default=None):
        with self.lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return json.loads(row[0]) if row else default

    def set(self, key, value):
        self.set_many({key: value})

    @staticmethod
    def _metadata_rows(values):
        # Validate and serialize the whole batch before starting a write. In
        # particular, a protected key cannot leave earlier keys half-applied.
        if any(key in {"config", "revision", "schema"} for key in values):
            raise ValueError("protected_bot_metadata")
        return [(key, canonical_json(value)) for key, value in values.items()]

    def _write_metadata(self, rows):
        """Caller owns the lock and transaction; never enter conn again here."""
        self.conn.executemany("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE meta.value<>excluded.value", rows)

    def set_many(self, values):
        """Publish a metadata snapshot in one durable, atomic transaction."""
        rows = self._metadata_rows(values)
        if not rows:
            return
        with self.lock:
            # Heartbeats/control loops often publish the same value. Avoid a
            # write transaction entirely, while checking under the same lock
            # as writers so a concurrent change cannot be discarded.
            changed = [row for row in rows if (old := self.conn.execute(
                "SELECT value FROM meta WHERE key=?", (row[0],)).fetchone()) is None or old[0] != row[1]]
            if changed:
                with self.conn:
                    self._write_metadata(changed)

    def config(self):
        with self.lock:
            value = self.get("config")
            value["revision"] = self.get("revision", 0)
            return value

    def configure(self, config, expected_revision=None, *, control=None):
        config = normalize_config(config)
        if expected_revision is not None and (not isinstance(expected_revision, int)
                or isinstance(expected_revision, bool) or expected_revision < 0):
            raise ValueError("invalid_bot_configuration_revision")
        if control is not None:
            if not isinstance(control, dict):
                raise ValueError("invalid_bot_control")
            command_id = _identifier(control.get("command_id"))
            cloud_revision = control.get("revision")
            if not isinstance(cloud_revision, int) or isinstance(cloud_revision, bool) or cloud_revision <= 0:
                raise ValueError("invalid_bot_control_revision")
            command_key = "config_" + command_id
            command_hash = hashlib.sha256(canonical_json({"config": config, "revision": cloud_revision}).encode()).hexdigest()
        with self.lock, self.conn:
            # BEGIN IMMEDIATE also serializes configuration writers in other processes.
            self.conn.execute("BEGIN IMMEDIATE")
            if control is not None:
                previous = self.conn.execute("SELECT * FROM admin_commands WHERE event_id=?", (command_key,)).fetchone()
                if previous:
                    if previous["payload_hash"] != command_hash:
                        raise ValueError("bot_control_identity_conflict")
                    return json.loads(previous["result"])["revision"]
                if cloud_revision <= self.get("cloud_config_revision", 0):
                    raise ValueError("bot_configuration_revision_stale")
            revision = self.get("revision", 0)
            if expected_revision is not None and expected_revision != revision:
                raise ValueError("bot_configuration_conflict")
            revision += 1
            self.conn.execute("UPDATE meta SET value=? WHERE key='config'", (canonical_json(config),))
            self.conn.execute("UPDATE meta SET value=? WHERE key='revision'", (canonical_json(revision),))
            if control is not None:
                outcome = {"status": "applied", "config_revision": cloud_revision}
                for key, value in (("cloud_config_revision", cloud_revision), ("control_" + command_id, outcome)):
                    self.conn.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, canonical_json(value)))
                self.conn.execute("INSERT INTO admin_commands VALUES(?,?,?)", (command_key, command_hash, canonical_json({"revision": revision, "outcome": outcome})))
            for row in self.conn.execute("SELECT reply_id,group_ref FROM reply_outbox WHERE status IN ('awaiting_generation','pending')").fetchall():
                binding = self._binding(config, row["group_ref"])
                if not binding or not binding["enabled"]:
                    self.conn.execute("UPDATE reply_outbox SET status='cancelled',error='bot_disabled',updated_at=? WHERE reply_id=?", (self.now(), row["reply_id"]))
            baselines = self.get("group_baselines", {})
            active = {group: value for group, value in baselines.items()
                      if (self._binding(config, group) or {}).get("enabled")}
            if active != baselines:
                self.conn.execute("INSERT INTO meta VALUES('group_baselines',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (canonical_json(active),))
            self._audit("configuration", payload={"revision": revision})
            return revision

    @staticmethod
    def _binding(config, group_ref):
        group = next((dict(g) for g in config["groups"] if g["group_ref"] == group_ref), None)
        if group is None:
            return None
        group["enabled"] = bool(config["enabled"] and group["enabled"])
        group["business"] = next((dict(b) for b in config["businesses"] if b["id"] == group["business_id"]), None)
        group["profile"] = next((dict(p) for p in config["providers"] if p["id"] == group.get("provider_id")), None)
        group["persona"] = next((dict(p) for p in config["personas"] if p["id"] == group.get("persona_id")), None)
        return group

    def binding(self, group_ref):
        return self._binding(self.config(), group_ref)

    @staticmethod
    def _request(row):
        if row is None:
            return None
        result = dict(row)
        for key in ("event", "profile", "persona"):
            result[key] = json.loads(result[key]) if result[key] is not None else None
        return result

    def request(self, event_id):
        with self.lock:
            return self._request(self.conn.execute("SELECT * FROM requests WHERE event_id=?", (event_id,)).fetchone())

    def pending_generations(self, limit=100):
        """Accepted requests whose durable final has never entered the sender.

        Their event cursor was committed at acceptance; recovery must schedule
        them independently of the archive's later cursor, without reaccepting
        an event or changing its frozen identity/configuration/timestamp.
        """
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError('invalid_bot_request_limit')
        with self.lock:
            return [self._request(row) for row in self.conn.execute(
                "SELECT r.* FROM requests r JOIN reply_outbox o USING(event_id) "
                "WHERE o.kind='final' AND o.status='awaiting_generation' AND r.status='processing' "
                "ORDER BY r.event_order,r.event_ts,r.event_id LIMIT ?", (limit,))]

    def begin_request(self, event, *, profile_id=None, persona_id=None, persona_version=None, event_cursor=None,
                      native_tool_plan=False, native_chat_policy=False):
        event = copy.deepcopy(event)
        if type(native_tool_plan) is not bool:
            raise ValueError("invalid_native_tool_plan_option")
        if type(native_chat_policy) is not bool:
            raise ValueError("invalid_native_chat_policy_option")
        # Accepted plans are created locally, never trusted from incoming data.
        event.pop("_native_tool_plan", None)
        event.pop(POLICY_KEY, None)
        message_ref = _identifier(event.get("message_ref"))
        event_id = _identifier(event.get("event_id", message_ref))
        group_ref = _identifier(event.get("group_ref"))
        sender_id = _identifier(event.get("sender_id"))
        text = event.get("text")
        if not isinstance(text, str) or not text.strip() or len(text) > 16000:
            raise ValueError("invalid_bot_message")
        timestamp = parse_time(event.get("occurred_at", event.get("timestamp", event.get("sent_at"))))
        occurred_at = timestamp.isoformat()
        event_order = event.get("archive_id", 0)
        if not isinstance(event_order, int) or isinstance(event_order, bool):
            raise ValueError("invalid_bot_event_order")
        if event_cursor is not None and (type(event_cursor) is not int or event_cursor < 0 or str(event_cursor) != event_id):
            raise ValueError("invalid_bot_event_cursor")
        event.update(event_id=event_id, message_ref=message_ref, group_ref=group_ref,
                     sender_id=sender_id, text=text, occurred_at=occurred_at)
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            old = self.conn.execute("SELECT * FROM requests WHERE event_id=? OR message_ref=?", (event_id, message_ref)).fetchone()
            if old:
                original = self._request(old)
                if any(original[key] != event[key] for key in ("event_id", "message_ref", "group_ref", "sender_id", "text")):
                    raise ValueError("bot_event_identity_conflict")
                return {"accepted": False, "request": original, **original}
            binding = self.binding(group_ref)
            if not binding or not binding["enabled"]:
                raise ValueError("bot_group_disabled")
            profile = binding["profile"]
            persona = binding["persona"] or default_persona()
            if profile_id is not None and profile_id != binding.get("provider_id"):
                raise ValueError("bot_profile_binding_changed")
            if persona_id is not None and persona_id != persona["id"]:
                raise ValueError("bot_persona_binding_changed")
            if persona_version is not None and str(persona_version) != str(persona["version"]):
                raise ValueError("bot_persona_version_changed")
            if native_tool_plan:
                from .direct_plan import PLAN_KEY, build_native_plan
                plan = build_native_plan(event, binding["business"], current_day=business_day(self.now()))
                if plan is not None:
                    event[PLAN_KEY] = plan
            if native_chat_policy:
                policy = build_native_policy(event, binding["business"])
                if policy is not None:
                    event[POLICY_KEY] = policy
            self.conn.execute("""INSERT INTO requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                              (event_id, message_ref, group_ref, sender_id, text, occurred_at,
                               timestamp.timestamp(), event_order, canonical_json(event), binding["business_id"],
                               binding.get("provider_id"), persona["id"], str(persona["version"]),
                               canonical_json(profile) if profile else None, canonical_json(persona), "processing", self.now()))
            reply_id = "reply_" + hashlib.sha256(event_id.encode()).hexdigest()[:24]
            reply_to = event.get("reply_to") or {"message_ref": message_ref, "group_ref": group_ref}
            self.conn.execute("INSERT INTO reply_outbox VALUES(?,?,?,?,?,?,NULL,NULL,?,?,'final')",
                              (reply_id, event_id, group_ref, None, canonical_json(reply_to), "awaiting_generation", self.now(), self.now()))
            if event_cursor is not None:
                # Acceptance, cursor and its exact configuration snapshot must
                # survive together before inference is allowed to begin.
                current_cursor = self.get("event_cursor", 0)
                if type(current_cursor) is not int or event_cursor < current_cursor:
                    raise ValueError("bot_event_cursor_regressed")
                self._write_metadata(self._metadata_rows({"event_cursor": event_cursor,
                    "active": {"group_ref": group_ref, "provider": (profile or {}).get("kind"),
                        "profile_id": (profile or {}).get("id"), "model": (profile or {}).get("model"),
                        "reasoning_effort": (profile or {}).get("reasoning_effort", "low"),
                        "persona_id": persona["id"], "persona_version": int(persona["version"])}}))
            request = self.request(event_id)
            return {"accepted": True, "request": request, **request}

    def set_request_status(self, event_id, status):
        if status not in {"processing", "generated", "completed", "interrupted", "failed", "cancelled"}:
            raise ValueError("invalid_bot_request_status")
        with self.lock, self.conn:
            self.conn.execute("UPDATE requests SET status=? WHERE event_id=?", (status, event_id))

    def history(self, group_ref, persona_id, version, limit=10):
        if not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("invalid_bot_history_limit")
        with self.lock:
            rows = self.conn.execute("""SELECT r.text AS user_text,o.text AS assistant_text
              FROM requests r JOIN reply_outbox o USING(event_id)
              WHERE r.group_ref=? AND r.persona_id=? AND r.persona_version=? AND r.status='completed' AND o.status='sent' AND o.kind='final'
              ORDER BY r.event_ts DESC,r.event_order DESC,r.event_id DESC LIMIT ?""",
              (group_ref, persona_id, str(version), limit)).fetchall()
        result = []
        for row in reversed(rows):
            result.extend(({"role": "user", "content": row["user_text"]},
                           {"role": "assistant", "content": row["assistant_text"]}))
        return result

    def offer_status(self, business_id, day, item_id):
        with self.lock:
            row = self.conn.execute("SELECT * FROM offers WHERE business_id=? AND day=? AND item_id=?", (business_id, day, item_id)).fetchone()
        return dict(row) if row else {"business_id": business_id, "day": day, "item_id": item_id,
                                     "status": "unknown", "version": 0, "message_ref": None,
                                     "updated_at": None, "sender_id": None, "group_ref": None}

    def offer_statuses(self, business_id, day, item_ids):
        """Read selected projects from one SQLite snapshot, in caller order.

        The single SELECT retains its read snapshot until fetchall completes,
        including when a separate MCP/admin connection commits concurrently.
        Reading this business/day together also avoids a variable-count limit
        for a configuration containing many projects.
        """
        item_ids = tuple(item_ids)
        if not item_ids:
            return []
        with self.lock:
            rows = self.conn.execute(
                "SELECT * FROM offers WHERE business_id=? AND day=?", (business_id, day)).fetchall()
        by_item = {row["item_id"]: row for row in rows}
        return [dict(by_item[item_id]) if item_id in by_item else {
            "business_id": business_id, "day": day, "item_id": item_id,
            "status": "unknown", "version": 0, "message_ref": None,
            "updated_at": None, "sender_id": None, "group_ref": None,
        } for item_id in item_ids]

    def _audit(self, kind, *, event_id=None, business_id=None, day=None, item_id=None, payload=None):
        self.conn.execute("INSERT INTO audit_log(kind,event_id,business_id,day,item_id,payload,created_at) VALUES(?,?,?,?,?,?,?)",
                          (kind, event_id, business_id, day, item_id, canonical_json(payload or {}), self.now()))

    def record_offer(self, event_id, item_id, status, *, expected_version=None, evidence=None):
        """Called only after Service validates trusted-source evidence and authorization."""
        if status not in {"available", "unavailable", "unknown"}:
            raise ValueError("invalid_offer_status")
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            request = self.request(event_id)
            if request is None:
                raise ValueError("bot_request_missing")
            if request_capability(request) == CHAT_ONLY:
                return {"ok": False, "error": "bot_business_capability_denied"}
            previous = self.conn.execute("SELECT status,result FROM reports WHERE event_id=? AND item_id=?", (event_id, item_id)).fetchone()
            if previous:
                if previous["status"] != status:
                    return {"ok": False, "error": "event_already_reported"}
                return {**json.loads(previous["result"]), "replayed": True}
            # Service may have checked processing before another connection
            # cancelled the request. Recheck under this write transaction;
            # committed receipts above remain readable without any new write.
            if request["status"] not in {"processing", "interrupted"}:
                return {"ok": False, "error": "bot_request_closed"}
            # configure() can cancel the final reply while a model is still
            # running, before the scheduler closes its request. Re-enabling the
            # group must not authorize a fresh write from that cancelled task.
            # Existing receipts above remain replayable without another write.
            reply = self.reply(event_id)
            if reply is not None and reply["status"] == "cancelled":
                return {"ok": False, "error": "bot_request_closed"}
            binding = self.binding(request["group_ref"])
            if not binding or not binding["enabled"] or binding["business_id"] != request["business_id"]:
                return {"ok": False, "error": "bot_binding_changed"}
            if request["event"].get("transport") == "native_protocol":
                # A model may have passed Service authorization before the
                # native account binding changed. Recheck under the same
                # transaction as the write; legacy archive IDs remain opaque.
                own = request["event"]["native_source"]["ownUsername"]
                configured = binding.get("bot_wxid") or self.get("bot_wxid")
                if configured and configured != own:
                    return {"ok": False, "error": "bot_binding_changed"}
            if request["sender_id"] not in binding["business"]["reporter_ids"]:
                return {"ok": False, "error": "report_not_authorized"}
            if not any(item["id"] == item_id for item in binding["business"]["items"]):
                return {"ok": False, "error": "unknown_offer_item"}
            day = business_day(request["occurred_at"])
            current = self.offer_status(request["business_id"], day, item_id)
            if expected_version is not None and expected_version != current["version"]:
                return {"ok": False, "error": "offer_version_conflict", "current": current}
            incoming_order = (request["event_ts"], request["event_order"], request["message_ref"])
            old_order = (current.get("event_ts", float("-inf")), current.get("event_order", 0), current.get("message_ref") or "")
            if incoming_order <= old_order:
                result = {"ok": False, "error": "stale_report", "current": current}
            else:
                version = current["version"] + 1
                self.conn.execute("""INSERT INTO offers VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                  ON CONFLICT(business_id,day,item_id) DO UPDATE SET status=excluded.status,
                  version=excluded.version,event_id=excluded.event_id,event_ts=excluded.event_ts,
                  event_order=excluded.event_order,message_ref=excluded.message_ref,group_ref=excluded.group_ref,
                  sender_id=excluded.sender_id,updated_at=excluded.updated_at""",
                  (request["business_id"], day, item_id, status, version, event_id,
                   request["event_ts"], request["event_order"], request["message_ref"],
                   request["group_ref"], request["sender_id"], request["occurred_at"]))
                result = {"ok": True, "recorded": True, **self.offer_status(request["business_id"], day, item_id)}
            self.conn.execute("INSERT INTO reports VALUES(?,?,?,?)", (event_id, item_id, status, canonical_json(result)))
            self._audit("report" if result["ok"] else "stale_report", event_id=event_id,
                        business_id=request["business_id"], day=day, item_id=item_id,
                        payload={"before": current, "result": result, "evidence": evidence})
            # The placeholder is durable in the same transaction as a business write.
            self.conn.execute("UPDATE reply_outbox SET updated_at=? WHERE event_id=?", (self.now(), event_id))
            return result

    def receipt(self, event_id):
        """Committed tool results, for truthful recovery without another inference."""
        with self.lock:
            rows = self.conn.execute("SELECT result FROM reports WHERE event_id=? ORDER BY item_id", (event_id,)).fetchall()
        results = [json.loads(row["result"]) for row in rows]
        if not results:
            return None
        if len(results) == 1:
            return {**results[0], "results": results}
        return {"ok": all(result.get("ok") for result in results),
                "recorded": any(result.get("recorded") for result in results), "results": results}

    def adjust_offer(self, payload):
        """Apply an authenticated web-admin edit. This is never a model tool."""
        import uuid
        if not isinstance(payload, dict):
            raise ValueError("invalid_offer_adjustment")
        business_id = _identifier(payload.get("business_id"))
        item_id = _identifier(payload.get("item_id"))
        day = payload.get("day")
        try:
            if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day:
                raise ValueError("invalid_offer_date")
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid_offer_date") from exc
        status = payload.get("status")
        if status not in {"available", "unavailable", "unknown"}:
            raise ValueError("invalid_offer_status")
        expected = payload.get("expected_revision", payload.get("expected_version"))
        if not isinstance(expected, int) or isinstance(expected, bool) or expected < 0:
            raise ValueError("offer_revision_required")
        actor = _identifier(payload.get("actor"))
        event_id = _identifier(payload.get("event_id", payload.get("command_id")) or "admin_" + uuid.uuid4().hex)
        command_hash = hashlib.sha256(canonical_json({"business_id": business_id, "item_id": item_id,
            "day": day, "status": status, "expected_revision": expected, "actor": actor}).encode()).hexdigest()
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            previous = self.conn.execute("SELECT * FROM admin_commands WHERE event_id=?", (event_id,)).fetchone()
            if previous:
                if previous["payload_hash"] != command_hash:
                    return {"ok": False, "error": "admin_command_identity_conflict"}
                return {**json.loads(previous["result"]), "replayed": True}
            business = next((row for row in self.config()["businesses"] if row["id"] == business_id), None)
            if not business or not any(row["id"] == item_id for row in business["items"]):
                result = {"ok": False, "error": "unknown_offer_item"}
                self.conn.execute("INSERT INTO admin_commands VALUES(?,?,?)", (event_id, command_hash, canonical_json(result)))
                return result
            current = self.offer_status(business_id, day, item_id)
            if expected != current["version"]:
                result = {"ok": False, "error": "offer_version_conflict", "current": current}
                self.conn.execute("INSERT INTO admin_commands VALUES(?,?,?)", (event_id, command_hash, canonical_json(result)))
                return result
            now = self.now()
            version = current["version"] + 1
            self.conn.execute("""INSERT INTO offers VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
              ON CONFLICT(business_id,day,item_id) DO UPDATE SET status=excluded.status,
              version=excluded.version,event_id=excluded.event_id,event_ts=excluded.event_ts,
              event_order=excluded.event_order,message_ref=excluded.message_ref,group_ref=excluded.group_ref,
              sender_id=excluded.sender_id,updated_at=excluded.updated_at""",
              (business_id, day, item_id, status, version, event_id,
               parse_time(now).timestamp(), 0, event_id, "", actor, now))
            result = {"ok": True, "recorded": True, **self.offer_status(business_id, day, item_id)}
            self._audit("admin_adjustment", event_id=event_id, business_id=business_id,
                        day=day, item_id=item_id, payload={"actor": actor, "before": current, "result": result})
            self.conn.execute("INSERT INTO admin_commands VALUES(?,?,?)", (event_id, command_hash, canonical_json(result)))
            return result

    @staticmethod
    def _reply(row):
        if row is None:
            return None
        value = dict(row)
        value["reply_to"] = json.loads(value["reply_to"])
        return value

    def reply(self, reply_id):
        with self.lock:
            return self._reply(self.conn.execute("SELECT * FROM reply_outbox WHERE reply_id=? OR (event_id=? AND kind='final')", (reply_id, reply_id)).fetchone())

    def queue_waiting(self, event_id):
        """One host-owned receipt; never generated by a model and never replayed."""
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            final = self.reply(event_id)
            request = self.request(event_id)
            if not final or not request or final["status"] != "awaiting_generation":
                return None
            reply_id = "waiting_" + hashlib.sha256(event_id.encode()).hexdigest()[:24]
            self.conn.execute("INSERT OR IGNORE INTO reply_outbox VALUES(?,?,?,?,?,'pending',NULL,NULL,?,?,'waiting')",
                (reply_id, event_id, request["group_ref"], "稍等，我查一下。", canonical_json(final["reply_to"]), self.now(), self.now()))
            return self.reply(reply_id)

    def queue_reply(self, event_id, text, reply_to=None, *, metadata=None):
        if not isinstance(text, str) or not text.strip() or len(text) > 4000:
            raise ValueError("invalid_bot_reply")
        rows = self._metadata_rows(metadata or {})
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            request = self.request(event_id)
            current = self.reply(event_id)
            if request is None or current is None:
                raise ValueError("bot_request_missing")
            if current["status"] != "awaiting_generation":
                if current["text"] == text:
                    return current
                raise ValueError("bot_reply_already_generated")
            binding = self.binding(request["group_ref"])
            if not binding or not binding["enabled"] or binding["business_id"] != request["business_id"]:
                self.conn.execute("UPDATE reply_outbox SET status='cancelled',error='bot_binding_changed',updated_at=? WHERE event_id=? AND status IN ('awaiting_generation','pending')", (self.now(), event_id))
                return self.reply(event_id)
            if reply_to is not None:
                if isinstance(reply_to, str):
                    reply_to = {"message_ref": reply_to, "group_ref": request["group_ref"]}
                if reply_to.get("message_ref") != request["message_ref"] or reply_to.get("group_ref", request["group_ref"]) != request["group_ref"]:
                    raise ValueError("bot_quote_target_mismatch")
            else:
                reply_to = current["reply_to"]
            # Metrics and the final reply become visible together. Do not call
            # set_many here: its connection context would commit this transaction.
            self._write_metadata(rows)
            self.conn.execute("UPDATE reply_outbox SET text=?,reply_to=?,status='pending',updated_at=? WHERE event_id=? AND kind='final'", (text, canonical_json(reply_to), self.now(), event_id))
            self.conn.execute("UPDATE reply_outbox SET status='cancelled',error='final_ready',updated_at=? WHERE event_id=? AND kind='waiting' AND status='pending'", (self.now(), event_id))
            self.conn.execute("UPDATE requests SET status='generated' WHERE event_id=?", (event_id,))
            return self.reply(event_id)

    def pending_replies(self, limit=20):
        if not isinstance(limit, int) or not 1 <= limit <= 500:
            raise ValueError("invalid_bot_reply_limit")
        with self.lock:
            rows = self.conn.execute("SELECT * FROM reply_outbox WHERE status='pending' ORDER BY CASE kind WHEN 'final' THEN 0 ELSE 1 END,created_at,reply_id LIMIT ?", (limit,)).fetchall()
            return [self._reply(row) for row in rows]

    def transition_reply(self, reply_id, status, *, error=None, confirmation_ref=None, metadata=None):
        allowed = {"awaiting_generation": {"cancelled", "failed"}, "pending": {"sending", "cancelled", "failed"},
                   "sending": {"sent", "unknown", "failed", "cancelled"},
                   "unknown": {"sent", "cancelled"}, "sent": set(), "failed": set(), "cancelled": set()}
        if status not in REPLY_STATES:
            raise ValueError("invalid_bot_reply_status")
        rows = self._metadata_rows(metadata or {})
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            current = self.reply(reply_id)
            if current is None:
                raise ValueError("bot_reply_missing")
            if current["status"] == status:
                return current
            if status not in allowed[current["status"]]:
                raise ValueError("invalid_bot_reply_transition")
            if status == "sent" and not confirmation_ref:
                raise ValueError("bot_send_confirmation_required")
            self._write_metadata(rows)
            self.conn.execute("UPDATE reply_outbox SET status=?,error=?,confirmation_ref=?,updated_at=? WHERE reply_id=?",
                              (status, error, confirmation_ref, self.now(), current["reply_id"]))
            if current["kind"] == "final" and status in {"sent", "unknown", "failed", "cancelled"}:
                request_status = {"sent": "completed", "unknown": "interrupted", "failed": "failed", "cancelled": "cancelled"}[status]
                if status == "sent" and self.get("request_metrics_" + current["event_id"], {}).get("error"):
                    request_status = "failed"
                self.conn.execute("UPDATE requests SET status=? WHERE event_id=?", (request_status, current["event_id"]))
            self._audit("reply_" + status, event_id=current["event_id"], payload={"reply_id": current["reply_id"], "error": error, "confirmation_ref": confirmation_ref})
            return self.reply(current["reply_id"])

    def defer_unstarted_reply(self, reply_id, error, *, metadata=None):
        """Host-only: retry only after the sender proves Send was never clicked.

        Keep this separate from transitions so an uncertain or failed send cannot
        accidentally be treated as retryable. The native sender owns that proof.
        """
        if not isinstance(error, str) or not error or len(error) > 160:
            raise ValueError("bot_unstarted_send_reason_required")
        rows = self._metadata_rows(metadata or {})
        with self.lock, self.conn:
            self.conn.execute("BEGIN IMMEDIATE")
            current = self.reply(reply_id)
            if current is None:
                raise ValueError("bot_reply_missing")
            if current["status"] != "sending":
                raise ValueError("invalid_bot_reply_transition")
            if current["kind"] == "waiting" and self.reply(current["event_id"])["status"] != "awaiting_generation":
                self._write_metadata(self._metadata_rows({**(metadata or {}),
                    "send_retry_" + reply_id: None, "last_sender_error": None}))
                self.conn.execute("UPDATE reply_outbox SET status='cancelled',error='final_ready',updated_at=? WHERE reply_id=?",
                                  (self.now(), current["reply_id"]))
                return self.reply(current["reply_id"])
            self._write_metadata(rows)
            self.conn.execute("UPDATE reply_outbox SET status='pending',error=?,updated_at=? WHERE reply_id=?", (error, self.now(), current["reply_id"]))
            self._audit("reply_deferred_unstarted", event_id=current["event_id"],
                        payload={"reply_id": current["reply_id"], "error": error})
            return self.reply(current["reply_id"])

    def audit(self, limit=100):
        if not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("invalid_bot_audit_limit")
        with self.lock:
            rows = self.conn.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def snapshot(self):
        with self.lock:
            offers = [dict(row) for row in self.conn.execute("SELECT * FROM offers ORDER BY day DESC,business_id,item_id")]
            replies = [self._reply(row) for row in self.conn.execute("SELECT * FROM reply_outbox ORDER BY created_at DESC LIMIT 100")]
            return {"config": self.config(), "offers": offers, "replies": replies, "audit": self.audit()}

    def close(self):
        self.conn.close()
