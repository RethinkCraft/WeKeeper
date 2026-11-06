"""Admin control plane; only a local worker can attest effective bot state."""
from __future__ import annotations

from datetime import date
import json
import re
import unicodedata
import uuid
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator

from .protocol import WireModel, canonical_json

IDENTIFIER = r"^[A-Za-z0-9_-]+$"
STATES = {"unknown", "available", "unavailable"}


def _state(value):
    return "unknown" if value == "unreported" else value


class BotPersona(WireModel):
    id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    name: str = Field(min_length=1, max_length=100)
    version: int = Field(default=1, ge=1)
    markdown: str = Field(min_length=1, max_length=16000)

    @model_validator(mode="before")
    @classmethod
    def migrate_sections(cls, value):
        from chatlog_keeper.bot.persona import normalize_persona
        return normalize_persona(value)


class BotProvider(WireModel):
    id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    name: str = Field(min_length=1, max_length=100)
    kind: str = Field(pattern=r"^(siwc|deepseek|compatible)$")
    model: str = Field(default="", max_length=200)
    base_url: str | None = Field(default=None, max_length=1000)
    credential_ref: str | None = Field(default=None, max_length=200, pattern=IDENTIFIER)
    reasoning_effort: str = Field(default="low", pattern=r"^(none|low|medium|high|xhigh|max)$")

    @model_validator(mode="after")
    def valid_endpoint(self):
        if self.base_url:
            parsed = urlsplit(self.base_url)
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
                raise ValueError("bot_https_endpoint_required")
        if self.kind == "compatible" and not self.base_url:
            raise ValueError("bot_endpoint_required")
        if self.kind != "siwc" and (not self.model.strip() or not self.credential_ref):
            raise ValueError("bot_model_and_credential_reference_required")
        return self


class BotItem(WireModel):
    id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    name: str = Field(min_length=1, max_length=100)
    aliases: list[str] = Field(default_factory=list, max_length=30)

    @field_validator("name")
    @classmethod
    def nonempty_name(cls, value):
        if not value.strip():
            raise ValueError("empty_bot_item_name")
        return value.strip()

    @field_validator("aliases")
    @classmethod
    def bounded_aliases(cls, values):
        if any(not v.strip() or len(v) > 100 for v in values):
            raise ValueError("invalid_bot_alias")
        return list(dict.fromkeys(v.strip() for v in values))


class BotBusiness(WireModel):
    id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    name: str = Field(min_length=1, max_length=100)
    reporter_ids: list[str] = Field(default_factory=list, max_length=500)
    items: list[BotItem] = Field(default_factory=list, max_length=100)

    @field_validator("reporter_ids")
    @classmethod
    def anonymous_reporters(cls, values):
        if len(set(values)) != len(values) or any(not re.fullmatch(r"sender_[A-Za-z0-9_-]{1,200}", v) for v in values):
            raise ValueError("bot_reporters_require_member_references")
        return values

    @model_validator(mode="after")
    def unique_items(self):
        if len({i.id for i in self.items}) != len(self.items):
            raise ValueError("duplicate_bot_item")
        aliases = {}
        for item in self.items:
            for alias in [item.name, *item.aliases]:
                key = unicodedata.normalize("NFKC", alias).casefold().strip()
                if key in aliases and aliases[key] != item.id:
                    raise ValueError("ambiguous_bot_item_alias")
                aliases[key] = item.id
        return self


class BotGroup(WireModel):
    group_ref: str = Field(min_length=1, max_length=256, pattern=IDENTIFIER)
    business_id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    persona_id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    provider_id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    enabled: bool = False


def _default_personas():
    from chatlog_keeper.bot.persona import default_persona
    return [BotPersona.model_validate(default_persona())]


def _default_providers():
    return [BotProvider(id="gpt", name="ChatGPT 授权", kind="siwc", model="")]


class BotConfig(WireModel):
    enabled: bool = False
    businesses: list[BotBusiness] = Field(default_factory=list, max_length=100)
    personas: list[BotPersona] = Field(default_factory=_default_personas, max_length=100)
    providers: list[BotProvider] = Field(default_factory=_default_providers, max_length=100)
    groups: list[BotGroup] = Field(default_factory=list, max_length=100)
    history_turns: int = Field(default=10, ge=0, le=10)
    nearby_messages: int = Field(default=5, ge=0, le=5)

    @model_validator(mode="after")
    def references(self):
        for values, key in ((self.businesses, "id"), (self.personas, "id"), (self.providers, "id"), (self.groups, "group_ref")):
            if len({getattr(v, key) for v in values}) != len(values):
                raise ValueError("duplicate_bot_configuration_id")
        for group in self.groups:
            if (group.business_id not in {b.id for b in self.businesses}
                    or group.persona_id not in {p.id for p in self.personas}
                    or group.provider_id not in {p.id for p in self.providers}):
                raise ValueError("invalid_bot_configuration_reference")
        return self


class BotConfigUpdate(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=IDENTIFIER)
    expected_revision: int = Field(ge=0)
    data: BotConfig


class BotStatus(WireModel):
    business_id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    item_id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    day: str
    status: str = Field(pattern=r"^(unknown|available|unavailable)$")
    revision: int = Field(ge=0)
    updated_at: float = Field(ge=0)
    actor: str | None = Field(default=None, max_length=256)
    message_ref: str | None = Field(default=None, max_length=256, pattern=IDENTIFIER)
    group_ref: str | None = Field(default=None, max_length=256, pattern=IDENTIFIER)

    @field_validator("status", mode="before")
    @classmethod
    def legacy_state(cls, value):
        return _state(value)

    @field_validator("day")
    @classmethod
    def valid_day(cls, value):
        if date.fromisoformat(value).isoformat() != value:
            raise ValueError("invalid_bot_day")
        return value

    @field_validator("actor")
    @classmethod
    def no_native_identity(cls, value):
        if value and (value.startswith("wxid_") or "@chatroom" in value):
            raise ValueError("bot_native_identity_must_remain_local")
        return value


class BotStatusUpdate(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=IDENTIFIER)
    business_id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    item_id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    day: str
    status: str = Field(pattern=r"^(unknown|available|unavailable)$")
    expected_revision: int = Field(ge=0)

    _day = field_validator("day")(BotStatus.valid_day.__func__)
    _legacy = field_validator("status", mode="before")(BotStatus.legacy_state.__func__)


class BotAudit(BotStatus):
    id: str = Field(min_length=1, max_length=200, pattern=IDENTIFIER)
    created_at: float = Field(ge=0)
    previous_status: str | None = Field(default=None, pattern=r"^(unknown|available|unavailable)$")


class BotActiveConfig(WireModel):
    group_ref: str | None = Field(default=None, max_length=256, pattern=IDENTIFIER)
    provider: str | None = Field(default=None, max_length=80, pattern=IDENTIFIER)
    profile_id: str | None = Field(default=None, max_length=80, pattern=IDENTIFIER)
    model: str | None = Field(default=None, max_length=200)
    reasoning_effort: str | None = Field(default=None, max_length=40, pattern=IDENTIFIER)
    persona_id: str | None = Field(default=None, max_length=80, pattern=IDENTIFIER)
    persona_version: int | None = Field(default=None, ge=1)
    persona_name: str | None = Field(default=None, max_length=100)


class BotLastRequest(WireModel):
    request_id: str | None = Field(default=None, max_length=256, pattern=r"^[A-Za-z0-9_.:-]+$")
    group_ref: str | None = Field(default=None, max_length=256, pattern=IDENTIFIER)
    status: str = Field(default="unknown", max_length=100, pattern=IDENTIFIER)
    started_at: float | None = Field(default=None, ge=0)
    finished_at: float | None = Field(default=None, ge=0)
    trigger_to_model_seconds: float | None = Field(default=None, ge=0)
    generation_seconds: float | None = Field(default=None, ge=0)
    send_confirmation_seconds: float | None = Field(default=None, ge=0)
    end_to_end_seconds: float | None = Field(default=None, ge=0)
    visible_reply_seconds: float | None = Field(default=None, ge=0)
    error: str | None = Field(default=None, max_length=160, pattern=IDENTIFIER)

    @model_validator(mode="after")
    def chronological(self):
        if self.started_at is not None and self.finished_at is not None and self.finished_at < self.started_at:
            raise ValueError("invalid_bot_runtime_times")
        return self


class BotCounts(WireModel):
    requests: int = Field(default=0, ge=0, strict=True)
    replied: int = Field(default=0, ge=0, strict=True)
    pending: int = Field(default=0, ge=0, strict=True)
    failed: int = Field(default=0, ge=0, strict=True)
    unknown: int = Field(default=0, ge=0, strict=True)


class BotLatencyMetric(WireModel):
    count: int = Field(ge=0, le=500, strict=True)
    p50_seconds: float | None = Field(default=None, ge=0)
    p95_seconds: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def available_samples(self):
        if self.count == 0 and (self.p50_seconds is not None or self.p95_seconds is not None):
            raise ValueError("bot_latency_requires_samples")
        if self.p50_seconds is not None and self.p95_seconds is not None and self.p95_seconds < self.p50_seconds:
            raise ValueError("invalid_bot_latency_percentiles")
        return self


class BotStaleEvent(WireModel):
    kind: str = Field(max_length=40, pattern=r"^[a-z][a-z0-9_]{0,39}$")
    request_id: str | None = Field(default=None, max_length=200)
    age_seconds: int | None = Field(default=None, ge=0)
    at: str | None = Field(default=None, max_length=40)
    detail: str | None = Field(default=None, max_length=40)


class BotRuntime(WireModel):
    state: str = Field(default="unknown", max_length=100, pattern=IDENTIFIER)
    heartbeat_at: float | None = Field(default=None, ge=0)
    last_message_at: float | None = Field(default=None, ge=0)
    last_receive_at: float | None = Field(default=None, ge=0)
    last_send_at: float | None = Field(default=None, ge=0)
    queue_depth: int = Field(default=0, ge=0)
    error: str | None = Field(default=None, max_length=160, pattern=IDENTIFIER)
    enabled: bool = False
    authenticated: bool | None = None
    connection: str | None = Field(default=None, max_length=40, pattern=IDENTIFIER)
    login_required: bool = False
    active: BotActiveConfig | None = None
    last_request: BotLastRequest | None = None
    counts: BotCounts | None = None
    latency_metrics: dict[str, BotLatencyMetric] = Field(default_factory=dict, max_length=64)
    stale_events: list[BotStaleEvent] = Field(default_factory=list, max_length=50)

    @field_validator("latency_metrics")
    @classmethod
    def safe_stage_names(cls, value):
        if any(not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", stage) for stage in value):
            raise ValueError("invalid_bot_latency_stage")
        return value


class BotCapabilities(WireModel):
    quote_reply: bool = False
    true_mentions: bool = False
    sender_verified: bool = False
    native_ready: bool = False


class BotProviderState(WireModel):
    id: str = Field(min_length=1, max_length=80, pattern=IDENTIFIER)
    configured: bool
    state: str | None = Field(default=None, max_length=100, pattern=IDENTIFIER)
    error: str | None = Field(default=None, max_length=100, pattern=IDENTIFIER)


class BotSnapshot(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=IDENTIFIER)
    config_revision: int = Field(ge=0)
    statuses: list[BotStatus] = Field(default_factory=list, max_length=10000)
    audit: list[BotAudit] = Field(default_factory=list, max_length=1000)
    runtime: BotRuntime = Field(default_factory=BotRuntime)
    capabilities: BotCapabilities = Field(default_factory=BotCapabilities)
    providers: list[BotProviderState] = Field(default_factory=list, max_length=100)


class BotPoll(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=IDENTIFIER)


class BotBootstrap(BotPoll):
    alignment_id: str = Field(min_length=1, max_length=100, pattern=IDENTIFIER)
    data: BotConfig


class BotAck(BotPoll):
    command_id: str = Field(min_length=1, max_length=200, pattern=IDENTIFIER)
    status: str = Field(pattern=r"^(applied|failed)$")
    error: str | None = Field(default=None, max_length=100, pattern=IDENTIFIER)
    config_revision: int | None = Field(default=None, ge=0)
    snapshot: BotSnapshot | None = None


BOT_SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_configs(source_id TEXT PRIMARY KEY,revision INTEGER NOT NULL,applied_revision INTEGER NOT NULL DEFAULT 0,data TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bot_commands(id TEXT PRIMARY KEY,source_id TEXT NOT NULL,kind TEXT NOT NULL,revision INTEGER,payload TEXT NOT NULL,user_id INTEGER NOT NULL,created_at REAL NOT NULL,status TEXT NOT NULL DEFAULT 'pending',error TEXT,completed_at REAL);
CREATE INDEX IF NOT EXISTS bot_pending ON bot_commands(source_id,status,created_at);
CREATE TABLE IF NOT EXISTS bot_statuses(source_id TEXT NOT NULL,business_id TEXT NOT NULL,item_id TEXT NOT NULL,day TEXT NOT NULL,revision INTEGER NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(source_id,business_id,item_id,day));
CREATE TABLE IF NOT EXISTS bot_audit(source_id TEXT NOT NULL,id TEXT NOT NULL,created_at REAL NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(source_id,id));
CREATE TABLE IF NOT EXISTS bot_snapshots(source_id TEXT PRIMARY KEY,received_at REAL NOT NULL,payload TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS bot_bootstraps(source_id TEXT NOT NULL,alignment_id TEXT NOT NULL,data TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(source_id,alignment_id));
"""


class BotControlMixin:
    def bootstrap_bot(self, value):
        """Machine-authorized, idempotent alignment from its effective local config."""
        from .cloud_features import Conflict
        value = BotBootstrap.model_validate(value).model_dump(mode="json")
        source_id, alignment_id = value["source_id"], value["alignment_id"]
        data = value["data"]
        encoded = canonical_json(data)
        with self.transaction():
            self._bot_source(source_id)
            old = self.conn.execute("SELECT data,revision FROM bot_bootstraps WHERE source_id=? AND alignment_id=?",
                                    (source_id, alignment_id)).fetchone()
            if old:
                if old["data"] != encoded:
                    raise Conflict("bot_bootstrap_id_conflict")
                return {"accepted": True, "config_revision": old["revision"]}
            owned = {row[0] for row in self.conn.execute("SELECT group_ref FROM groups WHERE source_id=?", (source_id,))}
            if not {g["group_ref"] for g in data["groups"]}.issubset(owned):
                raise ValueError("bot_group_source_mismatch")
            observed = {row[0] for row in self.conn.execute("SELECT DISTINCT sender_id FROM messages WHERE source_id=? AND sender_id<>''", (source_id,))}
            if not {reporter for business in data["businesses"] for reporter in business["reporter_ids"]}.issubset(observed):
                raise ValueError("bot_reporter_not_observed")
            previous = self.conn.execute("SELECT revision FROM bot_configs WHERE source_id=?", (source_id,)).fetchone()
            revision = (previous[0] if previous else 0) + 1
            self.conn.execute("UPDATE bot_commands SET status='failed',error='cloud_control_reinitialized',completed_at=? WHERE source_id=? AND status='pending'",
                              (self.clock(), source_id))
            self.conn.execute("INSERT INTO bot_configs VALUES(?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET revision=excluded.revision,applied_revision=excluded.applied_revision,data=excluded.data",
                              (source_id, revision, revision, encoded))
            self.conn.execute("INSERT INTO bot_bootstraps VALUES(?,?,?,?)", (source_id, alignment_id, encoded, revision))
            self._bump()
        return {"accepted": True, "config_revision": revision}

    def _bot_source(self, source_id):
        if not self.conn.execute("SELECT 1 FROM sources WHERE source_id=? UNION SELECT 1 FROM groups WHERE source_id=?", (source_id, source_id)).fetchone():
            raise ValueError("unknown_bot_source")

    @staticmethod
    def _bot_command(row):
        return {**{k: row[k] for k in ("id", "source_id", "kind", "revision", "created_at", "status", "error", "completed_at")}, "payload": json.loads(row["payload"])}

    def _queue_bot_command(self, source_id, kind, payload, user_id, revision=None):
        command_id = uuid.uuid4().hex
        self.conn.execute("INSERT INTO bot_commands(id,source_id,kind,revision,payload,user_id,created_at) VALUES(?,?,?,?,?,?,?)", (command_id, source_id, kind, revision, canonical_json(payload), user_id, self.clock()))
        self._bump()
        return self._bot_command(self.conn.execute("SELECT * FROM bot_commands WHERE id=?", (command_id,)).fetchone())

    def bot_overview(self, source_id=None):
        with self._lock:
            defaults = {"persona": _default_personas()[0].model_dump(mode="json"), "provider": _default_providers()[0].model_dump(mode="json")}
            sources = [row[0] for row in self.conn.execute("SELECT source_id FROM sources UNION SELECT source_id FROM groups ORDER BY source_id")]
            source_id = source_id or (sources[0] if sources else None)
            if not source_id:
                return {"source_id": None, "sources": [], "config": {"revision": 0, "applied_revision": 0, "data": BotConfig().model_dump()}, "commands": [], "statuses": [], "audit": [], "runtime": {}, "capabilities": {}, "providers": [], "groups": [], "received_at": None, "defaults": defaults}
            self._bot_source(source_id)
            row = self.conn.execute("SELECT * FROM bot_configs WHERE source_id=?", (source_id,)).fetchone()
            config = {"revision": row["revision"], "applied_revision": row["applied_revision"], "data": BotConfig.model_validate_json(row["data"]).model_dump(mode="json")} if row else {"revision": 0, "applied_revision": 0, "data": BotConfig().model_dump()}
            snapshot = self.conn.execute("SELECT * FROM bot_snapshots WHERE source_id=?", (source_id,)).fetchone()
            value = json.loads(snapshot["payload"]) if snapshot else {}
            return {"source_id": source_id, "sources": sources, "config": config, "defaults": defaults,
                    "commands": [self._bot_command(r) for r in self.conn.execute("SELECT * FROM bot_commands WHERE source_id=? ORDER BY created_at DESC,rowid DESC LIMIT 100", (source_id,))],
                    "statuses": [json.loads(r[0]) for r in self.conn.execute("SELECT payload FROM bot_statuses WHERE source_id=? ORDER BY day DESC,business_id,item_id LIMIT 10000", (source_id,))],
                    "audit": [json.loads(r[0]) for r in self.conn.execute("SELECT payload FROM bot_audit WHERE source_id=? ORDER BY created_at DESC,id DESC LIMIT 200", (source_id,))],
                    "runtime": value.get("runtime", {}), "capabilities": value.get("capabilities", {}), "providers": value.get("providers", []),
                    "groups": [dict(r) for r in self.conn.execute("SELECT group_ref,name FROM groups WHERE source_id=? ORDER BY name,group_ref", (source_id,))],
                    "received_at": snapshot["received_at"] if snapshot else None}

    def save_bot_config(self, value, user_id):
        from .cloud_features import Conflict
        data = BotConfig.model_validate(value["data"]).model_dump(mode="json")
        source_id = value["source_id"]
        with self.transaction():
            self._bot_source(source_id)
            previous = self.conn.execute("SELECT revision,data FROM bot_configs WHERE source_id=?", (source_id,)).fetchone()
            current = previous["revision"] if previous else 0
            if current != value["expected_revision"]:
                raise Conflict("bot_config_revision_conflict")
            old_personas = {p["id"]: p for p in BotConfig.model_validate_json(previous["data"]).model_dump(mode="json")["personas"]} if previous else {}
            content_fields = ("name", "markdown")
            for persona in data["personas"]:
                old = old_personas.get(persona["id"])
                if old:
                    changed = any(persona.get(k) != old.get(k) for k in content_fields)
                    persona["version"] = max(persona["version"], int(old["version"]) + int(changed))
            owned = {r[0] for r in self.conn.execute("SELECT group_ref FROM groups WHERE source_id=?", (source_id,))}
            if not {g["group_ref"] for g in data["groups"]}.issubset(owned):
                raise ValueError("bot_group_source_mismatch")
            reporters = {r for b in data["businesses"] for r in b["reporter_ids"]}
            observed = {r[0] for r in self.conn.execute("SELECT DISTINCT sender_id FROM messages WHERE source_id=? AND sender_id<>''", (source_id,))}
            if not reporters.issubset(observed):
                raise ValueError("bot_reporter_not_observed")
            revision = current + 1
            self.conn.execute("INSERT INTO bot_configs VALUES(?,?,0,?) ON CONFLICT(source_id) DO UPDATE SET revision=excluded.revision,data=excluded.data", (source_id, revision, canonical_json(data)))
            command = self._queue_bot_command(source_id, "apply_config", {"config": data, "revision": revision}, user_id, revision)
        return {"revision": revision, "command": command}

    def adjust_bot_status(self, value, user_id):
        from .cloud_features import Conflict
        source_id = value["source_id"]
        with self.transaction():
            self._bot_source(source_id)
            row = self.conn.execute("SELECT * FROM bot_configs WHERE source_id=?", (source_id,)).fetchone()
            if not row or row["revision"] != row["applied_revision"]:
                raise Conflict("bot_configuration_not_applied")
            business = next((b for b in json.loads(row["data"])["businesses"] if b["id"] == value["business_id"]), None)
            if not business or not any(i["id"] == value["item_id"] for i in business["items"]):
                raise ValueError("unknown_bot_item")
            current = self.conn.execute("SELECT revision FROM bot_statuses WHERE source_id=? AND business_id=? AND item_id=? AND day=?", (source_id, value["business_id"], value["item_id"], value["day"])).fetchone()
            if (current[0] if current else 0) != value["expected_revision"]:
                raise Conflict("bot_status_revision_conflict")
            payload = {k: value[k] for k in ("business_id", "item_id", "day", "status", "expected_revision")}
            payload["actor"] = "dashboard:" + str(user_id)
            command = self._queue_bot_command(source_id, "set_status", payload, user_id)
        return {"command": command}

    def poll_bot_commands(self, source_id):
        with self.transaction():
            self._bot_source(source_id)
            # Revoked administrators cannot leave queued write operations behind.
            changed = self.conn.execute("UPDATE bot_commands SET status='failed',error='admin_authorization_revoked',completed_at=? WHERE source_id=? AND status='pending' AND NOT EXISTS(SELECT 1 FROM users u WHERE u.id=bot_commands.user_id AND u.role='admin' AND u.disabled=0)", (self.clock(), source_id)).rowcount
            if changed:
                self._bump()
            rows = self.conn.execute("SELECT * FROM bot_commands WHERE source_id=? AND status='pending' ORDER BY created_at,rowid LIMIT 100", (source_id,)).fetchall()
            return {"commands": [self._bot_command(r) for r in rows]}

    def acknowledge_bot_command(self, value):
        from .cloud_features import Conflict
        if value.get("snapshot") and value["snapshot"]["source_id"] != value["source_id"]:
            raise ValueError("bot_snapshot_source_mismatch")
        with self.transaction():
            row = self.conn.execute("SELECT * FROM bot_commands WHERE id=? AND source_id=?", (value["command_id"], value["source_id"])).fetchone()
            if not row:
                raise KeyError("bot_command_not_found")
            if row["status"] != "pending":
                if row["status"] != value["status"] or row["error"] != value.get("error"):
                    raise Conflict("bot_command_ack_conflict")
                return {"accepted": True, "status": row["status"]}
            if value["status"] == "applied" and row["kind"] == "apply_config":
                if value.get("config_revision") != row["revision"]:
                    raise ValueError("bot_config_ack_revision_mismatch")
                self.conn.execute("UPDATE bot_configs SET applied_revision=MAX(applied_revision,?) WHERE source_id=?", (row["revision"], row["source_id"]))
            self.conn.execute("UPDATE bot_commands SET status=?,error=?,completed_at=? WHERE id=?", (value["status"], value.get("error"), self.clock(), row["id"]))
            self._bump()
        if value.get("snapshot"):
            self.ingest_bot_snapshot(value["snapshot"])
        return {"accepted": True, "status": value["status"]}

    def ingest_bot_snapshot(self, value):
        from .cloud_features import Conflict
        value = BotSnapshot.model_validate(value).model_dump(mode="json")
        source_id = value["source_id"]
        with self.transaction():
            self._bot_source(source_id)
            config_row = self.conn.execute("SELECT revision FROM bot_configs WHERE source_id=?", (source_id,)).fetchone()
            if value["config_revision"] > (config_row[0] if config_row else 0):
                raise ValueError("unknown_bot_config_revision")
            owned = {r[0] for r in self.conn.execute("SELECT group_ref FROM groups WHERE source_id=?", (source_id,))}
            for field in ("active", "last_request"):
                runtime_group = (value["runtime"].get(field) or {}).get("group_ref")
                if runtime_group and runtime_group not in owned:
                    raise ValueError("bot_group_source_mismatch")
            for status in value["statuses"]:
                if status.get("group_ref") and status["group_ref"] not in owned:
                    raise ValueError("bot_group_source_mismatch")
                key = (source_id, status["business_id"], status["item_id"], status["day"])
                previous = self.conn.execute("SELECT revision,payload FROM bot_statuses WHERE source_id=? AND business_id=? AND item_id=? AND day=?", key).fetchone()
                encoded = canonical_json(status)
                if previous and previous["revision"] == status["revision"] and previous["payload"] != encoded:
                    raise Conflict("bot_status_snapshot_conflict")
                if not previous or previous["revision"] < status["revision"]:
                    self.conn.execute("INSERT INTO bot_statuses VALUES(?,?,?,?,?,?) ON CONFLICT(source_id,business_id,item_id,day) DO UPDATE SET revision=excluded.revision,payload=excluded.payload", (*key, status["revision"], encoded))
            for audit in value["audit"]:
                if audit.get("group_ref") and audit["group_ref"] not in owned:
                    raise ValueError("bot_group_source_mismatch")
                encoded = canonical_json(audit)
                previous = self.conn.execute("SELECT payload FROM bot_audit WHERE source_id=? AND id=?", (source_id, audit["id"])).fetchone()
                if previous and previous[0] != encoded:
                    raise Conflict("bot_audit_snapshot_conflict")
                self.conn.execute("INSERT OR IGNORE INTO bot_audit VALUES(?,?,?,?)", (source_id, audit["id"], audit["created_at"], encoded))
            # Store only operational data; desired configuration and credentials
            # are never reflected back from the machine.
            snapshot = {key: value[key] for key in ("config_revision", "runtime", "capabilities", "providers")}
            self.conn.execute("INSERT INTO bot_snapshots VALUES(?,?,?) ON CONFLICT(source_id) DO UPDATE SET received_at=excluded.received_at,payload=excluded.payload", (source_id, self.clock(), canonical_json(snapshot)))
            self._bump()
        return {"accepted": True}

    def bot_members(self, source_id, query="", limit=100):
        with self._lock:
            self._bot_source(source_id)
            rows = self.conn.execute("""WITH names AS (
              SELECT sender_id,sender_name,group_ref,create_time,message_ref,
              ROW_NUMBER() OVER(PARTITION BY sender_id ORDER BY create_time DESC,message_ref DESC) pos
              FROM messages WHERE source_id=? AND sender_id<>'')
              SELECT sender_id,sender_name,group_ref FROM names WHERE pos=1 AND (instr(lower(sender_name),lower(?))>0 OR instr(sender_id,?)>0)
              ORDER BY sender_name,sender_id LIMIT ?""", (source_id, query, query, limit + 1)).fetchall()
            return {"members": [dict(r) for r in rows[:limit]], "has_more": len(rows) > limit}
