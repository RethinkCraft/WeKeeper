"""Bounded, allowlisted machine protocol; no archive rows are serialized directly."""
from datetime import date, datetime
import hashlib
import json
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ZONE = ZoneInfo("Asia/Shanghai")


def canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def content_hash(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


class WireModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Group(WireModel):
    group_ref: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    name: str = Field(max_length=512)


class Reply(WireModel):
    target_ref: str | None = Field(default=None, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    text: str = Field(default="", max_length=65536)


class Message(WireModel):
    message_ref: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    create_time: int = Field(ge=0, le=253402214400, strict=True)
    sender_id: str = Field(max_length=256)
    sender_name: str = Field(default="", max_length=512)
    msg_type: int = Field(ge=0, le=65535, strict=True)
    text: str = Field(max_length=262144)
    reply: Reply | None = None


class Coverage(WireModel):
    local_backfill_complete: bool = False
    source_available: bool | None = None
    sources_readable: int | None = Field(default=None, ge=0)
    sources_total: int | None = Field(default=None, ge=0)
    backfill_streams_pending: int | None = Field(default=None, ge=0)
    gaps: list[str] = Field(default_factory=list, max_length=100)


class PartitionData(WireModel):
    group: Group
    day: date
    messages: list[Message] = Field(max_length=50000)
    coverage: Coverage = Field(default_factory=Coverage)

    @model_validator(mode="after")
    def validate_partition(self):
        refs = set()
        for message in self.messages:
            if message.message_ref in refs:
                raise ValueError("duplicate_message_ref")
            refs.add(message.message_ref)
            if datetime.fromtimestamp(message.create_time, ZONE).date() != self.day:
                raise ValueError("message_outside_partition")
        return self


class Partition(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    group_ref: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    day: date
    revision: int = Field(gt=0, strict=True)
    content_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    initial: bool = False
    data: PartitionData

    @model_validator(mode="after")
    def validate_identity(self):
        if self.group_ref != self.data.group.group_ref or self.day != self.data.day:
            raise ValueError("partition_identity_mismatch")
        # Validate the exact received JSON before adding optional-field defaults.
        return self


HEALTH_STATES = Literal["receiving", "session_present", "login_required", "not_running", "network_unavailable", "source_unavailable", "collector_error", "unknown", "sync_unverified", "not_initialized"]


class Health(WireModel):
    state: HEALTH_STATES
    reason: str | None = Field(default=None, max_length=200)


class Collector(WireModel):
    running: bool = False
    last_cycle_at: float | None = None
    last_error: str | None = Field(default=None, max_length=200)


class SummaryStatus(WireModel):
    state: str = Field(default="not_configured", max_length=80)
    last_success_at: float | None = None
    error: str | None = Field(default=None, max_length=200)


class SyncStatus(WireModel):
    state: str = Field(default="waiting", max_length=80)
    last_success_at: float | None = None
    error: str | None = Field(default=None, max_length=200)
    pending_partitions: int = Field(default=0, ge=0, strict=True)


class Heartbeat(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    health: Health
    collector: Collector
    last_message_at: float | None = None
    summary: SummaryStatus = Field(default_factory=SummaryStatus)
    sync: SyncStatus = Field(default_factory=SyncStatus)
    sent_at: float


class SummaryItem(WireModel):
    text: str = Field(min_length=1, max_length=4000)
    message_refs: list[str] = Field(min_length=1, max_length=100)

    @field_validator("message_refs")
    @classmethod
    def valid_refs(cls, value):
        import re
        if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", ref) for ref in value):
            raise ValueError("invalid_message_reference")
        return list(dict.fromkeys(value))


class SummaryBody(WireModel):
    topics: list[SummaryItem] = Field(default_factory=list, max_length=100)
    decisions: list[SummaryItem] = Field(default_factory=list, max_length=100)
    todos: list[SummaryItem] = Field(default_factory=list, max_length=100)


class Summary(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    group_ref: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    day: date
    revision: int = Field(gt=0, strict=True)
    input_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    model: str = Field(min_length=1, max_length=200)
    generated_at: float
    covered_until: float
    summary: SummaryBody
    processing: "SummaryProcessing | None" = None


class SummaryProcessing(WireModel):
    mode: Literal["legacy", "incremental"]
    checkpoint_seq: int = Field(ge=0, strict=True)
    baseline_complete: bool
    processed_messages: int = Field(ge=0, strict=True)
    pending_events: int = Field(ge=0, strict=True)


class AvatarUpload(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    group_ref: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    sender_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    avatar_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    mime_type: Literal["image/png", "image/jpeg"]
    content_base64: str = Field(min_length=1, max_length=180000)
    observed_at: float = Field(ge=0)
    display_name: str | None = Field(default=None, max_length=512)


class ChatScope(WireModel):
    group_refs: list[str] = Field(min_length=1, max_length=100)
    since: float = Field(ge=0)
    until: float = Field(ge=0)

    @model_validator(mode="after")
    def bounded_scope(self):
        import re
        if self.until < self.since or self.until > 253402214400 or len(set(self.group_refs)) != len(self.group_refs) or any(not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", ref) for ref in self.group_refs):
            raise ValueError("invalid_chat_scope")
        return self


class ConversationCreate(ChatScope):
    title: str = Field(default="新的群聊对话", min_length=1, max_length=120)


class TurnCreate(WireModel):
    content: str = Field(min_length=1, max_length=12000)
    client_request_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    group_refs: list[str] | None = Field(default=None, min_length=1, max_length=100)
    since: float | None = Field(default=None, ge=0)
    until: float | None = Field(default=None, ge=0)


class JobClaim(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    worker_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    group_refs: list[str] = Field(min_length=1, max_length=100)


class JobEvent(WireModel):
    seq: int = Field(ge=1, strict=True)
    type: Literal["delta", "tool", "heartbeat", "completed", "failed", "interrupted", "cancelled"]
    text: str | None = Field(default=None, max_length=262144)
    name: str | None = Field(default=None, max_length=128)
    state: str | None = Field(default=None, max_length=80)
    message_refs: list[str] | None = Field(default=None, max_length=500)
    error: str | None = Field(default=None, max_length=160, pattern=r"^[A-Za-z0-9_.:-]+$")

    @model_validator(mode="after")
    def correct_event(self):
        if self.type in {"delta", "completed"} and self.text is None:
            raise ValueError("event_text_required")
        if self.type == "completed" and self.message_refs is None:
            raise ValueError("event_references_required")
        if self.type in {"failed", "interrupted"} and not self.error:
            raise ValueError("event_error_required")
        if self.type == "tool" and not self.name:
            raise ValueError("event_tool_name_required")
        if self.message_refs:
            import re
            if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", ref) for ref in self.message_refs):
                raise ValueError("invalid_message_reference")
        return self


class JobEvents(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    lease_token: str = Field(min_length=1, max_length=256)
    events: list[JobEvent] = Field(min_length=1, max_length=100)


class TokenUsage(WireModel):
    input_tokens: int | None = Field(default=None, ge=0, le=10**12, strict=True)
    cached_input_tokens: int | None = Field(default=None, ge=0, le=10**12, strict=True)
    output_tokens: int | None = Field(default=None, ge=0, le=10**12, strict=True)
    reasoning_output_tokens: int | None = Field(default=None, ge=0, le=10**12, strict=True)
    total_tokens: int | None = Field(default=None, ge=0, le=10**12, strict=True)


class UsageRun(WireModel):
    run_id: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_.:-]+$")
    kind: Literal["chat", "summary"]
    user_id: int | None = Field(default=None, ge=1, strict=True)
    turn_id: str | None = Field(default=None, max_length=128)
    model: str = Field(min_length=1, max_length=200)
    started_at: float = Field(ge=0)
    completed_at: float = Field(ge=0)
    status: Literal["completed", "failed", "cancelled", "interrupted"]
    usage: TokenUsage | None = None

    @model_validator(mode="after")
    def run_scope(self):
        if self.completed_at < self.started_at:
            raise ValueError("invalid_usage_time")
        if self.kind == "summary" and (self.user_id is not None or self.turn_id is not None):
            raise ValueError("summary_usage_cannot_impersonate_user")
        if self.kind == "chat" and (self.user_id is None or not self.turn_id):
            raise ValueError("chat_usage_requires_turn")
        return self


class UsageBatch(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    runs: list[UsageRun] = Field(max_length=500)


class RateWindow(WireModel):
    usedPercent: float = Field(ge=0)
    windowDurationMins: int | None = Field(default=None, ge=0)
    resetsAt: float | None = None


class RateCredits(WireModel):
    hasCredits: bool
    unlimited: bool
    balance: str | None = Field(default=None, max_length=100)


class RateLimit(WireModel):
    limitId: str | None = Field(default=None, max_length=100)
    limitName: str | None = Field(default=None, max_length=200)
    primary: RateWindow | None = None
    secondary: RateWindow | None = None
    credits: RateCredits | None = None
    planType: str | None = Field(default=None, max_length=100)
    rateLimitReachedType: str | None = Field(default=None, max_length=100)


class RateLimits(WireModel):
    rateLimits: RateLimit | None = None
    rateLimitsByLimitId: dict[str, RateLimit] | None = None

    @model_validator(mode="after")
    def size_limit(self):
        if self.rateLimitsByLimitId and (len(self.rateLimitsByLimitId) > 100 or any(len(key) > 200 for key in self.rateLimitsByLimitId)):
            raise ValueError("quota_snapshot_too_large")
        return self


class QuotaSnapshot(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    provider: Literal["codex_app_server"]
    sampled_at: float = Field(ge=0)
    auth_state: Literal["ready", "unavailable"]
    rate_limits: RateLimits | None = None
    error: str | None = Field(default=None, max_length=160, pattern=r"^[A-Za-z0-9_.:-]+$")

Summary.model_rebuild()


class JobResult(WireModel):
    source_id: str = Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9_-]+$")
    lease_token: str = Field(min_length=1, max_length=256)
    text: str = Field(max_length=262144)
    message_refs: list[str] = Field(max_length=500)

    @field_validator("message_refs")
    @classmethod
    def refs_valid(cls, refs):
        import re
        if any(not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", ref) for ref in refs):
            raise ValueError("invalid_message_reference")
        return list(dict.fromkeys(refs))
