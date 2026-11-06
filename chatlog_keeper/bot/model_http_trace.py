"""Opt-in bounded local HTTP phase evidence, with no request/response content.

The httpcore trace callback intentionally never inspects its ``info`` argument.
This module changes neither transport configuration nor retry/model policy.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import math
import os
import re
import time

ENVIRONMENT = "WEKEEPER_MODEL_HTTP_TRACE"
MAX_EVENTS = 128
MAX_CALLS = 4
_OPERATIONS = ("connect_tcp", "start_tls", "send_connection_init", "send_request_headers",
               "send_request_body", "receive_response_headers", "receive_response_body",
               "response_closed", "receive_remote_settings", "receive_response")
EVENT_NAMES = frozenset(f"{prefix}.{operation}.{outcome}"
    for prefix in ("connection", "proxy", "http11", "http2")
    for operation in _OPERATIONS for outcome in ("started", "complete", "failed"))


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def validated_trace(value):
    """Copy a closed schema or drop it, never persisting arbitrary metadata."""
    try:
        if (type(value) is not dict or set(value) != {"schema", "call_index", "body_sha256",
                "utc_anchor", "monotonic_anchor", "duration_seconds", "outcome", "events",
                "dropped_events", "observer_errors"}
                or value["schema"] != 1 or type(value["schema"]) is not int
                or type(value["call_index"]) is not int or not 1 <= value["call_index"] <= 2**63-1
                or type(value["body_sha256"]) is not str
                or not re.fullmatch(r"[0-9a-f]{64}", value["body_sha256"])
                or type(value["utc_anchor"]) is not str
                or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+00:00", value["utc_anchor"])
                or not _number(value["monotonic_anchor"]) or not _number(value["duration_seconds"])
                or value["outcome"] not in ("returned", "unknown")
                or type(value["events"]) is not list or len(value["events"]) > MAX_EVENTS
                or any(type(value[k]) is not int or not 0 <= value[k] <= 2**63-1
                       for k in ("dropped_events", "observer_errors"))):
            return None
        events = []
        for event in value["events"]:
            if (type(event) is not dict or set(event) != {"name", "offset_seconds"}
                    or type(event["name"]) is not str or event["name"] not in EVENT_NAMES
                    or not _number(event["offset_seconds"])
                    or event["offset_seconds"] > value["duration_seconds"]):
                return None
            events.append(dict(event))
        return {**value, "events": events}
    except BaseException:
        return None


class _Trace:
    def __init__(self, body, index):
        import httpx
        # HTTPX's own encoder, also used by the unchanged real post(json=body).
        # No extra transport/client is constructed or request submitted here.
        encoded = httpx.Request("POST", "https://trace.invalid/", json=body).content
        self.body_sha256 = hashlib.sha256(encoded).hexdigest()
        self.index = index
        self.anchor = time.monotonic()
        self.utc = datetime.now(timezone.utc).isoformat(timespec="microseconds")
        self.events, self.dropped, self.errors = [], 0, 0

    def callback(self, name, info):
        # Even an observer clock/allocation failure cannot change HTTP outcome.
        try:
            if type(name) is not str or name not in EVENT_NAMES or len(self.events) >= MAX_EVENTS:
                self.dropped = min(2**63-1, self.dropped + 1)
                return
            offset = time.monotonic() - self.anchor
            if not _number(offset):
                self.dropped = min(2**63-1, self.dropped + 1)
                return
            self.events.append({"name": name, "offset_seconds": offset})
        except BaseException:
            try:
                self.errors = min(2**63-1, self.errors + 1)
            except BaseException:
                pass

    def finish(self, *, returned):
        try:
            return validated_trace({"schema": 1, "call_index": self.index,
                "body_sha256": self.body_sha256, "utc_anchor": self.utc,
                "monotonic_anchor": self.anchor, "duration_seconds": time.monotonic() - self.anchor,
                "outcome": "returned" if returned else "unknown", "events": self.events,
                "dropped_events": self.dropped, "observer_errors": self.errors})
        except BaseException:
            return None


def begin_trace(body, index):
    """Default off; construction errors leave the original HTTP path intact."""
    try:
        if os.environ.get(ENVIRONMENT) != "1":
            return None
        return _Trace(body, index)
    except BaseException:
        return None
