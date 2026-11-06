"""Fixed, model-free native quote probes and independent receipt checks.

This module has no UI, business tools, account login or automatic retries.
Sender-side acceptance and local history are deliberately separate from an
independent recipient's observation. Probe files belong in private storage.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import time

from .protocol_gateway import GatewayError, ProtocolGateway


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":")).encode()).hexdigest()


def prepare_probe(*, run_id, account_id, own_username, thread_id, sources):
    """Freeze exact source identities; repeated body text is not a key."""
    if not run_id or not run_id.isascii() or not run_id.replace("-", "").isalnum():
        raise ValueError("invalid_probe_run_id")
    if not sources or not own_username:
        raise ValueError("probe_sources_required")
    seen, cases = set(), []
    for source in sources:
        sid = source.get("serverId")
        if (source.get("accountId") != account_id or source.get("threadId") != thread_id
                or not isinstance(source.get("id"), str) or not source["id"]
                or not isinstance(sid, str) or not sid.isdecimal()
                or not 0 < int(sid) <= 2**64 - 1 or str(int(sid)) != sid
                or sid in seen or not isinstance(source.get("body"), str)
                or not isinstance(source.get("senderId"), str)
                or source.get("direction") not in {"incoming", "outgoing"}
                or (source["senderId"] == own_username) != (source["direction"] == "outgoing")):
            raise ValueError("invalid_probe_source")
        if type(source.get("type")) is not int or source["type"] != 1:
            raise ValueError("fixed_probe_requires_plain_text_source")
        seen.add(sid)
        case_id = str(len(cases) + 1)
        cases.append({"case_id": case_id, "message_id": source["id"],
                      "source_server_id": sid, "source_sender_id": source["senderId"],
                      "source_direction": source["direction"],
                      "source_type": source["type"],
                      "source_body_sha256": hashlib.sha256(source["body"].encode()).hexdigest(),
                      "idempotency_key": f"wk-protocol:{run_id}:{case_id}",
                      "reply_text": f"[协议引用验收 {run_id}-{case_id}]"})
    spec = {"schema_version": 1, "run_id": run_id, "account_id": account_id,
            "own_username": own_username, "thread_id": thread_id, "cases": cases,
            "model_calls": 0, "business_writes": 0}
    return {**spec, "spec_sha256": _digest(spec)}


def verify_spec(spec):
    expected = spec.get("spec_sha256")
    body = {k: v for k, v in spec.items() if k != "spec_sha256"}
    if spec.get("schema_version") != 1 or expected != _digest(body):
        raise ValueError("probe_spec_changed")
    if spec.get("model_calls") != 0 or spec.get("business_writes") != 0:
        raise ValueError("probe_must_be_model_free")
    if spec.get("source_mode", "profile") not in {"profile", "native"}:
        raise ValueError("probe_source_mode_invalid")


def prepare_native_probe(*, run_id, account_id, own_username, thread_id, sources):
    """Freeze native snapshots; never infer a profile row from their identity."""
    normalized = []
    for source in sources:
        if (source.get("ownUsername") != own_username or source.get("disposition") != "ready"
                or source.get("quoteEligible") is not True):
            raise ValueError("native_probe_source_unavailable")
        from .protocol_gateway import _identifier, _source_hash
        _identifier(source.get("receivedId"))
        _source_hash(source.get("sourceSha256"))
        normalized.append({**source, "id": source["receivedId"]})
    spec = prepare_probe(run_id=run_id, account_id=account_id, own_username=own_username,
                         thread_id=thread_id, sources=normalized)
    spec.pop("spec_sha256")
    spec["source_mode"] = "native"
    for case, source in zip(spec["cases"], sources):
        case["received_id"] = case.pop("message_id")
        case["source_sha256"] = source["sourceSha256"]
    return {**spec, "spec_sha256": _digest(spec)}


def execute_one(gateway: ProtocolGateway, spec, case_id):
    """One explicitly selected fixed probe, never a full-corpus test."""
    verify_spec(spec)
    if gateway.account_id != spec["account_id"]:
        raise ValueError("probe_account_mismatch")
    gateway.require_account(spec["own_username"])
    matches = [c for c in spec["cases"] if c["case_id"] == case_id]
    if len(matches) != 1:
        raise ValueError("probe_case_not_unique")
    case = matches[0]
    started = time.monotonic_ns()
    native = spec.get("source_mode") == "native"
    if native:
        source = gateway.get_received_message(case["received_id"], thread_id=spec["thread_id"],
            server_id=case["source_server_id"], source_sha256=case["source_sha256"])
        if source.get("disposition") != "ready" or source.get("quoteEligible") is not True:
            raise GatewayError("probe_source_unavailable")
    else:
        source = gateway.get_message(case["message_id"], thread_id=spec["thread_id"],
                                     server_id=case["source_server_id"])
    if (source.get("senderId") != case["source_sender_id"]
            or ("source_direction" in case and source.get("direction") != case["source_direction"])
            or type(source.get("type")) is not int
            or source["type"] != case.get("source_type", 1)
            or source["type"] != 1
            or not isinstance(source.get("body"), str)
            or hashlib.sha256(source["body"].encode()).hexdigest() != case["source_body_sha256"]
            or source.get("recall") is not None):
        raise GatewayError("probe_source_changed")
    checked = time.monotonic_ns()
    common = dict(thread_id=spec["thread_id"], server_id=case["source_server_id"],
                  text=case["reply_text"], idempotency_key=case["idempotency_key"])
    if native:
        result = gateway.send_received_quote(received_id=case["received_id"],
                                             source_sha256=case["source_sha256"], **common)
    else:
        result = gateway.send_quote(message_id=case["message_id"], **common)
    finished = time.monotonic_ns()
    return {"schema_version": 1, "spec_sha256": spec["spec_sha256"], "case_id": case_id,
            "send": asdict(result), "source_lookup_ms": (checked-started)/1e6,
            "submit_api_ms": (finished-checked)/1e6,
            "peer_received": False, "phone_full_received": False,
            "quote_navigation_verified": False, "model_calls": 0, "business_writes": 0}


def inspect_one(gateway: ProtocolGateway, spec, case_id):
    """Recover the original receipt without source lookup or send submission."""
    verify_spec(spec)
    if gateway.account_id != spec["account_id"]:
        raise ValueError("probe_account_mismatch")
    gateway.require_account(spec["own_username"])
    matches = [case for case in spec["cases"] if case["case_id"] == case_id]
    if len(matches) != 1:
        raise ValueError("probe_case_not_unique")
    result = gateway.get_send_status(matches[0]["idempotency_key"])
    return {"schema_version": 1, "spec_sha256": spec["spec_sha256"], "case_id": case_id,
            "send": asdict(result), "operation": "status_only",
            "peer_received": False, "phone_full_received": False,
            "quote_navigation_verified": False, "model_calls": 0, "business_writes": 0}


def assess_recipient(spec, case_id, observation):
    """Validate a separately collected recipient record, never a local echo.

    The collector must retain the evidence artifact and its hash. This checker
    verifies correlation, not the authenticity of an arbitrary supplied file.
    """
    verify_spec(spec)
    case = next(c for c in spec["cases"] if c["case_id"] == case_id)
    reasons = []
    if observation.get("evidence_kind") not in {"independent_peer", "phone_capture"}:
        reasons.append("independent_evidence_required")
    if (not observation.get("receiver_username")
            or observation["receiver_username"] == spec["own_username"]):
        reasons.append("independent_receiver_required")
    expected = {"thread_id": spec["thread_id"], "sender_username": spec["own_username"],
                "body": case["reply_text"], "quote_server_id": case["source_server_id"]}
    reasons.extend(f"{key}_mismatch" for key, value in expected.items()
                   if observation.get(key) != value)
    reply_sid = observation.get("reply_server_id")
    if (not isinstance(reply_sid, str) or not reply_sid.isascii() or not reply_sid.isdecimal()
            or not 0 < int(reply_sid) <= 2**64 - 1 or str(int(reply_sid)) != reply_sid):
        reasons.append("reply_server_id_required")
    artifact_hash = observation.get("artifact_sha256", "")
    if (not isinstance(artifact_hash, str) or len(artifact_hash) != 64
            or any(c not in "0123456789abcdef" for c in artifact_hash)):
        reasons.append("evidence_artifact_required")
    complete = observation.get("complete") is True
    if not complete:
        reasons.append("complete_reply_required")
    matched = not reasons
    # A supplied hash or boolean is not proof that a recipient artifact exists,
    # was collected independently, or belongs to this actual submission. This
    # pure checker must not turn a description into an acceptance certificate.
    return {"correlation_matched": matched, "reasons": reasons,
            "reported_navigation": matched and observation.get("quote_navigation_verified") is True,
            "evidence_artifact_verified": False,
            "peer_received": False, "phone_full_received": False,
            "quote_navigation_verified": False, "full_acceptance_passed": False}
