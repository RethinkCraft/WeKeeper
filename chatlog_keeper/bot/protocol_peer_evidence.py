"""Correlate one retained peer row with a frozen quote probe and send report.

``assess_peer_artifact(spec, send_report, path)`` reads a UTF-8 JSON file:
``{"schema_version": 1, "receiver_username": "...", "row": {
  "group_id": "...", "sender_id": "...", "server_id": "...",
  "local_type": 49, "message_content": "<msg><appmsg>...</appmsg></msg>"
}}``. ``message_content`` must be the retained raw XML, not rendered text or
pre-extracted quote fields. The collector must resolve the raw row's sender and
group identities in the independent recipient's archive. Both ordinary type 49
and the observed Linux composite ``57 << 32 | 49`` are supported.

The file hash binds bytes, not their provenance. ``peer_observed`` means that
the supplied recipient record matches this submission; this offline checker
cannot authenticate the collector/account or prove that the file was not made
up. It never validates a native wire receipt, phone display, navigation, or
duplicate-free delivery. Keep those acceptance gates false until separate real
evidence exists. No account access, sending, retries, or file writes occur here.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import xml.etree.ElementTree as ET

from .protocol_acceptance import verify_spec


MAX_ARTIFACT_BYTES = 1024 * 1024
MAX_XML_BYTES = 256 * 1024
QUOTE_LOCAL_TYPES = {49, (57 << 32) | 49}


class _Rejected(ValueError):
    pass


def _require(condition, code):
    if not condition:
        raise _Rejected(code)


def _identity(value):
    return (isinstance(value, str) and 0 < len(value) <= 4096
            and not any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value))


def _server_id(value):
    return (isinstance(value, str) and re.fullmatch(r"[1-9][0-9]{0,19}", value) is not None
            and int(value) <= 2**64 - 1)


def _unique_json(pairs):
    result = {}
    for name, value in pairs:
        _require(name not in result, "artifact_duplicate_json_key")
        result[name] = value
    return result


def _invalid_constant(_value):
    raise _Rejected("artifact_invalid_json")


def _read_artifact(path):
    # Bound input and reject symlinks/FIFOs before reading. Inspect the opened
    # descriptor, not a separate stat of a path that could have been replaced.
    try:
        descriptor = os.open(Path(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as file:
            info = os.fstat(file.fileno())
            _require(stat.S_ISREG(info.st_mode), "artifact_regular_file_required")
            _require(0 < info.st_size <= MAX_ARTIFACT_BYTES, "artifact_size_invalid")
            data = file.read(MAX_ARTIFACT_BYTES + 1)
            _require(0 < len(data) <= MAX_ARTIFACT_BYTES, "artifact_size_invalid")
    except OSError:
        raise _Rejected("artifact_unreadable") from None
    return data


def _one(parent, name):
    children = parent.findall(name)
    _require(len(children) == 1, "quote_xml_ambiguous_or_missing_field")
    return children[0]


def _text(parent, name):
    node = _one(parent, name)
    _require(not list(node) and not node.attrib, "quote_xml_non_text_field")
    return node.text or ""


def _quote_fields(content, sender):
    _require(isinstance(content, str), "raw_xml_required")
    _require(0 < len(content.encode("utf-8")) <= MAX_XML_BYTES, "quote_xml_size_invalid")
    # Group rows can retain the exact sender prefix. Do not strip arbitrary
    # text/newlines or search descendants for a convenient matching appmsg.
    if content.startswith(sender + ":\n"):
        content = content[len(sender) + 2:]
    content = content.strip()
    _require(not re.search(r"<!\s*(?:DOCTYPE|ENTITY)\b", content, re.I), "quote_xml_declaration_forbidden")
    # Validate a normal document first. Native appmsg+fromusername fragments
    # are the only additional carrier; wrapping still requires clean top level.
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        try:
            root = ET.fromstring("<peer-fragment>" + content + "</peer-fragment>")
        except ET.ParseError:
            raise _Rejected("quote_xml_invalid") from None
    if root.tag == "appmsg":
        app = root
    else:
        _require(root.tag in {"msg", "peer-fragment"}, "quote_xml_carrier_invalid")
        _require(not (root.text or "").strip()
                 and all(not (child.tail or "").strip() for child in root), "quote_xml_carrier_invalid")
        if root.tag == "peer-fragment":
            _require(all(child.tag in {"appmsg", "fromusername"} for child in root), "quote_xml_carrier_invalid")
        app = _one(root, "appmsg")
        if root.findall("fromusername"):
            _require(_text(root, "fromusername") == sender, "xml_sender_mismatch")
    _require(_text(app, "type") == "57", "quote_subtype_mismatch")
    reply = _text(app, "title")
    reference = _one(app, "refermsg")
    source_sid = _text(reference, "svrid")
    _require(_server_id(source_sid), "quote_server_id_invalid")
    return (reply, source_sid, _text(reference, "fromusr"), _text(reference, "chatusr"),
            _text(reference, "type"), _text(reference, "content"))


def _submission(spec, report):
    _require(isinstance(report, dict) and type(report.get("schema_version")) is int
             and report["schema_version"] == 1, "send_report_invalid")
    _require(report.get("spec_sha256") == spec["spec_sha256"], "send_report_spec_mismatch")
    _require(all(type(report.get(key)) is int and report[key] == 0
                 for key in ("model_calls", "business_writes")), "send_report_probe_mismatch")
    cases = [case for case in spec["cases"] if case["case_id"] == report.get("case_id")]
    _require(len(cases) == 1, "send_report_case_mismatch")
    case = cases[0]
    send = report.get("send")
    _require(isinstance(send, dict), "send_report_invalid")
    _require(send.get("idempotency_key") == case["idempotency_key"], "send_report_key_mismatch")
    _require(send.get("outcome") in {"unknown", "queued", "accepted"}, "send_report_outcome_conflict")
    intent = send.get("intent")
    _require(isinstance(intent, dict) and _identity(intent.get("id")), "send_intent_required")
    _require(intent.get("accountId") == spec["account_id"]
             and intent.get("destination") == spec["thread_id"], "send_intent_identity_mismatch")
    _require(intent.get("state") in {"queued", "submitting", "needs_review", "accepted", "sent", "server_accepted"},
             "send_intent_state_conflict")
    reply_sid = intent.get("serverId")
    if (reply_sid is not None or send["outcome"] == "accepted"
            or intent["state"] in {"accepted", "sent", "server_accepted"}):
        _require(_server_id(reply_sid), "send_intent_server_id_invalid")
    return case, intent


def assess_peer_artifact(spec, send_report, artifact_path):
    """Return bounded diagnostics; a match is not authenticated live acceptance.

    A report without a known send intent must first be reconciled by its original
    idempotency key. This function never treats missing evidence as permission
    to resubmit. It does not take a caller-supplied hash or acceptance boolean.
    """
    result = {"schema_version": 1, "artifact_sha256": None, "artifact_bytes": 0,
              "artifact_structure_verified": False, "correlation_matched": False,
              "peer_observed": False, "provenance_authenticated": False,
              "native_receipt_validated": False, "phone_full_received": False,
              "quote_navigation_verified": False, "duplicate_free_verified": False,
              "full_acceptance_passed": False, "reasons": []}
    try:
        try:
            verify_spec(spec)
        except (TypeError, ValueError, AttributeError):
            raise _Rejected("probe_spec_invalid") from None
        case, intent = _submission(spec, send_report)
        result.update(spec_sha256=spec["spec_sha256"], case_id=case["case_id"], send_intent_id=intent["id"])
        data = _read_artifact(artifact_path)
        result.update(artifact_sha256=hashlib.sha256(data).hexdigest(), artifact_bytes=len(data))
        try:
            artifact = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_json,
                                  parse_constant=_invalid_constant)
        except (UnicodeError, json.JSONDecodeError, RecursionError):
            raise _Rejected("artifact_invalid_json") from None
        _require(isinstance(artifact, dict) and type(artifact.get("schema_version")) is int
                 and artifact["schema_version"] == 1, "artifact_schema_invalid")
        receiver = artifact.get("receiver_username")
        _require(_identity(receiver) and receiver != spec["own_username"], "independent_receiver_required")
        row = artifact.get("row")
        _require(isinstance(row, dict), "raw_row_required")
        _require(row.get("group_id") == spec["thread_id"], "peer_group_mismatch")
        _require(row.get("sender_id") == spec["own_username"], "peer_sender_mismatch")
        reply_sid = row.get("server_id")
        _require(_server_id(reply_sid), "peer_server_id_invalid")
        _require(reply_sid != case["source_server_id"], "peer_server_id_is_source")
        _require(type(row.get("local_type")) is int and row["local_type"] in QUOTE_LOCAL_TYPES,
                 "peer_message_type_mismatch")
        reply, source_sid, source_from, source_sender, source_type, source_content = _quote_fields(
            row.get("message_content"), row["sender_id"])
        result["artifact_structure_verified"] = True
        _require(reply == case["reply_text"], "reply_body_mismatch")
        _require(source_sid == case["source_server_id"], "quote_server_id_mismatch")
        _require(source_sender == case["source_sender_id"], "quote_sender_mismatch")
        # A received group quote uses the group as fromusr and the actual
        # member as chatusr. These fields cannot be matched interchangeably.
        # Early frozen probes omitted direction but bound the exact author.
        direction = case.get("source_direction", "outgoing" if case["source_sender_id"] == spec["own_username"] else "incoming")
        expected_from = (spec["thread_id"] if spec["thread_id"].endswith("@chatroom")
                         and direction == "incoming" else case["source_sender_id"])
        _require(source_from == expected_from, "quote_source_conversation_mismatch")
        # These fixed probes freeze a plain-text original. A normalized app
        # title cannot establish the native XML representation of a quoted app.
        _require(type(case.get("source_type")) is int and case["source_type"] == 1,
                 "frozen_plain_text_source_type_required")
        _require(source_type == "1", "quote_source_type_mismatch")
        _require(hashlib.sha256(source_content.encode("utf-8")).hexdigest() == case["source_body_sha256"],
                 "quote_source_content_mismatch")
        if intent.get("serverId") is not None:
            _require(reply_sid == intent["serverId"], "reply_server_id_mismatch")
        result.update(correlation_matched=True, peer_observed=True,
                      reply_server_id=reply_sid, quote_server_id=source_sid)
    except _Rejected as error:
        result["reasons"] = [str(error)]
    except (KeyError, TypeError, ValueError, RecursionError):
        # Malformed in-memory plans/reports or invalid path values cannot expose
        # payloads in diagnostics or accidentally turn a partial check into pass.
        result["reasons"] = ["evidence_input_invalid"]
    return result
