"""Synthetic adversarial records only; these tests are not live quote evidence."""
import hashlib
import json

import pytest

from chatlog_keeper.bot.protocol_acceptance import prepare_probe
from chatlog_keeper.bot.protocol_peer_evidence import assess_peer_artifact, MAX_ARTIFACT_BYTES


def inputs():
    spec = prepare_probe(run_id="synthetic-negative", account_id="test-account", own_username="test-bot",
                         thread_id="test@chatroom", sources=[
                             {"id": "opaque-1", "accountId": "test-account", "threadId": "test@chatroom",
                              "serverId": "9007199254740993", "senderId": "test-author", "body": "same", "direction": "incoming", "type": 1},
                             {"id": "opaque-2", "accountId": "test-account", "threadId": "test@chatroom",
                              "serverId": "9007199254740994", "senderId": "test-author", "body": "same", "direction": "incoming", "type": 1}])
    case = spec["cases"][0]
    report = {"schema_version": 1, "spec_sha256": spec["spec_sha256"], "case_id": "1",
              "model_calls": 0, "business_writes": 0, "send": {
                  "idempotency_key": case["idempotency_key"], "outcome": "unknown", "intent": {
                      "id": "test-intent", "accountId": "test-account", "destination": "test@chatroom",
                      "state": "needs_review", "serverId": None}}}
    xml = ("<msg><appmsg><type>57</type><title>" + case["reply_text"] + "</title>"
           "<refermsg><type>1</type><svrid>9007199254740993</svrid><fromusr>test@chatroom</fromusr><chatusr>test-author</chatusr>"
           "<content>same</content></refermsg></appmsg><fromusername>test-bot</fromusername></msg>")
    artifact = {"schema_version": 1, "receiver_username": "test-peer", "row": {
        "group_id": "test@chatroom", "sender_id": "test-bot", "server_id": "18446744073709551615",
        "local_type": 49, "message_content": xml}}
    return spec, report, artifact


def assess(tmp_path, spec, report, artifact):
    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps(artifact, ensure_ascii=False), encoding="utf-8")
    return assess_peer_artifact(spec, report, path)


def no_acceptance(result):
    assert not result["provenance_authenticated"]
    assert not result["native_receipt_validated"]
    assert not result["phone_full_received"]
    assert not result["quote_navigation_verified"]
    assert not result["duplicate_free_verified"]
    assert not result["full_acceptance_passed"]


@pytest.mark.parametrize("field,value", [
    ("group_id", "other@chatroom"), ("sender_id", "other-bot"),
    ("server_id", 18446744073709551615), ("server_id", "018446744073709551615"),
    ("server_id", "18446744073709551616"), ("server_id", "9007199254740993"),
    ("local_type", True), ("local_type", 57), ("local_type", (6 << 32) | 49),
    ("message_content", {"type": 57}),
])
def test_wrong_raw_row_cannot_observe_peer(tmp_path, field, value):
    spec, report, artifact = inputs()
    artifact["row"][field] = value
    result = assess(tmp_path, spec, report, artifact)
    assert not result["peer_observed"] and result["reasons"]
    no_acceptance(result)


@pytest.mark.parametrize("before,after", [
    ("<type>57</type>", "<type>6</type>"),
    ("9007199254740993", "9007199254740994"),
    ("<fromusr>test@chatroom</fromusr>", "<fromusr>wrong</fromusr>"),
    ("<chatusr>test-author</chatusr>", "<chatusr>wrong</chatusr>"),
    ("<content>same</content>", "<content>wrong</content>"),
    ("<content>same</content>", ""),
    ("<content>same</content>", "<content>same</content><content>same</content>"),
    ("<refermsg><type>1</type>", "<refermsg><type>49</type>"),
    ("<refermsg><type>1</type>", "<refermsg><type>1</type><type>1</type>"),
    ("<fromusr>test@chatroom</fromusr><chatusr>test-author</chatusr>", "<fromusr>test-author</fromusr><chatusr>test@chatroom</chatusr>"),
    ("<fromusername>test-bot</fromusername>", "<fromusername>wrong</fromusername>"),
    ("<type>57</type>", "<type>57</type><type>6</type>"),
    ("<svrid>9007199254740993</svrid>", "<svrid>9007199254740993</svrid><svrid>9007199254740994</svrid>"),
    ("<svrid>9007199254740993</svrid>", "<nested><svrid>9007199254740993</svrid></nested>"),
    ("<fromusr>test@chatroom</fromusr>", "<fromusr><b>test@chatroom</b></fromusr>"),
    ("<msg>", "<!DOCTYPE msg [<!ENTITY injected 'anything'>]><msg>"),
    ("</appmsg>", "</appmsg><appmsg><type>57</type></appmsg>"),
    ("</msg>", "</msg>trailing-garbage"),
    ("<msg>", "other-bot:\n<msg>"),
])
def test_xml_identity_ambiguity_and_malformed_rejected(tmp_path, before, after):
    spec, report, artifact = inputs()
    artifact["row"]["message_content"] = artifact["row"]["message_content"].replace(before, after)
    result = assess(tmp_path, spec, report, artifact)
    assert not result["peer_observed"] and result["reasons"]
    no_acceptance(result)


@pytest.mark.parametrize("local_type", [49, (57 << 32) | 49])
@pytest.mark.parametrize("carrier", ["document", "fragment", "sender-prefix"])
def test_supported_carriers_still_reject_partial_reply(tmp_path, local_type, carrier):
    spec, report, artifact = inputs()
    artifact["row"]["local_type"] = local_type
    xml = artifact["row"]["message_content"].replace(spec["cases"][0]["reply_text"], "partial")
    if carrier == "fragment":
        xml = xml.removeprefix("<msg>").removesuffix("</msg>")
    elif carrier == "sender-prefix":
        xml = "test-bot:\n" + xml
    artifact["row"]["message_content"] = xml
    result = assess(tmp_path, spec, report, artifact)
    assert result["artifact_structure_verified"]
    assert result["reasons"] == ["reply_body_mismatch"]
    assert not result["peer_observed"]


@pytest.mark.parametrize("field,value", [("spec_sha256", "a" * 64), ("case_id", "2"),
                                         ("model_calls", 1), ("business_writes", True)])
def test_report_must_be_same_fixed_submission(tmp_path, field, value):
    spec, report, artifact = inputs()
    report[field] = value
    result = assess(tmp_path, spec, report, artifact)
    assert not result["peer_observed"] and result["reasons"]


@pytest.mark.parametrize("field,value", [("accountId", "wrong"), ("destination", "other@chatroom"),
                                         ("serverId", "18446744073709551614"),
                                         ("serverId", 9007199254740993), ("state", "failed")])
def test_wrong_send_intent_identity_rejected(tmp_path, field, value):
    spec, report, artifact = inputs()
    report["send"]["intent"][field] = value
    result = assess(tmp_path, spec, report, artifact)
    assert not result["peer_observed"] and result["reasons"]


def test_missing_intent_or_changed_key_is_not_permission_to_resubmit(tmp_path):
    spec, report, artifact = inputs()
    report["send"]["intent"] = None
    result = assess(tmp_path, spec, report, artifact)
    assert result["reasons"] == ["send_intent_required"]
    report["send"]["idempotency_key"] = "another-key"
    result = assess(tmp_path, spec, report, artifact)
    assert result["reasons"] == ["send_report_key_mismatch"]


@pytest.mark.parametrize("field,value", [("outcome", []), ("intent", "not-an-intent")])
def test_malformed_report_is_bounded_failure(tmp_path, field, value):
    spec, report, artifact = inputs()
    report["send"][field] = value
    result = assess(tmp_path, spec, report, artifact)
    assert not result["peer_observed"] and result["reasons"]
    no_acceptance(result)


def test_accepted_state_without_server_identity_is_rejected(tmp_path):
    spec, report, artifact = inputs()
    report["send"]["intent"]["state"] = "accepted"
    result = assess(tmp_path, spec, report, artifact)
    assert result["reasons"] == ["send_intent_server_id_invalid"]


def test_matching_synthetic_file_cannot_authenticate_provenance_or_live_acceptance(tmp_path):
    spec, report, artifact = inputs()
    # Supplied booleans and hashes are not read as evidence, even with a record
    # that correlates. This is a negative acceptance test, not a live fixture.
    artifact.update(provenance_authenticated=True, native_receipt_validated=True,
                    phone_full_received=True, quote_navigation_verified=True,
                    full_acceptance_passed=True, artifact_sha256="a" * 64)
    report["send"]["outcome"] = "accepted"
    report["send"]["intent"].update(state="accepted", serverId=artifact["row"]["server_id"])
    result = assess(tmp_path, spec, report, artifact)
    no_acceptance(result)
    actual = (tmp_path / "synthetic.json").read_bytes()
    assert result["artifact_sha256"] == hashlib.sha256(actual).hexdigest()
    assert result["artifact_sha256"] != artifact["artifact_sha256"]
    assert result["artifact_bytes"] == len(actual)


def test_sender_local_echo_is_not_an_independent_receiver(tmp_path):
    spec, report, artifact = inputs()
    artifact["receiver_username"] = spec["own_username"]
    result = assess(tmp_path, spec, report, artifact)
    assert result["reasons"] == ["independent_receiver_required"]
    assert not result["peer_observed"]


@pytest.mark.parametrize("data", [b"<appmsg><type>57</type></appmsg>", b'{"schema_version":1,"schema_version":1}',
                                  b'{"schema_version":NaN}', b"\xff", b"[]"])
def test_bare_xml_duplicate_keys_and_invalid_json_are_not_peer_records(tmp_path, data):
    spec, report, _artifact = inputs()
    path = tmp_path / "bad.json"
    path.write_bytes(data)
    result = assess_peer_artifact(spec, report, path)
    assert not result["peer_observed"] and result["reasons"]
    assert result["artifact_sha256"] == hashlib.sha256(data).hexdigest()


def test_missing_oversized_and_symlink_artifacts_are_rejected(tmp_path):
    spec, report, _artifact = inputs()
    path = tmp_path / "not-present.json"
    assert assess_peer_artifact(spec, report, path)["reasons"] == ["artifact_unreadable"]
    path.write_bytes(b" " * (MAX_ARTIFACT_BYTES + 1))
    assert assess_peer_artifact(spec, report, path)["reasons"] == ["artifact_size_invalid"]
    link = tmp_path / "link.json"
    link.symlink_to(path)
    assert assess_peer_artifact(spec, report, link)["reasons"] == ["artifact_unreadable"]
