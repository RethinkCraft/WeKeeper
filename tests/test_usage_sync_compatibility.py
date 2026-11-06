"""A local bot-capable package can still upload existing SIWC usage to v2."""
import json

import httpx

from chatlog_keeper.dashboard.sync import LEGACY_USAGE_FIELDS, SyncService, usage_projection
from chatlog_keeper.dashboard.usage import UsageLedger


def test_existing_summary_and_web_chat_keep_legacy_shape_and_acknowledge(tmp_path):
    dashboard = tmp_path / "dashboard"
    ledger = UsageLedger(dashboard)
    ledger.record_run(run_id="summary_old", kind="summary", model="gpt", provider="siwc", usage={"input_tokens": 4, "output_tokens": 1, "total_tokens": 5})
    ledger.record_run(run_id="chat_old", kind="chat", model="gpt", provider="siwc", user_id=3, turn_id="turn_a", usage=None)
    calls = []

    def old_cloud(request):
        assert request.url.path == "/v1/ingest/usage"
        body = json.loads(request.content)
        calls.append(body)
        # Emulate the old strict API: new fields are rejected, even when null.
        for run in body["runs"]:
            if set(run) != set(LEGACY_USAGE_FIELDS):
                return httpx.Response(422, json={"detail": "invalid_request"})
            assert run["kind"] in {"summary", "chat"}
        return httpx.Response(200, json={"accepted": True, "applied": len(body["runs"])})

    client = httpx.Client(transport=httpx.MockTransport(old_cloud))
    service = SyncService(tmp_path / "archive", dashboard,
        {"server_url": "https://synthetic.example.invalid", "groups": ["group_a"], "machine_token": "x" * 40}, client=client)
    try:
        service.flush_usage()
        assert len(calls) == 1 and len(calls[0]["runs"]) == 2
        assert not ledger.pending()
        assert service.state.get("last_usage_reconcile") > 0
        assert next(run for run in calls[0]["runs"] if run["kind"] == "chat")["usage"] is None
    finally:
        service.state.close()
        client.close()
        ledger.close()


def test_bot_records_retain_provider_profile_group_and_request_for_new_cloud(tmp_path):
    dashboard = tmp_path / "dashboard"
    ledger = UsageLedger(dashboard)
    ledger.record_run(run_id="bot_1", kind="bot_chat", model="test", provider="deepseek", profile_id="test_profile",
                      group_ref="group_test", bot_request_id="msg_trigger", usage={"total_tokens": 7})
    calls = []

    def new_cloud(request):
        body = json.loads(request.content)
        calls.append(body)
        run, = body["runs"]
        assert run["kind"] == "bot_chat" and run["provider"] == "deepseek"
        assert run["profile_id"] == "test_profile" and run["group_ref"] == "group_test"
        assert run["bot_request_id"] == "msg_trigger"
        return httpx.Response(200, json={"accepted": True, "applied": 1})

    client = httpx.Client(transport=httpx.MockTransport(new_cloud))
    service = SyncService(tmp_path / "archive", dashboard,
        {"server_url": "https://synthetic.example.invalid", "groups": ["group_test"], "machine_token": "x" * 40}, client=client)
    try:
        service.flush_usage()
        assert len(calls) == 1 and not ledger.pending()
    finally:
        service.state.close()
        client.close()
        ledger.close()


def test_non_siwc_metadata_is_never_silently_discarded_as_legacy():
    value = {"run_id": "summary", "kind": "summary", "provider": "deepseek", "profile_id": None, "group_ref": None, "bot_request_id": None}
    assert usage_projection(value)["provider"] == "deepseek"
