"""Protocol control upgrade: synthetic archives, mocked cloud, no model/client."""
from copy import deepcopy
from contextlib import closing
from dataclasses import replace
import json
from types import SimpleNamespace
import threading

import httpx
import pytest

from chatlog_keeper.bot.cli import main, parser
from chatlog_keeper.bot.control import ControlClient
from chatlog_keeper.bot.protocol_business import ProtocolBusinessWorker
from chatlog_keeper.bot.protocol_outbox import ProtocolOutbox
from chatlog_keeper.bot.protocol_runtime import RuntimeErrorCode, check_configuration
from chatlog_keeper.bot.protocol_runtime import ProviderResources
from chatlog_keeper.bot.providers import ProviderError, profile_signature
from chatlog_keeper.dashboard.bot_control import BotBootstrap
from test_bot_control_client import Harness
from test_bot_protocol_business import ACCOUNT, OWN, Generator, Inbox
from test_bot_protocol_runtime import cli_args, local_config, runtime


@pytest.fixture
def connected(tmp_path):
    h = Harness(tmp_path)
    original = h.transport
    def transport(request):
        if request.url.path == "/v1/ingest/usage":
            h.uploaded_usage = json.loads(request.content)
            return httpx.Response(200, json={"accepted": True})
        if request.url.path.endswith("/bootstrap"):
            assert request.headers["authorization"].startswith("Bearer ")
            data = BotBootstrap.model_validate_json(request.content).model_dump(mode="json")
            h.bootstrap_payload = data
            return httpx.Response(200, json=h.cloud.bootstrap_bot(data))
        return original(request)
    h.http.close()
    h.http = httpx.Client(transport=httpx.MockTransport(transport))
    h.control.http = h.http
    h.bot.configure(h.control._native_config(h.config))
    h.bot.set("cloud_control_bootstrap", None)
    yield h
    h.control.close(); h.http.close(); h.bot.close(); h.cloud.close()


def test_pending_commands_cannot_run_before_explicit_alignment(connected):
    h = connected
    before = deepcopy(h.bot.config())
    with pytest.raises(ValueError, match="bootstrap_required"):
        h.control.poll()
    assert h.bot.config() == before and h.provider_calls == []
    pending = h.cloud.poll_bot_commands(h.source)["commands"]
    assert pending and pending[0]["id"] == h.command["id"]
    result = h.control.bootstrap("release_0_7_0")
    assert result == {"accepted": True, "config_revision": 2, "cloud_control_enabled": False}
    assert h.bot.config() == before and h.provider_calls == []
    assert h.cloud.poll_bot_commands(h.source)["commands"] == []
    command = h.cloud.bot_overview(h.source)["commands"][0]
    assert command["status"] == "failed" and command["error"] == "cloud_control_reinitialized"
    assert "wxid_" not in json.dumps(h.bootstrap_payload)
    assert h.cloud.bot_overview(h.source)["config"]["data"]["personas"] == before["personas"]


def test_alignment_retry_does_not_cancel_later_admin_commands(connected):
    h = connected
    h.control.bootstrap("release_0_7_0")
    overview = h.cloud.bot_overview(h.source)
    changed = deepcopy(overview["config"]["data"])
    changed["personas"][0]["markdown"] += "\nsynthetic change"
    h.cloud.save_bot_config({"source_id": h.source, "expected_revision": 2, "data": changed}, h.admin["id"])
    assert h.control.bootstrap("release_0_7_0")["config_revision"] == 2
    assert len(h.cloud.poll_bot_commands(h.source)["commands"]) == 1
    h.control.poll()
    assert h.bot.config()["personas"][0]["markdown"].endswith("synthetic change")
    assert h.bot.get("cloud_config_revision") == 3


def test_alignment_id_rejects_a_different_local_configuration(connected):
    h = connected
    h.control.bootstrap("release_0_7_0")
    changed = deepcopy(h.bot.config()); changed["enabled"] = False
    h.bot.configure(changed)
    with pytest.raises(ValueError, match="bootstrap_id_conflict"):
        h.control.bootstrap("release_0_7_0")


def test_separate_bot_usage_uses_existing_dashboard_source(connected, tmp_path):
    from chatlog_keeper.dashboard.usage import UsageLedger
    h = connected
    usage_dir = tmp_path / "independent-bot-usage"
    with closing(UsageLedger(usage_dir)) as ledger:
        ledger.start_run(run_id="isolated_bot", kind="bot_chat", provider="deepseek",
            profile_id="private_api", group_ref=h.group, bot_request_id="synthetic_request")
        ledger.finish_run("isolated_bot", status="completed")
    h.control.usage_dir = usage_dir
    assert h.control.upload_usage() == 1
    assert h.uploaded_usage["source_id"] == h.source
    assert h.uploaded_usage["runs"][0]["run_id"] == "isolated_bot"
    assert h.control.upload_usage() == 0


def test_unverified_compatible_profile_uses_a_disposable_real_provider_probe(connected, tmp_path):
    h = connected
    requests = []
    def remote(request):
        body = json.loads(request.content); requests.append(body)
        assert [tool["function"]["name"] for tool in body["tools"]] == ["list_offer_items"]
        message = {"tool_calls": [{"id": "probe", "type": "function", "function": {
            "name": "list_offer_items", "arguments": "{}"}}]} if len(requests) == 1 else {"content": "verified"}
        return httpx.Response(200, json={"choices": [{"message": message}]})
    resources = ProviderResources(h.bot, state_dir=h.archive_state, usage_dir=tmp_path / "probe-usage",
        dashboard_dir=h.dashboard_dir, http_factory=lambda **kwargs: httpx.Client(
            **kwargs, transport=httpx.MockTransport(remote)))
    profile = deepcopy(h.config["providers"][1])
    try:
        h.control.validation_factory = resources.validation_provider
        result = h.control._verify_profiles({"enabled": True, "providers": [profile],
            "groups": [{"group_ref": h.group, "provider_id": profile["id"], "enabled": True}]})
        assert result == {profile["id"]: profile_signature(profile)} and len(requests) == 2
        assert h.bot.get("verified_profile_" + profile["id"]) is None
        assert "tools_verified" not in profile
        ordinary = resources.provider(profile)
        try:
            with pytest.raises(ProviderError, match="tools_not_verified"):
                ordinary.complete([], "", [], lambda *_: None, {})
        finally:
            ordinary.close()
        assert len(requests) == 2
    finally:
        resources.close()


@pytest.mark.parametrize("disable", ["global", "groups"])
def test_revocation_cancels_pending_without_contacting_any_model(connected, disable):
    h = connected
    h.control.bootstrap("release_0_7_0")
    event = {"event_id": "pending_native", "message_ref": h.message, "group_ref": h.group,
        "sender_id": "wxid_manager", "text": "你好", "occurred_at": "2026-10-27T12:00:00+08:00"}
    h.bot.begin_request(event)
    h.bot.queue_reply(event["event_id"], "synthetic reply")
    def unavailable(_profile):
        pytest.fail("revocation must not contact an unverified or expired provider")
    h.control.validation_factory = unavailable
    configuration = deepcopy(h.cloud.bot_overview(h.source)["config"]["data"])
    if disable == "global":
        configuration["enabled"] = False
    else:
        for group in configuration["groups"]:
            group["enabled"] = False
    h.cloud.save_bot_config({"source_id": h.source, "expected_revision": 2, "data": configuration}, h.admin["id"])
    h.control.poll()
    assert h.bot.reply(event["event_id"])["status"] == "cancelled"
    assert h.cloud.bot_overview(h.source)["config"]["applied_revision"] == 3


def test_stop_drains_current_probe_then_leaves_command_pending_for_restart(connected, monkeypatch):
    h = connected
    h.control.bootstrap("shutdown_test")
    native = h.control._native_config(h.config)
    native["groups"].append({**native["groups"][0], "group_ref": "group_second", "provider_id": "private_api"})
    entered, release, stop = threading.Event(), threading.Event(), threading.Event()
    h.control.runtime = SimpleNamespace(_stop=stop)
    calls, closed, acknowledgements, failures = [], [], [], []
    command = {"id": "shutdown_command", "kind": "apply_config", "revision": 3,
               "payload": {"config": native, "revision": 3}}
    monkeypatch.setattr(h.control, "_native_config", lambda value: value)
    def post(route, value):
        if route == "poll": return {"commands": [command]}
        acknowledgements.append(value); return {"accepted": True}
    monkeypatch.setattr(h.control, "_post", post)
    class Probe:
        def __init__(self, profile): self.profile = profile
        def complete(self, _messages, _instructions, _tools, handler, _metadata):
            calls.append(self.profile["id"])
            if len(calls) == 1:
                entered.set(); assert release.wait(3)
            handler("list_offer_items", {}); return "verified"
        def close(self): closed.append(self.profile["id"])
    h.control.validation_factory = Probe
    before = deepcopy(h.bot.config())
    def poll():
        try: h.control.poll()
        except Exception as error: failures.append(error)
    thread = threading.Thread(target=poll); thread.start()
    try:
        assert entered.wait(2)
        stop.set(); release.set(); thread.join(2)
        assert not thread.is_alive() and failures == []
        assert calls == closed == ["gpt"] and acknowledgements == []
        assert h.bot.config() == before and h.bot.get("control_shutdown_command") is None
        stop.clear()
        h.control.poll(); h.control.poll()
        assert len(calls) == 3 and len(acknowledgements) == 2
        assert h.bot.config()["revision"] == before["revision"] + 1
    finally:
        release.set(); thread.join(3)


def test_protocol_acceptance_counts_do_not_rewrite_legacy_delivery(connected, tmp_path):
    h = connected
    h.control.bootstrap("release_0_7_0")
    inbox = Inbox()
    key = inbox.add("42", "你好")
    outbox = ProtocolOutbox(tmp_path / "native.sqlite", account_id=ACCOUNT, own_username=OWN)
    worker = ProtocolBusinessWorker(inbox=inbox, store=h.bot, generator=Generator(h.bot), outbox=outbox,
        bindings={"a@chatroom": h.group}, own_username=OWN, account_id=ACCOUNT)
    try:
        worker.run_once(block=True); worker.project_outbox()
        with outbox._lock:
            outbox._db.execute("UPDATE replies SET state='accepted'")
        h.control.runtime = SimpleNamespace(business=worker, outbox=outbox,
            status=lambda: {"state": "running", "account": {"authenticated": True, "connection": "connected"}})
        assert h.control.snapshot()
        overview = h.cloud.bot_overview(h.source)
        assert overview["runtime"]["counts"] == {"requests": 1, "replied": 1, "pending": 0, "failed": 0, "unknown": 0}
        assert overview["runtime"]["last_request"]["status"] == "accepted"
        assert overview["capabilities"]["native_ready"]
        assert h.bot.reply(key)["status"] == "pending"
    finally:
        worker.close(); outbox.close()


def test_check_with_cloud_control_requires_bootstrap_without_writing(local_config):
    configuration = replace(local_config, cloud_control=True, archive_state=local_config.state_dir)
    with pytest.raises(RuntimeErrorCode):
        check_configuration(configuration)


def test_installed_check_never_initializes_mutable_bot_state(local_config, monkeypatch, capsys):
    from chatlog_keeper.bot.store import BotStore
    monkeypatch.setattr(BotStore, "__init__", lambda *_args, **_kwargs: pytest.fail("check cannot initialize state"))
    assert main(["check", *cli_args(local_config)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["mode"] == "check" and output["gateway_contacted"] is False


@pytest.mark.parametrize("flag", ["--send", "--once", "--native-lock-dir"])
def test_retired_ui_options_are_rejected_before_opening_state(flag):
    with pytest.raises(SystemExit):
        parser().parse_args(["run", flag])


def test_cloud_wait_cannot_block_native_lanes():
    entered, release = threading.Event(), threading.Event()
    owner = runtime()
    owner.business.store = SimpleNamespace(set=lambda *_: None)
    class Control:
        def poll(self):
            entered.set(); assert release.wait(3)
        def snapshot(self): pass
    owner.control = Control()
    try:
        owner.start()
        assert entered.wait(2)
        assert owner.inbox.received.wait(2) and owner.outbox.sent.wait(2)
        assert owner.business.generated.wait(2) and owner.business.projected.wait(2)
    finally:
        release.set(); owner.outbox.status_release.set(); owner.close()
