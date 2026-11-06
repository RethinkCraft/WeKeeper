"""A disabled, explicitly bootstrapped bot can restart its cloud control lane."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import json

import httpx
import pytest

import chatlog_keeper.bot.control as control_module
import chatlog_keeper.bot.protocol_runtime as runtime_module
from chatlog_keeper.bot.protocol_gateway import ProtocolGateway
from chatlog_keeper.bot.protocol_runtime import ProviderResources, RuntimeConfig, RuntimeErrorCode, check_configuration
from test_bot_protocol_business import Generator
from test_bot_protocol_inbox import ACCOUNT, CONSUMER, OWN, THREAD
from test_bot_protocol_native_inbox import DirectFixture
from test_protocol_cloud_control import connected


@pytest.mark.parametrize("disabled", ["global", "group"])
def test_disabled_restart_preserves_custody_and_accepts_cloud_reenable(connected, tmp_path, monkeypatch, disabled):
    h = connected
    h.archive_state.chmod(0o700)
    h.bot.set("bot_wxid", OWN)
    h.control.bootstrap("disabled_restart")
    disabled_config = deepcopy(h.cloud.bot_overview(h.source)["config"]["data"])
    if disabled == "global":
        disabled_config["enabled"] = False
    else:
        disabled_config["groups"][0]["enabled"] = False
    h.cloud.save_bot_config({"source_id": h.source, "expected_revision": 2, "data": disabled_config}, h.admin["id"])
    h.control.poll()
    assert h.provider_calls == [] and h.bot.get("verified_profile_gpt") is None

    custody, usage = tmp_path / "custody", tmp_path / "bot-usage"
    custody.mkdir(mode=0o700); usage.mkdir(mode=0o700)
    peer = DirectFixture(custody)
    with peer.gateway() as gateway, peer.inbox(gateway) as inbox:
        inbox.activate()
        checkpoint = inbox._checkpoint()
    token, bindings = custody / "token", custody / "bindings.json"
    token.write_text("synthetic-token"); token.chmod(0o600)
    bindings.write_text(json.dumps({THREAD: h.group})); bindings.chmod(0o600)
    config = RuntimeConfig(state_dir=h.archive_state, dashboard_dir=h.dashboard_dir, usage_dir=usage,
        inbox_path=custody / "inbox.sqlite", outbox_path=custody / "outbox.sqlite",
        journal_path=custody / "gateway.sqlite", token_file=token, bindings_file=bindings,
        account_id=ACCOUNT, own_username=OWN, consumer=CONSUMER, server="http://127.0.0.1:6174",
        cloud_control=True, archive_state=h.archive_state)
    assert not (h.dashboard_dir / "siwc" / "credentials.json").exists()
    checked = check_configuration(config)
    assert checked["activated"] and checked["profile_ids"] == []
    with pytest.raises(RuntimeErrorCode, match="bot_disabled|binding_unavailable"):
        check_configuration(replace(config, cloud_control=False))
    with pytest.raises(RuntimeErrorCode, match="inbox_binding_changed"):
        check_configuration(replace(config, consumer="another-consumer"))
    with pytest.raises(RuntimeErrorCode, match="bot_identity_changed"):
        check_configuration(replace(config, own_username="another-bot"))
    marker = h.bot.get("cloud_control_bootstrap")
    h.bot.set("cloud_control_bootstrap", None)
    with pytest.raises(RuntimeErrorCode, match="bootstrap_required"):
        check_configuration(config)
    h.bot.set("cloud_control_bootstrap", marker)

    # Keep the real inbox, business store, outbox and control client. Every
    # gateway request uses the synthetic peer; inference remains forbidden.
    def gateway_factory(**kwargs):
        return ProtocolGateway(**kwargs, transport=httpx.MockTransport(peer.handle))
    class NoInferenceResources(ProviderResources):
        def provider(self, *_args, **_kwargs):
            pytest.fail("disabled bindings cannot construct an inference provider")
    original_control = control_module.ControlClient
    monkeypatch.setattr(runtime_module, "ProtocolGateway", gateway_factory)
    monkeypatch.setattr(runtime_module, "ProviderResources", NoInferenceResources)
    monkeypatch.setattr(control_module, "ControlClient", lambda **kwargs: original_control(**kwargs, client=h.http))
    owner = runtime_module.open_runtime(config)
    try:
        peer.add("10", body="你好", senderId="wxid_manager", appMessageType=None, quote=None,
                 createdAt=datetime.now(timezone.utc).isoformat())
        owner._receive(); owner.business.run_once(block=True)
        assert owner.outbox.pending() == [] and h.provider_calls == []
        owner.control.snapshot()
        assert h.cloud.bot_overview(h.source)["runtime"]["enabled"] == disabled_config["enabled"]
        assert owner.inbox._checkpoint() == checkpoint
    finally:
        owner.close()

    # Reopen disabled with the original checkpoint, then receive an explicitly
    # newer cloud command. This command alone can re-enable the business loop.
    owner = runtime_module.open_runtime(config)
    try:
        assert owner.inbox._checkpoint() == checkpoint
        owner.control.validation_factory = h.worker.provider_factory
        enabled = deepcopy(disabled_config)
        enabled["enabled"] = True
        enabled["groups"][0]["enabled"] = True
        h.cloud.save_bot_config({"source_id": h.source, "expected_revision": 3, "data": enabled}, h.admin["id"])
        owner.control.poll()
        assert owner.business.store.binding(h.group)["enabled"]
        owner.control.snapshot()
        assert h.cloud.bot_overview(h.source)["runtime"]["enabled"] is True
        fake_generator = Generator(owner.business.store)
        owner.business.generator = fake_generator
        peer.add("20", body="你好", senderId="wxid_manager", appMessageType=None, quote=None,
                 createdAt=datetime.now(timezone.utc).isoformat())
        owner._receive(); owner.business.run_once(block=True)
        projection = owner.business.project_outbox()
        assert projection["errors"] == [], projection
        assert len(fake_generator.calls) == 1 and len(owner.outbox.pending()) == 1, owner.outbox.delivery_states()
        assert not any("/sends" in path for _method, path in peer.calls)
    finally:
        owner.close()
