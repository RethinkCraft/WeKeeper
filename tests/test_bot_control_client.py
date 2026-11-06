"""Real local/cloud control integration with synthetic state and mocked HTTPS."""
from datetime import datetime
import json
from types import SimpleNamespace

import httpx
import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.bot.control import ControlClient
from chatlog_keeper.bot.providers import save_credential
from chatlog_keeper.bot.service import Service
from chatlog_keeper.bot.store import BotStore
from chatlog_keeper.dashboard.bot_control import BotAck, BotConfig, BotPoll, BotSnapshot
from chatlog_keeper.dashboard.local_state import LocalState
from chatlog_keeper.dashboard.projection import projections
from chatlog_keeper.dashboard.protocol import content_hash
from chatlog_keeper.dashboard.store import DashboardStore
from test_archive import commit, raw, setup_source

NOW = datetime.fromisoformat("2026-10-27T12:00:00+08:00")
DAY = NOW.date().isoformat()
MACHINE_TOKEN = "synthetic-machine-token-which-never-leaves-the-mock"
PROVIDER_KEY = "SYNTHETIC-KEY-NOT-A-REAL-CREDENTIAL"


class Harness:
    def __init__(self, root):
        self.archive_state = root / "archive-state"
        self.dashboard_dir = self.archive_state / "dashboard"
        self.sent = []
        self.lose_ack = None
        with ArchiveStore(self.archive_state) as archive:
            source = setup_source(archive, maximum=0)
            self.group = archive.conn.execute("SELECT group_ref FROM groups").fetchone()[0]
            self.message = commit(archive, source, [raw(
                text="今日娱乐首单已无", timestamp=int(NOW.timestamp()), sender_id="wxid_manager",
                parsed={"msg_type": 1, "text": "今日娱乐首单已无", "parse_status": "parsed",
                        "mentions": ["wxid_robot"], "mentions_status": "verified"})], mode="live")["message_refs"][0]
        local = LocalState(self.dashboard_dir)
        self.source = local.source_id
        selected = projections(self.archive_state, [self.group], local.get("sender_salt"))
        self.reporter = selected[0]["messages"][0]["sender_id"]
        (self.dashboard_dir / "config.json").write_text(json.dumps({
            "server_url": "https://synthetic.example.invalid", "groups": [self.group], "machine_token": MACHINE_TOKEN}))
        local.close()
        self.cloud = DashboardStore(root / "cloud", clock=lambda: NOW.timestamp())
        self.admin = self.cloud.create_user("admin", "synthetic-admin-password")
        for data in selected:
            self.cloud.ingest_partition({"source_id": self.source, "group_ref": data["group"]["group_ref"],
                "day": data["day"], "revision": 1, "initial": True, "data": data, "content_hash": content_hash(data)})
        self.bot = BotStore(self.archive_state)
        self.bot.set("bot_wxid", "wxid_robot")
        self.worker = SimpleNamespace(store=self.bot, archive_state=self.archive_state, dashboard_dir=self.dashboard_dir, sender=None)
        self.provider_calls = []
        owner = self
        class ProbeProvider:
            def __init__(self, profile):
                self.profile = dict(profile)
            def complete(self, messages, instructions, tools, handler, metadata):
                owner.provider_calls.append({"profile": dict(self.profile), "tools": tools, "metadata": metadata})
                handler("list_offer_items", {})
                return "验证完成。"
            def close(self):
                pass
        self.worker.provider_factory = ProbeProvider
        self.http = httpx.Client(transport=httpx.MockTransport(self.transport))
        self.control = ControlClient(store=self.bot, state_dir=self.archive_state, archive_state=self.archive_state,
                                     dashboard_dir=self.dashboard_dir, provider_factory=self.worker.provider_factory, client=self.http)
        self.bot.set("cloud_control_bootstrap", {"source_id": self.source})
        # This is a fake secret fixture; no provider requests are ever issued.
        save_credential(self.archive_state, "private_profile", PROVIDER_KEY)
        self.config = BotConfig.model_validate({
            "enabled": True,
            "businesses": [{"id": "shop", "name": "总店", "reporter_ids": [self.reporter],
                            "items": [{"id": "fun", "name": "娱乐首单", "aliases": ["娱乐"]}]}],
            "providers": [{"id": "gpt", "name": "GPT", "kind": "siwc", "model": ""},
                          {"id": "private_api", "name": "DeepSeek", "kind": "deepseek", "model": "synthetic-model", "credential_ref": "private_profile"}],
            "groups": [{"group_ref": self.group, "business_id": "shop", "persona_id": "alpha", "provider_id": "gpt", "enabled": True}],
        }).model_dump(mode="json")
        self.command = self.cloud.save_bot_config({"source_id": self.source, "expected_revision": 0, "data": self.config}, self.admin["id"])["command"]

    def transport(self, request):
        assert request.headers["authorization"] == "Bearer " + MACHINE_TOKEN
        assert request.url.host == "synthetic.example.invalid"
        payload = json.loads(request.content)
        self.sent.append((request.url.path.rsplit("/", 1)[-1], payload))
        operation = self.sent[-1][0]
        if operation == "poll":
            value = BotPoll.model_validate(payload)
            response = self.cloud.poll_bot_commands(value.source_id)
        elif operation == "ack":
            value = BotAck.model_validate(payload).model_dump(mode="json", exclude_none=True)
            lost = self.lose_ack
            self.lose_ack = None
            if lost == "before_accept":
                raise httpx.ReadTimeout("synthetic lost ACK before cloud acceptance", request=request)
            response = self.cloud.acknowledge_bot_command(value)
            if lost == "after_accept":
                raise httpx.ReadTimeout("synthetic lost ACK after cloud acceptance", request=request)
        elif operation == "snapshot":
            # Validate the actual serialized ControlClient output at the wire
            # boundary, not a handcrafted replacement snapshot.
            value = BotSnapshot.model_validate(payload).model_dump(mode="json")
            response = self.cloud.ingest_bot_snapshot(value)
        else:
            pytest.fail("unexpected network operation")
        return httpx.Response(200, json=response)

    def restart_control(self):
        self.control.close()
        self.control = ControlClient(store=self.bot, state_dir=self.archive_state, archive_state=self.archive_state,
                                     dashboard_dir=self.dashboard_dir, provider_factory=self.worker.provider_factory, client=self.http)
        self.bot.set("cloud_control_bootstrap", {"source_id": self.source})

    def report_from_group(self):
        self.bot.begin_request({"event_id": "local_report", "message_ref": self.message, "group_ref": self.group,
            "sender_id": "wxid_manager", "text": "今日娱乐首单已无", "occurred_at": NOW.isoformat(), "archive_id": 1})
        result = Service(self.bot, {"event_id": "local_report", "writable": True, "now": NOW.isoformat()}).call(
            "report_offer_status", {"item": "娱乐", "status": "unavailable", "evidence": "今日娱乐首单已无"})
        assert result["ok"] and result["version"] == 1

    def adjust(self, expected_revision, state):
        return self.cloud.adjust_bot_status({"source_id": self.source, "business_id": "shop", "item_id": "fun",
            "day": DAY, "status": state, "expected_revision": expected_revision}, self.admin["id"])["command"]

    def close(self):
        self.control.close()
        self.http.close()
        self.bot.close()
        self.cloud.close()


@pytest.fixture
def harness(tmp_path):
    value = Harness(tmp_path)
    try:
        yield value
    finally:
        value.close()


@pytest.mark.parametrize("lost", ["before_accept", "after_accept"])
def test_anonymous_config_maps_locally_and_lost_ack_restart_never_reapplies(harness, lost):
    h = harness
    h.lose_ack = lost
    with pytest.raises(httpx.ReadTimeout):
        h.control.poll()
    assert h.bot.config()["businesses"][0]["reporter_ids"] == ["wxid_manager"]
    assert h.bot.config()["revision"] == 1
    assert len([row for row in h.bot.audit() if row["kind"] == "configuration"]) == 1
    assert h.cloud.bot_overview(h.source)["config"]["data"]["businesses"][0]["reporter_ids"] == [h.reporter]
    h.restart_control()
    h.control.poll()
    assert h.bot.config()["revision"] == 1
    assert len([row for row in h.bot.audit() if row["kind"] == "configuration"]) == 1
    assert h.cloud.bot_overview(h.source)["config"]["applied_revision"] == 1
    assert h.cloud.bot_overview(h.source)["commands"][0]["status"] == "applied"
    assert len([call for call in h.sent if call[0] == "ack"]) == (2 if lost == "before_accept" else 1)


def test_local_report_race_fails_stale_admin_command_and_snapshots_keep_identity_private(harness):
    h = harness
    h.control.poll()
    stale_command = h.adjust(0, "available")
    # The machine sees a new group report after the web admin froze revision 0.
    h.report_from_group()
    h.control.poll()
    command = next(c for c in h.cloud.bot_overview(h.source)["commands"] if c["id"] == stale_command["id"])
    assert command["status"] == "failed" and command["error"] == "offer_version_conflict"
    assert h.bot.offer_status("shop", DAY, "fun")["version"] == 1
    h.bot.set("runtime", {"state": "ready", "enabled": True, "heartbeat_at": NOW.timestamp(), "queue_depth": 0, "native_ready": False})
    h.control.snapshot()
    first = [payload for operation, payload in h.sent if operation == "snapshot"][-1]
    assert first["statuses"][0]["actor"] == h.reporter
    assert first["statuses"][0]["message_ref"] == h.message
    assert first["audit"][0]["status"] == "unavailable"
    assert first["audit"][0]["previous_status"] == "unknown"
    assert first["providers"][1]["configured"] and first["providers"][1]["state"] == "tools_unverified"
    # Now an up-to-date administrator edit succeeds and has no chat citation.
    applied_command = h.adjust(1, "available")
    h.control.poll()
    h.control.snapshot()
    overview = h.cloud.bot_overview(h.source)
    assert next(c for c in overview["commands"] if c["id"] == applied_command["id"])["status"] == "applied"
    state, = overview["statuses"]
    assert state["status"] == "available" and state["revision"] == 2
    assert state["actor"] == "dashboard:" + str(h.admin["id"])
    assert state["group_ref"] is None and state["message_ref"] is None
    admin_audit = next(row for row in overview["audit"] if row["revision"] == 2)
    assert admin_audit["group_ref"] is None and admin_audit["message_ref"] is None
    assert admin_audit["previous_status"] == "unavailable"
    snapshots = [payload for operation, payload in h.sent if operation == "snapshot"]
    serialized = json.dumps(snapshots)
    assert "wxid_manager" not in serialized and "wxid_robot" not in serialized
    assert "test@chatroom" not in serialized and PROVIDER_KEY not in serialized
    assert "api_key" not in serialized and "machine_token" not in serialized
    # Retrying identical snapshots preserves immutable audit and state revisions.
    h.control.snapshot()
    assert len(h.cloud.bot_overview(h.source)["audit"]) == 2
    assert h.bot.offer_status("shop", DAY, "fun")["version"] == 2




def test_snapshot_delivers_effective_model_role_timing_and_counts_without_secrets(harness):
    h = harness
    h.control.poll()
    h.bot.set("runtime", {"state": "ready", "enabled": True, "heartbeat_at": NOW.timestamp(), "native_ready": False,
        "active": {"model": "stale-model"}, "secret_debug_field": PROVIDER_KEY})
    h.bot.set("active", {"group_ref": h.group, "provider": "siwc", "profile_id": "gpt", "model": "resolved-model",
        "reasoning_effort": "low", "persona_id": "alpha", "persona_version": 2})
    h.bot.set("last_request", {"request_id": "req_1", "group_ref": h.group, "status": "completed", "started_at": NOW.timestamp() - 5,
        "finished_at": NOW.timestamp(), "generation_seconds": 2, "end_to_end_seconds": 5, "visible_reply_seconds": 4})
    h.bot.set("latency_metrics", {"generation": {"count": 3, "p50_seconds": 2, "p95_seconds": 4},
        **{f"stage_{n}": {"count": 1, "p50_seconds": 1, "p95_seconds": 1} for n in range(25)}})
    h.report_from_group()
    waiting = h.bot.queue_waiting("local_report")
    h.bot.transition_reply(waiting["reply_id"], "sending")
    h.bot.transition_reply(waiting["reply_id"], "sent", confirmation_ref="msg_synthetic_waiting_reply")
    h.control.snapshot()
    runtime = h.cloud.bot_overview(h.source)["runtime"]
    assert runtime["active"]["model"] == "resolved-model" and runtime["active"]["persona_version"] == 2
    assert runtime["last_request"]["send_confirmation_seconds"] is None
    assert runtime["counts"] == {"requests": 1, "replied": 0, "pending": 1, "failed": 0, "unknown": 0}
    assert runtime["last_request"]["visible_reply_seconds"] == 4
    assert runtime["latency_metrics"]["generation"]["p95_seconds"] == 4
    assert PROVIDER_KEY not in json.dumps(runtime)


def test_delivered_failure_receipt_does_not_count_as_successful_model_reply(harness):
    h = harness
    h.control.poll()
    for event_id, outcome in (("ok", "sent"), ("model_failure", "sent"), ("send_failure", "failed"), ("uncertain", "unknown")):
        h.bot.begin_request({"event_id": event_id, "message_ref": "msg_" + event_id, "group_ref": h.group,
            "sender_id": "wxid_manager", "text": "在吗", "occurred_at": NOW.isoformat()})
        if event_id == "model_failure":
            waiting = h.bot.queue_waiting(event_id)
            h.bot.transition_reply(waiting["reply_id"], "sending")
            h.bot.transition_reply(waiting["reply_id"], "sent", confirmation_ref="msg_waiting")
        reply = h.bot.queue_reply(event_id, "失败回执" if event_id == "model_failure" else "测试回复")
        h.bot.transition_reply(reply["reply_id"], "sending")
        h.bot.transition_reply(reply["reply_id"], outcome, confirmation_ref="msg_delivered" if outcome == "sent" else None)
        if event_id == "model_failure":
            h.bot.set_request_status(event_id, "failed")
    h.bot.set("last_request", {"request_id": "model_failure", "status": "fallback_sent", "error": "siwc_network_error"})
    h.control.snapshot()
    runtime = h.cloud.bot_overview(h.source)["runtime"]
    assert runtime["counts"] == {"requests": 4, "replied": 1, "pending": 0, "failed": 2, "unknown": 1}
    assert runtime["last_request"]["status"] == "fallback_sent"
    assert runtime["last_request"]["error"] == "siwc_network_error"
    assert h.bot.reply("model_failure")["status"] == "sent"
