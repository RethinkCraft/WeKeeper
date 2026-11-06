"""Synthetic bot control-plane tests; no real archive, provider or WeChat."""
import json

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from chatlog_keeper.dashboard.api import create_app
from chatlog_keeper.dashboard.bot_control import BotConfig, BotSnapshot, BotStatusUpdate
from chatlog_keeper.dashboard.protocol import UsageRun, canonical_json, content_hash
from chatlog_keeper.dashboard.store import Conflict, DashboardStore
from chatlog_keeper.dashboard.usage import UsageLedger, normalize_usage

NOW = 1793073600
DAY = "2026-10-27"
TOKEN = "synthetic-machine-token-at-least-32-characters"


def partition(group="group_a", source="source_a"):
    data = {"group": {"group_ref": group, "name": group}, "day": DAY, "coverage": {}, "messages": [
        {"message_ref": "msg_" + group, "create_time": NOW, "sender_id": "sender_a", "sender_name": "报备员", "msg_type": 1, "text": "synthetic", "reply": None}]}
    return {"source_id": source, "group_ref": group, "day": DAY, "revision": 1, "initial": True, "content_hash": content_hash(data), "data": data}


def config():
    return BotConfig.model_validate({"enabled": False,
        "businesses": [{"id": "shop", "name": "店铺", "reporter_ids": ["sender_a"], "items": [{"id": "first", "name": "娱乐首单", "aliases": ["娱乐"]}]}],
        "personas": [{"id": "pet", "name": "小元", "identity": "虚构群宠"}],
        "providers": [{"id": "gpt", "name": "ChatGPT", "kind": "siwc", "model": ""}],
        "groups": [{"group_ref": group, "business_id": "shop", "persona_id": "pet", "provider_id": "gpt", "enabled": False} for group in ("group_a", "group_b")]}).model_dump(mode="json")


def status(revision=1, state="unavailable"):
    return {"business_id": "shop", "item_id": "first", "day": DAY, "status": state, "revision": revision, "updated_at": NOW,
            "actor": "sender_a", "group_ref": "group_a", "message_ref": "msg_group_a"}


def snapshot(**changes):
    return {"source_id": "source_a", "config_revision": 1, "statuses": [], "audit": [], **changes}


@pytest.fixture
def store(tmp_path):
    value = DashboardStore(tmp_path, clock=lambda: NOW)
    value.admin = value.create_user("admin", "long-admin-password")
    value.viewer = value.create_user("viewer", "long-viewer-password", "viewer")
    value.ingest_partition(partition())
    value.ingest_partition(partition("group_b"))
    value.ingest_partition(partition("group_other", "source_other"))
    yield value
    value.close()


def configure(store):
    command = store.save_bot_config({"source_id": "source_a", "expected_revision": 0, "data": config()}, store.admin["id"])["command"]
    store.acknowledge_bot_command({"source_id": "source_a", "command_id": command["id"], "status": "applied", "config_revision": 1})
    return command


def test_configuration_is_pending_until_ack_and_poll_replays_without_mutating(store):
    command = store.save_bot_config({"source_id": "source_a", "expected_revision": 0, "data": config()}, store.admin["id"])["command"]
    overview = store.bot_overview("source_a")
    assert overview["config"]["revision"] == 1 and overview["config"]["applied_revision"] == 0
    assert command["payload"]["config"]["businesses"][0]["reporter_ids"] == ["sender_a"]
    assert store.poll_bot_commands("source_a")["commands"] == store.poll_bot_commands("source_a")["commands"]
    assert store.poll_bot_commands("source_other")["commands"] == []
    with pytest.raises(Conflict, match="revision"):
        store.save_bot_config({"source_id": "source_a", "expected_revision": 0, "data": config()}, store.admin["id"])
    with pytest.raises(ValueError, match="revision"):
        store.acknowledge_bot_command({"source_id": "source_a", "command_id": command["id"], "status": "applied", "config_revision": 2})
    ack = {"source_id": "source_a", "command_id": command["id"], "status": "applied", "config_revision": 1}
    store.acknowledge_bot_command(ack)
    assert store.acknowledge_bot_command(ack)["accepted"]
    assert store.bot_overview("source_a")["config"]["applied_revision"] == 1
    assert store.poll_bot_commands("source_a")["commands"] == []


def test_default_character_is_shared_and_persona_content_auto_versions(store):
    from chatlog_keeper.bot.persona import default_persona
    initial = store.bot_overview("source_a")
    default = initial["config"]["data"]
    assert not default["enabled"] and default["groups"] == []
    assert default["providers"][0]["kind"] == "siwc"
    assert default["personas"][0]["markdown"] == default_persona()["markdown"]
    assert default["personas"][0]["markdown"].count("### 示例 ") == 3
    assert isinstance(default["personas"][0]["version"], int)
    configure(store)
    changed = config()
    changed["personas"][0]["markdown"] += "\n\n表达更简短。"
    result = store.save_bot_config({"source_id": "source_a", "expected_revision": 1, "data": changed}, store.admin["id"])
    assert result["command"]["payload"]["config"]["personas"][0]["version"] == 2
    assert store.bot_overview("source_a")["config"]["data"]["personas"][0]["version"] == 2
    # An unchanged client draft must not roll a version backwards.
    result = store.save_bot_config({"source_id": "source_a", "expected_revision": 2, "data": changed}, store.admin["id"])
    assert result["command"]["payload"]["config"]["personas"][0]["version"] == 2
    changed["personas"][0]["markdown"] += "\n\n## 示例\n用户：你好\n回复：嗨"
    result = store.save_bot_config({"source_id": "source_a", "expected_revision": 3, "data": changed}, store.admin["id"])
    assert result["command"]["payload"]["config"]["personas"][0]["version"] == 3


def test_existing_legacy_cloud_config_reads_canonical_and_next_save_removes_sections(store):
    configure(store)
    legacy = config()
    legacy["personas"] = [{"id": "pet", "name": "小元", "version": 1, "identity": "原身份",
                           "prompt": "保留附加设定", "examples": [{"user": "你好", "assistant": "嗨"}]}]
    store.conn.execute("UPDATE bot_configs SET data=? WHERE source_id='source_a'", (json.dumps(legacy),))
    canonical = store.bot_overview("source_a")["config"]["data"]
    assert set(canonical["personas"][0]) == {"id", "name", "version", "markdown"}
    assert "保留附加设定" in canonical["personas"][0]["markdown"]
    store.save_bot_config({"source_id": "source_a", "expected_revision": 1, "data": canonical}, store.admin["id"])
    persisted = json.loads(store.conn.execute("SELECT data FROM bot_configs WHERE source_id='source_a'").fetchone()[0])
    assert set(persisted["personas"][0]) == {"id", "name", "version", "markdown"}
    assert persisted["personas"][0]["version"] == 1


def test_status_adjustments_wait_for_machine_and_conflict_is_visible(store):
    configure(store)
    store.ingest_bot_snapshot(snapshot(statuses=[status()]))
    request = {"source_id": "source_a", "business_id": "shop", "item_id": "first", "day": DAY, "status": "available", "expected_revision": 1}
    command = store.adjust_bot_status(request, store.admin["id"])["command"]
    assert store.bot_overview("source_a")["statuses"][0]["status"] == "unavailable"
    # A group report won the race while this admin command was in transit.
    store.ingest_bot_snapshot(snapshot(statuses=[status(2, "unknown")]))
    store.acknowledge_bot_command({"source_id": "source_a", "command_id": command["id"], "status": "failed", "error": "bot_status_revision_conflict"})
    overview = store.bot_overview("source_a")
    assert overview["statuses"][0]["revision"] == 2
    assert next(c for c in overview["commands"] if c["id"] == command["id"])["status"] == "failed"
    with pytest.raises(Conflict, match="revision"):
        store.adjust_bot_status(request, store.admin["id"])


def test_snapshot_merges_revisions_and_audit_idempotently_across_shared_groups(store):
    configure(store)
    audit = {**status(), "id": "audit_1", "created_at": NOW, "previous_status": "unknown"}
    current = snapshot(statuses=[status(2, "available")], audit=[audit])
    store.ingest_bot_snapshot(current)
    store.ingest_bot_snapshot(current)
    store.ingest_bot_snapshot(snapshot(statuses=[status()]))
    overview = store.bot_overview("source_a")
    assert len(overview["statuses"]) == len(overview["audit"]) == 1
    assert overview["statuses"][0]["status"] == "available"
    assert len(overview["config"]["data"]["groups"]) == 2
    with pytest.raises(Conflict, match="snapshot"):
        store.ingest_bot_snapshot(snapshot(statuses=[status(2, "unknown")]))
    with pytest.raises(ValueError, match="source"):
        store.ingest_bot_snapshot(snapshot(statuses=[dict(status(3), group_ref="group_other")]))


def test_admin_revocation_cancels_pending_commands_and_bumps_refresh(store):
    second = store.create_user("admin2", "long-admin2-password", "admin")
    store.save_bot_config({"source_id": "source_a", "expected_revision": 0, "data": config()}, store.admin["id"])
    store.update_user(store.admin["id"], {"role": "viewer"})
    revision = store.revision()
    assert store.poll_bot_commands("source_a")["commands"] == []
    assert store.revision() > revision
    assert store.bot_overview("source_a")["commands"][0]["error"] == "admin_authorization_revoked"
    assert second["role"] == "admin"


def test_configuration_rejects_ambiguous_aliases_raw_identity_secret_and_cross_source(store):
    bad = config()
    bad["businesses"][0]["items"].append({"id": "other", "name": "其他首单", "aliases": ["娱乐"]})
    with pytest.raises(ValidationError, match="ambiguous"):
        BotConfig.model_validate(bad)
    bad = config(); bad["businesses"][0]["reporter_ids"] = ["wxid_private"]
    with pytest.raises(ValidationError, match="references"):
        BotConfig.model_validate(bad)
    bad = config(); bad["providers"][0]["api_key"] = "synthetic-secret"
    with pytest.raises(ValidationError):
        BotConfig.model_validate(bad)
    bad = config(); bad["groups"][0]["group_ref"] = "group_other"
    with pytest.raises(ValueError, match="source"):
        store.save_bot_config({"source_id": "source_a", "expected_revision": 0, "data": bad}, store.admin["id"])
    with pytest.raises(ValidationError, match="remain_local"):
        BotSnapshot.model_validate(snapshot(statuses=[dict(status(), actor="wxid_private")]))
    parsed = BotStatusUpdate.model_validate({"source_id": "source_a", "business_id": "shop", "item_id": "first", "day": DAY, "status": "unreported", "expected_revision": 0})
    assert parsed.status == "unknown"


def test_old_usage_payload_replays_after_upgrade_and_bot_usage_has_admin_scope(store):
    old = {"run_id": "old_summary", "kind": "summary", "user_id": None, "turn_id": None, "model": "gpt", "started_at": NOW - 10, "completed_at": NOW, "status": "completed", "usage": None}
    store.ingest_usage({"source_id": "source_a", "runs": [old]})
    # Simulate a database written by the version before provider metadata.
    store.conn.execute("UPDATE usage_runs SET payload=? WHERE run_id=?", (canonical_json(old), old["run_id"]))
    assert store.ingest_usage({"source_id": "source_a", "runs": [old]})["applied"] == 0
    assert store.ingest_usage({"source_id": "source_a", "runs": [UsageRun.model_validate(old).model_dump()]})["applied"] == 0
    bot = {**old, "run_id": "bot_1", "kind": "bot_chat", "provider": "deepseek", "profile_id": "deepseek_main", "group_ref": "group_a", "bot_request_id": "msg_group_a", "usage": {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7}}
    store.ingest_usage({"source_id": "source_a", "runs": [bot]})
    assert store.usage(store.viewer["id"], False, NOW - 100, NOW)["totals"]["requests"] == 0
    report = store.usage(store.admin["id"], True, NOW - 100, NOW)
    assert {r["provider"] for r in report["by_provider"]} == {"siwc", "deepseek"}
    assert report["totals"]["usage_unavailable_requests"] == 1
    with pytest.raises(ValueError, match="source"):
        store.ingest_usage({"source_id": "source_other", "runs": [dict(bot, run_id="wrong_source")]})
    with pytest.raises(ValidationError):
        UsageRun.model_validate(dict(bot, user_id=store.admin["id"]))


def test_local_bot_usage_roundtrip_and_chat_completions_accounting(tmp_path):
    ledger = UsageLedger(tmp_path)
    run = ledger.start_run(kind="bot_chat", model="model", provider="deepseek", profile_id="profile", group_ref="group_a", bot_request_id="request")
    raw = {"prompt_tokens": 20, "completion_tokens": 3, "total_tokens": 23, "prompt_cache_hit_tokens": 8}
    ledger.record_call(run, usage=raw)
    ledger.finish_run(run)
    result = ledger.pending()[0]
    assert result["kind"] == "bot_chat" and result["group_ref"] == "group_a"
    assert result["provider"] == "deepseek" and result["usage"]["cached_input_tokens"] == 8
    assert result["usage"]["total_tokens"] == 23
    assert normalize_usage({"prompt_tokens": 1})["total_tokens"] is None
    ledger.close()


def test_bot_http_routes_enforce_roles_machine_tokens_and_private_errors(tmp_path):
    app = create_app(tmp_path, TOKEN)
    app.state.store.create_user("admin", "long-admin-password")
    app.state.store.create_user("viewer", "long-viewer-password", "viewer")
    app.state.store.ingest_partition(partition())
    app.state.store.ingest_partition(partition("group_b"))
    with TestClient(app, base_url="https://testserver") as client:
        assert client.get("/api/bot").status_code == 401
        viewer = client.post("/api/auth/login", json={"username": "viewer", "password": "long-viewer-password"})
        client.headers["X-CSRF-Token"] = viewer.json()["csrf_token"]
        assert client.get("/api/bot").status_code == 403
        assert client.post("/api/machine/bot/poll", json={"source_id": "source_a"}).status_code == 401
        admin = client.post("/api/auth/login", json={"username": "admin", "password": "long-admin-password"})
        client.headers["X-CSRF-Token"] = admin.json()["csrf_token"]
        request = {"source_id": "source_a", "expected_revision": 0, "data": config()}
        request["data"]["personas"] = [{"id": "pet", "name": "小元", "version": 1, "identity": "虚构群宠",
            "prompt": "旧附加设定也要生效", "examples": [{"user": "你好", "assistant": "嗨"}]}]
        client.headers.pop("X-CSRF-Token")
        assert client.post("/api/bot/config", json=request).status_code == 403
        client.headers["X-CSRF-Token"] = admin.json()["csrf_token"]
        bad = json.loads(json.dumps(request)); bad["data"]["providers"][0]["api_key"] = "synthetic-secret"
        response = client.post("/api/bot/config", json=bad)
        assert response.status_code == 422 and "synthetic-secret" not in response.text
        saved = client.post("/api/bot/config", json=request)
        assert saved.status_code == 200
        command = saved.json()["command"]
        persona = command["payload"]["config"]["personas"][0]
        assert set(persona) == {"id", "name", "version", "markdown"}
        assert "旧附加设定也要生效" in persona["markdown"] and "回复：嗨" in persona["markdown"]
        machine = {"Authorization": "Bearer " + TOKEN}
        polled = client.post("/api/machine/bot/poll", json={"source_id": "source_a"}, headers=machine)
        assert polled.json()["commands"][0]["id"] == command["id"]
        ack = client.post("/api/machine/bot/ack", json={"source_id": "source_a", "command_id": command["id"], "status": "applied", "config_revision": 1}, headers=machine)
        assert ack.status_code == 200
        uploaded = client.post("/api/machine/bot/snapshot", json=snapshot(statuses=[status()]), headers=machine)
        assert uploaded.status_code == 200
        assert client.get("/api/bot").json()["statuses"][0]["status"] == "unavailable"
        assert client.get("/api/bot/members", params={"source_id": "source_a", "q": "报备"}).json()["members"][0]["sender_id"] == "sender_a"
