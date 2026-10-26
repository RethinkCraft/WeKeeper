"""Reception health must not turn green just because an offline archive opens."""
import asyncio

import pytest

from chatlog_keeper.archive import ArchiveError, ArchiveStore
from test_archive import commit, raw, setup_source


@pytest.fixture
def observed(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr("chatlog_keeper.archive.time.time", lambda: clock[0])
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store, maximum=1)
        commit(store, source, [raw(timestamp=990)])
        store.set_collector_status(running=True, last_cycle_finished_at=1000,
                                   last_error=None, poll_interval=30)
        yield store, clock, tmp_path


def probe(state, **extra):
    return {"state": state, "checked_at": 1000, "reason": "synthetic", **extra}


@pytest.mark.parametrize("state", ["login_required", "not_running", "network_unavailable"])
def test_definite_client_failure_wins_over_healthy_collector_and_recent_messages(observed, state):
    store, _, _ = observed
    store.set_collector_status(client_health=probe(state))
    status = store.get_status()
    assert status["collector"]["running"] is True
    assert status["collector_stale"] is False
    assert status["health"]["collector_healthy"] is True
    assert status["health"]["state"] == state
    assert not status["health"]["message_reception_confirmed"]
    assert status["health"]["needs_attention"]
    assert store.list_groups()["coverage"]["health"]["state"] == state


def test_quiet_group_is_not_reported_offline_or_confirmed_receiving(observed):
    store, clock, _ = observed
    clock[0] = 1400
    store.set_collector_status(last_cycle_finished_at=1400)
    health = store.get_live_health(probe("session_present", checked_at=1400))
    assert health["state"] == "session_present"
    assert not health["message_reception_confirmed"]
    assert not health["needs_attention"]


def test_prolonged_silence_requests_sync_verification_without_claiming_logout(observed):
    store, clock, _ = observed
    clock[0] = 2000
    store.set_collector_status(last_cycle_finished_at=2000)
    health = store.get_live_health(probe("session_present", checked_at=2000))
    assert health["state"] == "sync_unverified"
    assert health["client"]["state"] == "session_present"
    assert health["collector_healthy"]
    assert health["needs_attention"]
    assert not health["message_reception_confirmed"]


def test_empty_archive_uses_persisted_monitor_start_for_sync_verification(observed):
    _, clock, path = observed
    clock[0] = 2000
    with ArchiveStore(path / "empty") as store:
        store.set_meta("client_health_alerts", {"monitor_started_at": 1000})
        store.set_collector_status(running=True, last_cycle_finished_at=2000)
        health = store.get_live_health(probe("session_present", checked_at=2000))
        assert health["state"] == "sync_unverified"


def test_fresh_reception_can_confirm_sync_without_a_visible_main_window(observed):
    store, _, _ = observed
    health = store.get_live_health(probe("unknown", reason="no_verified_visible_session_window",
        process_running=True, evidence={"verified_build": True, "account_binding": "single_account_scope"}))
    assert health["state"] == "receiving"
    assert health["last_recent_message_received_at"] == 1000


@pytest.mark.parametrize("reason", ["client_probe_failed", "selected_account_unverified",
                                   "unverified_client_build", "process_inspection_incomplete"])
def test_probe_identity_failures_are_not_hidden_by_old_recent_messages(observed, reason):
    store, _, _ = observed
    assert store.get_live_health(probe("unknown", reason=reason))["state"] == "unknown"


def test_old_receipt_does_not_mask_an_unobservable_client(observed):
    store, clock, _ = observed
    clock[0] = 1100
    store.set_collector_status(last_cycle_finished_at=1100)
    health = store.get_live_health(probe("unknown", checked_at=1100,
        reason="no_verified_visible_session_window", process_running=True,
        evidence={"verified_build": True, "account_binding": "single_account_scope"}))
    assert health["last_recent_message_received_at"] is not None
    assert health["state"] == "unknown"


def test_stale_monitor_and_monitor_failure_are_not_masked_by_recent_data(observed):
    store, _, _ = observed
    assert store.get_live_health(probe("session_present", checked_at=1))["state"] == "unknown"
    store.set_collector_status(health_monitor_error="TimeoutError")
    assert store.get_live_health(probe("session_present"))["state"] == "unknown"


@pytest.mark.parametrize("fields", [
    {"running": False}, {"last_cycle_finished_at": 1},
])
def test_dead_or_stale_collector_is_not_a_healthy_live_feed(observed, fields):
    store, _, _ = observed
    store.set_collector_status(**fields)
    health = store.get_live_health(probe("session_present"))
    assert health["state"] == "collector_error"
    assert not health["collector_healthy"]


def test_source_failure_is_separate_from_client_login(observed):
    store, _, _ = observed
    store.set_collector_status(last_error="cached_database_key_unavailable")
    health = store.get_live_health(probe("session_present"))
    assert health["state"] == "source_unavailable"
    assert health["client"]["state"] == "session_present"


def test_monitor_heartbeat_does_not_turn_collection_failure_into_success(observed):
    store, clock, _ = observed
    clock[0] = 3000
    store.set_collector_status(last_cycle_checked_at=3000, last_error="source_missing")
    status = store.get_status()
    assert status["collector_stale"] is False
    assert status["collector"]["last_cycle_finished_at"] == 1000
    assert not status["health"]["collector_healthy"]


def test_state_and_notification_dedupe_meta_survive_restart_without_readonly_writes(observed):
    store, _, path = observed
    saved = {"incident": "login_required", "delivered_at": 1000}
    store.set_meta("client_health_alerts", saved)
    with ArchiveStore(path, readonly=True) as reader:
        assert reader.get_meta("client_health_alerts") == saved
        assert reader.get_meta("missing", {}) == {}
        with pytest.raises(ArchiveError, match="read_only_archive"):
            reader.set_meta("client_health_alerts", {})


def test_mcp_exposes_logout_even_when_cached_messages_remain_readable(observed):
    pytest.importorskip("mcp")
    from chatlog_keeper.mcp_server import create_server
    store, _, path = observed
    store.set_collector_status(client_health=probe("login_required"))
    result = asyncio.run(create_server(path).call_tool("get_status", {}))
    assert result.structuredContent["messages"] == 1
    assert result.structuredContent["collector"]["running"]
    assert result.structuredContent["health"]["state"] == "login_required"
