import json
import socket
import subprocess
from types import SimpleNamespace

import pytest

from chatlog_keeper.health_alerts import DesktopNotifier, HealthAlerts
from chatlog_keeper import health_alerts


class Store:
    def __init__(self):
        self.values = {}

    def get_meta(self, key, default=None):
        return json.loads(json.dumps(self.values.get(key, default)))

    def set_meta(self, key, value):
        self.values[key] = json.loads(json.dumps(value))


class Notifier:
    def __init__(self, results=()):
        self.calls = []
        self.results = list(results)

    def notify(self, **kwargs):
        self.calls.append(kwargs)
        return self.results.pop(0) if self.results else {"delivered": True, "notification_id": 42, "transport": "fixture"}


@pytest.fixture
def setup():
    store, notifier, now = Store(), Notifier(), [1000]
    alerts = HealthAlerts(store, notifier=notifier, clock=lambda: now[0])
    return store, notifier, now, alerts


def observe(alerts, now, at, state, **extras):
    now[0] = at
    return alerts.observe({"state": state, **extras})


def confirmed_outage(alerts, now, state="login_required"):
    observe(alerts, now, 1000, state)
    return observe(alerts, now, 1030, state)


def test_constructor_is_lazy_and_quiet_online_groups_never_alert(setup):
    store, notifier, now, alerts = setup
    assert not store.values and not notifier.calls
    for at in [1000, 1030, 100000]:
        result = observe(alerts, now, at, "session_present", last_message_time=0)
        assert result["status"] == "healthy"
    assert notifier.calls == []


def test_monitor_start_is_persisted_and_not_reset_by_collector_restart(setup):
    store, notifier, now, alerts = setup
    result = observe(alerts, now, 1000, "session_present")
    assert result["monitor_started_at"] == 1000
    restarted = HealthAlerts(store, notifier=notifier, clock=lambda: now[0])
    result = observe(restarted, now, 1900, "sync_unverified")
    assert result["monitor_started_at"] == 1000
    assert store.get_meta("client_health_alerts")["monitor_started_at"] == 1000
    assert not notifier.calls


def test_login_failure_debounces_and_stays_deduped_across_restart(setup):
    store, notifier, now, alerts = setup
    assert observe(alerts, now, 1000, "login_required")["status"] == "debouncing"
    observe(alerts, now, 1010, "login_required")
    assert notifier.calls == []
    result = observe(alerts, now, 1030, "login_required", account_id="private-account", text="private message")
    assert result["status"] == "notified" and result["outage_notified"]
    assert "重新登录" in notifier.calls[0]["title"]
    assert "private" not in json.dumps(notifier.calls)
    restarted = HealthAlerts(store, notifier=notifier, clock=lambda: now[0])
    observe(restarted, now, 1060, "login_required")
    observe(restarted, now, 2829, "login_required")
    assert len(notifier.calls) == 1
    observe(restarted, now, 2830, "login_required")
    assert len(notifier.calls) == 2
    assert notifier.calls[1]["replace_id"] == 42


def test_unknown_does_not_reset_existing_outage_or_cause_false_recovery(setup):
    _, notifier, now, alerts = setup
    confirmed_outage(alerts, now)
    observe(alerts, now, 1060, "receiving")
    uncertain = observe(alerts, now, 1090, "unknown")
    assert uncertain["active_issue"] == "login_required"
    observe(alerts, now, 2000, "unknown")
    assert len(notifier.calls) == 1
    observe(alerts, now, 2030, "receiving")
    assert len(notifier.calls) == 1
    result = observe(alerts, now, 2060, "receiving")
    assert result["active_issue"] is None
    assert result["last_delivered_kind"] == "recovery"
    assert len(notifier.calls) == 2 and "收取已恢复" in notifier.calls[1]["title"]
    observe(alerts, now, 2090, "receiving")
    assert len(notifier.calls) == 2


def test_session_window_recovery_does_not_claim_receiving_messages(setup):
    _, notifier, now, alerts = setup
    confirmed_outage(alerts, now)
    observe(alerts, now, 1060, "session_present")
    observe(alerts, now, 1090, "session_present")
    recovery = notifier.calls[-1]
    assert recovery["urgent"] is False
    assert recovery["title"] == "微信会话界面已恢复"
    assert "不代表已经收到新消息" in recovery["body"]


def test_failed_delivery_is_not_counted_as_delivered_and_retries_with_backoff(setup):
    store, _, now, _ = setup
    notifier = Notifier([{"delivered": False, "error": "desktop_notification_timeout"}, {"delivered": True}])
    alerts = HealthAlerts(store, notifier=notifier, clock=lambda: now[0])
    result = confirmed_outage(alerts, now)
    assert result["status"] == "delivery_failed"
    assert result["last_delivered_at"] is None and not result["outage_notified"]
    assert result["last_error"] == "desktop_notification_timeout"
    assert result["next_attempt_at"] == 1090
    restarted = HealthAlerts(store, notifier=notifier, clock=lambda: now[0])
    assert observe(restarted, now, 1060, "login_required")["status"] == "retry_wait"
    assert len(notifier.calls) == 1
    result = observe(restarted, now, 1090, "login_required")
    assert result["status"] == "notified" and result["last_error"] is None
    assert len(notifier.calls) == 2


def test_changed_issue_failed_notification_is_still_retried(setup):
    store, _, now, _ = setup
    notifier = Notifier([True, False, True])
    alerts = HealthAlerts(store, notifier=notifier, clock=lambda: now[0])
    confirmed_outage(alerts, now)
    observe(alerts, now, 1060, "source_unavailable")
    assert observe(alerts, now, 1090, "source_unavailable")["status"] == "delivery_failed"
    assert observe(alerts, now, 1120, "source_unavailable")["status"] == "retry_wait"
    assert observe(alerts, now, 1150, "source_unavailable")["status"] == "notified"
    assert len(notifier.calls) == 3


@pytest.mark.parametrize("state", ["unknown", "sync_unverified"])
def test_uncertain_health_waits_five_minutes_and_never_labels_logout(setup, state):
    _, notifier, now, alerts = setup
    observe(alerts, now, 1000, state)
    observe(alerts, now, 1030, state)
    observe(alerts, now, 1299, state)
    assert not notifier.calls
    result = observe(alerts, now, 1300, state)
    assert result["status"] == "notified"
    assert "已退出登录" not in notifier.calls[0]["title"]


@pytest.mark.parametrize("code", ["not_running", "network_unavailable", "source_unavailable", "collector_error"])
def test_effective_failure_wins_over_nested_visible_session(setup, code):
    _, notifier, now, alerts = setup
    observe(alerts, now, 1000, code, client={"state": "session_present"})
    result = observe(alerts, now, 1030, code, client={"state": "session_present"})
    assert result["active_issue"] == code and len(notifier.calls) == 1
    assert notifier.calls[0]["title"] != "微信需要重新登录"


def test_brief_failure_without_a_delivered_warning_does_not_send_recovery(setup):
    _, notifier, now, alerts = setup
    observe(alerts, now, 1000, "login_required")
    observe(alerts, now, 1030, "session_present")
    assert len(notifier.calls) == 0
    assert observe(alerts, now, 1060, "session_present")["status"] == "healthy"


def test_notifier_exception_and_private_errors_are_sanitized(setup):
    store, _, now, _ = setup
    def fail(**_kwargs):
        raise RuntimeError("secret-account-and-bus-path")
    alerts = HealthAlerts(store, notifier=fail, clock=lambda: now[0])
    result = confirmed_outage(alerts, now)
    assert result["last_error"] == "desktop_notification_failed"
    assert "secret" not in json.dumps(store.values)


def test_alert_persistence_uses_real_archive_meta(tmp_path):
    from chatlog_keeper.archive import ArchiveStore
    now, notifier = [1000], Notifier()
    with ArchiveStore(tmp_path) as store:
        alerts = HealthAlerts(store, notifier=notifier, clock=lambda: now[0])
        confirmed_outage(alerts, now)
    with ArchiveStore(tmp_path) as reopened:
        restarted = HealthAlerts(reopened, notifier=notifier, clock=lambda: now[0])
        result = observe(restarted, now, 1060, "login_required")
        assert result["status"] == "notified" and len(notifier.calls) == 1


def test_desktop_notification_is_one_local_argv_call_and_bounded():
    calls = []
    environment = {"DBUS_SESSION_BUS_ADDRESS": "unix:path=/fixture/bus", "PATH": "/usr/bin:/bin"}
    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout="(uint32 1234,)")
    notifier = DesktopNotifier(run=run, which=lambda name: "/usr/bin/gdbus" if name == "gdbus" else None,
                               environment=lambda: environment)
    result = notifier.notify(title="微信需要重新登录", body="安全的本机提醒", replace_id=12)
    assert result == {"delivered": True, "transport": "gdbus", "notification_id": 1234}
    command, kwargs = calls[0]
    assert command[0] == "/usr/bin/gdbus" and "org.freedesktop.Notifications.Notify" in command
    assert "12" in command and command[-1] == "0"
    assert kwargs["timeout"] == 3 and kwargs["env"] == environment
    assert "shell" not in kwargs


def test_notify_send_fallback_is_only_used_when_gdbus_missing():
    calls = []
    notifier = DesktopNotifier(run=lambda command, **kw: calls.append(command) or SimpleNamespace(returncode=0, stdout=""),
                               which=lambda name: "/usr/bin/notify-send" if name == "notify-send" else None,
                               environment=lambda: {"DBUS_SESSION_BUS_ADDRESS": "unix:path=/fixture"})
    result = notifier.notify(title="--literal-title", body="local only", urgent=False)
    assert result["delivered"] and result["transport"] == "notify-send"
    assert calls[0][-3:] == ["--", "--literal-title", "local only"]


def test_timeout_does_not_fall_back_and_risk_duplicate_delivery():
    calls = []
    def timeout(command, **kwargs):
        calls.append(command)
        raise subprocess.TimeoutExpired(command, 3)
    notifier = DesktopNotifier(run=timeout, which=lambda name: "/usr/bin/" + name,
                               environment=lambda: {"DBUS_SESSION_BUS_ADDRESS": "unix:path=/fixture"})
    assert notifier.notify(title="test", body="test")["error"] == "desktop_notification_timeout"
    assert len(calls) == 1


def test_missing_local_bus_or_commands_is_reported_without_sending():
    def must_not_run(*_args, **_kwargs):
        pytest.fail("No notification should have been attempted")
    absent_bus = DesktopNotifier(run=must_not_run, environment=lambda: None)
    assert absent_bus.notify(title="test", body="test")["error"] == "desktop_session_bus_unavailable"
    absent_command = DesktopNotifier(run=must_not_run, which=lambda _name: None, environment=lambda: {"DBUS_SESSION_BUS_ADDRESS": "unix:path=/fixture"})
    assert absent_command.notify(title="test", body="test")["error"] == "desktop_notification_command_missing"


def test_bus_resolution_rejects_remote_and_multitransport_addresses(tmp_path):
    path = tmp_path / "bus"
    server = socket.socket(socket.AF_UNIX)
    try:
        server.bind(str(path))
        local = "unix:path=" + str(path)
        assert health_alerts._owned_bus(local) == local
        assert health_alerts._owned_bus("tcp:host=example.com,port=1234") is None
        assert health_alerts._owned_bus(local + ";tcp:host=example.com") is None
        assert health_alerts._owned_bus("unix:abstract=private") is None
        assert health_alerts._owned_bus("unix:abstract=private", allow_abstract=True)
    finally:
        server.close()


def test_notification_environment_whitelists_without_copying_credentials(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "private-api-key")
    monkeypatch.setenv("LD_PRELOAD", "/private/injection.so")
    monkeypatch.setenv("DBUS_SESSION_BUS_ADDRESS", "tcp:host=outside.example")
    monkeypatch.setattr(health_alerts, "_owned_bus", lambda address, **_kw: address if address and address.startswith("unix:path=/run/user/") else None)
    environment = health_alerts.notification_environment()
    assert environment["DBUS_SESSION_BUS_ADDRESS"].startswith("unix:path=/run/user/")
    assert "OPENAI_API_KEY" not in environment and "LD_PRELOAD" not in environment
