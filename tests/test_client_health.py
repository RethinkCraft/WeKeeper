import json
import subprocess

import pytest

from chatlog_keeper import client_health as health


def window(width=880, height=640, **extra):
    return {"width": width, "height": height, "title": "Weixin",
            "classes": ["wechat", "wechat"], "map_state": 2,
            "transient_for": None, "normal": True, **extra}


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(health, "_discover_processes", lambda: (True, [
        {"pid": 123, "executable": "/verified/wechat", "official_path": True}]))
    monkeypatch.setattr(health, "_verified_build", lambda _path: True)
    monkeypatch.setattr(health, "_account_binding", lambda _account: "single_account_scope")
    monkeypatch.setattr(health, "_network_state", lambda: "available")
    monkeypatch.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True, "windows": [window()]})
    return monkeypatch


def test_open_session_does_not_claim_network_online(client):
    value = health.probe_client("selected-account")
    assert value["state"] == "session_present"
    assert value["process_running"] is True
    assert value["evidence"]["server_connectivity"] == "not_probed"
    assert "selected-account" not in json.dumps(value)


def test_real_login_layout_is_login_required_even_process_and_archive_alive(client):
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True,
        "windows": [window(280, 380), window(330, 219, title="wechat", transient_for=1)]})
    result = health.probe_client("selected-account")
    assert result["state"] == "login_required"
    assert result["process_running"] is True


def test_visible_login_wins_over_stale_hidden_session(client):
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True, "windows": [window(map_state=0), window(280, 380)]})
    assert health.probe_client()["state"] == "login_required"


@pytest.mark.parametrize("width,height", [(420, 570), (560, 760), (840, 1140), (1120, 1520), (562, 760)])
def test_scaled_login_shape_cannot_be_mistaken_for_main_session(client, width, height):
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True, "windows": [window(width, height)]})
    assert health.probe_client()["state"] == "login_required"


def test_unrecognized_portrait_window_is_unknown_instead_of_session(client):
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True, "windows": [window(800, 1000)]})
    assert health.probe_client()["state"] == "unknown"


@pytest.mark.parametrize("candidate", [
    window(280, 380, title="Other app"),
    window(280, 380, classes=["unrelated"]),
    window(280, 380, map_state=0),
    window(330, 219),
    window(transient_for=123),
    window(title="Image preview"),
    window(map_state=0),
])
def test_unrecognized_or_hidden_windows_do_not_claim_logged_in_or_out(client, candidate):
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True, "windows": [candidate]})
    assert health.probe_client()["state"] == "unknown"


def test_no_process_only_means_stopped_when_enumeration_complete(client):
    client.setattr(health, "_discover_processes", lambda: (True, []))
    assert health.probe_client()["state"] == "not_running"
    client.setattr(health, "_discover_processes", lambda: (False, []))
    value = health.probe_client()
    assert value["state"] == "unknown"
    assert value["process_running"] is None


def test_unavailable_display_does_not_mean_client_stopped(client):
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": False, "reason": "display_access_unavailable"})
    value = health.probe_client()
    assert value["state"] == "unknown"
    assert value["process_running"] is True


def test_multiple_clients_or_account_binding_cannot_claim_selected_session(client):
    client.setattr(health, "_account_binding", lambda _account: "ambiguous")
    assert health.probe_client("selected")["state"] == "unknown"
    client.setattr(health, "_discover_processes", lambda: (True, [
        {"pid": 1, "executable": "one", "official_path": True},
        {"pid": 2, "executable": "two", "official_path": True}]))
    assert health.probe_client()["reason"] == "multiple_client_processes"


def test_client_upgrade_does_not_reuse_known_geometry(client):
    client.setattr(health, "_verified_build", lambda _path: False)
    assert health.probe_client()["reason"] == "unverified_client_build"


def test_inspection_exceptions_are_unknown_and_do_not_leak_details(client):
    def denied():
        raise PermissionError("private/path/or/account")
    client.setattr(health, "_discover_processes", denied)
    value = health.probe_client()
    assert value["state"] == "unknown"
    assert "private" not in json.dumps(value)


def test_helper_timeout_and_malformed_output_are_safe_unknown(monkeypatch):
    def timeout(*_args, **_kwargs):
        raise subprocess.TimeoutExpired("probe", 4)
    monkeypatch.setattr(health.subprocess, "run", timeout)
    assert health._inspect_windows(123)["reason"] == "display_inspection_timeout"
    monkeypatch.setattr(health.subprocess, "run", lambda *_a, **_k: type("R", (), {
        "returncode": 0, "stdout": "private unparseable helper output"})())
    assert health._inspect_windows(123)["reason"] == "display_inspection_failed"


def test_probe_has_no_message_inactivity_input_or_active_network_probe(client):
    # A quiet conversation and a busy one have identical client-session evidence.
    assert health.probe_client()["state"] == health.probe_client()["state"] == "session_present"


def test_verified_iconic_main_window_is_minimized_session_but_tray_hidden_is_unknown(client):
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True,
        "windows": [window(map_state=0, wm_state=3, hidden=True)]})
    value = health.probe_client()
    assert value["state"] == "session_present"
    assert value["reason"] == "minimized_session_window"
    assert value["evidence"]["session_window_minimized"] is True
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True,
        "windows": [window(map_state=0, wm_state=1, hidden=True)]})
    assert health.probe_client()["state"] == "unknown"


def test_definite_network_down_is_distinct_and_login_prompt_has_priority(client):
    client.setattr(health, "_network_state", lambda: "unavailable")
    value = health.probe_client()
    assert value["state"] == "network_unavailable"
    assert value["network_state"] == "unavailable"
    client.setattr(health, "_inspect_windows", lambda _pid: {
        "display_available": True, "windows": [window(280, 380)]})
    assert health.probe_client()["state"] == "login_required"


@pytest.mark.parametrize("text,expected", [
    ("connected\n", "available"), ("connected (externally)\n", "available"),
    ("disconnected\n", "unavailable"), ("asleep\n", "unavailable"),
    ("connecting\n", "unknown"), ("unknown\n", "unknown"),
])
def test_network_reads_only_cached_state(monkeypatch, text, expected):
    calls = []
    def run(args, **kwargs):
        calls.append((args, kwargs))
        return type("R", (), {"returncode": 0, "stdout": text})()
    monkeypatch.setattr(health.subprocess, "run", run)
    assert health._network_state() == expected
    assert calls[0][0] == ["nmcli", "-t", "-f", "STATE", "general"]
    assert "connectivity" not in calls[0][0]


def test_network_unavailable_command_is_unknown(monkeypatch):
    def missing(*_a, **_kw):raise FileNotFoundError("private details")
    monkeypatch.setattr(health.subprocess, "run", missing)
    assert health._network_state() == "unknown"


def test_x11_helper_rejects_remote_display_before_loading_x11(tmp_path, monkeypatch):
    class FakePath:
        def open(self, *_args):
            import io
            return io.BytesIO(b"DISPLAY=remote.example:0\0XAUTHORITY=/private/path\0")
    monkeypatch.setattr(health, "Path", lambda *_args: FakePath())
    monkeypatch.setattr(health.ctypes.util, "find_library", lambda *_: pytest.fail("Remote display was opened"))
    assert health._x11_probe(1)["reason"] == "local_client_display_unavailable"
