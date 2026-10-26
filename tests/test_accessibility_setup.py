"""Portable setup is exercised with fake settings and processes, never WeChat."""
import importlib.util
import json
import os
from pathlib import Path
import signal
from types import SimpleNamespace

import pytest

spec = importlib.util.spec_from_file_location(
    "accessibility_setup", Path(__file__).resolve().parents[1] / "scripts/restart_wechat_accessibility.py")
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)


@pytest.fixture
def fake_desktop(tmp_path, monkeypatch):
    process = {"pid": 101, "start_ticks": "1001", "executable": "/opt/wechat/wechat",
               "sha256": setup.client_health._VERIFIED_BUILD}
    flags = {"toolkit_accessibility": False, "bus_enabled": False, "screen_reader_enabled": None}
    environment = {"DISPLAY": ":7", "PATH": "/usr/bin", "QT_QPA_PLATFORM": "wayland"}
    calls = {"writes": [], "exit": [], "launch": []}
    monkeypatch.setattr(setup, "_verified_process", lambda: process.copy())
    monkeypatch.setattr(setup, "_process_environment", lambda _: environment.copy())
    monkeypatch.setattr(setup, "_read_flags", lambda _: flags.copy())
    monkeypatch.setattr(setup, "_write_flag", lambda key, value, env: calls["writes"].append((key, value)))
    monkeypatch.setattr(setup, "_graceful_exit", lambda p: calls["exit"].append(p.copy()))
    monkeypatch.setattr(setup, "_start_ticks", lambda pid: "2002")

    def launch(executable, env, log):
        calls["launch"].append((executable, env.copy(), log))
        return 202

    monkeypatch.setattr(setup, "_launch", launch)
    # Any accidental direct desktop, process kill or real child is a failure.
    monkeypatch.setattr(setup.subprocess, "run", lambda *_a, **_k: pytest.fail("real settings command forbidden"))
    monkeypatch.setattr(setup.subprocess, "Popen", lambda *_a, **_k: pytest.fail("real client launch forbidden"))
    monkeypatch.setattr(setup.os, "kill", lambda *_a: pytest.fail("real process kill forbidden"))
    return SimpleNamespace(state=tmp_path / "state", process=process, flags=flags,
                           environment=environment, calls=calls)


def test_default_prepare_only_reads_and_writes_private_self_contained_manifest(fake_desktop, capsys):
    desktop = fake_desktop
    assert setup.main(["--state-dir", str(desktop.state)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["prepared"] and output["client_unchanged"]
    manifest = desktop.state / "setup/accessibility.json"
    proof = json.loads(manifest.read_text())
    assert proof["expected_process"] == desktop.process
    assert proof["original_flags"] == desktop.flags
    assert proof["original_qt_environment"] == {"QT_LINUX_ACCESSIBILITY_ALWAYS_ON": None, "QT_QPA_PLATFORM": "wayland"}
    assert "DISPLAY" not in proof and ":7" not in manifest.read_text()
    assert manifest.stat().st_mode & 0o777 == 0o600
    assert manifest.parent.stat().st_mode & 0o777 == 0o700
    assert desktop.state.stat().st_mode & 0o777 == 0o700
    assert desktop.calls == {"writes": [], "exit": [], "launch": []}


def test_explicit_prepare_matches_default(fake_desktop):
    assert setup.main(["--state-dir", str(fake_desktop.state), "--prepare"]) == 0
    assert fake_desktop.calls["exit"] == []


def test_state_environment_default_is_used_without_machine_path(fake_desktop, monkeypatch):
    monkeypatch.setattr(setup, "data_dir", lambda: fake_desktop.state)
    assert setup.main([]) == 0
    assert (fake_desktop.state / "setup/accessibility.json").is_file()


def test_restart_requires_prepared_manifest_and_does_not_quit(fake_desktop):
    with pytest.raises(setup.SetupError, match="prepared_manifest_required"):
        setup.restart(fake_desktop.state)
    assert fake_desktop.calls["exit"] == []


@pytest.mark.parametrize("field,value", [("pid", 303), ("start_ticks", "9999"),
                                          ("executable", "/opt/other/wechat"),
                                          ("sha256", "a" * 64)])
def test_prepared_process_identity_changes_refuse_before_quit(fake_desktop, field, value):
    setup.prepare(fake_desktop.state)
    fake_desktop.process[field] = value
    with pytest.raises(setup.SetupError, match="prepared_client_changed"):
        setup.restart(fake_desktop.state)
    assert fake_desktop.calls["exit"] == []


def test_enable_restart_only_changes_known_settings_and_retains_original_baseline(fake_desktop):
    desktop = fake_desktop
    setup.prepare(desktop.state)
    result = setup.restart(desktop.state)
    assert result["restarted_pid"] == 202 and result["accessibility_environment"]
    assert not result["automatic_login"] and not result["messages_or_files_sent"]
    assert desktop.calls["writes"] == [("toolkit_accessibility", True), ("bus_enabled", True)]
    assert len(desktop.calls["exit"]) == 1
    _, environment, _ = desktop.calls["launch"][0]
    assert environment["QT_LINUX_ACCESSIBILITY_ALWAYS_ON"] == "1"
    assert environment["QT_QPA_PLATFORM"] == "xcb"
    proof = setup._load(desktop.state / "setup/accessibility.json")
    assert proof["state"] == "enabled" and proof["expected_process"]["pid"] == 202
    assert proof["original_flags"] == desktop.flags
    assert proof["original_qt_environment"]["QT_QPA_PLATFORM"] == "wayland"


def test_repeated_prepare_does_not_overwrite_enabled_rollback(fake_desktop):
    setup.prepare(fake_desktop.state)
    setup.restart(fake_desktop.state)
    fake_desktop.process.update(pid=202, start_ticks="2002")
    before = (fake_desktop.state / "setup/accessibility.json").read_bytes()
    result = setup.prepare(fake_desktop.state)
    assert result["reused"]
    assert (fake_desktop.state / "setup/accessibility.json").read_bytes() == before


def test_enabled_manifest_with_different_process_requires_review(fake_desktop):
    setup.prepare(fake_desktop.state)
    setup.restart(fake_desktop.state)
    fake_desktop.process.update(pid=999, start_ticks="9999")
    with pytest.raises(setup.SetupError, match="previous_setup_requires_review"):
        setup.prepare(fake_desktop.state)


def test_rollback_restores_recorded_qt_values_and_known_original_flags(fake_desktop):
    desktop = fake_desktop
    setup.prepare(desktop.state)
    setup.restart(desktop.state)
    desktop.process.update(pid=202, start_ticks="2002")
    desktop.environment.update(QT_LINUX_ACCESSIBILITY_ALWAYS_ON="1", QT_QPA_PLATFORM="xcb")
    desktop.flags.update(toolkit_accessibility=True, bus_enabled=True)
    desktop.calls["writes"].clear()
    result = setup.restart(desktop.state, without_accessibility=True)
    assert not result["accessibility_environment"]
    assert desktop.calls["writes"] == [("toolkit_accessibility", False), ("bus_enabled", False)]
    _, environment, _ = desktop.calls["launch"][-1]
    assert "QT_LINUX_ACCESSIBILITY_ALWAYS_ON" not in environment
    assert environment["QT_QPA_PLATFORM"] == "wayland"
    assert setup._load(desktop.state / "setup/accessibility.json")["state"] == "rolled_back"


def test_graceful_exit_failure_never_changes_settings_or_launches(fake_desktop, monkeypatch):
    setup.prepare(fake_desktop.state)
    monkeypatch.setattr(setup, "_graceful_exit", lambda _: (_ for _ in ()).throw(setup.SetupError("client_did_not_exit_gracefully")))
    with pytest.raises(setup.SetupError, match="client_did_not_exit_gracefully"):
        setup.restart(fake_desktop.state)
    assert fake_desktop.calls["writes"] == []
    assert fake_desktop.calls["launch"] == []


def test_settings_failure_rolls_back_changed_flags_and_relaunches_original_environment(fake_desktop, monkeypatch):
    desktop = fake_desktop
    setup.prepare(desktop.state)

    def write(key, value, _env):
        desktop.calls["writes"].append((key, value))
        if key == "bus_enabled":
            raise setup.SetupError("accessibility_setting_write_failed")

    monkeypatch.setattr(setup, "_write_flag", write)
    with pytest.raises(setup.SetupError, match="accessibility_setting_write_failed"):
        setup.restart(desktop.state)
    assert desktop.calls["writes"] == [("toolkit_accessibility", True), ("bus_enabled", True), ("toolkit_accessibility", False)]
    assert desktop.calls["launch"][0][1] == desktop.environment
    assert setup._load(desktop.state / "setup/accessibility.json")["state"] == "recovered_after_error"


@pytest.mark.parametrize("target", ["state", "setup", "manifest", "log"])
def test_symlink_state_or_setup_files_refuse_before_process_actions(tmp_path, fake_desktop, target):
    desktop = fake_desktop
    outside = tmp_path / "outside"
    outside.mkdir()
    if target == "state":
        desktop.state.symlink_to(outside, target_is_directory=True)
    else:
        desktop.state.mkdir()
        if target == "setup":
            (desktop.state / "setup").symlink_to(outside, target_is_directory=True)
        else:
            (desktop.state / "setup").mkdir()
            destination = outside / "victim"
            destination.write_text("untouched")
            filename = "accessibility.json" if target == "manifest" else "accessibility-client.log"
            (desktop.state / "setup" / filename).symlink_to(destination)
    with pytest.raises(setup.SetupError, match="unsafe_setup"):
        setup.prepare(desktop.state)
    assert desktop.calls["exit"] == []


def test_invalid_manifest_cannot_reach_restart(fake_desktop):
    manifest, _ = setup._paths(fake_desktop.state)
    with setup.private_text_writer(manifest, secure_parent=True) as handle:
        handle.write('{"schema":1,"expected_process":{},"original_flags":{},"original_qt_environment":{}}')
    with pytest.raises(setup.SetupError, match="invalid_prepared_manifest"):
        setup.restart(fake_desktop.state)
    assert fake_desktop.calls["exit"] == []


def test_flag_read_unavailable_is_explicit_and_read_commands_only(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append(tuple(command))
        assert kwargs["timeout"] == 3 and kwargs["stdin"] is setup.subprocess.DEVNULL
        if command[0] == "gsettings":
            return SimpleNamespace(returncode=0, stdout=b"false\n")
        return SimpleNamespace(returncode=1, stdout=b"private error")

    monkeypatch.setattr(setup.subprocess, "run", run)
    assert setup._read_flags({}) == {"toolkit_accessibility": False, "bus_enabled": None, "screen_reader_enabled": None}
    assert calls == list(setup.FLAGS.values())


def test_setting_writes_use_exact_allowlist_and_boolean_variants(monkeypatch):
    calls = []
    monkeypatch.setattr(setup.subprocess, "run", lambda cmd, **kwargs: (calls.append(cmd), SimpleNamespace(returncode=0))[1])
    setup._write_flag("toolkit_accessibility", False, {})
    setup._write_flag("bus_enabled", True, {})
    assert calls[0] == ["gsettings", "set", "org.gnome.desktop.interface", "toolkit-accessibility", "false"]
    assert calls[1][-3:] == ["org.a11y.Status", "IsEnabled", "<true>"]
    assert "org.freedesktop.DBus.Properties.Set" in calls[1]
    with pytest.raises(setup.SetupError, match="invalid_accessibility_setting"):
        setup._write_flag("arbitrary", True, {})


def test_graceful_exit_sends_only_sigterm_and_never_force_kills(fake_desktop, monkeypatch):
    # Restore this one implementation; its dependencies remain fake.
    spec2 = importlib.util.spec_from_file_location("exit_setup", Path(setup.__file__))
    real = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(real)
    monkeypatch.setattr(real, "_verified_process", lambda: fake_desktop.process.copy())
    calls = []
    monkeypatch.setattr(real.os, "kill", lambda pid, sig: calls.append((pid, sig)))
    monkeypatch.setattr(real.time, "monotonic", iter([0, 1, 16]).__next__)
    monkeypatch.setattr(real.time, "sleep", lambda _: None)
    monkeypatch.setattr(real.client_health, "_discover_processes", lambda: (True, [{"pid": 101}]))
    with pytest.raises(real.SetupError, match="client_did_not_exit_gracefully"):
        real._graceful_exit(fake_desktop.process)
    assert calls == [(101, signal.SIGTERM)]


def test_cli_rejects_rollback_without_restart_before_preparation(fake_desktop):
    with pytest.raises(SystemExit) as failure:
        setup.main(["--state-dir", str(fake_desktop.state), "--without-accessibility"])
    assert failure.value.code == 2
    assert not fake_desktop.state.exists()
