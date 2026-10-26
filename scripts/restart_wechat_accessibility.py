#!/usr/bin/env python3
"""Prepare or explicitly restart a verified WeChat for native accessibility.

The default action only observes settings and saves a private rollback record.
It never logs in, interacts with a chat, sends content, or downloads media.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import time

from chatlog_keeper import client_health
from chatlog_keeper.core._path_resolver import data_dir
from chatlog_keeper.core._secrets import private_text_writer, read_secret_text

DESKTOP_ENV = {
    "DISPLAY", "XAUTHORITY", "WAYLAND_DISPLAY", "XDG_RUNTIME_DIR",
    "DBUS_SESSION_BUS_ADDRESS", "XDG_SESSION_TYPE", "XDG_CURRENT_DESKTOP",
}
BASE_ENV = {"HOME", "USER", "LOGNAME", "PATH", "LANG", "LC_ALL", "LC_CTYPE",
            "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME"}
QT_ENV = {"QT_LINUX_ACCESSIBILITY_ALWAYS_ON", "QT_QPA_PLATFORM"}
FLAGS = {
    "toolkit_accessibility": ("gsettings", "get", "org.gnome.desktop.interface", "toolkit-accessibility"),
    "bus_enabled": ("gdbus", "call", "--session", "--dest", "org.a11y.Bus", "--object-path", "/org/a11y/bus",
                    "--method", "org.freedesktop.DBus.Properties.Get", "org.a11y.Status", "IsEnabled"),
    "screen_reader_enabled": ("gdbus", "call", "--session", "--dest", "org.a11y.Bus", "--object-path", "/org/a11y/bus",
                             "--method", "org.freedesktop.DBus.Properties.Get", "org.a11y.Status", "ScreenReaderEnabled"),
}


class SetupError(RuntimeError):
    pass


def _private_directory(path):
    path = Path(path).expanduser().absolute()
    for item in [*reversed(path.parents), path]:
        if item.is_symlink():
            raise SetupError("unsafe_setup_directory")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
        raise SetupError("unsafe_setup_directory")
    path.chmod(0o700)
    return path


def _paths(state_dir):
    state = _private_directory(state_dir)
    directory = _private_directory(state / "setup")
    manifest, log = directory / "accessibility.json", directory / "accessibility-client.log"
    for path in (manifest, log):
        if path.is_symlink():
            raise SetupError("unsafe_setup_file")
        if path.exists():
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
                raise SetupError("unsafe_setup_file")
    return manifest, log


def _start_ticks(pid):
    try:
        value = (Path("/proc") / str(pid) / "stat").read_text()
        return value.rsplit(") ", 1)[1].split()[19]
    except (OSError, IndexError):
        raise SetupError("client_identity_unavailable") from None


def _verified_process():
    complete, processes = client_health._discover_processes()
    if not complete or len(processes) != 1:
        raise SetupError("exactly_one_verified_client_required")
    process = processes[0]
    if not process["official_path"] or not client_health._verified_build(process["executable"]):
        raise SetupError("verified_official_client_required")
    return {"pid": process["pid"], "executable": process["executable"],
            "start_ticks": _start_ticks(process["pid"]), "sha256": client_health._VERIFIED_BUILD}


def _process_environment(process):
    environment = {key: value for key, value in os.environ.items() if key in BASE_ENV}
    try:
        with (Path("/proc") / str(process["pid"]) / "environ").open("rb") as handle:
            raw = handle.read(65537)
        if len(raw) > 65536:
            raise SetupError("client_environment_limit")
        for part in raw.split(b"\0"):
            key, separator, value = part.partition(b"=")
            name = key.decode(errors="replace")
            if separator and name in DESKTOP_ENV | QT_ENV:
                environment[name] = value.decode()
        return environment
    except (OSError, UnicodeError):
        raise SetupError("client_environment_unavailable") from None


def _read_flags(environment):
    values = {}
    for key, command in FLAGS.items():
        try:
            result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                    timeout=3, check=False, env=environment)
            words = re.findall(rb"\b(?:true|false)\b", result.stdout) if result.returncode == 0 and len(result.stdout) < 4096 else []
            values[key] = words[0] == b"true" if len(words) == 1 else None
        except (OSError, subprocess.SubprocessError):
            values[key] = None
    return values


def _write_flag(key, value, environment):
    if key not in FLAGS or type(value) is not bool:
        raise SetupError("invalid_accessibility_setting")
    command = list(FLAGS[key])
    if key == "toolkit_accessibility":
        command[1] = "set"
        command.append(str(value).lower())
    else:
        command[command.index("org.freedesktop.DBus.Properties.Get")] = "org.freedesktop.DBus.Properties.Set"
        command.append("<" + str(value).lower() + ">")
    try:
        result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True,
                                timeout=3, check=False, env=environment)
        if result.returncode:
            raise SetupError("accessibility_setting_write_failed")
    except (OSError, subprocess.SubprocessError):
        raise SetupError("accessibility_setting_write_failed") from None


def _save(path, proof):
    text = json.dumps(proof, indent=2) + "\n"
    if len(text.encode()) > 4096:
        raise SetupError("prepared_manifest_limit")
    with private_text_writer(path, secure_parent=True) as handle:
        handle.write(text)


def _load(path):
    value = read_secret_text(path)
    try:
        proof = json.loads(value) if value else None
    except ValueError:
        proof = None
    if not isinstance(proof, dict) or proof.get("schema") != 1:
        raise SetupError("prepared_manifest_required")
    identity = proof.get("expected_process")
    original = proof.get("original_flags")
    qt = proof.get("original_qt_environment")
    if (not isinstance(identity, dict) or set(identity) != {"pid", "start_ticks", "executable", "sha256"}
            or type(identity["pid"]) is not int or identity["pid"] <= 1
            or not isinstance(identity["start_ticks"], str) or not identity["start_ticks"].isdigit()
            or not isinstance(identity["executable"], str) or not Path(identity["executable"]).is_absolute()
            or identity["sha256"] != client_health._VERIFIED_BUILD
            or not isinstance(original, dict) or set(original) != set(FLAGS)
            or any(value is not None and type(value) is not bool for value in original.values())
            or not isinstance(qt, dict) or set(qt) != QT_ENV
            or any(value is not None and (not isinstance(value, str) or len(value) > 1024 or "\0" in value) for value in qt.values())):
        raise SetupError("invalid_prepared_manifest")
    return proof


def prepare(state_dir):
    manifest, _ = _paths(state_dir)
    process = _verified_process()
    if manifest.exists():
        previous = _load(manifest)
        if previous.get("state") in {"enabled", "restarting", "recovered_after_error"}:
            if previous["expected_process"] != process:
                raise SetupError("previous_setup_requires_review")
            return {"prepared": True, "reused": True, "manifest": str(manifest), "client_unchanged": True}
    environment = _process_environment(process)
    proof = {"schema": 1, "state": "prepared", "prepared_at": time.time(), "expected_process": process,
             "original_flags": _read_flags(environment),
             "original_qt_environment": {key: environment.get(key) for key in sorted(QT_ENV)},
             "rollback": "Arrange a restart, then run this script with the same --state-dir and --restart --without-accessibility.",
             "automatic_login": False, "messages_or_files_sent": False}
    if any(value is not None and len(value) > 1024 for value in proof["original_qt_environment"].values()):
        raise SetupError("client_environment_limit")
    _save(manifest, proof)
    return {"prepared": True, "manifest": str(manifest), "client_unchanged": True,
            "unavailable_settings": [key for key, value in proof["original_flags"].items() if value is None]}


def _launch(executable, environment, log):
    if not client_health._verified_build(executable):
        raise SetupError("prepared_build_changed")
    try:
        descriptor = os.open(log, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    except OSError:
        raise SetupError("client_log_unavailable") from None
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise SetupError("unsafe_setup_file")
        process = subprocess.Popen([executable], cwd=Path(executable).parent, env=environment,
                                   stdin=subprocess.DEVNULL, stdout=descriptor, stderr=descriptor,
                                   close_fds=True, start_new_session=True)
        return process.pid
    except OSError:
        raise SetupError("client_restart_failed") from None
    finally:
        os.close(descriptor)


def _graceful_exit(process):
    if _verified_process() != process:
        raise SetupError("prepared_client_changed")
    os.kill(process["pid"], signal.SIGTERM)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        complete, running = client_health._discover_processes()
        if not complete:
            raise SetupError("client_exit_unverified")
        if not running:
            return
        if any(item["pid"] != process["pid"] for item in running):
            raise SetupError("another_client_started")
        time.sleep(0.2)
    raise SetupError("client_did_not_exit_gracefully")


def restart(state_dir, without_accessibility=False):
    manifest, log = _paths(state_dir)
    proof = _load(manifest)
    process = _verified_process()
    if proof["expected_process"] != process:
        raise SetupError("prepared_client_changed")
    original_environment = _process_environment(process)
    environment = original_environment.copy()
    if without_accessibility:
        for key, value in proof["original_qt_environment"].items():
            environment.pop(key, None)
            if value is not None:
                environment[key] = value
    else:
        environment.update(QT_LINUX_ACCESSIBILITY_ALWAYS_ON="1", QT_QPA_PLATFORM="xcb")
    before = _read_flags(original_environment)
    previous_state = proof.get("state")
    proof.update(state="restarting", restart_requested_at=time.time())
    _save(manifest, proof)
    _graceful_exit(process)
    changed = []
    try:
        for key, original in proof["original_flags"].items():
            # Unknown settings are never modified: there is no reliable rollback.
            if original is None:
                continue
            wanted = original if without_accessibility else True
            if before[key] != wanted:
                _write_flag(key, wanted, environment)
                changed.append(key)
        pid = _launch(process["executable"], environment, log)
    except SetupError as failure:
        for key in reversed(changed):
            restore = before[key] if type(before[key]) is bool else proof["original_flags"][key]
            try:
                _write_flag(key, restore, original_environment)
            except SetupError:
                pass
        try:
            pid = _launch(process["executable"], original_environment, log)
            proof.update(expected_process={**process, "pid": pid, "start_ticks": _start_ticks(pid)},
                         state="enabled" if previous_state == "enabled" else "recovered_after_error",
                         last_error=str(failure))
            _save(manifest, proof)
        except (SetupError, OSError):
            pass
        raise
    proof.update(expected_process={**process, "pid": pid, "start_ticks": _start_ticks(pid)},
                 state="rolled_back" if without_accessibility else "enabled", restarted_at=time.time())
    _save(manifest, proof)
    return {"restarted_pid": pid, "accessibility_environment": not without_accessibility,
            "manifest": str(manifest), "automatic_login": False, "messages_or_files_sent": False,
            "unavailable_settings": [key for key, value in proof["original_flags"].items() if value is None]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, help="Private state directory; defaults to CHATLOG_KEEPER_DATA_DIR/XDG.")
    actions = parser.add_mutually_exclusive_group()
    actions.add_argument("--prepare", action="store_true", help="Read settings and prepare rollback only (default).")
    actions.add_argument("--restart", action="store_true", help="Explicitly restart the prepared, verified client; never log in.")
    parser.add_argument("--without-accessibility", action="store_true", help="With --restart, restore recorded environment and known settings.")
    args = parser.parse_args(argv)
    if args.without_accessibility and not args.restart:
        parser.error("--without-accessibility requires --restart")
    previous = os.umask(0o077)
    try:
        state = args.state_dir if args.state_dir is not None else data_dir()
        result = restart(state, args.without_accessibility) if args.restart else prepare(state)
        print(json.dumps(result))
        return 0
    except (SetupError, OSError, ValueError) as exc:
        reason = str(exc) if isinstance(exc, SetupError) else "accessibility_setup_failed"
        print(json.dumps({"ok": False, "reason": reason, "automatic_login": False, "messages_or_files_sent": False}))
        return 2
    finally:
        os.umask(previous)


if __name__ == "__main__":
    raise SystemExit(main())
