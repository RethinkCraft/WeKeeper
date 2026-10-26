from __future__ import annotations

import os
from pathlib import Path

import pytest

from chatlog_keeper import active_key, linux_key


def test_linux_active_refuses_when_daily_client_is_running(monkeypatch, tmp_path):
    db = tmp_path / "message_0.db"
    db.write_bytes(b"w" * 4096)
    monkeypatch.setattr(linux_key, "is_linux", lambda: True)
    monkeypatch.setattr(linux_key, "daily_client_running", lambda _source: True)
    monkeypatch.setattr(
        linux_key,
        "official_client_executable",
        lambda _source: Path("/opt/wechat/wechat"),
    )
    monkeypatch.setattr(linux_key, "locate_kdf_boundary", lambda _exe: None)
    monkeypatch.setattr(linux_key, "wechat_exports_capture_symbols", lambda _exe: False)
    monkeypatch.setattr(
        linux_key.subprocess, "Popen", lambda *_args, **_kwargs: pytest.fail("no launch")
    )
    assert linux_key.extract_wechat_key_active(db_path=str(db), timeout=1) is None
    assert linux_key.last_error() == "daily_client_single_instance_conflict"


def test_linux_active_refuses_missing_official_binary(monkeypatch, tmp_path):
    db = tmp_path / "message_0.db"
    db.write_bytes(b"w" * 64)
    monkeypatch.setattr(linux_key, "is_linux", lambda: True)
    monkeypatch.setattr(linux_key, "daily_client_running", lambda _source: False)
    monkeypatch.setattr(linux_key, "official_client_executable", lambda _source: None)
    assert linux_key.extract_wechat_key_active(db_path=str(db), timeout=1) is None
    assert linux_key.last_error() == "official_client_not_found"


def test_wechat_active_skips_preload_without_exported_symbols(monkeypatch, tmp_path):
    db = tmp_path / "message_0.db"
    db.write_bytes(b"w" * 4096)
    spawned = {}

    def _spawn(source, *, capture_library=None, capture_fifo=None):
        spawned["source"] = source
        spawned["capture_library"] = capture_library
        spawned["capture_fifo"] = capture_fifo
        return 4242

    monkeypatch.setattr(linux_key, "is_linux", lambda: True)
    monkeypatch.setattr(linux_key, "daily_client_running", lambda _source: False)
    monkeypatch.setattr(
        linux_key, "official_client_executable", lambda _source: Path("/opt/wechat/wechat")
    )
    monkeypatch.setattr(linux_key, "wechat_exports_capture_symbols", lambda _exe: False)
    monkeypatch.setattr(linux_key, "locate_kdf_boundary", lambda _exe: None)
    monkeypatch.setattr(linux_key, "ensure_capture_library", lambda: Path("/tmp/should-not-load.so"))
    monkeypatch.setattr(linux_key, "create_capture_channel", lambda: pytest.fail("no FIFO"))
    monkeypatch.setattr(linux_key, "spawn_official_client", _spawn)
    monkeypatch.setattr(linux_key, "extract_verified", lambda *_args, **_kwargs: b"\x00" * 32)
    monkeypatch.setattr(linux_key, "terminate_spawned", lambda _pid: True)
    monkeypatch.setattr(
        "chatlog_keeper.wechat_db._read_stable_page1", lambda _path: b"p" * 4096
    )
    monkeypatch.setattr(
        "chatlog_keeper.wechat_db._verify_key_v4",
        lambda candidate, _page: candidate == b"\x00" * 32,
    )
    assert linux_key.extract_wechat_key_active(db_path=str(db), timeout=1) == b"\x00" * 32
    assert spawned["source"] == "wechat"
    assert spawned["capture_library"] is None
    assert spawned["capture_fifo"] is None


def test_wechat_symbol_probe_reads_nm_output(tmp_path, monkeypatch):
    binary = tmp_path / "wechat"
    binary.write_bytes(b"\x7fELF")

    def _fake_run(argv, **_kwargs):
        if argv[0] == "nm" and argv[-1] == str(binary):
            return type("R", (), {"returncode": 0, "stdout": "0000 T sqlite3_key\n"})()
        return type("R", (), {"returncode": 1, "stdout": ""})()

    monkeypatch.setattr(linux_key.subprocess, "run", _fake_run)
    assert linux_key.wechat_exports_capture_symbols(binary) is True


def test_active_key_entry_forwards_linux_wechat_options(monkeypatch, tmp_path):
    db = tmp_path / "message_0.db"
    observed = {}
    cancel_requested = lambda: False
    expected = bytes(range(32))

    def _fake(**kwargs):
        observed.update(kwargs)
        return expected

    monkeypatch.setattr(linux_key, "extract_wechat_key_active", _fake)
    result = active_key.extract_wechat_key_active(
        db_path=str(db), analyze_only=True, timeout=12,
        _cancel_requested=cancel_requested,
    )
    assert result == expected
    assert observed == {
        "db_path": str(db), "analyze_only": True, "timeout": 12,
        "cancel_requested": cancel_requested,
    }


def test_linux_script_discovery_ignores_environment_override(monkeypatch, tmp_path):
    monkeypatch.setenv("CHATLOG_KEEPER_SCRIPTS_DIR", str(tmp_path))
    monkeypatch.delattr(linux_key.sys, "_MEIPASS", raising=False)
    scripts = linux_key._scripts_dir()
    assert scripts == Path(linux_key.__file__).resolve().parent / "scripts"
    assert scripts != tmp_path
    assert (scripts / "linux_wechat_gdb_capture.py").is_file()


@pytest.mark.skipif(
    not hasattr(os, "mkfifo") or not hasattr(os, "geteuid"),
    reason="capture channel requires POSIX FIFO and user ownership semantics",
)
def test_capture_channel_accepts_only_wxk1(tmp_path, monkeypatch):
    monkeypatch.setattr(linux_key, "data_dir", lambda: tmp_path)
    channel = linux_key.create_capture_channel()
    assert channel is not None
    payload = b"WXK1" + bytes(range(32))
    write_fd = os.open(channel.path, os.O_WRONLY | os.O_NONBLOCK)
    try:
        os.write(write_fd, payload)
    finally:
        os.close(write_fd)
    assert channel.read_candidates() == [bytes(range(32))]
    assert channel.close() is True
