from pathlib import Path

from chatlog_keeper.core import _linux
from chatlog_keeper import wechat_db


def test_linux_data_roots_are_bounded(monkeypatch):
    home = Path("/home/tester")
    monkeypatch.setattr(_linux, "xdg_documents", lambda _home: home / "Documents")
    wechat = _linux.wechat_data_roots(home)
    assert home / "Documents" / "xwechat_files" in wechat
    assert home / "文档" / "xwechat_files" in wechat
    assert any("com.tencent.WeChat" in str(path) for path in wechat)
    assert wechat[0].name == "xwechat_files"


def test_linux_process_pids_skip_chromium_helpers(monkeypatch):
    monkeypatch.setattr(_linux, "is_linux", lambda: True)
    monkeypatch.setattr(
        _linux,
        "_iter_proc_clients",
        lambda: (
            (10, "/opt/wechat/wechat", "wechat", "/opt/wechat/wechat"),
            (11, "/opt/wechat/wechat", "wechat", "/opt/wechat/wechat --type=renderer"),
            (12, "/opt/wechat/wechat", "wechat", "/opt/wechat/wechat --type=gpu-process"),
            (13, "/usr/bin/unrelated", "unrelated", "/usr/bin/unrelated"),
            (14, "/usr/bin/crashpad_handler", "crashpad", "/usr/bin/crashpad_handler"),
        ),
    )
    assert _linux.process_pids(("wechat", "WeChat")) == [10]


def test_linux_process_pids_fail_closed_off_linux(monkeypatch):
    monkeypatch.setattr(_linux, "is_linux", lambda: False)
    monkeypatch.setattr(
        _linux,
        "_iter_proc_clients",
        lambda: (_ for _ in ()).throw(AssertionError("/proc must not be read")),
    )
    assert _linux.process_pids(("wechat",)) == []


def test_linux_exact_executable_pid_match(monkeypatch):
    monkeypatch.setattr(_linux, "is_linux", lambda: True)
    expected = Path("/opt/wechat/wechat")
    monkeypatch.setattr(_linux.os, "path", _linux.os.path)
    monkeypatch.setattr(_linux.os.path, "realpath", lambda value: expected.as_posix())
    monkeypatch.setattr(_linux.os, "listdir", lambda _path: ["1", "42", "self"])
    monkeypatch.setattr(
        _linux,
        "_iter_proc_clients",
        lambda: (
            (42, "/opt/wechat/wechat", "wechat", "/opt/wechat/wechat"),
            (43, "/opt/wechat/wechat", "wechat", "/opt/wechat/wechat --type=renderer"),
            (44, "/usr/bin/wechat", "wechat", "/usr/bin/wechat"),
        ),
    )
    assert _linux.process_pids_for_executable(expected) == [42]


def test_linux_user_dirs_documents(tmp_path):
    config = tmp_path / ".config"
    config.mkdir()
    (config / "user-dirs.dirs").write_text(
        'XDG_DOCUMENTS_DIR="$HOME/文档"\n',
        encoding="utf-8",
    )
    monkey_home = tmp_path
    assert _linux._documents_from_user_dirs(monkey_home) == tmp_path / "文档"


def test_wechat_resolver_finds_linux_documents(monkeypatch, tmp_path):
    root = tmp_path / "xwechat_files"
    root.mkdir()
    monkeypatch.delenv("CHATLOG_WECHAT_DATA_ROOT", raising=False)
    monkeypatch.setattr(_linux, "wechat_data_roots", lambda: [root])
    assert wechat_db.find_weixin_data_root() == root


def test_official_binary_discovery_ignores_environment_override(monkeypatch, tmp_path):
    untrusted = tmp_path / "wechat"
    untrusted.write_bytes(b"untrusted executable")
    monkeypatch.setenv("CHATLOG_WECHAT_EXECUTABLE", str(untrusted))
    monkeypatch.setenv("CHATLOG_KEEPER_WECHAT_EXECUTABLE", str(untrusted))
    # Keep the test off the real machine's reviewed portable binary. A changed
    # or unreviewed portable build must not become another launch candidate.
    monkeypatch.setattr(Path, "read_bytes", lambda _path: b"unreviewed build")
    candidates = []
    monkeypatch.setattr(
        _linux,
        "_existing_executables",
        lambda paths: candidates.extend(paths) or [],
    )
    assert _linux.official_wechat_executables() == []
    assert candidates == [Path("/opt/wechat/wechat"), Path("/usr/bin/wechat")]
    assert untrusted not in candidates
