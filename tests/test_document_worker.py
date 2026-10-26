"""The worker uses local synthetic bytes and never opens the WeChat client."""
import io
import json
from pathlib import Path
import struct
import subprocess
import sys
from types import SimpleNamespace

import pytest

from chatlog_keeper import document_worker as worker


def test_real_isolated_worker_extracts_utf8_text():
    result = worker.run_extract("Synthetic 文档\nsecond line".encode(), "sample.txt")
    assert result["status"] in {"ok", "parsed", "ready"}
    assert "Synthetic 文档" in "\n".join(unit["text"] for unit in result["units"])


def test_real_isolated_worker_filename_is_metadata_not_a_path(tmp_path):
    secret = tmp_path / "private.txt"
    secret.write_text("not-the-document", encoding="utf-8")
    result = worker.run_extract(b"provided-bytes-only", str(secret))
    assert result["status"] in {"ok", "parsed", "ready"}
    assert "provided-bytes-only" in "\n".join(unit["text"] for unit in result["units"])
    assert "not-the-document" not in json.dumps(result)


def test_parent_rejects_oversized_input_and_invalid_names_before_spawning(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("invalid input must not spawn")

    monkeypatch.setattr(worker.subprocess, "run", unexpected)
    monkeypatch.setattr(worker, "MAX_INPUT_BYTES", 3)
    assert worker.run_extract(b"four", "sample.txt")["reason"] == "document_input_too_large"
    assert worker.run_extract(bytearray(b"hi"), "sample.txt")["reason"] == "invalid_document_input"
    for name in ("", "a\0b", "a" * 1025, "\ud800"):
        assert worker.run_extract(b"hi", name)["reason"] == "invalid_document_filename"


def test_parent_uses_pinned_interpreter_isolation_and_binary_protocol(monkeypatch):
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout=b'{"status":"ok","units":[],"warnings":[]}')

    monkeypatch.setattr(worker.subprocess, "run", run)
    assert worker.run_extract(b"payload", "测试.txt")["status"] == "ok"
    command, kwargs = calls[0]
    assert command[:3] == [sys.executable, "-I", "-B"]
    assert command[3] == str(Path(worker.__file__).resolve())
    assert command[4:] == ["--worker"]
    encoded = "测试.txt".encode()
    assert kwargs["input"] == struct.pack(">I", len(encoded)) + encoded + b"payload"
    assert kwargs["timeout"] == 15
    assert kwargs["stderr"] == subprocess.DEVNULL
    assert kwargs["env"] == {"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    assert "shell" not in kwargs


@pytest.mark.parametrize("result,reason", [
    (SimpleNamespace(returncode=-9, stdout=b"private-error"), "document_worker_failed"),
    (SimpleNamespace(returncode=0, stdout=b"not-json"), "document_worker_invalid_result"),
    (SimpleNamespace(returncode=0, stdout=b"[]"), "document_worker_invalid_result"),
    (SimpleNamespace(returncode=0, stdout=b'{"status":"ok","units":null,"warnings":[]}'), "document_worker_invalid_result"),
    (SimpleNamespace(returncode=0, stdout=b"\xff"), "document_worker_invalid_result"),
])
def test_parent_reports_fixed_errors_without_child_output(monkeypatch, result, reason):
    monkeypatch.setattr(worker.subprocess, "run", lambda *args, **kwargs: result)
    actual = worker.run_extract(b"hello", "file.txt")
    assert actual["reason"] == reason
    assert actual["units"] == []
    assert "private" not in json.dumps(actual)


def test_parent_timeout_is_safe_and_does_not_sleep(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("private-command", 15, output=b"private-output")

    monkeypatch.setattr(worker.subprocess, "run", timeout)
    assert worker.run_extract(b"hello", "sample.txt") == {
        "status": "timeout", "reason": "document_extraction_timeout", "format": "unknown",
        "units": [], "warnings": [],
    }


def test_parent_reports_unavailable_worker_and_oversized_output(monkeypatch):
    def unavailable(*args, **kwargs):
        raise OSError("private-path")

    monkeypatch.setattr(worker.subprocess, "run", unavailable)
    assert worker.run_extract(b"hi", "f.txt")["reason"] == "document_worker_unavailable"
    monkeypatch.setattr(worker, "MAX_OUTPUT_BYTES", 3)
    monkeypatch.setattr(worker.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=b"four"))
    assert worker.run_extract(b"hi", "f.txt")["reason"] == "document_output_too_large"


@pytest.mark.parametrize("payload", [b"", b"\0\0\0", struct.pack(">I", 1025),
                                        struct.pack(">I", 3) + b"x", struct.pack(">I", 1) + b"\xff"])
def test_binary_protocol_rejects_malformed_input(payload):
    with pytest.raises((ValueError, UnicodeError)):
        worker._read_request(io.BytesIO(payload))


def test_protocol_limits_data_and_output_without_truncation(monkeypatch):
    monkeypatch.setattr(worker, "MAX_INPUT_BYTES", 3)
    with pytest.raises(OverflowError):
        worker._read_request(io.BytesIO(struct.pack(">I", 1) + b"x" + b"four"))
    monkeypatch.setattr(worker, "MAX_OUTPUT_BYTES", 100)
    output = io.BytesIO()
    worker._write_result({"text": "a" * 101}, output)
    assert json.loads(output.getvalue())["reason"] == "document_output_too_large"


def test_real_worker_cli_protocol_error_has_json_only():
    result = subprocess.run([sys.executable, "-I", "-B", str(Path(worker.__file__).resolve()), "--worker"],
                            input=b"bad", capture_output=True, timeout=5)
    assert result.returncode == 0
    assert result.stderr == b""
    assert json.loads(result.stdout)["reason"] == "invalid_document_protocol"


def test_limits_are_applied_inside_child(monkeypatch):
    import resource

    observed = []
    monkeypatch.setattr(resource, "setrlimit", lambda kind, limits: observed.append((kind, limits)))
    worker._apply_limits()
    assert (resource.RLIMIT_AS, (768 * 1024 * 1024,) * 2) in observed
    assert (resource.RLIMIT_CPU, (10, 10)) in observed
    assert (resource.RLIMIT_FSIZE, (0, 0)) in observed
    assert (resource.RLIMIT_CORE, (0, 0)) in observed
    assert (resource.RLIMIT_NOFILE, (32, 32)) in observed
