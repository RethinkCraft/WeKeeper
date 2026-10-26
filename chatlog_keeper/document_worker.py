"""Bounded, short-lived document parsing without request-provided file paths.

The trusted extractor consumes bytes only and performs no network requests or
external program calls. Resource limits protect the collector/MCP process from
expensive or malformed input; they are not an operating-system network sandbox.
"""
from __future__ import annotations

from contextlib import redirect_stdout
import json
from pathlib import Path
import struct
import subprocess
import sys

MAX_INPUT_BYTES = 32 * 1024 * 1024
MAX_FILENAME_BYTES = 1024
MAX_OUTPUT_BYTES = 12 * 1024 * 1024
TIMEOUT_SECONDS = 15
CPU_SECONDS = 10
MEMORY_BYTES = 768 * 1024 * 1024


def _failure(status: str, reason: str) -> dict:
    return {"format": "unknown", "status": status, "reason": reason,
            "units": [], "warnings": []}


def _filename_bytes(filename: str) -> bytes | None:
    if not isinstance(filename, str) or not filename or "\0" in filename:
        return None
    try:
        encoded = filename.encode("utf-8", errors="strict")
    except UnicodeError:
        return None
    return encoded if len(encoded) <= MAX_FILENAME_BYTES else None


def run_extract(data: bytes, filename: str) -> dict:
    """Parse archived bytes in a separate pinned Python process.

    ``filename`` is a format hint only, never a source path. Queries must resolve
    and verify archive blobs before calling this boundary.
    """
    if not isinstance(data, bytes):
        return _failure("error", "invalid_document_input")
    if len(data) > MAX_INPUT_BYTES:
        return _failure("limit_exceeded", "document_input_too_large")
    name = _filename_bytes(filename)
    if name is None:
        return _failure("error", "invalid_document_filename")
    payload = struct.pack(">I", len(name)) + name + data
    try:
        result = subprocess.run(
            [sys.executable, "-I", "-B", str(Path(__file__).resolve()), "--worker"],
            input=payload, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            timeout=TIMEOUT_SECONDS, check=False,
            env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
        )
    except subprocess.TimeoutExpired:
        return _failure("timeout", "document_extraction_timeout")
    except OSError:
        return _failure("error", "document_worker_unavailable")
    if result.returncode != 0:
        return _failure("error", "document_worker_failed")
    if not isinstance(result.stdout, bytes) or len(result.stdout) > MAX_OUTPUT_BYTES:
        return _failure("limit_exceeded", "document_output_too_large")
    try:
        parsed = json.loads(result.stdout.decode("utf-8", errors="strict"))
    except (UnicodeError, ValueError, RecursionError):
        return _failure("error", "document_worker_invalid_result")
    if (not isinstance(parsed, dict) or not isinstance(parsed.get("status"), str)
            or not isinstance(parsed.get("units"), list)
            or not isinstance(parsed.get("warnings"), list)):
        return _failure("error", "document_worker_invalid_result")
    return parsed


def _apply_limits() -> None:
    import resource

    resource.setrlimit(resource.RLIMIT_AS, (MEMORY_BYTES, MEMORY_BYTES))
    resource.setrlimit(resource.RLIMIT_CPU, (CPU_SECONDS, CPU_SECONDS))
    resource.setrlimit(resource.RLIMIT_FSIZE, (0, 0))
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_NOFILE, (32, 32))


def _read_request(stream) -> tuple[bytes, str]:
    header = stream.read(4)
    if len(header) != 4:
        raise ValueError("invalid_document_protocol")
    name_length = struct.unpack(">I", header)[0]
    if not 1 <= name_length <= MAX_FILENAME_BYTES:
        raise ValueError("invalid_document_protocol")
    name = stream.read(name_length)
    if len(name) != name_length:
        raise ValueError("invalid_document_protocol")
    filename = name.decode("utf-8", errors="strict")
    if _filename_bytes(filename) != name:
        raise ValueError("invalid_document_protocol")
    data = stream.read(MAX_INPUT_BYTES + 1)
    if len(data) > MAX_INPUT_BYTES:
        raise OverflowError("document_input_too_large")
    return data, filename


def _write_result(result: dict, stream) -> None:
    try:
        encoded = json.dumps(result, ensure_ascii=False, allow_nan=False,
                             separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        encoded = json.dumps(_failure("error", "document_worker_invalid_result")).encode()
    if len(encoded) > MAX_OUTPUT_BYTES:
        encoded = json.dumps(_failure("limit_exceeded", "document_output_too_large")).encode()
    stream.write(encoded)
    stream.flush()


def _worker_main() -> int:
    try:
        _apply_limits()
        data, filename = _read_request(sys.stdin.buffer)
    except OverflowError:
        result = _failure("limit_exceeded", "document_input_too_large")
    except Exception:
        result = _failure("error", "invalid_document_protocol")
    else:
        try:
            # -I excludes CWD/PYTHONPATH; add only this trusted package checkout.
            # This also supports an editable installation with an isolated child.
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
            # Third-party parser diagnostics must not become protocol content.
            # The caller discards stderr and never returns it to the agent.
            with redirect_stdout(sys.stderr):
                from chatlog_keeper.document_extractors import extract_document

                result = extract_document(data, filename)
        except MemoryError:
            result = _failure("limit_exceeded", "document_memory_limit")
        except Exception:
            result = _failure("error", "document_extraction_failed")
    _write_result(result, sys.stdout.buffer)
    return 0


if __name__ == "__main__":
    if sys.argv[1:] != ["--worker"]:
        _write_result(_failure("error", "invalid_document_protocol"), sys.stdout.buffer)
        raise SystemExit(2)
    raise SystemExit(_worker_main())
