"""One explicitly requested, verified native conversion in an isolated child.

The collector only starts or polls a child. A watchdog enforces the real-time
deadline while bounded pipe readers prevent a helper from blocking collection.
Only pump/close touch the archive; background threads never use SQLite.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
from typing import Any


MAX_OUTPUT_BYTES = 128 * 1024
MAX_INPUT_BYTES = 16 * 1024
_CODE = re.compile(r"[a-z][a-z0-9_]{0,80}")
_BUILD = re.compile(r"[0-9a-f]{64}")
_IDENTITY = ("account_id", "group_id", "server_id", "create_time", "sender_id", "voice_length_ms", "build")
_FAILURES = {"needs_login", "ambiguous", "unsupported", "timeout", "failed", "cancelled"}


@dataclass
class _Active:
    job: dict
    payload: dict
    process: Any
    output: dict = field(default_factory=lambda: {"stdout": bytearray(), "stderr": bytearray()})
    done: dict = field(default_factory=lambda: {"stdout": threading.Event(), "stderr": threading.Event()})
    lock: Any = field(default_factory=threading.Lock)
    reason: str | None = None
    timer: Any = None
    drain_deadline: float | None = None


class NativeTranscriptionWorker:
    def __init__(self, python="/usr/bin/python3", timeout=90, *, helper=None,
                 process_factory=None, timer_factory=None):
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 90:
            raise ValueError("invalid_native_transcription_timeout")
        self.python = str(python)
        self.timeout = timeout
        self.helper = Path(helper) if helper is not None else Path(__file__).with_name("native_accessibility.py")
        self._process_factory = process_factory or subprocess.Popen
        self._timer_factory = timer_factory or threading.Timer
        self._active = None
        self._closed = False

    @staticmethod
    def _capability(store):
        value = store.get_meta("native_transcription_capability", {})
        if (not isinstance(value, dict) or value.get("enabled") is not True
                or value.get("verified") is not True
                or not isinstance(value.get("build"), str) or not _BUILD.fullmatch(value["build"])):
            return None
        return value

    @staticmethod
    def _payload(job, capability, store):
        payload = {key: job.get(key) for key in (
            "job_id", "message_ref", "account_id", "group_id", "group_name", "server_id",
            "create_time", "sender_id", "sender_name", "voice_length_ms")}
        payload["build"] = capability["build"]
        payload["state_dir"] = str(store.state_dir)
        payload["parent_pid"] = os.getpid()
        payload["source_python"] = sys.executable
        for key in ("job_id", "message_ref", "account_id", "group_id", "group_name", "server_id", "sender_id"):
            if not isinstance(payload[key], str) or not payload[key] or len(payload[key]) > 1024:
                raise ValueError("native_job_identity_incomplete")
        if not payload["group_id"].endswith("@chatroom"):
            raise ValueError("native_job_identity_incomplete")
        if (type(payload["create_time"]) is not int or payload["create_time"] < 0
                or type(payload["voice_length_ms"]) is not int or not 0 < payload["voice_length_ms"] <= 86400000):
            raise ValueError("native_job_identity_incomplete")
        if payload["sender_name"] is not None and (not isinstance(payload["sender_name"], str) or len(payload["sender_name"]) > 1024):
            raise ValueError("native_job_identity_incomplete")
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8") + b"\n"
        if len(encoded) > MAX_INPUT_BYTES:
            raise ValueError("native_job_identity_too_large")
        return payload, encoded

    @staticmethod
    def _kill(active, reason):
        with active.lock:
            exited = active.process.poll() is not None
            if exited and all(event.is_set() for event in active.done.values()):
                return
            if exited and reason == "native_transcription_timeout":
                reason = "native_helper_pipe_timeout"
            if active.reason is None:
                active.reason = reason
            try:
                if isinstance(active.process, subprocess.Popen):
                    os.killpg(active.process.pid, signal.SIGKILL)
                elif not exited:
                    active.process.kill()
            except (OSError, ProcessLookupError):
                pass

    def _read(self, active, stream, name):
        try:
            while True:
                chunk = stream.read(8192)
                if not chunk:
                    break
                with active.lock:
                    available = MAX_OUTPUT_BYTES + 1 - len(active.output[name])
                    active.output[name].extend(chunk[:max(0, available)])
                    overflow = len(active.output[name]) > MAX_OUTPUT_BYTES
                if overflow:
                    self._kill(active, "native_helper_output_limit")
                    break
        except (OSError, ValueError):
            self._kill(active, "native_helper_pipe_failed")
        finally:
            stream.close()
            active.done[name].set()

    def _write(self, active, encoded):
        try:
            active.process.stdin.write(encoded)
            active.process.stdin.flush()
        except (OSError, ValueError):
            self._kill(active, "native_helper_pipe_failed")
        finally:
            active.process.stdin.close()

    def _start(self, job, payload, encoded):
        process = self._process_factory(
            [self.python, "-B", str(self.helper), "--transcribe"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            close_fds=True, start_new_session=True,
        )
        active = _Active(job=job, payload=payload, process=process)
        self._active = active
        active.timer = self._timer_factory(self.timeout, self._kill, args=(active, "native_transcription_timeout"))
        active.timer.daemon = True
        active.timer.start()
        for name in ("stdout", "stderr"):
            threading.Thread(target=self._read, args=(active, getattr(process, name), name), daemon=True).start()
        threading.Thread(target=self._write, args=(active, encoded), daemon=True).start()

    @staticmethod
    def _result(active):
        if active.reason:
            return ("timeout" if active.reason in {"native_transcription_timeout", "native_helper_pipe_timeout"} else "failed"), active.reason, None
        try:
            value = json.loads(bytes(active.output["stdout"]).decode("utf-8"))
        except (ValueError, UnicodeError, RecursionError):
            return "failed", "native_helper_invalid_result", None
        if not isinstance(value, dict):
            return "failed", "native_helper_invalid_result", None
        status = value.get("status")
        if isinstance(status, str) and status in _FAILURES:
            reason = value.get("reason")
            return status, reason if isinstance(reason, str) and _CODE.fullmatch(reason) else "native_conversion_failed", None
        if active.process.returncode != 0:
            return "failed", "native_helper_failed", None
        proof, provenance, text = value.get("proof"), value.get("provenance"), value.get("text")
        if (status != "succeeded" or not isinstance(text, str) or not text.strip()
                or not isinstance(proof, dict) or not isinstance(provenance, dict)):
            return "failed", "native_helper_invalid_result", None
        if any(type(proof.get(key)) is not type(active.payload[key]) or proof.get(key) != active.payload[key] for key in _IDENTITY):
            return "ambiguous", "native_result_identity_mismatch", None
        located = provenance.get("located_by")
        if (provenance.get("source") != "wechat_native_accessibility" or type(provenance.get("version")) is not int
                or provenance["version"] != 1 or provenance.get("build") != active.payload["build"]
                or not isinstance(located, list) or not located or len(located) > 20
                or any(not isinstance(item, str) or not _CODE.fullmatch(item) for item in located)):
            return "failed", "native_result_provenance_invalid", None
        # Only a small verified provenance schema reaches the archive. Stderr,
        # helper diagnostics, raw content and arbitrary output fields stay out.
        clean = {key: provenance[key] for key in ("source", "version", "build", "located_by")}
        clean["verified_identity"] = {key: proof[key] for key in _IDENTITY}
        return "succeeded", None, {"text": text, "provenance": clean}

    def pump(self, store):
        """Start/poll at most one task, without waiting for the UI or its result."""
        if self._closed:
            return {"state": "closed"}
        capability = self._capability(store)
        active = self._active
        if active is not None:
            revoked = capability is None or capability["build"] != active.payload["build"]
            if revoked:
                self._kill(active, "native_capability_revoked")
            if active.process.poll() is None:
                return {"state": "running", "job_id": active.job["job_id"]}
            if not all(event.is_set() for event in active.done.values()):
                if active.drain_deadline is None:
                    active.drain_deadline = time.monotonic() + 1
                if time.monotonic() >= active.drain_deadline:
                    self._kill(active, "native_helper_pipe_timeout")
                if active.reason is None:
                    return {"state": "running", "job_id": active.job["job_id"]}
            active.timer.cancel()
            active.process.wait(timeout=0)
            if revoked:
                status, reason, result = "cancelled", "native_capability_revoked", None
            else:
                status, reason, result = self._result(active)
            if result is not None:
                store.save_native_transcript(active.job["message_ref"], result["text"], result["provenance"], job_id=active.job["job_id"])
            else:
                store.update_voice_transcription_job(active.job["job_id"], status, reason)
            self._active = None
            return {"state": status, "job_id": active.job["job_id"], "reason": reason}
        if capability is None:
            return {"state": "disabled"}
        job = store.claim_voice_transcription_job(lease_seconds=100)
        if job is None:
            return {"state": "idle"}
        try:
            payload, encoded = self._payload(job, capability, store)
        except ValueError as exc:
            store.update_voice_transcription_job(job["job_id"], "unsupported", str(exc))
            return {"state": "unsupported", "job_id": job["job_id"], "reason": str(exc)}
        try:
            self._start(job, payload, encoded)
        except OSError:
            store.update_voice_transcription_job(job["job_id"], "failed", "native_helper_start_failed")
            return {"state": "failed", "job_id": job["job_id"], "reason": "native_helper_start_failed"}
        return {"state": "running", "job_id": job["job_id"]}

    def close(self, store=None):
        """Stop a child during collector shutdown; never wait on its UI work."""
        self._closed = True
        active = self._active
        if active is None:
            return
        active.timer.cancel()
        self._kill(active, "native_worker_interrupted")
        try:
            active.process.wait(timeout=0.5)
        except (subprocess.TimeoutExpired, OSError):
            pass
        self._active = None
        if store is not None:
            store.update_voice_transcription_job(active.job["job_id"], "cancelled", "native_worker_interrupted")
