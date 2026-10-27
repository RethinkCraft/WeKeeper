"""Read-only Codex account telemetry. Never exports authentication material."""
from __future__ import annotations

import json
from pathlib import Path
import queue
import shutil
import subprocess
import threading
import time


def _bucket(value):
    if not isinstance(value, dict):
        return None
    result = {k: value.get(k) for k in ("limitId", "limitName", "planType", "rateLimitReachedType")}
    for key in ("primary", "secondary"):
        window = value.get(key)
        result[key] = {k: window.get(k) for k in ("usedPercent", "windowDurationMins", "resetsAt")} if isinstance(window, dict) else None
    return result


class CodexQuotaReader:
    """Only initialize, account/read and account/rateLimits/read are sent."""
    def __init__(self, *, command=None, clock=time.time):
        self.command = command or shutil.which("codex") or str(Path.home() / ".local/bin/codex")
        self.clock = clock
        self.process = None
        self.messages = queue.Queue()
        self.ident = 0
        self.changed = threading.Event()

    def _read_lines(self, process):
        try:
            for line in process.stdout:
                if len(line) > 2 * 1024 * 1024:
                    continue
                try:
                    message = json.loads(line)
                except (ValueError, TypeError):
                    continue
                if message.get("method") == "account/rateLimits/updated":
                    self.changed.set()
                elif "id" in message and ("result" in message or "error" in message):
                    self.messages.put(message)
        finally:
            self.messages.put(None)

    def _send(self, value):
        self.process.stdin.write(json.dumps(value) + "\n")
        self.process.stdin.flush()

    def _rpc(self, method, params=None):
        if method not in {"initialize", "account/read", "account/rateLimits/read"}:
            raise ValueError("quota_method_not_allowed")
        self.ident += 1
        ident = self.ident
        value = {"method": method, "id": ident}
        if params is not None:
            value["params"] = params
        self._send(value)
        end = time.monotonic() + 25
        while True:
            value = self.messages.get(timeout=max(0.01, end - time.monotonic()))
            if value is None:
                raise RuntimeError("codex_quota_process_closed")
            if value.get("id") != ident:
                continue
            if "error" in value:
                raise RuntimeError("codex_quota_unavailable")
            return value.get("result") or {}

    def _start(self):
        if self.process and self.process.poll() is None:
            return
        self.close()
        self.messages = queue.Queue()
        self.process = subprocess.Popen([self.command, "app-server", "--listen", "stdio://", "-c", "analytics.enabled=false"],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                        text=True, bufsize=1)
        threading.Thread(target=self._read_lines, args=(self.process,), daemon=True).start()
        self._rpc("initialize", {"clientInfo": {"name": "wekeeper_quota", "title": "WeKeeper Quota Monitor", "version": "0.2.0"}})
        self._send({"method": "initialized", "params": {}})

    def snapshot(self, source_id):
        value = {"source_id": source_id, "provider": "codex_app_server", "sampled_at": self.clock(),
                 "auth_state": "unavailable", "rate_limits": {"rateLimits": None, "rateLimitsByLimitId": None}, "error": None}
        try:
            self._start()
            account = self._rpc("account/read", {"refreshToken": False}).get("account")
            if not account or account.get("type") != "chatgpt":
                value["error"] = "codex_chatgpt_login_required"
                return value
            limits = self._rpc("account/rateLimits/read")
            by_id = limits.get("rateLimitsByLimitId")
            value.update(auth_state="ready", rate_limits={
                "rateLimits": _bucket(limits.get("rateLimits")),
                "rateLimitsByLimitId": {k: _bucket(v) for k, v in list(by_id.items())[:64]} if isinstance(by_id, dict) else None})
            self.changed.clear()
        except (OSError, ValueError, RuntimeError, queue.Empty):
            value["error"] = "codex_quota_unavailable"
            self.close()
        return value

    def close(self):
        process, self.process = self.process, None
        if process:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for stream in (process.stdin, process.stdout):
                if stream:
                    stream.close()
