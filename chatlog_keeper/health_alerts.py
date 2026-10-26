"""Local desktop reminders for collector/client health, never WeChat messages.

Only a fixed set of health codes reaches notification text. Dedupe, debounce
and delivery retry state live in the archive so restarting the collector does
not turn an outage into a stream of fresh desktop notifications.
"""
from __future__ import annotations

import math
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import time

from chatlog_keeper.core._linux import official_wechat_executables, process_pids_for_executable

_META_KEY = "client_health_alerts"
_ISSUES = {
    "login_required": ("微信需要重新登录", "采集服务可能仍在运行，但微信已停留在登录界面。请在本机微信中完成登录，恢复新消息同步。"),
    "not_running": ("微信未运行", "现有归档仍可读取，但无法接收新的微信消息。请在本机启动微信并确认已登录。"),
    "network_unavailable": ("微信同步受网络影响", "本机网络当前不可用，微信新消息同步无法确认。请检查网络；现有归档继续保留。"),
    "source_unavailable": ("微信归档数据源不可用", "采集器无法正常读取当前微信本地数据。新消息归档可能中断，请查看归档状态中的来源或密钥错误。"),
    "collector_error": ("微信消息采集异常", "最近一轮消息采集没有正常完成。请检查本机归档服务状态；不要将服务进程存在视为收取正常。"),
    "unknown": ("无法确认微信在线状态", "持续无法核实微信客户端状态。采集器运行不代表微信在线，请检查本机微信窗口和登录状态。"),
    "sync_unverified": ("微信消息同步待确认", "近期未确认新的消息同步。群聊安静也可能出现此状态，不能据此认定已退出登录，请检查本机微信。"),
}
_HEALTHY = frozenset({"session_present", "receiving", "healthy"})
_UNCERTAIN = frozenset({"unknown", "sync_unverified"})


def _number(value, default=0.0):
    return float(value) if type(value) in (int, float) and math.isfinite(value) else default


def _variant_string(value):
    # GVariant string syntax; these are subprocess argv entries, never a shell.
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _owned_bus(address, *, allow_abstract=False):
    """Accept only local Unix session buses; never inherit a TCP bus address."""
    if not isinstance(address, str) or ";" in address:
        return None
    if address.startswith("unix:abstract=") and allow_abstract:
        return address if "\x00" not in address else None
    if not address.startswith("unix:path="):
        return None
    path = Path(address[10:].split(",", 1)[0])
    try:
        info = path.stat()
        parent = path.parent.stat()
    except OSError:
        return None
    return address if (stat.S_ISSOCK(info.st_mode) and info.st_uid == os.geteuid()
                       and parent.st_uid == os.geteuid()) else None


def notification_environment():
    """Resolve the same user's desktop bus without copying arbitrary secrets."""
    environment = {key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL", "LC_CTYPE")
                   if key in os.environ}
    environment.setdefault("PATH", "/usr/bin:/bin")
    runtime = Path("/run/user") / str(os.geteuid())
    address = _owned_bus("unix:path=" + str(runtime / "bus"))
    if address:
        environment.update(DBUS_SESSION_BUS_ADDRESS=address, XDG_RUNTIME_DIR=str(runtime))
        return environment
    address = _owned_bus(os.environ.get("DBUS_SESSION_BUS_ADDRESS"))
    if address:
        environment["DBUS_SESSION_BUS_ADDRESS"] = address
        return environment
    # Some desktops use an abstract Unix socket rather than /run/user/UID/bus.
    # Only trust that value when obtained from this user's exact official client.
    for executable in official_wechat_executables():
        for pid in process_pids_for_executable(executable):
            try:
                process = Path("/proc") / str(pid)
                if process.stat().st_uid != os.geteuid():
                    continue
                with (process / "environ").open("rb") as handle:
                    raw = handle.read(128 * 1024)
                values = dict(item.split(b"=", 1) for item in raw.split(b"\0") if b"=" in item)
                raw_address = values.get(b"DBUS_SESSION_BUS_ADDRESS", b"").decode("utf-8")
                address = _owned_bus(raw_address, allow_abstract=True)
            except (OSError, UnicodeError):
                continue
            if address:
                environment["DBUS_SESSION_BUS_ADDRESS"] = address
                return environment
    return None


class DesktopNotifier:
    """One bounded local org.freedesktop.Notifications call, no other transport."""

    def __init__(self, *, run=subprocess.run, which=shutil.which, environment=notification_environment):
        self._run, self._which, self._environment = run, which, environment

    def notify(self, *, title, body, urgent=True, replace_id=0):
        environment = self._environment()
        if not environment:
            return {"delivered": False, "error": "desktop_session_bus_unavailable"}
        binary = self._which("gdbus")
        transport = "gdbus"
        if binary:
            hints = "{'urgency': <byte " + ("2" if urgent else "1") + ">}"
            command = [binary, "call", "--session", "--dest", "org.freedesktop.Notifications",
                       "--object-path", "/org/freedesktop/Notifications", "--method",
                       "org.freedesktop.Notifications.Notify", _variant_string("WeChat archive"),
                       str(max(0, int(replace_id))), _variant_string("dialog-warning" if urgent else "dialog-information"),
                       _variant_string(title), _variant_string(body), "[]", hints,
                       "0" if urgent else "10000"]
        else:
            binary = self._which("notify-send")
            transport = "notify-send"
            if not binary:
                return {"delivered": False, "error": "desktop_notification_command_missing"}
            command = [binary, "--app-name=WeChat archive", "--urgency=" + ("critical" if urgent else "normal"),
                       "--expire-time=" + ("0" if urgent else "10000"), "--", title, body]
        try:
            result = self._run(command, capture_output=True, text=True, timeout=3,
                               check=False, env=environment)
        except subprocess.TimeoutExpired:
            return {"delivered": False, "transport": transport, "error": "desktop_notification_timeout"}
        except OSError:
            return {"delivered": False, "transport": transport, "error": "desktop_notification_command_failed"}
        if result.returncode != 0:
            return {"delivered": False, "transport": transport, "error": "desktop_notification_rejected"}
        found = re.search(r"uint32\s+(\d+)", result.stdout or "") if transport == "gdbus" else None
        return {"delivered": True, "transport": transport,
                "notification_id": int(found[1]) if found else 0}


class HealthAlerts:
    def __init__(self, store, *, clock=time.time, notifier=None, debounce_seconds=20,
                 uncertain_seconds=300, reminder_seconds=1800):
        self.store, self.clock = store, clock
        self.notifier = notifier if notifier is not None else DesktopNotifier()
        self.debounce_seconds = max(0, debounce_seconds)
        self.uncertain_seconds = max(self.debounce_seconds, uncertain_seconds)
        self.reminder_seconds = max(1800, reminder_seconds)

    @staticmethod
    def _state(health):
        code = health.get("alert_code") or health.get("state")
        if code is None and isinstance(health.get("client"), dict):
            code = health["client"].get("state")
        return code if isinstance(code, str) and (code in _ISSUES or code in _HEALTHY) else "unknown"

    def observe(self, health):
        now = self.clock()
        saved = self.store.get_meta(_META_KEY, {})
        state = dict(saved) if isinstance(saved, dict) else {}
        state.setdefault("monitor_started_at", now)
        code = self._state(health)
        state.update(checked_at=now, observed_state=code)
        if code in _HEALTHY:
            state.pop("candidate_issue", None)
            state.pop("candidate_since", None)
            state.pop("candidate_count", None)
            if not state.get("active_issue"):
                state.update(status="healthy", last_error=None)
                self.store.set_meta(_META_KEY, state)
                return self._public(state)
            if state.get("candidate_recovery") != code:
                state.update(candidate_recovery=code, recovery_since=now, recovery_count=1)
            else:
                state["recovery_count"] = int(state.get("recovery_count", 0)) + 1
            state["status"] = "recovering"
            if (state["recovery_count"] >= 2 and now - _number(state.get("recovery_since"), now) >= self.debounce_seconds):
                if state.get("outage_notified"):
                    self._attempt(state, now, code, recovery=True)
                else:
                    self._clear_outage(state)
        else:
            state.pop("candidate_recovery", None)
            state.pop("recovery_since", None)
            state.pop("recovery_count", None)
            # A temporary unknown reading must not erase an already notified
            # login failure or cause a misleading recovery notification.
            if code in _UNCERTAIN and state.get("active_issue") not in (None, *_UNCERTAIN):
                state["status"] = "uncertain_during_outage"
                self.store.set_meta(_META_KEY, state)
                return self._public(state)
            if state.get("candidate_issue") != code:
                state.update(candidate_issue=code, candidate_since=now, candidate_count=1)
            else:
                state["candidate_count"] = int(state.get("candidate_count", 0)) + 1
            threshold = self.uncertain_seconds if code in _UNCERTAIN else self.debounce_seconds
            confirmed = state["candidate_count"] >= 2 and now - _number(state.get("candidate_since"), now) >= threshold
            state["status"] = "debouncing"
            if confirmed:
                changed = state.get("active_issue") != code
                if changed:
                    state.update(active_issue=code, issue_since=state.get("candidate_since", now))
                due = (not state.get("outage_notified") or state.get("last_delivered_issue") != code
                       or now - _number(state.get("last_delivered_at"), now) >= self.reminder_seconds)
                if due:
                    self._attempt(state, now, code)
                else:
                    state["status"] = "notified"
        self.store.set_meta(_META_KEY, state)
        return self._public(state)

    def _attempt(self, state, now, code, *, recovery=False):
        if now < _number(state.get("next_attempt_at")):
            state["status"] = "retry_wait"
            return
        if recovery:
            title = "微信新消息收取已恢复" if code == "receiving" else "微信会话界面已恢复"
            body = ("已重新确认收到新的微信消息，后台归档继续运行。" if code == "receiving"
                    else "已重新看到微信聊天会话界面，后台将继续监测消息同步；这不代表已经收到新消息。")
        else:
            title, body = _ISSUES[code]
        state.update(last_attempt_at=now)
        try:
            notify = self.notifier.notify if hasattr(self.notifier, "notify") else self.notifier
            result = notify(title=title, body=body, urgent=not recovery,
                            replace_id=int(state.get("notification_id") or 0))
        except Exception:
            result = {"delivered": False, "error": "desktop_notification_failed"}
        if isinstance(result, bool):
            result = {"delivered": result}
        if not isinstance(result, dict):
            result = {"delivered": False, "error": "desktop_notification_failed"}
        if result.get("delivered"):
            state.update(last_delivered_at=now, last_delivered_kind="recovery" if recovery else "problem",
                         last_error=None, delivery_failures=0, next_attempt_at=0,
                         notification_id=int(result.get("notification_id") or 0),
                         transport=result.get("transport"), status="recovered" if recovery else "notified")
            if recovery:
                self._clear_outage(state)
            else:
                state["outage_notified"] = True
                state["last_delivered_issue"] = code
        else:
            failures = int(state.get("delivery_failures") or 0) + 1
            raw_error = result.get("error")
            error = raw_error if isinstance(raw_error, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,80}", raw_error) else "desktop_notification_failed"
            state.update(status="delivery_failed", last_error=error, delivery_failures=failures,
                         next_attempt_at=now + min(300, 60 * 2 ** min(failures - 1, 3)))

    @staticmethod
    def _clear_outage(state):
        for key in ("active_issue", "issue_since", "outage_notified", "candidate_recovery",
                    "recovery_since", "recovery_count", "candidate_issue", "candidate_since", "candidate_count"):
            state.pop(key, None)
        state.update(status="healthy", last_error=None, delivery_failures=0, next_attempt_at=0)

    @staticmethod
    def _public(state):
        return {key: state.get(key) for key in (
            "status", "observed_state", "checked_at", "monitor_started_at", "active_issue", "issue_since",
            "candidate_issue", "candidate_since", "candidate_count", "outage_notified",
            "last_delivered_at", "last_delivered_kind", "last_attempt_at", "last_error",
            "next_attempt_at", "transport",
        )}
