"""Explicit native-only runtime wiring; importing or checking never starts work.

Receiving, business scheduling, frozen-result projection, new submission and
old-result reconciliation have separate lanes. Gateway clients share retained
idempotency state, not an HTTP transport. Accepted sends never mean phone receipt.
Native receive HTTP waits run outside the custody database lock; persisted source
and cursor checks still guard admission, generation and final delivery.
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
import copy
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import tempfile
import threading
import time
from urllib.parse import quote

import httpx

from .protocol_gateway import ProtocolGateway, _base_url, _identifier
from .providers import ChatGPTProvider, CompatibleProvider, ProviderError, profile_signature
from .store import BotStore, normalize_config
from .usage_actor import UsageLedgerActor


class RuntimeErrorCode(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def error_code(error):
    code = getattr(error, "code", None)
    if isinstance(code, str) and re.fullmatch(r"[A-Za-z0-9_]{1,100}", code):
        return code
    return type(error).__name__


class _ProviderLease:
    def __init__(self, provider, release):
        self._provider, self._release = provider, release
        self._lock, self._closed = threading.Lock(), False

    def complete(self, *args, **kwargs):
        return self._provider.complete(*args, **kwargs)

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        try:
            self._provider.close()
        finally:
            self._release()


class _LocalExecutionLease:
    """An accounting owner lease; this is not a model provider."""
    def __init__(self, ledger, release):
        self.ledger, self._release = ledger, release
        self._lock, self._closed = threading.Lock(), False

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._release()


class ProviderResources:
    """Current provider configuration with pooled HTTP and owned usage lifetime.

    Compatible providers borrow one UsageLedgerActor. SIWC retains its existing
    request-local, inference-thread-owned usage ledger and auth client. Both
    provider kinds have leases; HTTP and shared accounting close only afterwards.
    Construction is lazy and opens neither model transport nor accounting DB.
    """
    def __init__(self, store, *, state_dir, usage_dir, dashboard_dir,
                 http_factory=None, compatible_factory=None, chatgpt_factory=None, usage_factory=None):
        self.store = store
        self.state_dir, self.usage_dir, self.dashboard_dir = map(Path, (state_dir, usage_dir, dashboard_dir))
        self._http_factory = http_factory or httpx.Client
        self._compatible_factory = compatible_factory or CompatibleProvider
        self._chatgpt_factory = chatgpt_factory or ChatGPTProvider
        self._usage_factory = usage_factory or UsageLedgerActor
        self._condition, self._resource_lock = threading.Condition(), threading.Lock()
        self._active, self._closing, self._closed = 0, False, False
        self._clients, self._usage = {}, None

    def _release(self):
        with self._condition:
            self._active -= 1
            self._condition.notify_all()

    def validation_provider(self, profile):
        """A fresh leased provider for the controller's fixed read-only probe."""
        return self.provider(profile, validation_probe=True)

    def provider(self, profile, *, validation_probe=False):
        with self._condition:
            if self._closing:
                raise ProviderError("bot_provider_resources_closed")
            self._active += 1
        try:
            profile = copy.deepcopy(profile)
            with self._resource_lock:
                key = (profile.get("kind"), profile.get("base_url"))
                if key not in self._clients:
                    self._clients[key] = self._http_factory(timeout=60, follow_redirects=False, http2=True,
                        limits=httpx.Limits(max_connections=4, max_keepalive_connections=4, keepalive_expiry=60))
                client = self._clients[key]
                if profile.get("kind") != "siwc" and self._usage is None:
                    self._usage = self._usage_factory(self.usage_dir)
            if profile.get("kind") == "siwc":
                provider = self._chatgpt_factory(self.dashboard_dir, profile,
                    usage_dir=self.usage_dir, http_client=client)
            else:
                verified = validation_probe or self.store.get("verified_profile_" + profile["id"]) == profile_signature(profile)
                provider = self._compatible_factory(self.state_dir, self.usage_dir,
                    {**profile, "tools_verified": verified}, client=client, ledger=self._usage)
            return _ProviderLease(provider, self._release)
        except BaseException:
            self._release()
            raise

    def local_business_execution(self, profile=None):
        """Borrow durable local accounting without opening a model transport."""
        with self._condition:
            if self._closing:
                raise ProviderError("bot_provider_resources_closed")
            self._active += 1
        try:
            with self._resource_lock:
                if self._usage is None:
                    self._usage = self._usage_factory(self.usage_dir)
            return _LocalExecutionLease(self._usage, self._release)
        except BaseException:
            self._release()
            raise

    def usage_ledger(self):
        """Shared actor used to FULL-write deferred generation accounting."""
        with self._condition:
            if self._closing:
                raise ProviderError("bot_provider_resources_closed")
        with self._resource_lock:
            if self._usage is None:
                self._usage = self._usage_factory(self.usage_dir)
            return self._usage

    def close(self):
        with self._condition:
            if self._closing:
                self._condition.wait_for(lambda: self._closed)
                return
            self._closing = True
            self._condition.wait_for(lambda: self._active == 0)
        errors = []
        try:
            for resource in ([self._usage] if self._usage is not None else []) + list(self._clients.values()):
                try:
                    resource.close()
                except BaseException as error:
                    errors.append(error)
        finally:
            with self._condition:
                self._closed = True
                self._condition.notify_all()
        if errors:
            raise errors[0]


@dataclass(frozen=True)
class RuntimeConfig:
    state_dir: Path
    usage_dir: Path
    inbox_path: Path
    outbox_path: Path
    journal_path: Path
    token_file: Path
    bindings_file: Path
    account_id: str
    own_username: str
    consumer: str
    server: str
    dashboard_dir: Path | None = None
    activate: bool = False
    gateway_timeout: float = 10.0
    status_file: Path | None = None
    cloud_control: bool = False
    archive_state: Path | None = None


def _private_path(value, *, directory=False, required=True):
    path = Path(value).expanduser()
    if not path.is_absolute() or ".." in path.parts:
        raise RuntimeErrorCode("protocol_runtime_absolute_paths_required")
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise RuntimeErrorCode("protocol_runtime_symlink_path")
    if not path.exists():
        if required:
            raise RuntimeErrorCode("protocol_runtime_path_missing")
        _private_path(path.parent, directory=True)
        return path
    info = path.stat()
    expected = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise RuntimeErrorCode("protocol_runtime_private_path_required")
    return path


def _read_json(path, limit=1024 * 1024):
    if path.stat().st_size > limit:
        raise RuntimeErrorCode("protocol_runtime_document_too_large")
    try:
        return json.loads(path.read_text())
    except (ValueError, UnicodeError):
        raise RuntimeErrorCode("protocol_runtime_invalid_json") from None


def _timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _safe_code(value):
    return value if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_]{1,100}", value) else "UnknownError"


def _status_path(config):
    if config.status_file is None:
        return None
    path = _private_path(config.status_file, required=False)
    reserved = [Path(config.state_dir) / "bot" / "bot.sqlite", config.inbox_path, config.outbox_path,
                config.journal_path, config.token_file, config.bindings_file]
    protected = {Path(str(item) + suffix).expanduser().absolute() for item in reserved
                 for suffix in ("", "-wal", "-shm", "-journal", ".lock")}
    if path in protected:
        raise RuntimeErrorCode("protocol_runtime_path_collision")
    return path


class RuntimeStatusFile:
    """Optional observer, never a business ledger or delivery receipt.

    Only bounded counters, lifecycle states and error codes are persisted. A
    pre-existing unrelated file is refused, including credentials and DBs.
    """
    MIN_INTERVAL = 1.0
    HEARTBEAT = 10.0
    COMPONENT = "wekeeper_protocol_runtime"

    def __init__(self, path):
        self.path = _private_path(path, required=False)
        self._lock = threading.Lock()
        self._validate_existing()

    @classmethod
    def for_config(cls, config):
        path = _status_path(config)
        return cls(path) if path is not None else None

    def _validate_existing(self):
        _private_path(self.path, required=False)
        if self.path.exists():
            try:
                current = _read_json(self.path, limit=65536)
            except (RuntimeErrorCode, OSError):
                raise RuntimeErrorCode("protocol_runtime_status_file_invalid") from None
            if not isinstance(current, dict) or current.get("component") != self.COMPONENT or current.get("schema") != 1:
                raise RuntimeErrorCode("protocol_runtime_status_file_invalid")

    @classmethod
    def document(cls, snapshot):
        states = {"starting", "running", "attention", "stopping", "stopped", "failed"}
        result = {"schema": 1, "component": cls.COMPONENT, "updated_at": _timestamp(), "pid": os.getpid(),
                  "state": snapshot["state"] if snapshot.get("state") in states else "attention",
                  "phone_received": False, "lanes": {}}
        if snapshot.get("error"):
            result["error"] = _safe_code(snapshot["error"])
        account = snapshot.get("account") if isinstance(snapshot.get("account"), dict) else {}
        authenticated = account.get("authenticated")
        connection = account.get("connection")
        login_required = account.get("login_required") is True or authenticated is False
        if authenticated is not None or connection is not None or login_required:
            payload = {"login_required": bool(login_required)}
            if authenticated is not None:
                payload["authenticated"] = authenticated is True
            if isinstance(connection, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,31}", connection):
                payload["connection"] = connection
            result["account"] = payload
        for key in ("last_receive_at", "last_send_at"):
            value = snapshot.get(key)
            if isinstance(value, str) and re.fullmatch(r"[0-9T:.+Z-]{20,40}", value):
                result[key] = value
            elif type(value) in (int, float) and 0 <= value <= 2 ** 31:
                result[key] = float(value)
        events = snapshot.get("stale_events")
        if isinstance(events, list):
            cleaned = []
            for entry in events[:50]:
                if not isinstance(entry, dict) or not isinstance(entry.get("kind"), str):
                    continue
                kind = _safe_code(entry["kind"])
                if not kind:
                    continue
                item = {"kind": kind}
                for field, limit in (("request_id", 200), ("at", 40), ("detail", 40), ("thread_id", 128)):
                    value = entry.get(field)
                    if isinstance(value, str) and value and len(value) <= limit:
                        item[field] = value
                if type(entry.get("age_seconds")) is int and 0 <= entry["age_seconds"] <= 86400 * 30:
                    item["age_seconds"] = entry["age_seconds"]
                cleaned.append(item)
            if cleaned:
                result["stale_events"] = cleaned
        for name in ("receive", "business", "projection", "send", "status"):
            raw = snapshot.get("lanes", {}).get(name, {})
            lane = {"state": raw.get("state") if raw.get("state") in ("ready", "attention") else "starting"}
            if raw.get("error"):
                lane["error"] = _safe_code(raw["error"])
            counters = {key: value for key, value in raw.get("result", {}).items()
                        if key in ("received_events", "new_requests", "admitted", "generating", "projected", "errors", "processed")
                        and type(value) is int and 0 <= value <= 2 ** 63 - 1}
            if counters:
                lane["counters"] = counters
            delivery = raw.get("result", {}).get("state")
            if delivery in ("queued", "submitting", "accepted", "rejected", "unknown"):
                lane["delivery_state"] = delivery
            if raw.get("result", {}).get("blocked") is True:
                lane["blocked"] = True
            history = snapshot.get("lane_history", {}).get(name, {})
            for key in ("last_seen_at", "last_failure_at"):
                value = history.get(key)
                if isinstance(value, str) and re.fullmatch(r"[0-9T:.+Z-]{20,40}", value):
                    lane[key] = value
            if history.get("last_error"):
                lane["last_error"] = _safe_code(history["last_error"])
            result["lanes"][name] = lane
        return result

    def write(self, snapshot):
        document = self.document(snapshot)
        with self._lock:
            self._validate_existing()
            descriptor, temporary = tempfile.mkstemp(prefix=".protocol-status-", dir=self.path.parent)
            try:
                with os.fdopen(descriptor, "w") as stream:
                    json.dump(document, stream, sort_keys=True, ensure_ascii=True)
                    stream.write("\n")
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, self.path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)


def _read_db(path, statement, parameters=()):
    # Even mode=ro can create source -wal/-shm files. Inspect a private, stable
    # DB+WAL copy instead; immutable=1 would incorrectly ignore committed WAL.
    # Concurrent checkpoint/write changes cause a bounded retry, never a write
    # or checkpoint of the operator's source database.
    sources = (path, Path(str(path) + "-wal"))
    def stamps():
        result = []
        for source in sources:
            if source.is_symlink():
                raise RuntimeErrorCode("protocol_runtime_symlink_path")
            try:
                info = source.stat()
                result.append((info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
            except FileNotFoundError:
                result.append(None)
        return result
    for _ in range(3):
        before = stamps()
        with tempfile.TemporaryDirectory(prefix="wekeeper-protocol-check-") as directory:
            snapshot = Path(directory) / "state.sqlite"
            try:
                for source, stamp, target in zip(sources, before, (snapshot, Path(str(snapshot) + "-wal"))):
                    if stamp is not None:
                        shutil.copyfile(source, target)
                        target.chmod(0o600)
            except FileNotFoundError:
                continue
            if before != stamps():
                continue
            connection = sqlite3.connect("file:" + quote(str(snapshot), safe="/") + "?mode=ro", uri=True)
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA query_only=ON")
                return connection.execute(statement, parameters).fetchall()
            finally:
                connection.close()
    raise RuntimeErrorCode("protocol_runtime_state_changing")


def check_configuration(config):
    """Read explicit local configuration; no gateway/model/provider construction."""
    state = _private_path(config.state_dir, directory=True)
    _private_path(config.usage_dir, directory=True)
    dashboard = Path(config.dashboard_dir) if config.dashboard_dir is not None else state / "dashboard"
    _private_path(dashboard, directory=True, required=False)
    database = _private_path(state / "bot" / "bot.sqlite")
    files = [_private_path(path, required=False) for path in
             (config.inbox_path, config.outbox_path, config.journal_path)]
    token_path, bindings_path = (_private_path(config.token_file), _private_path(config.bindings_file))
    if len(set([database, token_path, bindings_path, *files])) != 6:
        raise RuntimeErrorCode("protocol_runtime_path_collision")
    RuntimeStatusFile.for_config(config)  # Validate only; checking never writes.
    _identifier(config.account_id, maximum=256)
    _identifier(config.own_username, maximum=256)
    endpoint = _base_url(config.server)
    if (not isinstance(config.consumer, str) or not re.fullmatch(r"[A-Za-z0-9._-]{1,128}", config.consumer)
            or type(config.activate) is not bool or type(config.gateway_timeout) not in (int, float)
            or not math.isfinite(config.gateway_timeout) or not 0 < config.gateway_timeout <= 60):
        raise RuntimeErrorCode("protocol_runtime_invalid_options")
    if token_path.stat().st_size > 8192:
        raise RuntimeErrorCode("protocol_runtime_invalid_token")
    token = token_path.read_text().strip()
    if not token or any(not 33 <= ord(char) <= 126 for char in token):
        raise RuntimeErrorCode("protocol_runtime_invalid_token")
    bindings = _read_json(bindings_path)
    if (not isinstance(bindings, dict) or not bindings
            or not all(isinstance(value, str) for value in bindings.values())
            or len(set(bindings.values())) != len(bindings)):
        raise RuntimeErrorCode("protocol_runtime_invalid_bindings")
    for thread, group in bindings.items():
        _identifier(thread, maximum=256)
        _identifier(group, maximum=256)
    metadata = {row["key"]: json.loads(row["value"]) for row in _read_db(database,
        "SELECT key,value FROM meta WHERE key IN ('config','bot_wxid','cloud_control_bootstrap') OR key LIKE 'verified_profile_%'")}
    if config.cloud_control:
        if config.archive_state is None:
            raise RuntimeErrorCode("protocol_runtime_archive_state_required")
        archive = _private_path(config.archive_state, directory=True)
        _private_path(archive / "archive" / "archive.sqlite")
        local = _private_path(dashboard / "sync.sqlite")
        rows = _read_db(local, "SELECT value FROM meta WHERE key='source_id'")
        marker = metadata.get("cloud_control_bootstrap") or {}
        if not rows or marker.get("source_id") != json.loads(rows[0][0]):
            raise RuntimeErrorCode("bot_cloud_bootstrap_required")
    value = normalize_config(metadata.get("config"))
    if not value.get("enabled") and not config.cloud_control:
        raise RuntimeErrorCode("protocol_runtime_bot_disabled")
    profiles = {}
    for group in bindings.values():
        binding = BotStore._binding(value, group)
        if not binding or not binding.get("profile") or (not binding["enabled"] and not config.cloud_control):
            raise RuntimeErrorCode("protocol_runtime_binding_unavailable")
        bot = binding.get("bot_wxid") or metadata.get("bot_wxid")
        if bot is not None and bot != config.own_username:
            raise RuntimeErrorCode("protocol_runtime_bot_identity_changed")
        if not binding["enabled"]:
            continue  # A bootstrapped control connection can receive re-enable.
        profile = binding["profile"]
        profiles[profile["id"]] = profile
        if profile["kind"] == "siwc":
            _private_path(dashboard / "siwc" / "credentials.json")
        else:
            from .providers import credential_path
            _private_path(credential_path(state, profile.get("credential_ref")))
            if metadata.get("verified_profile_" + profile["id"]) != profile_signature(profile):
                raise RuntimeErrorCode("protocol_runtime_provider_unverified")
    activated = False
    inbox, outbox, journal = files
    if inbox.exists():
        rows = _read_db(inbox, "SELECT * FROM inbox_meta")
        if len(rows) != 1 or tuple(rows[0][key] for key in ("endpoint", "account_id", "own_username", "consumer")) != (
                endpoint, config.account_id, config.own_username, config.consumer):
            raise RuntimeErrorCode("protocol_runtime_inbox_binding_changed")
        tables = _read_db(inbox, "SELECT name FROM sqlite_master WHERE type='table' AND name='native_checkpoint'")
        activated = bool(tables and _read_db(inbox, "SELECT checkpoint_json FROM native_checkpoint"))
        if rows[0]["cutover_cursor"] is not None and not activated:
            raise RuntimeErrorCode("protocol_runtime_checkpoint_missing")
    if outbox.exists():
        rows = _read_db(outbox, "SELECT * FROM outbox_meta")
        if len(rows) != 1 or (rows[0]["account_id"], rows[0]["own_username"]) != (config.account_id, config.own_username):
            raise RuntimeErrorCode("protocol_runtime_outbox_binding_changed")
        if rows[0]["endpoint"] not in (None, endpoint):
            raise RuntimeErrorCode("protocol_runtime_gateway_changed")
    if journal.exists():
        rows = _read_db(journal, "SELECT value FROM gateway_meta WHERE key='endpoint'")
        if len(rows) != 1 or rows[0][0] != endpoint:
            raise RuntimeErrorCode("protocol_runtime_gateway_changed")
        rows = _read_db(journal, "SELECT own_username FROM gateway_accounts WHERE account_id=?", (config.account_id,))
        if rows and rows[0][0] != config.own_username:
            raise RuntimeErrorCode("protocol_runtime_account_changed")
    return {"mode": "check", "local_configuration_valid": True, "activated": activated,
            "activation_required": not activated, "bindings": bindings, "profile_ids": sorted(profiles),
            "gateway_contacted": False, "phone_received": False}


class ProtocolRuntime:
    """Five isolated lanes. No gateway HTTP runs while holding BotStore.lock."""
    def __init__(self, *, inbox, business, outbox, receive_gateway, send_gateway, status_gateway,
                 interval=0.05, status_interval=1.0, close_callback=None, status_file=None):
        if any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0
               for value in (interval, status_interval)):
            raise ValueError("invalid_protocol_runtime_interval")
        if len({id(receive_gateway), id(send_gateway), id(status_gateway)}) != 3:
            raise RuntimeErrorCode("protocol_runtime_separate_gateways_required")
        self.inbox, self.business, self.outbox = inbox, business, outbox
        self.receive_gateway, self.send_gateway, self.status_gateway = receive_gateway, send_gateway, status_gateway
        self.interval, self.status_interval = interval, status_interval
        self._stop, self._lock = threading.Event(), threading.Lock()
        self._lifecycle_lock = threading.RLock()
        self._threads, self._state = [], {}
        self._started, self._closed = False, False
        self._close_callback = close_callback or business.close
        self._observer = RuntimeStatusFile(status_file) if status_file is not None else None
        self._lifecycle, self._failure, self._lane_history = "starting", None, {}
        self._observer_error = None
        self._account = {"authenticated": None, "connection": None, "login_required": False}
        self._last_receive_at = None
        self._last_send_at = None
        self._account_checked_at = 0.0
        self.control = None
        if hasattr(business, "store") and hasattr(outbox, "path"):
            business.store.set("protocol_outbox_file", str(outbox.path))
        if status_file is not None and hasattr(business, "store"):
            try:
                business.store.set("protocol_status_file", str(Path(status_file).expanduser().absolute()))
            except Exception:
                pass

    def _persist(self):
        if self._observer is not None:
            self._observer.write(self.status())

    def record_failure(self, error):
        with self._lock:
            self._lifecycle, self._failure = "failed", error_code(error)
        self._persist()

    def _refresh_account(self, *, force=False):
        now = time.monotonic()
        interval = self._observer.HEARTBEAT if self._observer is not None else 10.0
        with self._lock:
            due = force or (now - self._account_checked_at) >= interval
        if not due:
            return
        try:
            status = self.receive_gateway.account_status()
            authenticated = status.get("authenticated") is True
            account = {
                "authenticated": authenticated,
                "connection": status.get("connection"),
                "login_required": not authenticated,
            }
        except Exception as error:
            code = error_code(error)
            account = {
                "authenticated": False if code == "auth_required" else None,
                "connection": "disconnected" if code == "auth_required" else None,
                "login_required": code == "auth_required",
                "error": code,
            }
        with self._lock:
            self._account = account
            self._account_checked_at = now
        if hasattr(self.business, "store"):
            try:
                runtime = dict(self.business.store.get("runtime") or {})
                runtime.update({
                    "authenticated": account.get("authenticated"),
                    "connection": account.get("connection"),
                    "login_required": bool(account.get("login_required")),
                    "last_receive_at": self._last_receive_at_unix(),
                    "last_send_at": self._last_send_at_unix(),
                    "heartbeat_at": time.time(),
                    "enabled": True,
                })
                if account.get("login_required"):
                    runtime["state"] = "login_required"
                    runtime["error"] = "auth_required"
                elif runtime.get("state") == "login_required":
                    runtime["state"] = "ready"
                    if runtime.get("error") == "auth_required":
                        runtime["error"] = None
                events = getattr(self.business, "stale_events", None)
                if events is not None:
                    runtime["stale_events"] = list(events)[:50]
                self.business.store.set("runtime", runtime)
            except Exception:
                pass

    def _last_receive_at_unix(self):
        with self._lock:
            value = self._last_receive_at
        return self._iso_to_unix(value)

    def _last_send_at_unix(self):
        with self._lock:
            value = self._last_send_at
        return self._iso_to_unix(value)

    @staticmethod
    def _iso_to_unix(value):
        if not isinstance(value, str) or not value:
            return None
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed.timestamp()

    def _observe(self):
        last_health, last_write = None, time.monotonic()
        while not self._stop.wait(self._observer.MIN_INTERVAL):
            self._refresh_account()
            snapshot = self.status()
            # Timestamps are refreshed on traffic; heartbeat covers those writes.
            health = (snapshot["state"], snapshot.get("error"),
                      snapshot.get("account", {}).get("authenticated"),
                      snapshot.get("account", {}).get("login_required"),
                      snapshot.get("account", {}).get("connection"),
                      bool(snapshot.get("stale_events")),
                      tuple((name, lane.get("state"), lane.get("error"),
                             snapshot["lane_history"].get(name, {}).get("last_error"))
                            for name, lane in sorted(snapshot["lanes"].items())))
            if health != last_health or time.monotonic() - last_write >= self._observer.HEARTBEAT:
                try:
                    self._observer.write(snapshot)
                except Exception:
                    # Retain observability in-process and retry at a bounded
                    # rate. Raw filesystem errors can contain private paths.
                    with self._lock:
                        self._observer_error = "protocol_runtime_status_write_failed"
                else:
                    with self._lock:
                        self._observer_error = None
                    last_health, last_write = health, time.monotonic()

    def _receive(self):
        batch = self.inbox.poll(limit=100)
        return {"received_events": batch.received_events, "new_requests": batch.new_requests}

    def _business(self):
        return self.business.run_once(block=False)

    def _project(self):
        result = self.business.project_outbox()
        return {"projected": result["projected"], "errors": len(result["errors"])}

    def _send(self):
        rows = self.outbox.pending(limit=1)
        if not rows:
            return {"processed": 0}
        reason = self.business.delivery_block_reason(rows[0])
        if reason is not None:
            result = self.outbox.block(rows[0]["reply_id"], reason)
            return {"processed": 1, "state": result["state"], "blocked": True}
        result = self.outbox.deliver_one(self.send_gateway, rows[0]["reply_id"])
        return {"processed": 1, "state": result["state"]}

    def _status(self):
        rows = self.outbox.awaiting_status(limit=1)
        if not rows:
            return {"processed": 0}
        result = self.outbox.reconcile(self.status_gateway, rows[0]["reply_id"])
        return {"processed": 1, "state": result["state"]}

    def _loop(self, name, operation, interval):
        while not self._stop.is_set():
            failed = False
            try:
                result = {"state": "ready", "result": operation()}
                if name == "projection" and result["result"].get("errors", 0):
                    failed = True
                    result.update(state="attention", error="protocol_outbox_projection_failed")
            except Exception as error:
                failed = True
                result = {"state": "attention", "error": error_code(error)}
            with self._lock:
                self._state[name] = result
                history = self._lane_history.setdefault(name, {})
                history["last_seen_at"] = _timestamp()
                if failed:
                    history.update(last_failure_at=history["last_seen_at"], last_error=result["error"])
                payload = result.get("result") or {}
                if name == "receive" and not failed and int(payload.get("received_events") or 0) > 0:
                    self._last_receive_at = history["last_seen_at"]
                if name == "send" and not failed and payload.get("state") == "accepted":
                    self._last_send_at = history["last_seen_at"]
            self._stop.wait(max(interval, 1.0) if failed else interval)

    def _cloud_control(self):
        """Cloud HTTP/model verification has its own lane, never a delivery lock."""
        last_snapshot = 0.0
        while not self._stop.is_set():
            try:
                self.control.poll()
                self.control.upload_usage()
                if time.monotonic() - last_snapshot >= 10:
                    self.control.snapshot()
                    last_snapshot = time.monotonic()
                self.business.store.set("control_error", None)
            except Exception as error:
                if not self._stop.is_set():
                    self.business.store.set("control_error", error_code(error))
            self._stop.wait(2)

    def start(self):
        with self._lifecycle_lock:
            with self._lock:
                if self._closed or self._started:
                    raise RuntimeErrorCode("protocol_runtime_already_started")
                self._started = True
            operations = (("receive", self._receive), ("business", self._business),
                          ("projection", self._project), ("send", self._send), ("status", self._status))
            candidates = [threading.Thread(target=self._loop,
                args=(name, operation, self.status_interval if name == "status" else self.interval),
                name="protocol-" + name, daemon=False) for name, operation in operations]
            try:
                self._refresh_account(force=True)
                self._persist()
                for thread in candidates:
                    thread.start()
                    self._threads.append(thread)
                with self._lock:
                    self._lifecycle = "running"
                self._persist()
                if self._observer is not None:
                    observer = threading.Thread(target=self._observe, name="protocol-observer", daemon=False)
                    observer.start()
                    self._threads.append(observer)
                if self.control is not None:
                    control = threading.Thread(target=self._cloud_control, name="protocol-cloud-control", daemon=False)
                    control.start()
                    self._threads.append(control)
            except BaseException as error:
                # Only successfully started threads can be joined. Partial
                # startup still drains/closes all owned resources.
                try:
                    self.record_failure(error)
                finally:
                    self.close()
                raise

    def status(self):
        with self._lock:
            state = self._lifecycle
            account = copy.deepcopy(self._account)
            if state == "running" and (self._observer_error or account.get("login_required")
                    or any(lane.get("state") == "attention" for lane in self._state.values())):
                state = "attention"
            result = {"state": state, "lanes": copy.deepcopy(self._state),
                      "lane_history": copy.deepcopy(self._lane_history), "phone_received": False,
                      "account": account}
            if self._last_receive_at:
                result["last_receive_at"] = self._last_receive_at
            if self._last_send_at:
                result["last_send_at"] = self._last_send_at
            events = getattr(self.business, "stale_events", None)
            if events:
                result["stale_events"] = list(events)[:50]
            if self._failure or self._observer_error:
                result["error"] = self._failure or self._observer_error
            elif account.get("login_required"):
                result["error"] = "auth_required"
            return result

    def request_stop(self):
        self._stop.set()

    def run_forever(self):
        self.start()
        try:
            while not self._stop.wait(0.5):
                pass
        finally:
            self.close()

    def close(self):
        with self._lifecycle_lock:
            with self._lock:
                if self._closed:
                    return
                if self._lifecycle != "failed":
                    self._lifecycle = "stopping"
            self.request_stop()
            failures = []
            try:
                self._persist()
            except BaseException as error:
                failures.append(error)
            for thread in self._threads:
                thread.join()
            try:
                self._close_callback()
            except BaseException as error:
                failures.append(error)
            with self._lock:
                self._closed = True
                if failures:
                    self._lifecycle, self._failure = "failed", error_code(failures[0])
                elif self._lifecycle != "failed":
                    self._lifecycle = "stopped"
            try:
                self._persist()
            except BaseException as error:
                failures.append(error)
            if failures:
                raise failures[0]


def open_runtime(config):
    """Construct only after explicit --run; never choose or migrate any state."""
    checked = check_configuration(config)
    if checked["activation_required"] and not config.activate:
        raise RuntimeErrorCode("protocol_runtime_activation_required")
    if checked["activated"] and config.activate:
        raise RuntimeErrorCode("protocol_runtime_already_activated")
    from .protocol_business import ProtocolBusinessWorker
    from .protocol_history import ProtocolHistoryStore
    from .protocol_native_inbox import ProtocolNativeInbox
    from .protocol_outbox import ProtocolOutbox
    from .reply_generator import ReplyGenerator
    from .memory_host import MemoryBusinessMCPHost

    stack = ExitStack()
    try:
        store = BotStore(config.state_dir)
        stack.callback(store.close)
        resources = ProviderResources(store, state_dir=config.state_dir, usage_dir=config.usage_dir,
                                      dashboard_dir=config.dashboard_dir or Path(config.state_dir) / "dashboard")
        stack.callback(resources.close)
        token = Path(config.token_file).read_text().strip()
        gateways = []
        for _ in range(3):
            gateway = ProtocolGateway(token=token, account_id=config.account_id, journal_path=config.journal_path,
                                      base_url=config.server, timeout=config.gateway_timeout)
            stack.callback(gateway.close)
            gateways.append(gateway)
        inbox = ProtocolNativeInbox(gateways[0], path=config.inbox_path,
                                    own_username=config.own_username, consumer=config.consumer)
        stack.callback(inbox.close)
        if config.activate:
            inbox.activate()
        outbox = ProtocolOutbox(config.outbox_path, account_id=config.account_id, own_username=config.own_username)
        stack.callback(outbox.close)
        history = ProtocolHistoryStore(store, outbox, own_username=config.own_username, account_id=config.account_id)
        generator = ReplyGenerator(history, state_dir=config.state_dir, provider_factory=resources.provider,
                                   local_execution_factory=resources.local_business_execution,
                                   host_factory=MemoryBusinessMCPHost)
        business = ProtocolBusinessWorker(inbox=inbox, store=store, generator=generator, outbox=outbox,
            bindings=checked["bindings"], own_username=config.own_username, account_id=config.account_id, concurrency=2,
            usage_ledger=resources.usage_ledger())
        stack.callback(business.close)
        runtime = ProtocolRuntime(inbox=inbox, business=business, outbox=outbox,
            receive_gateway=gateways[0], send_gateway=gateways[1], status_gateway=gateways[2], close_callback=stack.close,
            status_file=config.status_file)
        if config.cloud_control:
            from .control import ControlClient
            control = ControlClient(store=store, state_dir=config.state_dir, archive_state=config.archive_state,
                dashboard_dir=resources.dashboard_dir, usage_dir=config.usage_dir, provider_factory=resources.provider,
                validation_factory=resources.validation_provider, runtime=runtime)
            stack.callback(control.close)
            control.require_bootstrap()
            runtime.control = control
        return runtime
    except BaseException:
        stack.close()
        raise
