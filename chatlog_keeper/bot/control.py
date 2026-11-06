"""Outbound-only dashboard control and safe, credential-free bot snapshots."""
from __future__ import annotations

import json
import hashlib
import re
from pathlib import Path
import time
import uuid

import httpx

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.dashboard.avatars import sender_reference
from chatlog_keeper.dashboard.local_state import LocalState
from chatlog_keeper.dashboard.sync import read_config
from .errors import safe_error
from .store import normalize_config, parse_time
from .providers import profile_signature


_CLOUD_LATENCY_PRIORITY = (
    "visible_reply", "chat_visible_reply", "business_visible_reply",
    "generation", "chat_generation", "business_generation",
    "trigger_to_model", "chat_trigger_to_model", "business_trigger_to_model",
    "ui_total", "body_ready_to_click", "send_confirmation",
    "end_to_end", "chat_end_to_end", "business_end_to_end", "fallback_delivery", "failed_generation",
    "archive_to_model_start", "archive_commit_to_model_start", "native_hint_to_model_start", "mirror_to_model_start",
    "model_call", "model_call_1", "model_call_2", "model_call_3", "model_call_4",
    "mcp", "mcp_init", "mcp_tool", "mcp_shutdown",
    "usage_start", "usage_record_call", "usage_finish", "usage_finish_with_call", "provider_init", "provider_close",
    "failed_trigger_to_model", "failed_model_call", "failed_mcp", "failed_mcp_init", "failed_mcp_tool", "failed_mcp_shutdown",
)


def _cloud_latency_metrics(metrics):
    """Bound only the outgoing summary; complete local metrics stay intact."""
    if len(metrics) <= 64:
        return dict(metrics)
    priority = [key for key in _CLOUD_LATENCY_PRIORITY if key in metrics]
    keys = priority + sorted(set(metrics) - set(priority))
    return {key: metrics[key] for key in keys[:64]}


def _timestamp(value):
    return parse_time(value).timestamp() if value else 0


class ControlClient:
    def __init__(self, *, store, state_dir, archive_state, dashboard_dir,
                 usage_dir=None, provider_factory=None, validation_factory=None, runtime=None, client=None):
        self.store = store
        self.state_dir, self.archive_state, self.dashboard_dir = map(Path, (state_dir, archive_state, dashboard_dir))
        self.provider_factory = provider_factory
        self.validation_factory = validation_factory
        self.usage_dir = Path(usage_dir) if usage_dir is not None else self.dashboard_dir
        self.runtime = runtime
        self.local = LocalState(self.dashboard_dir)
        self.http = client or httpx.Client(timeout=3, follow_redirects=False)
        self.owned = client is None

    def require_bootstrap(self):
        marker = self.store.get("cloud_control_bootstrap") or {}
        if marker.get("source_id") != self.local.source_id:
            raise ValueError("bot_cloud_bootstrap_required")

    def stopping(self):
        stop = getattr(self.runtime, "_stop", None)
        return stop is not None and stop.is_set()

    def bootstrap(self, alignment_id):
        """Upload local configuration without changing local permissions/models."""
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", alignment_id):
            raise ValueError("bot_alignment_id_invalid")
        from chatlog_keeper.dashboard.bot_control import BotConfig
        config = json.loads(json.dumps(self.store.config()))
        config.pop("revision", None)
        provider_keys = {"id", "name", "kind", "model", "base_url", "credential_ref", "reasoning_effort"}
        config["providers"] = [{**{key: val for key, val in profile.items() if key in provider_keys},
                                "name": profile.get("name") or profile["id"]} for profile in config["providers"]]
        with ArchiveStore(self.archive_state, readonly=True) as archive:
            groups = {r["group_ref"]: dict(r) for r in archive.conn.execute("SELECT * FROM groups")}
        selected = set(read_config(self.dashboard_dir / "config.json")["groups"])
        if any(g["group_ref"] not in groups or g["group_ref"] not in selected for g in config["groups"]):
            raise ValueError("bot_group_not_in_dashboard_allowlist")
        for group in config["groups"]:
            group.pop("bot_wxid", None)
        for business in config["businesses"]:
            accounts = {groups[g["group_ref"]]["account_id"] for g in config["groups"] if g["business_id"] == business["id"]}
            if business["reporter_ids"] and len(accounts) != 1:
                raise ValueError("bot_business_requires_single_account")
            business["reporter_ids"] = [sender_reference(self.local.get("sender_salt"), next(iter(accounts)), native)
                                        for native in business["reporter_ids"]]
        data = BotConfig.model_validate(config).model_dump(mode="json")
        result = self._post("bootstrap", {"alignment_id": alignment_id, "data": data})
        if result.get("accepted") is not True or type(result.get("config_revision")) is not int:
            raise ValueError("bot_cloud_bootstrap_invalid_response")
        previous = self.store.get("cloud_control_bootstrap") or {}
        revision = max(self.store.get("cloud_config_revision", 0), result["config_revision"]) if previous.get("source_id") == self.local.source_id else result["config_revision"]
        self.store.set_many({"cloud_config_revision": revision,
                             "cloud_control_bootstrap": {"source_id": self.local.source_id, "alignment_id": alignment_id}})
        return {"accepted": True, "config_revision": result["config_revision"], "cloud_control_enabled": False}

    def _post(self, route, value):
        return self._send("/api/machine/bot/" + route, {"source_id": self.local.source_id, **value})

    def _send(self, route, value):
        if self.stopping():
            raise ValueError("bot_control_stopping")
        config = read_config(self.dashboard_dir / "config.json")
        response = self.http.post(config["server_url"] + route,
            headers={"Authorization": "Bearer " + config["machine_token"]},
            json=value)
        response.raise_for_status()
        return response.json()

    def upload_usage(self):
        if self.stopping():
            return 0
        from chatlog_keeper.dashboard.usage_sync import upload_usage
        return upload_usage(self.usage_dir, state=self.local, post=self._send)

    def _check_status_scope(self, business_id):
        if business_id not in {g["business_id"] for g in self.store.config()["groups"]}:
            raise ValueError("bot_business_scope_violation")

    def _native_config(self, remote):
        # Resolve the dashboard's opaque member references on the owning machine.
        value = json.loads(json.dumps(remote))
        if self.runtime is not None and value.get("enabled") and any(group.get("enabled") and group["group_ref"] not in self.runtime.business.bindings.values()
                                            for group in value.get("groups", [])):
            raise ValueError("bot_group_not_in_protocol_bindings")
        with ArchiveStore(self.archive_state, readonly=True) as archive:
            groups = {r["group_ref"]: dict(r) for r in archive.conn.execute("SELECT * FROM groups")}
            selected = set(read_config(self.dashboard_dir / "config.json")["groups"])
            if any(g["group_ref"] not in groups or g["group_ref"] not in selected for g in value.get("groups", [])):
                raise ValueError("bot_group_not_in_dashboard_allowlist")
            identities = {}
            for row in archive.conn.execute("SELECT DISTINCT account_id,sender_id FROM messages WHERE sender_id<>''"):
                identities[sender_reference(self.local.get("sender_salt"), row["account_id"], row["sender_id"])] = (row["account_id"], row["sender_id"])
            for business in value.get("businesses", []):
                accounts = {groups[g["group_ref"]]["account_id"] for g in value.get("groups", []) if g["business_id"] == business["id"]}
                if len(accounts) > 1:
                    raise ValueError("bot_business_requires_single_account")
                native = []
                for reference in business.get("reporter_ids", []):
                    if reference not in identities or (accounts and identities[reference][0] not in accounts):
                        raise ValueError("bot_reporter_identity_unresolved")
                    native.append(identities[reference][1])
                business["reporter_ids"] = native
        return value

    def _verify_profiles(self, config):
        """Prove a candidate model's tool protocol before activating its config.

        This probe has no BusinessMCPHost or event identity. Its only callable
        is a synthetic, read-only list_offer_items returning an empty list.
        A candidate can never acquire the sender/group's write permissions.
        """
        verified = {}
        active_profiles = {group["provider_id"] for group in config["groups"] if group.get("enabled")} if config.get("enabled") else set()
        for profile in config["providers"]:
            if self.stopping():
                raise ValueError("bot_control_stopping")
            if profile["id"] not in active_profiles:
                continue
            signature = profile_signature(profile)
            if self.store.get("verified_profile_" + profile["id"]) == signature:
                continue
            factory = self.validation_factory or self.provider_factory
            if not callable(factory):
                raise ValueError("bot_provider_validation_unavailable")
            provider = factory(profile)
            called = []
            def probe(name, arguments):
                if name != "list_offer_items" or not isinstance(arguments, dict) or arguments:
                    raise ValueError("bot_provider_tool_validation_failed")
                called.append(True)
                return {"ok": True, "items": []}
            group = next((g["group_ref"] for g in config["groups"] if g.get("provider_id") == profile["id"]),
                         config["groups"][0]["group_ref"] if config["groups"] else "provider_selftest")
            try:
                text = provider.complete(
                    [{"role": "user", "content": "请调用list_offer_items一次，再简短回复验证完成。"}],
                    "本轮是工具能力自检，先调用提供的只读工具，再回复；不要编造工具结果。",
                    [{"name": "list_offer_items", "description": "只读工具自检",
                      "parameters": {"type": "object", "properties": {}, "additionalProperties": False}}],
                    probe,
                    {"run_id": "bot_verify_" + uuid.uuid4().hex, "provider": profile["kind"],
                     "profile_id": profile["id"], "group_ref": group,
                     "bot_request_id": "verify_" + uuid.uuid4().hex, "_timeout": 60})
                if not called or not isinstance(text, str) or not text.strip():
                    raise ValueError("bot_provider_tool_validation_failed")
                verified[profile["id"]] = signature
            finally:
                provider.close()
        return verified

    def poll(self):
        self.require_bootstrap()
        if not (self.dashboard_dir / "config.json").is_file():
            return False
        response = self._post("poll", {})
        for command in response.get("commands", []):
            if self.stopping():
                return False
            key = "control_" + command["id"]
            outcome = self.store.get(key)
            if outcome is None:
                try:
                    if command["kind"] == "apply_config":
                        config = normalize_config(self._native_config(command["payload"]["config"]))
                        revision = command.get("revision", command["payload"]["revision"])
                        last = self.store.get("cloud_config_revision", 0)
                        if revision <= last:
                            raise ValueError("bot_configuration_revision_stale")
                        verified = self._verify_profiles(config)
                        if self.stopping():
                            return False
                        with self.store.lock:
                            self.store.configure(config, control={"command_id": command["id"], "revision": revision})
                            for profile_id, signature in verified.items():
                                self.store.set("verified_profile_" + profile_id, signature)
                        outcome = {"status": "applied", "config_revision": revision}
                    elif command["kind"] == "set_status":
                        payload = dict(command["payload"], command_id=command["id"], event_id="admin_" + command["id"])
                        self._check_status_scope(payload["business_id"])
                        if self.stopping():
                            return False
                        result = self.store.adjust_offer(payload)
                        if not result.get("ok"):
                            raise ValueError(result.get("error", "bot_adjustment_failed"))
                        outcome = {"status": "applied"}
                    else:
                        raise ValueError("bot_unknown_control_command")
                except Exception as exc:
                    if self.stopping():
                        return False
                    outcome = {"status": "failed", "error": safe_error(exc)}
                self.store.set(key, outcome)
            self._post("ack", {"command_id": command["id"], **outcome})
        return True

    def _actor(self, native, group_ref, groups):
        if not native:
            return None
        if str(native).startswith("dashboard:"):
            return native
        account = groups.get(group_ref, {}).get("account_id")
        return sender_reference(self.local.get("sender_salt"), account, native) if account else None

    def snapshot(self):
        if not (self.dashboard_dir / "config.json").is_file():
            return False
        snapshot = self.store.snapshot()
        with ArchiveStore(self.archive_state, readonly=True) as archive:
            groups = {row["group_ref"]: dict(row) for row in archive.conn.execute("SELECT * FROM groups")}
        statuses = []
        for row in snapshot.get("offers", []):
            admin = str(row.get("sender_id", "")).startswith("dashboard:")
            statuses.append({"business_id": row["business_id"], "item_id": row["item_id"], "day": row["day"],
                "status": row["status"], "revision": row.get("version", row.get("revision", 0)),
                "updated_at": _timestamp(row.get("updated_at")), "message_ref": None if admin or str(row.get("message_ref", "")).startswith("native:") else row.get("message_ref"),
                "group_ref": row.get("group_ref") or None, "actor": self._actor(row.get("sender_id"), row.get("group_ref"), groups)})
        audit = []
        for row in snapshot.get("audit", []):
            payload = row.get("payload", {})
            if not all(row.get(key) for key in ("business_id", "item_id", "day")):
                continue
            result = payload.get("result") or payload
            admin = str(result.get("sender_id", "")).startswith("dashboard:")
            status = result.get("status")
            if status not in {"available", "unavailable", "unknown"}:
                continue
            audit.append({"id": str(row["id"]), "business_id": row["business_id"], "item_id": row["item_id"],
                "day": row["day"], "status": status, "revision": result.get("version", result.get("revision", 0)),
                "created_at": _timestamp(row["created_at"]), "updated_at": _timestamp(row["created_at"]),
                "message_ref": None if admin or str(result.get("message_ref", "")).startswith("native:") else result.get("message_ref"), "group_ref": result.get("group_ref") or None,
                "actor": self._actor(result.get("sender_id"), result.get("group_ref"), groups),
                "previous_status": (payload.get("before") or {}).get("status")})
        with self.store.lock:
            runtime = dict(self.store.get("runtime", {}))
            for key in ("active", "last_request", "latency_metrics"):
                value = self.store.get(key)
                if value is not None:
                    runtime[key] = value
            if isinstance(runtime.get("latency_metrics"), dict):
                runtime["latency_metrics"] = _cloud_latency_metrics(runtime["latency_metrics"])
            replies = dict(self.store.conn.execute("SELECT status,COUNT(*) FROM reply_outbox WHERE kind='final' GROUP BY status"))
            runtime["counts"] = {
                "requests": self.store.conn.execute("SELECT COUNT(*) FROM requests").fetchone()[0],
                # A delivered failure receipt is still an outbox `sent` record
                # (and must never be replayed), but it is not a successful LLM
                # answer. Request outcome and delivery outcome are distinct.
                "replied": self.store.conn.execute("""SELECT COUNT(*) FROM requests r
                    JOIN reply_outbox o USING(event_id)
                    WHERE o.kind='final' AND o.status='sent' AND r.status='completed'""").fetchone()[0],
                "pending": sum(replies.get(status, 0) for status in ("awaiting_generation", "pending", "sending")),
                "failed": self.store.conn.execute("""SELECT COUNT(*) FROM requests r
                    JOIN reply_outbox o USING(event_id)
                    WHERE o.kind='final' AND (r.status='failed' OR o.status='failed')""").fetchone()[0],
                "unknown": replies.get("unknown", 0),
            }
        providers = []
        from .providers import credential_path, profile_signature
        for profile in self.store.config()["providers"]:
            if profile.get("kind") == "siwc":
                configured = (self.dashboard_dir / "siwc" / "credentials.json").is_file()
            else:
                try:
                    configured = credential_path(self.state_dir, profile.get("credential_ref")).is_file()
                except ValueError:
                    configured = False
                except RuntimeError:
                    configured = False
            verified = self.store.get("verified_profile_" + profile["id"]) == profile_signature(profile)
            providers.append({"id": profile["id"], "configured": configured,
                              "state": "configured" if configured and verified else "tools_unverified" if configured else "not_configured"})
        runtime_keys = {"state", "heartbeat_at", "last_message_at", "last_receive_at", "last_send_at",
                        "enabled", "queue_depth", "error", "authenticated", "connection", "login_required",
                        "active", "last_request", "counts", "latency_metrics", "stale_events"}
        runtime = self._merge_protocol_status(runtime)
        if self.runtime is not None:
            runtime.update(self._protocol_snapshot())
        self._post("snapshot", {"config_revision": self.store.get("cloud_config_revision", 0),
            "statuses": statuses, "audit": audit,
            "runtime": {k: v for k, v in runtime.items() if k in runtime_keys},
            "capabilities": {"quote_reply": self.runtime is not None, "native_ready": self.runtime is not None and runtime.get("authenticated") is True,
                             "true_mentions": True, "sender_verified": bool(self.store.get("bot_wxid"))},
            "providers": providers})
        return True

    def _protocol_snapshot(self):
        document = self.runtime.status()
        account = document.get("account", {})
        transport = self.runtime.outbox.delivery_states()
        with self.store.lock:
            rows = self.store.conn.execute("""SELECT r.event_id,r.group_ref,r.status,r.profile,r.persona,
                r.event_ts,o.status AS reply_status FROM requests r LEFT JOIN reply_outbox o ON o.event_id=r.event_id AND o.kind='final'
                WHERE json_extract(r.event,'$.transport')='native_protocol'
                AND json_extract(r.event,'$.account_id')=?
                AND json_extract(r.event,'$.gateway_account_id')=? ORDER BY r.event_order DESC,r.event_ts DESC""",
                (self.runtime.business.own_username, self.runtime.business.account_id)).fetchall()
            metrics = [(row["event_id"], self.store.get("protocol_metrics_" + row["event_id"], {})) for row in rows[:500]]
        failed = {row["event_id"] for row in rows if row["status"] == "failed" or transport.get(row["event_id"]) == "rejected"}
        pending = {row["event_id"] for row in rows if row["reply_status"] in {"awaiting_generation", "pending"}
                   and transport.get(row["event_id"]) not in {"accepted", "rejected", "unknown"}}
        result = {"state": "login_required" if account.get("login_required") else document["state"],
                  "enabled": bool(self.store.config()["enabled"]), "heartbeat_at": time.time(),
                  "authenticated": account.get("authenticated"), "connection": account.get("connection"),
                  "login_required": account.get("login_required", False), "error": document.get("error"),
                  "counts": {"requests": len(rows), "replied": sum(state == "accepted" for state in transport.values()),
                             "pending": len(pending), "failed": len(failed),
                             "unknown": sum(state == "unknown" for state in transport.values())},
                  "queue_depth": len(pending), "active": None, "last_request": None, "latency_metrics": {}}
        result["stale_events"] = [{key: value for key, value in entry.items()
            if key in {"kind", "request_id", "age_seconds", "at", "detail"}}
            for entry in document.get("stale_events", [])[:50] if isinstance(entry, dict)]
        if rows:
            latest = rows[0]
            profile, persona = json.loads(latest["profile"]), json.loads(latest["persona"])
            measured_model = metrics[0][1].get("model") if metrics else None
            result["active"] = {"group_ref": latest["group_ref"], "provider": profile.get("kind"),
                "profile_id": profile.get("id"), "model": measured_model if isinstance(measured_model, str) else profile.get("model"), "reasoning_effort": profile.get("reasoning_effort"),
                "persona_id": persona.get("id"), "persona_version": persona.get("version"), "persona_name": persona.get("name")}
            result["last_request"] = {"request_id": latest["event_id"], "group_ref": latest["group_ref"],
                "status": transport.get(latest["event_id"], latest["status"]), "started_at": latest["event_ts"]}
        samples = {}
        for identifier, metric in metrics:
            measurements = dict(metric.get("timings", {}))
            if "generation_seconds" in metric:
                measurements["generation"] = metric["generation_seconds"]
            for phase, value in measurements.items():
                if phase not in {"generation", "trigger_to_model", "model_call", "mcp_init", "mcp_tool", "mcp_shutdown"}:
                    continue
                if type(value) in {int, float} and 0 <= value < 86400:
                    samples.setdefault(phase, []).append(value)
                    if result["last_request"] and identifier == result["last_request"]["request_id"] and phase in {"generation", "trigger_to_model"}:
                        result["last_request"][phase + "_seconds"] = value
        for phase, values in samples.items():
            values.sort()
            result["latency_metrics"][phase] = {"count": len(values),
                "p50_seconds": values[max(0, int(len(values) * .5 + .999) - 1)],
                "p95_seconds": values[max(0, int(len(values) * .95 + .999) - 1)]}
        return result

    def _merge_protocol_status(self, runtime):
        """Overlay local protocol status.json health when the file is known."""
        path = self.store.get("protocol_status_file")
        if not isinstance(path, str) or not path.strip():
            return runtime
        try:
            document = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return runtime
        if not isinstance(document, dict) or document.get("component") != "wekeeper_protocol_runtime":
            return runtime
        account = document.get("account") if isinstance(document.get("account"), dict) else {}
        merged = dict(runtime)
        if "authenticated" in account:
            merged["authenticated"] = account.get("authenticated") is True
        connection = account.get("connection")
        if isinstance(connection, str) and connection:
            merged["connection"] = connection
        login_required = account.get("login_required") is True or (
            account.get("authenticated") is False and document.get("state") in {"attention", "failed", "running"})
        if login_required or document.get("lanes", {}).get("receive", {}).get("error") == "auth_required":
            merged["login_required"] = True
            merged["state"] = "login_required"
            merged["error"] = merged.get("error") or "auth_required"
        for key in ("last_receive_at", "last_send_at"):
            value = document.get(key)
            if isinstance(value, (int, float)) and value >= 0:
                merged[key] = float(value)
            elif isinstance(value, str) and value:
                try:
                    merged[key] = _timestamp(value)
                except Exception:
                    pass
        events = document.get("stale_events")
        if isinstance(events, list):
            cleaned = []
            for entry in events[:50]:
                if not isinstance(entry, dict) or not isinstance(entry.get("kind"), str):
                    continue
                item = {"kind": entry["kind"][:40]}
                for field in ("request_id", "at", "detail"):
                    if isinstance(entry.get(field), str):
                        item[field] = entry[field][:200 if field == "request_id" else 40]
                if type(entry.get("age_seconds")) is int and entry["age_seconds"] >= 0:
                    item["age_seconds"] = entry["age_seconds"]
                cleaned.append(item)
            merged["stale_events"] = cleaned
        return merged

    def close(self):
        self.local.close()
        if self.owned:
            self.http.close()
