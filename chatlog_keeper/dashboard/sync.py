"""Outgoing HTTPS sync. Heartbeats and archival payloads use separate workers."""
from __future__ import annotations

import fcntl
import json
import logging
from pathlib import Path
import threading
import time
from urllib.parse import urlsplit

import httpx

from .local_state import LocalState, content_hash
from .projection import heartbeat, projections

LOG = logging.getLogger(__name__)


def read_config(path):
    target = Path(path)
    if target.is_symlink():
        raise ValueError("unsafe_config_path")
    config = json.loads(target.read_text())
    parsed = urlsplit(config.get("server_url", ""))
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("https_server_url_required")
    if not config.get("groups") or not config.get("machine_token"):
        raise ValueError("groups_and_machine_token_required")
    return config


class SyncService:
    def __init__(self, archive_dir, state_dir, config, *, client=None, config_path=None):
        self.archive_dir = Path(archive_dir)
        self.state = LocalState(state_dir)
        self.config = config
        self.config_path = Path(config_path) if config_path else None
        self.client = client or httpx.Client(timeout=httpx.Timeout(25, connect=10), follow_redirects=False, trust_env=False)
        self._owns_client = client is None
        self.stop = threading.Event()
        self._summary = None

    def refresh_config(self):
        if self.config_path:
            self.config = read_config(self.config_path)

    def post(self, route, payload):
        config = self.config
        response = self.client.post(config["server_url"].rstrip("/") + route,
                                    json=payload, headers={"Authorization": "Bearer " + config["machine_token"]})
        response.raise_for_status()
        return response.json()

    def scan(self):
        self.refresh_config()
        data = projections(self.archive_dir, self.config["groups"], self.state.get("sender_salt"))
        initial = not self.state.get("bootstrapped", False)
        current = {(d["group"]["group_ref"], d["day"]) for d in data}
        current_coverage = {d["group"]["group_ref"]: d["coverage"] for d in data}
        for old in self.state.partitions():
            key = old["group_ref"], old["day"]
            if key not in current and old["group_ref"] in self.config["groups"]:
                empty = dict(old["data"], messages=[], coverage=current_coverage[old["group_ref"]])
                self.state.queue(empty, initial=initial)
        for partition in data:
            self.state.queue(partition, initial=initial)
        self.state.set("bootstrapped", True)

    def scan_avatars(self):
        from .avatars import avatar_projections
        self.refresh_config()
        for item in avatar_projections(self.archive_dir, self.config["groups"], self.state.get("sender_salt"), self.state.source_id):
            key = item["group_ref"] + ":" + item["sender_id"]
            digest = content_hash({k: item[k] for k in ("avatar_hash", "display_name")})
            self.state.queue_feature("avatar", key, item, digest)

    def flush_avatars(self):
        self.refresh_config()
        for item in self.state.features("avatar"):
            if item["data"]["group_ref"] not in self.config["groups"]:
                continue
            self.post("/v1/ingest/avatars", item["data"])
            self.state.acknowledge_feature("avatar", item["item_key"], item["digest"])

    def flush_usage(self):
        from .usage import UsageLedger
        ledger = UsageLedger(self.state.directory)
        try:
            reconcile = time.time() - self.state.get("last_usage_reconcile", 0) >= 86400
            rows = ledger.list_runs() if reconcile else ledger.pending()
            keys = ("run_id", "kind", "user_id", "turn_id", "model", "started_at", "completed_at", "status", "usage")
            errors = []
            for start in range(0, len(rows), 100):
                runs = [{key: row.get(key) for key in keys} for row in rows[start:start + 100] if row.get("completed_at") is not None]
                if runs:
                    try:
                        self.post("/v1/ingest/usage", {"source_id": self.state.source_id, "runs": runs})
                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code not in {400, 409, 422}:
                            raise
                        # An orphaned restored chat must not block other usage.
                        for run in runs:
                            try:
                                self.post("/v1/ingest/usage", {"source_id": self.state.source_id, "runs": [run]})
                                ledger.acknowledge(run["run_id"])
                            except httpx.HTTPStatusError as item_error:
                                errors.append(item_error)
                    else:
                        for run in runs:
                            ledger.acknowledge(run["run_id"])
            if errors:
                raise errors[0]
            if reconcile:
                self.state.set("last_usage_reconcile", time.time())
        finally:
            ledger.close()

    def flush(self, limit=100):
        self.refresh_config()
        count = 0
        allowed = set(self.config["groups"])
        queued = [p for p in self.state.partitions(pending_only=True) if p["group_ref"] in allowed][:limit]
        failures = []
        for partition in queued:
            try:
                ack = self.post("/v1/ingest/partitions", partition)
                if ack.get("accepted", True) and ack.get("revision", partition["revision"]) >= partition["revision"]:
                    self.state.acknowledge(partition["group_ref"], partition["day"], partition["revision"])
                    count += 1
            except httpx.HTTPStatusError as exc:
                # One invalid/oversized partition must not starve other groups.
                if exc.response.status_code in {400, 409, 413, 422}:
                    failures.append(exc)
                    continue
                raise
        if failures:
            raise failures[0]
        return count

    def flush_summaries(self):
        self.refresh_config()
        from .summaries import SummaryWorker
        worker = SummaryWorker(self.state.directory, self.state.source_id)
        try:
            for item in worker.pending(group_refs=self.config["groups"]):
                partition = self.state.partition_status(item["group_ref"], item["day"])
                if partition and partition["content_hash"] != item["input_hash"]:
                    worker.acknowledge(item["group_ref"], item["day"], item["revision"])
                    continue
                if not partition or partition["pending"]:
                    continue
                try:
                    self.post("/v1/ingest/summaries", item)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 409 and exc.response.json().get("detail") == "summary_input_stale":
                        # Cloud restore/missing partition: resend evidence before its result.
                        self.state.retry_partition(item["group_ref"], item["day"])
                        continue
                    raise
                worker.acknowledge(item["group_ref"], item["day"], item["revision"])
        finally:
            worker.close()

    def send_heartbeat(self):
        self.refresh_config()
        from .summaries import SummaryWorker
        worker = SummaryWorker(self.state.directory, self.state.source_id)
        try:
            status = worker.status()
            error = status.get("error")
            if isinstance(error, dict):
                error = error.get("code", "summary_failed")
            safe_status = {"state": status.get("state", "unknown"), "last_success_at": status.get("last_success_at"), "error": error}
            payload = heartbeat(self.archive_dir, self.state.source_id, safe_status)
            sync_status = self.state.get("sync_status", {})
            payload["sync"] = {"state": sync_status.get("state", "starting"),
                               "last_success_at": sync_status.get("last_success_at"),
                               "error": sync_status.get("error"),
                               "pending_partitions": sum(p["group_ref"] in self.config["groups"] for p in self.state.partitions(pending_only=True))}
            ack = self.post("/v1/ingest/heartbeat", payload)
            if "partitions" in ack:
                cloud = {(p["group_ref"], p["day"]): p for p in ack["partitions"]}
                for local in self.state.partitions():
                    if local["group_ref"] not in self.config["groups"]:
                        continue
                    remote = cloud.get((local["group_ref"], local["day"]))
                    if not remote or remote["content_hash"] != local["content_hash"] or remote["revision"] < local["revision"]:
                        self.state.retry_partition(local["group_ref"], local["day"])
            if "summaries" in ack:
                cloud = {(p["group_ref"], p["day"]): p for p in ack["summaries"]}
                for row in worker.conn.execute("SELECT group_ref,day,revision,input_hash FROM summary_jobs").fetchall():
                    if row["group_ref"] not in self.config["groups"]:
                        continue
                    remote = cloud.get((row["group_ref"], row["day"]))
                    if not remote or remote["input_hash"] != row["input_hash"] or remote["revision"] < row["revision"]:
                        with worker.conn:
                            worker.conn.execute("UPDATE summary_jobs SET delivered=0 WHERE group_ref=? AND day=?", (row["group_ref"], row["day"]))
            if "avatars" in ack:
                cloud = {(a["group_ref"], a["sender_id"]): a["avatar_hash"] for a in ack["avatars"]}
                for item in self.state.features("avatar", pending_only=False):
                    data = item["data"]
                    if data["group_ref"] in self.config["groups"] and cloud.get((data["group_ref"], data["sender_id"])) != data["avatar_hash"]:
                        self.state.retry_feature("avatar", item["item_key"])
            if "usage_count" in ack:
                from .usage import UsageLedger
                ledger = UsageLedger(self.state.directory)
                try:
                    if ack["usage_count"] < len(ledger.list_runs()):
                        self.state.set("last_usage_reconcile", 0)
                finally:
                    ledger.close()
            return ack
        finally:
            worker.close()

    def _heartbeats(self):
        while not self.stop.is_set():
            try:
                self.send_heartbeat()
            except Exception as exc:
                LOG.warning("heartbeat_failed: %s", type(exc).__name__)
            self.stop.wait(30)

    def _quota(self):
        from .quota import CodexQuotaReader
        reader = CodexQuotaReader()
        try:
            while not self.stop.is_set():
                try:
                    self.post("/v1/ingest/quota", reader.snapshot(self.state.source_id))
                except Exception as exc:
                    LOG.warning("quota_sync_failed: %s", type(exc).__name__)
                for _ in range(30):
                    if self.stop.wait(1) or reader.changed.is_set():
                        break
        finally:
            reader.close()

    def run(self):
        lock_path = self.state.directory / "sync.lock"
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError("sync_already_running") from None
            thread = threading.Thread(target=self._heartbeats, daemon=True)
            thread.start()
            quota_thread = threading.Thread(target=self._quota, daemon=True)
            quota_thread.start()
            failures = 0
            try:
                while not self.stop.is_set():
                    started = time.monotonic()
                    try:
                        self.scan()
                        transfer_errors = []
                        for operation in (self.flush, self.flush_summaries, self.scan_avatars, self.flush_avatars, self.flush_usage):
                            try:
                                operation()
                            except Exception as exc:
                                transfer_errors.append(exc)
                        if transfer_errors:
                            raise transfer_errors[0]
                        failures = 0
                        self.state.set("sync_status", {"state": "healthy", "last_success_at": time.time()})
                    except Exception as exc:
                        failures = min(failures + 1, 4)
                        self.state.set("sync_status", {**self.state.get("sync_status", {}), "state": "error", "error": type(exc).__name__})
                        LOG.warning("sync_failed: %s", type(exc).__name__)
                    self.stop.wait(max(1, min(300, 30 * 2 ** max(0, failures - 1)) - (time.monotonic() - started)))
            finally:
                self.stop.set()
                thread.join(timeout=30)
                quota_thread.join(timeout=30)

    def close(self):
        self.stop.set()
        if self._owns_client:
            self.client.close()
        self.state.close()
