"""Native business custody and generation, independent of protocol delivery.

The existing business database is authoritative for report transactions and
frozen replies. Moving its frozen result into the transport outbox is an
idempotent projection: a crash between those commits never regenerates a reply.
No archive reader, legacy worker scheduler, UI sender or automatic send is used.
"""
from __future__ import annotations

import asyncio
import copy
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import fcntl
import hashlib
import os
import time

from .business_reply import grounded_business_reply
from .deferred_usage import restore_deferred_usage
from .persona import default_persona
from .protocol_gateway import _json
from .protocol_inbox import _private_file
from .reply_generator import GenerationResult, GenerationTrace, generation_metrics
from .service import required_report
from .store import business_day
from .request_policy import CHAT_ONLY, request_capability

# Catch-up after long offline must not auto-reply. Reports still bookkeep.
DEFAULT_STALE_AFTER_SECONDS = 600


class ProtocolBusinessError(RuntimeError):
    pass


class ProtocolBusinessWorker:
    def __init__(self, *, inbox, store, generator, outbox, bindings,
                 own_username, account_id, concurrency=2, clock=time.time,
                 stale_after_seconds=DEFAULT_STALE_AFTER_SECONDS, usage_ledger=None):
        if type(concurrency) is not int or not 1 <= concurrency <= 2:
            raise ValueError("invalid_protocol_model_concurrency")
        if (type(stale_after_seconds) not in (int, float) or not 0 < stale_after_seconds <= 86400 * 7):
            raise ValueError("invalid_protocol_stale_after_seconds")
        if (inbox.own_username != own_username or inbox.gateway.account_id != account_id
                or outbox.own_username != own_username or outbox.account_id != account_id):
            raise ProtocolBusinessError("protocol_business_account_mismatch")
        if len(set(bindings.values())) != len(bindings):
            raise ProtocolBusinessError("protocol_business_group_mapping_conflict")
        owner = hashlib.sha256(_json([own_username, account_id]).encode()).hexdigest()
        self._lease = _private_file(store.directory / ("protocol-business-" + owner + ".lock"))
        try:
            fcntl.flock(self._lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self._lease)
            raise ProtocolBusinessError("protocol_business_already_running") from None
        self.inbox, self.store, self.generator, self.outbox = inbox, store, generator, outbox
        self.bindings = dict(bindings)  # Native thread -> existing configured group_ref.
        self.own_username, self.account_id, self.clock = own_username, account_id, clock
        self.concurrency = concurrency
        self.stale_after_seconds = float(stale_after_seconds)
        self.usage_ledger = usage_ledger
        self.pool = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="protocol-business")
        self.inflight = {}
        self.stale_events = deque(maxlen=50)

    @staticmethod
    def _frozen_key(request_id):
        return "protocol_frozen_" + request_id

    def _event(self, row, group_ref, binding):
        source = copy.deepcopy(row["message_json"])
        identity = [self.own_username, source.get("threadId"), source.get("serverId")]
        request_id = "protocol:" + hashlib.sha256(_json(identity).encode()).hexdigest()
        if (row["request_id"] != request_id or row["own_username"] != self.own_username
                or row["account_id"] != self.account_id or source.get("accountId") != self.account_id
                or source.get("ownUsername") != self.own_username
                or row["server_id"] != source.get("serverId")
                or row["thread_id"] != source.get("threadId")
                or row["message_id"] != source.get("receivedId")
                or source.get("disposition") != "ready" or source.get("quoteEligible") is not True):
            raise ProtocolBusinessError("protocol_business_source_conflict")
        created = datetime.fromisoformat(source["createdAt"].replace("Z", "+00:00"))
        if created.tzinfo is None:
            raise ProtocolBusinessError("protocol_business_source_time_invalid")
        message_ref = "native:" + request_id.removeprefix("protocol:")
        reply_to = {"transport": "native_protocol", "message_ref": message_ref,
                    "group_ref": group_ref, "account_id": self.account_id,
                    "own_username": self.own_username, "thread_id": source["threadId"],
                    "received_id": source["receivedId"], "server_id": source["serverId"],
                    "source_sha256": source["sourceSha256"]}
        mentions = source.get("mentions") or {}
        return {"event_id": request_id, "message_ref": message_ref, "group_ref": group_ref,
                "account_id": self.own_username, "gateway_account_id": self.account_id,
                "group_id": source["threadId"], "sender_id": source["senderId"],
                "server_id": source["serverId"], "text": source["body"],
                "occurred_at": created.isoformat(), "create_time": created.timestamp(),
                "archive_id": int(row["event_cursor"]), "native_cursor": row["event_cursor"],
                "native_received_at": row["received_at"], "transport": "native_protocol",
                "native_source": source, "native_business": copy.deepcopy(binding["business"]),
                "msg_type": source["type"], "sub_type": source.get("appMessageType"),
                "mentions_status": "verified" if mentions.get("state") == "known" else "unknown",
                "mentions": list(mentions.get("userIds") or []), "reply_to": reply_to}

    def _eligible(self, source, binding):
        mentions = source.get("mentions") or {}
        if not binding or not binding.get("enabled"):
            return False
        configured = binding.get("bot_wxid") or self.store.get("bot_wxid")
        if configured and configured != self.own_username:
            raise ProtocolBusinessError("protocol_business_bot_identity_changed")
        return (source.get("direction") == "incoming" and source.get("senderId") != self.own_username
                and isinstance(source.get("body"), str) and 0 < len(source["body"].strip()) <= 16000
                and mentions.get("state") == "known" and self.own_username in (mentions.get("userIds") or [])
                and (source.get("type") == 1 or
                     (source.get("type") == 49 and source.get("appMessageType") == 57)))

    def admit_pending(self):
        admitted = 0
        for row in self.inbox.pending_requests(limit=500):
            if row["own_username"] != self.own_username or row["account_id"] != self.account_id:
                raise ProtocolBusinessError("protocol_business_source_conflict")
            group_ref = self.bindings.get(row["thread_id"])
            binding = self.store.binding(group_ref) if group_ref else None
            existing = self.store.request(row["request_id"])
            if existing is None and not self._eligible(row["message_json"], binding):
                self.inbox.complete(row["request_id"])
                continue
            if existing is not None:
                event = existing["event"]
                # Replaying custody must not silently accept changed native
                # identity, source bytes, timestamp or configuration mapping.
                if (event.get("native_source") != row["message_json"]
                        or event.get("native_cursor") != row["event_cursor"]
                        or event.get("native_received_at") != row["received_at"]
                        or existing["group_ref"] != group_ref):
                    raise ProtocolBusinessError("protocol_business_replay_conflict")
                continue
            event = self._event(row, group_ref, binding)
            result = self.store.begin_request(event,
                native_tool_plan=getattr(self.generator, "direct_plan_enabled", False),
                native_chat_policy=getattr(self.generator, "chat_policy_enabled", False))
            admitted += int(result["accepted"])
        return admitted

    def _requests(self):
        with self.store.lock:
            rows = self.store.conn.execute("""SELECT * FROM requests
                WHERE json_extract(event,'$.transport')='native_protocol'
                AND json_extract(event,'$.account_id')=?
                AND json_extract(event,'$.gateway_account_id')=?
                ORDER BY event_order,event_ts,event_id""",
                (self.own_username, self.account_id)).fetchall()
            return [self.store._request(row) for row in rows]

    def _check_binding(self, request):
        binding = self.store.binding(request["group_ref"])
        return bool(binding and binding.get("enabled")
                    and (binding.get("bot_wxid") or self.store.get("bot_wxid") or self.own_username) == self.own_username
                    and binding.get("business") == request["event"]["native_business"]
                    and binding.get("profile") == request["profile"]
                    and (binding.get("persona") or default_persona()) == request["persona"])

    def _source_blocked(self, request):
        return self.inbox.source_quarantine_reason(request["event"]["native_source"]["receivedId"])

    def _cancel(self, request, reason):
        # Revoke permission before a still-running provider can invoke another
        # tool. record_offer rechecks this status inside its write transaction.
        self.store.set_request_status(request["event_id"], "cancelled")
        receipt = copy.deepcopy(self.store.receipt(request["event_id"]))
        self.store.set("protocol_blocked_" + request["event_id"],
                       {"reason": reason, "business_receipt": receipt, "delivery_blocked": True})
        reply = self.store.reply(request["event_id"])
        if reply and reply["status"] == "awaiting_generation":
            self.store.transition_reply(reply["reply_id"], "cancelled", error=reason)
        # A quarantined source remains explicitly quarantined in the inbox.
        if self.inbox.request_state(request["event_id"]) != "quarantined":
            self.inbox.complete(request["event_id"])

    def _frozen_record(self, request):
        key = self._frozen_key(request["event_id"])
        frozen = self.store.get(key)
        if frozen is None:
            reply = self.store.reply(request["event_id"])
            if reply and reply["status"] == "pending" and reply["text"] is not None:
                raise ProtocolBusinessError("protocol_final_not_frozen")
            return None
        reply = self.store.reply(request["event_id"])
        event = request["event"]
        if (reply is None or reply["text"] != frozen["text"]
                or reply["reply_to"] != event["reply_to"]
                or frozen["source"] != event["native_source"]
                or frozen["request_id"] != request["event_id"]
                or frozen["reply_id"] != reply["reply_id"]):
            raise ProtocolBusinessError("protocol_frozen_reply_conflict")
        return frozen

    def _parse_occurred_at(self, value):
        if not isinstance(value, str) or not value.strip():
            return None
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _message_age_seconds(self, request):
        occurred = self._parse_occurred_at(request.get("occurred_at"))
        if occurred is None:
            return None
        return max(0.0, float(self.clock()) - occurred.timestamp())

    def _is_stale(self, request):
        age = self._message_age_seconds(request)
        return age is not None and age > self.stale_after_seconds

    def _stale_allows_bookkeeping(self, request):
        # Reports still bookkeep after long offline; queries and chat do not.
        # Classify against the message's own business day so catch-up reports
        # are recognized even when the calendar day has already rolled.
        if request_capability(request) == CHAT_ONLY:
            return False
        event = request["event"]
        business = event.get("native_business") or {}
        day = business_day(request["occurred_at"])
        return required_report(event, business, current_day=day) is not None

    def _note_stale(self, *, kind, request, age_seconds, detail=None):
        entry = {
            "kind": kind,
            "request_id": request["event_id"],
            "thread_id": request.get("group_id") or request.get("event", {}).get("group_id"),
            "occurred_at": request.get("occurred_at"),
            "age_seconds": int(age_seconds) if age_seconds is not None else None,
            "at": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        if detail:
            entry["detail"] = detail
        self.stale_events.appendleft(entry)
        self.store.set("protocol_stale_log", list(self.stale_events))

    def _expire(self, request):
        age = self._message_age_seconds(request)
        self.store.set_request_status(request["event_id"], "cancelled")
        payload = {
            "reason": "expired",
            "occurred_at": request.get("occurred_at"),
            "age_seconds": age,
            "capability": request_capability(request),
            "at": datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        self.store.set("protocol_expired_" + request["event_id"], payload)
        reply = self.store.reply(request["event_id"])
        if reply and reply["status"] == "awaiting_generation":
            self.store.transition_reply(reply["reply_id"], "cancelled", error="protocol_request_expired")
        if self.inbox.request_state(request["event_id"]) != "quarantined":
            self.inbox.complete(request["event_id"])
        self._note_stale(kind="expired", request=request, age_seconds=age,
                         detail=request_capability(request))

    def delivery_block_reason(self, row):
        request = self.store.request(row["request_id"])
        evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
        if evidence.get("delivery_suppressed") == "suppressed_stale":
            return "suppressed_stale"
        if request is not None and self.store.get("protocol_delivery_suppress_" + request["event_id"]) == "suppressed_stale":
            return "suppressed_stale"
        if request is None or not self._check_binding(request):
            return "configuration_changed"
        if self._source_blocked(request):
            return "source_quarantined"
        reply = self.store.reply(request["event_id"])
        if reply is None or reply["status"] == "cancelled":
            return "configuration_changed"
        return None

    def recover_frozen(self):
        recovered = 0
        for request in self._requests():
            if self._frozen_record(request) is not None:
                if self.inbox.request_state(request["event_id"]) != "quarantined":
                    self.inbox.complete(request["event_id"])
                recovered += 1
        return recovered

    def _retain_deferred_usage(self, request, evidence):
        ops = evidence.get("deferred_usage") if isinstance(evidence, dict) else None
        if not isinstance(ops, list) or not ops:
            return
        digest = hashlib.sha256(_json(ops).encode()).hexdigest()
        key = "protocol_usage_pending_" + request["event_id"]
        with self.store.lock:
            batches = self.store.get(key, {})
            if digest not in batches:
                self.store.set(key, {**batches, digest: copy.deepcopy(ops)})

    def _flush_deferred_usage(self, request, evidence, *, pending=False):
        """FULL-write retained accounting, independently of delivery permission."""
        if self.usage_ledger is None or not isinstance(evidence, dict):
            return
        ops = evidence.get("deferred_usage")
        if not ops:
            return
        marker = "protocol_usage_flushed_" + request["event_id"]
        digest_marker = marker + "_" + hashlib.sha256(_json(ops).encode()).hexdigest()
        if self.store.get(digest_marker) or (not pending and self.store.get(marker)):
            return
        restore_deferred_usage(ops, self.usage_ledger)
        self.store.set_many({marker: "1", digest_marker: "1"})

    def _flush_pending_usage(self, request):
        batches = self.store.get("protocol_usage_pending_" + request["event_id"], {})
        for ops in batches.values():
            self._flush_deferred_usage(request, {"deferred_usage": ops}, pending=True)

    def project_outbox(self):
        """Run separately from generation; projection failure keeps the final.

        Per-request errors are returned for reconciliation, never thrown into
        the business scheduler or used as permission to regenerate an answer.
        """
        projected, errors = 0, []
        for request in self._requests():
            try:
                frozen = self._frozen_record(request)
                if frozen is None:
                    self._flush_pending_usage(request)
                    continue
                blocked = self.delivery_block_reason(frozen)
                self.outbox.freeze(request_id=request["event_id"], reply_id=frozen["reply_id"],
                                   source=frozen["source"], text=frozen["text"], evidence=frozen["evidence"],
                                   blocked_reason=blocked)
                projected += 1
                try:
                    if self.store.get("protocol_usage_pending_" + request["event_id"]):
                        self._flush_pending_usage(request)
                    else:
                        self._flush_deferred_usage(request, frozen.get("evidence"))
                except Exception as error:
                    # Delivery already committed; accounting retry is separate.
                    errors.append({"request_id": request["event_id"],
                                   "error": "deferred_usage_" + type(error).__name__})
            except Exception as error:
                errors.append({"request_id": request["event_id"], "error": type(error).__name__})
        return {"projected": projected, "errors": errors}

    def _sync_permissions(self):
        for request in self._requests():
            reply = self.store.reply(request["event_id"])
            if not reply or reply["status"] != "awaiting_generation":
                continue
            if self._source_blocked(request):
                self._cancel(request, "protocol_source_quarantined")
            elif not self._check_binding(request):
                self._cancel(request, "bot_configuration_changed")

    def _sync_cancelled(self):
        # Configuration changes can cancel the durable reply before the
        # scheduler sees them. Close its request and custody as well, including
        # after a restart or a quick disable/enable cycle. A running generator
        # cannot gain write permission back from the new configuration.
        for request in self._requests():
            reply = self.store.reply(request["event_id"])
            if (reply and reply["status"] == "cancelled"
                    and (request["status"] != "cancelled"
                         or self.store.get("protocol_blocked_" + request["event_id"]) is None
                         or self.inbox.request_state(request["event_id"]) == "pending")):
                self._cancel(request, reply.get("error") or "bot_configuration_changed")

    def _trace_progress(self, request):
        trace = GenerationTrace()
        trace.wall_observation("inbox_batch_staging_before_commit", request["event"].get("native_received_at"))
        trace.wall_observation("business_admission_before_commit", request.get("created_at"))
        return {"started": self.clock(), "timings": {}, "used_mcp": False, "_generation_trace": trace}

    def _generate(self, request, nearby, progress):
        trace = progress["_generation_trace"]
        trace.mark("generation_started")
        outcome = "failed"
        try:
            result = asyncio.run(self.generator.generate(request["event"], request, nearby, progress=progress))
            outcome = "completed"
        finally:
            # Includes asyncio/provider/MCP cleanup before the pool future ends.
            trace.mark("generation_ended", outcome=outcome)
        return GenerationResult(result.text, result.evidence, {**result.metrics, "trace": trace.snapshot()})

    def _fallback(self, request, error, progress=None):
        receipt = copy.deepcopy(self.store.receipt(request["event_id"]))
        evidence = {"schema_version": 1, "request": {"event_id": request["event_id"]},
                    "recovery": "committed_report" if receipt else "generation_failed",
                    "report_receipt": receipt, "error_code": type(error).__name__}
        text = "这次请求没能完成，请稍后再试。"
        if receipt and receipt.get("recorded"):
            facts = {r["item_id"]: r for r in receipt.get("results", [receipt])
                     if r.get("ok") and r.get("recorded") and r.get("business_id") == request["business_id"]}
            day = business_day(request["occurred_at"])
            text = grounded_business_reply("", facts, request["event"]["native_business"], day, recorded=True)
            expected = required_report(request["event"], request["event"]["native_business"], current_day=day)
            if expected and {r["item_id"] for r in expected["items"]} - facts.keys():
                text += "其余项目未确认成功。"
        if progress is None:
            progress = self._trace_progress(request)
            progress["_generation_trace"].mark("generation_ended", outcome="recovered_without_generation")
        if isinstance(progress.get("_deferred_usage"), list):
            evidence["deferred_usage"] = copy.deepcopy(progress["_deferred_usage"])
        return GenerationResult(text, evidence, generation_metrics(progress))

    def _publish(self, request, result, progress=None):
        # Accounting survives cancellation/quarantine without creating a reply.
        # Actual provider calls remain chargeable after permissions are revoked.
        self._retain_deferred_usage(request, result.evidence)
        event = request["event"]
        if self._source_blocked(request):
            self._cancel(request, "protocol_source_quarantined")
            return
        if not self._check_binding(request):
            self._cancel(request, "bot_configuration_changed")
            return
        if (not isinstance(result.text, str) or not result.text.strip() or len(result.text) > 4000
                or result.evidence.get("request", {}).get("event_id") != request["event_id"]):
            raise ProtocolBusinessError("protocol_generation_result_invalid")
        evidence = copy.deepcopy(result.evidence)
        suppress = None if progress is None else progress.get("suppress_delivery")
        if suppress == "suppressed_stale":
            evidence["delivery_suppressed"] = "suppressed_stale"
            age = self._message_age_seconds(request)
            self.store.set("protocol_delivery_suppress_" + request["event_id"], "suppressed_stale")
            self._note_stale(kind="suppressed_stale", request=request, age_seconds=age, detail="report")
        reply = self.store.reply(request["event_id"])
        frozen = {"request_id": request["event_id"], "reply_id": reply["reply_id"],
                  "source": copy.deepcopy(event["native_source"]), "text": result.text,
                  "evidence": evidence}
        key = self._frozen_key(request["event_id"])
        previous = self.store.get(key)
        if previous is not None and _json(previous) != _json(frozen):
            raise ProtocolBusinessError("protocol_frozen_reply_conflict")
        # queue_reply writes final text, original quote and evidence metadata
        # in the SAME business DB transaction. Transport waits happen later.
        metrics = copy.deepcopy(result.metrics)
        if progress is not None:
            trace = progress["_generation_trace"]
            trace.mark("final_freeze_begin")
            metrics["trace"] = trace.snapshot()
        metadata = {key: frozen, "protocol_metrics_" + request["event_id"]: metrics}
        if suppress == "suppressed_stale":
            metadata["protocol_delivery_suppress_" + request["event_id"]] = "suppressed_stale"
        try:
            saved = self.store.queue_reply(request["event_id"], result.text, reply_to=event["reply_to"],
                                           metadata=metadata)
        except ValueError:
            saved = self.store.reply(request["event_id"])
            if saved is None or saved["status"] != "cancelled":
                raise
        if saved["status"] == "cancelled":
            self._cancel(request, saved.get("error") or "bot_configuration_changed")
            return
        if saved["status"] != "pending" or self._frozen_record(request) != frozen:
            raise ProtocolBusinessError("protocol_final_not_frozen")
        self.inbox.complete(request["event_id"])

    def _reap(self):
        for request_id, info in list(self.inflight.items()):
            if not info["future"].done():
                continue
            if "result" not in info:
                try:
                    info["result"] = info["future"].result()
                except Exception as error:
                    info["result"] = self._fallback(info["request"], error, info["progress"])
            # A failed durable final commit keeps this business lane occupied.
            self._publish(info["request"], info["result"], info["progress"])
            del self.inflight[request_id]

    def run_once(self, *, block=False):
        admitted = self.admit_pending()
        self._sync_cancelled()
        self._sync_permissions()
        self.recover_frozen()
        self._reap()
        lanes = {info["request"]["business_id"] for info in self.inflight.values()
                 if request_capability(info["request"]) != CHAT_ONLY}
        chat_inflight = sum(request_capability(info["request"]) == CHAT_ONLY for info in self.inflight.values())
        for request in self._requests():
            reply = self.store.reply(request["event_id"])
            if reply is None or reply["status"] != "awaiting_generation":
                continue
            chat_only = request_capability(request) == CHAT_ONLY
            if (request["event_id"] in self.inflight
                    or (not chat_only and request["business_id"] in lanes)
                    or (chat_only and chat_inflight >= 1)):
                continue
            if len(self.inflight) >= self.concurrency:
                break
            if self._source_blocked(request):
                self._cancel(request, "protocol_source_quarantined")
                continue
            if not self._check_binding(request):
                self._cancel(request, "bot_configuration_changed")
                continue
            stale = self._is_stale(request)
            if stale and not self._stale_allows_bookkeeping(request):
                self._expire(request)
                continue
            if self.store.receipt(request["event_id"]):
                progress = self._trace_progress(request)
                if stale:
                    progress["suppress_delivery"] = "suppressed_stale"
                progress["_generation_trace"].mark("generation_ended", outcome="recovered_without_generation")
                self._publish(request, self._fallback(request, RuntimeError("generation_interrupted"), progress), progress)
                continue
            progress = self._trace_progress(request)
            if stale:
                progress["suppress_delivery"] = "suppressed_stale"
            nearby_limit = min(5, max(0, int(self.store.config().get("nearby_messages", 5))))
            nearby = self.inbox.nearby_context(request["event_id"], limit=nearby_limit) if nearby_limit else []
            progress["_generation_trace"].mark("generation_queued")
            future = self.pool.submit(self._generate, request, nearby, progress)
            self.inflight[request["event_id"]] = {"request": request, "future": future, "progress": progress}
            if chat_only:
                chat_inflight += 1
            else:
                lanes.add(request["business_id"])
            if block:
                try:
                    future.result()
                except Exception:
                    pass  # _reap freezes the explicit failure/committed receipt.
                self._reap()
                break
        return {"admitted": admitted, "generating": len(self.inflight),
                "stale_events": list(self.stale_events)}

    def drain_completed(self):
        """Freeze completed work during shutdown without starting new work."""
        self._reap()

    def close(self):
        self.pool.shutdown(wait=True)
        try:
            self.drain_completed()
        finally:
            if self._lease is not None:
                os.close(self._lease)
                self._lease = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
