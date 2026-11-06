#!/usr/bin/env python3
"""Replay native business failure samples in a fresh private ledger copy.

The receive peer is in memory and has no send operation. Only --real-model
enables external model calls; no WeChat API, archive or UI is opened.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import sqlite3
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from chatlog_keeper.bot.protocol_business import ProtocolBusinessWorker
from chatlog_keeper.bot.protocol_gateway import _json
from chatlog_keeper.bot.protocol_history import ProtocolHistoryStore
from chatlog_keeper.bot.protocol_native_inbox import ProtocolNativeInbox
from chatlog_keeper.bot.protocol_outbox import ProtocolOutbox
from chatlog_keeper.bot.protocol_runtime import ProviderResources
from chatlog_keeper.bot.reply_generator import ReplyGenerator
from chatlog_keeper.bot.store import BotStore, business_day


class MemoryReceivePeer:
    """Explicit synthetic input, with no network client or send capability."""
    account_id = "synthetic-native-business"
    base_url = "memory://native-business-no-sends"

    def __init__(self, own, thread):
        self.own, self.thread = own, thread
        self.events, self.messages = [], {}
        self.acknowledged = self.delivered = "0"

    def state(self, consumer):
        return {"consumerId": consumer, "required": True, "registeredAfter": "0",
                "acknowledgedCursor": self.acknowledged, "deliveredCursor": self.delivered}

    def activate_native_consumer(self, consumer, *, own_username):
        assert own_username == self.own
        return {"consumer": self.state(consumer), "checkpoint": {
            "schemaVersion": 1, "accountId": self.account_id, "ownUsername": self.own,
            "ownershipGeneration": "synthetic", "serverId": "synthetic",
            "receivedBoundary": "0", "eventCursor": "0", "readiness": "native_ingress_caught_up"}}

    def receive(self, *, consumer, after, limit):
        items = [event for event in self.events if int(event["cursor"]) > int(after)][:limit]
        cursor = items[-1]["cursor"] if items else after
        self.delivered = str(max(int(self.delivered), int(cursor)))
        return deepcopy({"items": items, "nextCursor": cursor, "consumer": self.state(consumer)})

    def ack(self, consumer, cursor):
        assert int(self.acknowledged) <= int(cursor) <= int(self.delivered)
        self.acknowledged = cursor
        return {"cursor": cursor, "messageReadStateChanged": False}

    def get_received_message(self, identifier):
        return deepcopy(self.messages[identifier])

    def add(self, text, sender):
        cursor = str(len(self.events) + 1)
        sid, received = str(18000000000000000000 + int(cursor)), "synthetic-received-" + cursor
        now = datetime.now(timezone.utc).isoformat()
        source = {"receivedId": received, "firstReceivedSequence": cursor,
                  "accountId": self.account_id, "ownUsername": self.own, "threadId": self.thread,
                  "serverId": sid, "senderId": sender, "direction": "incoming", "type": 1,
                  "appMessageType": None, "createdAt": now, "body": text,
                  "mentions": {"state": "known", "userIds": [self.own]}, "quote": None,
                  "quoteEligible": True, "disposition": "ready", "reason": None}
        source["sourceSha256"] = hashlib.sha256(_json(source).encode()).hexdigest()
        self.messages[received] = source
        self.events.append({"cursor": cursor, "createdAt": now, "kind": "message.received",
                            "data": {"receivedId": received, "firstReceivedSequence": cursor,
                                     "disposition": "ready"}})
        return "protocol:" + hashlib.sha256(_json([self.own, self.thread, sid]).encode()).hexdigest()


def clone_state(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if destination.exists() or source == destination or source in destination.parents:
        raise ValueError("fresh_separate_output_required")
    destination.mkdir(parents=True, mode=0o700)
    state = destination / "state"
    (state / "bot").mkdir(parents=True, mode=0o700)
    state.chmod(0o700)
    with sqlite3.connect(f"file:{source}/bot/bot.sqlite?mode=ro", uri=True) as original:
        with sqlite3.connect(state / "bot" / "bot.sqlite") as copied:
            original.backup(copied)
    (state / "bot" / "bot.sqlite").chmod(0o600)
    shutil.copytree(source / "bot" / "secrets", state / "bot" / "secrets")
    for path in (state / "bot" / "secrets", *(state / "bot" / "secrets").rglob("*")):
        path.chmod(0o700 if path.is_dir() else 0o600)
    (destination / "usage").mkdir(mode=0o700)
    (state / "dashboard").mkdir(mode=0o700)
    return state


def unavailable_final_matches(text, *, day, item, category):
    # Independent fixture oracle: accept only an entire dated, single-item
    # negative status statement. A matching keyword in contradictory prose
    # must not certify the final. Do not call the production reply renderer.
    prefix = ("记好啦|记好了|已记录|已经记录|已完成报备|已经记好了" if category == "report" else
              "查好啦|查好了|我查过了|查询结果|本次查询结果")
    pattern = (rf"(?:(?:{prefix})[，,:：\s]*)?{re.escape(day)}[，,:：\s]*"
               rf"{re.escape(item)}\s*(?:目前|当前|现在)?(?:状态)?(?:是|为)?\s*"
               r"(?:已无|已经没有了|没有了|已经用完了|已用完)(?:啦|哦|喔|呢)?[。！!~～\s]*")
    return isinstance(text, str) and re.fullmatch(pattern, text.strip()) is not None


def run_samples(state, output, *, group_ref, own, thread, provider_factory=None):
    output = Path(output)
    with ExitStack() as stack:
        store = BotStore(state); stack.callback(store.close)
        resources = ProviderResources(store, state_dir=state, usage_dir=output / "usage", dashboard_dir=Path(state) / "dashboard")
        stack.callback(resources.close)
        binding = store.binding(group_ref)
        if not binding or not binding.get("enabled") or not binding["business"]["reporter_ids"]:
            raise ValueError("configured_reporter_and_group_required")
        if binding["profile"]["kind"] == "siwc" and provider_factory is None:
            raise ValueError("probe_requires_explicit_siwc_credential_copy")
        reporter = binding["business"]["reporter_ids"][0]
        items = {item["id"]: item for item in binding["business"]["items"]}
        item = next((i for i in items.values() if i["name"] == "娱乐首单"), None)
        if item is None:
            raise ValueError("original_failure_sample_item_missing")
        peer = MemoryReceivePeer(own, thread)
        inbox = ProtocolNativeInbox(peer, path=output / "inbox.sqlite", own_username=own, consumer="isolated-business")
        stack.callback(inbox.close); inbox.activate()
        outbox = ProtocolOutbox(output / "outbox.sqlite", account_id=peer.account_id, own_username=own)
        stack.callback(outbox.close)
        resources = ProviderResources(store, state_dir=state, usage_dir=output / "usage", dashboard_dir=Path(state) / "dashboard")
        stack.callback(resources.close)
        model_requests = []
        def provider(profile):
            model_requests.append(time.time())
            return (provider_factory or resources.provider)(profile)
        history = ProtocolHistoryStore(store, outbox, own_username=own, account_id=peer.account_id)
        generator = ReplyGenerator(history, state_dir=state, provider_factory=provider)
        def worker():
            value = ProtocolBusinessWorker(inbox=inbox, store=store, generator=generator, outbox=outbox,
                bindings={thread: group_ref}, own_username=own, account_id=peer.account_id,
                usage_ledger=resources.usage_ledger())
            stack.callback(value.close)
            return value
        business = worker()
        counts = lambda: {table: store.conn.execute("SELECT count(*) FROM " + table).fetchone()[0]
                          for table in ("reports", "offers", "audit_log", "requests")}
        before = counts()
        cases = [("synthetic_0018_report", "今天娱乐首单已无，请报备", "report"),
                 ("synthetic_0040_query", "帮我查今天娱乐首单的状态", "query"),
                 ("duplicate_text_query", "帮我查今天娱乐首单的状态", "query"),
                 ("chat", "今天心情不错，陪我聊一句", "chat")]
        identifiers = [peer.add(text, reporter) for _, text, _ in cases]
        expected_sources = dict(zip(identifiers, deepcopy(list(peer.messages.values()))))
        assert inbox.poll().new_requests == len(cases)
        results = []
        report_fact = None
        for (name, text, category), identifier in zip(cases, identifiers):
            started = time.monotonic()
            business.run_once(block=True)
            projection = business.project_outbox()
            frozen = store.get("protocol_frozen_" + identifier)
            failures = []
            if frozen is None:
                failures.append("missing_frozen_reply")
                evidence, final = {}, None
            else:
                evidence, final = frozen["evidence"], frozen["text"]
                queued = outbox.get(frozen["reply_id"])
                intended = expected_sources[identifier]
                identity_fields = ("ownUsername", "threadId", "serverId", "receivedId", "senderId", "sourceSha256")
                if (any(queued["source"].get(key) != intended[key] or frozen["source"].get(key) != intended[key]
                        for key in identity_fields) or queued["text"] != final):
                    failures.append("frozen_identity_or_text_mismatch")
                if queued["state"] != "queued":
                    failures.append("unexpected_transport_state")
                if evidence.get("failure") or not evidence.get("usage_run_id"):
                    failures.append("generation_failed_or_evidence_missing")
            facts = evidence.get("report_facts" if category == "report" else "read_facts", {})
            if category in {"report", "query"}:
                fact = facts.get(item["id"], {})
                if fact.get("day") != business_day(time.time()) or fact.get("status") != "unavailable":
                    failures.append("business_result_mismatch")
                if not unavailable_final_matches(final, day=business_day(time.time()), item=item["name"], category=category):
                    failures.append("final_business_claim_mismatch")
                if category == "report":
                    report_fact = fact
                    receipt = store.receipt(identifier)
                    if not receipt or not receipt.get("recorded"):
                        failures.append("report_not_committed")
                    if store.conn.execute("SELECT count(*) FROM audit_log WHERE kind='report' AND event_id=?", (identifier,)).fetchone()[0] != 1:
                        failures.append("report_audit_count_mismatch")
                elif not report_fact or fact.get("version") != report_fact.get("version"):
                    failures.append("query_did_not_observe_preceding_report")
            if projection["errors"]:
                failures.append("outbox_projection_failed")
            result = {"case": name, "category": category, "request_id": identifier,
                      "final": final, "evidence": evidence, "passed": not failures, "failures": failures,
                      "business_freeze_seconds": time.monotonic() - started,
                      "metrics": store.get("protocol_metrics_" + identifier)}
            results.append(result)
            print(json.dumps({key: result[key] for key in ("case", "passed", "failures", "business_freeze_seconds")}), flush=True)
        stable = counts(); calls = len(model_requests)
        stack.close()
        # Reopen every persistent component; this is a local file-reopen test,
        # not a claim of real process kill/restart or remote gateway recovery.
        store = BotStore(state); stack.callback(store.close)
        resources = ProviderResources(store, state_dir=state, usage_dir=output / "usage", dashboard_dir=Path(state) / "dashboard")
        stack.callback(resources.close)
        inbox = ProtocolNativeInbox(peer, path=output / "inbox.sqlite", own_username=own, consumer="isolated-business")
        stack.callback(inbox.close)
        outbox = ProtocolOutbox(output / "outbox.sqlite", account_id=peer.account_id, own_username=own)
        stack.callback(outbox.close)
        def forbidden_provider(_):
            model_requests.append(time.time())
            raise AssertionError("frozen_reply_must_not_regenerate")
        history = ProtocolHistoryStore(store, outbox, own_username=own, account_id=peer.account_id)
        generator = ReplyGenerator(history, state_dir=state, provider_factory=forbidden_provider)
        business = worker()
        business.run_once(block=True); business.project_outbox()
        assert inbox.poll().new_requests == 0
        recovered = counts()
        recovery = {"unchanged_ledger": stable == recovered, "no_new_model": calls == len(model_requests),
                    "frozen_replies": len(outbox.pending()), "report_rows_added": recovered["reports"] - before["reports"]}
        passed = all(r["passed"] for r in results) and recovery == {
            "unchanged_ledger": True, "no_new_model": True, "frozen_replies": 4, "report_rows_added": 1}
        report = {"schema": 1, "passed": passed, "transport": "synthetic memory receive; no send capability",
                  "wechat_sent": False, "phone_received": False, "model_profile": {key: binding["profile"].get(key)
                  for key in ("id", "kind", "model", "reasoning_effort")}, "cases": results,
                  "before_counts": before, "after_counts": recovered, "recovery": recovery,
                  "recovery_kind": "business, inbox and outbox files reopened; no process-crash or remote recovery claim"}
        path = output / "report.json"
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n"); path.chmod(0o600)
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ("source-state", "output"):
        parser.add_argument("--" + key, type=Path, required=True)
    for key in ("group-ref", "own", "thread"):
        parser.add_argument("--" + key, required=True)
    parser.add_argument("--real-model", action="store_true")
    args = parser.parse_args()
    if not args.real_model:
        parser.error("--real-model explicitly authorizes model calls on a fresh isolated copy")
    state = clone_state(args.source_state, args.output)
    report = run_samples(state, args.output, group_ref=args.group_ref, own=args.own, thread=args.thread)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
