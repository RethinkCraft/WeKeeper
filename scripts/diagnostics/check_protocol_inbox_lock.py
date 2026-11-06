#!/usr/bin/env python3
"""Fixed 300 ms in-memory receive wait against the actual NativeInbox.poll.

Run the identical script before and after a lock change. This is local lock
contention evidence, not a model/network/WeChat or phone latency benchmark.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from scripts.diagnostics.check_protocol_business import MemoryReceivePeer
from chatlog_keeper.bot.protocol_business import ProtocolBusinessWorker
from chatlog_keeper.bot.protocol_gateway import SendResult
from chatlog_keeper.bot.protocol_native_inbox import ProtocolNativeInbox
from chatlog_keeper.bot.protocol_outbox import ProtocolOutbox
from chatlog_keeper.bot.reply_generator import GenerationResult
from chatlog_keeper.bot.store import BotStore


class BlockingPeer(MemoryReceivePeer):
    def __init__(self):
        super().__init__("synthetic-robot", "synthetic-lock-probe@chatroom")
        self.block = False
        self.entered, self.release = threading.Event(), threading.Event()

    def activate_native_consumer(self, consumer, *, own_username):
        if self.block:
            self.entered.set()
            if not self.release.wait(5):
                raise AssertionError("fixture_release_missing")
        return super().activate_native_consumer(consumer, own_username=own_username)


class Generator:
    def __init__(self):
        self.entered, self.calls = threading.Event(), []

    async def generate(self, event, request, nearby, progress=None):
        self.calls.append(event["event_id"])
        self.entered.set()
        return GenerationResult("synthetic final", {"request": {"event_id": event["event_id"]}}, {})


class SyntheticSender:
    """Immediate in-memory acceptance, checking the exact frozen native source."""
    def __init__(self, peer):
        self.peer, self.account_id, self.base_url = peer, peer.account_id, peer.base_url
        self.calls = []

    def require_account(self, own):
        assert own == self.peer.own

    def send_received_quote(self, *, thread_id, received_id, server_id, source_sha256, text, idempotency_key):
        source = self.peer.messages[received_id]
        assert (thread_id, server_id, source_sha256) == (source["threadId"], source["serverId"], source["sourceSha256"])
        self.calls.append(idempotency_key)
        return SendResult(idempotency_key, "accepted", {"id": "synthetic-send-" + idempotency_key,
            "accountId": self.account_id, "destination": thread_id, "state": "accepted",
            "clientId": "synthetic-client-" + idempotency_key, "serverId": "17000000000000000001"})


def run_one():
    with tempfile.TemporaryDirectory(prefix="wekeeper-inbox-lock-") as directory, ExitStack() as stack:
        root = Path(directory)
        store = BotStore(root / "state"); stack.callback(store.close)
        peer = BlockingPeer()
        store.configure({"enabled": True, "providers": [{"id": "fixture", "kind": "siwc", "model": "never-opened"}],
            "businesses": [{"id": "fixture", "name": "fixture", "reporter_ids": ["synthetic-manager"],
                            "items": [{"id": "item", "name": "item"}]}],
            "groups": [{"group_ref": "fixture", "enabled": True, "business_id": "fixture", "provider_id": "fixture",
                        "persona_id": "alpha", "bot_wxid": peer.own}]})
        inbox = ProtocolNativeInbox(peer, path=root / "inbox.sqlite", own_username=peer.own, consumer="fixture")
        stack.callback(inbox.close); inbox.activate()
        outbox = ProtocolOutbox(root / "outbox.sqlite", account_id=peer.account_id, own_username=peer.own)
        stack.callback(outbox.close)
        generator = Generator()
        worker = ProtocolBusinessWorker(inbox=inbox, store=store, generator=generator, outbox=outbox,
            bindings={peer.thread: "fixture"}, own_username=peer.own, account_id=peer.account_id)
        stack.callback(worker.close)
        first = peer.add("same synthetic body", "synthetic-manager")
        inbox.poll(); worker.run_once(block=True); worker.project_outbox()
        input_started = time.monotonic()
        current = peer.add("same synthetic body", "synthetic-manager")
        inbox.poll(); generator.entered.clear()
        sender = SyntheticSender(peer)
        admitted, source_attempted = threading.Event(), threading.Event()
        errors, timings = [], {}
        original_admit, original_source = worker.admit_pending, inbox.source_quarantine_reason

        def admit():
            result = original_admit()
            timings["admitted"] = time.monotonic(); admitted.set()
            if not peer.entered.wait(3):
                raise AssertionError("fixture_poll_not_entered")
            return result

        def source(received):
            source_attempted.set()
            return original_source(received)

        worker.admit_pending, inbox.source_quarantine_reason = admit, source

        def business():
            try:
                worker.run_once(block=True)
                timings["final_frozen"] = time.monotonic()
                worker.project_outbox()
                final = store.get("protocol_frozen_" + current)
                assert outbox.deliver_one(sender, final["reply_id"])["state"] == "accepted"
                timings["fake_accepted"] = time.monotonic()
            except Exception as error:
                errors.append(type(error).__name__)
            finally:
                timings["business_done"] = time.monotonic()

        def receive():
            try:
                if not admitted.wait(3):
                    raise AssertionError("fixture_not_admitted")
                peer.block = True
                inbox.poll()  # Always the real implementation, never a lock bypass.
            except Exception as error:
                errors.append(type(error).__name__)

        receiver = threading.Thread(target=receive)
        runner = threading.Thread(target=business)
        receiver.start(); runner.start()
        try:
            if not source_attempted.wait(3):
                raise AssertionError("fixture_source_check_missing")
            deadline = time.monotonic() + .300
            generated_before_release = generator.entered.wait(.300)
            threading.Event().wait(max(0, deadline - time.monotonic()))
            frame, names = sys._current_frames().get(runner.ident), []
            while frame:
                names.append(frame.f_code.co_name); frame = frame.f_back
            frozen_before_release = store.get("protocol_frozen_" + current) is not None
            accepted_before_release = "fake_accepted" in timings
        finally:
            timings["network_release"] = time.monotonic(); peer.release.set()
            receiver.join(3); runner.join(3)
        if receiver.is_alive() or runner.is_alive() or errors:
            raise AssertionError("fixture_thread_failure")
        projection = worker.project_outbox()
        trace = store.get("protocol_metrics_" + current)["trace"]
        admission = trace["wall_observations"]["business_admission_before_commit"]["utc"]
        gap = (datetime.fromisoformat(trace["host_utc_anchor"]) - datetime.fromisoformat(admission)).total_seconds()
        replies = [outbox.get(store.get("protocol_frozen_" + key)["reply_id"]) for key in (first, current)]
        correct = {"requests": store.conn.execute("SELECT COUNT(*) FROM requests").fetchone()[0],
            "frozen_replies": len(replies), "generator_calls": len(generator.calls),
            "distinct_generated_requests": len(set(generator.calls)),
            "distinct_quote_sources": len({row["source"]["serverId"] for row in replies}),
            "completed_inbox_requests": sum(inbox.request_state(key) == "completed" for key in (first, current)),
            "acked_cursor": peer.acknowledged, "projection_errors": len(projection["errors"]),
            "business_writes": store.conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0],
            "synthetic_accepted": sum(row["state"] == "accepted" for row in replies),
            "synthetic_send_attempts": len(sender.calls)}
        expected = {"requests": 2, "frozen_replies": 2, "generator_calls": 2, "distinct_generated_requests": 2,
            "distinct_quote_sources": 2, "completed_inbox_requests": 2, "acked_cursor": "2",
            "projection_errors": 0, "business_writes": 0, "synthetic_accepted": 1, "synthetic_send_attempts": 1}
        return {"correctness_passed": correct == expected, "correctness_counts": correct,
            "generated_before_network_release": generated_before_release,
            "final_frozen_before_network_release": frozen_before_release,
            "synthetic_accepted_before_network_release": accepted_before_release,
            "admission_to_generation_anchor_seconds": gap,
            "local_append_to_final_frozen_seconds": timings["final_frozen"] - input_started,
            "local_append_to_synthetic_accepted_seconds": timings["fake_accepted"] - input_started,
            "business_thread_stack_while_network_blocked": names[:8],
            "network_gate_seconds": timings["network_release"] - timings["admitted"],
            "business_complete_seconds": timings["business_done"] - timings["admitted"]}


def run(repeats=3):
    if type(repeats) is not int or not 3 <= repeats <= 20:
        raise ValueError("invalid_fixture_repeats")
    hashes = {name: hashlib.sha256((ROOT / "chatlog_keeper/bot" / name).read_bytes()).hexdigest()
              for name in ("protocol_inbox.py", "protocol_native_inbox.py")}
    rows = [run_one() for _ in range(repeats)]
    return {"schema": 1, "fixture_only": True, "live_network": False, "real_model": False,
            "phone_latency_tested": False, "hold_seconds": .300, "repeats": repeats,
            "acceptance_semantic": "immediate fake acceptance after real local freeze/projection; not network or phone receipt",
            "implementation_sha256": hashes, "correctness_passed": all(row["correctness_passed"] for row in rows),
            "runs": rows}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("output must be a fresh file")
    result = run(args.repeats)
    with args.output.open("x") as stream:
        args.output.chmod(0o600)
        json.dump(result, stream, indent=2); stream.write("\n")
    print(json.dumps(result, indent=2))
    return 0 if result["correctness_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
