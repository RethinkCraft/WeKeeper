"""Failed generations freeze accounting before a restart can replay projection."""
import json
import sqlite3

import httpx
import pytest

from chatlog_keeper.bot.reply_generator import ReplyGenerator
from chatlog_keeper.bot.protocol_business import ProtocolBusinessWorker
from chatlog_keeper.bot.store import BotStore
from chatlog_keeper.bot.usage_actor import UsageLedgerActor
from test_bot_direct_business_plan import admit_direct, make_runner
from test_bot_protocol_business import ACCOUNT, GROUPS, OWN, harness
from test_bot_required_read import compatible


@pytest.mark.parametrize("route", ["model", "local_report"])
def test_failed_generation_usage_survives_frozen_restart_once(harness, tmp_path, route):
    h = harness
    usage_dir = tmp_path / "usage"
    actor = UsageLedgerActor(usage_dir)
    attempts = []
    if route == "local_report":
        key, worker, _ = admit_direct(h, "今天娱乐首单已无，请报备")
        def lose_tool_response(service, name, arguments):
            attempts.append(name)
            assert service.call(name, arguments)["recorded"]
            raise OSError("synthetic lost report response")
        runner, _, _, _ = make_runner(h, usage_dir, callback=lose_tool_response)
    else:
        key = h.inbox.add("70", "查询娱乐首单")
        worker = h.worker()
        def unavailable(request):
            attempts.append("model")
            return httpx.Response(429, json={"usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}})
        provider = compatible(unavailable)
        provider.ledger = actor
        runner = ReplyGenerator(h.store, state_dir=h.store.directory.parent, provider_factory=lambda _: provider)
    worker.generator = runner
    worker.usage_ledger = actor
    try:
        worker.run_once(block=True)
        frozen = h.store.get("protocol_frozen_" + key)
        assert frozen["evidence"]["deferred_usage"]
        assert len(attempts) == 1
        if route == "local_report":
            assert h.store.receipt(key)["version"] == 1
        worker.close()
        restarted = h.worker(usage_ledger=actor)
        restarted.run_once(block=True)
        assert restarted.project_outbox()["errors"] == []
        assert restarted.project_outbox()["errors"] == []
        assert len(attempts) == 1 and h.generator.calls == []
        with sqlite3.connect(usage_dir / "usage.sqlite") as database:
            assert database.execute("SELECT count(*) FROM usage_runs").fetchone()[0] == 1
            assert database.execute("SELECT count(*) FROM usage_calls").fetchone()[0] == (1 if route == "model" else 0)
        assert h.store.get("protocol_usage_flushed_" + key)
        restarted.close()
    finally:
        worker.close(); actor.close()


@pytest.mark.parametrize("cancel", ["configuration", "quarantine"])
def test_cancelled_model_usage_is_durable_without_a_sendable_final(harness, tmp_path, cancel):
    h = harness
    usage_dir = tmp_path / "cancelled-usage"
    actor = UsageLedgerActor(usage_dir)
    key = h.inbox.add("80", "你好")
    attempts = []
    def remote(request):
        attempts.append(True)
        if cancel == "configuration":
            config = h.store.config(); config["enabled"] = False
            h.store.configure(config)
        else:
            h.inbox.quarantine(key)
        return httpx.Response(200, json={"choices": [{"message": {"content": "在呢。"}}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}})
    provider = compatible(remote); provider.ledger = actor
    runner = ReplyGenerator(h.store, state_dir=h.store.directory.parent, provider_factory=lambda _: provider)
    worker = h.worker(usage_ledger=actor); worker.generator = runner
    reopened = restarted = None
    try:
        worker.run_once(block=True)
        assert len(attempts) == 1 and h.store.request(key)["status"] == "cancelled"
        assert h.store.get("protocol_usage_pending_" + key)
        assert h.store.get("protocol_frozen_" + key) is None and h.outbox.pending() == []
        worker.close()
        reopened = BotStore(h.store.directory.parent)
        restarted = ProtocolBusinessWorker(inbox=h.inbox, store=reopened, generator=h.generator,
            outbox=h.outbox, bindings=GROUPS, own_username=OWN, account_id=ACCOUNT, usage_ledger=actor)
        restarted.run_once(block=True)
        assert restarted.project_outbox() == {"projected": 0, "errors": []}
        assert restarted.project_outbox() == {"projected": 0, "errors": []}
        with sqlite3.connect(usage_dir / "usage.sqlite") as database:
            assert database.execute("SELECT count(*) FROM usage_runs").fetchone()[0] == 1
            assert database.execute("SELECT count(*) FROM usage_calls").fetchone()[0] == 1
            usage = json.loads(database.execute("SELECT usage FROM usage_calls").fetchone()[0])
            assert usage["total_tokens"] == 7
        assert h.outbox._db.execute("SELECT count(*) FROM replies").fetchone()[0] == 0
        assert len(attempts) == 1 and h.generator.calls == []
    finally:
        if restarted is not None: restarted.close()
        if reopened is not None: reopened.close()
        worker.close(); actor.close()
