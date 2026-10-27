import copy
import json

from chatlog_keeper.dashboard.siwc import SIWCError
from chatlog_keeper.dashboard.summaries import SummaryWorker, canonical_hash


def partition(text="discussion", revision=1):
    data = {"group": {"group_ref": "group_one", "name": "Test"}, "day": "2026-10-26",
            "messages": [{"message_ref": "msg_one", "create_time": 1793000000,
                          "sender_id": "sender", "sender_name": "Name", "msg_type": 1, "text": text, "reply": None}],
            "coverage": {"local_backfill_complete": True}}
    return {"source_id": "source", "group_ref": "group_one", "day": data["day"],
            "revision": revision, "content_hash": canonical_hash(data), "initial": True, "data": data}


class Client:
    def __init__(self):
        self.calls = []
        self.error = None

    def status(self):
        return {"state": "ready", "model": "test-model"}

    def summarize(self, messages, **kwargs):
        self.calls.append(messages)
        if self.error:
            raise self.error
        return {"topics": [{"text": "discussion", "message_refs": [messages[0]["message_ref"]]}], "decisions": [], "todos": []}


def test_summary_restart_hash_dedupe_and_safe_ack(tmp_path):
    client = Client()
    worker = SummaryWorker(tmp_path, "source", client=client)
    assert worker.run_once([partition()])["completed"] == 1
    envelope = worker.pending()[0]
    assert envelope["input_hash"] == partition()["content_hash"]
    assert envelope["covered_until"] == 1793000000
    worker.close()
    worker = SummaryWorker(tmp_path, "source", client=client)
    assert worker.run_once([partition()])["skipped"] == 1
    assert len(client.calls) == 1
    worker.acknowledge("group_one", "2026-10-26", 0)
    assert len(worker.pending()) == 1
    worker.acknowledge("group_one", "2026-10-26", 1)
    assert not worker.pending()
    worker.run_once([partition("corrected", revision=2)])
    assert len(worker.pending()) == 1
    assert worker.pending()[0]["revision"] == 2
    worker.close()


def test_quota_pause_is_durable_and_requires_explicit_resume(tmp_path):
    client = Client()
    worker = SummaryWorker(tmp_path, "source", client=client)
    worker.run_once([partition()])
    previous = worker.pending()[0]
    client.error = SIWCError("subscription_sharing_usage_limit_exceeded", paused=True)
    assert worker.run_once([partition("new", 2)])["state"] == "paused"
    assert worker.pending()[0] == previous
    worker.close()
    worker = SummaryWorker(tmp_path, "source", client=client)
    client.error = None
    calls = len(client.calls)
    worker.run_once([partition("new", 2)])
    assert len(client.calls) == calls
    worker.resume()
    worker.run_once([partition("new", 2)])
    assert worker.pending()[0]["revision"] == 2
    worker.close()


def test_chunk_retry_reuses_completed_work_and_preserves_old_success(tmp_path):
    class Flaky(Client):
        def summarize(self, messages, **kwargs):
            if len(self.calls) == 1:
                self.calls.append(messages)
                raise SIWCError("siwc_network_error", retryable=True)
            return super().summarize(messages, **kwargs)
    client = Flaky()
    clock = [1000.0]
    worker = SummaryWorker(tmp_path, "source", client=client, clock=lambda: clock[0])
    data = partition("a" * 50000)
    assert worker.run_once([data])["state"] == "error"
    assert not worker.pending()
    assert len(client.calls) == 2
    clock[0] += 60
    assert worker.run_once([data])["state"] == "ready"
    assert sum(call[0].get("text_segment", {}).get("index") == 1 for call in client.calls) == 1
    # Smaller incremental batches still cover every slice of the long message.
    successful = {message["text_segment"]["index"]: message["text"] for call in client.calls for message in call}
    assert "".join(successful[index] for index in sorted(successful)) == "a" * 50000
    assert len(worker.pending()) == 1
    worker.close()


def test_rejects_bad_partition_hash_without_calling_model(tmp_path):
    client = Client()
    worker = SummaryWorker(tmp_path, "source", client=client)
    value = partition()
    value["data"]["messages"][0]["text"] = "tampered"
    assert worker.run_once([value])["state"] == "error"
    assert not client.calls and not worker.pending()
    worker.close()


def test_fabricated_evidence_does_not_advance_summary(tmp_path):
    class Hallucinating(Client):
        def summarize(self, messages, **kwargs):
            return {"topics": [{"text": "invented", "message_refs": ["msg_other"]}], "decisions": [], "todos": []}
    worker = SummaryWorker(tmp_path, "source", client=Hallucinating())
    assert worker.run_once([partition()])["state"] == "error"
    assert not worker.pending()
    worker.close()


def test_empty_day_is_completed_without_model_use(tmp_path):
    client = Client()
    worker = SummaryWorker(tmp_path, "source", client=client)
    value = partition()
    value["data"]["messages"] = []
    value["content_hash"] = canonical_hash(value["data"])
    assert worker.run_once([value])["state"] == "ready"
    assert not client.calls
    assert worker.pending()[0]["summary"] == {"topics": [], "decisions": [], "todos": []}
    worker.close()


def test_pending_filters_allowlist_before_limit_and_empty_means_none(tmp_path):
    worker = SummaryWorker(tmp_path, "source", client=Client())
    with worker.conn:
        for index in range(501):
            group = f"group_removed_{index:04}"
            worker.conn.execute("INSERT INTO summary_jobs VALUES(?,?,?,?,?,?,0)",
                                (group, "2026-10-25", "hash", 1, "model", json.dumps({"group_ref": group})))
        worker.conn.execute("INSERT INTO summary_jobs VALUES(?,?,?,?,?,?,0)",
                            ("group_allowed", "2026-10-26", "hash", 1, "model", '{"group_ref":"group_allowed"}'))
    assert len(worker.pending(limit=500)) == 500
    assert worker.pending(limit=1, group_refs=["group_allowed"]) == [{"group_ref": "group_allowed"}]
    assert worker.pending(limit=500, group_refs=[]) == []
    worker.close()


def test_operational_metadata_changes_do_not_request_inference(tmp_path):
    client = Client()
    worker = SummaryWorker(tmp_path, "source", client=client)
    value = partition()
    worker.run_once([value])
    original_calls = len(client.calls)
    value["revision"] += 1
    value["data"]["group"]["name"] = "Renamed display label"
    value["data"]["coverage"] = {"local_backfill_complete": False, "source_available": False}
    value["content_hash"] = canonical_hash(value["data"])
    assert worker.run_once([value])["state"] == "ready"
    assert len(client.calls) == original_calls
    assert worker.pending()[0]["input_hash"] == value["content_hash"]
    worker.close()


def test_high_volume_persistent_backlog_never_drops_messages_after_thousand(tmp_path):
    class Capturing(Client):
        def __init__(self):
            super().__init__()
            self.requests = []
        def summarize(self, messages, **kwargs):
            self.requests.append(json.loads(json.dumps({"messages": messages, "context": kwargs["context"]})))
            return super().summarize(messages, **kwargs)
    client = Capturing()
    worker = SummaryWorker(tmp_path, "source", client=client)
    value = partition()
    template = value["data"]["messages"][0]
    value["data"]["messages"] = [dict(template, message_ref=f"msg_{index:04}", text=f"evidence {index}", create_time=1793000000 + index) for index in range(1201)]
    value["content_hash"] = canonical_hash(value["data"])
    for _ in range(30):
        state = worker.run_once([value], baseline_days={"2026-10-26"}, max_batches=3)
        if state["state"] == "ready":
            break
    assert state["state"] == "ready"
    assert worker.status()["pending_events"] == 0
    assert worker.status()["checkpoint_seq"] == 1201
    observed = [message["message_ref"] for request in client.requests for message in request["messages"]]
    assert len(observed) == 1201 and len(set(observed)) == 1201
    for request in client.requests:
        assert len(request["messages"]) <= 500
        assert len(json.dumps(request, ensure_ascii=False, separators=(",", ":"))) <= 20000
        assert len(request["context"]["background"]) <= 50
        assert len(json.dumps(request["context"]["memory"], ensure_ascii=False)) <= 4000
    assert worker.pending()[0]["processing"]["baseline_complete"]
    worker.close()


def test_old_day_initially_skipped_but_later_correction_and_deletion_are_processed(tmp_path):
    client = Client()
    worker = SummaryWorker(tmp_path, "source", client=client)
    today, old = partition("today"), partition("old history")
    old["day"] = old["data"]["day"] = "2026-10-01"
    old["data"]["messages"][0].update(message_ref="msg_old", create_time=1759334400)
    old["content_hash"] = canonical_hash(old["data"])
    worker.run_once([old, today], baseline_days={"2026-10-26"})
    assert not any(message["message_ref"] == "msg_old" for call in client.calls for message in call)
    old["data"]["messages"][0]["text"] = "corrected history"
    old["revision"] = 2
    old["content_hash"] = canonical_hash(old["data"])
    worker.run_once([old, today], baseline_days={"2026-10-26"})
    assert any(message["message_ref"] == "msg_old" for call in client.calls for message in call)
    result = next(item for item in worker.pending() if item["day"] == "2026-10-01")
    assert result["processing"]["baseline_complete"] is False
    old["data"]["messages"] = []
    old["revision"] = 3
    old["content_hash"] = canonical_hash(old["data"])
    worker.run_once([old, today], baseline_days={"2026-10-26"})
    result = next(item for item in worker.pending() if item["day"] == "2026-10-01")
    assert result["summary"] == {"topics": [], "decisions": [], "todos": []}
    worker.close()


def test_same_invalid_input_has_only_two_automatic_retries(tmp_path):
    client = Client()
    client.error = SIWCError("invalid_summary_json")
    now = [1000.0]
    worker = SummaryWorker(tmp_path, "source", client=client, clock=lambda: now[0])
    for current in [1000, 1030, 1060, 1100, 1360, 1500, 5000]:
        now[0] = current
        worker.run_once([partition()])
    assert len(client.calls) == 3  # Initial attempt plus at most two retries.
    assert worker.status()["checkpoint_seq"] == 0
    assert not worker.pending()
    client.error = None
    worker.resume()
    worker.run_once([partition()])
    assert worker.status()["checkpoint_seq"] == 1
    worker.close()


def test_chat_marker_yields_after_completed_batch_without_losing_checkpoint(tmp_path):
    class Yielding(Client):
        def summarize(self, messages, **kwargs):
            result = super().summarize(messages, **kwargs)
            if len(self.calls) == 1:
                (tmp_path / "chat-waiting").touch()
            return result
    client = Yielding()
    worker = SummaryWorker(tmp_path, "source", client=client)
    value = partition("a" * 50000)
    state = worker.run_once([value])
    assert state["state"] == "partial" and state["yielded"]
    assert len(client.calls) == 1 and worker.status()["pending_events"] == 1
    assert not worker.pending()
    worker.close()
    (tmp_path / "chat-waiting").unlink()
    worker = SummaryWorker(tmp_path, "source", client=client)
    assert worker.run_once([value])["state"] == "ready"
    assert worker.status()["pending_events"] == 0
    worker.close()


def test_old_correction_invalidates_transitive_memory_suffix(tmp_path):
    class Dependent(Client):
        def summarize(self, messages, **kwargs):
            self.calls.append(messages)
            old = json.dumps(kwargs["context"]["memory"], ensure_ascii=False)
            source = next((message["text"] for message in messages if message["message_ref"] == "msg_a"), "new A" if "new A" in old else "old A")
            return {"topics": [{"text": message["text"] if message["message_ref"] == "msg_a" else message["message_ref"] + " based on " + source,
                                "message_refs": [message["message_ref"]]} for message in messages], "decisions": [], "todos": []}
    client = Dependent()
    worker = SummaryWorker(tmp_path, "source", client=client)
    value = partition("old A")
    value["data"]["messages"][0]["message_ref"] = "msg_a"
    for index, ref in enumerate(("msg_a", "msg_b", "msg_c")):
        if index:
            value["data"]["messages"].append(dict(value["data"]["messages"][0], message_ref=ref, text=ref, create_time=1793000000 + index))
        value["revision"] = index + 1
        value["content_hash"] = canonical_hash(value["data"])
        worker.run_once([value])
    # Model a bounded-memory chain where C directly knows B but A has fallen
    # outside its direct evidence window. Parent-segment invalidation must still
    # invalidate C when A changes.
    with worker.conn:
        rows = worker.conn.execute("SELECT segment_id,input_refs FROM summary_v2_segments WHERE stale=0").fetchall()
        for row in rows:
            if json.loads(row["input_refs"]) == ["msg_c"]:
                worker.conn.execute("UPDATE summary_v2_segments SET dependencies=? WHERE segment_id=?",
                                    (json.dumps({"msg_b": "hash", "msg_c": "hash"}), row["segment_id"]))
    before = len(client.calls)
    value["data"]["messages"][0]["text"] = "new A"
    value["revision"] = 4
    value["content_hash"] = canonical_hash(value["data"])
    assert worker.run_once([value])["state"] == "ready"
    assert len(client.calls) > before
    body = worker.pending()[0]["summary"]
    assert "old A" not in json.dumps(body)
    assert any("msg_c based on new A" == item["text"] for item in body["topics"])
    worker.close()


def test_rebuild_respects_provider_budget_and_resumes_from_chunk_cache(tmp_path):
    client = Client()
    worker = SummaryWorker(tmp_path, "source", client=client)
    value = partition("a" * 50000)
    while worker.run_once([value], max_batches=2)["state"] != "ready":
        pass
    value["data"]["messages"][0]["text"] = "b" * 50000
    value["revision"] = 2
    value["content_hash"] = canonical_hash(value["data"])
    for _ in range(10):
        before = len(client.calls)
        result = worker.run_once([value], max_batches=2)
        assert len(client.calls) - before <= 2
        if result["state"] == "ready":
            break
    assert result["state"] == "ready"
    assert worker.status()["checkpoint_seq"] == 2
    worker.close()


def test_summary_crash_recovery_runs_only_after_worker_lock_and_preserves_chat(tmp_path):
    from chatlog_keeper.dashboard.usage import UsageLedger
    ledger = UsageLedger(tmp_path)
    ledger.start_run(run_id="summary-crash", kind="summary", model="test")
    ledger.record_call("summary-crash", usage={"input_tokens": 5, "output_tokens": 2, "total_tokens": 7})
    ledger.start_run(run_id="active-chat", kind="chat", model="test", turn_id="turn")
    worker = SummaryWorker(tmp_path, "source", client=Client())
    assert len(ledger.list_running()) == 2  # Constructors and sync readers do not recover.
    worker.run_once([])
    assert ledger.get("summary-crash")["status"] == "interrupted"
    assert ledger.get("summary-crash")["usage"]["total_tokens"] == 7
    assert ledger.get("active-chat")["status"] == "running"
    worker.close()
    ledger.close()


def test_shared_pause_is_sticky_until_explicit_resume(tmp_path):
    one = SummaryWorker(tmp_path, "source", client=Client())
    two = SummaryWorker(tmp_path, "source", client=Client())
    one.record_error(SIWCError("subscription_sharing_usage_limit_exceeded", paused=True))
    assert two._set_state(state="ready", paused=False, error=None)["state"] == "paused"
    assert two.record_error(SIWCError("invalid_summary_json"))["paused"]
    assert two.status()["error"]["code"] == "subscription_sharing_usage_limit_exceeded"
    one.resume()
    assert two.status()["paused"] is False
    one.close()
    two.close()


def test_pause_written_during_batch_completion_is_not_cleared(tmp_path):
    other = SummaryWorker(tmp_path, "source", client=Client())
    class Pausing(Client):
        def summarize(self, messages, **kwargs):
            result = super().summarize(messages, **kwargs)
            other.record_error(SIWCError("siwc_login_required", paused=True))
            return result
    client = Pausing()
    worker = SummaryWorker(tmp_path, "source", client=client)
    assert worker.run_once([partition("a" * 50000)])["state"] == "paused"
    assert len(client.calls) == 1
    assert worker.status()["error"]["code"] == "siwc_login_required"
    other.close()
    worker.close()
