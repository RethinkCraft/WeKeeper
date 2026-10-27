from chatlog_keeper.dashboard.usage import UsageLedger, normalize_usage


def test_unknown_usage_is_not_zero_and_provider_details_are_normalized(tmp_path):
    ledger = UsageLedger(tmp_path)
    run = ledger.start_run(run_id="unknown", kind="summary", model="test-model")
    ledger.record_call(run, request_id="request", response_id="response", usage=None)
    ledger.finish_run(run, status="failed")
    assert ledger.pending()[0]["usage"] is None
    assert normalize_usage({"input_tokens": 20, "output_tokens": 4, "total_tokens": 24,
                            "input_tokens_details": {"cached_tokens": 10},
                            "output_tokens_details": {"reasoning_tokens": 2}}) == {
        "input_tokens": 20, "cached_input_tokens": 10, "output_tokens": 4,
        "reasoning_output_tokens": 2, "total_tokens": 24}
    ledger.close()


def test_started_calls_survive_crash_and_can_be_marked_interrupted(tmp_path):
    ledger = UsageLedger(tmp_path)
    run = ledger.start_run(run_id="interrupted", kind="chat", model="test", user_id=3, turn_id="turn", source_id="source")
    ledger.record_call(run, usage={"input_tokens": 5, "output_tokens": 2, "total_tokens": 7})
    assert not ledger.pending()
    ledger.close()
    ledger = UsageLedger(tmp_path)
    assert ledger.list_running("turn")[0]["run_id"] == run
    ledger.interrupt_turn("turn")
    output = ledger.pending()[0]
    assert output["status"] == "interrupted" and output["usage"]["total_tokens"] == 7
    assert output["user_id"] == 3 and output["turn_id"] == "turn"
    ledger.acknowledge(run)
    assert not ledger.pending() and ledger.list_runs()[0] == output
    # Already uploaded terminal records remain immutable.
    ledger.finish_run(run, status="completed", usage={"total_tokens": 999})
    assert ledger.get(run) == output
    ledger.close()
