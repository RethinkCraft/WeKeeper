"""Independent bot accounting uploads through the existing dashboard source."""
import json

import httpx
import pytest

from chatlog_keeper.dashboard.local_state import LocalState
from chatlog_keeper.dashboard.usage import UsageLedger
from chatlog_keeper.dashboard.usage_sync import upload_usage


def bot_run(ledger, identity="bot_1", **changes):
    value = dict(run_id=identity, kind="bot_chat", model="synthetic", provider="deepseek",
                 profile_id="profile_a", group_ref="group_a", bot_request_id="native_1",
                 usage={"input_tokens": 3, "output_tokens": 2, "total_tokens": 5})
    ledger.record_run(**{**value, **changes})


def test_independent_bot_ledger_keeps_source_identity_path_and_known_unknown_usage(tmp_path):
    state = LocalState(tmp_path / "dashboard")
    directory = tmp_path / "business-runtime" / "usage"
    ledger = UsageLedger(directory)
    bot_run(ledger)
    bot_run(ledger, "unknown", usage=None)
    ledger.start_run(run_id="in_progress", kind="bot_chat", model="synthetic", group_ref="group_a", bot_request_id="native_2")
    calls = []
    def post(route, value):
        assert route == "/v1/ingest/usage"
        calls.append(value)
        return {"accepted": True}
    try:
        source = state.source_id
        assert upload_usage(directory, state=state, post=post, clock=lambda: 200000) == 2
        assert calls[0]["source_id"] == source == state.source_id
        runs = {run["run_id"]: run for run in calls[0]["runs"]}
        assert runs["bot_1"]["usage"]["total_tokens"] == 5
        assert runs["unknown"]["usage"] is None
        assert runs["bot_1"]["provider"] == "deepseek" and runs["bot_1"]["group_ref"] == "group_a"
        assert "in_progress" not in runs
        assert not ledger.pending()
        assert not (directory / "sync.sqlite").exists()
        assert not (state.directory / "usage.sqlite").exists()
        assert upload_usage(directory, state=state, post=post, clock=lambda: 200001) == 0
    finally:
        ledger.close(); state.close()


def test_main_and_bot_ledgers_have_independent_restore_reconciliation(tmp_path):
    state = LocalState(tmp_path / "dashboard")
    bot_directory = tmp_path / "separate-usage"
    calls = []
    def post(_route, value):
        calls.append(value)
        return {"accepted": True}
    try:
        for directory, identity in ((state.directory, "main"), (bot_directory, "bot")):
            ledger = UsageLedger(directory)
            bot_run(ledger, identity)
            ledger.acknowledge(identity)
            ledger.close()
        assert upload_usage(state.directory, state=state, post=post, clock=lambda: 200000) == 1
        assert state.get("last_usage_reconcile") == 200000
        assert upload_usage(bot_directory, state=state, post=post, clock=lambda: 200001) == 1
        assert [value["runs"][0]["run_id"] for value in calls] == ["main", "bot"]
        assert len({value["source_id"] for value in calls}) == 1
        assert upload_usage(bot_directory, state=state, post=post, clock=lambda: 200002) == 0
        assert upload_usage(bot_directory, state=state, post=post, clock=lambda: 300000) == 1
    finally:
        state.close()


def test_invalid_restored_run_is_isolated_and_valid_bot_accounting_is_acknowledged(tmp_path):
    state = LocalState(tmp_path / "dashboard")
    directory = tmp_path / "usage"
    ledger = UsageLedger(directory)
    ledger.record_run(run_id="orphan_chat", kind="chat", model="synthetic", user_id=1, turn_id="missing_turn")
    bot_run(ledger)
    calls = []
    def post(route, value):
        identifiers = [run["run_id"] for run in value["runs"]]
        calls.append(identifiers)
        if "orphan_chat" in identifiers:
            response = httpx.Response(400, json={"detail": "usage_turn_owner_mismatch"}, request=httpx.Request("POST", "https://synthetic.invalid" + route))
            response.raise_for_status()
        return {"accepted": True}
    try:
        with pytest.raises(httpx.HTTPStatusError):
            upload_usage(directory, state=state, post=post, clock=lambda: 200000)
        assert sorted(calls[0]) == ["bot_1", "orphan_chat"]
        assert ["bot_1"] in calls and ["orphan_chat"] in calls
        assert [run["run_id"] for run in ledger.pending()] == ["orphan_chat"]
        assert not state.get("last_usage_reconcile")
    finally:
        ledger.close(); state.close()


def test_lost_ack_replays_identical_records_without_losing_accounting(tmp_path):
    state = LocalState(tmp_path / "dashboard")
    directory = tmp_path / "usage"
    ledger = UsageLedger(directory)
    bot_run(ledger)
    calls, accepted = [], {}
    def post(route, value):
        calls.append(json.dumps(value, sort_keys=True))
        for run in value["runs"]:
            accepted[run["run_id"]] = run
        if len(calls) == 1:
            raise httpx.ReadTimeout("synthetic lost ACK")
        return {"accepted": True}
    try:
        with pytest.raises(httpx.ReadTimeout):
            upload_usage(directory, state=state, post=post, clock=lambda: 200000)
        assert len(ledger.pending()) == 1
        assert upload_usage(directory, state=state, post=post, clock=lambda: 200001) == 1
        assert calls[0] == calls[1] and len(accepted) == 1
        assert not ledger.pending()
    finally:
        ledger.close(); state.close()


def test_batches_remain_bounded_and_local_business_audits_stay_local(tmp_path):
    state = LocalState(tmp_path / "dashboard")
    directory = tmp_path / "usage"
    ledger = UsageLedger(directory)
    for index in range(101):
        bot_run(ledger, f"bot_{index}")
    ledger.record_run(run_id="no_model", kind="local_business", model="local-business",
                      provider="local-business", model_invoked=False, group_ref="group_a", bot_request_id="native_local")
    calls = []
    def post(_route, value):
        calls.append(value)
        return {"accepted": True}
    try:
        assert upload_usage(directory, state=state, post=post, clock=lambda: 200000) == 101
        assert [len(value["runs"]) for value in calls] == [100, 1]
        assert all(run["kind"] == "bot_chat" for value in calls for run in value["runs"])
        assert not ledger.pending()
        assert ledger.get("no_model")["usage"]["total_tokens"] == 0
    finally:
        ledger.close(); state.close()


def test_uploader_keeps_existing_symlink_path_protection(tmp_path):
    state = LocalState(tmp_path / "dashboard")
    ledger = UsageLedger(tmp_path / "real-usage")
    ledger.close()
    linked = tmp_path / "linked-usage"
    linked.symlink_to(tmp_path / "real-usage", target_is_directory=True)
    try:
        with pytest.raises(ValueError, match="unsafe_usage_state"):
            upload_usage(linked, state=state, post=lambda *_: pytest.fail("must not upload"))
    finally:
        state.close()
