"""Upgrade a synthetic deployed installation without touching production state."""
import fcntl
import json
import sqlite3

import pytest

from chatlog_keeper.dashboard.cli import main, parser
from chatlog_keeper.dashboard.inference_state import InferenceState
from chatlog_keeper.dashboard.maintenance import remove_automatic_summaries
from chatlog_keeper.dashboard.protocol import Heartbeat
from chatlog_keeper.dashboard.siwc import SIWCError
from chatlog_keeper.dashboard.store import DashboardStore
from chatlog_keeper.dashboard.usage import UsageLedger
from test_dashboard_store import heartbeat, partition


def legacy_state(directory, *, paused=True):
    directory.mkdir(exist_ok=True)
    with sqlite3.connect(directory / "summary.sqlite") as conn:
        conn.executescript("CREATE TABLE summary_meta(key TEXT PRIMARY KEY,value TEXT); CREATE TABLE summary_jobs(payload TEXT);")
        conn.execute("INSERT INTO summary_meta VALUES('status',?)", (json.dumps({"paused": paused, "state": "paused", "pending": 99, "error": {"code": "siwc_usage_limited", "paused": True}}),))
        conn.execute("INSERT INTO summary_jobs VALUES('discard this generated content')")


def test_local_cleanup_migrates_pause_and_preserves_credentials_chat_and_actual_usage(tmp_path):
    legacy_state(tmp_path)
    for filename in ("agent.sqlite", "sync.sqlite", "siwc/credentials.json"):
        path = tmp_path / filename
        path.parent.mkdir(exist_ok=True)
        path.write_text("preserved")
    ledger = UsageLedger(tmp_path)
    ledger.start_run(run_id="interrupted_summary", kind="summary", model="test")
    ledger.record_call("interrupted_summary", usage={"input_tokens": 3, "output_tokens": 2, "total_tokens": 5})
    ledger.start_run(run_id="chat_running", kind="chat", model="test", user_id=1, turn_id="turn_1")
    ledger.close()
    result = remove_automatic_summaries(tmp_path)
    assert result["inference"]["paused"]
    assert "pending" not in result["inference"]
    assert not (tmp_path / "summary.sqlite").exists()
    assert not (tmp_path / "summary.sqlite-wal").exists()
    for filename in ("agent.sqlite", "sync.sqlite", "siwc/credentials.json"):
        assert (tmp_path / filename).read_text() == "preserved"
    ledger = UsageLedger(tmp_path)
    assert ledger.get("interrupted_summary")["status"] == "interrupted"
    assert ledger.get("interrupted_summary")["usage"]["total_tokens"] == 5
    assert ledger.get("chat_running")["status"] == "running"
    ledger.close()
    state = InferenceState(tmp_path)
    state.resume()
    state.close()
    remove_automatic_summaries(tmp_path)
    state = InferenceState(tmp_path)
    assert not state.status()["paused"]
    state.close()


def test_resume_is_not_overwritten_if_cleanup_is_interrupted_before_unlink(tmp_path):
    legacy_state(tmp_path)
    state = InferenceState(tmp_path)
    assert state.status()["paused"]
    state.resume()
    state.close()
    again = InferenceState(tmp_path)
    assert not again.status()["paused"]
    again.record_error(SIWCError("quota", paused=True))
    again.record_error(SIWCError("temporary", paused=False))
    assert again.status()["error"]["code"] == "quota"
    again.close()


def test_cleanup_refuses_live_worker_and_unsafe_paths_before_deleting(tmp_path):
    legacy_state(tmp_path)
    with (tmp_path / "sync.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="workers_must_be_stopped"):
            remove_automatic_summaries(tmp_path)
    assert (tmp_path / "summary.sqlite").exists()
    (tmp_path / "summary-wake").symlink_to(tmp_path / "protected")
    with pytest.raises(ValueError, match="unsafe_summary_cleanup_path"):
        remove_automatic_summaries(tmp_path)
    assert (tmp_path / "summary.sqlite").exists()


def test_failed_pause_migration_never_discards_summary_database(tmp_path):
    legacy_state(tmp_path)
    with sqlite3.connect(tmp_path / "summary.sqlite") as conn:
        conn.execute("UPDATE summary_meta SET value='malformed'")
    with pytest.raises(ValueError):
        remove_automatic_summaries(tmp_path)
    assert (tmp_path / "summary.sqlite").exists()
    with sqlite3.connect(tmp_path / "summary.sqlite") as conn:
        conn.execute("UPDATE summary_meta SET value=?", ('{"paused":true,"error":{"code":"quota"}}',))
    assert remove_automatic_summaries(tmp_path)["inference"]["paused"]


def test_cloud_schema3_removes_summary_data_and_keeps_messages_users_usage_and_chats(tmp_path):
    store = DashboardStore(tmp_path)
    store.create_user("owner", "long-owner-password")
    store.ingest_partition(partition())
    store.ingest_heartbeat(heartbeat(1000))
    with store.transaction():
        store.conn.execute("CREATE TABLE summaries(payload TEXT)")
        store.conn.execute("INSERT INTO summaries VALUES('obsolete summary content')")
        store.conn.execute("UPDATE sources SET payload=json_set(payload,'$.summary',json(?))", ('{"state":"paused"}',))
        for identity, kind in ((1, "summary"), (2, "collector")):
            store.conn.execute("INSERT INTO alerts VALUES(?,?,?,?,'warning',?,?,NULL,NULL,1,NULL,1)", (identity, f"source_a:{kind}", "source_a", kind, kind, kind))
            store.conn.execute("INSERT INTO alert_events(alert_id,kind,created_at) VALUES(?,'opened',1)", (identity,))
            store.conn.execute("INSERT INTO alert_user_state VALUES(?,1,NULL,NULL)", (identity,))
            store.conn.execute("INSERT OR REPLACE INTO candidates VALUES(?,1,2,1)", (f"source_a:{kind}",))
        store.conn.execute("INSERT INTO usage_runs VALUES('source_a','summary_old','summary',NULL,NULL,'model',1,2,'completed','{}')")
        store.conn.execute("INSERT INTO chat_conversations VALUES('conversation',1,'Keep this question','{}',1,2)")
        store.conn.execute("PRAGMA user_version=2")
    store.close()
    for _ in range(2):
        store = DashboardStore(tmp_path)
        assert store.conn.execute("PRAGMA user_version").fetchone()[0] == 3
        assert not store.conn.execute("SELECT name FROM sqlite_master WHERE name='summaries'").fetchone()
        assert "summary" not in json.loads(store.conn.execute("SELECT payload FROM sources").fetchone()[0])
        assert store.conn.execute("SELECT kind FROM alerts").fetchone()[0] == "collector"
        assert store.conn.execute("SELECT alert_id FROM alert_events").fetchone()[0] == 2
        assert store.conn.execute("SELECT alert_id FROM alert_user_state").fetchone()[0] == 2
        assert not store.conn.execute("SELECT 1 FROM candidates WHERE key='source_a:summary'").fetchone()
        assert store.conn.execute("SELECT 1 FROM candidates WHERE key='source_a:collector'").fetchone()
        for table in ("messages", "users", "usage_runs", "chat_conversations"):
            assert store.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 1
        assert store.conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        store.close()


def test_new_cli_and_wire_have_no_summary_generation_entry(tmp_path, capsys):
    choices = next(action for action in parser()._actions if action.dest == "command").choices
    assert "summarize" not in choices and "cleanup-summaries" in choices
    assert main(["--local-state", str(tmp_path), "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["inference"]["paused"] is False and "summary" not in status
    assert not (tmp_path / "summary.sqlite").exists()
    value = heartbeat(1000)
    Heartbeat.model_validate(value)
    with pytest.raises(ValueError):
        Heartbeat.model_validate({**value, "summary": {"state": "ready"}})
