"""Native task isolation is tested with fake children, never with WeChat UI."""
import io
import json
import os
import threading
import time

import pytest

from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.native_transcription import MAX_OUTPUT_BYTES, NativeTranscriptionWorker
from test_p2_archive import seed_voice


BUILD = "a" * 64
CAPABILITY = {"enabled": True, "verified": True, "build": BUILD}
PROOF = {"account_id": "account", "group_id": "test@chatroom", "server_id": "v1",
         "create_time": 10, "sender_id": "wxid_sender", "voice_length_ms": 5000, "build": BUILD}


class Input(io.BytesIO):
    def __init__(self):
        super().__init__()
        self.saved = None
        self.done = threading.Event()

    def close(self):
        self.saved = self.getvalue()
        super().close()
        self.done.set()


class Child:
    def __init__(self, output=b"", errors=b""):
        self.stdin = Input()
        self.stdout = io.BytesIO(output)
        self.stderr = io.BytesIO(errors)
        self.returncode = None
        self.kills = 0
        self.waits = []
        self.killed = threading.Event()

    def poll(self):
        return self.returncode

    def kill(self):
        self.kills += 1
        self.returncode = -9
        self.killed.set()

    def wait(self, timeout=None):
        self.waits.append(timeout)
        assert self.returncode is not None
        return self.returncode


class Timer:
    def __init__(self, seconds, target, args):
        self.seconds, self.target, self.args = seconds, target, args
        self.cancelled = False
        self.started = False

    def start(self):
        self.started = True

    def cancel(self):
        self.cancelled = True

    def fire(self):
        if not self.cancelled:
            self.target(*self.args)


def result(**changes):
    return {"status": "succeeded", "text": "WeChat native result", "proof": dict(PROOF),
            "provenance": {"source": "wechat_native_accessibility", "version": 1,
                           "build": BUILD, "located_by": ["account", "group", "sender", "time", "duration"]}, **changes}


def worker(output=None, errors=b"", **kwargs):
    child = Child(json.dumps(output or result()).encode() if not isinstance(output, bytes) else output, errors)
    opened = []

    def factory(command, **arguments):
        opened.append((command, arguments))
        return child

    return NativeTranscriptionWorker(process_factory=factory, timer_factory=Timer, **kwargs), child, opened


def queued(store):
    _, reference = seed_voice(store)
    store.set_meta("native_transcription_capability", CAPABILITY)
    task = store.request_voice_transcription(reference)
    return reference, task


def drained(worker):
    active = worker._active
    assert active.process.stdin.done.wait(1)
    assert all(event.wait(1) for event in active.done.values())


def finish(worker, child, store):
    drained(worker)
    child.returncode = 0
    return worker.pump(store)


@pytest.mark.parametrize("capability", [{}, {"enabled": True}, {"enabled": True, "verified": True},
    {"enabled": True, "verified": False, "build": BUILD}, {"enabled": False, "verified": True, "build": BUILD}])
def test_disabled_capability_never_launches_even_existing_queued_job(tmp_path, capability):
    with ArchiveStore(tmp_path) as store:
        reference, task = queued(store)
        store.set_meta("native_transcription_capability", capability)
        runner, child, opened = worker()
        assert runner.pump(store)["state"] == "disabled" and opened == []
        assert store.get_voice(reference)["task"]["status"] == "queued"
        assert store.get_voice(reference)["native_text"] is None


def test_pump_launch_is_nonblocking_and_success_requires_verified_identity(tmp_path):
    with ArchiveStore(tmp_path) as store:
        reference, task = queued(store)
        before = tuple(store.conn.execute("SELECT first_seen,text,updated_at FROM messages").fetchone())
        runner, child, opened = worker()
        started = time.monotonic()
        assert runner.pump(store) == {"state": "running", "job_id": task["job_id"]}
        assert time.monotonic() - started < 0.2
        assert runner.pump(store)["state"] == "running"
        assert len(opened) == 1 and runner._active.timer.seconds == 90
        command, arguments = opened[0]
        assert command[0:2] == ["/usr/bin/python3", "-B"] and command[-1] == "--transcribe"
        assert arguments["start_new_session"] and arguments["close_fds"]
        drained(runner)
        payload = json.loads(child.stdin.saved)
        assert {key: payload[key] for key in PROOF} == PROOF
        assert payload["state_dir"] == str(store.state_dir)
        assert payload["parent_pid"] == os.getpid()
        assert "raw_path" not in payload and "source" not in payload and "audio" not in payload
        assert finish(runner, child, store)["state"] == "succeeded"
        assert store.get_voice(reference)["native_text"] == "WeChat native result"
        assert store.get_voice(reference)["task"]["status"] == "succeeded"
        assert store.get_voice(reference)["transcript"]["provenance"]["verified_identity"] == PROOF
        assert tuple(store.conn.execute("SELECT first_seen,text,updated_at FROM messages").fetchone()) == before
        assert store.get_status()["messages"] == 1
        assert runner.pump(store)["state"] == "idle" and len(opened) == 1


@pytest.mark.parametrize("key", list(PROOF))
def test_identity_mismatch_stops_without_saving_text(tmp_path, key):
    proof = {**PROOF, key: "wrong"}
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner, child, _ = worker(result(proof=proof))
        runner.pump(store)
        assert finish(runner, child, store)["state"] == "ambiguous"
        assert store.get_voice(reference)["native_text"] is None
        assert store.get_voice(reference)["task"]["reason"] == "native_result_identity_mismatch"


@pytest.mark.parametrize("status", ["needs_login", "ambiguous", "unsupported", "timeout"])
def test_native_failures_are_explicit_and_do_not_archive_diagnostics(tmp_path, status):
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner, child, _ = worker({"status": status, "reason": "native_sample_unavailable", "text": "must not save"}, errors=b"private diagnostic")
        runner.pump(store)
        response = finish(runner, child, store)
        assert response["state"] == status
        assert store.get_voice(reference)["task"]["reason"] == "native_sample_unavailable"
        assert store.get_voice(reference)["native_text"] is None
        assert "private diagnostic" not in json.dumps(store.get_voice(reference))


@pytest.mark.parametrize("output", [b"not json", b'[]', b'{"status":[]}',
    json.dumps(result(text="")).encode(), json.dumps(result(provenance={})).encode()])
def test_invalid_success_data_never_becomes_a_transcript(tmp_path, output):
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner, child, _ = worker(output)
        runner.pump(store)
        assert finish(runner, child, store)["state"] == "failed"
        assert store.get_voice(reference)["native_text"] is None


def test_watchdog_terminates_at_deadline_without_waiting_for_collector_pump(tmp_path):
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner, child, _ = worker()
        runner.pump(store)
        drained(runner)
        runner._active.timer.fire()
        assert child.kills == 1
        # The watchdog never writes SQLite from its thread.
        assert store.get_voice(reference)["task"]["status"] == "running"
        assert runner.pump(store)["state"] == "timeout"
        assert store.get_voice(reference)["task"]["reason"] == "native_transcription_timeout"
        assert store.get_voice(reference)["audio_status"] == "archived"


def test_real_watchdog_timer_fires_between_collector_pumps(tmp_path):
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        child = Child()
        runner = NativeTranscriptionWorker(timeout=0.03, process_factory=lambda *args, **kwargs: child)
        assert runner.pump(store)["state"] == "running"
        assert child.killed.wait(1)
        assert store.get_voice(reference)["task"]["status"] == "running"
        drained(runner)
        assert runner.pump(store)["state"] == "timeout"


class HeldPipe:
    def __init__(self):
        self.release = threading.Event()

    def read(self, count):
        self.release.wait(5)
        return b""

    def close(self):
        self.release.set()


@pytest.mark.parametrize("use_watchdog", [False, True])
def test_exited_helper_with_inherited_pipe_never_keeps_worker_active_forever(tmp_path, monkeypatch, use_watchdog):
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner, child, _ = worker()
        held = HeldPipe()
        child.stdout = held
        runner.pump(store)
        assert child.stdin.done.wait(1)
        child.returncode = 0
        try:
            monkeypatch.setattr("chatlog_keeper.native_transcription.time.monotonic", lambda: 10)
            assert runner.pump(store)["state"] == "running"
            assert not runner._active.timer.cancelled
            if use_watchdog:
                runner._active.timer.fire()
            else:
                monkeypatch.setattr("chatlog_keeper.native_transcription.time.monotonic", lambda: 12)
            assert runner.pump(store)["reason"] == "native_helper_pipe_timeout"
            assert runner._active is None
            assert store.get_voice(reference)["task"]["status"] == "timeout"
            assert store.get_voice(reference)["native_text"] is None
        finally:
            held.release.set()


def test_output_is_bounded_and_oversize_terminates_helper(tmp_path):
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner, child, _ = worker(b"x" * (MAX_OUTPUT_BYTES + 8192))
        runner.pump(store)
        drained(runner)
        assert child.kills == 1
        assert len(runner._active.output["stdout"]) == MAX_OUTPUT_BYTES + 1
        assert runner.pump(store)["reason"] == "native_helper_output_limit"
        assert store.get_voice(reference)["native_text"] is None


def test_revoking_capability_cancels_running_child_and_discards_finished_result(tmp_path):
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner, child, _ = worker()
        runner.pump(store)
        drained(runner)
        store.set_meta("native_transcription_capability", {**CAPABILITY, "build": "b" * 64})
        assert runner.pump(store)["state"] == "cancelled"
        assert child.kills == 1 and store.get_voice(reference)["native_text"] is None


def test_shutdown_cancels_and_reaps_child_and_future_pumps_do_nothing(tmp_path):
    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner, child, opened = worker()
        runner.pump(store)
        drained(runner)
        timer = runner._active.timer
        runner.close(store)
        assert child.kills == 1 and child.waits == [0.5] and timer.cancelled
        assert store.get_voice(reference)["task"]["status"] == "cancelled"
        assert runner.pump(store)["state"] == "closed" and len(opened) == 1


def test_shutdown_does_not_retain_child_if_archive_update_fails(tmp_path, monkeypatch):
    with ArchiveStore(tmp_path) as store:
        queued(store)
        runner, child, _ = worker()
        runner.pump(store)
        drained(runner)
        def unavailable(*args, **kwargs):
            raise RuntimeError("archive temporarily unavailable")
        monkeypatch.setattr(store, "update_voice_transcription_job", unavailable)
        with pytest.raises(RuntimeError):
            runner.close(store)
        assert child.kills == 1 and child.waits == [0.5]
        assert runner._active is None


def test_helper_start_failure_is_reported_and_recovered_queue_can_retry(tmp_path):
    def broken(*args, **kwargs):
        raise FileNotFoundError("private helper path")

    with ArchiveStore(tmp_path) as store:
        reference, _ = queued(store)
        runner = NativeTranscriptionWorker(process_factory=broken)
        assert runner.pump(store)["reason"] == "native_helper_start_failed"
        assert store.get_voice(reference)["task"]["status"] == "failed"
        assert store.request_voice_transcription(reference)["status"] == "queued"
        healthy, child, _ = worker()
        healthy.pump(store)
        assert finish(healthy, child, store)["state"] == "succeeded"
