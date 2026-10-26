"""Native targeting guards use fake controls and read-only synthetic archives.

These tests never initialize AT-SPI, open an X display, or operate WeChat.
"""
from datetime import datetime, timedelta
import io
import json
import sqlite3
import subprocess
import sys
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from chatlog_keeper import client_health, native_accessibility as native
from chatlog_keeper.archive import ArchiveStore
from test_archive import commit, raw, setup_source


class Node:
    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name


def target(*, duration=2300, candidates=None):
    return {"minute": "03:56", "duration": duration,
            "candidates": candidates if candidates is not None else [("msg_target", duration)]}


def test_history_voice_matching_requires_explicit_minute_and_duration():
    chosen = Node('Audio2"sec 03:56')
    nodes = [Node('Audio2"sec 03:55'), Node('Audio8"sec 03:56'), chosen]
    assert native._match_audio(nodes, target(), "msg_target", history=True) == (chosen, "")


@pytest.mark.parametrize("name", ['Audio2"sec 03:55', 'Audio7"sec 03:56',
    'Audio2"sec', 'Audio2"sec 03:56extra', 'Text 03:56', None])
def test_history_voice_matching_rejects_wrong_or_incomplete_signature(name):
    with pytest.raises(native.NativeUnavailable, match="native_voice_not_unique_or_visible"):
        native._match_audio([Node(name)], target(), "msg_target", history=True)


def test_history_voice_matching_rejects_two_visible_matches():
    with pytest.raises(native.NativeUnavailable, match="native_voice_not_unique_or_visible"):
        native._match_audio([Node('Audio2"sec 03:56'), Node('Audio2"sec 03:56')],
                            target(), "msg_target", history=True)


@pytest.mark.parametrize("candidates", [
    [("msg_target", 2300), ("msg_other", 2500)], [("msg_other", 2300)], [],
])
def test_voice_signature_must_identify_exactly_the_requested_archived_message(candidates):
    with pytest.raises(native.NativeUnavailable, match="native_voice_signature_ambiguous"):
        native._match_audio([Node('Audio2"sec 03:56')], target(candidates=candidates),
                            "msg_target", history=True)


def test_main_voice_matching_uses_nearest_time_anchor_and_preserves_native_text():
    chosen = Node('Audio2"sec嘿')
    nodes = [Node("03:55"), Node('Audio2"secWrong voice'), Node("03:56"), chosen]
    assert native._match_audio(nodes, target(), "msg_target", history=False) == (chosen, "嘿")
    with pytest.raises(native.NativeUnavailable, match="native_voice_not_unique_or_visible"):
        native._match_audio([Node("03:55"), chosen], target(), "msg_target", history=False)


def test_main_voice_requires_a_preceding_minute_and_preserves_unplayed_marker():
    chosen = Node('Audio2"secUnplayed')
    assert native._match_audio([Node("03:56"), chosen], target(), "msg_target", history=False) == (chosen, "Unplayed")
    with pytest.raises(native.NativeUnavailable, match="native_voice_not_unique_or_visible"):
        native._match_audio([chosen, Node("03:56")], target(), "msg_target", history=False)


def roster(store, members=None):
    store.update_group_directory("account", [{
        "group_id": "test@chatroom", "name": "测试", "owner_id": "wxid_sender",
        "members": members if members is not None else [
            {"sender_id": "wxid_sender", "display_name": "Observed member", "nickname": "Sender"}],
        "coverage": {"status": "available", "group_nickname_status": "parsed"}}])


@pytest.fixture
def archived(tmp_path, monkeypatch):
    created = int(datetime.now(ZoneInfo("Asia/Shanghai")).replace(hour=3, minute=56, second=10, microsecond=0).timestamp())
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        parsed = {"msg_type": 34, "text": "[语音 2s]", "parse_status": "parsed",
                  "attachment": {"kind": "voice", "voice_length_ms": 2300}}
        reference = commit(store, source, [raw(timestamp=created, local_type=34, parsed=parsed)])["message_refs"][0]
        roster(store)
    monkeypatch.setattr(client_health, "_account_binding", lambda account: "single_account_scope" if account == "account" else "ambiguous")
    # Any attempt to initialize or act on the real desktop is a test failure.
    monkeypatch.setattr(native, "context", lambda: pytest.fail("real desktop access forbidden"))
    job = {"state_dir": str(tmp_path), "message_ref": reference, "account_id": "account",
           "group_id": "test@chatroom", "group_name": "测试", "server_id": "s1",
           "sender_id": "wxid_sender", "sender_name": "not used for member proof",
           "create_time": created, "voice_length_ms": 2300, "build": client_health._VERIFIED_BUILD}
    return tmp_path, source, job, parsed


def test_target_load_proves_archive_account_group_directory_and_same_day_readonly(archived, monkeypatch):
    state, _, job, _ = archived
    connect = sqlite3.connect
    calls = []

    def readonly(database, **kwargs):
        assert kwargs.get("uri") is True and str(database).endswith("?mode=ro")
        connection = connect(database, **kwargs)
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("CREATE TABLE must_not_write(value)")
        calls.append(str(database))
        return connection

    monkeypatch.setattr(sqlite3, "connect", readonly)
    result = native._load_target(job)
    assert result["state_dir"] == state.resolve()
    assert result["sender_display_name"] == "Observed member"
    assert result["minute"] == "03:56" and result["duration"] == 2300
    assert (job["message_ref"], 2300) in result["candidates"] and len(calls) == 1


@pytest.mark.parametrize("field,value", [("message_ref", "msg_missing"), ("account_id", "another"),
    ("group_id", "another@chatroom"), ("server_id", "another"), ("sender_id", "another"),
    ("create_time", 1)])
def test_target_load_rejects_job_archive_identity_mismatch(archived, field, value):
    _, _, job, _ = archived
    with pytest.raises(native.NativeUnavailable, match="native_archive_identity_mismatch"):
        native._load_target({**job, field: value})


def test_target_load_rejects_duration_build_and_account_ambiguity(archived, monkeypatch):
    _, _, job, _ = archived
    with pytest.raises(native.NativeUnavailable, match="native_voice_duration_unverified"):
        native._load_target({**job, "voice_length_ms": 5000})
    with pytest.raises(native.NativeUnavailable, match="native_client_build_not_verified"):
        native._load_target({**job, "build": "f" * 64})
    monkeypatch.setattr(client_health, "_account_binding", lambda account: "ambiguous")
    with pytest.raises(native.NativeUnavailable, match="native_account_not_unique"):
        native._load_target(job)


def test_target_load_rejects_duplicate_group_display_name(archived):
    state, _, job, _ = archived
    with ArchiveStore(state) as store:
        store.upsert_groups("account", [{"id": "other@chatroom", "name": "测试"}])
    with pytest.raises(native.NativeUnavailable, match="native_group_not_unique"):
        native._load_target(job)


@pytest.mark.parametrize("members,code", [
    ([], "native_sender_not_in_directory"),
    ([{"sender_id": "wxid_sender", "display_name": ""}], "native_sender_not_in_directory"),
    ([{"sender_id": "wxid_sender", "display_name": "Duplicate"},
      {"sender_id": "other", "display_name": "Duplicate"}], "native_sender_name_not_unique"),
])
def test_target_load_requires_unique_observed_member_identity(archived, members, code):
    state, _, job, _ = archived
    with ArchiveStore(state) as store:
        roster(store, members)
    with pytest.raises(native.NativeUnavailable, match=code):
        native._load_target(job)


def test_target_load_rejects_unreviewed_older_date(archived):
    state, source, job, parsed = archived
    old = int((datetime.now(ZoneInfo("Asia/Shanghai")) - timedelta(days=2)).timestamp())
    with ArchiveStore(state) as store:
        reference = commit(store, source, [raw(2, "old", timestamp=old, local_type=34, parsed=parsed)])["message_refs"][0]
    with pytest.raises(native.NativeUnavailable, match="native_history_date_not_verified"):
        native._load_target({**job, "create_time": old, "message_ref": reference, "server_id": "old"})


@pytest.mark.parametrize("sender_id", ["wxid_sender", "other_sender"])
def test_target_load_exposes_near_duration_same_minute_collision_for_match_guard(archived, sender_id):
    state, source, job, parsed = archived
    with ArchiveStore(state) as store:
        other = commit(store, source, [raw(2, "s2", timestamp=job["create_time"] + 1,
            sender_id=sender_id, local_type=34, parsed=parsed)])["message_refs"][0]
    loaded = native._load_target(job)
    assert (other, 2300) in loaded["candidates"]
    with pytest.raises(native.NativeUnavailable, match="native_voice_signature_ambiguous"):
        native._match_audio([Node('Audio2"sec 03:56')], loaded, job["message_ref"], history=True)


@pytest.mark.parametrize("reason,status", [
    ("native_login_required", "needs_login"), ("native_transcription_timeout", "timeout"),
    ("native_voice_signature_ambiguous", "ambiguous"), ("native_account_or_process_changed", "ambiguous"),
    ("native_sender_name_not_unique", "ambiguous"), ("native_history_date_not_verified", "unsupported"),
])
def test_transcribe_failure_mapping_never_initializes_the_desktop(monkeypatch, capsys, reason, status):
    def fail(job):
        raise native.NativeUnavailable(reason)

    monkeypatch.setattr(native, "transcribe", fail)
    monkeypatch.setattr(sys, "argv", ["native_accessibility.py", "--transcribe"])
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(b"{}")))
    native.main()
    assert json.loads(capsys.readouterr().out) == {"status": status, "reason": reason}


@pytest.fixture
def fake_conversion(tmp_path, monkeypatch):
    """Exercise conversion control flow with every desktop primitive replaced."""
    import signal

    class Control(Node):
        def __init__(self, name, role="button", children=()):
            super().__init__(name)
            self.role, self.children = role, list(children)

        def get_role_name(self):
            return self.role

        def get_child_count(self):
            return len(self.children)

        def get_child_at_index(self, index):
            return self.children[index]

    main = Control("Weixin", "frame")
    frame = Control('Chat history of "测试"(3)', "frame")
    history_voice = Control('Audio2"sec 03:56', "list item")
    history = Control("Chat History", "list", [history_voice])
    root = object()
    actions, alarms = [], []
    opened = [False]
    job = {"message_ref": "msg_target", "account_id": "account", "group_id": "test@chatroom",
           "group_name": "测试", "server_id": "s1", "create_time": 10, "sender_id": "wxid_sender",
           "voice_length_ms": 2300, "build": client_health._VERIFIED_BUILD}

    def one(root, atspi, role, name):
        if role == "frame" and name == "Weixin":
            return main
        if role == "list" and name == "Chat History":
            return history
        return Control(name, role)

    def open_history(*args):
        opened[0] = True

    monkeypatch.setattr(native.C, "CDLL", lambda *args, **kwargs: SimpleNamespace(prctl=lambda *args: 0))
    monkeypatch.setattr(signal, "signal", lambda *args: None)
    monkeypatch.setattr(signal, "alarm", lambda seconds: alarms.append(seconds))
    monkeypatch.setattr(native, "_load_target", lambda job: {
        **target(), "state_dir": tmp_path, "sender_display_name": "Observed member"})
    monkeypatch.setattr(native, "_cached_voice", lambda job: None)
    monkeypatch.setattr(native, "context", lambda: ({"pid": 123}, None, root))
    monkeypatch.setattr(native, "walk", lambda node: iter([(main, ())] + ([(frame, ())] if opened[0] else [])))
    monkeypatch.setattr(native, "showing", lambda *args: True)
    monkeypatch.setattr(native, "select_chat", lambda *args: actions.append(("select_chat", args[-1])))
    monkeypatch.setattr(native, "open_history", open_history)
    monkeypatch.setattr(native, "_one", one)
    monkeypatch.setattr(native, "_member_filter", lambda *args: actions.append(("member_filter", args[-1])))
    monkeypatch.setattr(native, "_click", lambda process, node, atspi, button=1, **kwargs: actions.append(("click", node.name)))
    monkeypatch.setattr(native.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(client_health, "_account_binding", lambda account: "single_account_scope")

    def run(suffixes):
        sequence = iter(suffixes)
        monkeypatch.setattr(native, "_main_voice", lambda *args: (history_voice, next(sequence)))
        return native.transcribe(job)

    return run, job, actions, alarms


@pytest.mark.parametrize("progress", ["Converting...", "converting", "转换中"])
def test_conversion_progress_is_polled_until_native_text(fake_conversion, progress):
    run, job, actions, alarms = fake_conversion
    result = run(["", progress, "嘿"])
    assert result["status"] == "succeeded" and result["text"] == "嘿"
    assert result["proof"] == {key: job[key] for key in result["proof"]}
    assert ("click", "Audio to Text") in actions
    assert "native_audio_to_text" in result["provenance"]["located_by"]
    assert alarms == [90, 0]


def test_existing_native_text_is_reused_without_conversion(fake_conversion):
    run, _, actions, alarms = fake_conversion
    result = run(["已转好的原生文字", "已转好的原生文字"])
    assert result["text"] == "已转好的原生文字"
    assert ("click", "Audio to Text") not in actions
    assert "existing_native_text" in result["provenance"]["located_by"]
    assert alarms == [90, 0]


def test_native_conversion_failure_is_terminal_and_releases_deadline(fake_conversion):
    run, _, _, alarms = fake_conversion
    with pytest.raises(native.NativeUnavailable, match="native_transcript_unavailable"):
        run(["", "conversion failed"])
    assert alarms == [90, 0]


def test_progress_stops_at_monotonic_deadline(fake_conversion, monkeypatch):
    run, _, _, alarms = fake_conversion
    moments = iter([100.0, 176.0])
    monkeypatch.setattr(native.time, "monotonic", lambda: next(moments))
    with pytest.raises(native.NativeUnavailable, match="native_transcription_timeout"):
        run(["", "Converting..."])
    assert alarms == [90, 0]


def probe_job():
    return {"source_python": sys.executable, "state_dir": "/synthetic/state",
            "message_ref": "msg_probe", "account_id": "synthetic_account",
            "group_id": "synthetic@chatroom", "server_id": "source-voice-1", "create_time": 10}


def probe_proof(job):
    return {"status": "cached", "proof": {key: job[key] for key in
            ("message_ref", "account_id", "group_id", "server_id", "create_time")}}


def test_cached_voice_probe_requires_exact_source_proof_and_bounded_companion(monkeypatch):
    job, calls = probe_job(), []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return SimpleNamespace(returncode=0, stdout=json.dumps(probe_proof(job)).encode(),
                               stderr=b"private diagnostic must stay private")

    monkeypatch.setattr(subprocess, "run", run)
    assert native._cached_voice(job) is None
    command, kwargs = calls[0]
    assert command == [sys.executable, "-B", "-m", "chatlog_keeper.native_cache_probe"]
    assert json.loads(kwargs["input"]) == {**job, "parent_pid": native.os.getpid()}
    assert kwargs["timeout"] == 12 and kwargs["check"] is False
    assert kwargs["stdout"] is subprocess.PIPE and kwargs["stderr"] is subprocess.PIPE


def test_cached_voice_probe_rejects_mismatched_identity(monkeypatch):
    job = probe_job()
    proof = probe_proof(job)
    proof["proof"]["server_id"] = "different-voice"
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs:
        SimpleNamespace(returncode=0, stdout=json.dumps(proof).encode()))
    with pytest.raises(native.NativeUnavailable, match="native_source_identity_mismatch"):
        native._cached_voice(job)


def test_cached_voice_probe_rejects_unavailable_or_missing_source(monkeypatch):
    for payload in ({"status": "unavailable"}, {"status": "missing"}, {}):
        monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs:
            SimpleNamespace(returncode=0, stdout=json.dumps(payload).encode()))
        with pytest.raises(native.NativeUnavailable, match="native_source_audio_not_cached"):
            native._cached_voice(probe_job())


def test_cached_voice_probe_timeout_is_a_fixed_failure_without_diagnostics(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("private source probe", 12,
                                        output=b"private source payload", stderr=b"private diagnostic")

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(native.NativeUnavailable) as failure:
        native._cached_voice(probe_job())
    assert str(failure.value) == "native_source_probe_failed"


def test_cached_voice_probe_rejects_invalid_interpreter_without_launch(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: pytest.fail("invalid interpreter launched"))
    for interpreter in (None, "", "python3", "relative/python3", 42):
        with pytest.raises(native.NativeUnavailable, match="native_source_probe_unavailable"):
            native._cached_voice({**probe_job(), "source_python": interpreter})
