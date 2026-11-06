"""Runtime lanes and opt-in CLI use only fake components and temporary files."""
import importlib.util
import json
from pathlib import Path
import sqlite3
import threading
import time
from types import SimpleNamespace

import pytest

import chatlog_keeper.bot.protocol_runtime as runtime_module
from chatlog_keeper.bot.protocol_runtime import ProtocolRuntime, RuntimeConfig, RuntimeErrorCode, check_configuration
from chatlog_keeper.bot.store import BotStore
from test_bot_service import config as bot_config


class Inbox:
    def __init__(self):
        self.received = threading.Event()

    def poll(self, *, limit):
        assert limit == 100
        self.received.set()
        return SimpleNamespace(received_events=1, new_requests=1)


class Business:
    def __init__(self):
        self.generated, self.projected = threading.Event(), threading.Event()
        self.block_reason, self.closed = None, False

    def run_once(self, *, block):
        assert block is False
        self.generated.set()
        return {"admitted": 1, "generating": 0}

    def project_outbox(self):
        self.projected.set()
        return {"projected": 1, "errors": []}

    def delivery_block_reason(self, row):
        return self.block_reason

    def close(self):
        self.closed = True


class Outbox:
    def __init__(self):
        self.status_started, self.status_release, self.sent = threading.Event(), threading.Event(), threading.Event()
        self.lock = threading.Lock()
        self.queued, self.unknown = True, True
        self.send_gateway, self.status_gateway = None, None
        self.blocked = []

    def pending(self, *, limit):
        with self.lock:
            return [{"reply_id": "new"}] if self.queued else []

    def awaiting_status(self, *, limit):
        with self.lock:
            return [{"reply_id": "old"}] if self.unknown else []

    def deliver_one(self, gateway, key):
        assert key == "new"
        with self.lock:
            assert self.queued
            self.queued = False
        self.send_gateway = gateway
        self.sent.set()
        return {"state": "accepted", "source": {"body": "private never publish this"}}

    def reconcile(self, gateway, key):
        assert key == "old"
        self.status_gateway = gateway
        self.status_started.set()
        assert self.status_release.wait(5), "test must release only the old-result lane"
        with self.lock:
            self.unknown = False
        return {"state": "unknown"}

    def block(self, key, reason):
        self.blocked.append((key, reason))
        self.queued = False
        return {"state": "rejected"}


def runtime(**kwargs):
    return ProtocolRuntime(inbox=kwargs.pop("inbox", Inbox()), business=kwargs.pop("business", Business()),
        outbox=kwargs.pop("outbox", Outbox()), receive_gateway=object(), send_gateway=object(), status_gateway=object(),
        interval=0.01, status_interval=0.01, **kwargs)


def test_old_unknown_network_wait_does_not_occupy_new_send_receive_generation_or_projection():
    inbox, business, outbox = Inbox(), Business(), Outbox()
    owner = runtime(inbox=inbox, business=business, outbox=outbox)
    try:
        owner.start()
        assert outbox.status_started.wait(2)
        assert not outbox.status_release.is_set()
        assert outbox.sent.wait(2) and inbox.received.wait(2)
        assert business.generated.wait(2) and business.projected.wait(2)
        assert outbox.send_gateway is owner.send_gateway
        assert outbox.status_gateway is owner.status_gateway
        assert owner.send_gateway is not owner.status_gateway
        status = owner.status()
        assert status["phone_received"] is False
        assert "private never publish this" not in json.dumps(status)
    finally:
        outbox.status_release.set()
        owner.close()
    assert business.closed
    assert all(not thread.is_alive() for thread in owner._threads)


@pytest.mark.parametrize("reason", ["source_quarantined", "configuration_changed"])
def test_send_preflight_blocks_frozen_reply_without_posting(reason):
    business, outbox = Business(), Outbox()
    business.block_reason = reason
    owner = runtime(business=business, outbox=outbox)
    try:
        assert owner._send() == {"processed": 1, "state": "rejected", "blocked": True}
        assert outbox.blocked == [("new", reason)]
        assert not outbox.sent.is_set()
    finally:
        owner.close()


def test_projection_failure_is_bounded_and_does_not_stop_other_lanes():
    business, outbox = Business(), Outbox()
    outbox.unknown = False
    def failed_projection():
        raise ValueError("private body and token must not be logged")
    business.project_outbox = failed_projection
    owner = runtime(business=business, outbox=outbox)
    try:
        owner.start()
        assert outbox.sent.wait(2) and business.generated.wait(2)
        owner.request_stop()
    finally:
        owner.close()
    assert owner.status()["lanes"]["projection"] == {"state": "attention", "error": "ValueError"}


def wait_until(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        threading.Event().wait(0.01)
    assert predicate()


def test_projection_returned_errors_are_attention_and_persist_without_request_details(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_module.RuntimeStatusFile, "MIN_INTERVAL", 0.02)
    business, outbox = Business(), Outbox()
    outbox.unknown = False
    business.project_outbox = lambda: {"projected": 1, "errors": [{"request_id": "private-source", "error": "private-message"}]}
    path = tmp_path / "health.json"
    owner = runtime(business=business, outbox=outbox, status_file=path)
    try:
        owner.start()
        wait_until(lambda: json.loads(path.read_text())["lanes"]["projection"].get("state") == "attention")
        saved = json.loads(path.read_text())
        assert saved["state"] == "attention"
        lane = saved["lanes"]["projection"]
        assert lane["error"] == "protocol_outbox_projection_failed"
        assert lane["counters"] == {"errors": 1, "projected": 1}
        assert lane["last_failure_at"] and lane["last_error"] == lane["error"]
        assert outbox.sent.wait(2)  # A projection error does not stop other lanes.
        assert "private" not in path.read_text()
        assert path.stat().st_mode & 0o777 == 0o600
    finally:
        owner.close()
    saved = json.loads(path.read_text())
    assert saved["state"] == "stopped" and saved["phone_received"] is False
    assert saved["lanes"]["projection"]["state"] == "attention"
    assert list(tmp_path.iterdir()) == [path]


def test_status_observer_heartbeats_are_bounded_and_healthy_results_do_not_trigger_each_poll(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_module.RuntimeStatusFile, "MIN_INTERVAL", 0.03)
    monkeypatch.setattr(runtime_module.RuntimeStatusFile, "HEARTBEAT", 0.12)
    outbox = Outbox()
    outbox.unknown = False
    owner = runtime(outbox=outbox, status_file=tmp_path / "health.json")
    documents = []
    write = owner._observer.write
    def record(snapshot):
        write(snapshot)
        documents.append(json.loads(owner._observer.path.read_text()))
    monkeypatch.setattr(owner._observer, "write", record)
    owner.start()
    try:
        wait_until(lambda: len(documents) >= 5)
        running = [row for row in documents if row["state"] == "running"]
        assert len(running) >= 3
        assert running[-1]["updated_at"] != running[-2]["updated_at"]
        # After the initial observer snapshot, unchanged health waits for the
        # heartbeat instead of syncing a file on each 10 ms business poll.
        from datetime import datetime
        gaps = [(datetime.fromisoformat(right["updated_at"]) - datetime.fromisoformat(left["updated_at"])).total_seconds()
                for left, right in zip(running[1:], running[2:])]
        assert gaps and min(gaps) >= 0.10
    finally:
        owner.close()


def test_status_document_whitelists_data_and_keeps_only_safe_error_codes(tmp_path):
    path = tmp_path / "health.json"
    observer = runtime_module.RuntimeStatusFile(path)
    observer.write({"state": "attention", "error": "token/private text", "account": "private-account", "lanes": {
        "send": {"state": "ready", "result": {"processed": 1, "state": "accepted", "text": "private-body", "source": "private-source"}},
        "projection": {"state": "attention", "error": "secret/path", "result": {"errors": 1}},
        "private-lane": {"state": "ready"}}, "lane_history": {"projection": {"last_error": "secret/path"}}})
    saved = json.loads(path.read_text())
    assert saved["error"] == "UnknownError"
    assert saved["lanes"]["send"]["delivery_state"] == "accepted"
    assert saved["phone_received"] is False
    assert saved["lanes"]["projection"]["error"] == "UnknownError"
    assert "private" not in path.read_text() and "secret" not in path.read_text()


def test_status_write_failure_preserves_previous_atomic_document_and_cleans_temporary_file(tmp_path, monkeypatch):
    path = tmp_path / "health.json"
    observer = runtime_module.RuntimeStatusFile(path)
    observer.write({"state": "starting"})
    before = path.read_bytes()
    def denied(*args):
        raise OSError("private path cannot be published")
    monkeypatch.setattr(runtime_module.os, "replace", denied)
    with pytest.raises(OSError):
        observer.write({"state": "running"})
    assert path.read_bytes() == before
    assert list(tmp_path.iterdir()) == [path]


def test_observer_write_failure_is_visible_and_recovers_without_stopping_business(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_module.RuntimeStatusFile, "MIN_INTERVAL", 0.02)
    outbox = Outbox()
    outbox.unknown = False
    owner = runtime(outbox=outbox, status_file=tmp_path / "health.json")
    owner.start()
    write, failing = owner._observer.write, threading.Event()
    failing.set()
    def temporary_failure(snapshot):
        if failing.is_set():
            raise OSError("private filesystem path")
        return write(snapshot)
    monkeypatch.setattr(owner._observer, "write", temporary_failure)
    try:
        wait_until(lambda: owner.status().get("error") == "protocol_runtime_status_write_failed")
        assert owner.status()["state"] == "attention"
        assert outbox.sent.wait(2) and owner.business.generated.wait(2)
        failing.clear()
        wait_until(lambda: owner.status().get("error") is None)
        wait_until(lambda: json.loads(owner._observer.path.read_text())["state"] == "running")
    finally:
        failing.clear()
        owner.close()
    assert json.loads(owner._observer.path.read_text())["state"] == "stopped"


def test_recovered_lane_keeps_failure_history_for_bounded_observer_to_record(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime_module.RuntimeStatusFile, "MIN_INTERVAL", 0.02)
    outbox = Outbox()
    outbox.unknown = False
    owner = runtime(outbox=outbox, status_file=tmp_path / "health.json")
    calls = []
    def flaky_projection():
        calls.append(1)
        if len(calls) == 1:
            return {"projected": 0, "errors": [{"request_id": "private"}]}
        return {"projected": 1, "errors": []}
    owner.business.project_outbox = flaky_projection
    try:
        owner.start()
        wait_until(lambda: len(calls) > 1)
        wait_until(lambda: json.loads(owner._observer.path.read_text())["lanes"]["projection"]["state"] == "ready")
        saved = json.loads(owner._observer.path.read_text())
        assert saved["lanes"]["projection"]["last_error"] == "protocol_outbox_projection_failed"
        assert saved["lanes"]["projection"]["last_failure_at"]
        assert saved["state"] == "running" and "private" not in owner._observer.path.read_text()
    finally:
        owner.close()


def test_status_refuses_unrelated_files_and_symlinks_without_modification(tmp_path):
    path = tmp_path / "credentials.json"
    path.write_text('{"token":"do not overwrite"}')
    path.chmod(0o600)
    with pytest.raises(RuntimeErrorCode, match="status_file_invalid"):
        runtime_module.RuntimeStatusFile(path)
    link = tmp_path / "health.json"
    link.symlink_to(path)
    with pytest.raises(RuntimeErrorCode, match="symlink_path"):
        runtime_module.RuntimeStatusFile(link)
    assert path.read_text() == '{"token":"do not overwrite"}'


def test_status_start_failure_and_shutdown_failure_remain_observable(tmp_path, monkeypatch):
    path = tmp_path / "health.json"
    owner = runtime(status_file=path)
    original = threading.Thread.start
    def fail_third(thread):
        if thread.name == "protocol-projection":
            raise RuntimeError("private thread details")
        return original(thread)
    monkeypatch.setattr(threading.Thread, "start", fail_third)
    with pytest.raises(RuntimeError):
        owner.start()
    saved = json.loads(path.read_text())
    assert saved["state"] == "failed" and saved["error"] == "RuntimeError"
    assert owner.business.closed and "private" not in path.read_text()
    def failed_close():
        raise ValueError("private shutdown details")
    owner = runtime(status_file=path, close_callback=failed_close)
    with pytest.raises(ValueError):
        owner.close()
    saved = json.loads(path.read_text())
    assert saved["state"] == "failed" and saved["error"] == "ValueError"


def test_shared_gateway_object_is_rejected():
    shared = object()
    with pytest.raises(RuntimeErrorCode, match="separate_gateways"):
        ProtocolRuntime(inbox=Inbox(), business=Business(), outbox=Outbox(),
                        receive_gateway=object(), send_gateway=shared, status_gateway=shared)


def test_partial_thread_start_failure_stops_started_lanes_and_closes_resources(monkeypatch):
    owner = runtime()
    original = threading.Thread.start
    def fail_third(thread):
        if thread.name == "protocol-projection":
            raise RuntimeError("synthetic_thread_capacity")
        return original(thread)
    monkeypatch.setattr(threading.Thread, "start", fail_third)
    with pytest.raises(RuntimeError, match="synthetic_thread_capacity"):
        owner.start()
    assert owner.business.closed
    assert len(owner._threads) == 2 and all(not thread.is_alive() for thread in owner._threads)
    owner.close()


@pytest.fixture
def local_config(tmp_path):
    state, usage, custody = (tmp_path / name for name in ("state", "usage", "custody"))
    for path in (state, usage, custody):
        path.mkdir(mode=0o700)
    store = BotStore(state)
    store.configure(bot_config())
    store.close()
    auth = state / "dashboard" / "siwc"
    auth.mkdir(parents=True, mode=0o700)
    auth.parent.chmod(0o700)
    (auth / "credentials.json").write_text('{}')
    (auth / "credentials.json").chmod(0o600)
    token, bindings = custody / "token", custody / "bindings.json"
    token.write_text("synthetic-test-token")
    bindings.write_text(json.dumps({"synthetic@chatroom": "group_a"}))
    token.chmod(0o600); bindings.chmod(0o600)
    return RuntimeConfig(state_dir=state, usage_dir=usage, inbox_path=custody / "inbox.sqlite",
        outbox_path=custody / "outbox.sqlite", journal_path=custody / "journal.sqlite",
        token_file=token, bindings_file=bindings, server="http://127.0.0.1:19999",
        account_id="synthetic", own_username="wxid_robot", consumer="synthetic-business")


def test_check_is_local_readonly_and_does_not_construct_models_gateway_or_mutable_store(local_config, monkeypatch):
    before = {path.relative_to(local_config.state_dir): path.read_bytes()
              for path in local_config.state_dir.rglob('*') if path.is_file()}
    def forbidden(*args, **kwargs):
        pytest.fail("check cannot construct live or writable components")
    monkeypatch.setattr(runtime_module.ProtocolGateway, "__init__", forbidden)
    monkeypatch.setattr(runtime_module.ProviderResources, "__init__", forbidden)
    monkeypatch.setattr(runtime_module.BotStore, "__init__", forbidden)
    result = check_configuration(local_config)
    assert result["local_configuration_valid"] and result["activation_required"]
    assert result["gateway_contacted"] is False and result["phone_received"] is False
    assert not any(path.exists() for path in (local_config.inbox_path, local_config.outbox_path, local_config.journal_path))
    after = {path.relative_to(local_config.state_dir): path.read_bytes()
             for path in local_config.state_dir.rglob('*') if path.is_file()}
    assert before == after


def cli_module():
    spec = importlib.util.spec_from_file_location("run_protocol_bot_test", Path(__file__).resolve().parents[1] / "scripts/run_protocol_bot.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def cli_args(config):
    result = []
    for option, value in (("state-dir", config.state_dir), ("usage-dir", config.usage_dir),
            ("inbox", config.inbox_path), ("outbox", config.outbox_path), ("journal", config.journal_path),
            ("token-file", config.token_file), ("bindings", config.bindings_file), ("server", config.server),
            ("account", config.account_id), ("own", config.own_username), ("consumer", config.consumer)):
        result.extend(("--" + option, str(value)))
    return result


def test_cli_defaults_to_check_and_never_prints_token(local_config, monkeypatch, capsys):
    module = cli_module()
    monkeypatch.setattr(module, "open_runtime", lambda *_: pytest.fail("--run was not supplied"))
    assert module.main(cli_args(local_config)) == 0
    output = capsys.readouterr().out
    assert json.loads(output)["mode"] == "check"
    assert "synthetic-test-token" not in output


def test_cli_status_file_check_is_readonly_but_failed_run_persists_safe_startup_error(local_config, monkeypatch, capsys):
    module = cli_module()
    path = local_config.inbox_path.parent / "health.json"
    args = [*cli_args(local_config), "--status-file", str(path)]
    assert module.main(args) == 0
    assert not path.exists()
    def fail(config):
        assert config.status_file == path
        assert json.loads(path.read_text())["state"] == "starting"
        raise RuntimeErrorCode("protocol_runtime_activation_required")
    monkeypatch.setattr(module, "open_runtime", fail)
    assert module.main([*args, "--run"]) == 1
    saved = json.loads(path.read_text())
    assert saved["state"] == "failed" and saved["error"] == "protocol_runtime_activation_required"
    assert saved["phone_received"] is False
    assert "synthetic-test-token" not in path.read_text() + capsys.readouterr().err


@pytest.mark.parametrize("suffix", ["", "-wal", "-shm", ".lock"])
def test_status_path_cannot_overwrite_database_or_its_companions(local_config, suffix):
    from dataclasses import replace
    destination = Path(str(local_config.inbox_path) + suffix)
    with pytest.raises(RuntimeErrorCode, match="path_collision"):
        runtime_module.RuntimeStatusFile.for_config(replace(local_config, status_file=destination))
    assert not destination.exists()


def test_cli_activation_requires_explicit_run(local_config):
    module = cli_module()
    with pytest.raises(SystemExit) as error:
        module.main([*cli_args(local_config), "--activate"])
    assert error.value.code == 2


def test_first_run_requires_explicit_activation_before_constructing_any_component(local_config, monkeypatch):
    monkeypatch.setattr(runtime_module.BotStore, "__init__", lambda *args: pytest.fail("must reject before writing"))
    with pytest.raises(RuntimeErrorCode, match="activation_required"):
        runtime_module.open_runtime(local_config)


def test_existing_checkpoint_is_checked_without_reactivating_or_resetting_cursor(local_config):
    with sqlite3.connect(local_config.inbox_path) as database:
        database.executescript('''CREATE TABLE inbox_meta(endpoint,account_id,own_username,consumer,cutover_cursor);
            CREATE TABLE native_checkpoint(checkpoint_json);''')
        database.execute('INSERT INTO inbox_meta VALUES(?,?,?,?,?)', (
            local_config.server + '/v1', local_config.account_id, local_config.own_username, local_config.consumer, '42'))
        database.execute('INSERT INTO native_checkpoint VALUES(?)', ('{"eventCursor":"42"}',))
    local_config.inbox_path.chmod(0o600)
    before = local_config.inbox_path.read_bytes()
    assert check_configuration(local_config)["activated"] is True
    assert before == local_config.inbox_path.read_bytes()
    from dataclasses import replace
    with pytest.raises(RuntimeErrorCode, match="already_activated"):
        runtime_module.open_runtime(replace(local_config, activate=True))


def test_readonly_check_includes_committed_wal_without_creating_or_changing_source_files(local_config):
    database = local_config.state_dir / "bot" / "bot.sqlite"
    with sqlite3.connect(database) as writer:
        writer.execute("PRAGMA wal_autocheckpoint=0")
        value = json.loads(writer.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        value["enabled"] = False
        writer.execute("UPDATE meta SET value=? WHERE key='config'", (json.dumps(value),))
        writer.commit()
        assert Path(str(database) + "-wal").stat().st_size > 0
        before = {path.name: path.read_bytes() for path in database.parent.iterdir() if path.is_file()}
        with pytest.raises(RuntimeErrorCode, match="bot_disabled"):
            check_configuration(local_config)
        after = {path.name: path.read_bytes() for path in database.parent.iterdir() if path.is_file()}
        assert before == after


@pytest.mark.parametrize("activated", [False, True])
def test_open_runtime_wires_three_gateways_same_journal_history_and_safe_close_order(local_config, monkeypatch, activated):
    from dataclasses import replace
    import chatlog_keeper.bot.protocol_business as business_module
    import chatlog_keeper.bot.protocol_history as history_module
    import chatlog_keeper.bot.protocol_native_inbox as inbox_module
    import chatlog_keeper.bot.protocol_outbox as outbox_module
    import chatlog_keeper.bot.reply_generator as generator_module

    log, instances = [], {}
    if activated:
        with sqlite3.connect(local_config.inbox_path) as database:
            database.executescript('''CREATE TABLE inbox_meta(endpoint,account_id,own_username,consumer,cutover_cursor);
                CREATE TABLE native_checkpoint(checkpoint_json);''')
            database.execute('INSERT INTO inbox_meta VALUES(?,?,?,?,?)', (
                local_config.server + '/v1', local_config.account_id, local_config.own_username, local_config.consumer, '42'))
            database.execute('INSERT INTO native_checkpoint VALUES(?)', ('{"eventCursor":"42"}',))
        local_config.inbox_path.chmod(0o600)

    class Store:
        _binding = staticmethod(BotStore._binding)
        def __init__(self, path):
            assert path == local_config.state_dir
            instances["store"] = self
        def close(self): log.append("store")

    class Resources:
        def __init__(self, store, **kwargs):
            assert store is instances["store"]
        def provider(self, *args): pytest.fail("construction cannot call model")
        def local_business_execution(self, *args): pytest.fail("construction cannot start local accounting")
        def usage_ledger(self): return None
        def close(self): log.append("resources")

    class Gateway:
        def __init__(self, **kwargs):
            assert kwargs["journal_path"] == local_config.journal_path
            assert kwargs["account_id"] == local_config.account_id
            instances.setdefault("gateways", []).append(self)
        def close(self): log.append("gateway")

    class NativeInbox:
        def __init__(self, gateway, **kwargs):
            assert gateway is instances["gateways"][0]
            self.activated = False
            instances["inbox"] = self
        def activate(self): self.activated = True
        def close(self): log.append("inbox")

    class NativeOutbox:
        def __init__(self, *args, **kwargs): instances["outbox"] = self
        def close(self): log.append("outbox")

    class History:
        def __init__(self, store, outbox, **kwargs):
            assert store is instances["store"] and outbox is instances["outbox"]
            instances["history"] = self

    class Generator:
        def __init__(self, store, **kwargs):
            assert store is instances["history"]
            assert kwargs["local_execution_factory"].__name__ == "local_business_execution"
            instances["generator"] = self

    class Coordinator:
        def __init__(self, **kwargs):
            assert kwargs["store"] is instances["store"]
            assert kwargs["generator"] is instances["generator"]
            assert kwargs["concurrency"] == 2
        def close(self): log.append("business")

    for target, name, replacement in ((runtime_module, "BotStore", Store),
            (runtime_module, "ProviderResources", Resources), (runtime_module, "ProtocolGateway", Gateway),
            (inbox_module, "ProtocolNativeInbox", NativeInbox), (outbox_module, "ProtocolOutbox", NativeOutbox),
            (history_module, "ProtocolHistoryStore", History), (generator_module, "ReplyGenerator", Generator),
            (business_module, "ProtocolBusinessWorker", Coordinator)):
        monkeypatch.setattr(target, name, replacement)
    owner = runtime_module.open_runtime(replace(local_config, activate=not activated))
    assert len(instances["gateways"]) == 3 and len({id(g) for g in instances["gateways"]}) == 3
    assert instances["inbox"].activated is (not activated)
    assert owner._threads == []  # Constructing an opted-in runtime still does not start it.
    owner.close()
    owner.close()
    assert log == ["business", "outbox", "inbox", "gateway", "gateway", "gateway", "resources", "store"]
