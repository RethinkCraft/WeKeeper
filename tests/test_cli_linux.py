"""Linux command behavior with synthetic accounts, messages and key material."""
from contextlib import contextmanager
from datetime import datetime
import json
from types import SimpleNamespace

import pytest

from chatlog_keeper import active_key, cli, image_key, wechat_db
from chatlog_keeper.core import _secrets


def contact_reader(names):
    return SimpleNamespace(
        _load_contacts=lambda: SimpleNamespace(_wxid_to_display=names),
    )


def sample_message(**overrides):
    fields = dict(
        server_id="synthetic-server-id",
        timestamp=datetime(2026, 1, 2, 12, 30, tzinfo=cli.ZONE),
        msg_type=1,
        sender="synthetic-sender",
        sender_display_name="测试发送者",
        chat_name="123@chatroom",
        content="测试消息",
        attachment_meta=None,
    )
    return SimpleNamespace(**{**fields, **overrides})


def test_group_selection_requires_an_exact_group_name_or_id():
    reader = contact_reader({
        "123@chatroom": "测试群",
        "456@chatroom": "测试群二",
        "direct-contact": "测试",
    })
    assert cli.select_group(reader, "测试群") == "123@chatroom"
    assert cli.select_group(reader, "123@chatroom") == "123@chatroom"
    with pytest.raises(cli.ReaderError, match="^group_not_found$"):
        cli.select_group(reader, "测试")


def test_duplicate_group_names_require_the_native_id():
    reader = contact_reader({"123@chatroom": "同名群", "456@chatroom": "同名群"})
    with pytest.raises(cli.ReaderError, match="^group_name_ambiguous_use_id$"):
        cli.select_group(reader, "同名群")
    assert cli.select_group(reader, "456@chatroom") == "456@chatroom"


@pytest.mark.parametrize("initialized", [False, True])
def test_cache_only_reader_failure_does_not_start_key_capture(monkeypatch, tmp_path, initialized):
    account = tmp_path / "wxid_synthetic"
    cleared = []

    class LockedReader:
        enc_keys = {}

        def __init__(self, root, account_id, *, allow_live_key_extract):
            assert root == tmp_path and account_id == account.name
            assert allow_live_key_extract is False

        def initialize(self):
            return initialized

    monkeypatch.setattr(cli, "account_dir", lambda *_args: (tmp_path, account))
    monkeypatch.setattr(wechat_db, "WeChatDBReader", LockedReader)
    monkeypatch.setattr(wechat_db, "_decrypt_cache_clear", lambda **kwargs: cleared.append(kwargs))
    monkeypatch.setattr(
        active_key, "extract_wechat_key_active",
        lambda **_kwargs: pytest.fail("a read command must not start key capture"),
    )
    args = cli.parser().parse_args(["messages", "--group", "测试群"])
    with pytest.raises(cli.ReaderError, match="^cached_database_key_unavailable_run_extract_key$"):
        with cli.reader_for(args):
            pytest.fail("a locked reader must not be yielded")
    assert cleared == [{"retry_failed": False}]


def test_reader_refuses_partial_message_shard_unlock(monkeypatch, tmp_path):
    account = tmp_path / "wxid_synthetic"
    one, two = account / "message_0.db", account / "message_1.db"
    reader = SimpleNamespace(initialize=lambda: True, enc_keys={one: b"synthetic"})
    monkeypatch.setattr(cli, "account_dir", lambda *_args: (tmp_path, account))
    monkeypatch.setattr(wechat_db, "WeChatDBReader", lambda *args, **kwargs: reader)
    monkeypatch.setattr(wechat_db, "find_msg_databases", lambda _account: [one, two])
    monkeypatch.setattr(wechat_db, "_decrypt_cache_clear", lambda **_kwargs: None)
    args = cli.parser().parse_args(["messages", "--group", "测试群"])
    with pytest.raises(cli.ReaderError, match="^some_message_databases_remain_locked$"):
        with cli.reader_for(args):
            pytest.fail("a partial result must not look complete")


@pytest.mark.parametrize("verified", [False, True])
def test_extraction_uses_account_bound_hmac_verified_save(monkeypatch, tmp_path, verified):
    account = tmp_path / "wxid_synthetic"
    database = account / "db_storage" / "message" / "message_0.db"
    candidate = bytes(range(32))
    operations = []
    writes = []
    monkeypatch.setattr(cli, "account_dir", lambda *_args: (tmp_path, account))
    monkeypatch.setattr(wechat_db, "find_msg_databases", lambda _account: [database])

    def capture(**kwargs):
        assert kwargs == {"db_path": str(database), "timeout": 42}
        operations.append("capture")
        return candidate

    def verify(key, page):
        assert key == candidate and page == b"synthetic-page"
        operations.append("verify")
        return verified

    def write(path, value):
        operations.append("write")
        writes.append((path, value))
        return True

    monkeypatch.setattr(active_key, "extract_wechat_key_active", capture)
    monkeypatch.setattr(wechat_db, "_read_stable_page1", lambda _path: b"synthetic-page")
    monkeypatch.setattr(wechat_db, "_verify_key_v4", verify)
    monkeypatch.setattr(wechat_db, "_wechat_account_key_cache_path", lambda _account: tmp_path / "private.key")
    monkeypatch.setattr(_secrets, "write_secret_text", write)
    monkeypatch.setattr(
        wechat_db, "save_cached_wechat_key",
        lambda *_args: pytest.fail("do not use the unverified global key cache"),
    )
    args = cli.parser().parse_args(["extract-key", "--timeout", "42"])
    if verified:
        result = cli.extract_key(args)
        assert result == {"ok": True, "database_key_verified": True, "key_saved": True}
        assert candidate.hex() not in json.dumps(result)
        assert operations == ["capture", "verify", "write"]
        assert writes == [(tmp_path / "private.key", candidate.hex())]
    else:
        with pytest.raises(cli.ReaderError, match="^database_key_verification_or_save_failed$"):
            cli.extract_key(args)
        assert operations == ["capture", "verify"]
        assert writes == []


def test_message_output_omits_attachment_credentials():
    message = sample_message(msg_type=3, content="[图片]", attachment_meta={
        "kind": "image", "file_md5": "synthetic-image-id",
        "aeskey": "synthetic-xml-secret", "cdnthumburl": "synthetic-thumb-secret",
        "cdnmidimgurl": "synthetic-mid-secret", "cdnbigimgurl": "synthetic-big-secret",
        "unexpected_private_field": "synthetic-extra-secret",
    })
    result = cli.message_dict(message)
    assert result["attachment"] == {"kind": "image", "file_md5": "synthetic-image-id"}
    assert "secret" not in json.dumps(result)


@pytest.mark.parametrize("text", [
    '[引用 测试: <msg><img aeskey="synthetic-quoted-secret"/></msg>]',
    "[引用 测试: <msg><img AESKEY='synthetic-quoted-secret'/></msg>]",
    "[引用 测试: <msg><aeskey>synthetic-quoted-secret</aeskey></msg>]",
])
def test_message_output_redacts_xml_credentials_in_quote_fallback_text(text):
    message = sample_message(msg_type=49, content=text)
    serialized = json.dumps(cli.message_dict(message))
    assert "synthetic-quoted-secret" not in serialized


@pytest.mark.parametrize("found", [False, True])
def test_image_key_discovery_receives_the_selected_account(monkeypatch, tmp_path, found):
    account = tmp_path / "wxid_selected_synthetic"
    observed = {}

    def discover(root, *, expected_account):
        observed.update(root=root, expected_account=expected_account)
        return {"found": found, "error": "account_selection_mismatch"} if not found else {"found": True}

    monkeypatch.setattr(cli, "account_dir", lambda *_args: (tmp_path, account))
    monkeypatch.setattr(image_key, "discover_uin", discover)
    args = cli.parser().parse_args(["image-key", "--account", account.name])
    if found:
        assert cli.image_key(args) == {"found": True}
    else:
        with pytest.raises(cli.ReaderError, match="^account_selection_mismatch$"):
            cli.image_key(args)
    assert observed == {"root": tmp_path, "expected_account": account.name}


def test_message_page_receives_the_selected_group_limit_and_time_window(monkeypatch):
    fixed_now = datetime(2026, 1, 2, 13, 0, 0, tzinfo=cli.ZONE)
    observed = {}
    reader = contact_reader({"123@chatroom": "测试群"})

    def read_page(**kwargs):
        observed.update(kwargs)
        return SimpleNamespace(messages=[sample_message()], has_more=True, scanned_rows=7)

    reader.read_conversation_page = read_page

    @contextmanager
    def selected_reader(_args):
        yield reader

    class FixedDatetime:
        @staticmethod
        def now(zone):
            assert zone == cli.ZONE
            return fixed_now

    monkeypatch.setattr(cli, "reader_for", selected_reader)
    monkeypatch.setattr(cli, "datetime", FixedDatetime)
    args = cli.parser().parse_args([
        "messages", "--group", "测试群", "--minutes", "37", "--limit", "7",
    ])
    result = cli.messages(args)
    until = int(fixed_now.timestamp())
    assert observed == {
        "conversation_id": "123@chatroom", "since_ts": until - 37 * 60,
        "until_ts": until, "page_size": 7,
    }
    assert result["has_more"] is True and result["scanned_rows"] == 7
    assert result["messages"][0]["text"] == "测试消息"


@pytest.mark.parametrize("argv", [
    ["messages", "--group", "x", "--limit", "0"],
    ["messages", "--group", "x", "--limit", "1001"],
    ["messages", "--group", "x", "--minutes", "0"],
    ["messages", "--group", "x", "--minutes", "1441"],
    ["extract-key", "--timeout", "9"],
    ["extract-key", "--timeout", "601"],
])
def test_cli_rejects_unbounded_work_before_reading_data(argv):
    with pytest.raises(SystemExit) as stopped:
        cli.parser().parse_args(argv)
    assert stopped.value.code == 2


def test_cli_has_only_linux_wechat_and_archive_commands():
    commands = next(action for action in cli.parser()._actions if hasattr(action, "choices") and isinstance(action.choices, dict))
    assert set(commands.choices) == {"probe", "extract-key", "groups", "messages", "image-key", "image", "collect", "mcp"}


def test_main_reports_a_short_reader_error_without_exception_content(monkeypatch, capsys):
    def failed(_args):
        raise OSError("synthetic-sensitive-file-path-and-secret")

    monkeypatch.setattr(cli, "probe", failed)
    assert cli.main(["probe"]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result == {"ok": False, "error": "operation_failed", "type": "OSError"}


@pytest.mark.parametrize("probe_fails,notify_fails", [(False, False), (True, False), (False, True)])
def test_collect_wires_readonly_client_probe_and_effective_health_alerts(
    monkeypatch, tmp_path, probe_fails, notify_fails,
):
    import sys
    from chatlog_keeper import archive, collector as collector_module

    observed = {"probes": [], "notifications": [], "closed": 0}
    selected = "wxid_selected_synthetic"
    monkeypatch.setenv("CHATLOG_KEEPER_DATA_DIR", str(tmp_path))

    class Store:
        def __init__(self, state):
            assert state == tmp_path

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def get_status(self):
            return {"collector": {"account_id": selected}}

        def get_live_health(self, *, client_health):
            observed["client_health"] = dict(client_health)
            # A readable cached source must not turn a logged-out client healthy.
            return {"state": client_health["state"], "client": client_health,
                    "collector_healthy": True}

    class Alerts:
        def __init__(self, store):
            assert isinstance(store, Store)

        def observe(self, effective):
            observed["notifications"].append(effective)
            if notify_fails:
                raise RuntimeError("sensitive desktop notification details")
            return {"status": "debouncing"}

    class Collector:
        def __init__(self, store, **options):
            self.store = store
            self.account_id = options["account"]
            self.monitor = options["health_monitor"]

        def collect_once(self):
            return {"errors": [], "client_health": self.monitor(self)}

        def close(self):
            observed["closed"] += 1

    def probe(*, account_id):
        observed["probes"].append(account_id)
        if probe_fails:
            raise OSError("sensitive desktop session path")
        return {"state": "login_required", "reason": "official_login_window", "checked_at": 123}

    monkeypatch.setattr(archive, "ArchiveStore", Store)
    monkeypatch.setattr(collector_module, "Collector", Collector)
    monkeypatch.setitem(sys.modules, "chatlog_keeper.client_health", SimpleNamespace(probe_client=probe))
    monkeypatch.setitem(sys.modules, "chatlog_keeper.health_alerts", SimpleNamespace(HealthAlerts=Alerts))
    result = cli.collect(cli.parser().parse_args(["collect", "--once", "--state-dir", str(tmp_path)]))
    assert observed["probes"] == [selected]
    assert observed["closed"] == 1
    assert result["errors"] == []
    expected = "unknown" if probe_fails else "login_required"
    assert observed["client_health"]["state"] == expected
    assert observed["notifications"][0]["state"] == expected
    assert result["client_health"]["notifications"]["status"] == (
        "failed" if notify_fails else "debouncing")
    assert "sensitive" not in json.dumps(result)
