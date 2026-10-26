"""P1 source metadata is collected without inventing messages or checkpoints."""
import sqlite3
from types import SimpleNamespace

import zstandard

from chatlog_keeper.collector import Collector
from chatlog_keeper.message_model import source_row_hash
from test_collector import environment, GROUP, TABLE  # noqa: F401


def source_connection(source_columns=True):
    connection = sqlite3.connect(":memory:")
    connection.execute(f'''CREATE TABLE "{TABLE}"(local_type INTEGER, create_time INTEGER,
        real_sender_id INTEGER, message_content BLOB, server_id TEXT, packed_info_data BLOB
        {', source BLOB, WCDB_CT_source INTEGER, message_source BLOB' if source_columns else ''})''')
    return connection


def row_reader(store=None):
    collector = Collector(store or SimpleNamespace())
    collector.account_id = "account"
    collector._source_ref = "source_ref"
    return collector


def test_linux_native_source_and_compression_are_preserved_without_alias_override():
    connection = source_connection()
    native_xml = '<msgsource><atuserlist>wxid_target,notify@all</atuserlist></msgsource>'
    native = zstandard.ZstdCompressor().compress(native_xml.encode())
    connection.execute(f'INSERT INTO "{TABLE}" VALUES(?,?,?,?,?,?,?,?,?)',
                       (1, 100, 2, b"hello", "123", None, native, 4,
                        "<msgsource><atuserlist>wrong</atuserlist></msgsource>"))
    record = row_reader()._rows(connection, TABLE, GROUP, "group", {2: "sender"}, 0)[0]
    assert record["message_source"] == native
    assert record["source_compression"] == 4
    assert record["source_metadata_checked"] is True
    assert record["parsed"]["mentions"] == ["wxid_target", "notify@all"]
    connection.close()


def test_unsupported_source_column_is_checked_without_claiming_metadata_exists():
    connection = source_connection(False)
    connection.execute(f'INSERT INTO "{TABLE}" VALUES(?,?,?,?,?,?)',
                       (1, 100, 2, b"@someone", "123", None))
    record = row_reader()._rows(connection, TABLE, GROUP, "group", {2: "sender"}, 0)[0]
    assert record["message_source"] is None
    assert record["source_compression"] is None
    assert record["source_metadata_checked"] is True
    assert record["parsed"]["mentions"] == []
    connection.close()


def test_native_source_upgrade_does_not_trigger_full_reconcile():
    connection = source_connection()
    connection.execute(f'INSERT INTO "{TABLE}" VALUES(?,?,?,?,?,?,?,?,?)',
                       (1, 100, 2, b"hello", "123", None,
                        "<msgsource><atuserlist>target</atuserlist></msgsource>", None, None))
    collector = row_reader()
    current = collector._rows(connection, TABLE, GROUP, "group", {2: "sender"}, 0)[0]
    legacy = dict(current, message_source=None)
    stream = {"live_rowid": 1, "backfill_rowid": 1, "boundary_rowid": 1,
              "boundary_hash": source_row_hash(legacy), "reconcile_active": False}
    collector.store = SimpleNamespace(get_stream=lambda *args: stream,
                                     get_origin_info=lambda *args: legacy)
    reset_reason, reconciles = collector._validate_streams(
        connection, [(TABLE, GROUP, "group")], {2: "sender"},
        {"source_ref": "source_ref", "generation": 1})
    assert reset_reason is None
    assert reconciles == []
    connection.close()


def test_metadata_backfill_is_bounded_and_marks_missing_source_rows_checked():
    connection = source_connection()
    connection.executemany(f'INSERT INTO "{TABLE}" VALUES(?,?,?,?,?,?,?,?,?)', [
        (1, 100, 2, b"one", "101", None, "<msgsource/>", None, None),
        (1, 100, 2, b"two", "102", None, "<msgsource/>", None, None),
    ])
    calls = []
    store = SimpleNamespace(
        metadata_backfill_rows=lambda *args, limit: [1, 2, 3][:limit],
        backfill_source_metadata=lambda *args: calls.append(("refresh", args)),
        mark_metadata_checked=lambda *args: calls.append(("missing", args)),
    )
    collector = row_reader(store)
    collector._metadata_budget = 3
    source = {"source_ref": "source_ref", "generation": 1}
    groups = [(TABLE, GROUP, "group"), ("Msg_" + "a" * 32, "other@chatroom", "other")]
    assert collector._backfill_metadata(connection, groups, {2: "sender"}, source) == 2
    assert collector._metadata_budget == 0
    assert [r["row_id"] for r in calls[0][1][-1]] == [1, 2]
    assert calls[1] == ("missing", ("source_ref", 1, TABLE, [3]))
    connection.close()


def test_contact_snapshot_is_saved_when_contact_family_changes(tmp_path, monkeypatch):
    calls = []
    store = SimpleNamespace(update_group_directory=lambda *args: calls.append(args))
    collector = row_reader(store)
    collector.account_dir = tmp_path
    groups = [{"group_id": GROUP, "coverage": {"status": "available"}, "members": []}]
    resolver = SimpleNamespace(_wxid_to_display={GROUP: "group"},
                               directory_snapshot=lambda: groups)
    reader = SimpleNamespace(_load_contacts=lambda: resolver)
    monkeypatch.setattr("chatlog_keeper.collector._family_signature", lambda _: "one")
    assert collector._names(reader) == {GROUP: "group"}
    assert calls == [("account", groups)]
    collector._names(reader)
    assert len(calls) == 1
    monkeypatch.setattr("chatlog_keeper.collector._family_signature", lambda _: "two")
    collector._names(reader)
    assert len(calls) == 2


def test_unchanged_source_metadata_backfill_runs_once_and_then_releases_plaintext(environment):
    collector, store, database, flags = environment
    collector.page_size = 500
    collector.collect_once()
    assert flags["decryptions"] == 1
    pending = [1, 2, 99]
    refreshes = []
    missing = []
    store.metadata_backfill_needed = lambda *args: bool(pending)
    store.metadata_backfill_rows = lambda *args, limit: pending[:limit]

    def refresh(ref, generation, table, rows):
        refreshes.extend(rows)
        for row in rows:
            pending.remove(row["row_id"])

    def mark(ref, generation, table, rows):
        missing.extend(rows)
        for row_id in rows:
            pending.remove(row_id)

    store.backfill_source_metadata = refresh
    store.mark_metadata_checked = mark
    store.reparse_batch = lambda limit: {"reparsed": 2, "pending": 0}
    result = collector.collect_once()
    assert result["metadata_rows_refreshed"] == 2
    assert result["historical_rows_reparsed"] == 2
    assert [row["row_id"] for row in refreshes] == [1, 2]
    assert missing == [99]
    assert not collector._snapshots
    assert flags["decryptions"] == 2
    collector.collect_once()
    assert flags["decryptions"] == 2
    assert len(store.records) == 3
    assert next(iter(store.sources.values()))["generation"] == 1


def test_real_legacy_metadata_upgrade_preserves_identity_watermarks_and_boundary(environment, tmp_path):
    from chatlog_keeper.archive import ArchiveStore
    from chatlog_keeper.message_model import parse_record

    fixture, _, database, flags = environment
    with sqlite3.connect(database) as connection:
        connection.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN source BLOB')
        connection.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN WCDB_CT_source INTEGER')
        source = '<msgsource><atuserlist>wxid_target</atuserlist></msgsource>'
        compressed = zstandard.ZstdCompressor().compress(source.encode())
        connection.execute(f'UPDATE "{TABLE}" SET source=?, WCDB_CT_source=4', (compressed,))
    with ArchiveStore(tmp_path / "actual-metadata-upgrade") as store:
        collector = Collector(store, data_root=fixture.data_root, page_size=500)
        modern_rows = collector._rows
        modern_needed = collector._metadata_needs_work

        def legacy_rows(*args, **kwargs):
            rows = modern_rows(*args, **kwargs)
            for raw in rows:
                raw.update(message_source=None, source_compression=None,
                           source_metadata_checked=False)
                raw["parsed"] = parse_record(raw)
            return rows

        # Simulate exactly the P0 omission rather than modifying the source DB.
        collector._rows = legacy_rows
        collector._metadata_needs_work = lambda source: False
        try:
            assert not collector.collect_once()["errors"]
            identities = [tuple(row) for row in store.conn.execute(
                "SELECT message_ref,first_seen FROM messages ORDER BY archive_id")]
            progress = [tuple(row) for row in store.conn.execute(
                "SELECT live_rowid,backfill_rowid,backfill_done,last_reconciled FROM streams")]
            collector._rows = modern_rows
            collector._metadata_needs_work = modern_needed
            upgraded = collector.collect_once()
            assert not upgraded["errors"]
            assert upgraded["rows_processed"] == 0
            assert upgraded["metadata_rows_refreshed"] == 3
            assert upgraded["historical_rows_reparsed"] == 3
            assert identities == [tuple(row) for row in store.conn.execute(
                "SELECT message_ref,first_seen FROM messages ORDER BY archive_id")]
            assert progress == [tuple(row) for row in store.conn.execute(
                "SELECT live_rowid,backfill_rowid,backfill_done,last_reconciled FROM streams")]
            assert all(bytes(row[0]) == compressed and row[1] == 4 and row[2] == 1
                       for row in store.conn.execute(
                           "SELECT message_source,source_compression,source_metadata_checked FROM origins"))
            assert store.conn.execute("SELECT COUNT(*) FROM messages WHERE mentions<> '[]'").fetchone()[0] == 3
            # On the next actual source change the upgraded boundary hash must
            # not start a redundant historical reconcile.
            with sqlite3.connect(database) as connection:
                connection.execute(f'INSERT INTO "{TABLE}" VALUES(?,?,?,?,?,?,?,?)',
                                   (1, 101, 2, b"new", "104", None, source, 0))
            new_cycle = collector.collect_once()
            assert not new_cycle["errors"]
            assert new_cycle["rows_processed"] == 1
            assert store.conn.execute("SELECT generation FROM sources").fetchone()[0] == 1
            assert store.conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 4
        finally:
            collector.close()
