"""Legacy archive upgrade must queue documents without changing message identity."""
import hashlib

from chatlog_keeper.archive import ArchiveStore
from test_archive import setup_source, commit, raw
from chatlog_keeper.documents import DocumentService


def test_schema2_upgrade_and_parser3_backfill_preserve_origins_and_progress(tmp_path):
    body = b"synthetic file body"
    digest = hashlib.md5(body).hexdigest()
    content = ('<msg><appmsg><type>6</type><title>history.txt</title><md5>' + digest +
               '</md5><appattach><totallen>' + str(len(body)) +
               '</totallen><fileext>txt</fileext></appattach></appmsg></msg>')
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        ref = commit(store, source, [raw(local_type=49, message_content=content,
                    parsed={"msg_type": 49, "sub_type": 6, "text": "[file]", "parse_status": "parsed"})])["message_refs"][0]
        store.conn.execute("UPDATE messages SET parser_version=2")
        before = tuple(store.conn.execute("SELECT message_ref,archive_id,first_seen FROM messages").fetchone())
        origins = [tuple(r) for r in store.conn.execute("SELECT * FROM origins")]
        stream = [tuple(r) for r in store.conn.execute("SELECT * FROM streams")]
        store.conn.execute("DROP INDEX media_forward")
        store.conn.execute("ALTER TABLE media DROP COLUMN forward_ref")
        store.conn.execute("PRAGMA user_version=2")
        store.conn.commit()
    with ArchiveStore(tmp_path) as upgraded:
        assert upgraded.conn.execute("PRAGMA user_version").fetchone()[0] == 4
        assert len(list((upgraded.archive_dir / "backups").glob("schema-2-*.sqlite"))) == 1
        result = upgraded.reparse_batch(500)
        assert result == {"reparsed": 1, "pending": 0}
        assert tuple(upgraded.conn.execute("SELECT message_ref,archive_id,first_seen FROM messages").fetchone()) == before
        assert [tuple(r) for r in upgraded.conn.execute("SELECT * FROM origins")] == origins
        assert [tuple(r) for r in upgraded.conn.execute("SELECT * FROM streams")] == stream
        media = upgraded.get_media_records(message_ref=ref)
        assert len(media) == 1 and media[0]["kind"] == "file"
        assert media[0]["attachment"]["md5"] == digest
        assert media[0]["attachment"]["total_bytes"] == len(body)
        assert media[0]["status"] == "pending"
        assert upgraded.reparse_batch(500) == {"reparsed": 0, "pending": 0}
        assert len(upgraded.get_media_records(message_ref=ref)) == 1


def test_message_reference_does_not_choose_old_ready_file_after_content_change(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        def record(digest):
            return raw(local_type=49, parsed={"msg_type": 49, "sub_type": 6, "text": "file", "parse_status": "parsed",
                                             "attachment": {"kind": "file", "filename": "updated.txt", "md5": digest, "total_bytes": 10}})
        ref = commit(store, source, [record("a" * 32)])["message_refs"][0]
        old = store.get_media_records(message_ref=ref)[0]
        store.update_media(old["media_ref"], status="ready")
        commit(store, source, [record("b" * 32)])
        current = DocumentService(store).get_attachment(ref)["metadata"]
        assert current["expected_md5"] == "b" * 32 and current["status"] == "pending"
        # Explicit historical media IDs still describe that archived object.
        historical = DocumentService(store).get_attachment(old["media_ref"])["metadata"]
        assert historical["expected_md5"] == "a" * 32
