"""Synthetic local cached files only; never discover a real WeChat account."""
from datetime import datetime
import hashlib
import html
from pathlib import Path
import shutil
from zoneinfo import ZoneInfo

import pytest

from chatlog_keeper import archive_media
from chatlog_keeper.archive_media import MediaArchiver, MediaService
from chatlog_keeper.message_model import parse_record
from test_archive_media import FakeStore

STAMP = int(datetime(2026, 10, 24, 12, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())


def message(title="report.pdf", md5="a" * 32, size="12", *, app_md5="", root_tag="msg"):
    app = (f'<appmsg><type>6</type><title>{html.escape(title)}</title><md5>{app_md5}</md5>'
           f'<appattach><md5>{md5}</md5><totallen>{size}</totallen><fileext>pdf</fileext>'
           '<aeskey>PRIVATE</aeskey><cdnattachurl>https://example.invalid/PRIVATE</cdnattachurl>'
           '</appattach></appmsg>')
    return f"<msg>{app}</msg>" if root_tag == "msg" else app


@pytest.fixture
def files(tmp_path):
    data_root = tmp_path / "wechat"
    account = data_root / "wxid_synthetic"
    account.mkdir(parents=True)
    store = FakeStore(tmp_path / "state")
    archiver = MediaArchiver(store, data_root, account)
    return store, account, archiver


def row(store, account, payload, filename="report.pdf", **attachment):
    item = {"media_ref": "media_file", "message_ref": "msg_file", "account_id": account.name,
            "group_id": "synthetic@chatroom", "create_time": STAMP, "kind": "file",
            "attachment": {"kind": "file", "filename": filename,
                           "md5": hashlib.md5(payload).hexdigest(), "total_bytes": len(payload), **attachment}}
    store.rows[item["media_ref"]] = item
    return item


def put(account, payload, relative="msg/file/2026-10/report.pdf"):
    path = account / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def test_file_metadata_accepts_known_md5_sources_and_hides_keys():
    for body in (message(md5="A" * 32), message(md5="", app_md5="A" * 32),
                 message(md5="", app_md5="A" * 32, root_tag="appmsg")):
        parsed = parse_record({"local_type": 49, "message_content": body})
        assert parsed["parse_status"] == "parsed"
        assert parsed["attachment"]["md5"] == "a" * 32
        assert parsed["attachment"]["total_bytes"] == 12
        assert parsed["attachment"]["metadata_status"] == "complete"
        assert "PRIVATE" not in str(parsed)


def test_file_metadata_preserves_missing_bad_and_conflicting_evidence():
    parsed = parse_record({"local_type": 49, "message_content": message(size="not-an-integer")})
    assert parsed["parse_status"] == "parsed"
    assert parsed["attachment"]["total_bytes"] is None
    assert parsed["attachment"]["size_status"] == "missing_or_invalid"
    parsed = parse_record({"local_type": 49, "message_content": message(md5="a" * 32, app_md5="b" * 32)})
    assert parsed["attachment"]["md5"] == ""
    assert parsed["attachment"]["md5_status"] == "conflicting"


def test_forwarded_file_keeps_public_digest_name_and_size():
    record = '<recordinfo><datalist><dataitem datatype="8"><datatitle>Report.docx</datatitle><fullmd5>' + 'A' * 32 + '</fullmd5><datasize>123</datasize><fileext>docx</fileext><key>PRIVATE</key></dataitem></datalist></recordinfo>'
    body = '<msg><appmsg><type>19</type><recorditem>' + html.escape(record) + '</recorditem></appmsg></msg>'
    parsed = parse_record({"local_type": 49, "message_content": body})
    attachment = parsed["forwarded"][0]["attachment"]
    assert attachment["kind"] == "file"
    assert attachment["md5"] == attachment["file_md5"] == "a" * 32
    assert attachment["filename"] == "Report.docx"
    assert attachment["total_bytes"] == 123
    assert "PRIVATE" not in str(parsed)


def test_exact_file_survives_source_removal_with_hash_evidence(files):
    store, account, archiver = files
    payload = b"synthetic PDF contents"
    item = row(store, account, payload)
    source = put(account, payload)
    assert archiver.process_pending()["ready"] == 1
    assert item["association_evidence"] == "content_md5_and_size"
    assert item["raw_size"] == len(payload)
    source.unlink()
    shutil.rmtree(account)
    assert archiver.process_pending()["ready"] == 1
    assert MediaService(store).get_media("media_file")["data"] == payload


def test_renamed_cache_is_linked_only_by_identical_contents(files):
    store, account, archiver = files
    payload = b"verified document"
    item = row(store, account, payload)
    put(account, b"wrong file", "msg/file/2026-10/report.pdf")
    put(account, payload, "msg/attach/other-chat/2026-10/File/report(1).pdf")
    assert archiver.process_pending()["ready"] == 1
    assert item["source_filename"] == "report(1).pdf"
    assert item["filename"] == "report.pdf"
    assert MediaService(store).get_media("media_file")["data"] == payload
    assert len(list(store.media_dir.rglob("*.bin"))) == 1


def test_identical_name_never_proves_link_and_wrong_size_never_copied(files):
    store, account, archiver = files
    payload = b"correct"
    item = row(store, account, payload)
    put(account, b"incorrect")
    assert archiver.process_pending()["unresolved"] == 1
    assert item["reason"] == "cached_file_size_mismatch"
    assert not list(store.media_dir.rglob("*.bin"))
    put(account, payload)
    item["attachment"]["total_bytes"] += 1
    assert archiver.process_pending()["unresolved"] == 1
    assert item["reason"] == "cached_file_size_mismatch"


def test_missing_digest_never_adopts_a_same_named_file(files):
    store, account, archiver = files
    item = row(store, account, b"file", md5="")
    put(account, b"file")
    assert archiver.process_pending()["unresolved"] == 1
    assert item["reason"] == "file_link_unresolved"
    assert not list(store.media_dir.rglob("*.bin"))


def test_empty_file_and_unknown_size_are_distinct_valid_cases(files):
    store, account, archiver = files
    item = row(store, account, b"")
    put(account, b"")
    assert archiver.process_pending()["ready"] == 1
    assert item["raw_size"] == 0
    item["attachment"]["total_bytes"] = None
    assert archiver.process_pending()["ready"] == 1
    assert item["association_evidence"] == "content_md5"


def test_file_size_and_renamed_scan_budgets_are_explicit(files, monkeypatch):
    store, account, archiver = files
    payload = b"123456789"
    item = row(store, account, payload)
    put(account, payload, "msg/file/2026-10/renamed.pdf")
    monkeypatch.setattr(archive_media, "_MAX_FILE_SCAN_BYTES", 4)
    assert archiver.process_pending()["unresolved"] == 1
    assert item["reason"] == "file_scan_byte_limit"
    put(account, payload)
    assert archiver.process_pending()["ready"] == 1  # Exact names are not renamed scans.
    item.pop("variants")
    item.pop("raw_path")
    monkeypatch.setattr(archive_media, "_MAX_FILE_BYTES", 4)
    assert archiver.process_pending()["unresolved"] == 1
    assert item["reason"] == "cached_file_size_limit"


def test_candidate_limit_does_not_prevent_exact_name_proof(files, monkeypatch):
    store, account, archiver = files
    item = row(store, account, b"proof")
    put(account, b"wrong", "msg/file/2026-10/a.pdf")
    put(account, b"proof")
    monkeypatch.setattr(archive_media, "_MAX_FILE_CANDIDATES", 1)
    assert archiver.process_pending()["ready"] == 1
    assert item["content_md5"] == hashlib.md5(b"proof").hexdigest()


def test_cache_symlinks_are_ignored_including_parent_directory(files, tmp_path):
    store, account, archiver = files
    payload = b"outside"
    item = row(store, account, payload)
    outside = tmp_path / "outside.pdf"
    outside.write_bytes(payload)
    path = account / "msg/file/2026-10/report.pdf"
    path.parent.mkdir(parents=True)
    path.symlink_to(outside)
    assert archiver.process_pending()["missing"] == 1
    path.unlink()
    shutil.rmtree(path.parent)
    path.parent.symlink_to(tmp_path)
    assert archiver.process_pending()["missing"] == 1
    assert not list(store.media_dir.rglob("*.bin"))


def test_copy_deadline_leaves_no_published_partial_file(files, monkeypatch):
    store, account, archiver = files
    item = row(store, account, b"timeout")
    put(account, b"timeout")
    monkeypatch.setattr(archive_media, "_FILE_COPY_SECONDS", -1)
    assert archiver.process_pending()["unresolved"] == 1
    assert item["reason"] == "cached_file_copy_timeout"
    assert not list(store.media_dir.rglob("*.bin"))
    assert not list(store.media_dir.rglob(".copy-*"))


def test_corrupt_archive_is_not_falsely_ready_when_source_is_gone(files):
    store, account, archiver = files
    item = row(store, account, b"original")
    source = put(account, b"original")
    assert archiver.process_pending()["ready"] == 1
    (store.media_dir / item["raw_path"]).write_bytes(b"tampered")
    source.unlink()
    assert archiver.process_pending()["unresolved"] == 1
    assert item["reason"] == "archive_blob_corrupt"


def test_other_month_and_other_account_files_are_not_scanned(files):
    store, account, archiver = files
    item = row(store, account, b"proof")
    put(account, b"proof", "msg/file/2026-09/report.pdf")
    put(account.parent / "other_account", b"proof")
    assert archiver.process_pending()["missing"] == 1
    assert item["reason"] == "media_not_cached"
