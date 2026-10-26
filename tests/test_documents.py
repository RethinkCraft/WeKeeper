"""Archived document queries use synthetic blobs only, independent of WeChat."""
import hashlib
import json
import os
from pathlib import Path

import pytest

from chatlog_keeper import documents
from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.archive_media import MediaUnavailable
from chatlog_keeper.document_extractors import extract_document
from chatlog_keeper.documents import DocumentService
from test_archive import commit, raw, setup_source
from test_document_extractors import docx, paragraph, pdf_bytes, xlsx


def file_parsed(data, filename="sample.txt"):
    return {"msg_type": 49, "sub_type": 6, "text": "[文件] " + filename,
            "parse_status": "parsed", "attachment": {
                "kind": "file", "filename": filename, "md5": hashlib.md5(data).hexdigest(),
                "fileext": Path(filename).suffix.lstrip("."), "total_bytes": len(data)}}


def put_blob(store, media_ref, data):
    digest = hashlib.sha256(data).hexdigest()
    relative = f"raw/{digest[:2]}/{digest}.bin"
    path = store.media_dir / relative
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_bytes(data)
    path.chmod(0o600)
    store.update_media(media_ref, status="ready", raw_path=relative, raw_sha256=digest,
                       raw_size=len(data), tier="full_cache", mime="application/octet-stream",
                       association_evidence="filename_and_content_md5")
    return path


def seed(store, data=b"Synthetic document\n", filename="sample.txt", number=1):
    source = setup_source(store)
    message = commit(store, source, [raw(number, f"sid-{number}", local_type=49,
                                        parsed=file_parsed(data, filename))])["message_refs"][0]
    record = store.get_media_records(message_ref=message)[0]
    path = put_blob(store, record["media_ref"], data)
    return source, message, record["media_ref"], path


def read_all(service, reference, limit=100):
    cursor, units, pages = None, [], []
    while True:
        page = service.read_document(reference, cursor=cursor, limit=limit)
        pages.append(page)
        units.extend(page["units"])
        assert len(json.dumps(page, ensure_ascii=False).encode()) < 128 * 1024
        assert sum(len(json.dumps(u, ensure_ascii=False).encode()) for u in page["units"]) <= documents.PAGE_BYTES
        if not page["has_more"]:
            assert page["next_cursor"] is None
            return units, pages
        assert page["next_cursor"] and page["next_cursor"] != cursor
        cursor = page["next_cursor"]
        assert len(pages) < 100


def test_real_worker_long_unicode_single_line_paginates_without_truncation_offline(tmp_path):
    text = "第一行完整正文" * 15000 + "\nlast line\n"
    with ArchiveStore(tmp_path) as store:
        _, message, media, _ = seed(store, text.encode())
    # No source cache, encryption key or client is available in this fixture.
    with ArchiveStore(tmp_path, readonly=True) as store:
        service = DocumentService(store)
        units, pages = read_all(service, message)
        assert len(pages) > 1
        assert "".join(unit["text"] for unit in units) == text
        first_unit = [unit for unit in units if unit["unit"] == 1]
        assert [u["offset_chars"] for u in first_unit] == list(range(0, len(first_unit) * 4096, 4096))
        assert all(u["length_chars"] == len(u["text"]) and u["offset_scope"] == "unit_text" for u in units)
        assert all(page["extraction_complete"] and page["text_complete"] for page in pages)
        assert all(page["encoding"] == "utf-8" for page in pages)
        assert pages[0]["document_metadata"]["scope"] == "text_lines"
        assert service.get_attachment(media, include_content=True)["data"] == text.encode()


@pytest.mark.parametrize("kind", ["docx", "xlsx", "pdf"])
def test_real_worker_office_and_pdf_match_direct_parser(tmp_path, kind):
    if kind == "docx":
        data = docx(paragraph("before") + '<w:tbl><w:tr><w:tc>' + paragraph("cell") +
                    '</w:tc></w:tr></w:tbl>' + paragraph("after"))
    elif kind == "xlsx":
        data = xlsx([("First", '<sheetData><row r="2"><c r="C2"><f>1+2</f><v>3</v></c></row></sheetData>'),
                     ("空工作表", '<sheetData/>')])
    else:
        data = pdf_bytes("Synthetic PDF body")
    expected = extract_document(data, "sample." + kind)
    with ArchiveStore(tmp_path) as store:
        _, message, _, _ = seed(store, data, "sample." + kind)
        actual = DocumentService(store).read_document(message)
        assert actual["status"] == expected["status"]
        assert [u["text"] for u in actual["units"]] == [u["text"] for u in expected["units"]]
        assert actual["document_metadata"] == expected["metadata"]
        assert actual["metadata_status"] == "complete"
        if kind == "xlsx":
            assert len(actual["document_metadata"]["sheets"]) == 2
            assert actual["units"][0]["cells"][0]["formula_status"] == "not_evaluated"


@pytest.mark.parametrize("filename,data,status", [
    ("legacy.doc", b"legacy Office", "unsupported"),
    ("empty.txt", b"", "empty"),
    ("bad.pdf", b"broken PDF", "parse_error"),
    ("unknown.bin", b"document bytes", "unsupported"),
])
def test_explicit_document_failure_and_empty_statuses(tmp_path, filename, data, status):
    with ArchiveStore(tmp_path) as store:
        _, message, _, _ = seed(store, data, filename)
        page = DocumentService(store).read_document(message)
        assert page["status"] == status
        assert page["has_more"] is False and page["next_cursor"] is None
        assert page["extraction_complete"] is False
        assert page["attachment"]["status"] == "ready"


def test_pdf_password_and_blank_pages_remain_distinct_from_missing_file(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, encrypted, _, _ = seed(store, pdf_bytes("private", encrypted=True), "encrypted.pdf")
        _, empty, _, _ = seed(store, pdf_bytes(), "no-text.pdf", 2)
        service = DocumentService(store)
        assert service.read_document(encrypted)["status"] == "encrypted"
        page = service.read_document(empty)
        assert page["status"] == "empty"
        assert page["document_metadata"]["ocr_performed"] is False
        assert page["document_metadata"]["pages_without_text"] == [1]


def test_missing_attachment_has_explicit_status_and_no_extractor_call(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        message = commit(store, source, [raw(parsed=file_parsed(b"not-cached"))])["message_refs"][0]
        media = store.get_media_records(message_ref=message)[0]["media_ref"]
        store.update_media(media, status="missing", reason="media_not_cached")
        def unexpected(*args):
            raise AssertionError("must not parse absent bytes")
        service = DocumentService(store, extractor=unexpected)
        assert service.get_attachment(message)["metadata"]["status"] == "missing"
        page = service.read_document(message)
        assert page["status"] == "missing" and page["units"] == []
        assert page["reason"] == "media_not_cached"


@pytest.mark.parametrize("mode,reason", [("missing", "archive_blob_missing"),
                                         ("corrupt", "archive_blob_corrupt"),
                                         ("symlink", "unsafe_archive_blob"),
                                         ("traversal", "invalid_archive_blob"),
                                         ("wrong_filename_hash", "archive_blob_corrupt")])
def test_metadata_and_content_both_validate_archive_blobs(tmp_path, mode, reason):
    with ArchiveStore(tmp_path) as store:
        _, message, media, path = seed(store)
        data = path.read_bytes()
        if mode == "missing":
            path.unlink()
        elif mode == "corrupt":
            path.write_bytes(b"changed")
        elif mode == "symlink":
            target = tmp_path / "other-bytes"
            target.write_bytes(data)
            path.unlink()
            path.symlink_to(target)
        elif mode == "traversal":
            store.update_media(media, raw_path="../../outside.txt")
        else:
            wrong = store.media_dir / "raw" / "00" / ("0" * 64 + ".bin")
            wrong.parent.mkdir(mode=0o700)
            wrong.write_bytes(data)
            store.update_media(media, raw_path=str(wrong.relative_to(store.media_dir)))
        service = DocumentService(store)
        for operation in (lambda: service.get_attachment(message),
                          lambda: service.get_attachment(message, include_content=True),
                          lambda: service.read_document(message)):
            with pytest.raises(MediaUnavailable) as error:
                operation()
            assert error.value.code == reason


def test_fifo_and_foreign_owned_blob_are_rejected_before_read(tmp_path, monkeypatch):
    with ArchiveStore(tmp_path) as store:
        _, message, _, path = seed(store)
        service = DocumentService(store)
        actual_uid = os.geteuid()
        monkeypatch.setattr(documents.os, "geteuid", lambda: actual_uid + 1)
        with pytest.raises(MediaUnavailable, match="unsafe_archive_blob"):
            service.get_attachment(message)
        monkeypatch.undo()
        path.unlink()
        os.mkfifo(path)
        with pytest.raises(MediaUnavailable, match="unsafe_archive_blob"):
            service.get_attachment(message)


def test_cursor_binds_blob_filename_and_extractor_version(tmp_path, monkeypatch):
    data = b"one,two\nthree,four\n"
    with ArchiveStore(tmp_path) as store:
        _, message, media, _ = seed(store, data, "table.csv")
        service = DocumentService(store)
        first = service.read_document(message, limit=1)
        assert first["has_more"]
        record = store.get_media_records(media_ref=media)[0]
        with store.conn:
            store.conn.execute("UPDATE media SET attachment=? WHERE media_ref=?",
                               (json.dumps({**record["attachment"], "filename": "table.txt"}), media))
        with pytest.raises(MediaUnavailable, match="cursor_snapshot_changed"):
            service.read_document(message, cursor=first["next_cursor"], limit=1)
        with store.conn:
            store.conn.execute("UPDATE media SET attachment=? WHERE media_ref=?", (json.dumps(record["attachment"]), media))
        monkeypatch.setattr(documents, "EXTRACTOR_VERSION", 999)
        with pytest.raises(MediaUnavailable, match="cursor_snapshot_changed"):
            service.read_document(message, cursor=first["next_cursor"], limit=1)


def test_cursor_rejects_new_bytes_and_other_files(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, message, media, _ = seed(store, b"one\ntwo\n")
        _, other, _, _ = seed(store, b"other\nfile\n", number=2)
        service = DocumentService(store)
        token = service.read_document(message, limit=1)["next_cursor"]
        with pytest.raises(MediaUnavailable, match="cursor_scope_changed"):
            service.read_document(other, cursor=token)
        put_blob(store, media, b"updated\nbytes\n")
        with pytest.raises(MediaUnavailable, match="cursor_snapshot_changed"):
            service.read_document(message, cursor=token)
        with pytest.raises(MediaUnavailable, match="cached_file_md5_mismatch"):
            service.get_attachment(media)
        with pytest.raises(MediaUnavailable, match="cached_file_md5_mismatch"):
            service.read_document(media)


def test_extensionless_filename_uses_explicit_format_hint_and_binds_it_to_cursor(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, message, media, _ = seed(store, b"one,two\nthree,four\n", "table")
        record = store.get_media_records(media_ref=media)[0]
        attachment = {**record["attachment"], "fileext": "csv"}
        with store.conn:
            store.conn.execute("UPDATE media SET attachment=? WHERE media_ref=?", (json.dumps(attachment), media))
        service = DocumentService(store)
        first = service.read_document(message, limit=1)
        assert first["format"] == "csv" and first["attachment"]["filename"] == "table"
        assert first["units"][0]["cells"][1]["text"] == "two"
        with store.conn:
            store.conn.execute("UPDATE media SET attachment=? WHERE media_ref=?", (json.dumps({**attachment, "fileext": "txt"}), media))
        with pytest.raises(MediaUnavailable, match="cursor_snapshot_changed"):
            service.read_document(message, cursor=first["next_cursor"])
        assert service.read_document(message)["format"] == "text"
        # Hints never replace an existing suffix or supply a path-like extension.
        assert DocumentService._format_hint({"filename": "table.txt", "fileext": "csv"}) == "table.txt"
        assert DocumentService._format_hint({"filename": "table", "fileext": "../txt"}) == "table"


def test_retried_page_timeout_is_reported_without_advancing_cursor(tmp_path):
    def timeout(*args):
        return {"format": "unknown", "status": "timeout", "reason": "document_extraction_timeout",
                "units": [], "warnings": []}
    with ArchiveStore(tmp_path) as store:
        _, message, _, _ = seed(store, b"one\ntwo\n")
        cursor = DocumentService(store).read_document(message, limit=1)["next_cursor"]
        failed = DocumentService(store, extractor=timeout).read_document(message, cursor=cursor)
        assert failed["status"] == "timeout"
        assert failed["reason"] == "document_extraction_timeout"
        assert failed["units"] == [] and failed["next_cursor"] is None
        assert failed["retry_cursor"] == cursor and failed["retryable"] is True
        recovered = DocumentService(store).read_document(message, cursor=cursor)
        assert recovered["units"][0]["text"] == "two\n"


def test_large_cell_field_values_preserved_with_coordinates_and_paging(tmp_path):
    long = "中文很长" * 6000
    def extract(*args):
        return {"format": "xlsx", "status": "ready", "warnings": [], "metadata": {},
                "units": [{"ordinal": 1, "kind": "sheet_row", "locator": {"sheet": 1, "sheet_name": "One", "row": 5},
                           "text": "A5: " + long, "cells": [{"coordinate": "A5", "column": 1,
                              "type": "str", "value": long, "formula": "=REPT()", "cached_value": long}]}]}
    with ArchiveStore(tmp_path) as store:
        _, message, _, _ = seed(store)
        units, pages = read_all(DocumentService(store, extractor=extract), message)
        assert len(pages) > 1
        assert all(u["layout"] == "cell_fields" and u["cell"]["coordinate"] == "A5" for u in units)
        assert all(u["offset_scope"] == "cell_field" and u["length_chars"] == len(u["text"]) for u in units)
        assert "".join(u["text"] for u in units if u["field"] == "value") == long
        assert "".join(u["text"] for u in units if u["field"] == "cached_value") == long
        assert "".join(u["text"] for u in units if u["field"] == "formula") == "=REPT()"


def test_large_document_metadata_is_explicitly_summarized_without_body_loss(tmp_path):
    def extract(*args):
        return {"format": "xlsx", "status": "ready", "warnings": [],
                "metadata": {"scope": "worksheet_cells", "sheets": [{"name": "x" * 1024}] * 20},
                "units": [{"ordinal": 1, "text": "body", "locator": {"sheet": 1}}]}
    with ArchiveStore(tmp_path) as store:
        _, message, _, _ = seed(store)
        page = DocumentService(store, extractor=extract).read_document(message)
        assert page["units"][0]["text"] == "body" and page["status"] == "ready"
        assert page["metadata_status"] == "partial"
        assert page["document_metadata"]["field_counts"]["sheets"] == 20
        assert "sheets" in page["document_metadata"]["omitted_fields"]


def test_document_queries_do_not_write_sqlite_or_modify_archive_bytes(tmp_path):
    with ArchiveStore(tmp_path) as writer:
        _, message, media, blob = seed(writer)
    database = tmp_path / "archive/archive.sqlite"
    before_database, before_blob = database.read_bytes(), blob.read_bytes()
    with ArchiveStore(tmp_path, readonly=True) as store:
        assert store.conn.execute("PRAGMA query_only").fetchone()[0] == 1
        service = DocumentService(store)
        service.get_attachment(media)
        service.get_attachment(message, include_content=True)
        service.read_document(message)
        assert store.conn.total_changes == 0
    assert database.read_bytes() == before_database and blob.read_bytes() == before_blob


def test_forwarded_file_remains_readable_after_parent_ack_merge(tmp_path):
    data = b"one\ntwo\n"
    attachment = file_parsed(data)["attachment"]
    parsed = {"msg_type": 49, "sub_type": 19, "parse_status": "parsed", "text": "Forward",
              "forwarded_status": "parsed", "forwarded": [
                  {"ordinal": 1, "datatype": 8, "text": "sample.txt", "attachment": attachment, "children": []}]}
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        old = commit(store, source, [raw(server_id="", local_type=49, parsed=parsed)])["message_refs"][0]
        old_child = store.read_forwarded_messages(old)["children"][0]["forward_ref"]
        record = store.conn.execute("SELECT media_ref FROM media WHERE forward_ref=?", (old_child,)).fetchone()
        put_blob(store, record[0], data)
        service = DocumentService(store)
        first = service.read_document(old_child, limit=1)
        other = setup_source(store, "db_storage/message/message_1.db")
        canonical = commit(store, other, [raw(server_id="server-ack", local_type=49, parsed=parsed)])["message_refs"][0]
        commit(store, source, [raw(server_id="server-ack", local_type=49, parsed=parsed)])
        assert store.get_status()["messages"] == 1
        assert service.get_attachment(old_child, include_content=True)["data"] == data
        second = service.read_document(old_child, cursor=first["next_cursor"], limit=1)
        assert second["attachment"]["message_ref"] == canonical
        assert second["units"][0]["text"] == "two\n"
        with pytest.raises(MediaUnavailable, match="file_attachment_not_found"):
            service.get_attachment(canonical)
