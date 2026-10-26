"""Inert note HTML and card-component attachment queries use synthetic bytes."""
import hashlib

import pytest

from chatlog_keeper import document_extractors
from chatlog_keeper.archive import ArchiveStore
from chatlog_keeper.document_extractors import extract_document
from chatlog_keeper.documents import DocumentService
from test_archive import commit, raw, setup_source
from test_documents import put_blob, read_all
from test_p2_archive import note


def test_html_reads_ordered_body_and_tables_without_executing_or_fetching():
    data = b'''<!doctype html><html><head><title>ignored title</title><style>ignored-css</style></head>
    <body><h1>Body &amp; heading</h1><p>before<br>second line</p>
    <script>ignored-script;fetch("https://example.invalid/send")</script>
    <img src="https://example.invalid/private?token=secret"><link href="file:///etc/passwd">
    <table><caption>Table title</caption><tr><th rowspan="2">Name</th><th>Value</th></tr>
    <tr><td colspan="2">Cell <b>content</b></td></tr></table>
    <pre>  preserve indent\n next line\n</pre><p>after</p></body></html>'''
    result = extract_document(data, "note.htm")
    assert result["status"] == "ready" and result["format"] == "html"
    units = result["units"]
    assert [u["kind"] for u in units] == ["html_block", "html_block", "html_table_caption",
        "html_table_row", "html_table_row", "html_pre", "html_block"]
    assert units[0]["text"] == "Body & heading" and units[-1]["text"] == "after"
    assert units[1]["text"] == "before\nsecond line"
    assert units[2]["text"] == "Table title"
    assert units[4]["cells"][0]["column"] == 2
    assert units[4]["cells"][0]["column_span"] == 2
    assert units[5]["text"] == "  preserve indent\n next line\n"
    assert result["metadata"]["scripts_executed"] is False
    assert result["metadata"]["external_resources_loaded"] is False
    assert "html_external_resources_not_loaded" in result["warnings"]
    assert "html_scripts_and_styles_ignored" in result["warnings"]
    assert all(value not in "".join(u["text"] for u in units)
               for value in ["ignored", "fetch", "passwd", "token=secret"])


@pytest.mark.parametrize("suffix", ["HTML", "htm"])
def test_html_unicode_fragments_and_empty_external_only_body(suffix):
    result = extract_document("<div>本地笔记<p>第二段 &lt;内容&gt;</p></div>".encode(), "note." + suffix)
    assert [u["text"] for u in result["units"]] == ["本地笔记", "第二段 <内容>"]
    empty = extract_document(b'<img src="https://example.invalid/a.png"><script>hidden()</script>', "note.htm")
    assert empty["status"] == "empty" and empty["units"] == []


def test_html_embedded_documents_are_partial_and_nested_table_text_is_retained():
    data = b'<p>body</p><iframe src="https://example.invalid">hidden</iframe><table><tr><td>A<table><tr><td>B</td></tr></table>C</td></tr></table>'
    result = extract_document(data, "note.html")
    assert result["status"] == "partial"
    assert "html_nested_tables_flattened" in result["warnings"]
    assert "B" in result["units"][1]["text"]
    assert "hidden" not in str(result["units"])


@pytest.mark.parametrize("data,reason", [
    (b"<div>" * 130, "html_depth_limit"),
    (b"<table><tr><td colspan='0'>bad</td></tr></table>", "invalid_html_table_span"),
    (b"<table><tr><td rowspan='NaN'>bad</td></tr></table>", "invalid_html_table_span"),
    (b"<table><tr><td colspan='16384'>a</td><td>b</td></tr></table>", "html_table_column_limit"),
])
def test_html_structure_limits_and_invalid_spans_are_explicit(data, reason):
    result = extract_document(data, "note.htm")
    assert result["status"] in {"parse_error", "limit_exceeded"}
    assert result["reason"] == reason and result["units"] == []


def test_html_node_and_cell_limits_are_enforced(monkeypatch):
    monkeypatch.setattr(document_extractors, "MAX_HTML_NODES", 3)
    result = extract_document(b"<p>1</p><p>2</p>", "note.htm")
    assert result["status"] == "limit_exceeded" and result["reason"] == "html_node_limit"
    monkeypatch.setattr(document_extractors, "MAX_HTML_NODES", 100)
    monkeypatch.setattr(document_extractors, "MAX_CELLS", 1)
    result = extract_document(b"<table><tr><td>1</td><td>2</td></tr></table>", "note.htm")
    assert result["status"] == "limit_exceeded" and result["reason"] == "document_cell_limit"


def component_file(data, name="note.htm"):
    return {"datatype": 8, "text": "[文件] " + name, "children": [], "attachment": {
        "kind": "file", "filename": name, "fileext": "htm", "md5": hashlib.md5(data).hexdigest(),
        "total_bytes": len(data)}}


def test_card_component_attachment_and_full_html_are_readable_offline(tmp_path):
    body = "完整正文" * 10000
    data = ("<html><body><p>" + body + "</p><table><tr><td>表格</td></tr></table></body></html>").encode()
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        ref = commit(store, source, [raw(parsed=note(children=[component_file(data)]))])["message_refs"][0]
        item = store.read_card(ref)["components"][0]["item_ref"]
        media = store.component_media_records(item)[0]
        put_blob(store, media["media_ref"], data)
    with ArchiveStore(tmp_path, readonly=True) as store:
        service = DocumentService(store)
        assert service.get_attachment(item, include_content=True)["data"] == data
        units, pages = read_all(service, item, 2)
        assert "".join(u["text"] for u in units) == body + "表格"
        assert all(p["format"] == "html" and p["extraction_complete"] for p in pages)
        assert pages[0]["attachment"]["item_ref"] == item


def test_changed_component_does_not_resolve_a_previous_ready_file(tmp_path):
    old, new = b"<p>old</p>", b"<p>new body</p>"
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        ref = commit(store, source, [raw(parsed=note(children=[component_file(old)]))])["message_refs"][0]
        item = store.read_card(ref)["components"][0]["item_ref"]
        old_media = store.component_media_records(item)[0]
        put_blob(store, old_media["media_ref"], old)
        commit(store, source, [raw(parsed=note(children=[component_file(new)]))])
        result = DocumentService(store).get_attachment(item)
        assert result["metadata"]["status"] != "ready" and result["metadata"].get("sha256") is None
        assert DocumentService(store).get_attachment(item, include_content=True)["data"] is None
