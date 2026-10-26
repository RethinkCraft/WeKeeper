"""Synthetic document bodies only; no source discovery, network or UI calls."""
import codecs
import io
import zipfile

import pytest

from chatlog_keeper import document_extractors as docs

W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
S = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
REL = "http://schemas.openxmlformats.org/package/2006/relationships"


def package(parts, compression=zipfile.ZIP_DEFLATED):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=compression) as archive:
        for name, body in parts.items():
            archive.writestr(name, body)
    return out.getvalue()


def docx(body, **parts):
    return package({"word/document.xml": f'<w:document xmlns:w="{W}"><w:body>{body}</w:body></w:document>', **parts})


def paragraph(text):
    return f'<w:p><w:r><w:t>{text}</w:t></w:r></w:p>'


def xlsx(sheets, extra=None):
    workbook, relationships, parts = [], [], dict(extra or {})
    for index, (name, body) in enumerate(sheets, 1):
        workbook.append(f'<sheet name="{name}" sheetId="{index}" r:id="rId{index}"/>')
        relationships.append(f'<Relationship Id="rId{index}" Target="worksheets/sheet{index}.xml" Type="{R}/worksheet"/>')
        parts[f"xl/worksheets/sheet{index}.xml"] = f'<worksheet xmlns="{S}">{body}</worksheet>'
    parts["xl/workbook.xml"] = f'<workbook xmlns="{S}" xmlns:r="{R}"><sheets>{"".join(workbook)}</sheets></workbook>'
    parts["xl/_rels/workbook.xml.rels"] = f'<Relationships xmlns="{REL}">{"".join(relationships)}</Relationships>'
    return package(parts)


@pytest.mark.parametrize("encoding,bom", [("utf-8", b""), ("utf-8", codecs.BOM_UTF8), ("utf-16-le", codecs.BOM_UTF16_LE), ("utf-16-be", codecs.BOM_UTF16_BE), ("gb18030", b"")])
def test_text_strict_encoding_and_complete_line_endings(encoding, bom):
    text = "第一行\r\nsecond\n尾行"
    result = docs.extract_document(bom + text.encode(encoding), "../not-opened.txt")
    assert result["status"] == "ready"
    assert "".join(unit["text"] for unit in result["units"]) == text
    assert [unit["locator"]["line"] for unit in result["units"]] == [1, 2, 3]
    if encoding == "gb18030":
        assert result["metadata"]["encoding"] == "gb18030"
        assert "legacy_text_encoding_gb18030" in result["warnings"]


def test_csv_preserves_multiline_quoted_cells_and_coordinates():
    result = docs.extract_document('姓名,说明,价格\r\n甲,"两行\n正文",3\r\n'.encode(), "sample.csv")
    assert result["status"] == "ready"
    assert result["units"][1]["locator"] == {"row": 2}
    assert result["units"][1]["cells"] == [{"column": 1, "text": "甲"}, {"column": 2, "text": "两行\n正文"}, {"column": 3, "text": "3"}]
    malformed = docs.extract_document(b'one,"unterminated', "bad.csv")
    assert malformed["status"] == "parse_error" and malformed["units"] == []


def test_docx_body_order_tables_spans_notes_and_revision_view():
    body = (paragraph("before") + '<w:tbl><w:tr><w:tc><w:tcPr><w:gridSpan w:val="2"/></w:tcPr>' + paragraph("cell one") +
            '</w:tc><w:tc><w:tcPr><w:vMerge/></w:tcPr>' + paragraph("cell two") + '</w:tc></w:tr></w:tbl>' +
            '<w:sdt><w:sdtContent>' + paragraph("after") + '</w:sdtContent></w:sdt>' +
            '<w:p><w:r><w:t>visible</w:t></w:r><w:del><w:r><w:delText>deleted</w:delText></w:r></w:del>'
            '<w:r><w:tab/><w:t>end</w:t><w:br/><w:t>new line</w:t></w:r></w:p>')
    notes = f'<w:footnotes xmlns:w="{W}"><w:footnote w:type="separator" w:id="-1"/><w:footnote w:id="2">{paragraph("note text")}</w:footnote></w:footnotes>'
    result = docs.extract_document(docx(body, **{"word/footnotes.xml": notes}), "body.docx")
    assert result["status"] == "ready"
    assert [unit["kind"] for unit in result["units"]] == ["paragraph", "table_row", "paragraph", "paragraph", "footnote"]
    assert [unit["text"] for unit in result["units"]] == ["before", "cell one\tcell two", "after", "visible\tend\nnew line", "note text"]
    assert result["units"][1]["cells"][0]["column_span"] == 2
    assert result["units"][1]["cells"][1]["column"] == 3
    assert result["units"][1]["cells"][1]["row_merge"] == "continue"
    assert "tracked_changes_current_view" in result["warnings"]


def test_docx_marks_unread_embedded_body_and_does_not_follow_links():
    data = docx(paragraph("safe body") + '<w:altChunk xmlns:r="' + R + '" r:id="external"/>', **{
        "word/_rels/document.xml.rels": f'<Relationships xmlns="{REL}"><Relationship Id="external" Target="https://invalid.example/doc" TargetMode="External"/></Relationships>'})
    result = docs.extract_document(data, "body.docx")
    assert result["status"] == "partial"
    assert result["units"][0]["text"] == "safe body"
    assert result["reason"] == "document_content_partially_supported"


def test_xlsx_multiple_sheets_shared_strings_formulas_missing_cache_and_styles():
    sheet = ('<sheetData><row r="2"><c r="A2" t="s"><v>0</v></c><c r="C2" s="0"><v>3.5</v></c>'
             '<c r="D2"><f>SUM(C2,2)</f><v>5.5</v></c><c r="E2"><f>WEBSERVICE("https://invalid.example")</f></c>'
             '<c r="F2" t="b"><v>1</v></c></row></sheetData><mergeCells><mergeCell ref="A2:B2"/></mergeCells>')
    second = '<sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>二表</t></is></c></row></sheetData>'
    strings = f'<sst xmlns="{S}"><si><r><t>Price</t></r><rPh><t>phonetic</t></rPh></si></sst>'
    styles = f'<styleSheet xmlns="{S}"><numFmts><numFmt numFmtId="164" formatCode="0.00"/></numFmts><cellXfs><xf numFmtId="164"/></cellXfs></styleSheet>'
    result = docs.extract_document(xlsx([("First", sheet), ("第二张", second)], {"xl/sharedStrings.xml": strings, "xl/styles.xml": styles}), "sample.xlsx")
    assert result["status"] == "ready"
    assert result["units"][0]["locator"] == {"sheet": 1, "sheet_name": "First", "row": 2}
    cells = result["units"][0]["cells"]
    assert cells[0]["value"] == "Price"
    assert cells[1]["coordinate"] == "C2" and cells[1]["number_format"] == "0.00"
    assert cells[2]["formula"] == "SUM(C2,2)" and cells[2]["cached_value"] == "5.5"
    assert cells[3]["formula"].startswith("WEBSERVICE") and cells[3]["cached_value"] is None
    assert cells[3]["formula_status"] == "not_evaluated"
    assert cells[4]["value"] == "true"
    assert result["metadata"]["sheets"][0]["merged_ranges"] == ["A2:B2"]
    assert result["units"][1]["text"] == "A1: 二表"
    assert "formulas_not_evaluated" in result["warnings"]


def test_xlsx_invalid_coordinates_and_shared_string_are_explicit_failures():
    for bad in ['<c r="XFE1"><v>1</v></c>', '<c r="A2"><v>1</v></c>', '<c r="A1" t="s"><v>999</v></c>']:
        result = docs.extract_document(xlsx([("One", f'<sheetData><row r="1">{bad}</row></sheetData>')]), "bad.xlsx")
        assert result["status"] == "parse_error" and result["units"] == []


def test_sniffed_format_mismatch_never_decodes_archive_as_plaintext():
    result = docs.extract_document(docx(paragraph("real Word body")), "renamed.txt")
    assert result["format"] == "docx" and result["status"] == "ready"
    assert "filename_format_mismatch" in result["warnings"]
    assert docs.extract_document(b"not a pdf", "bad.pdf")["reason"] == "document_format_mismatch"
    assert docs.extract_document(b"\x00binary", "bad.txt")["reason"] == "binary_content_not_text"
    assert docs.extract_document(b"legacy", "old.doc")["reason"] == "unsupported_legacy_format"
    assert docs.extract_document(docs._OLE + b"encrypted OOXML", "secret.xlsx")["status"] == "unsupported"


def test_zip_unsafe_names_entities_and_unknown_container_are_not_extracted():
    unsafe = docs.extract_document(package({"../outside": "bad", "word/document.xml": "bad"}), "bad.docx")
    assert unsafe["reason"] == "unsafe_document_archive"
    entity = f'<!DOCTYPE doc [<!ENTITY secret "abc">]><w:document xmlns:w="{W}"><w:body>{paragraph("&secret;")}</w:body></w:document>'
    result = docs.extract_document(package({"word/document.xml": entity}), "bad.docx")
    assert result["reason"] == "document_xml_entities_not_allowed"
    assert docs.extract_document(package({"other.txt": "text"}), "data.zip")["status"] == "unsupported"


def test_every_limit_is_explicit_and_discards_partial_prefix(monkeypatch):
    monkeypatch.setattr(docs, "MAX_TEXT_CHARS", 5)
    result = docs.extract_document(b"one\ntwo\n", "long.txt")
    assert result["status"] == "limit_exceeded" and result["units"] == []
    monkeypatch.setattr(docs, "MAX_TEXT_CHARS", 100)
    monkeypatch.setattr(docs, "MAX_UNITS", 1)
    result = docs.extract_document(b"one\ntwo", "many.txt")
    assert result["reason"] == "document_unit_limit" and result["units"] == []
    monkeypatch.setattr(docs, "MAX_INPUT_BYTES", 3)
    assert docs.extract_document(b"1234", "big.txt")["reason"] == "document_input_limit"


def test_zip_expansion_and_cell_limits_are_bounded(monkeypatch):
    data = docx(paragraph("ordinary"))
    monkeypatch.setattr(docs, "MAX_EXPANDED_BYTES", 20)
    assert docs.extract_document(data, "big.docx")["reason"] == "document_archive_expansion_limit"
    monkeypatch.setattr(docs, "MAX_EXPANDED_BYTES", 1024 * 1024)
    monkeypatch.setattr(docs, "MAX_CELLS", 1)
    result = docs.extract_document(b"a,b", "cells.csv")
    assert result["reason"] == "document_cell_limit" and result["units"] == []


def pdf_bytes(text=None, encrypted=False):
    pypdf = pytest.importorskip("pypdf")
    from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject
    writer = pypdf.PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    if text:
        font = DictionaryObject({NameObject("/Type"): NameObject("/Font"), NameObject("/Subtype"): NameObject("/Type1"), NameObject("/BaseFont"): NameObject("/Helvetica")})
        page[NameObject("/Resources")] = DictionaryObject({NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})})
        content = DecodedStreamObject()
        content.set_data(f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode())
        page[NameObject("/Contents")] = writer._add_object(content)
    if encrypted:
        writer.encrypt("synthetic-password")
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def test_pdf_page_text_encryption_and_no_text_are_distinct():
    result = docs.extract_document(pdf_bytes("Synthetic PDF body"), "body.pdf")
    assert result["status"] == "ready"
    assert result["units"][0]["locator"] == {"page": 1}
    assert "Synthetic PDF body" in result["units"][0]["text"]
    encrypted = docs.extract_document(pdf_bytes("secret", encrypted=True), "secret.pdf")
    assert encrypted["status"] == "encrypted" and encrypted["units"] == []
    blank = docs.extract_document(pdf_bytes(), "blank.pdf")
    assert blank["status"] == "empty" and blank["metadata"]["pages_without_text"] == [1]
    assert blank["metadata"]["ocr_performed"] is False
    mismatch = docs.extract_document(pdf_bytes("Still PDF"), "renamed.txt")
    assert mismatch["format"] == "pdf" and "filename_format_mismatch" in mismatch["warnings"]


def test_corrupt_pdf_is_parse_error_without_leaking_payload():
    result = docs.extract_document(b"%PDF-1.7 private-secret broken document", "bad.pdf")
    assert result["status"] in {"parse_error", "dependency_unavailable"}
    assert "private-secret" not in str(result)
