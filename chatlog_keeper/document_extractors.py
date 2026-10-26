"""Bounded document-body extraction from bytes, without filesystem or network access.

Office files are read as inert ZIP/XML data. Macros, formulas, linked objects,
PDF actions and external relationships are never executed or followed. The
caller should additionally isolate this parser in a process with CPU/memory
limits; input size alone cannot bound third-party PDF parser memory usage.
"""
from __future__ import annotations

import codecs
import csv
import io
from html.parser import HTMLParser
import logging
import posixpath
import re
import stat
from pathlib import PurePosixPath
from xml.etree import ElementTree as ET
import zipfile

MAX_INPUT_BYTES = 32 * 1024 * 1024
MAX_EXPANDED_BYTES = 64 * 1024 * 1024
MAX_XML_BYTES = 16 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 5000
MAX_UNITS = 10000
MAX_TEXT_CHARS = 2_000_000
MAX_CELLS = 50000
MAX_HTML_NODES = 50000
MAX_HTML_DEPTH = 128

_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
_S = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
_REL = "{http://schemas.openxmlformats.org/package/2006/relationships}"
_TEXT_EXTENSIONS = {"txt", "md", "markdown", "log", "json", "yaml", "yml", "xml", "csv", "tsv"}
_CELL = re.compile(r"([A-Z]{1,3})([1-9][0-9]{0,6})\Z")
_OLE = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"


class ExtractionFailure(ValueError):
    def __init__(self, status, reason):
        self.status, self.reason = status, reason
        super().__init__(reason)


def _fail(status, reason):
    raise ExtractionFailure(status, reason)


class _Units:
    def __init__(self):
        self.items = []
        self.characters = 0
        self.cells = 0

    def add(self, kind, locator, text, **extra):
        if len(self.items) >= MAX_UNITS:
            _fail("limit_exceeded", "document_unit_limit")
        self.characters += len(text)
        self.cells += len(extra.get("cells", []))
        # Structured cells duplicate their printable values. Bound the actual
        # parser output too, so a caller's JSON framing remains predictable.
        if self.characters > MAX_TEXT_CHARS:
            _fail("limit_exceeded", "document_text_limit")
        if self.cells > MAX_CELLS:
            _fail("limit_exceeded", "document_cell_limit")
        self.items.append({"ordinal": len(self.items) + 1, "kind": kind,
                           "locator": locator, "text": text, **extra})


class _Package:
    def __init__(self, data):
        self.archive = zipfile.ZipFile(io.BytesIO(data))
        entries = self.archive.infolist()
        if len(entries) > MAX_ARCHIVE_ENTRIES:
            _fail("limit_exceeded", "document_archive_entry_limit")
        self.names, self.expanded = {}, 0
        total = 0
        for info in entries:
            name = info.filename
            parts = PurePosixPath(name).parts
            if (not name or name.startswith("/") or "\\" in name or "\0" in name
                    or ".." in parts or name in self.names
                    or stat.S_ISLNK(info.external_attr >> 16)):
                _fail("parse_error", "unsafe_document_archive")
            if info.flag_bits & 1:
                _fail("encrypted", "encrypted_document_archive")
            total += info.file_size
            if total > MAX_EXPANDED_BYTES:
                _fail("limit_exceeded", "document_archive_expansion_limit")
            if info.file_size > 1024 * 1024 and info.file_size > max(info.compress_size, 1) * 1000:
                _fail("limit_exceeded", "document_archive_compression_ratio_limit")
            self.names[name] = info

    def close(self):
        self.archive.close()

    def xml(self, name):
        info = self.names.get(name)
        if info is None:
            _fail("parse_error", "document_part_missing")
        if info.file_size > MAX_XML_BYTES:
            _fail("limit_exceeded", "document_xml_part_limit")
        with self.archive.open(info) as stream:
            data = stream.read(MAX_XML_BYTES + 1)
        self.expanded += len(data)
        if len(data) > MAX_XML_BYTES or self.expanded > MAX_EXPANDED_BYTES:
            _fail("limit_exceeded", "document_archive_expansion_limit")
        if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
            _fail("parse_error", "document_xml_entities_not_allowed")
        return ET.fromstring(data)


def _part(base, target):
    if not isinstance(target, str) or "\\" in target or "\0" in target:
        _fail("parse_error", "invalid_document_relationship")
    # An absolute OOXML part URI is package-rooted, not a filesystem path.
    joined = target.lstrip("/") if target.startswith("/") else posixpath.join(posixpath.dirname(base), target)
    normalized = posixpath.normpath(joined)
    if normalized in ("", ".", "..") or normalized.startswith("../") or ":" in normalized:
        _fail("parse_error", "invalid_document_relationship")
    return normalized


def _relationships(package, base):
    name = posixpath.join(posixpath.dirname(base), "_rels", posixpath.basename(base) + ".rels")
    if name not in package.names:
        return {}
    root = package.xml(name)
    return {item.get("Id"): item for item in root.findall(_REL + "Relationship") if item.get("Id")}


def _word_text(node):
    parts = []

    def visit(current):
        # Deletions belong to revision history, not the current document body.
        if current.tag == _W + "del":
            return
        if current.tag == _W + "t":
            parts.append(current.text or "")
        elif current.tag == _W + "tab":
            parts.append("\t")
        elif current.tag in {_W + "br", _W + "cr"}:
            parts.append("\n")
        else:
            for child in current:
                visit(child)

    visit(node)
    return "".join(parts)


def _word_paragraphs(node):
    """Visible paragraphs in a table cell or note, skipping deleted blocks."""
    if node.tag == _W + "del":
        return
    if node.tag == _W + "p":
        yield _word_text(node)
        return
    for child in node:
        yield from _word_paragraphs(child)


def _docx(package, units, warnings):
    root = package.xml("word/document.xml")
    body = root.find(_W + "body")
    if body is None:
        _fail("parse_error", "docx_body_missing")
    paragraph, table = 0, 0

    def blocks(container):
        nonlocal paragraph, table
        for block in container:
            if block.tag == _W + "p":
                paragraph += 1
                units.add("paragraph", {"paragraph": paragraph}, _word_text(block))
            elif block.tag == _W + "tbl":
                table += 1
                for row_index, row in enumerate(block.findall(_W + "tr"), 1):
                    cells, column = [], 1
                    for cell in row.findall(_W + "tc"):
                        cell_text = "\n".join(_word_paragraphs(cell))
                        span = cell.find(_W + "tcPr/" + _W + "gridSpan")
                        try:
                            width = int(span.get(_W + "val", "1")) if span is not None else 1
                        except ValueError:
                            _fail("parse_error", "invalid_docx_table_span")
                        if not 1 <= width <= 16384:
                            _fail("parse_error", "invalid_docx_table_span")
                        merge = cell.find(_W + "tcPr/" + _W + "vMerge")
                        cells.append({"column": column, "column_span": width, "text": cell_text,
                                      "row_merge": (merge.get(_W + "val") or "continue") if merge is not None else None})
                        column += width
                    units.add("table_row", {"table": table, "row": row_index},
                              "\t".join(cell["text"] for cell in cells), cells=cells)
            elif block.tag == _W + "altChunk":
                warnings.add("embedded_content_not_extracted")
            elif block.tag not in {_W + "sectPr", _W + "del"}:
                # Includes content controls and inserted body blocks, in order.
                blocks(block)

    blocks(body)
    if root.find(".//" + _W + "del") is not None or root.find(".//" + _W + "ins") is not None:
        warnings.add("tracked_changes_current_view")
    if root.find(".//" + _W + "drawing") is not None or root.find(".//" + _W + "pict") is not None:
        warnings.add("embedded_images_not_ocr")
    # Notes are part of readable document content, but are not inserted into
    # paragraph text at guessed locations. Their IDs retain a precise locator.
    for part, tag in (("word/footnotes.xml", "footnote"), ("word/endnotes.xml", "endnote")):
        if part not in package.names:
            continue
        for note in package.xml(part).findall(_W + tag):
            identifier = note.get(_W + "id", "")
            if note.get(_W + "type") in {"separator", "continuationSeparator"}:
                continue
            units.add(tag, {tag: identifier}, "\n".join(_word_paragraphs(note)))
    return {"scope": "main_document_body_and_notes", "paragraphs": paragraph, "tables": table}


def _cell_position(coordinate):
    match = _CELL.fullmatch(coordinate or "")
    if match is None or int(match.group(2)) > 1048576:
        _fail("parse_error", "invalid_spreadsheet_coordinate")
    column = 0
    for letter in match.group(1):
        column = column * 26 + ord(letter) - ord("A") + 1
    if column > 16384:
        _fail("parse_error", "invalid_spreadsheet_coordinate")
    return column, int(match.group(2))


def _sheet_text(node):
    # Phonetic guides do not duplicate the visible cell value.
    parts = []
    def visit(current):
        if current.tag == _S + "rPh":
            return
        if current.tag == _S + "t":
            parts.append(current.text or "")
        else:
            for child in current:
                visit(child)
    visit(node)
    return "".join(parts)


def _xlsx(package, units, warnings):
    workbook = package.xml("xl/workbook.xml")
    if workbook.tag != _S + "workbook":
        _fail("parse_error", "invalid_spreadsheet_workbook")
    relationships = _relationships(package, "xl/workbook.xml")
    shared = []
    if "xl/sharedStrings.xml" in package.names:
        shared_root = package.xml("xl/sharedStrings.xml")
        shared = [_sheet_text(item) for item in shared_root.findall(_S + "si")]
        if sum(map(len, shared)) > MAX_TEXT_CHARS:
            _fail("limit_exceeded", "document_text_limit")
    formats, styles = {}, []
    if "xl/styles.xml" in package.names:
        root = package.xml("xl/styles.xml")
        formats = {item.get("numFmtId"): item.get("formatCode", "") for item in root.findall(_S + "numFmts/" + _S + "numFmt")}
        styles = [item.get("numFmtId", "0") for item in root.findall(_S + "cellXfs/" + _S + "xf")]
    sheets = []
    for sheet_index, sheet in enumerate(workbook.findall(_S + "sheets/" + _S + "sheet"), 1):
        name = sheet.get("name") or ""
        relation = relationships.get(sheet.get(_R + "id"))
        summary = {"index": sheet_index, "name": name, "state": sheet.get("state", "visible"), "rows": 0, "merged_ranges": []}
        sheets.append(summary)
        if relation is None or relation.get("TargetMode") == "External":
            warnings.add("worksheet_relationship_unavailable")
            continue
        root = package.xml(_part("xl/workbook.xml", relation.get("Target")))
        if root.tag != _S + "worksheet":
            warnings.add("worksheet_type_not_supported")
            continue
        previous_row = 0
        for row in root.findall(_S + "sheetData/" + _S + "row"):
            try:
                row_index = int(row.get("r", str(previous_row + 1)))
            except ValueError:
                _fail("parse_error", "invalid_spreadsheet_coordinate")
            if not previous_row < row_index <= 1048576:
                _fail("parse_error", "invalid_spreadsheet_coordinate")
            previous_row, cells, seen = row_index, [], set()
            for cell in row.findall(_S + "c"):
                coordinate = cell.get("r")
                column, actual_row = _cell_position(coordinate)
                if actual_row != row_index or coordinate in seen:
                    _fail("parse_error", "invalid_spreadsheet_coordinate")
                seen.add(coordinate)
                kind, value = cell.get("t", "n"), cell.findtext(_S + "v")
                if kind == "s":
                    try:
                        index = int(value)
                        if index < 0:
                            raise IndexError()
                        value = shared[index]
                    except (ValueError, TypeError, IndexError):
                        _fail("parse_error", "invalid_spreadsheet_shared_string")
                elif kind == "inlineStr":
                    inline = cell.find(_S + "is")
                    value = _sheet_text(inline) if inline is not None else ""
                elif kind == "b":
                    if value not in {"0", "1", None}:
                        _fail("parse_error", "invalid_spreadsheet_boolean")
                    value = {"0": "false", "1": "true", None: ""}[value]
                item = {"coordinate": coordinate, "column": column, "type": kind, "value": value or ""}
                if cell.get("s") is not None:
                    try:
                        style = int(cell.get("s"))
                        if style < 0:
                            raise IndexError()
                        fmt = styles[style]
                        item.update(number_format_id=fmt, number_format=formats.get(fmt))
                    except (ValueError, IndexError):
                        warnings.add("spreadsheet_style_unavailable")
                formula = cell.find(_S + "f")
                if formula is not None:
                    item.update(formula=formula.text or "", cached_value=value,
                                formula_status="not_evaluated", formula_type=formula.get("t", "normal"))
                    if formula.get("si") is not None:
                        item["shared_formula_index"] = formula.get("si")
                    if not formula.text:
                        warnings.add("shared_or_array_formula_not_expanded")
                    warnings.add("formulas_not_evaluated")
                cells.append(item)
            def printable(item):
                if "formula" in item:
                    formula = "=" + item["formula"] if item["formula"] else "[shared/array formula]"
                    cached = item.get("cached_value")
                    value = formula + (" [cached: " + cached + "]" if cached is not None else " [cached value unavailable]")
                else:
                    value = item["value"]
                return item["coordinate"] + ": " + value
            units.add("sheet_row", {"sheet": sheet_index, "sheet_name": name, "row": row_index},
                      "\t".join(printable(item) for item in cells), cells=cells)
            summary["rows"] += 1
        summary["merged_ranges"] = [item.get("ref") for item in root.findall(_S + "mergeCells/" + _S + "mergeCell") if item.get("ref")]
    if any(name.startswith("xl/externalLinks/") for name in package.names):
        warnings.add("external_links_not_followed")
    if any(name.startswith("xl/media/") for name in package.names):
        warnings.add("embedded_images_not_ocr")
    if any(name.lower().endswith("vbaproject.bin") for name in package.names):
        warnings.add("macros_not_executed")
    properties = workbook.find(_S + "workbookPr")
    return {"scope": "worksheet_cells", "sheets": sheets,
            "date_system": "1904" if properties is not None and properties.get("date1904") in {"1", "true"} else "1900",
            "values": "stored values; number formats are metadata, not rendered display values"}


def _decode_text(data, warnings):
    if data.startswith(codecs.BOM_UTF8):
        encoding = "utf-8-sig"
    elif data.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        encoding = "utf-16"
    else:
        encoding = "utf-8"
    try:
        text = data.decode(encoding, errors="strict")
    except UnicodeError:
        if encoding != "utf-8":
            _fail("parse_error", "invalid_text_encoding")
        try:
            text = data.decode("gb18030", errors="strict")
            encoding = "gb18030"
            warnings.add("legacy_text_encoding_gb18030")
        except UnicodeError:
            _fail("unsupported", "text_encoding_unavailable")
    if any(ord(character) < 32 and character not in "\t\n\r\f" for character in text):
        _fail("unsupported", "binary_content_not_text")
    if len(text) > MAX_TEXT_CHARS:
        _fail("limit_exceeded", "document_text_limit")
    return text, encoding


def _plain(data, extension, units, warnings):
    text, encoding = _decode_text(data, warnings)
    if extension in {"csv", "tsv"}:
        delimiter = "\t" if extension == "tsv" else ","
        previous_limit = csv.field_size_limit()
        try:
            csv.field_size_limit(MAX_TEXT_CHARS)
            rows = csv.reader(io.StringIO(text, newline=""), delimiter=delimiter, strict=True)
            for number, cells in enumerate(rows, 1):
                units.add("csv_row", {"row": number}, "\t".join(cells),
                          cells=[{"column": column, "text": value} for column, value in enumerate(cells, 1)])
        finally:
            csv.field_size_limit(previous_limit)
        return {"encoding": encoding, "delimiter": delimiter, "scope": "table_rows"}
    # splitlines retains line endings, including a final newline; reconstruction
    # therefore reproduces the decoded original without discarded whitespace.
    for number, line in enumerate(text.splitlines(keepends=True), 1):
        units.add("text_line", {"line": number}, line)
    return {"encoding": encoding, "scope": "text_lines"}


def _html(data, units, warnings):
    """Read an inert local HTML body; attributes never initiate requests."""
    text, encoding = _decode_text(data, warnings)
    skipped = {"script", "style", "head", "template", "iframe", "object", "embed", "svg", "canvas"}
    external = {"img", "link", "iframe", "object", "embed", "source", "video", "audio"}
    void = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}
    blocks = {"address", "article", "aside", "blockquote", "div", "dl", "dt", "dd", "figcaption", "figure", "footer",
              "h1", "h2", "h3", "h4", "h5", "h6", "header", "li", "main", "nav", "ol", "p", "section", "ul"}

    class Parser(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.root = {"tag": "document", "children": []}
            self.stack = [self.root]
            self.nodes = 0

        def handle_starttag(self, tag, attrs):
            self.nodes += 1
            if self.nodes > MAX_HTML_NODES:
                _fail("limit_exceeded", "html_node_limit")
            if len(self.stack) >= MAX_HTML_DEPTH:
                _fail("limit_exceeded", "html_depth_limit")
            attributes = {key: value for key, value in attrs if key in {"colspan", "rowspan"}}
            node = {"tag": tag, "children": [], "attrs": attributes}
            self.stack[-1]["children"].append(node)
            if tag not in void:
                self.stack.append(node)

        def handle_startendtag(self, tag, attrs):
            self.handle_starttag(tag, attrs)
            if tag not in void:
                self.handle_endtag(tag)

        def handle_endtag(self, tag):
            for index in range(len(self.stack) - 1, 0, -1):
                if self.stack[index]["tag"] == tag:
                    del self.stack[index:]
                    return

        def handle_data(self, value):
            if value and not any(node["tag"] in skipped for node in self.stack):
                self.nodes += 1
                if self.nodes > MAX_HTML_NODES:
                    _fail("limit_exceeded", "html_node_limit")
                self.stack[-1]["children"].append(value)

    parser = Parser()
    parser.feed(text)
    parser.close()
    count = {"block": 0, "table": 0}
    buffer = []

    def skip(node):
        tag = node["tag"]
        if tag in external:
            warnings.add("html_external_resources_not_loaded")
        if tag in {"script", "style"}:
            warnings.add("html_scripts_and_styles_ignored")
        if tag in {"iframe", "object", "embed", "svg", "canvas"}:
            warnings.add("html_embedded_content_not_extracted")
        return tag in skipped or tag in {"img", "input", "source", "video", "audio"}

    def flush(kind="html_block"):
        value = "".join(buffer)
        if kind != "html_pre":
            value = value.strip()
        buffer.clear()
        if value.strip():
            count["block"] += 1
            units.add(kind, {"block": count["block"]}, value)

    def cell_text(node):
        values = []
        def visit(current):
            if isinstance(current, str):
                values.append(re.sub(r"\s+", " ", current))
                return
            if skip(current):
                return
            if current["tag"] == "table":
                warnings.add("html_nested_tables_flattened")
            if current["tag"] in blocks | {"br", "tr"}:
                values.append("\n")
            for child in current["children"]:
                visit(child)
            if current["tag"] in blocks | {"tr"}:
                values.append("\n")
        visit(node)
        return "".join(values).strip()

    def table_rows(node):
        for child in node["children"]:
            if isinstance(child, str):
                continue
            if child["tag"] == "tr":
                yield child
            elif child["tag"] != "table":
                yield from table_rows(child)

    def walk(node, pre=False):
        if isinstance(node, str):
            buffer.append(node if pre else re.sub(r"\s+", " ", node))
            return
        if skip(node):
            return
        tag = node["tag"]
        if tag == "table":
            flush()
            count["table"] += 1
            number = count["table"]
            for child in node["children"]:
                if isinstance(child, dict) and child["tag"] == "caption":
                    value = cell_text(child)
                    if value:
                        units.add("html_table_caption", {"table": number}, value)
            occupied = {}
            for row_index, row in enumerate(table_rows(node), 1):
                cells, column = [], 1
                for cell in row["children"]:
                    if isinstance(cell, str) or cell["tag"] not in {"td", "th"}:
                        continue
                    attrs = cell.get("attrs", {})
                    try:
                        width, height = int(attrs.get("colspan") or 1), int(attrs.get("rowspan") or 1)
                    except ValueError:
                        _fail("parse_error", "invalid_html_table_span")
                    if not 1 <= width <= 16384 or not 1 <= height <= 10000:
                        _fail("parse_error", "invalid_html_table_span")
                    while True:
                        if column + width - 1 > 16384:
                            _fail("limit_exceeded", "html_table_column_limit")
                        collision = next((c for c in range(column, column + width)
                                          if occupied.get(c, 0) >= row_index), None)
                        if collision is None:
                            break
                        column = collision + 1
                    if units.cells + len(cells) >= MAX_CELLS:
                        _fail("limit_exceeded", "document_cell_limit")
                    cells.append({"column": column, "column_span": width, "row_span": height,
                                  "kind": "header" if cell["tag"] == "th" else "data", "text": cell_text(cell)})
                    if height > 1:
                        occupied.update((c, row_index + height - 1) for c in range(column, column + width))
                    column += width
                if cells:
                    units.add("html_table_row", {"table": number, "row": row_index},
                              "\t".join(cell["text"] for cell in cells), cells=cells)
            return
        if tag == "br":
            buffer.append("\n")
            return
        if tag == "hr":
            flush()
            return
        boundary = tag in blocks or tag == "pre"
        if boundary:
            flush()
        for child in node["children"]:
            walk(child, pre or tag == "pre")
        if boundary:
            flush("html_pre" if tag == "pre" else "html_block")

    walk(parser.root)
    flush()
    return {"encoding": encoding, "scope": "inert_html_body", "tables": count["table"],
            "blocks": count["block"], "scripts_executed": False, "external_resources_loaded": False}


def _pdf(data, units, warnings):
    try:
        from pypdf import PdfReader
    except ImportError:
        _fail("dependency_unavailable", "pdf_parser_not_installed")
    # The process boundary discards stderr because third-party diagnostics may
    # echo input strings. Also silence the library's top-level logger here.
    logger = logging.getLogger("pypdf")
    old_disabled, logger.disabled = logger.disabled, True
    try:
        reader = PdfReader(io.BytesIO(data), strict=False)
        if reader.is_encrypted:
            _fail("encrypted", "document_password_required")
        count = len(reader.pages)
        if count > MAX_UNITS:
            _fail("limit_exceeded", "document_unit_limit")
        failed, no_text, expanded = 0, [], 0
        for number, page in enumerate(reader.pages, 1):
            try:
                contents = page.get_contents()
                if contents is not None:
                    page_bytes = contents.get_data()
                    expanded += len(page_bytes)
                    if len(page_bytes) > MAX_XML_BYTES or expanded > MAX_EXPANDED_BYTES:
                        _fail("limit_exceeded", "pdf_content_stream_limit")
                text = page.extract_text() or ""
                if not text.strip():
                    no_text.append(number)
                units.add("pdf_page", {"page": number}, text, status="ready" if text.strip() else "no_text")
            except ExtractionFailure:
                raise
            except Exception:
                failed += 1
                units.add("pdf_page", {"page": number}, "", status="parse_error", reason="pdf_page_extraction_failed")
        if failed:
            warnings.add("pdf_page_extraction_failed")
        if no_text:
            warnings.add("pages_without_text_ocr_not_performed")
        warnings.add("pdf_text_reading_order_is_approximate")
        return {"scope": "page_text", "page_count": count, "pages_without_text": no_text,
                "failed_pages": failed, "ocr_performed": False}
    finally:
        logger.disabled = old_disabled


def extract_document(data: bytes, filename: str) -> dict:
    """Return complete bounded text units, or a fixed explicit failure status.

    File extensions are hints, not permission to treat PDF/ZIP/binary data as
    plain text. Passwords and legacy binary Office formats are not supported.
    Failures do not return a partial prefix that could be mistaken for a complete
    document. Page-level PDF failures are individually located and reported.
    """
    units, warnings = _Units(), set()
    result = {"format": "unknown", "status": "parse_error", "reason": None,
              "units": [], "warnings": [], "metadata": {}}
    package = None
    try:
        if not isinstance(data, bytes) or not isinstance(filename, str):
            _fail("parse_error", "invalid_document_input")
        if len(data) > MAX_INPUT_BYTES:
            _fail("limit_exceeded", "document_input_limit")
        extension = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if data.startswith(b"%PDF-"):
            result["format"] = "pdf"
            if extension != "pdf":
                warnings.add("filename_format_mismatch")
            metadata = _pdf(data, units, warnings)
        elif data.startswith((b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08")):
            package = _Package(data)
            word, excel = "word/document.xml" in package.names, "xl/workbook.xml" in package.names
            if word and excel:
                _fail("parse_error", "ambiguous_office_document")
            if word:
                result["format"] = "docx"
                metadata = _docx(package, units, warnings)
            elif excel:
                result["format"] = "xlsx"
                metadata = _xlsx(package, units, warnings)
            else:
                _fail("unsupported", "document_archive_format_not_supported")
            if extension != result["format"]:
                warnings.add("filename_format_mismatch")
            if any(name.lower().endswith("vbaproject.bin") for name in package.names):
                warnings.add("macros_not_executed")
        elif data.startswith(_OLE):
            result["format"] = extension or "ole"
            _fail("unsupported", "unsupported_legacy_format" if extension in {"doc", "xls", "ppt"} else "office_ole_container_not_supported")
        elif extension in {"doc", "xls", "ppt"}:
            result["format"] = extension
            _fail("unsupported", "unsupported_legacy_format")
        elif extension in {"pdf", "docx", "xlsx"}:
            result["format"] = extension
            _fail("parse_error", "document_format_mismatch")
        elif extension in {"html", "htm"}:
            result["format"] = "html"
            metadata = _html(data, units, warnings)
        elif extension in _TEXT_EXTENSIONS:
            result["format"] = extension if extension in {"csv", "tsv", "json"} else "text"
            metadata = _plain(data, extension, units, warnings)
        else:
            _fail("unsupported", "document_format_not_supported")
        result.update(metadata=metadata, units=units.items)
        if metadata.get("failed_pages"):
            result["status"] = "partial" if metadata["failed_pages"] < metadata["page_count"] else "parse_error"
            result["reason"] = "pdf_page_extraction_failed"
        elif warnings & {"embedded_content_not_extracted", "worksheet_relationship_unavailable", "worksheet_type_not_supported", "html_embedded_content_not_extracted"}:
            result.update(status="partial", reason="document_content_partially_supported")
        elif any(unit["text"].strip() for unit in units.items):
            result["status"] = "ready"
        else:
            result.update(status="empty", reason="no_extractable_text")
        result["unit_count"] = len(units.items)
        result["text_characters"] = units.characters
    except ExtractionFailure as exc:
        result.update(status=exc.status, reason=exc.reason, units=[], unit_count=0, text_characters=0)
    except MemoryError:
        result.update(status="limit_exceeded", reason="document_memory_limit", units=[], unit_count=0, text_characters=0)
    except RecursionError:
        result.update(status="limit_exceeded", reason="document_structure_depth_limit", units=[], unit_count=0, text_characters=0)
    except Exception:
        result.update(status="parse_error", reason="document_parse_failed", units=[], unit_count=0, text_characters=0)
    finally:
        if package is not None:
            package.close()
    result["warnings"] = sorted(warnings)
    return result
