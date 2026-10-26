"""Loss-aware parsing of private archived WeChat rows.

Raw payloads belong in the private archive. These normalized fields are safe to
publish to an agent and keep complete reference IDs rather than display anchors.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from chatlog_keeper import wechat_db

MAX_PARSE_BYTES = 16 * 1024 * 1024
PARSER_VERSION = 4
MAX_FORWARD_DEPTH = 32
MAX_FORWARD_ITEMS = 10000
_ATTACHMENT_FIELDS = {
    "kind", "filename", "file_md5", "md5", "length", "total_bytes", "fileext",
    "voice_length_ms", "voiceformat", "encryver", "playlength", "cdnthumblength",
    "cdnthumbwidth", "cdnthumbheight",
}
_PRIVATE_URL_FIELDS = frozenset({
    "aeskey", "aes_key", "aes-key", "key", "cdnkey", "cdndatakey", "cdnthumbkey",
    "access_token", "accesstoken", "auth", "authorization", "authkey", "auth_key",
    "token", "wxtoken", "wx_token", "sessionid", "session_id", "sessionkey",
    "session_key", "pass_ticket", "passticket", "exportkey", "export_key",
    "secret", "appsecret", "password", "passwd", "ticket", "credential",
})
_PRIVATE_LINK_HOSTS = ("qpic.cn", "qlogo.cn")


def sanitize_text(text: str) -> str:
    text = re.sub(r'''(\baes[_-]?key\s*=\s*["'])[^"']*''', r'\1[redacted]',
                  text, flags=re.IGNORECASE)
    return re.sub(r'(<aes[_-]?key\b[^>]*>).*?(</aes[_-]?key>)',
                  r'\1[redacted]\2', text, flags=re.IGNORECASE | re.DOTALL)


def source_row_hash(raw: dict) -> str:
    """Hash source fields only; a renamed contact must not reset a source."""
    h = hashlib.sha256()
    for field in ("row_id", "local_type", "create_time", "real_sender_id", "server_id",
                  "message_content", "packed_info_data", "message_source"):
        value = raw.get(field)
        if isinstance(value, (bytes, bytearray, memoryview)):
            body = bytes(value)
            kind = b"bytes"
        else:
            body = json.dumps(value, ensure_ascii=False, sort_keys=True).encode()
            kind = b"json"
        h.update(kind + len(body).to_bytes(8, "big") + body)
    return h.hexdigest()


def _decode(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if raw.startswith(b"\x28\xb5\x2f\xfd"):
            import zstandard
            with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(raw)) as reader:
                raw = reader.read(MAX_PARSE_BYTES + 1)
        if len(raw) > MAX_PARSE_BYTES:
            raise ValueError("message_parse_limit")
        return raw.decode("utf-8", errors="strict")
    text = str(value)
    if len(text.encode("utf-8")) > MAX_PARSE_BYTES:
        raise ValueError("message_parse_limit")
    return text


def _text(node, path: str, default="") -> str:
    child = node.find(path) if node is not None else None
    return (child.text or "").strip() if child is not None else default


def _integer(value, default=None):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _content_md5(value) -> str:
    value = str(value or "").strip().lower()
    return value if re.fullmatch(r"[0-9a-f]{32}", value) else ""


def _file_size(value):
    value = str(value).strip() if value is not None else ""
    return int(value) if value.isascii() and value.isdecimal() else None


def _url_has_credentials(query: str) -> bool:
    try:
        fields = parse_qsl(query, keep_blank_values=True, max_num_fields=200)
    except ValueError:
        return True
    return any(key.casefold().replace("-", "_") in _PRIVATE_URL_FIELDS
               or key.casefold().replace("_", "") in _PRIVATE_URL_FIELDS
               for key, _ in fields)


def safe_web_url(value) -> str | None:
    """Expose only ordinary HTTP(S) link-card URLs, never media credentials.

    An authenticated URL is withheld whole rather than silently changed into a
    different link. No URL is requested by this parser.
    """
    if not isinstance(value, str) or not value or len(value) > 8192:
        return None
    value = value.strip()
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127
           for character in value) or "\\" in value:
        return None
    try:
        parts = urlsplit(value)
        hostname = (parts.hostname or "").casefold().rstrip(".")
        if (parts.scheme.casefold() not in {"http", "https"} or not hostname
                or parts.username is not None or parts.password is not None):
            return None
        _ = parts.port  # Reject malformed port values without performing DNS.
        if any(hostname == host or hostname.endswith("." + host) for host in _PRIVATE_LINK_HOSTS):
            return None
        if _url_has_credentials(parts.query) or _url_has_credentials(parts.fragment):
            return None
        return urlunsplit((parts.scheme.casefold(), parts.netloc, parts.path,
                           parts.query, parts.fragment))
    except (ValueError, UnicodeError):
        return None


def _public_page_path(value) -> str | None:
    if not isinstance(value, str) or not value or len(value) > 8192:
        return None
    if "\\" in value or any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        return None
    try:
        parts = urlsplit(value)
        if parts.scheme or parts.netloc or _url_has_credentials(parts.query) or _url_has_credentials(parts.fragment):
            return None
    except (ValueError, UnicodeError):
        return None
    return sanitize_text(value)


def _number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def _location_card(root, app=None):
    location = root if root is not None and root.tag == "location" else None
    if location is None and app is not None:
        location = app.find("location")
    if location is None and root is not None:
        location = root.find("location")
    attrs = location.attrib if location is not None else {}
    latitude, longitude = _number(attrs.get("x")), _number(attrs.get("y"))
    valid = (latitude is not None and longitude is not None
             and -90 <= latitude <= 90 and -180 <= longitude <= 180)
    return {"kind": "location", "latitude": latitude if valid else None,
            "longitude": longitude if valid else None,
            "coordinate_system": "unknown", "coordinate_status": "present" if valid else "missing_or_invalid",
            "label": sanitize_text(attrs.get("label", "")),
            "poi_name": sanitize_text(attrs.get("poiname", "")),
            "scale": _integer(attrs.get("scale")),
            "title": sanitize_text(_text(app, "title")) if app is not None else "",
            "parse_status": "parsed" if location is not None else "missing_metadata"}


def _name_card(root):
    node = root
    if root is not None and not root.get("username"):
        for tag in ("card", "contact"):
            candidate = root.find(tag)
            if candidate is not None:
                node = candidate
                break
    attrs = node.attrib if node is not None else {}
    username = sanitize_text(attrs.get("username", "") or _text(node, "username"))
    return {"kind": "namecard", "username": username,
            "nickname": sanitize_text(attrs.get("nickname", "") or _text(node, "nickname")),
            "alias": sanitize_text(attrs.get("alias", "") or _text(node, "alias")),
            "signature": sanitize_text(attrs.get("sign", "") or attrs.get("signature", "")
                                       or _text(node, "sign")),
            "region": {field: sanitize_text(attrs.get(field, "") or _text(node, field))
                       for field in ("country", "province", "city")},
            "sex": _integer(attrs.get("sex")), "identity_status": "present" if username else "unresolved",
            "parse_status": "parsed" if node is not None else "missing_metadata"}


def _app_card(root, app, subtype):
    if app is None or subtype == 57:
        return None
    title = sanitize_text(_text(app, "title"))
    description = sanitize_text(_text(app, "des"))
    source_name = sanitize_text(_text(app, "sourcedisplayname") or _text(app, "appinfo/appname")
                                or _text(root, "appinfo/appname"))
    source_username = sanitize_text(_text(app, "sourceusername"))
    card = {"kind": "unknown", "sub_type": subtype, "title": title,
            "description": description, "source_name": source_name,
            "source_username": source_username, "content_scope": "local_payload",
            "parse_status": "parsed"}
    if subtype == 24:
        card["kind"] = "note"
    elif subtype == 19:
        card["kind"] = "forward"
    elif subtype == 6:
        card["kind"] = "file"
    elif subtype == 17:
        card.update(_location_card(root, app))
    elif subtype == 62:
        pat = app.find("patinfo")
        card.update(kind="pat", actor_id=sanitize_text(_text(pat, "fromusername")),
                    target_id=sanitize_text(_text(pat, "pattedusername")),
                    chat_id=sanitize_text(_text(pat, "chatusername")),
                    suffix=sanitize_text(_text(pat, "patsuffix")),
                    suffix_version=_integer(_text(pat, "patsuffixversion")),
                    template=sanitize_text(_text(pat, "template")),
                    parse_status="parsed" if pat is not None else "missing_metadata")
    elif subtype in {33, 36} or app.find("weappinfo") is not None:
        info = app.find("weappinfo")
        page_path = _text(info, "pagepath")
        card.update(kind="miniprogram", app_id=sanitize_text(_text(info, "appid")),
                    app_username=sanitize_text(_text(info, "username")),
                    app_name=sanitize_text(_text(info, "appname") or source_name),
                    page_path=_public_page_path(page_path),
                    page_path_status="available" if _public_page_path(page_path) else "not_public_or_missing",
                    app_version=_integer(_text(info, "version")),
                    app_type=_integer(_text(info, "type")),
                    parse_status="parsed" if info is not None else "missing_metadata")
    elif subtype in {35, 51, 63} or app.find("finderFeed") is not None or app.find("finderLive") is not None:
        info = app.find("finderFeed")
        if info is None:
            info = app.find("finderLive")
        card.update(kind="video_channel", author_id=sanitize_text(_text(info, "username")),
                    author_name=sanitize_text(_text(info, "nickname")),
                    feed_id=sanitize_text(_text(info, "objectId") or _text(info, "objectid") or _text(info, "id")),
                    live_id=sanitize_text(_text(info, "liveId") or _text(info, "liveid")),
                    channel_description=sanitize_text(_text(info, "desc")),
                    content_scope="local_card_only",
                    parse_status="parsed" if info is not None else "missing_metadata")
    elif subtype in {1, 5}:
        value = _text(app, "url")
        public = safe_web_url(value)
        article = source_username.startswith("gh_")
        if public:
            article = article or (urlsplit(public).hostname or "").casefold() == "mp.weixin.qq.com"
        card.update(kind="article" if article else "link", public_web_url=public,
                    web_url_status="available" if public else "not_public_or_missing")
    else:
        card["parse_status"] = "unsupported"
    return card


def _card_summary(card):
    """Display normalized card fields without legacy raw-URL interpolation."""
    label = {"link": "链接", "article": "公众号", "miniprogram": "小程序",
             "video_channel": "视频号", "location": "位置"}.get(card["kind"], "卡片")
    parts = ["[" + label + "]"]
    if card.get("title"):
        parts.append(card["title"])
    description = card.get("description") or card.get("channel_description")
    if description and description != card.get("title"):
        parts.append(description)
    return " ".join(parts)


def _file_attachment(appmsg) -> dict:
    """Only public metadata; a filename alone never establishes a cache link."""
    attach = appmsg.find("appattach") if appmsg is not None else None
    values = [_text(attach, "md5"), _text(appmsg, "md5")]
    digests = {_content_md5(value) for value in values if _content_md5(value)}
    digest = next(iter(digests)) if len(digests) == 1 else ""
    size_text = _text(attach, "totallen")
    size = _file_size(size_text)
    filename = sanitize_text(_text(appmsg, "title"))
    extension = _text(attach, "fileext").strip().lower().lstrip(".")
    if not re.fullmatch(r"[a-z0-9]{1,16}", extension):
        suffix = re.search(r"\.([A-Za-z0-9]{1,16})$", filename)
        extension = suffix.group(1).lower() if suffix else ""
    return {"kind": "file", "filename": filename, "md5": digest,
            "total_bytes": size, "fileext": extension,
            "md5_status": "verified_metadata" if digest else "conflicting" if len(digests) > 1 else "missing_or_invalid",
            "size_status": "known" if size is not None else "missing_or_invalid",
            "metadata_status": "complete" if digest and filename and size is not None else "partial"}


def _quote(appmsg) -> dict | None:
    refer = appmsg.find("refermsg") if appmsg is not None else None
    if refer is None:
        return None
    quoted = _text(refer, "content")
    quoted_type = _integer(_text(refer, "type"), 0)
    if quoted.lstrip().startswith("<"):
        rendered = None
        if quoted_type == 49:
            root = wechat_db._parse_xml_strict(quoted)
            app = root if root is not None and root.tag == "appmsg" else root.find("appmsg") if root is not None else None
            card = _app_card(root, app, _integer(_text(app, "type")))
            if card and card["kind"] in {"link", "article", "miniprogram", "video_channel", "location"}:
                rendered = _card_summary(card)
        if rendered is None:
            rendered = wechat_db._extract_wechat_xml(quoted, quoted_type)
        if rendered:
            quoted = rendered
    return {
        "server_id": _text(refer, "svrid"),
        "sender_id": _text(refer, "fromusr"),
        "sender_name": _text(refer, "displayname"),
        "msg_type": quoted_type,
        "text": sanitize_text(quoted),
        "time": _integer(_text(refer, "createtime")),
    }


def _record_root(node):
    """A record container may hold XML children or CDATA-encoded XML."""
    if node is None:
        return None
    if node.tag == "recordinfo":
        return node
    child = node.find("recordinfo")
    if child is not None:
        return child
    if node.find("datalist") is not None:
        return node
    parsed = wechat_db._parse_xml_strict(_decode(node.text or ""))
    return parsed if parsed is not None and parsed.tag == "recordinfo" else None


def _record_items(appmsg, nested_label="[合并转发]"):
    record = appmsg.find("recorditem") if appmsg is not None else None
    root = _record_root(record)
    if root is None:
        return [], "parse_error" if record is not None and (record.text or "").strip() else "missing_metadata"
    budget = [MAX_FORWARD_ITEMS]

    def items(container, depth):
        if depth > MAX_FORWARD_DEPTH:
            raise ValueError("forward_depth_limit")
        datalist = container.find("datalist")
        children = (datalist if datalist is not None else container).findall("dataitem")
        result = []
        for ordinal, node in enumerate(children, 1):
            budget[0] -= 1
            if budget[0] < 0:
                raise ValueError("forward_item_limit")
            datatype = _integer(node.get("datatype"), 0)
            body = _text(node, "datadesc") or _text(node, "datatitle")
            nested = []
            status = "parsed"
            if datatype == 17:
                nested_root = _record_root(node.find("recordxml"))
                if nested_root is None:
                    nested_root = _record_root(node.find("recorditem"))
                if nested_root is not None:
                    nested = items(nested_root, depth + 1)
                else:
                    status = "missing_metadata"
            if not body:
                body = {2: "[图片]", 3: "[语音]", 4: "[视频]", 5: "[链接]",
                        8: "[文件]", 17: nested_label}.get(datatype, "" if datatype == 1 else "[消息]")
            attachment = None
            if datatype in (2, 3, 4, 8):
                attachment = {"kind": {2: "image", 3: "voice", 4: "video", 8: "file"}[datatype]}
                # CDN URLs and encryption keys remain only in the private raw row.
                for field, paths in {
                    "file_md5": ("fullmd5", "filemd5"),
                    "thumbnail_md5": ("thumbfullmd5",),
                    "filename": ("datatitle",),
                }.items():
                    value = next((_text(node, path) for path in paths if _text(node, path)), "")
                    if value:
                        attachment[field] = sanitize_text(value)
                if datatype == 8:
                    digest = _content_md5(attachment.get("file_md5"))
                    attachment["file_md5"] = digest
                    attachment["md5"] = digest
                    attachment["total_bytes"] = _file_size(_text(node, "datasize"))
                    extension = (_text(node, "fileext") or _text(node, "datafmt")).lower().lstrip(".")
                    attachment["fileext"] = extension if re.fullmatch(r"[a-z0-9]{1,16}", extension) else ""
                    attachment["md5_status"] = "verified_metadata" if digest else "missing_or_invalid"
                    attachment["size_status"] = "known" if attachment["total_bytes"] is not None else "missing_or_invalid"
                elif datatype == 2:
                    attachment["file_md5"] = _content_md5(attachment.get("file_md5"))
                    if attachment.get("thumbnail_md5"):
                        attachment["thumbnail_md5"] = _content_md5(attachment["thumbnail_md5"])
                    attachment["total_bytes"] = _file_size(_text(node, "datasize"))
                elif datatype == 3:
                    attachment["voice_md5"] = _content_md5(attachment.get("file_md5"))
                    attachment["voice_length_ms"] = _file_size(_text(node, "duration"))
                attachment["cache_status"] = "unresolved"
            sender_id = (_text(node, "dataitemsource/fromusr") or _text(node, "fromusr")
                         or node.get("fromusr") or "")
            result.append({
                "ordinal": ordinal, "datatype": datatype,
                "data_id": sanitize_text(node.get("dataid") or _text(node, "dataid")),
                "source_local_id": sanitize_text(_text(node, "srcMsgLocalid")),
                "message_uuid": sanitize_text(_text(node, "messageuuid")),
                "favorite_time": _integer(_text(node, "favcreatetime")),
                "text": sanitize_text(body), "sender_id": sanitize_text(sender_id),
                "sender_name": sanitize_text(_text(node, "sourcename") or node.get("sourcename") or ""),
                "create_time": _integer(_text(node, "srcMsgCreateTime")),
                "source_time": sanitize_text(_text(node, "sourcetime")),
                "server_id": (_text(node, "fromnewmsgid") or node.get("datasvrid")
                              or node.get("svrid") or _text(node, "svrid")),
                "children": nested, "attachment": attachment,
                "parse_status": status if datatype in {1, 2, 3, 4, 5, 6, 7, 8, 17} else "unsupported",
            })
        return result

    try:
        return items(root, 0), "parsed"
    except Exception:
        return [], "parse_error"


def _forwarded(appmsg):
    return _record_items(appmsg)


def _voice_attachment(root):
    node = root if root is not None and root.tag == "voicemsg" else root.find("voicemsg") if root is not None else None
    if node is None:
        return None
    client = sanitize_text(node.get("clientmsgid") or "")
    return {"kind": "voice", "voice_length_ms": _file_size(node.get("voicelength")),
            "voiceformat": sanitize_text(node.get("voiceformat") or ""),
            "voice_md5": _content_md5(node.get("voicemd5")), "clientmsgid": client,
            "length": _file_size(node.get("length")), "silk_length": _file_size(node.get("silklength"))}


def _sticker_attachment(root):
    node = root if root is not None and root.tag == "emoji" else root.find("emoji") if root is not None else None
    if node is None:
        return None
    return {"kind": "sticker", "md5": _content_md5(node.get("md5")),
            "length": _file_size(node.get("len") or node.get("length")),
            "sticker_type": _integer(node.get("type")),
            "width": _file_size(node.get("width")), "height": _file_size(node.get("height")),
            "format": sanitize_text(node.get("format") or "")}


def _profile_members(link):
    if link is None:
        return []
    return [{"sender_id": sanitize_text(_text(member, "username")) or None,
             "name": sanitize_text(_text(member, "nickname"))}
            for member in link.findall("memberlist/member")]


def _system_event(xml, raw):
    kind = xml.get("type") or "system"
    event = {"kind": kind, "event_type": "unknown", "actors": [], "targets": [],
             "old_name": None, "new_name": None, "identity_status": "unresolved"}
    revoke = xml.find("revokemsg")
    if revoke is not None:
        event.update(event_type="message_revoke",
                     target_server_id=_text(revoke, "newmsgid") or _text(revoke, "msgid"),
                     target_group_id=_text(revoke, "session"),
                     evidence="revokemsg")
        return event, sanitize_text(_text(revoke, "replacemsg") or _text(revoke, "content"))
    template_root = xml.find("sysmsgtemplate/content_template")
    if kind != "sysmsgtemplate" or template_root is None:
        return event, sanitize_text(" ".join(xml.itertext()).strip())
    template = _text(template_root, "template")
    links = {link.get("name"): link for link in template_root.findall("link_list/link")}
    rendered = template
    for name, link in links.items():
        if not name:
            continue
        profiles = _profile_members(link)
        value = "、".join(member["name"] or member["sender_id"] or "" for member in profiles)
        value = value or _text(link, "plain") or _text(link, "title")
        rendered = rendered.replace("$" + name + "$", value)
    event["evidence"] = "sysmsgtemplate"
    if "修改群名" in template and "$remark$" in template:
        event.update(event_type="group_rename", actors=_profile_members(links.get("username")),
                     new_name=sanitize_text(_text(links.get("remark"), "plain")) or None)
    elif "加入了群聊" in template and "邀请" in template:
        event["event_type"] = "member_join"
        event["actors"] = _profile_members(links.get("username"))
        event["targets"] = _profile_members(links.get("names"))
        # A local-self label is explicit evidence, but not a verified native ID.
        if template.startswith("你邀请"):
            event["actors"] = [{"sender_id": None, "name": "你"}]
        elif "邀请你加入" in template:
            event["targets"] = [{"sender_id": None, "name": "你"}]
        plain_targets = _text(links.get("names"), "plain")
        if plain_targets and not event["targets"]:
            event["targets"] = [{"sender_id": None, "name": sanitize_text(plain_targets)}]
    people = event["actors"] + event["targets"]
    if people:
        event["identity_status"] = "resolved" if all(p["sender_id"] for p in people) else "partial"
    return event, sanitize_text(_text(template_root, "plain") or rendered)


def _parse_mentions(raw, result):
    source = raw.get("message_source")
    if source is None:
        source = raw.get("source")
    if source is None:
        return
    try:
        decoded = _decode(source)
        # An explicitly present empty source is different from a legacy missing column.
        if not decoded.strip():
            result["mentions_status"] = "verified"
            return
        source_xml = wechat_db._parse_xml_strict(decoded)
        if source_xml is None:
            raise ValueError("invalid_message_source")
        # Only own msgsource. Never crawl embedded reply/forward XML.
        msgsource = source_xml if source_xml.tag == "msgsource" else source_xml.find("msgsource")
        if msgsource is None:
            raise ValueError("missing_msgsource")
        atlist = _text(msgsource, "atuserlist")
        result["mentions"] = list(dict.fromkeys(s.strip() for s in atlist.split(",") if s.strip()))
        result["mention_all"] = "notify@all" in result["mentions"]
        result["mentions_status"] = "verified"
    except Exception:
        result["mentions_status"] = "parse_error"


def parse_record(raw: dict) -> dict:
    """Parse one row without dropping unknown, empty or malformed records."""
    kind = int(raw.get("local_type") or 0) & 0xFFFF
    result = {"msg_type": kind, "sub_type": None, "text": "", "parse_status": "parsed",
              "reply": None, "attachment": None, "mentions": [], "event": None,
              "mentions_status": "missing_metadata", "mention_all": False,
              "forwarded": [], "forwarded_status": "not_applicable",
              "card": None, "card_components": [], "card_components_status": "not_applicable"}
    try:
        content = _decode(raw.get("message_content"))
        sender = str(raw.get("sender_id") or "")
        prefix, separator, rest = content.partition("\n")
        prefix_identity = prefix.strip().rstrip(":")
        if separator and (prefix_identity == sender or
                          (kind == 10000 and prefix_identity == str(raw.get("group_id") or ""))):
            content = rest
        if kind == 1:
            result["text"] = sanitize_text(content.strip())
        elif kind not in wechat_db._WX_KEPT_TYPES:
            result.update(text=f"[不支持的消息类型: {kind}]", parse_status="unsupported")
        else:
            xml = wechat_db._parse_xml_strict(content)
            if kind == 49:
                app = xml.find("appmsg") if xml is not None else None
                if app is None and xml is not None and xml.tag == "appmsg":
                    app = xml
                result["sub_type"] = _integer(_text(app, "type"))
                result["card"] = _app_card(xml, app, result["sub_type"])
                if result["sub_type"] == 57:
                    result["reply"] = _quote(app)
                    # The reply's own text stays separate from the quoted content.
                    result["text"] = sanitize_text(_text(app, "title"))
                elif result["sub_type"] == 6:
                    result["text"] = "[文件: " + sanitize_text(_text(app, "title") or "未命名") + "]"
                elif result["sub_type"] == 19:
                    result["forwarded"], result["forwarded_status"] = _forwarded(app)
                    # Child bodies are separately paged, rather than cut into a display summary.
                    result["text"] = sanitize_text(_text(app, "title") or "[合并转发]")
                elif result["sub_type"] == 24:
                    result["card_components"], result["card_components_status"] = _record_items(app, "[嵌套记录]")
                    result["card"]["parse_status"] = result["card_components_status"]
                    # Notes are not merged forwards. Their complete bodies are
                    # separately paged and do not replace the outer message.
                    result["text"] = sanitize_text(_text(app, "title") or "[笔记]")
                elif result["sub_type"] == 62:
                    result["text"] = sanitize_text(_text(app, "title") or "[拍一拍]")
                elif result["card"] and result["card"]["kind"] in {"link", "article", "miniprogram", "video_channel", "location"}:
                    result["text"] = _card_summary(result["card"])
                else:
                    result["text"] = sanitize_text(wechat_db._extract_wechat_xml(content, kind))
            elif kind == 10000:
                if xml is not None and xml.tag == "sysmsg":
                    result["event"], result["text"] = _system_event(xml, raw)
                else:
                    result["event"] = {"kind": "system", "event_type": "unknown"}
                    result["text"] = sanitize_text(content.strip())
            else:
                result["text"] = sanitize_text(wechat_db._extract_wechat_xml(content, kind))
                if kind == 42:
                    result["card"] = _name_card(xml)
                elif kind == 48:
                    result["card"] = _location_card(xml)
            if kind == 34:
                attachment = _voice_attachment(xml)
            elif kind == 47:
                attachment = _sticker_attachment(xml)
            elif kind == 49 and result["sub_type"] == 6:
                attachment = _file_attachment(app)
            else:
                attachment = wechat_db._extract_attachment_meta(content, kind)
            if attachment and attachment.get("kind") in {"file", "voice", "sticker"}:
                result["attachment"] = attachment
            elif attachment:
                result["attachment"] = {k: v for k, v in attachment.items()
                                        if k in _ATTACHMENT_FIELDS}
            if kind == 3:
                file_md5 = wechat_db._extract_file_md5_from_packed_info(raw.get("packed_info_data"))
                if file_md5:
                    result["attachment"] = result["attachment"] or {"kind": "image"}
                    result["attachment"]["file_md5"] = file_md5
            if not content.strip():
                result["parse_status"] = "empty"
            elif not result["text"] and result["reply"] is None:
                result["parse_status"] = "parse_error"
        if kind == 1 and not result["text"]:
            result["parse_status"] = "empty"
    except Exception:
        result["parse_status"] = "parse_error"
        result["text"] = result["text"] or f"[无法解析的消息: {kind}]"
    _parse_mentions(raw, result)
    return result
