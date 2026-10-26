"""WeChat 4.x contact resolver — wxid → display_name + group member resolution.

LIVE-verified schema (Weixin 4.x contact.db, 2026-04-30):
    contact (17184 rows): id, username, local_type (1=friend, 2=group), alias, encrypt_username,
                          flag, delete_flag, verify_flag, remark, remark_quan_pin,
                          remark_pin_yin_initial, nick_name, pin_yin_initial, quan_pin,
                          big_head_url, small_head_url, head_img_md5, chat_room_notify,
                          is_in_chat_room, description, extra_buffer, chat_room_type
    chat_room (224 rows): id, username (xxx@chatroom), owner (wxid), ext_buffer
    chatroom_member (19437 rows): room_id, member_id
    stranger (11 rows): same schema as contact

Display name priority: COALESCE(NULLIF(remark, ''), NULLIF(nick_name, ''),
                                 NULLIF(alias, ''), username)

Usage:
    from chatlog_keeper.wechat_db import WeChatDBReader
    from chatlog_keeper.wechat_contacts import WeChatContactResolver
    reader = WeChatDBReader()
    reader.initialize()
    resolver = WeChatContactResolver(reader)
    resolver.load()
    name = resolver.resolve_display_name("wxid_xxxx")
    is_grp = resolver.is_group("18461014732@chatroom")
"""
from __future__ import annotations

import logging
import sqlite3
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from chatlog_keeper.core._private_temp import create_private_temp_dir, cleanup_private_temp_dir

logger = logging.getLogger(__name__)

_CONTACT_DB_REL = Path("db_storage") / "contact" / "contact.db"
_MAX_ROOM_BUFFER = 8 * 1024 * 1024


def _proto_fields(raw: bytes):
    """Read bounded protobuf fields without assigning meanings to unknown tags."""
    if len(raw) > _MAX_ROOM_BUFFER:
        raise ValueError("room_buffer_too_large")

    def varint(pos):
        value = 0
        for shift in range(0, 70, 7):
            if pos >= len(raw):
                raise ValueError("truncated_room_buffer")
            octet = raw[pos]
            pos += 1
            value |= (octet & 127) << shift
            if not octet & 128:
                return value, pos
        raise ValueError("invalid_room_varint")

    pos = 0
    while pos < len(raw):
        tag, pos = varint(pos)
        field, wire = tag >> 3, tag & 7
        if field == 0:
            raise ValueError("invalid_room_tag")
        if wire == 0:
            value, pos = varint(pos)
        elif wire in (1, 5):
            width = 8 if wire == 1 else 4
            if pos + width > len(raw):
                raise ValueError("truncated_room_buffer")
            value, pos = raw[pos:pos + width], pos + width
        elif wire == 2:
            size, pos = varint(pos)
            if pos + size > len(raw):
                raise ValueError("truncated_room_buffer")
            value, pos = raw[pos:pos + size], pos + size
        else:
            raise ValueError("unsupported_room_wire_type")
        yield field, wire, value


def parse_group_nicknames(value) -> tuple[dict[str, str], str]:
    """Decode only the two Linux-verified room-member fields.

    Root field 1 repeats member records; nested field 1 is the stable wxid and
    field 2 is that member's group nickname. In particular, root field 5 is not
    interpreted as an administrator list. A malformed buffer is never used as
    partial evidence for names.
    """
    if value in (None, b"", ""):
        return {}, "missing"
    if not isinstance(value, (bytes, bytearray, memoryview)):
        return {}, "unsupported_encoding"
    try:
        names, conflicts = {}, set()
        for field, wire, member in _proto_fields(bytes(value)):
            if field != 1 or wire != 2:
                continue
            parts = {}
            for member_field, member_wire, text in _proto_fields(member):
                if member_field in (1, 2) and member_wire == 2:
                    decoded = text.decode("utf-8", errors="strict")
                    if member_field in parts and parts[member_field] != decoded:
                        raise ValueError("ambiguous_room_member")
                    parts[member_field] = decoded
            username = parts.get(1, "")
            nickname = _clean_wechat_display_name(parts.get(2, ""))
            if not username:
                continue
            if username in names and names[username] != nickname:
                conflicts.add(username)
            names[username] = nickname
        for username in conflicts:
            names.pop(username, None)
        return names, "partial" if conflicts else "parsed"
    except (ValueError, UnicodeError):
        return {}, "parse_error"


def read_group_directory(connection: sqlite3.Connection) -> list[dict]:
    """Read the current local roster using verified integer joins.

    This does not establish server completeness or historical membership.
    Missing relations remain unresolved rather than becoming invented wxids.
    """
    contact_names = {}
    try:
        for username, alias, remark, nickname in connection.execute(
            "SELECT username,alias,remark,nick_name FROM contact"
        ):
            if username:
                contact_names[username] = WeChatContactIdentity(
                    username=str(username), alias=_clean_wechat_display_name(alias),
                    remark=_clean_wechat_display_name(remark),
                    nickname=_clean_wechat_display_name(nickname),
                )
    except sqlite3.Error:
        pass
    try:
        rooms = connection.execute("SELECT id,username,owner,ext_buffer FROM chat_room").fetchall()
    except sqlite3.Error:
        try:
            rooms = [(None, username, owner, None) for username, owner in
                     connection.execute("SELECT username,owner FROM chat_room")]
        except sqlite3.Error:
            rooms = [(None, username, None, None) for username in contact_names
                     if str(username).endswith("@chatroom")]
    groups = []
    for room_id, username, owner, buffer in rooms:
        if not isinstance(username, str) or not username.endswith("@chatroom"):
            continue
        nickname_map, nickname_status = parse_group_nicknames(buffer)
        members, unresolved, status = [], 0, "available"
        try:
            if room_id is None:
                raise sqlite3.OperationalError("missing_room_id")
            rows = connection.execute(
                "SELECT cm.member_id,c.username,c.alias,c.remark,c.nick_name "
                "FROM chatroom_member AS cm LEFT JOIN contact AS c ON cm.member_id=c.id "
                "WHERE cm.room_id=? ORDER BY c.username,cm.member_id", (room_id,)
            ).fetchall()
            seen = set()
            for _member_id, member_name, alias, remark, nickname in rows:
                if not member_name or str(member_name).endswith("@chatroom"):
                    unresolved += 1
                    continue
                if member_name in seen:
                    continue
                seen.add(member_name)
                identity = WeChatContactIdentity(
                    username=str(member_name), alias=_clean_wechat_display_name(alias),
                    remark=_clean_wechat_display_name(remark),
                    nickname=_clean_wechat_display_name(nickname),
                )
                group_nickname = nickname_map.get(member_name, "")
                members.append({
                    "sender_id": member_name, "alias": identity.alias,
                    "remark": identity.remark, "nickname": identity.nickname,
                    "group_nickname": group_nickname,
                    "display_name": identity.remark or group_nickname or identity.preferred_name,
                    "is_owner": bool(owner and owner == member_name),
                })
            if unresolved:
                status = "partial"
        except sqlite3.Error:
            status = "unsupported_schema"
        identity = contact_names.get(username)
        groups.append({
            "group_id": username, "name": identity.preferred_name if identity else username,
            "owner_id": str(owner or ""), "members": members,
            "coverage": {"status": status, "scope": "local_cached_directory",
                         "member_count": len(members), "unresolved_member_count": unresolved,
                         "group_nickname_status": nickname_status},
        })
    return sorted(groups, key=lambda item: item["group_id"])


def _clean_wechat_display_name(value) -> str:
    """把微信资料字段收敛为适合本机 UI 的单行短文本。"""

    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip()[:120]


@dataclass(frozen=True)
class WeChatContactIdentity:
    """微信联系人或群聊的本机备注、昵称与微信号。"""

    username: str = ""
    alias: str = ""
    remark: str = ""
    nickname: str = ""

    @property
    def preferred_name(self) -> str:
        """聊天正文优先使用本机备注，其次昵称、微信号和内部账号标识。"""

        return self.remark or self.nickname or self.alias or self.username

    @property
    def directory_label(self) -> str:
        """生成能区分备注和昵称的微信会话目录标签。"""

        if self.remark and self.nickname and self.remark != self.nickname:
            return f"备注：{self.remark} · 昵称：{self.nickname}"
        if self.remark and self.nickname:
            return f"备注/昵称：{self.remark}"
        if self.remark:
            return f"备注：{self.remark}"
        if self.nickname:
            return f"昵称：{self.nickname}"
        if self.alias:
            return f"微信号：{self.alias}"
        return self.username

    @property
    def account_directory_label(self) -> str:
        """生成账号选择器标签，始终明确展示可用的微信号。"""

        wechat_id = self.alias or self.username
        if not wechat_id:
            return ""
        label = f"微信号：{wechat_id}"
        if self.nickname and self.nickname != wechat_id:
            label += f" · 昵称：{self.nickname}"
        return label


class WeChatContactResolver:
    """Read contact.db and provide wxid → display_name + group lookups."""

    def __init__(self, reader):
        """reader: WeChatDBReader (must be .initialize()'d)."""
        self.reader = reader
        self._wxid_to_display: dict = {}
        self._wxid_to_identity: dict[str, WeChatContactIdentity] = {}
        self._group_wxids: set = set()
        self._chatroom_owner: dict = {}
        self._directory: list[dict] = []
        self.directory_status = {"status": "not_loaded", "scope": "local_cached_directory"}
        self._loaded = False

    def _emit_trace(self, event: str, payload: dict) -> None:
        """Emit trace event (best-effort; stdlib import to avoid cycle)."""
        try:
            from chatlog_keeper.core.trace_sink import emit  # type: ignore
            emit(event, payload)
        except Exception:
            pass

    def _contact_db_path(self) -> Optional[Path]:
        if not self.reader or not self.reader.wxid_dir:
            return None
        p = self.reader.wxid_dir / _CONTACT_DB_REL
        return p if p.exists() else None

    def _extract_contact_key(self) -> Optional[bytes]:
        """Resolve the contact DB key cache-first, then try live Weixin pids."""
        from chatlog_keeper.wechat_db import (
            _get_weixin_pids,
            _read_stable_page1,
            _verify_key_v4,
            extract_key_from_weixin,
            load_cached_wechat_key,
            load_cached_wechat_key_for_account,
        )

        db = self._contact_db_path()
        if db is None:
            return None
        account_id = str(getattr(self.reader, "account_id", "") or "").strip()
        cached = (
            load_cached_wechat_key_for_account(account_id)
            if account_id
            else load_cached_wechat_key()
        )
        page1 = _read_stable_page1(db) if cached else None
        if cached and page1 and _verify_key_v4(cached, page1):
            return cached
        if not getattr(self.reader, "_allow_live_key_extract", True):
            return None
        for pid in _get_weixin_pids():
            k = extract_key_from_weixin(pid, db_path=db)
            if k and isinstance(k, (bytes, bytearray)) and len(k) == 32:
                return bytes(k)
        return None

    def load(self) -> bool:
        """Decrypt contact.db + load wxid maps. Idempotent. Returns True on success."""
        if self._loaded:
            return True
        db = self._contact_db_path()
        if db is None:
            logger.warning("contact.db not found; resolver returns wxid as display_name fallback")
            self.directory_status["status"] = "unavailable"
            self._loaded = True
            return False

        key = self._extract_contact_key()
        if not key:
            logger.warning("contact.db key extraction failed; fallback to wxid")
            self.directory_status["status"] = "unavailable"
            self._loaded = True
            return False

        from chatlog_keeper.wechat_db import _decrypt_db_v4

        temporary = create_private_temp_dir("wechat_contacts_")
        decrypt_path = temporary / "snapshot.db"
        conn = None
        try:
            ok = _decrypt_db_v4(db, key, decrypt_path)
            if not ok:
                logger.warning("contact.db decrypt failed")
                self.directory_status["status"] = "unavailable"
                return False
            conn = sqlite3.connect(str(decrypt_path))
            cur = conn.cursor()
            # Load contact + stranger (same schema). Hard-coded queries — no
            # f-string with table-name variable, to avoid security_scanner false-positive
            # on sql_injection (table names are literal, not user input).
            _CONTACT_QUERIES = (
                "SELECT username, local_type, alias, remark, nick_name FROM contact",
                "SELECT username, local_type, alias, remark, nick_name FROM stranger",
            )
            for query in _CONTACT_QUERIES:
                try:
                    cur.execute(query)
                except sqlite3.Error as e:
                    logger.debug(f"query failed: {e}")
                    continue
                for username, local_type, alias, remark, nick_name in cur.fetchall():
                    if not username:
                        continue
                    identity = WeChatContactIdentity(
                        username=_clean_wechat_display_name(username),
                        alias=_clean_wechat_display_name(alias),
                        remark=_clean_wechat_display_name(remark),
                        nickname=_clean_wechat_display_name(nick_name),
                    )
                    self._wxid_to_identity[username] = identity
                    self._wxid_to_display[username] = identity.preferred_name
                    if local_type == 2 or username.endswith("@chatroom"):
                        self._group_wxids.add(username)
            # Load chat_room owners
            try:
                cur.execute("SELECT username, owner FROM chat_room")
                for username, owner in cur.fetchall():
                    if username:
                        self._group_wxids.add(username)
                        if owner:
                            self._chatroom_owner[username] = owner
            except sqlite3.Error:
                pass
            self._directory = read_group_directory(conn)
            self.directory_status = {
                "status": "available" if self._directory else "empty_or_unsupported",
                "scope": "local_cached_directory", "group_count": len(self._directory),
            }
        finally:
            if conn is not None:
                conn.close()
            cleanup_private_temp_dir(temporary)

        self._loaded = True
        self._emit_trace("wechat_contacts_loaded", {
            "contact_count": len(self._wxid_to_display),
            "group_count": len(self._group_wxids),
        })
        logger.info(
            f"contacts loaded: {len(self._wxid_to_display)} entries, "
            f"{len(self._group_wxids)} groups"
        )
        return True

    def directory_snapshot(self) -> list[dict]:
        """Return a detached, current local roster; source failures stay explicit.

        An empty list after an unavailable/unsupported source must not be used
        to remove an archived roster. Each group also carries its own coverage.
        """
        if not self._loaded:
            self.load()
        return deepcopy(self._directory)

    def resolve_display_name(self, wxid: str) -> str:
        """Return display_name for wxid; falls back to wxid string itself."""
        if not wxid:
            return wxid or ""
        if not self._loaded:
            self.load()
        return self._wxid_to_display.get(wxid, wxid)

    def is_group(self, wxid: str) -> bool:
        """True if wxid is a group chat."""
        if not wxid:
            return False
        if wxid.endswith("@chatroom"):
            return True
        if not self._loaded:
            self.load()
        return wxid in self._group_wxids

    def search_by_name(self, name_substr: str) -> list:
        """Return [(wxid, display_name)] tuples whose display contains name_substr (case-insensitive)."""
        if not name_substr:
            return []
        if not self._loaded:
            self.load()
        ns = name_substr.lower()
        return [
            (wxid, display)
            for wxid, display in self._wxid_to_display.items()
            if ns in display.lower()
        ]

    def all_displays(self) -> dict:
        """Return copy of internal wxid → display dict."""
        if not self._loaded:
            self.load()
        return dict(self._wxid_to_display)

    def all_directory_labels(self) -> dict:
        """返回 wxid 到本机目录标签的副本，同时保留备注与昵称。"""

        if not self._loaded:
            self.load()
        return {
            wxid: identity.directory_label or wxid
            for wxid, identity in self._wxid_to_identity.items()
        }

    def account_directory_label(self, wxid: str) -> str:
        """返回账号选择器标签；资料缺失时仍展示内部微信号。"""

        if not self._loaded:
            self.load()
        identity = self._wxid_to_identity.get(wxid)
        if identity is not None and identity.account_directory_label:
            return identity.account_directory_label
        normalized = _clean_wechat_display_name(wxid)
        return f"微信号：{normalized}" if normalized else "微信账号"
