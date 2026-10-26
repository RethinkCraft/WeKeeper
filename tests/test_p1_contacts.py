"""Synthetic Linux contact schemas; no live WeChat access or outgoing actions."""
import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from chatlog_keeper import wechat_contacts, wechat_db
from chatlog_keeper.wechat_contacts import (
    WeChatContactResolver, parse_group_nicknames, read_group_directory,
)


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def field(number, value):
    if isinstance(value, str):
        value = value.encode()
    return varint((number << 3) | 2) + varint(len(value)) + value


def member(username, nickname):
    return field(1, field(1, username) + field(2, nickname))


def database():
    connection = sqlite3.connect(":memory:")
    connection.executescript("""
        CREATE TABLE contact(id INTEGER,username TEXT,local_type INTEGER,
                             alias TEXT,remark TEXT,nick_name TEXT);
        CREATE TABLE chat_room(id INTEGER,username TEXT,owner TEXT,ext_buffer BLOB);
        CREATE TABLE chatroom_member(room_id INTEGER,member_id INTEGER);
        INSERT INTO contact VALUES(70,'group@chatroom',2,'','','合成测试群');
        INSERT INTO contact VALUES(80,'wxid_a',1,'alias-a','本地备注','甲');
        INSERT INTO contact VALUES(90,'wxid_b',1,'alias-b','','乙');
        INSERT INTO contact VALUES(100,'wxid_c',1,'','','');
        INSERT INTO chatroom_member VALUES(4,80),(4,90),(4,100);
    """)
    connection.execute("INSERT INTO chat_room VALUES(?,?,?,?)", (
        4, "group@chatroom", "wxid_b",
        member("wxid_a", "群名甲") + member("wxid_b", "群名乙") + member("wxid_c", ""),
    ))
    connection.commit()
    return connection


def test_integer_joins_and_identity_priorities():
    connection = database()
    group = read_group_directory(connection)[0]
    assert group["group_id"] == "group@chatroom"
    assert group["name"] == "合成测试群"
    assert group["owner_id"] == "wxid_b"
    assert group["coverage"] == {
        "status": "available", "scope": "local_cached_directory",
        "member_count": 3, "unresolved_member_count": 0,
        "group_nickname_status": "parsed",
    }
    people = {row["sender_id"]: row for row in group["members"]}
    assert people["wxid_a"] == {
        "sender_id": "wxid_a", "alias": "alias-a", "remark": "本地备注", "nickname": "甲",
        "group_nickname": "群名甲", "display_name": "本地备注", "is_owner": False,
    }
    assert people["wxid_b"]["display_name"] == "群名乙"
    assert people["wxid_b"]["is_owner"] is True
    assert people["wxid_c"]["display_name"] == "wxid_c"
    connection.close()


def test_unresolved_member_is_partial_not_fake_sender():
    connection = database()
    connection.execute("INSERT INTO chatroom_member VALUES(4,999)")
    group = read_group_directory(connection)[0]
    assert len(group["members"]) == 3
    assert group["coverage"]["status"] == "partial"
    assert group["coverage"]["unresolved_member_count"] == 1
    connection.close()


def test_duplicate_relations_do_not_duplicate_people():
    connection = database()
    connection.execute("INSERT INTO chatroom_member VALUES(4,80)")
    assert len(read_group_directory(connection)[0]["members"]) == 3
    connection.close()


def test_empty_and_unsupported_rosters_are_distinct():
    connection = database()
    connection.execute("DELETE FROM chatroom_member")
    assert read_group_directory(connection)[0]["coverage"]["status"] == "available"
    connection.execute("DROP TABLE chatroom_member")
    group = read_group_directory(connection)[0]
    assert group["coverage"]["status"] == "unsupported_schema"
    assert group["members"] == []
    connection.close()


def test_unsupported_room_schema_falls_back_without_inventing_members():
    connection = database()
    connection.executescript("DROP TABLE chat_room; CREATE TABLE chat_room(username TEXT,owner TEXT)")
    connection.execute("INSERT INTO chat_room VALUES('group@chatroom','wxid_b')")
    group = read_group_directory(connection)[0]
    assert group["coverage"]["status"] == "unsupported_schema"
    assert group["members"] == []
    connection.close()


def test_only_verified_fields_supply_group_nicknames():
    raw = member("wxid_a", "群昵称") + field(5, "wxid_admin_candidate")
    raw += varint((9 << 3) | 0) + varint(100)
    assert parse_group_nicknames(raw) == ({"wxid_a": "群昵称"}, "parsed")
    connection = database()
    connection.execute("UPDATE chat_room SET ext_buffer=?", (raw,))
    rows = read_group_directory(connection)[0]["members"]
    assert all("is_admin" not in row and "role" not in row for row in rows)
    connection.close()


@pytest.mark.parametrize("raw", [b"\x0a\xff", b"\x0a\x02\x00\x00", b"\x00", b"\x0b", b"\x80" * 12])
def test_malformed_protobuf_produces_no_partial_names(raw):
    assert parse_group_nicknames(member("wxid_a", "valid") + raw) == ({}, "parse_error")


def test_missing_or_unrecognized_buffer_is_explicit():
    assert parse_group_nicknames(None) == ({}, "missing")
    assert parse_group_nicknames("not protobuf") == ({}, "unsupported_encoding")
    assert parse_group_nicknames(member("wxid_a", "a") + member("wxid_a", "b")) == ({}, "partial")


def test_resolver_load_uses_private_temp_and_returns_detached_directory(monkeypatch, tmp_path):
    source = tmp_path / "account" / "db_storage/contact/contact.db"
    source.parent.mkdir(parents=True)
    connection = database()
    on_disk = sqlite3.connect(source)
    connection.backup(on_disk)
    on_disk.close()
    connection.close()
    temporary = []

    def decrypt(_source, _key, output):
        assert output.parent.stat().st_mode & 0o777 == 0o700
        temporary.append(output.parent)
        output.write_bytes(source.read_bytes())
        os.chmod(output, 0o600)
        return True

    monkeypatch.setattr(wechat_db, "_decrypt_db_v4", decrypt)
    monkeypatch.setattr(WeChatContactResolver, "_extract_contact_key", lambda self: b"x" * 32)
    resolver = WeChatContactResolver(SimpleNamespace(wxid_dir=tmp_path / "account"))
    assert resolver.load()
    assert resolver.resolve_display_name("wxid_a") == "本地备注"
    original = resolver.directory_snapshot()
    original[0]["members"].clear()
    assert len(resolver.directory_snapshot()[0]["members"]) == 3
    assert resolver.directory_status["status"] == "available"
    assert all(not folder.exists() for folder in temporary)


def test_failed_load_cannot_mean_empty_roster(monkeypatch, tmp_path):
    resolver = WeChatContactResolver(SimpleNamespace(wxid_dir=tmp_path / "missing"))
    monkeypatch.setattr(resolver, "_extract_contact_key", lambda: pytest.fail("not needed"))
    assert resolver.directory_snapshot() == []
    assert resolver.directory_status["status"] == "unavailable"


def test_snapshot_cleanup_when_directory_query_fails(monkeypatch, tmp_path):
    source = tmp_path / "account" / "db_storage/contact/contact.db"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"placeholder")
    temporary = []

    def decrypt(_source, _key, output):
        temporary.append(output.parent)
        sqlite3.connect(output).close()
        return True

    monkeypatch.setattr(wechat_db, "_decrypt_db_v4", decrypt)
    monkeypatch.setattr(WeChatContactResolver, "_extract_contact_key", lambda self: b"x" * 32)
    monkeypatch.setattr(wechat_contacts, "read_group_directory", lambda conn: (_ for _ in ()).throw(RuntimeError("synthetic")))
    resolver = WeChatContactResolver(SimpleNamespace(wxid_dir=tmp_path / "account"))
    with pytest.raises(RuntimeError, match="synthetic"):
        resolver.load()
    assert all(not folder.exists() for folder in temporary)
