from chatlog_keeper.message_model import parse_record, source_row_hash


def test_full_quote_id_and_long_quote_are_not_display_truncated():
    quoted = "完整引用内容" * 100
    body = f'''<msg><appmsg><type>57</type><title>这是回答</title><refermsg>
      <svrid>1234567890123456789</svrid><fromusr>wxid_sender</fromusr>
      <displayname>张三</displayname><type>1</type><content>{quoted}</content>
      <createtime>1790000000</createtime></refermsg></appmsg></msg>'''
    parsed = parse_record({"local_type": 49, "message_content": body})
    assert parsed["sub_type"] == 57
    assert parsed["reply"]["server_id"] == "1234567890123456789"
    assert parsed["reply"]["text"] == quoted
    assert parsed["text"] == "这是回答"


def test_image_quote_remains_distinct_from_its_own_reply():
    body = '''<msg><appmsg><type>57</type><title>看这张</title><refermsg>
      <svrid>987654321000001</svrid><type>3</type>
      <content>&lt;msg&gt;&lt;img aeskey="never-publish" /&gt;&lt;/msg&gt;</content>
      </refermsg></appmsg></msg>'''
    parsed = parse_record({"local_type": 49, "message_content": body})
    assert parsed["reply"]["msg_type"] == 3
    assert parsed["reply"]["text"] == "[图片]"
    assert "never-publish" not in str(parsed)


def test_unknown_empty_and_invalid_utf8_are_retained_with_status():
    assert parse_record({"local_type": 12345, "message_content": b"raw"})["parse_status"] == "unsupported"
    assert parse_record({"local_type": 1, "message_content": b""})["parse_status"] == "empty"
    assert parse_record({"local_type": 1, "message_content": b"\xff"})["parse_status"] == "parse_error"


def test_type_flags_and_sender_prefix_are_normalized():
    parsed = parse_record({"local_type": 0x100000001, "sender_id": "wxid_a",
                           "message_content": "wxid_a:\n你好"})
    assert parsed["msg_type"] == 1
    assert parsed["text"] == "你好"


def test_credentials_are_not_in_normalized_text_or_attachment():
    parsed = parse_record({"local_type": 3,
                           "message_content": '<msg><img aeskey="SECRET" md5="abcd" length="12" /></msg>'})
    assert parsed["attachment"]["kind"] == "image"
    assert "aeskey" not in parsed["attachment"]
    assert "SECRET" not in str(parsed)
    text = parse_record({"local_type": 1, "message_content": '<aeskey>SECRET</aeskey> AESKEY="SECRET"'})
    assert "SECRET" not in text["text"]


def test_revoke_preserves_target_and_does_not_return_raw_xml():
    parsed = parse_record({"local_type": 10000, "message_content": '''<sysmsg type="revokemsg">
      <revokemsg><newmsgid>123456789012345</newmsgid><session>group@chatroom</session>
      <replacemsg>某人撤回了一条消息</replacemsg></revokemsg></sysmsg>'''})
    assert parsed["event"]["target_server_id"] == "123456789012345"
    assert parsed["text"] == "某人撤回了一条消息"


def test_row_fingerprint_ignores_display_names_and_includes_source_payload():
    raw = {"row_id": 2, "local_type": 1, "create_time": 123, "real_sender_id": 3,
           "server_id": "12", "message_content": b"hello", "packed_info_data": b"packed"}
    renamed = {**raw, "sender_name": "new name", "group_name": "new group"}
    assert source_row_hash(raw) == source_row_hash(renamed)
    assert source_row_hash(raw) != source_row_hash({**raw, "message_content": b"edited"})


def test_zstandard_is_parsed_and_source_mentions_are_explicit():
    import zstandard
    parsed = parse_record({"local_type": 1,
        "message_content": zstandard.ZstdCompressor().compress("压缩文字".encode()),
        "message_source": '<msgsource><atuserlist>wxid_a,notify@all</atuserlist></msgsource>'})
    assert parsed["text"] == "压缩文字"
    assert parsed["mentions"] == ["wxid_a", "notify@all"]
