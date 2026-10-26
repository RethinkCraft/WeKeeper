"""Synthetic P1 samples: no real chat bodies or contact identities."""
import html

import zstandard

from chatlog_keeper.message_model import PARSER_VERSION, parse_record


def forward(items):
    record = f'<recordinfo><datalist count="99">{items}</datalist></recordinfo>'
    return '<msg><appmsg><type>19</type><title>合并记录</title><recorditem>' + html.escape(record) + '</recorditem></appmsg></msg>'


def test_forward_direct_children_nested_hierarchy_and_full_text():
    body = "长正文" * 1000
    nested = '<dataitem datatype="17"><datatitle>内层</datatitle><recordxml><recordinfo><datalist><dataitem datatype="1"><datadesc>内层正文</datadesc></dataitem></datalist></recordinfo></recordxml></dataitem>'
    first = f'<dataitem datatype="1"><datadesc>{body}</datadesc><sourcename>甲</sourcename><srcMsgCreateTime>1790000000</srcMsgCreateTime><fromnewmsgid>1234567890123456789</fromnewmsgid><dataitemsource><fromusr>wxid_a</fromusr></dataitemsource></dataitem>'
    parsed = parse_record({"local_type": 49, "message_content": forward(first + nested)})
    assert PARSER_VERSION == 4
    assert parsed["forwarded_status"] == "parsed"
    assert len(parsed["forwarded"]) == 2
    one, two = parsed["forwarded"]
    assert one["text"] == body
    assert one["ordinal"] == 1 and two["ordinal"] == 2
    assert one["sender_id"] == "wxid_a"
    assert one["server_id"] == "1234567890123456789"
    assert one["create_time"] == 1790000000
    assert len(two["children"]) == 1
    assert two["children"][0]["text"] == "内层正文"


def test_forward_datatype_two_image_has_safe_metadata_and_no_secrets():
    xml = forward('<dataitem datatype="2"><fullmd5>' + 'a' * 32 + '</fullmd5><thumbfullmd5>' + 'b' * 32 + '</thumbfullmd5><key>PRIVATE</key><cdnthumbkey>PRIVATE</cdnthumbkey><cdndataurl>https://example.invalid/PRIVATE</cdndataurl><dataitemsource><hashusername>not-a-wxid</hashusername></dataitemsource></dataitem>')
    item = parse_record({"local_type": 49, "message_content": xml})["forwarded"][0]
    assert item["datatype"] == 2
    assert item["attachment"]["kind"] == "image"
    assert item["attachment"]["file_md5"] == 'a' * 32
    assert item["attachment"]["cache_status"] == "unresolved"
    assert item["sender_id"] == ""
    assert "PRIVATE" not in str(item)


def test_missing_and_malformed_nested_forward_are_explicit():
    parsed = parse_record({"local_type": 49, "message_content": forward('<dataitem datatype="17"><datatitle>缺失</datatitle></dataitem>')})
    assert parsed["forwarded"][0]["parse_status"] == "missing_metadata"
    empty = parse_record({"local_type": 49, "message_content": '<msg><appmsg><type>19</type></appmsg></msg>'})
    assert empty["forwarded_status"] == "missing_metadata"
    malformed = parse_record({"local_type": 49, "message_content": '<msg><appmsg><type>19</type><recorditem>&lt;malformed</recorditem></appmsg></msg>'})
    assert malformed["forwarded_status"] == "parse_error"


def test_forward_depth_limit_reports_incomplete_not_a_successful_empty_record():
    item = '<dataitem datatype="1"><datadesc>end</datadesc></dataitem>'
    for _ in range(35):
        item = '<dataitem datatype="17"><recordxml><recordinfo><datalist>' + item + '</datalist></recordinfo></recordxml></dataitem>'
    parsed = parse_record({"local_type": 49, "message_content": forward(item)})
    assert parsed["forwarded_status"] == "parse_error"
    assert parsed["forwarded"] == []


def test_mentions_only_own_source_and_no_plaintext_or_quote_inference():
    quote = '<msg><appmsg><type>57</type><title>@所有人 正文</title><refermsg><type>1</type><content>&lt;msgsource&gt;&lt;atuserlist&gt;notify@all&lt;/atuserlist&gt;&lt;/msgsource&gt;</content></refermsg></appmsg></msg>'
    parsed = parse_record({"local_type": 49, "message_content": quote,
                           "message_source": '<msgsource><nested><atuserlist>notify@all</atuserlist></nested></msgsource>'})
    assert parsed["mentions"] == []
    assert not parsed["mention_all"]
    assert parsed["mentions_status"] == "verified"
    missing = parse_record({"local_type": 1, "message_content": "@所有人 hello"})
    assert missing["mentions_status"] == "missing_metadata"
    assert not missing["mention_all"]


def test_compressed_actual_source_alias_and_duplicates():
    source = zstandard.ZstdCompressor().compress(b'<msgsource><atuserlist>wxid_a,notify@all,wxid_a</atuserlist></msgsource>')
    parsed = parse_record({"local_type": 1, "message_content": "hello", "source": source, "WCDB_CT_source": 4})
    assert parsed["mentions"] == ["wxid_a", "notify@all"]
    assert parsed["mention_all"] is True
    assert parsed["mentions_status"] == "verified"


def test_bad_source_does_not_corrupt_valid_message_parse():
    parsed = parse_record({"local_type": 1, "message_content": "hello", "message_source": b'\xff'})
    assert parsed["parse_status"] == "parsed"
    assert parsed["mentions_status"] == "parse_error"
    assert parsed["text"] == "hello"


def system(template, links):
    return '<sysmsg type="sysmsgtemplate"><sysmsgtemplate><content_template><template>' + template + '</template><link_list>' + links + '</link_list></content_template></sysmsgtemplate></sysmsg>'


def profile(name, sender, nickname):
    return f'<link name="{name}" type="link_profile"><memberlist><member><username>{sender}</username><nickname>{nickname}</nickname></member></memberlist></link>'


def test_invited_self_and_invited_other_identities_without_inference():
    join = system('"$username$"邀请你加入了群聊，群聊参与人还有：$others$', profile('username', 'wxid_inviter', '邀请人') + '<link name="others" type="link_plain"><plain>其他已有成员</plain></link>')
    parsed = parse_record({"local_type": 10000, "message_content": join})
    event = parsed["event"]
    assert event["kind"] == "sysmsgtemplate" and event["event_type"] == "member_join"
    assert event["actors"] == [{"sender_id": "wxid_inviter", "name": "邀请人"}]
    assert event["targets"] == [{"sender_id": None, "name": "你"}]
    assert event["identity_status"] == "partial"
    assert "其他已有成员" in parsed["text"]
    assert "其他已有成员" not in str(event["targets"])
    other = system('你邀请"$names$"加入了群聊 $revoke$', profile('names', 'wxid_joiner', '新成员'))
    event = parse_record({"local_type": 10000, "message_content": other})["event"]
    assert event["targets"] == [{"sender_id": "wxid_joiner", "name": "新成员"}]
    assert event["actors"][0]["sender_id"] is None


def test_rename_preserves_new_name_old_unknown_and_actor():
    xml = system('"$username$"修改群名为“$remark$”', profile('username', 'wxid_owner', '群主') + '<link name="remark" type="link_plain"><plain>新群名</plain></link>')
    event = parse_record({"local_type": 10000, "message_content": xml})["event"]
    assert event["event_type"] == "group_rename"
    assert event["new_name"] == "新群名"
    assert event["old_name"] is None
    assert event["actors"][0]["sender_id"] == 'wxid_owner'


def test_linux_system_group_prefix_does_not_need_to_equal_sender():
    xml = system('"$username$"修改群名为“$remark$”', profile('username', 'wxid_owner', '群主') + '<link name="remark" type="link_plain"><plain>新群名</plain></link>')
    parsed = parse_record({"local_type": 10000, "group_id": "123@chatroom",
                           "sender_id": "unresolved:shard:1",
                           "message_content": "123@chatroom:\n" + xml})
    assert parsed["event"]["event_type"] == "group_rename"


def test_revoke_has_complete_target_and_unverified_event_stays_unknown():
    xml = '<sysmsg type="revokemsg"><revokemsg><newmsgid>1234567890123456789</newmsgid><session>room@chatroom</session><replacemsg>撤回消息</replacemsg></revokemsg></sysmsg>'
    event = parse_record({"local_type": 10000, "message_content": xml})["event"]
    assert event["event_type"] == "message_revoke"
    assert event["target_server_id"] == '1234567890123456789'
    unknown = parse_record({"local_type": 10000, "message_content": system('有人被移出了群聊', '')})
    assert unknown["event"]["event_type"] == "unknown"
