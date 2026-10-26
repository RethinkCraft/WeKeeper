"""Synthetic P2 payloads; no real chat bodies, UI, network or cache discovery."""
import html
import json

import pytest
import zstandard

from chatlog_keeper.message_model import PARSER_VERSION, parse_record, safe_web_url


def record(subtype, body="", **raw):
    return parse_record({"local_type": 49, "message_content":
                         f'<msg><appmsg><type>{subtype}</type>{body}</appmsg></msg>', **raw})


def note(items, title="Synthetic note"):
    payload = '<recordinfo><datalist>' + items + '</datalist></recordinfo>'
    return record(24, '<title>' + html.escape(title) + '</title><recorditem>' + html.escape(payload) + '</recorditem>')


def test_notes_are_ordered_components_and_not_merged_forwards():
    text = "完整笔记正文" * 1000
    nested = ('<dataitem datatype="17"><recordxml><recordinfo><datalist>'
              '<dataitem datatype="1"><datadesc>nested text</datadesc></dataitem>'
              '</datalist></recordinfo></recordxml></dataitem>')
    parsed = note('<dataitem datatype="1"><datadesc>' + text + '</datadesc><sourcename>Sender</sourcename>'
                  '<fromnewmsgid>12345678901234567890</fromnewmsgid></dataitem>' + nested)
    assert PARSER_VERSION == 4
    assert parsed["card"]["kind"] == "note"
    assert parsed["card_components_status"] == "parsed"
    assert parsed["forwarded"] == [] and parsed["forwarded_status"] == "not_applicable"
    first, second = parsed["card_components"]
    assert first["text"] == text and first["sender_name"] == "Sender"
    assert first["server_id"] == "12345678901234567890"
    assert [first["ordinal"], second["ordinal"]] == [1, 2]
    assert second["text"] == "[嵌套记录]"
    assert second["children"][0]["text"] == "nested text"
    assert parsed["text"] == "Synthetic note"


def test_note_images_and_html_attachments_keep_only_safe_metadata():
    private = "PRIVATE-CDN-CREDENTIAL"
    parsed = note('<dataitem datatype="2"><fullmd5>' + 'A' * 32 + '</fullmd5>'
                  '<thumbfullmd5>' + 'b' * 32 + '</thumbfullmd5><datasize>24</datasize>'
                  f'<cdndataurl>https://cdn.invalid/{private}</cdndataurl><cdndatakey>{private}</cdndatakey></dataitem>'
                  '<dataitem datatype="8"><datatitle>note.htm</datatitle><fullmd5>' + 'c' * 32 + '</fullmd5>'
                  f'<datafmt>.htm</datafmt><datasize>0</datasize><cdnthumbkey>{private}</cdnthumbkey></dataitem>')
    image, attachment = parsed["card_components"]
    assert image["attachment"]["kind"] == "image"
    assert image["attachment"]["file_md5"] == 'a' * 32
    assert image["attachment"]["thumbnail_md5"] == 'b' * 32
    assert image["attachment"]["total_bytes"] == 24
    assert attachment["attachment"]["kind"] == "file"
    assert attachment["attachment"]["filename"] == "note.htm"
    assert attachment["attachment"]["fileext"] == "htm"
    assert attachment["attachment"]["total_bytes"] == 0
    assert private not in json.dumps(parsed)


def test_missing_malformed_and_unknown_note_components_remain_explicit():
    absent = record(24, '<title>No cached record</title>')
    assert absent["card"]["parse_status"] == "missing_metadata"
    assert absent["card_components_status"] == "missing_metadata"
    assert absent["card_components"] == []
    malformed = record(24, '<recorditem>&lt;broken</recorditem>')
    assert malformed["card_components_status"] == "parse_error"
    unsupported = note('<dataitem datatype="999"><datadesc>unknown full body</datadesc></dataitem>')
    item = unsupported["card_components"][0]
    assert item["datatype"] == 999 and item["text"] == "unknown full body"
    assert item["parse_status"] == "unsupported"


def test_notes_reuse_bounded_parser_without_partial_success():
    item = '<dataitem datatype="1"><datadesc>end</datadesc></dataitem>'
    for _ in range(35):
        item = '<dataitem datatype="17"><recordxml><recordinfo><datalist>' + item + '</datalist></recordinfo></recordxml></dataitem>'
    parsed = note(item)
    assert parsed["card_components"] == []
    assert parsed["card_components_status"] == "parse_error"


@pytest.mark.parametrize("url", [
    "https://example.invalid/path?ticket=secret",
    "https://example.invalid/path?access_token=secret",
    "https://example.invalid/path?%61eskey=secret",
    "https://example.invalid/path#session_key=secret",
    "https://user:password@example.invalid/path",
    "https://example.invalid:bad/path",
    "https://example.invalid/path\nother",
    "https://example.invalid/\\other",
    "javascript:alert(1)", "file:///etc/passwd", "//example.invalid/path",
    "https://mmbiz.qpic.cn/image/abc", "https://wx.qlogo.cn/head/abc",
])
def test_private_or_non_web_urls_are_withheld_whole(url):
    assert safe_web_url(url) is None
    parsed = record(5, '<title>Link title</title><url>' + html.escape(url) + '</url>')
    assert parsed["card"]["public_web_url"] is None
    assert parsed["card"]["web_url_status"] == "not_public_or_missing"
    assert url not in json.dumps(parsed)


def test_public_article_url_and_complete_local_title_description_are_retained():
    title, description = "完整标题" * 100, "完整摘要" * 150
    url = "https://mp.weixin.qq.com/s?__biz=public&id=17&sn=public-signature"
    parsed = record(5, '<title>' + title + '</title><des>' + description + '</des><sourceusername>gh_sample</sourceusername>'
                    '<sourcedisplayname>Sample source</sourcedisplayname><url>' + html.escape(url) + '</url>')
    card = parsed["card"]
    assert card["kind"] == "article"
    assert card["title"] == title and card["description"] == description
    assert card["source_name"] == "Sample source" and card["source_username"] == "gh_sample"
    assert card["public_web_url"] == url and card["web_url_status"] == "available"
    web = record(1, '<title>Ordinary page</title><url>https://example.invalid/a?q=words#section</url>')
    assert web["card"]["kind"] == "link"
    assert web["card"]["public_web_url"] == 'https://example.invalid/a?q=words#section'


@pytest.mark.parametrize("subtype", [33, 36])
def test_miniprogram_local_identity_and_page_path_without_auth_material(subtype):
    parsed = record(subtype, '<title>Program card</title><weappinfo><appid>wx_public_app</appid>'
                    '<username>program@app</username><appname>Public program</appname><pagepath>pages/item?id=7</pagepath>'
                    '<version>3</version><type>2</type><appsecret>PRIVATE</appsecret></weappinfo>')
    card = parsed["card"]
    assert card["kind"] == "miniprogram" and card["app_id"] == "wx_public_app"
    assert card["app_name"] == "Public program" and card["page_path"] == "pages/item?id=7"
    assert card["app_version"] == 3 and card["app_type"] == 2
    assert "PRIVATE" not in json.dumps(parsed)
    authenticated = record(subtype, '<weappinfo><pagepath>pages/item?session_key=PRIVATE</pagepath></weappinfo>')
    assert authenticated["card"]["page_path"] is None
    assert "PRIVATE" not in json.dumps(authenticated)


@pytest.mark.parametrize("subtype", [35, 51, 63])
def test_video_channel_card_metadata_never_reads_video_or_private_urls(subtype):
    parsed = record(subtype, '<title>Channel share</title><finderFeed><nickname>Author</nickname><username>author-id</username>'
                    '<objectId>feed-id</objectId><desc>Full local description</desc><mediaList>'
                    '<media><url>https://cdn.invalid/PRIVATE</url><aeskey>PRIVATE</aeskey></media></mediaList></finderFeed>')
    card = parsed["card"]
    assert card["kind"] == "video_channel" and card["author_name"] == "Author"
    assert card["feed_id"] == "feed-id" and card["channel_description"] == "Full local description"
    assert card["content_scope"] == "local_card_only"
    assert "PRIVATE" not in json.dumps(parsed)


def test_native_location_has_complete_label_and_unknown_coordinate_system():
    label = 'Detailed address ' * 60
    parsed = parse_record({"local_type": 48, "message_content": '<msg><location x="31.2" y="121.5" scale="15" label="' + label + '" poiname="Point name"/></msg>'})
    card = parsed["card"]
    assert card["kind"] == "location" and card["label"] == label
    assert card["latitude"] == 31.2 and card["longitude"] == 121.5
    assert card["coordinate_system"] == "unknown" and card["scale"] == 15
    invalid = parse_record({"local_type": 48, "message_content": '<msg><location x="nan" y="999"/></msg>'})["card"]
    assert invalid["latitude"] is None and invalid["longitude"] is None
    assert invalid["coordinate_status"] == "missing_or_invalid"


def test_namecard_preserves_local_fields_without_head_urls_or_secret_fields():
    nickname, signature = "Long nickname" * 30, "Long signature" * 50
    xml = '<msg username="wxid_public" nickname="' + nickname + '" alias="alias" country="CN" province="Province" city="City" sign="' + signature + '" sex="2" bigheadimgurl="https://cdn.invalid/PRIVATE" aeskey="PRIVATE"/>'
    card = parse_record({"local_type": 42, "message_content": xml})["card"]
    assert card["kind"] == "namecard" and card["username"] == "wxid_public"
    assert card["nickname"] == nickname and card["signature"] == signature
    assert card["region"] == {"country": "CN", "province": "Province", "city": "City"}
    assert card["sex"] == 2 and card["identity_status"] == "present"
    assert "PRIVATE" not in json.dumps(card)


def test_pat_has_structured_actor_target_suffix_without_extra_group_event():
    parsed = record(62, '<title>Pat notice</title><patinfo><fromusername>wxid_actor</fromusername>'
                    '<pattedusername>wxid_target</pattedusername><chatusername>group@chatroom</chatusername>'
                    '<patsuffix> shoulder</patsuffix><patsuffixversion>2</patsuffixversion><template>local template</template></patinfo>')
    card = parsed["card"]
    assert card["kind"] == "pat" and card["actor_id"] == "wxid_actor"
    assert card["target_id"] == "wxid_target" and card["chat_id"] == "group@chatroom"
    assert card["suffix"] == "shoulder" and card["suffix_version"] == 2
    assert parsed["event"] is None


def test_voice_and_sticker_safe_link_metadata():
    voice = parse_record({"local_type": 34, "message_content": '<msg><voicemsg voiceformat="4" voicelength="1234" length="50" silklength="44" voicemd5="' + 'A' * 32 + '" clientmsgid="client-public" aeskey="PRIVATE" voiceurl="https://cdn.invalid/PRIVATE"/></msg>'})
    attachment = voice["attachment"]
    assert attachment == {"kind": "voice", "voice_length_ms": 1234, "voiceformat": "4", "voice_md5": 'a' * 32,
                          "clientmsgid": "client-public", "length": 50, "silk_length": 44}
    assert "PRIVATE" not in json.dumps(voice)
    sticker = parse_record({"local_type": 47, "message_content": '<msg><emoji md5="' + 'b' * 32 + '" len="99" type="2" width="100" height="120" format="gif" cdnurl="https://cdn.invalid/PRIVATE" aeskey="PRIVATE"/></msg>'})
    attachment = sticker["attachment"]
    assert attachment["kind"] == "sticker" and attachment["md5"] == 'b' * 32
    assert attachment["length"] == 99 and attachment["format"] == "gif"
    assert "PRIVATE" not in json.dumps(sticker)
    malformed = parse_record({"local_type": 34, "message_content": '<msg><voicemsg voicelength="bad" voicemd5="bad"/></msg>'})
    assert malformed["parse_status"] == "parsed" and malformed["attachment"]["voice_length_ms"] is None
    assert malformed["attachment"]["voice_md5"] == ""


def test_compressed_notes_and_quoted_notes_do_not_inflate_components():
    content = '<msg><appmsg><type>24</type><recorditem>' + html.escape('<recordinfo><datalist><dataitem datatype="1"><datadesc>body</datadesc></dataitem></datalist></recordinfo>') + '</recorditem></appmsg></msg>'
    parsed = parse_record({"local_type": 49, "message_content": zstandard.ZstdCompressor().compress(content.encode())})
    assert parsed["card_components"][0]["text"] == "body"
    quote = record(57, '<title>Reply</title><refermsg><type>49</type><content>' + html.escape(content) + '</content></refermsg>')
    assert quote["card_components"] == [] and quote["forwarded"] == []
    assert quote["card"] is None and quote["reply"]


def test_quoted_link_uses_safe_summary_instead_of_legacy_url_credentials():
    body = '<msg><appmsg><type>5</type><title>Shared page</title><url>https://user:PRIVATE@example.invalid/path</url></appmsg></msg>'
    parsed = record(57, '<title>Reply</title><refermsg><type>49</type><content>' + html.escape(body) + '</content></refermsg>')
    assert parsed["reply"]["text"] == '[链接] Shared page'
    assert "PRIVATE" not in json.dumps(parsed)


def test_component_source_identifiers_and_favorite_time_are_not_original_send_time():
    parsed = note('<dataitem datatype="1" dataid="component-id"><datadesc>body</datadesc><favcreatetime>123</favcreatetime>'
                  '<messageuuid>uuid-id</messageuuid><srcMsgLocalid>local-id</srcMsgLocalid></dataitem>')
    item = parsed["card_components"][0]
    assert item["data_id"] == "component-id" and item["message_uuid"] == "uuid-id"
    assert item["source_local_id"] == "local-id" and item["favorite_time"] == 123
    assert item["create_time"] is None


def test_unknown_card_preserves_full_title_and_explicit_unknown_type():
    text = "unknown full title " * 500
    parsed = record(9000, '<title>' + text + '</title>')
    assert parsed["card"]["kind"] == "unknown" and parsed["card"]["title"] == text.strip()
    assert parsed["card"]["sub_type"] == 9000 and parsed["card"]["parse_status"] == "unsupported"
