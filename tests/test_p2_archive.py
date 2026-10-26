"""P2 source/query contracts use only local synthetic records and blobs."""
import hashlib
import io
import json
import sqlite3

from PIL import Image
import pytest

from chatlog_keeper.archive import ArchiveError, ArchiveStore, _safe
from chatlog_keeper.archive_media import MediaUnavailable
from test_archive import commit, raw, setup_source


def note(text="完整卡片正文", children=None):
    return {"msg_type": 49, "sub_type": 24, "text": "笔记", "parse_status": "parsed",
            "card": {"kind": "note", "title": "完整标题", "description": "卡片内容说明"},
            "card_components": children if children is not None else [
                {"datatype": 1, "text": text, "children": []},
                {"datatype": 2, "text": "[图片]", "attachment": {"kind": "image", "file_md5": "a" * 32}, "children": []},
                {"datatype": 8, "text": "[文件]", "attachment": {"kind": "file", "filename": "note.htm", "md5": "b" * 32}, "children": []}]}


def voice():
    return {"msg_type": 34, "text": "[语音 5s]", "parse_status": "parsed",
            "attachment": {"kind": "voice", "voice_length_ms": 5000}}


def blob(store, payload=b"#!SILK_V3synthetic audio"):
    digest = hashlib.sha256(payload).hexdigest()
    relative = f"raw/{digest[:2]}/{digest}.bin"
    path = store.media_dir / relative
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(payload)
    path.chmod(0o600)
    return {"raw_path": relative, "raw_sha256": digest, "raw_size": len(payload), "codec": "silk_v3", "mime": "audio/silk"}


def seed_voice(store, number=1, sid="v1"):
    source = setup_source(store)
    reference = commit(store, source, [raw(number, sid, local_type=34, parsed=voice())])["message_refs"][0]
    result = store.upsert_voice("account", "test@chatroom", sid, blob(store), {"database": "db_storage/message/media_0.db", "table": "VoiceInfo", "row_id": 1})
    assert result["matched"] and result["message_ref"] == reference
    return source, reference


def test_note_components_complete_ordered_and_not_forward_or_group_records(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        long_text = "完整笔记" * 4000
        reference = commit(store, source, [raw(local_type=49, parsed=note(long_text))])["message_refs"][0]
        page = store.read_card(reference, limit=1)
        assert page["card"]["kind"] == "note" and page["components"][0]["text"] == long_text
        assert page["has_more"]
        second = store.read_card(reference, limit=1, cursor=page["next_cursor"])
        third = store.read_card(reference, limit=1, cursor=second["next_cursor"])
        assert [page["components"][0]["datatype"], second["components"][0]["datatype"], third["components"][0]["datatype"]] == [1, 2, 8]
        assert third["has_more"] is False
        assert second["components"][0]["media_status"] == "unresolved"
        assert store.component_media_records(second["components"][0]["item_ref"])[0]["kind"] == "image"
        assert store.component_media_records(third["components"][0]["item_ref"])[0]["kind"] == "file"
        assert store.get_media_records(message_ref=reference) == []
        stats = store.get_group_stats(["test@chatroom"], 0, 100)
        assert stats["total_records"] == 1 and stats["active_senders"] == 1 and stats["forwarded_items"] == 0
        record = store.get_message_context(reference)["message"]
        assert record["card_component_count"] == 3 and record["forwarded_count"] == 0


def test_card_component_nested_refs_survive_reparse_and_parent_ack_merge(tmp_path):
    parsed = note(children=[{"datatype": 17, "text": "nested", "children": [{"datatype": 1, "text": "nested child"}]}])
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        old = commit(store, source, [raw(server_id="", parsed=parsed)])["message_refs"][0]
        old_item = store.read_card(old)["components"][0]["item_ref"]
        old_child = store.read_card(old_item)["components"][0]["item_ref"]
        other = setup_source(store, "db_storage/message/message_1.db")
        canonical = commit(store, other, [raw(server_id="ack", parsed=parsed)])["message_refs"][0]
        commit(store, source, [raw(server_id="ack", parsed=parsed)])
        assert store.read_card(old_item)["message_ref"] == canonical
        assert store.read_card(old_child)["item"]["text"] == "nested child"
        assert store.get_status()["messages"] == 1
        first_ref = store.read_card(canonical)["components"][0]["item_ref"]
        commit(store, other, [raw(server_id="ack", parsed=parsed)])
        assert store.read_card(canonical)["components"][0]["item_ref"] == first_ref


def test_card_cursor_scope_and_revision(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        refs = commit(store, source, [raw(1, "one", parsed=note()), raw(2, "two", parsed=note())])["message_refs"]
        token = store.read_card(refs[0], limit=1)["next_cursor"]
        with pytest.raises(ArchiveError, match="cursor_scope_changed"):
            store.read_card(refs[1], cursor=token)
        commit(store, source, [raw(1, "one", parsed=note("changed"))])
        with pytest.raises(ArchiveError, match="cursor_snapshot_changed"):
            store.read_card(refs[0], cursor=token)


def test_replaced_component_media_never_resolves_previous_ready_attachment(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        reference = commit(store, source, [raw(local_type=49, parsed=note())])["message_refs"][0]
        item_ref = store.read_card(reference)["components"][1]["item_ref"]
        old_media = store.component_media_reference(item_ref)
        store.update_media(old_media, status="ready")
        replacement = note()
        replacement["card_components"][1]["attachment"]["file_md5"] = "c" * 32
        commit(store, source, [raw(local_type=49, parsed=replacement)])
        current = store.component_media_reference(item_ref)
        assert current != old_media
        assert store.component_media_records(item_ref)[0]["status"] != "ready"
        assert store.read_card(reference)["components"][1]["media_status"] == "unresolved"
        replacement["card_components"][1].pop("attachment")
        commit(store, source, [raw(local_type=49, parsed=replacement)])
        assert store.component_media_records(item_ref) == []


def test_card_and_transcript_search_opt_in_with_original_group_time_scope(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        reference = commit(store, source, [raw(local_type=49, parsed=note("内嵌专用关键字"))])["message_refs"][0]
        assert not store.search_messages(["test@chatroom"], 0, 100, "专用关键字")["messages"]
        found = store.search_messages(["test@chatroom"], 0, 100, "专用关键字", include_card_text=True)
        assert found["messages"][0]["message_ref"] == reference
        assert found["messages"][0]["content_matches"][0]["source_kind"] == "card"
        assert found["messages"][0]["content_matches"][0]["source_ref"].startswith("item_")
        assert not store.search_messages(["test@chatroom"], 11, 100, "专用关键字", include_card_text=True)["messages"]
        assert store.search_messages(["test@chatroom"], 0, 100, "关键", include_card_text=True)["messages"]
        assert not store.search_messages(["test@chatroom"], 0, 100, '" OR 1=1', include_card_text=True)["messages"]


def test_native_text_preserves_message_identity_stats_progress_and_health_evidence(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source, reference = seed_voice(store)
        before = tuple(store.conn.execute("SELECT message_ref,archive_id,first_seen,updated_at,text FROM messages").fetchone())
        stream = [tuple(r) for r in store.conn.execute("SELECT * FROM streams")]
        count = store.get_status()["messages"]
        store.save_native_transcript(reference, "微信原生识别的文字", {"kind": "wechat_native", "build": "synthetic"})
        assert tuple(store.conn.execute("SELECT message_ref,archive_id,first_seen,updated_at,text FROM messages").fetchone()) == before
        assert [tuple(r) for r in store.conn.execute("SELECT * FROM streams")] == stream
        assert store.get_status()["messages"] == count
        assert store.get_voice(reference)["native_text"] == "微信原生识别的文字"
        assert store.get_voice(reference)["transcript"]["provenance"]["kind"] == "wechat_native"
        assert not store.search_messages(["test@chatroom"], 0, 100, "原生识别")["messages"]
        found = store.search_messages(["test@chatroom"], 0, 100, "原生识别", include_transcripts=True)
        assert found["messages"][0]["message_ref"] == reference
        assert found["messages"][0]["content_matches"][0]["source_kind"] == "transcript"
        assert store.get_status()["health"]["message_reception_confirmed"] is False


def test_native_content_revision_does_not_invalidate_plain_message_cursor(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source, reference = seed_voice(store)
        commit(store, source, [raw(2, "two", parsed=voice(), local_type=34)])
        plain = store.read_messages(["test@chatroom"], 0, 100, limit=1)
        derived = store.read_messages(["test@chatroom"], 0, 100, limit=1, include_transcripts=True)
        store.save_native_transcript(reference, "first result", {"kind": "wechat_native"})
        assert store.read_messages(["test@chatroom"], 0, 100, cursor=plain["next_cursor"])["messages"]
        with pytest.raises(ArchiveError, match="cursor_snapshot_changed"):
            store.read_messages(["test@chatroom"], 0, 100, cursor=derived["next_cursor"], include_transcripts=True)


def test_native_result_does_not_invalidate_unrelated_card_pagination(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source, voice_ref = seed_voice(store)
        note_ref = commit(store, source, [raw(2, "note", parsed=note())])["message_refs"][0]
        first = store.read_card(note_ref, limit=1)
        store.save_native_transcript(voice_ref, "new text", {"kind": "wechat_native"})
        assert store.read_card(note_ref, cursor=first["next_cursor"], limit=1)["components"][0]["datatype"] == 2


def test_voice_binding_requires_account_group_and_server_id(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, reference = seed_voice(store)
        b = blob(store)
        for account, group, sid in [("wrong", "test@chatroom", "v1"), ("account", "wrong@chatroom", "v1"), ("account", "test@chatroom", "wrong")]:
            assert store.upsert_voice(account, group, sid, b, {})["matched"] is False
        result = store.get_voice(reference)
        assert result["audio_status"] == "archived" and result["audio"]["codec"] == "silk_v3"
        public = json.dumps(result)
        assert "raw_path" not in public and "raw_sha256" not in public


def test_voice_versions_and_transcripts_remain_after_source_cache_absence(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, reference = seed_voice(store)
        store.upsert_voice("account", "test@chatroom", "v1", blob(store, b"#!SILK_V3other"), {"row_id": 2})
        assert store.conn.execute("SELECT COUNT(*) FROM voice_assets").fetchone()[0] == 2
        store.save_native_transcript(reference, "first", {"kind": "wechat_native"})
        store.save_native_transcript(reference, "second", {"kind": "wechat_native"})
        assert store.conn.execute("SELECT COUNT(*) FROM native_transcripts").fetchone()[0] == 2
    with ArchiveStore(tmp_path, readonly=True) as store:
        result = store.get_voice(reference)
        assert result["audio_status"] == "archived" and result["native_text"] == "second"


@pytest.mark.parametrize("damage", ["missing", "corrupt", "size_metadata", "oversized"])
def test_voice_reports_unavailable_raw_blob_and_refuses_new_transcription(tmp_path, damage):
    with ArchiveStore(tmp_path) as store:
        _, reference = seed_voice(store)
        store.set_meta("native_transcription_capability", {"enabled": True, "verified": True})
        asset = store.conn.execute("SELECT * FROM voice_assets").fetchone()
        data = json.loads(asset["blob"])
        path = store.media_dir / data["raw_path"]
        if damage == "missing":
            path.unlink()
        elif damage == "corrupt":
            path.write_bytes(b"wrong" * 5)
        elif damage == "oversized":
            with path.open("r+b") as handle:
                handle.truncate(8 * 1024 * 1024 + 1)
        else:
            data["raw_size"] += 1
            store.conn.execute("UPDATE voice_assets SET blob=?", (json.dumps(data),))
            store.conn.commit()
        voice_result = store.get_voice(reference)
        assert voice_result["audio_status"] == ("missing" if damage == "missing" else "corrupt")
        assert voice_result["status"] != "ready" and voice_result["audio_reason"]
        assert store.request_voice_transcription(reference)["status"] == "not_archived"
        assert store.conn.execute("SELECT COUNT(*) FROM native_voice_jobs").fetchone()[0] == 0
        store.save_native_transcript(reference, "Previously archived native text", {"kind": "wechat_native"})
        assert store.request_voice_transcription(reference)["status"] == "ready"
        assert store.get_voice(reference)["native_text"] == "Previously archived native text"
        assert store.get_voice(reference)["audio_status"] != "archived"


def test_voice_deleted_after_queue_is_not_claimed_for_ui_conversion(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, reference = seed_voice(store)
        store.set_meta("native_transcription_capability", {"enabled": True, "verified": True})
        store.request_voice_transcription(reference)
        asset = store.conn.execute("SELECT blob FROM voice_assets").fetchone()
        (store.media_dir / json.loads(asset[0])["raw_path"]).unlink()
        assert store.claim_voice_transcription_job() is None
        assert store.get_voice(reference)["task"]["status"] == "failed"
        assert store.get_voice(reference)["task"]["reason"] == "archive_blob_missing"


def test_native_queue_disabled_until_verified_and_reuses_task_or_text(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, reference = seed_voice(store)
        assert store.request_voice_transcription(reference)["status"] == "unavailable"
        store.set_meta("native_transcription_capability", {"enabled": True})
        assert store.request_voice_transcription(reference)["status"] == "unavailable"
        assert store.claim_voice_transcription_job() is None
        store.set_meta("native_transcription_capability", {"enabled": True, "verified": True})
        first = store.request_voice_transcription(reference)
        assert first["status"] == "queued"
        assert store.request_voice_transcription(reference)["job_id"] == first["job_id"]
        claimed = store.claim_voice_transcription_job("synthetic-worker")
        assert claimed["job_id"] == first["job_id"] and claimed["voice_length_ms"] == 5000
        assert claimed["group_name"] == "测试" and claimed["server_id"] == "v1"
        assert store.claim_voice_transcription_job("other-worker") is None
        store.save_native_transcript(reference, "native result", {"kind": "wechat_native"}, job_id=first["job_id"])
        assert store.get_voice(reference)["task"]["status"] == "succeeded"
        assert store.request_voice_transcription(reference)["status"] == "ready"
        assert store.conn.execute("SELECT COUNT(*) FROM native_voice_jobs").fetchone()[0] == 1


def test_native_queue_single_concurrency_timeout_and_revoked_capability(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, first = seed_voice(store)
        _, second = seed_voice(store, 2, "v2")
        store.set_meta("native_transcription_capability", {"enabled": True, "verified": True})
        j1 = store.request_voice_transcription(first)
        j2 = store.request_voice_transcription(second)
        assert store.claim_voice_transcription_job()["job_id"] == j1["job_id"]
        with store.conn:
            store.conn.execute("UPDATE native_voice_jobs SET lease_until=1 WHERE job_id=?", (j1["job_id"],))
        assert store.claim_voice_transcription_job()["job_id"] == j2["job_id"]
        assert store.get_voice(first)["task"]["status"] == "timeout"
        store.set_meta("native_transcription_capability", {"enabled": False, "verified": True})
        assert store.claim_voice_transcription_job() is None
        store.update_voice_transcription_job(j2["job_id"], "failed", "native_ui_unavailable")
        assert store.get_voice(second)["task"]["reason"] == "native_ui_unavailable"


def test_profiles_group_member_avatar_and_versions_without_message_rows(tmp_path):
    with ArchiveStore(tmp_path) as store:
        setup_source(store)
        store.update_group_directory("account", [{"group_id": "test@chatroom", "name": "测试", "owner_id": "person",
            "members": [{"sender_id": "person", "nickname": "Contact", "group_nickname": "Room alias"}],
            "coverage": {"status": "available"}}])
        output = io.BytesIO(); Image.new("RGB", (4, 5), "blue").save(output, format="PNG")
        b = blob(store, output.getvalue())
        avatar = {"relative_path": b["raw_path"], "sha256": b["raw_sha256"], "mime": "image/png", "width": 4, "height": 5}
        profiles = [{"subject_id": "person", "profile": {"nickname": "Contact", "remark": "Remark"}, "avatar": avatar},
                    {"subject_id": "test@chatroom", "profile": {"nickname": "Group"}, "avatar": avatar}]
        assert store.upsert_profiles("account", profiles) == {"profiles": 2, "avatars": 2}
        member = store.get_profile("test@chatroom", "person", True)
        assert member["profile"]["group_nickname"] == "Room alias"
        assert member["avatar"]["status"] == "archived"
        assert store.get_profile_avatar(member["avatar"]["avatar_ref"])["data"] == output.getvalue()
        assert store.get_profile("test@chatroom")["kind"] == "group"
        assert store.get_status()["messages"] == 0
        store.upsert_profiles("account", profiles)
        assert store.conn.execute("SELECT COUNT(*) FROM profile_versions").fetchone()[0] == 2
        store.upsert_profiles("account", [{"subject_id": "person", "profile": {"nickname": "New name"}}])
        assert store.conn.execute("SELECT COUNT(*) FROM profile_versions").fetchone()[0] == 3
        assert store.get_profile("test@chatroom", "person")["avatar"]["status"] == "archived"
        with pytest.raises(ArchiveError, match="member_not_observed"):
            store.get_profile("test@chatroom", "unobserved")
        assert "relative_path" not in json.dumps(member)


def test_profile_avatar_corruption_is_reported(tmp_path):
    with ArchiveStore(tmp_path) as store:
        setup_source(store)
        b = blob(store, b"image bytes")
        store.upsert_profiles("account", [{"subject_id": "test@chatroom", "profile": {}, "avatar": {
            "relative_path": b["raw_path"], "sha256": b["raw_sha256"], "mime": "image/png", "width": 1, "height": 1}}])
        profile = store.get_profile("test@chatroom")
        (store.media_dir / b["raw_path"]).write_bytes(b"wrong")
        with pytest.raises(MediaUnavailable, match="archive_blob_corrupt"):
            store.get_profile_avatar(profile["avatar"]["avatar_ref"])


def test_public_web_links_allowed_only_for_typed_safe_field(tmp_path):
    good = "https://example.com/article?a=1&b=2"
    result = _safe({"public_web_url": good, "cdnurl": "https://private.example/aes", "avatar_url": "private", "aes_key": "secret", "voice_data": b"private", "raw_path": "/private/path"})
    assert result == {"public_web_url": good}
    for bad in ["javascript:alert(1)", "https://user:pass@example.com", "https://example.com/?access_token=private", "https://example.com/?aeskey=private"]:
        assert "public_web_url" not in _safe({"public_web_url": bad})


def test_card_transaction_failure_rolls_back_all_components_and_media(tmp_path):
    with ArchiveStore(tmp_path) as store:
        source = setup_source(store)
        store.conn.executescript("CREATE TRIGGER fail_p2 BEFORE INSERT ON card_components BEGIN SELECT RAISE(ABORT,'synthetic'); END;")
        with pytest.raises(sqlite3.IntegrityError):
            commit(store, source, [raw(parsed=note())])
        assert store.get_status()["messages"] == 0
        assert store.p2_status()["card_components"] == 0
        assert store.conn.execute("SELECT COUNT(*) FROM media").fetchone()[0] == 0


def test_readonly_p2_queries_do_not_write_and_queue_rejects(tmp_path):
    with ArchiveStore(tmp_path) as store:
        _, reference = seed_voice(store)
    with ArchiveStore(tmp_path, readonly=True) as store:
        before = store.conn.serialize()
        assert store.get_voice(reference)["audio_status"] == "archived"
        assert store.p2_status()["voices_archived"] == 1
        assert store.get_profile("test@chatroom")["avatar"]["status"] == "not_cached"
        assert store.conn.serialize() == before
        with pytest.raises(ArchiveError, match="read_only_archive"):
            store.request_voice_transcription(reference)
