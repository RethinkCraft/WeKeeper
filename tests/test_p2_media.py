"""P2 cached media proofs use synthetic local bytes, never a running client."""
import hashlib
import io
import json
import shutil

from PIL import Image
import pytest

from chatlog_keeper.archive_media import MediaArchiver, MediaService, MediaUnavailable
from chatlog_keeper.core._secrets import write_secret_text
from test_archive_media import FakeStore, STAMP, encrypted, image_bytes


@pytest.fixture
def local(tmp_path):
    data_root = tmp_path / "wechat"
    account = data_root / "wxid_fixture_abcd"
    images = account / "msg/attach/group/2026-10/Img"
    images.mkdir(parents=True)
    store = FakeStore(tmp_path / "state")
    assert write_secret_text(store.state_dir / "secrets/wechat_v2_uin.txt", "256")
    archiver = MediaArchiver(store, data_root, account)
    return store, account, images, archiver


def seed(store, *, kind="image", attachment=None, **extra):
    record = {"media_ref": "media_p2", "message_ref": "msg_p2", "kind": kind,
              "account_id": "wxid_fixture_abcd", "create_time": STAMP,
              "status": "pending", "attachment": attachment or {"kind": kind}, **extra}
    store.rows[record["media_ref"]] = record
    return record


def content_md5(data):
    return hashlib.md5(data).hexdigest()


@pytest.mark.parametrize("reference", ["component_ref", "forward_ref"])
def test_linked_image_requires_content_hash_and_survives_offline_cleanup(local, reference):
    store, account, images, archiver = local
    pixels = image_bytes()
    record = seed(store, attachment={"kind": "image", "file_md5": content_md5(pixels)},
                  **{reference: "item_fixture" if reference == "component_ref" else "fwd_fixture"})
    (images / "unrelated-cache-filename.dat").write_bytes(pixels)
    archiver.process_pending()
    assert record["status"] == "ready" and record["association_evidence"] == "decoded_content_md5"
    assert record["original_proven"] is False
    shutil.rmtree(account)
    shutil.rmtree(store.state_dir / "secrets")
    archiver.process_pending()
    result = MediaService(store).get_media("media_p2")
    assert result["data"] == pixels and result["mime_type"] == "image/png"
    assert record["source_status"] == "archived_copy_verified"
    assert str(account) not in json.dumps(result["metadata"])


@pytest.mark.parametrize("is_encrypted", [False, True])
def test_linked_image_matching_filename_is_never_link_evidence(local, is_encrypted):
    store, _, images, archiver = local
    claimed = "a" * 32
    pixels = image_bytes()
    (images / f"{claimed}.dat").write_bytes(encrypted(pixels) if is_encrypted else pixels)
    record = seed(store, component_ref="item_fixture",
                  attachment={"kind": "image", "file_md5": claimed})
    archiver.process_pending()
    assert record["status"] == "unresolved"
    assert record["reason"] == "image_content_link_unverified"
    assert not list(store.media_dir.rglob("*.bin"))
    with pytest.raises(MediaUnavailable):
        MediaService(store).get_media("media_p2")


def test_linked_encrypted_image_needs_decoded_content_proof(local):
    store, _, images, archiver = local
    pixels = image_bytes(fmt="JPEG")
    digest = content_md5(pixels)
    (images / f"{digest}_h.dat").write_bytes(encrypted(pixels))
    record = seed(store, forward_ref="fwd_fixture",
                  attachment={"kind": "image", "file_md5": digest})
    archiver.process_pending()
    assert record["status"] == "ready" and record["content_md5"] == digest
    assert record["encryption_format"] == "v2" and record["material_status"] == "preserved"
    assert MediaService(store).get_media("media_p2")["data"] == pixels


def test_thumbnail_hash_is_explicit_and_cannot_satisfy_full_quality(local):
    store, _, images, archiver = local
    pixels = image_bytes((16, 8))
    (images / "thumbnail-without-t-suffix.dat").write_bytes(pixels)
    record = seed(store, component_ref="item_fixture", attachment={
        "kind": "image", "file_md5": "a" * 32, "thumbnail_md5": content_md5(pixels)})
    archiver.process_pending()
    assert record["status"] == "ready" and record["tier"] == "thumbnail"
    assert MediaService(store).get_media("media_p2", "thumbnail")["data"] == pixels
    with pytest.raises(MediaUnavailable, match="requested_media_quality_unavailable"):
        MediaService(store).get_media("media_p2", "full")


@pytest.mark.parametrize("new_hash", ["f" * 32, ""])
def test_component_change_cannot_serve_an_older_ready_image(local, new_hash):
    store, _, images, archiver = local
    pixels = image_bytes()
    (images / "cache.dat").write_bytes(pixels)
    record = seed(store, component_ref="item_fixture", attachment={
        "kind": "image", "file_md5": content_md5(pixels)})
    archiver.process_pending()
    assert record["status"] == "ready"
    record["attachment"]["file_md5"] = new_hash
    archiver.process_pending()
    assert record["status"] == "unresolved" and record["variants"] == []
    with pytest.raises(MediaUnavailable):
        MediaService(store).get_media("media_p2")


def animation_bytes(fmt):
    output = io.BytesIO()
    frames = [Image.new("RGB", (12, 8), color) for color in ("red", "blue")]
    frames[0].save(output, format=fmt, save_all=True, append_images=frames[1:],
                   duration=[100, 100], loop=0, lossless=True)
    return output.getvalue()


@pytest.mark.parametrize("fmt", ["GIF", "WEBP"])
def test_sticker_returns_first_frame_and_keeps_original_animation(local, fmt):
    store, account, _, archiver = local
    pixels = animation_bytes(fmt)
    cached = account / "msg/emoticon/nested"
    cached.mkdir(parents=True)
    (cached / "message-cache-object.bin").write_bytes(pixels)
    record = seed(store, kind="sticker", attachment={"kind": "sticker", "md5": content_md5(pixels)})
    archiver.process_pending()
    assert record["status"] == "ready" and record["animated"] is True
    assert record["frame_count"] == 2 and record["selected_frame"] == 0
    assert (store.media_dir / record["original_decoded_path"]).read_bytes() == pixels
    assert (store.media_dir / record["raw_path"]).read_bytes() == pixels
    result = MediaService(store).get_media("media_p2")
    assert result["mime_type"] == "image/png" and result["data"].startswith(b"\x89PNG")
    with Image.open(io.BytesIO(result["data"])) as image:
        assert image.getpixel((0, 0))[:3] == (255, 0, 0)
    assert result["metadata"]["animated"] and result["metadata"]["original_animation_preserved"]
    assert "raw_path" not in json.dumps(result["metadata"])


def test_numbered_system_emoji_assets_are_never_message_stickers(local):
    store, account, _, archiver = local
    pixels = image_bytes(fmt="WEBP")
    resources = account / "resource/emoticon_webp"
    resources.mkdir(parents=True)
    (resources / "1.webp").write_bytes(pixels)
    record = seed(store, kind="sticker", attachment={"kind": "sticker", "md5": content_md5(pixels)})
    archiver.process_pending()
    assert record["status"] == "unresolved"
    assert record["reason"] == "sticker_content_link_unverified"
    assert not list(store.media_dir.rglob("*.bin"))


def test_symlinked_linked_cache_candidate_is_ignored(local, tmp_path):
    store, _, images, archiver = local
    pixels = image_bytes()
    outside = tmp_path / "outside.png"
    outside.write_bytes(pixels)
    (images / "cache.dat").symlink_to(outside)
    record = seed(store, component_ref="item_fixture", attachment={
        "kind": "image", "file_md5": content_md5(pixels)})
    archiver.process_pending()
    assert record["status"] == "unresolved" and not list(store.media_dir.rglob("*.bin"))


def test_authenticated_voice_stays_ready_and_corruption_is_explicit(local):
    store, _, _, archiver = local
    voice = b"\x02#!SILK_V3synthetic audio body"
    path, digest = archiver._put_bytes("raw", voice)
    record = seed(store, kind="voice", voice_asset_ref="voice_fixture", raw_path=path,
                  raw_sha256=digest, raw_size=len(voice), status="ready")
    archiver.process_pending()
    assert record["status"] == "ready" and record["codec"] == "silk_v3"
    assert record["source_status"] == "authenticated_media_database"
    (store.media_dir / path).write_bytes(b"corrupt")
    archiver.process_pending()
    assert record["status"] == "failed" and record["reason"] == "archive_blob_corrupt"
    assert record["voice_asset_ref"] == "voice_fixture"


def test_unassociated_voice_is_still_explicitly_unsupported(local):
    store, _, _, archiver = local
    record = seed(store, kind="voice")
    archiver.process_pending()
    assert record["status"] == "unsupported"


def test_ordinary_packed_cache_id_still_returns_exact_large_jpeg(local):
    store, _, images, archiver = local
    pixels = image_bytes((1080, 1920), "JPEG")
    cache_id = "a" * 32
    assert content_md5(pixels) != cache_id
    (images / f"{cache_id}.dat").write_bytes(encrypted(pixels))
    record = seed(store, attachment={"kind": "image", "file_md5": cache_id})
    archiver.process_pending()
    result = MediaService(store).get_media("media_p2")
    assert record["status"] == "ready" and result["data"] == pixels
    assert result["metadata"]["width"] == 1080 and result["metadata"]["height"] == 1920
