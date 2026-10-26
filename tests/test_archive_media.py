"""Synthetic cached media, real V2 codec, and source-independent archive reads."""
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path
import shutil
import struct
from zoneinfo import ZoneInfo

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from PIL import Image
import pytest

from chatlog_keeper import wechat_image
from chatlog_keeper import archive_media
from chatlog_keeper.archive_media import MediaArchiver, MediaService, MediaUnavailable
from chatlog_keeper.core._secrets import write_secret_text

STAMP = int(datetime(2026, 10, 24, 12, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp())
LOCAL_ID = "a" * 32


class FakeStore:
    def __init__(self, state_dir):
        self.state_dir = state_dir
        self.media_dir = state_dir / "archive/media"
        self.rows = {}
        self.updated = []

    def pending_media(self, limit):
        return list(self.rows.values())[:limit]

    def update_media(self, ref, **fields):
        self.rows[ref].update(fields)
        self.updated.append(ref)

    def get_media_records(self, media_ref=None, message_ref=None):
        return [dict(row) for row in self.rows.values()
                if (media_ref is None or row["media_ref"] == media_ref)
                and (message_ref is None or row["message_ref"] == message_ref)]


def image_bytes(size=(64, 32), fmt="PNG"):
    out = io.BytesIO()
    Image.new("RGB", size, (31, 121, 199)).save(out, format=fmt)
    return out.getvalue()


def encrypted(raw, uin=256, account="wxid_fixture"):
    key = wechat_image.derive_v2_key_from_uin(uin, account)
    prefix, xor_size = 32, 4
    return (wechat_image._V2_SIG + struct.pack("<LL", prefix, xor_size) + b"\0"
            + AES.new(key, AES.MODE_ECB).encrypt(pad(raw[:prefix], 16))
            + raw[prefix:-xor_size]
            + bytes(value ^ (uin & 255) for value in raw[-xor_size:]))


@pytest.fixture
def setup(tmp_path):
    data_root = tmp_path / "wechat"
    account = data_root / "wxid_fixture_abcd"
    directory = account / "msg/attach/group/2026-10/Img"
    directory.mkdir(parents=True)
    store = FakeStore(tmp_path / "state")
    assert write_secret_text(store.state_dir / "secrets/wechat_v2_uin.txt", "256")
    store.rows["media_fixture"] = {
        "media_ref": "media_fixture", "message_ref": "msg_fixture",
        "account_id": account.name, "group_id": "fixture@chatroom",
        "create_time": STAMP, "kind": "image", "status": "pending",
        "attachment": {"kind": "image", "file_md5": LOCAL_ID, "aeskey": "never-public"},
    }
    return store, account, directory, MediaArchiver(store, data_root, account)


def test_v2_full_and_thumbnail_archive_and_source_removal(setup):
    store, account, directory, archiver = setup
    thumb, full = image_bytes((16, 8)), image_bytes()
    (directory / f"{LOCAL_ID}_t.dat").write_bytes(encrypted(thumb))
    (directory / f"{LOCAL_ID}.dat").write_bytes(encrypted(full))
    result = archiver.process_pending()
    row = store.rows["media_fixture"]
    assert result["ready"] == 1
    assert len(row["variants"]) == 2
    assert (row["width"], row["height"], row["tier"]) == (64, 32, "full_cache")
    assert row["encryption_format"] == "v2"
    assert row["original_proven"] is False
    assert (store.media_dir / row["raw_path"]).read_bytes() == encrypted(full)
    shutil.rmtree(account)
    shutil.rmtree(store.state_dir / "secrets")
    service = MediaService(store)
    assert service.get_media("media_fixture")["data"] == full
    assert service.get_media("msg_fixture", quality="thumbnail")["data"] == thumb
    public = json.dumps(service.get_media("media_fixture")["metadata"])
    assert "aeskey" not in public and str(account) not in public and "never-public" not in public
    with pytest.raises(MediaUnavailable, match="original_not_verified"):
        service.get_media("msg_fixture", quality="original")
    for path in store.media_dir.rglob("*"):
        assert path.stat().st_mode & 0o777 == (0o700 if path.is_dir() else 0o600)


def test_thumbnail_is_upgraded_and_unchanged_media_is_not_decoded_again(setup, monkeypatch):
    store, _, directory, archiver = setup
    (directory / f"{LOCAL_ID}_t.dat").write_bytes(encrypted(image_bytes((16, 8))))
    archiver.process_pending()
    assert store.rows["media_fixture"]["tier"] == "thumbnail"
    with pytest.raises(MediaUnavailable, match="requested_media_quality_unavailable"):
        MediaService(store).get_media("msg_fixture", quality="full")
    (directory / f"{LOCAL_ID}.dat").write_bytes(encrypted(image_bytes()))
    archiver.process_pending()
    assert store.rows["media_fixture"]["tier"] == "full_cache"
    count_before = len(list(store.media_dir.rglob("*.bin")))
    monkeypatch.setattr(archiver, "_decode", lambda *_args: pytest.fail("decoded unchanged cache again"))
    archiver.process_pending()
    assert len(list(store.media_dir.rglob("*.bin"))) == count_before
    assert len(store.updated) == 3  # Unchanged rows still advance queue fairness.


def test_failed_decode_preserves_raw_and_retries_when_material_appears(setup):
    store, _, directory, archiver = setup
    path = directory / f"{LOCAL_ID}.dat"
    path.write_bytes(encrypted(image_bytes()))
    (store.state_dir / "secrets/wechat_v2_uin.txt").unlink()
    archiver.process_pending()
    row = store.rows["media_fixture"]
    assert row["status"] == "failed"
    assert (store.media_dir / row["raw_path"]).read_bytes() == path.read_bytes()
    path.unlink()  # Retry the archived raw file, not a surviving source copy.
    assert write_secret_text(store.state_dir / "secrets/wechat_v2_uin.txt", "256")
    archiver.process_pending()
    assert store.rows["media_fixture"]["status"] == "ready"


def test_missing_or_unlinked_media_is_explicit_and_does_not_abort_queue(setup):
    store, _, _, archiver = setup
    store.rows["media_voice"] = {**store.rows["media_fixture"], "media_ref": "media_voice",
                                 "kind": "voice", "attachment": {"kind": "voice"}}
    store.rows["media_unlinked"] = {**store.rows["media_fixture"], "media_ref": "media_unlinked",
                                    "attachment": {"kind": "image", "md5": LOCAL_ID}}
    summary = archiver.process_pending()
    assert summary["processed"] == 3
    assert store.rows["media_fixture"]["status"] == "missing"
    assert store.rows["media_voice"]["status"] == "unsupported"
    assert store.rows["media_unlinked"]["reason"] == "image_link_unresolved"


def test_xml_md5_is_not_used_as_local_filename_and_symlinks_are_not_followed(setup, tmp_path):
    store, _, directory, archiver = setup
    other_id = "b" * 32
    store.rows["media_fixture"]["attachment"]["md5"] = other_id
    (directory / f"{other_id}.dat").write_bytes(encrypted(image_bytes()))
    outside = tmp_path / "outside.dat"
    outside.write_bytes(encrypted(image_bytes()))
    (directory / f"{LOCAL_ID}.dat").symlink_to(outside)
    archiver.process_pending()
    assert store.rows["media_fixture"]["status"] == "missing"
    assert not list(store.media_dir.rglob("*.bin"))


def test_wxgf_transcode_is_fully_loaded_and_recorded(setup, monkeypatch):
    store, _, directory, archiver = setup
    (directory / f"{LOCAL_ID}.dat").write_bytes(encrypted(b"wxgf" + b"metadata-and-stream" * 10))
    jpeg = image_bytes(fmt="JPEG")
    monkeypatch.setattr(wechat_image, "wxgf_to_jpeg", lambda _: jpeg)
    archiver.process_pending()
    row = store.rows["media_fixture"]
    assert row["status"] == "ready" and row["codec"] == "wxgf"
    assert row["mime"] == "image/jpeg" and row["full_image_load_verified"]
    assert MediaService(store).get_media("media_fixture")["data"] == jpeg


def test_service_rejects_traversal_and_corrupt_archive_blob(setup):
    store, _, directory, archiver = setup
    (directory / f"{LOCAL_ID}.dat").write_bytes(encrypted(image_bytes()))
    archiver.process_pending()
    service = MediaService(store)
    with pytest.raises(MediaUnavailable, match="invalid_media_reference"):
        service.get_media("../../secrets/key")
    row = store.rows["media_fixture"]
    blob = store.media_dir / row["decoded_path"]
    blob.write_bytes(b"tampered")
    with pytest.raises(MediaUnavailable, match="archive_blob_corrupt"):
        service.get_media("media_fixture")
    row["variants"][0]["decoded_path"] = "../../secrets/key"
    with pytest.raises(MediaUnavailable, match="invalid_archive_blob"):
        service.get_media("media_fixture")


def test_file_copy_requires_content_md5_and_remains_readable_offline(setup):
    store, account, _, archiver = setup
    payload = b"a small PDF-like archived file"
    path = account / "msg/file/2026-10/report.pdf"
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    store.rows.clear()
    store.rows["media_document"] = {
        "media_ref": "media_document", "message_ref": "msg_document", "account_id": account.name,
        "group_id": "fixture@chatroom", "create_time": STAMP, "kind": "file",
        "attachment": {"kind": "file", "filename": "report.pdf", "md5": hashlib.md5(payload).hexdigest()},
    }
    archiver.process_pending()
    assert store.rows["media_document"]["status"] == "ready"
    path.unlink()
    archived = MediaService(store).get_media("msg_document")
    assert archived["data"] == payload and archived["mime_type"] == "application/pdf"


def test_wrong_file_content_is_not_archived(setup):
    store, account, _, archiver = setup
    path = account / "msg/file/2026-10/report.pdf"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"wrong")
    row = store.rows["media_fixture"]
    row.update(kind="file", attachment={"kind": "file", "filename": "report.pdf", "md5": "c" * 32})
    archiver.process_pending()
    assert row["status"] == "unresolved"
    assert row["reason"] == "cached_file_md5_mismatch"
    assert not list(store.media_dir.rglob("*.bin"))


def test_real_archive_store_media_transaction_survives_source_and_key_cleanup(setup, monkeypatch):
    from chatlog_keeper.archive import ArchiveStore
    fake, account, directory, _ = setup
    payload = image_bytes()
    (directory / f"{LOCAL_ID}.dat").write_bytes(encrypted(payload))
    store = ArchiveStore(fake.state_dir)
    group = "fixture@chatroom"
    store.upsert_groups(account.name, [{"id": group, "name": "Fixture"}])
    source = store.register_source(account.name, "db_storage/message/message_0.db", {"inode": 1}, "fp")
    store.get_stream(source["source_ref"], source["generation"], "Msg_fixture", group, 1)
    raw = {
        "row_id": 1, "local_type": 3, "create_time": STAMP, "real_sender_id": 1,
        "sender_id": "wxid_synthetic_sender", "server_id": "1234567890123456789",
        "message_content": "<msg><img/></msg>", "packed_info_data": b"synthetic",
        "parsed": {"msg_type": 3, "text": "[图片]", "parse_status": "parsed",
                   "attachment": {"kind": "image", "file_md5": LOCAL_ID}},
    }
    result = store.commit_page(source["source_ref"], source["generation"], "Msg_fixture", group,
                               [raw], mode="backfill", checkpoint=1, complete=True)
    archiver = MediaArchiver(store, account.parent, account)
    with monkeypatch.context() as context:
        def failed_media(_record):
            raise OSError("synthetic codec failure")
        context.setattr(archiver, "_process_one", failed_media)
        assert archiver.process_pending()["failed"] == 1
    # The source-page commit remains durable across a failed media stage.
    assert store.conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 1
    assert store.get_stream(source["source_ref"], source["generation"], "Msg_fixture", group, 1)["backfill_rowid"] == 1
    assert archiver.process_pending()["ready"] == 1
    message_ref = result["message_refs"][0]
    store.close()
    shutil.rmtree(account)
    shutil.rmtree(fake.state_dir / "secrets")
    readonly = ArchiveStore(fake.state_dir, readonly=True)
    try:
        image = MediaService(readonly).get_media(message_ref)
        assert image["data"] == payload
        assert image["metadata"]["tier"] == "full_cache"
        assert image["metadata"]["original_proven"] is False
    finally:
        readonly.close()


def test_batch_budget_leaves_unstarted_items_and_checked_state_untouched(setup, monkeypatch):
    store, _, _, archiver = setup
    store.rows["media_second"] = {**store.rows["media_fixture"], "media_ref": "media_second"}
    store.rows["media_third"] = {**store.rows["media_fixture"], "media_ref": "media_third"}
    calls = []
    clock = iter([100.0, 100.0, 110.0])
    monkeypatch.setattr(archive_media.time, "monotonic", lambda: next(clock))

    def process(record):
        calls.append(record["media_ref"])
        return {"status": "ready", "reason": None}

    monkeypatch.setattr(archiver, "_process_one", process)
    result = archiver.process_pending(limit=100, time_budget=10.0)
    assert calls == ["media_fixture"]
    assert store.updated == ["media_fixture"]
    assert result["processed"] == 1 and result["budget_exhausted"] is True
    assert store.rows["media_second"]["status"] == "pending"
    assert store.rows["media_third"]["status"] == "pending"


def test_failed_item_finishes_before_budget_stops_next_item(setup, monkeypatch):
    store, _, _, archiver = setup
    store.rows["media_second"] = {**store.rows["media_fixture"], "media_ref": "media_second"}
    clock = iter([0.0, 0.0, 12.0])
    monkeypatch.setattr(archive_media.time, "monotonic", lambda: next(clock))

    def fail(_record):
        raise OSError("synthetic codec failure")

    monkeypatch.setattr(archiver, "_process_one", fail)
    result = archiver.process_pending(time_budget=10.0)
    assert result["processed"] == 1 and result["failed"] == 1
    assert result["budget_exhausted"] is True
    assert store.updated == ["media_fixture"]
    assert store.rows["media_second"]["status"] == "pending"


def test_zero_and_invalid_batch_budgets(setup):
    store, _, _, archiver = setup
    result = archiver.process_pending(time_budget=0)
    assert result["processed"] == 0 and result["budget_exhausted"] is True
    assert store.updated == []
    for invalid in [-1, float("nan"), float("inf"), True, "10"]:
        with pytest.raises(ValueError, match="time budget"):
            archiver.process_pending(time_budget=invalid)


def test_validated_material_is_private_immutable_and_survives_global_cache_replacement(setup):
    store, account, directory, archiver = setup
    payload = image_bytes()
    source = directory / f"{LOCAL_ID}.dat"
    source.write_bytes(encrypted(payload))
    archiver.process_pending()
    row = store.rows["media_fixture"]
    variant = row["variants"][0]
    material_ref = variant["material_ref"]
    material_dir = store.state_dir / "secrets/archive_media"
    material_path = material_dir / f"{material_ref}.json"
    original_bytes = material_path.read_bytes()
    original_identity = (material_path.stat().st_ino, material_path.stat().st_mtime_ns)
    saved = json.loads(original_bytes)
    assert saved["aes_key_hex"] == wechat_image.derive_v2_key_from_uin(256, "wxid_fixture").hex()
    assert saved["xor_key"] == 0
    assert saved["raw_sha256"] == variant["raw_sha256"]
    assert material_path.stat().st_mode & 0o777 == 0o600
    assert material_dir.stat().st_mode & 0o777 == 0o700
    assert variant["material_status"] == "preserved"

    # Replacing mutable global caches cannot destroy old media materials.
    assert write_secret_text(store.state_dir / "secrets/wechat_v2_uin.txt", "999")
    assert write_secret_text(store.state_dir / "secrets/wechat_v2_image_aes.key", "wrong-key-value!")
    archiver.process_pending()
    assert material_path.read_bytes() == original_bytes
    assert (material_path.stat().st_ino, material_path.stat().st_mtime_ns) == original_identity

    # Force future re-decoding from only the archived encrypted bytes.
    (store.media_dir / variant["decoded_path"]).unlink()
    shutil.rmtree(account)
    for key in ["decoded_path", "decoded_sha256", "mime", "width", "height"]:
        variant.pop(key, None)
    row["variants"] = [variant]
    archiver.process_pending()
    assert row["status"] == "ready"
    assert MediaService(store).get_media("media_fixture")["data"] == payload
    assert material_path.read_bytes() == original_bytes
    assert (material_path.stat().st_ino, material_path.stat().st_mtime_ns) == original_identity
    public = json.dumps(MediaService(store).get_media("media_fixture")["metadata"])
    assert "aes_key" not in public and "xor_key" not in public and "material_ref" not in public
    assert saved["aes_key_hex"] not in public


def test_ready_image_without_material_backfills_it_from_archived_raw(setup):
    store, account, directory, archiver = setup
    (directory / f"{LOCAL_ID}.dat").write_bytes(encrypted(image_bytes()))
    archiver.process_pending()
    row = store.rows["media_fixture"]
    material_path = store.state_dir / "secrets/archive_media" / f"{row['material_ref']}.json"
    material_path.unlink()
    for key in ["material_ref", "material_status"]:
        row.pop(key, None)
        row["variants"][0].pop(key, None)
    shutil.rmtree(account)
    archiver.process_pending()
    assert row["status"] == "ready" and row["material_status"] == "preserved"
    assert material_path.is_file()


def test_unverified_v2_material_is_not_retained(setup):
    store, _, directory, archiver = setup
    (directory / f"{LOCAL_ID}.dat").write_bytes(encrypted(image_bytes()))
    assert write_secret_text(store.state_dir / "secrets/wechat_v2_uin.txt", "999")
    archiver.process_pending()
    row = store.rows["media_fixture"]
    assert row["status"] == "failed"
    assert not row.get("material_ref")
    assert not list((store.state_dir / "secrets/archive_media").glob("*.json"))
