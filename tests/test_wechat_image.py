"""Real local codecs over synthetic images; no user data or process scans."""
import io
from pathlib import Path
import struct
import sys
from types import SimpleNamespace

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from PIL import Image
import pytest

from chatlog_keeper import wechat_image as media
from chatlog_keeper.core._secrets import write_secret_text


def png():
    out = io.BytesIO()
    Image.new("RGB", (64, 32), (50, 120, 210)).save(out, format="PNG")
    return out.getvalue()


def packed(raw, *, sig, key, xor):
    aes_size, xor_size = 32, 4
    header = sig + struct.pack("<LL", aes_size, xor_size) + b"\0"
    return (header + AES.new(key, AES.MODE_ECB).encrypt(pad(raw[:aes_size], 16))
            + raw[aes_size:-xor_size] + bytes(value ^ xor for value in raw[-xor_size:]))


@pytest.fixture(autouse=True)
def private_state(tmp_path, monkeypatch):
    monkeypatch.setenv("CHATLOG_KEEPER_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setattr(media, "_V2_KEY_CACHE", None)
    monkeypatch.setattr(media, "_V2_XOR_KEY_OVERRIDE", None)
    return tmp_path / "state"


def test_legacy_xor_and_v1_decode_real_png(tmp_path):
    raw = png()
    legacy = tmp_path / "legacy.dat"
    legacy.write_bytes(bytes(value ^ 79 for value in raw))
    assert media.decode_image(legacy) == raw
    v1 = tmp_path / "v1.dat"
    v1.write_bytes(packed(raw, sig=media._V1_SIG, key=media._V1_FIXED_AES_KEY, xor=0x88))
    assert media.decode_image(v1) == raw


@pytest.mark.parametrize("uin", [256, 4294967295])
def test_v2_canonical_layout_account_normalization_and_xor_zero(tmp_path, private_state, uin):
    raw = png()
    path = tmp_path / "wxid_synthetic_abcd" / "msg" / "attach" / "chat" / "2026-10" / "Img" / "sample_h.dat"
    path.parent.mkdir(parents=True)
    key = media.derive_v2_key_from_uin(uin, "wxid_synthetic")
    path.write_bytes(packed(raw, sig=media._V2_SIG, key=key, xor=uin & 255))
    assert write_secret_text(media._V2_UIN_CACHE_PATH_FN(), str(uin))
    assert media._attach_root_for_image(path) == path.parents[3]
    assert media.derive_v2_xor_key(path.parents[3]) == (uin & 255)
    decoded = media.decode_image(path)
    assert decoded == raw
    with Image.open(io.BytesIO(decoded)) as im:
        im.load()
        assert im.size == (64, 32)


def test_wrong_uin_and_aes_cache_alone_cannot_guess_xor(tmp_path):
    path = tmp_path / "wxid_synthetic" / "sample.dat"
    path.parent.mkdir()
    key = media.derive_v2_key_from_uin(256, "wxid_synthetic")
    path.write_bytes(packed(png(), sig=media._V2_SIG, key=key, xor=0))
    assert write_secret_text(media._V2_UIN_CACHE_PATH_FN(), "999")
    assert media.save_cached_v2_key(key)
    assert media.decode_image(path, account_dir=path.parent) is None


def test_malformed_lengths_truncated_cipher_and_wrong_key_fail(tmp_path):
    raw = packed(png(), sig=media._V1_SIG, key=media._V1_FIXED_AES_KEY, xor=0x88)
    assert media._decrypt_v1v2(raw[:30], media._V1_FIXED_AES_KEY) is None
    assert media._decrypt_v1v2(raw, b"wrong-key-16bytes") is None
    broken = bytearray(raw)
    struct.pack_into("<I", broken, 10, len(raw) + 1)
    assert media._decrypt_v1v2(bytes(broken), media._V1_FIXED_AES_KEY) is None
    path = tmp_path / "unknown.dat"
    path.write_bytes(b"not-an-image")
    assert media.decode_image(path) is None


def test_image_key_cache_requires_private_permissions(tmp_path):
    key = b"0123456789abcdef"
    assert media.save_cached_v2_key(key)
    assert media.load_cached_v2_key() == key
    path = media._V2_KEY_CACHE_PATH_FN()
    assert path.parent.stat().st_mode & 0o777 == 0o700
    assert path.stat().st_mode & 0o777 == 0o600
    path.chmod(0o644)
    assert media.load_cached_v2_key() is None


def test_wxgf_uses_packaged_ffmpeg_when_path_has_none(monkeypatch):
    import shutil
    import subprocess
    out = io.BytesIO()
    Image.linear_gradient("L").resize((256, 256)).convert("RGB").save(out, format="JPEG", quality=95)
    jpeg = out.getvalue()
    assert len(jpeg) > 1000
    monkeypatch.setattr(shutil, "which", lambda _: None)
    monkeypatch.setitem(sys.modules, "imageio_ffmpeg", SimpleNamespace(get_ffmpeg_exe=lambda: "/synthetic/ffmpeg"))
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=0, stdout=jpeg, stderr=b"")
    monkeypatch.setattr(subprocess, "run", run)
    assert media.wxgf_to_jpeg(b"wxgfmetadata\x00\x00\x00\x01bitstream") == jpeg
    assert calls[0][0] == "/synthetic/ffmpeg"
