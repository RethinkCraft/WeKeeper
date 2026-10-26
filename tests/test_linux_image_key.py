"""Synthetic MMKV fixtures: no real clients, account state, or processes."""
import hashlib
import json
from pathlib import Path
import struct
import zlib

from Crypto.Cipher import AES
import pytest

from chatlog_keeper import image_key


def varint(value):
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def string(value):
    raw = value.encode()
    return varint(len(raw)) + raw


def entries(values):
    return b"\0" + b"".join(varint(len(k)) + k + varint(len(v)) + v for k, v in values)


@pytest.fixture
def config(tmp_path, monkeypatch):
    root = tmp_path / "xwechat_files"
    (root / "wxid_synthetic_abcd" / "db_storage").mkdir(parents=True)
    directory = root / "all_users" / "config"
    directory.mkdir(parents=True)
    binary = tmp_path / "official-test-elf"
    constant = b"test-mmkv-key-value"
    image = b"\x7fELF" + b"\0" * 60 + constant + b"\0"
    binary.write_bytes(image)
    monkeypatch.setattr(image_key, "ELF_SHA256", hashlib.sha256(image).hexdigest())
    monkeypatch.setattr(image_key, "CRYPT_CONSTANT_OFFSET", 64)
    monkeypatch.setattr(image_key, "official_wechat_executables", lambda: [binary])
    key = constant[:16]
    state = tmp_path / "state"

    def write(*, name="wxid_synthetic", disagree=False, missing=False):
        values = [
            (b"unrelated_private_field", b"UNRELATED SECRET MUST NOT ESCAPE"),
            (image_key.UIN_KEY + b"_suffix", varint(999)),
            (image_key.UIN_KEY, varint(1)),
            (image_key.UIN_KEY, varint(257)),
            (image_key.NAME_KEYS[0], string(name)),
        ]
        if not missing:
            values.append((image_key.NAME_KEYS[1], string("another_account" if disagree else name)))
        plain = entries(values)
        iv = bytes(range(16))
        encrypted = AES.new(key, AES.MODE_CFB, iv=iv, segment_size=128).encrypt(plain)
        (directory / "global_config").write_bytes(struct.pack("<I", len(encrypted)) + encrypted + b"\0" * 32)
        meta = bytearray(32)
        struct.pack_into("<II", meta, 0, zlib.crc32(encrypted), 4)
        meta[12:28] = iv
        struct.pack_into("<I", meta, 28, len(encrypted))
        (directory / "global_config.crc").write_bytes(meta)
    write()
    return root, binary, state, directory, write


def test_reviewed_mmkv_exact_fields_account_binding_and_private_cache(config):
    root, binary, state, directory, write = config
    result = image_key.discover_uin(root, state=state)
    assert result["found"] is True and result["image_key_validated"] is False
    assert image_key.load_cached_uin(root / "wxid_synthetic_abcd", state=state) == 257
    assert image_key.load_cached_uin(root / "another_account", state=state) is None
    rendered = json.dumps(result)
    for secret in ("257", "wxid_synthetic", "UNRELATED SECRET", "test-mmkv-key"):
        assert secret not in rendered
    assert (state / "secrets").stat().st_mode & 0o777 == 0o700
    assert all(p.stat().st_mode & 0o777 == 0o600 for p in (state / "secrets").iterdir())


@pytest.mark.parametrize("mutation,reason", [
    ("crc", "global_config_crc_mismatch"),
    ("disagree", "account_names_disagree"),
    ("missing", "required_account_fields_missing"),
    ("mismatch", "account_match_failed"),
])
def test_rejects_corrupt_or_unmatched_state_before_cache_write(config, mutation, reason):
    root, binary, state, directory, write = config
    if mutation == "crc":
        raw = bytearray((directory / "global_config").read_bytes())
        raw[8] ^= 1
        (directory / "global_config").write_bytes(raw)
    else:
        write(disagree=mutation == "disagree", missing=mutation == "missing", name="unknown" if mutation == "mismatch" else "wxid_synthetic")
    assert image_key.discover_uin(root, binary, state) == {"found": False, "error": reason}
    assert not (state / "secrets" / "wechat_v2_uin.txt").exists()


def test_unknown_elf_never_reads_account_configuration(tmp_path, monkeypatch):
    binary = tmp_path / "unknown"
    binary.write_bytes(b"unreviewed-build")
    monkeypatch.setattr(image_key, "_snapshot_global_config", lambda _: pytest.fail("must fail before MMKV read"))
    result = image_key.discover_uin(tmp_path, binary, tmp_path / "state")
    assert result == {"found": False, "error": "unreviewed_wechat_build"}


def test_explicit_account_mismatch_never_writes_latest_login_cache(config):
    root, binary, state, directory, write = config
    result = image_key.discover_uin(root, binary, state, expected_account="different_selected_account")
    assert result == {"found": False, "error": "account_mismatch"}
    assert not (state / "secrets").exists()


def test_deleted_required_key_is_not_revived_by_previous_record():
    plain = entries([(image_key.UIN_KEY, varint(257)), (image_key.UIN_KEY, b"")])
    with pytest.raises(image_key.ImageKeyDiscoveryError, match="required_account_fields_missing"):
        image_key.exact_account_fields(plain)


def test_configuration_symlink_is_rejected(config, tmp_path):
    root, binary, state, directory, write = config
    source = directory / "global_config"
    moved = tmp_path / "outside-config"
    source.rename(moved)
    source.symlink_to(moved)
    assert image_key.discover_uin(root, binary, state) == {"found": False, "error": "configuration_read_failed"}
