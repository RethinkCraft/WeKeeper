"""Read the reviewed Linux client's three exact MMKV account fields locally.

Discovery never scans processes, contacts, messages, or arbitrary MMKV keys.
The public result contains no UIN, account name, MMKV crypt key, or image key.
Only the exact, reviewed official ELF is accepted for its public MMKV constant.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import zlib

from Crypto.Cipher import AES

from chatlog_keeper.core._linux import official_wechat_executables
from chatlog_keeper.core._path_resolver import data_dir
from chatlog_keeper.core._secrets import read_secret_text, write_secret_text

WECHAT_VERSION = "4.1.13.26"
ELF_SHA256 = "2749535f299ba67feb8ef80cab898bb0531fa8f376b16f8a72db706811c84132"
CRYPT_CONSTANT_OFFSET = 0xF83E0C
UIN_KEY = b"mmkv_key_latest_login_uin"
NAME_KEYS = (b"mmkv_key_latest_login_username", b"mmkv_key_user_name")
WANTED = frozenset((UIN_KEY, *NAME_KEYS))
MAX_CONFIG_BYTES = 1024 * 1024


class ImageKeyDiscoveryError(ValueError):
    """Contains a fixed error code, never source data or native identifiers."""


def read_varint(raw: bytes, pos: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 70, 7):
        if pos >= len(raw):
            raise ImageKeyDiscoveryError("invalid_mmkv_encoding")
        item = raw[pos]
        pos += 1
        if shift == 63 and item > 1:
            raise ImageKeyDiscoveryError("invalid_mmkv_encoding")
        value |= (item & 127) << shift
        if item < 128:
            return value, pos
    raise ImageKeyDiscoveryError("invalid_mmkv_encoding")


def exact_account_fields(plain: bytes) -> dict[bytes, bytes]:
    _, pos = read_varint(plain, 0)  # MMKV's container header is not an entry.
    fields: dict[bytes, bytes] = {}
    while pos < len(plain):
        size, pos = read_varint(plain, pos)
        if not 0 < size <= 512 or pos + size > len(plain):
            raise ImageKeyDiscoveryError("invalid_mmkv_encoding")
        key = plain[pos:pos + size]
        pos += size
        size, pos = read_varint(plain, pos)
        if pos + size > len(plain):
            raise ImageKeyDiscoveryError("invalid_mmkv_encoding")
        if key in WANTED:
            if size:
                fields[key] = plain[pos:pos + size]
            else:
                fields.pop(key, None)
        pos += size
    if set(fields) != WANTED:
        raise ImageKeyDiscoveryError("required_account_fields_missing")
    return fields


def _decode_string(raw: bytes) -> str:
    size, start = read_varint(raw, 0)
    if size != len(raw) - start:
        raise ImageKeyDiscoveryError("invalid_account_encoding")
    try:
        text = raw[start:].decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise ImageKeyDiscoveryError("invalid_account_encoding") from None
    if not text or len(text) > 256 or "\x00" in text:
        raise ImageKeyDiscoveryError("invalid_account_encoding")
    return text


def normalized_account_names(account_name: str) -> tuple[str, ...]:
    match = re.fullmatch(r"(.+)_[0-9a-fA-F]{4}", account_name)
    return tuple(dict.fromkeys((match.group(1), account_name))) if match else (account_name,)


def _account_ref(account_name: str) -> str:
    return hashlib.sha256(account_name.encode("utf-8")).hexdigest()


def secret_path(filename: str, state: Path | None = None) -> Path:
    return (Path(state) if state is not None else data_dir()) / "secrets" / filename


def load_cached_uin(
    account_dir: Path | None = None, *, state: Path | None = None,
    cache_path: Path | None = None,
) -> int | None:
    """Load a private cache; decoders must still validate against a real image.

    Old unbound PoC caches remain readable as candidates. Newly discovered
    caches include a binding, and a mismatching binding is rejected.
    """
    path = Path(cache_path) if cache_path is not None else secret_path("wechat_v2_uin.txt", state)
    text = read_secret_text(path, max_bytes=128)
    if not text or not re.fullmatch(r"[0-9]{1,10}", text.strip()):
        return None
    value = int(text.strip())
    if not 0 < value < 2**32:
        return None
    source_text = read_secret_text(path.with_name("wechat_v2_uin_source.json"))
    if source_text:
        try:
            source = json.loads(source_text)
            binding = source.get("account_ref")
        except (ValueError, AttributeError):
            return None
        if account_dir is not None and binding and binding != _account_ref(Path(account_dir).name):
            return None
    return value


def _bounded_read(path: Path, limit: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ImageKeyDiscoveryError("unsupported_mmkv_metadata")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            value = handle.read(limit + 1)
        if len(value) > limit:
            raise ImageKeyDiscoveryError("unsupported_mmkv_metadata")
        return value
    finally:
        os.close(fd)


def _reviewed_mmkv_key(binary: Path) -> bytes:
    digest = hashlib.sha256()
    with Path(binary).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
        if digest.hexdigest() != ELF_SHA256:
            raise ImageKeyDiscoveryError("unreviewed_wechat_build")
        handle.seek(CRYPT_CONSTANT_OFFSET)
        constant = handle.read(129)
    end = constant.find(b"\0")
    if end <= 0:
        raise ImageKeyDiscoveryError("invalid_public_mmkv_constant")
    return constant[:end][:16].ljust(16, b"\0")


def _official_mmkv_key(binary: Path | None) -> bytes:
    if binary is not None:
        return _reviewed_mmkv_key(Path(binary))
    candidates = official_wechat_executables()
    for candidate in candidates:
        try:
            return _reviewed_mmkv_key(candidate)
        except (OSError, ImageKeyDiscoveryError):
            continue
    raise ImageKeyDiscoveryError("unreviewed_wechat_build" if candidates else "official_client_not_found")


def _snapshot_global_config(data_root: Path) -> tuple[bytes, bytes]:
    source = data_root / "all_users" / "config" / "global_config"
    metadata = source.with_name(source.name + ".crc")
    for _ in range(3):
        before = _bounded_read(metadata, 65536)
        content = _bounded_read(source, MAX_CONFIG_BYTES)
        after = _bounded_read(metadata, 65536)
        if before == after and content == _bounded_read(source, MAX_CONFIG_BYTES):
            return content, before
    raise ImageKeyDiscoveryError("global_config_changed")


def _decode_global_fields(raw: bytes, meta: bytes, key: bytes) -> dict[bytes, bytes]:
    if len(meta) < 32 or len(raw) < 4 or len(raw) > MAX_CONFIG_BYTES:
        raise ImageKeyDiscoveryError("unsupported_mmkv_metadata")
    version = struct.unpack_from("<I", meta, 4)[0]
    size = struct.unpack_from("<I", meta, 28)[0]
    if version != 4 or not 0 < size <= len(raw) - 4:
        raise ImageKeyDiscoveryError("unsupported_mmkv_metadata")
    ciphertext = raw[4:4 + size]
    if zlib.crc32(ciphertext) & 0xFFFFFFFF != struct.unpack_from("<I", meta)[0]:
        raise ImageKeyDiscoveryError("global_config_crc_mismatch")
    plain = AES.new(key, AES.MODE_CFB, iv=meta[12:28], segment_size=128).decrypt(ciphertext)
    return exact_account_fields(plain)


def _discover_uin(
    data_root: Path, binary: Path | None, state: Path | None,
    expected_account: str | None = None,
) -> dict:
    key = _official_mmkv_key(binary)
    raw, meta = _snapshot_global_config(data_root)
    fields = _decode_global_fields(raw, meta, key)
    uin, consumed = read_varint(fields[UIN_KEY], 0)
    if consumed != len(fields[UIN_KEY]) or not 0 < uin < 2**32:
        raise ImageKeyDiscoveryError("invalid_uin_encoding")
    names = {_decode_string(fields[name]) for name in NAME_KEYS}
    if len(names) != 1:
        raise ImageKeyDiscoveryError("account_names_disagree")
    expected_name = next(iter(names))
    matches = [
        item for item in data_root.iterdir()
        if not item.is_symlink() and item.is_dir() and (item / "db_storage").is_dir()
        and expected_name in normalized_account_names(item.name)
    ]
    if len(matches) != 1:
        raise ImageKeyDiscoveryError("account_match_failed")
    if expected_account is not None and matches[0].name != expected_account:
        raise ImageKeyDiscoveryError("account_mismatch")
    summary = {
        "found": True, "account_matched": True,
        "encoding": "mmkv-aes128-cfb128/protobuf-varint",
        "source_filename": "all_users/config/global_config",
        "wechat_version": WECHAT_VERSION, "image_key_validated": False,
    }
    provenance = {
        **summary, "account_ref": _account_ref(matches[0].name),
        "wechat_elf_sha256": ELF_SHA256, "source_sha256": hashlib.sha256(raw).hexdigest(),
    }
    if not write_secret_text(secret_path("wechat_v2_uin_source.json", state), json.dumps(provenance, indent=2) + "\n"):
        raise ImageKeyDiscoveryError("private_cache_write_failed")
    if not write_secret_text(secret_path("wechat_v2_uin.txt", state), str(uin) + "\n"):
        raise ImageKeyDiscoveryError("private_cache_write_failed")
    return summary


def discover_uin(
    data_root: Path, binary: Path | None = None, state: Path | None = None, *,
    expected_account: str | None = None,
) -> dict:
    """Return a sanitized discovery summary and privately cache the matched UIN.

    This only observes the two local MMKV files. It does not validate an image
    key; decode a specific local V2 image afterwards to establish that result.
    """
    try:
        return _discover_uin(
            Path(data_root), Path(binary) if binary is not None else None,
            state, expected_account,
        )
    except ImageKeyDiscoveryError as exc:
        return {"found": False, "error": str(exc)}
    except (OSError, ValueError, IndexError, struct.error):
        return {"found": False, "error": "configuration_read_failed"}
