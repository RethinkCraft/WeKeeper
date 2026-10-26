import hashlib
import os
from pathlib import Path
import pytest
from chatlog_keeper import wechat_db, wechat_image
from chatlog_keeper.core import _secrets
from chatlog_keeper.core._secrets import private_binary_writer, read_secret_text, write_secret_text


def test_secret_write_is_atomic_and_restrictive(tmp_path):
    target = tmp_path / "secrets" / "key"
    assert write_secret_text(target, "first") is True
    assert write_secret_text(target, "second") is True
    assert target.read_text(encoding="utf-8") == "second"
    assert read_secret_text(target) == "second"
    assert not list(target.parent.glob(".key.*"))
    if os.name != "nt":
        assert target.stat().st_mode & 0o777 == 0o600
        assert target.parent.stat().st_mode & 0o777 == 0o700


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission contract")
def test_secret_read_rejects_expanded_file_or_parent_permissions(tmp_path):
    target = tmp_path / "secrets" / "key"
    assert write_secret_text(target, "private") is True

    target.chmod(0o640)
    assert read_secret_text(target) is None

    target.chmod(0o600)
    target.parent.chmod(0o750)
    assert read_secret_text(target) is None


def test_secret_read_is_bounded_and_rejects_invalid_utf8(tmp_path):
    target = tmp_path / "secrets" / "key"
    assert write_secret_text(target, "0123456789") is True
    assert read_secret_text(target, max_bytes=8) is None

    target.write_bytes(b"\xff")
    if os.name != "nt":
        target.chmod(0o600)
    assert read_secret_text(target) is None


def test_secret_read_rejects_final_symlink(tmp_path):
    secret_dir = tmp_path / "secrets"
    target = secret_dir / "target"
    link = secret_dir / "key"
    assert write_secret_text(target, "private") is True
    try:
        link.symlink_to(target)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are unavailable")
    assert read_secret_text(link) is None


def test_secret_read_rejects_identity_swap_before_open(monkeypatch, tmp_path):
    target = tmp_path / "secrets" / "key"
    replacement = tmp_path / "secrets" / "replacement"
    assert write_secret_text(target, "original") is True
    assert write_secret_text(replacement, "changed!") is True
    real_open = _secrets.os.open
    swapped = False

    def swap_then_open(path, flags):
        nonlocal swapped
        if Path(path) == target and not swapped:
            swapped = True
            os.replace(replacement, target)
        return real_open(path, flags)

    monkeypatch.setattr(_secrets.os, "open", swap_then_open)
    assert read_secret_text(target) is None


def test_private_binary_write_is_atomic_and_restrictive(tmp_path):
    target = tmp_path / "exports" / "image.jpg"
    with private_binary_writer(target) as handle:
        handle.write(b"\xff\xd8\xff")
    assert target.read_bytes() == b"\xff\xd8\xff"
    assert not list(target.parent.glob(".image.jpg.*"))
    if os.name != "nt":
        assert target.stat().st_mode & 0o777 == 0o600


def test_wechat_account_keys_are_isolated_and_hmac_verified(monkeypatch, tmp_path):
    global_path = tmp_path / "secrets" / "wechat_db.key"
    monkeypatch.setattr(wechat_db, "_wechat_key_cache_path", lambda: global_path)

    first_db = tmp_path / "wxid_first" / "db_storage" / "message" / "message_0.db"
    second_db = tmp_path / "wxid_second" / "db_storage" / "message" / "message_0.db"
    first_db.parent.mkdir(parents=True)
    second_db.parent.mkdir(parents=True)
    first_db.write_bytes(b"first")
    second_db.write_bytes(b"second")
    first_key = b"\x11" * 32
    second_key = b"\x22" * 32

    monkeypatch.setattr(
        wechat_db,
        "_read_stable_page1",
        lambda path: b"first-page" if path == first_db else b"second-page",
    )
    monkeypatch.setattr(
        wechat_db,
        "_verify_key_v4",
        lambda key, page: (key, page) in {
            (first_key, b"first-page"),
            (second_key, b"second-page"),
        },
    )

    assert wechat_db.save_cached_wechat_key_for_account(
        first_key, "wxid_first", first_db
    ) is True
    assert wechat_db.save_cached_wechat_key_for_account(
        second_key, "wxid_second", second_db
    ) is True

    first_path = wechat_db._wechat_account_key_cache_path("wxid_first")
    second_path = wechat_db._wechat_account_key_cache_path("wxid_second")
    assert first_path != second_path
    assert "wxid_first" not in first_path.name
    assert "wxid_second" not in second_path.name
    assert first_path.name == hashlib.sha256(b"wxid_first").hexdigest() + ".key"
    assert second_path.name == hashlib.sha256(b"wxid_second").hexdigest() + ".key"
    assert wechat_db.load_cached_wechat_key_for_account("wxid_first") == first_key
    assert wechat_db.load_cached_wechat_key_for_account("wxid_second") == second_key

    # A key verified for another account must not overwrite this account's key.
    assert wechat_db.save_cached_wechat_key_for_account(
        second_key, "wxid_first", first_db
    ) is False
    assert wechat_db.load_cached_wechat_key_for_account("wxid_first") == first_key
    if os.name != "nt":
        assert first_path.stat().st_mode & 0o777 == 0o600
        assert second_path.stat().st_mode & 0o777 == 0o600


def test_wechat_account_key_load_falls_back_to_legacy_global(monkeypatch, tmp_path):
    legacy_key = b"\x33" * 32
    monkeypatch.setattr(
        wechat_db,
        "_wechat_account_key_cache_path",
        lambda _account_id: tmp_path / "missing.key",
        raising=False,
    )
    monkeypatch.setattr(wechat_db, "load_cached_wechat_key", lambda: legacy_key)

    assert wechat_db.load_cached_wechat_key_for_account("wxid_legacy") == legacy_key


def test_wechat_v2_image_key_uses_unified_private_secret_io(monkeypatch, tmp_path):
    key_path = tmp_path / "image-secrets" / "v2.key"
    monkeypatch.setattr(wechat_image, "_V2_KEY_CACHE_PATH_FN", lambda: key_path)

    assert wechat_image.save_cached_v2_key(b"0123456789abcdef") is True
    assert wechat_image.load_cached_v2_key() == b"0123456789abcdef"
    if os.name != "nt":
        assert key_path.stat().st_mode & 0o777 == 0o600
        assert key_path.parent.stat().st_mode & 0o777 == 0o700
        key_path.chmod(0o644)
        assert wechat_image.load_cached_v2_key() is None


def test_wechat_v2_uin_cache_uses_unified_private_secret_reader(
    monkeypatch, tmp_path
):
    uin_path = tmp_path / "uin-secrets" / "uin.txt"
    attach_root = tmp_path / "attach"
    monkeypatch.setattr(wechat_image, "_V2_UIN_CACHE_PATH_FN", lambda: uin_path)
    monkeypatch.setattr(wechat_image, "_V2_XOR_KEY_OVERRIDE", None)
    assert write_secret_text(uin_path, "123456") is True

    assert wechat_image.derive_v2_xor_key(attach_root) == (123456 & 0xFF)
