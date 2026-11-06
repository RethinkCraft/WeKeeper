"""Derived-key reuse is not permission to skip current-page authentication."""
import hashlib

import pytest

from chatlog_keeper import wechat_db
from test_snapshot import _encrypted_header_page


@pytest.fixture(autouse=True)
def bounded_test_cache(monkeypatch):
    wechat_db._password_page_key.cache_clear()
    monkeypatch.setattr(wechat_db, "_WECHAT_KDF_ITER", 64)
    yield
    wechat_db._password_page_key.cache_clear()


def password_page(master, salt):
    key = hashlib.pbkdf2_hmac("sha512", master, salt, wechat_db._WECHAT_KDF_ITER, dklen=32)
    return key, _encrypted_header_page(key, salt, 1)


def test_warm_derived_key_still_authenticates_current_page_one(monkeypatch):
    master, salt = b"m" * 32, b"s" * 16
    expected, page = password_page(master, salt)
    original = hashlib.pbkdf2_hmac
    expensive_calls = []
    def counted(name, key, salt, rounds, **kwargs):
        if rounds == 64:
            expensive_calls.append(True)
        return original(name, key, salt, rounds, **kwargs)
    monkeypatch.setattr(hashlib, "pbkdf2_hmac", counted)
    assert wechat_db._effective_page_key(master, page) == expected
    assert wechat_db._effective_page_key(bytearray(master), page) == expected
    for position in (16, 4016, 4095):
        corrupted = bytearray(page)
        corrupted[position] ^= 1
        assert wechat_db._effective_page_key(master, bytes(corrupted)) is None
    assert expensive_calls == [True]
    assert wechat_db._password_page_key.cache_info().hits == 4


def test_master_salt_and_kdf_parameters_are_distinct_cache_identities(monkeypatch):
    master, salt = b"m" * 32, b"s" * 16
    first, page = password_page(master, salt)
    assert wechat_db._effective_page_key(master, page) == first
    other_salt = b"t" * 16
    second, second_page = password_page(master, other_salt)
    assert wechat_db._effective_page_key(master, second_page) == second
    assert second != first
    other_master = b"n" * 32
    third, third_page = password_page(other_master, salt)
    assert wechat_db._effective_page_key(other_master, third_page) == third
    assert wechat_db._effective_page_key(master, third_page) is None
    monkeypatch.setattr(wechat_db, "_WECHAT_KDF_ITER", 65)
    fourth, fourth_page = password_page(master, salt)
    assert wechat_db._effective_page_key(master, fourth_page) == fourth
    assert fourth != first
    assert wechat_db._password_page_key.cache_info().currsize == 4


def test_derived_cache_has_a_fixed_memory_bound_and_raw_keys_do_not_enter_it():
    raw, salt = b"r" * 32, b"s" * 16
    assert wechat_db._effective_page_key(raw, _encrypted_header_page(raw, salt, 1)) == raw
    assert wechat_db._password_page_key.cache_info().currsize == 0
    for n in range(40):
        wechat_db._password_page_key(b"m" * 32, n.to_bytes(16, "little"), 64)
    info = wechat_db._password_page_key.cache_info()
    assert info.maxsize == info.currsize == 16


@pytest.mark.parametrize("position", [0, 2048, 4095])
def test_zero_padding_comparison_rejects_every_nonzero_position(tmp_path, position):
    key, salt = b"r" * 32, b"s" * 16
    page = _encrypted_header_page(key, salt, 1)
    source, output = tmp_path / "encrypted.db", tmp_path / "plain.db"
    source.write_bytes(page + bytes(4096) * 3)
    assert wechat_db._decrypt_db_v4_snapshot(source, key, output)
    previous = output.read_bytes()
    trailing = bytearray(4096)
    trailing[position] = 1
    source.write_bytes(page + bytes(4096) * 2 + trailing)
    assert not wechat_db._decrypt_db_v4_snapshot(source, key, output)
    assert output.read_bytes() == previous


def test_cached_password_page_key_does_not_publish_corrupt_plaintext(tmp_path):
    master, salt = b"m" * 32, b"s" * 16
    _, page = password_page(master, salt)
    source, output = tmp_path / "encrypted.db", tmp_path / "plain.db"
    source.write_bytes(page)
    assert wechat_db._decrypt_db_v4_snapshot(source, master, output)
    previous = output.read_bytes()
    tampered = bytearray(page)
    tampered[32] ^= 1
    source.write_bytes(tampered)
    assert not wechat_db._decrypt_db_v4_snapshot(source, master, output)
    assert output.read_bytes() == previous
