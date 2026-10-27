"""Password and opaque session primitives. No credentials enter logs."""
import base64
import hashlib
import hmac
import os
import secrets


def password_hash(password: str) -> str:
    if not isinstance(password, str) or not 12 <= len(password) <= 1024:
        raise ValueError("password_must_be_12_to_1024_characters")
    salt = os.urandom(16)
    value = hashlib.scrypt(password.encode(), salt=salt, n=16384, r=8, p=1)
    return "scrypt$" + base64.b64encode(salt).decode() + "$" + base64.b64encode(value).decode()


def password_verify(password: str, encoded: str) -> bool:
    try:
        name, salt, expected = encoded.split("$")
        if name != "scrypt" or not isinstance(password, str) or len(password) > 1024:
            return False
        value = hashlib.scrypt(password.encode(), salt=base64.b64decode(salt), n=16384, r=8, p=1)
        return hmac.compare_digest(value, base64.b64decode(expected))
    except (ValueError, TypeError):
        return False


def opaque_token() -> str:
    return secrets.token_urlsafe(32)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()
