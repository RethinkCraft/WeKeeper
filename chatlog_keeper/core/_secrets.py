"""Linux private files: bounded reads, owner-only permissions and atomic writes."""
from __future__ import annotations

import os
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, IO, Iterator, Optional, TextIO, cast

_DEFAULT_MAX_SECRET_BYTES = 4096


def _same_identity(first: os.stat_result, second: os.stat_result) -> bool:
    return (first.st_dev, first.st_ino) == (second.st_dev, second.st_ino)


def _posix_parent_is_private(value: os.stat_result) -> bool:
    return (
        stat.S_ISDIR(value.st_mode)
        and value.st_uid == os.geteuid()
        and stat.S_IMODE(value.st_mode) == 0o700
    )


def _posix_file_is_private(value: os.stat_result) -> bool:
    return (
        stat.S_ISREG(value.st_mode)
        and value.st_uid == os.geteuid()
        and stat.S_IMODE(value.st_mode) == 0o600
    )


def _secret_parent_is_safe(value: os.stat_result) -> bool:
    return _posix_parent_is_private(value)


def _secret_file_is_safe(value: os.stat_result) -> bool:
    return _posix_file_is_private(value)


def _prepare_secret_parent(path: Path) -> os.stat_result:
    """Restrict one real owner-controlled directory and recheck its identity."""
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    before = os.lstat(path)
    if not stat.S_ISDIR(before.st_mode) or before.st_uid != os.geteuid():
        raise PermissionError("secret parent must be an owner-controlled directory")
    os.chmod(path, 0o700, follow_symlinks=False)
    after = os.lstat(path)
    if not _same_identity(before, after) or not _secret_parent_is_safe(after):
        raise PermissionError("secret parent identity changed")
    return after


@contextmanager
def _private_writer(
    path: Path, *, binary: bool, secure_parent: bool
) -> Iterator[IO]:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    parent_before = (
        _prepare_secret_parent(path.parent) if secure_parent else None
    )

    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    tmp = Path(tmp_name)
    temp_identity = None
    replaced = False
    published = False
    try:
        temp_identity = os.fstat(fd)
        os.fchmod(fd, 0o600)
        if secure_parent:
            parent_during = os.lstat(path.parent)
            if not _same_identity(parent_before, parent_during):
                raise PermissionError("secret parent identity changed")
        if binary:
            handle = os.fdopen(fd, "wb")
        else:
            handle = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
        fd = -1
        with handle:
            yield handle
            handle.flush()
            os.fsync(handle.fileno())
        temp_after = os.lstat(tmp)
        if not _same_identity(temp_identity, temp_after):
            raise PermissionError("temporary secret identity changed")
        if secure_parent and not _same_identity(
            parent_before, os.lstat(path.parent)
        ):
            raise PermissionError("secret parent identity changed")
        os.replace(tmp, path)
        replaced = True
        os.chmod(path, 0o600, follow_symlinks=False)
        published_value = os.lstat(path)
        if not _same_identity(temp_identity, published_value):
            raise PermissionError("published secret identity changed")
        if secure_parent:
            if not _secret_file_is_safe(published_value):
                raise PermissionError("published secret is not private")
            parent_after = os.lstat(path.parent)
            if (
                not _same_identity(parent_before, parent_after)
                or not _secret_parent_is_safe(parent_after)
            ):
                raise PermissionError("secret parent identity changed")
        published = True
    finally:
        if fd >= 0:
            os.close(fd)
        if not published:
            cleanup_path = path if replaced else tmp
            try:
                cleanup_value = os.lstat(cleanup_path)
                if temp_identity is None and not replaced:
                    cleanup_path.unlink()
                elif temp_identity is not None and _same_identity(
                    temp_identity, cleanup_value
                ):
                    cleanup_path.unlink()
            except OSError:
                pass


@contextmanager
def private_text_writer(
    path: Path, *, secure_parent: bool = False
) -> Iterator[TextIO]:
    """Yield an atomic UTF-8 writer whose published file is owner-only."""
    with _private_writer(
        path, binary=False, secure_parent=secure_parent
    ) as handle:
        yield cast(TextIO, handle)


@contextmanager
def private_binary_writer(
    path: Path, *, secure_parent: bool = False
) -> Iterator[BinaryIO]:
    """Binary counterpart to :func:`private_text_writer`."""
    with _private_writer(
        path, binary=True, secure_parent=secure_parent
    ) as handle:
        yield cast(BinaryIO, handle)


def read_secret_text(
    path: Path, *, max_bytes: int = _DEFAULT_MAX_SECRET_BYTES
) -> Optional[str]:
    """Read one private UTF-8 secret without following a final symlink.

    The read is bounded and accepted only when the parent and file identities
    match before/open/after observations. Require current-UID ``0700`` parent
    and ``0600`` regular file; unsafe ownership or permissions fail closed.
    """
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool):
        return None
    if max_bytes <= 0 or max_bytes > _DEFAULT_MAX_SECRET_BYTES:
        return None
    path = Path(path)
    fd = -1
    try:
        parent_initial = os.lstat(path.parent)
        file_initial = os.lstat(path)
        if (
            not stat.S_ISDIR(parent_initial.st_mode)
            or not stat.S_ISREG(file_initial.st_mode)
        ):
            return None
        parent_before = os.lstat(path.parent)
        file_before = os.lstat(path)
        if (
            not _same_identity(parent_initial, parent_before)
            or not _same_identity(file_initial, file_before)
            or not _secret_parent_is_safe(parent_before)
            or not _secret_file_is_safe(file_before)
        ):
            return None

        flags = os.O_RDONLY
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        opened_before = os.fstat(fd)
        if (
            not _same_identity(file_before, opened_before)
            or not _secret_file_is_safe(opened_before)
        ):
            return None

        chunks = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > max_bytes:
            return None

        opened_after = os.fstat(fd)
        file_after = os.lstat(path)
        parent_after = os.lstat(path.parent)
        if (
            not _same_identity(opened_before, opened_after)
            or opened_before.st_size != opened_after.st_size
            or opened_before.st_mtime_ns != opened_after.st_mtime_ns
            or not _same_identity(file_before, file_after)
            or not _same_identity(parent_before, parent_after)
            or not _secret_file_is_safe(file_after)
            or not _secret_parent_is_safe(parent_after)
        ):
            return None
        return raw.decode("utf-8", errors="strict")
    except (OSError, UnicodeError, ValueError):
        return None
    finally:
        if fd >= 0:
            os.close(fd)


def write_secret_text(path: Path, text: str) -> bool:
    if not isinstance(text, str):
        return False
    try:
        encoded = text.encode("utf-8", errors="strict")
    except UnicodeError:
        return False
    if len(encoded) > _DEFAULT_MAX_SECRET_BYTES:
        return False
    try:
        with private_text_writer(path, secure_parent=True) as handle:
            handle.write(text)
        return True
    except (OSError, ValueError):
        return False
