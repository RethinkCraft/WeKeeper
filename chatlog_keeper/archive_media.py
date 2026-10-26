"""Archive only exactly linked local media; never fetch, launch, or scan a client.

Encrypted cache bytes and successfully decoded image bytes are content-addressed
separately.  Serving an archived image therefore does not depend on the source
cache or on mutable account-key caches still being present.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import io
import json
import math
import mimetypes
import os
from pathlib import Path
import re
import stat
import tempfile
import time
from zoneinfo import ZoneInfo

from chatlog_keeper import image_key, wechat_image
from chatlog_keeper.core._secrets import private_binary_writer, read_secret_text

ZONE = ZoneInfo("Asia/Shanghai")
_FILE_ID = re.compile(r"[0-9a-f]{32}")
_REFERENCE = re.compile(r"(?:media|msg)_[A-Za-z0-9_-]{1,128}")
_BLOB = re.compile(r"(?:raw|decoded)/([0-9a-f]{2})/([0-9a-f]{64})\.(?:bin|jpg|png|gif|webp)")
_MAX_IMAGE_BYTES = 64 * 1024 * 1024
_MAX_INLINE_BYTES = 32 * 1024 * 1024
_MAX_IMAGE_PIXELS = 40_000_000
_MAX_FILE_BYTES = 512 * 1024 * 1024
_MAX_FILE_SCAN_BYTES = 128 * 1024 * 1024
_MAX_FILE_CANDIDATES = 1000
_FILE_COPY_SECONDS = 10.0
_IMAGE_SCAN_SECONDS = 2.0
_IMAGE_INDEX_SECONDS = 25.0


class MediaUnavailable(RuntimeError):
    """A fixed error code safe to return through MCP."""

    def __init__(self, code: str):
        self.code = code if re.fullmatch(r"[a-z0-9_]{1,80}", code) else "media_unavailable"
        super().__init__(self.code)


def _media_root(store) -> Path:
    path = getattr(store, "media_dir", None)
    return Path(path) if path is not None else Path(store.state_dir) / "archive" / "media"


def _private_directory(path: Path) -> Path:
    if path.is_symlink():
        raise MediaUnavailable("unsafe_archive_directory")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    value = path.lstat()
    if not stat.S_ISDIR(value.st_mode) or value.st_uid != os.geteuid():
        raise MediaUnavailable("unsafe_archive_directory")
    path.chmod(0o700)
    return path


def _blob_path(root: Path, relative: str) -> Path:
    """Accept only our content-addressed layout, never an arbitrary file path."""
    if not isinstance(relative, str):
        raise MediaUnavailable("invalid_archive_blob")
    match = _BLOB.fullmatch(relative)
    if match is None or match.group(1) != match.group(2)[:2]:
        raise MediaUnavailable("invalid_archive_blob")
    path = root / relative
    for ancestor in (root, path.parent.parent, path.parent, path):
        if ancestor.is_symlink():
            raise MediaUnavailable("unsafe_archive_blob")
    try:
        path.resolve().relative_to(root.resolve())
    except (ValueError, OSError):
        raise MediaUnavailable("unsafe_archive_blob") from None
    return path


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_blob(root: Path, relative: str, expected: str | None, max_bytes=None) -> bytes:
    path = _blob_path(root, relative)
    try:
        value = path.lstat()
        if not stat.S_ISREG(value.st_mode) or value.st_uid != os.geteuid():
            raise MediaUnavailable("unsafe_archive_blob")
        if max_bytes is not None and value.st_size > max_bytes:
            raise MediaUnavailable("media_exceeds_inline_limit")
        data = path.read_bytes()
    except MediaUnavailable:
        raise
    except OSError:
        raise MediaUnavailable("archive_blob_missing") from None
    digest = hashlib.sha256(data).hexdigest()
    if digest != path.name.split(".", 1)[0] or (expected and digest != expected):
        raise MediaUnavailable("archive_blob_corrupt")
    return data


def _variants(record: dict) -> list[dict]:
    variants = record.get("variants")
    if isinstance(variants, str):
        try:
            variants = json.loads(variants)
        except (ValueError, TypeError):
            variants = None
    if isinstance(variants, list):
        return [dict(value) for value in variants if isinstance(value, dict)]
    return [record] if record.get("raw_path") or record.get("decoded_path") else []


def _rank(variant: dict):
    return (0 if variant.get("tier") != "thumbnail" else 1,
            -int(variant.get("width") or 0) * int(variant.get("height") or 0))


def _month(value) -> str:
    try:
        if isinstance(value, datetime):
            date = value.replace(tzinfo=ZONE) if value.tzinfo is None else value.astimezone(ZONE)
        elif isinstance(value, str) and not value.isdigit():
            date = datetime.fromisoformat(value)
            date = date.replace(tzinfo=ZONE) if date.tzinfo is None else date.astimezone(ZONE)
        else:
            date = datetime.fromtimestamp(int(value), ZONE)
        return date.strftime("%Y-%m")
    except (TypeError, ValueError, OSError, OverflowError):
        raise MediaUnavailable("invalid_media_message_time") from None


def _attachment(record: dict) -> dict:
    attachment = record.get("attachment") or {}
    if isinstance(attachment, str):
        try:
            attachment = json.loads(attachment)
        except (TypeError, ValueError):
            attachment = {}
    return attachment if isinstance(attachment, dict) else {}


class MediaArchiver:
    def __init__(self, store, data_root: Path, account_dir: Path):
        self.store = store
        self.state_dir = Path(store.state_dir)
        self.media_dir = _private_directory(_media_root(store))
        self.data_root = Path(data_root).resolve()
        self.account_dir = Path(account_dir).resolve()
        try:
            self.account_dir.relative_to(self.data_root)
        except ValueError:
            raise MediaUnavailable("account_outside_data_root") from None
        self._image_indexes = {}

    def _put_bytes(self, kind: str, payload: bytes, extension="bin") -> tuple[str, str]:
        digest = hashlib.sha256(payload).hexdigest()
        relative = f"{kind}/{digest[:2]}/{digest}.{extension}"
        path = _blob_path(self.media_dir, relative)
        _private_directory(path.parent.parent)
        _private_directory(path.parent)
        if path.exists():
            try:
                if _read_blob(self.media_dir, relative, digest) == payload:
                    return relative, digest
            except MediaUnavailable:
                pass
        with private_binary_writer(path, secure_parent=True) as handle:
            handle.write(payload)
        return relative, digest

    def _safe_source(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.account_dir)
            relative = path.relative_to(self.account_dir)
            if any((self.account_dir / Path(*relative.parts[:i])).is_symlink()
                   for i in range(1, len(relative.parts) + 1)):
                return False
            value = path.lstat()
            return stat.S_ISREG(value.st_mode) and value.st_uid == os.geteuid()
        except (OSError, ValueError):
            return False

    def _candidates(self, record: dict, attachment: dict, kind: str) -> list[Path]:
        month = _month(record.get("create_time", record.get("timestamp")))
        if kind == "image":
            file_id = str(attachment.get("file_md5") or "")
            if _FILE_ID.fullmatch(file_id) is None:
                raise MediaUnavailable("image_link_unresolved")
            root = self.account_dir / "msg" / "attach"
            candidates = [p for p in root.glob(f"*/{month}/Img/{file_id}*.dat")
                          if re.fullmatch(re.escape(file_id) + r"(?:_[A-Za-z0-9]+)?\.dat", p.name)
                          and self._safe_source(p)]
            return sorted(candidates, key=lambda p: (0 if p.stem.endswith("_h") else
                                                    2 if p.stem.endswith("_t") else 1, p.name))
        if kind == "video":
            file_id = str(attachment.get("md5") or "").lower()
            if _FILE_ID.fullmatch(file_id) is None:
                raise MediaUnavailable("video_link_unresolved")
            path = self.account_dir / "msg" / "video" / month / f"{file_id}.mp4"
            return [path] if self._safe_source(path) else []
        if kind == "file":
            name = attachment.get("filename")
            expected = str(attachment.get("md5") or attachment.get("file_md5") or "").lower()
            if (not isinstance(name, str) or not name or Path(name).name != name
                    or name in {".", ".."} or "\\" in name or "\0" in name):
                raise MediaUnavailable("file_link_unresolved")
            if _FILE_ID.fullmatch(expected) is None:
                raise MediaUnavailable("file_link_unresolved")
            return self._file_candidates(month, name)
        raise MediaUnavailable("media_kind_not_supported")

    def _safe_source_directory(self, path: Path) -> bool:
        try:
            relative = path.relative_to(self.account_dir)
            if any((self.account_dir / Path(*relative.parts[:i])).is_symlink()
                   for i in range(1, len(relative.parts) + 1)):
                return False
            value = path.lstat()
            return stat.S_ISDIR(value.st_mode) and value.st_uid == os.geteuid()
        except (ValueError, OSError):
            return False

    @staticmethod
    def _plain_image(data: bytes) -> bool:
        return (data.startswith((b"\xff\xd8\xff", b"\x89PNG\r\n\x1a\n", b"GIF87a", b"GIF89a"))
                or (data.startswith(b"RIFF") and data[8:12] == b"WEBP"))

    def _read_source_image(self, path: Path, *, deadline=None) -> bytes:
        """Read one stable owned local candidate without publishing a guess."""
        if not self._safe_source(path):
            raise MediaUnavailable("unsafe_cached_media")
        before = path.lstat()
        if before.st_size > _MAX_IMAGE_BYTES:
            raise MediaUnavailable("cached_image_exceeds_decode_limit")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as handle:
            opened = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened.st_mode):
                raise MediaUnavailable("unsafe_cached_media")
            signature = lambda value: (value.st_dev, value.st_ino, value.st_size,
                                       value.st_mtime_ns, value.st_ctime_ns)
            if signature(before) != signature(opened):
                raise MediaUnavailable("cached_media_changed")
            chunks, length = [], 0
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                length += len(chunk)
                if length > _MAX_IMAGE_BYTES:
                    raise MediaUnavailable("cached_image_exceeds_decode_limit")
                if deadline is not None and time.monotonic() > deadline:
                    raise MediaUnavailable("image_scan_timeout")
                chunks.append(chunk)
            if signature(before) != signature(os.fstat(handle.fileno())) or signature(before) != signature(path.lstat()):
                raise MediaUnavailable("cached_media_changed")
        return b"".join(chunks)

    def _linked_image_index(self, month: str, sticker: bool):
        """Bounded monthly/account image cache index, never filename evidence.

        Ordinary message cache IDs and XML component MD5s have different
        meanings. Only plaintext image content is indexed here. Encrypted
        candidates are retained separately for an exact claimed-ID decode and
        subsequent full content-hash proof. Numbered bundled emoji resources
        are deliberately excluded, even if their bytes happen to match.
        """
        cache_key = (month, sticker)
        cached = self._image_indexes.get(cache_key)
        if cached and time.monotonic() - cached[0] < _IMAGE_INDEX_SECONDS:
            return cached[1:]
        deadline = time.monotonic() + _IMAGE_SCAN_SECONDS
        roots, queue = [], []
        attach = self.account_dir / "msg/attach"
        examined, scanned, limited = 0, 0, False
        if self._safe_source_directory(attach):
            with os.scandir(attach) as entries:
                for entry in entries:
                    examined += 1
                    if examined > _MAX_FILE_CANDIDATES or time.monotonic() > deadline:
                        limited = True
                        break
                    if entry.is_dir(follow_symlinks=False):
                        roots.append((Path(entry.path) / month / "Img", 0))
        if sticker:
            # Cached message stickers only. resource/emoticon_webp contains
            # numbered system assets, not a proven message association.
            roots.extend((self.account_dir / name, 3) for name in
                         ("msg/emoticon", "msg/emoji", "cache", "resource/emoticon"))
        queue.extend(roots)
        index, encrypted = {}, []
        visited = set()
        while queue and not limited:
            directory, depth = queue.pop(0)
            if directory in visited or not self._safe_source_directory(directory):
                continue
            visited.add(directory)
            with os.scandir(directory) as entries:
                for entry in entries:
                    examined += 1
                    if examined > _MAX_FILE_CANDIDATES or time.monotonic() > deadline:
                        limited = True
                        break
                    path = Path(entry.path)
                    if entry.is_dir(follow_symlinks=False):
                        if depth and entry.name != "emoticon_webp":
                            queue.append((path, depth - 1))
                        continue
                    if not self._safe_source(path):
                        continue
                    try:
                        value = path.lstat()
                        if value.st_size > _MAX_IMAGE_BYTES:
                            continue
                        if scanned + value.st_size > _MAX_FILE_SCAN_BYTES:
                            limited = True
                            break
                        scanned += value.st_size
                        data = self._read_source_image(path, deadline=deadline)
                    except (OSError, MediaUnavailable):
                        continue
                    if self._plain_image(data):
                        index.setdefault(hashlib.md5(data).hexdigest(), []).append(path)
                    elif not sticker and _FILE_ID.fullmatch(path.stem.split("_", 1)[0]):
                        encrypted.append(path)
        result = (time.monotonic(), index, encrypted, limited)
        self._image_indexes[cache_key] = result
        return result[1:]

    def _file_candidates(self, month: str, name: str) -> list[Path]:
        """Read only the account/month cache, including renamed local copies.

        Both directory enumeration and later hashing are bounded. Filenames
        merely prioritize candidates; content MD5 is mandatory before adoption.
        """
        paths, seen, examined = [], set(), 0
        self._file_scan_limited = False
        directories = [self.account_dir / "msg/file" / month]
        direct = directories[0] / name
        if self._safe_source(direct):
            paths.append(direct)
            seen.add(direct)
        attach_root = self.account_dir / "msg/attach"
        if self._safe_source_directory(attach_root):
            with os.scandir(attach_root) as entries:
                for entry in entries:
                    examined += 1
                    if examined > _MAX_FILE_CANDIDATES:
                        self._file_scan_limited = True
                        break
                    if entry.is_dir(follow_symlinks=False):
                        directories.append(Path(entry.path) / month / "File")
        for directory in directories:
            if not self._safe_source_directory(directory):
                continue
            exact = directory / name
            if exact not in seen and self._safe_source(exact):
                paths.append(exact)
                seen.add(exact)
            if examined > _MAX_FILE_CANDIDATES:
                continue
            with os.scandir(directory) as entries:
                for entry in entries:
                    examined += 1
                    if examined > _MAX_FILE_CANDIDATES:
                        self._file_scan_limited = True
                        break
                    path = Path(entry.path)
                    if path not in seen and self._safe_source(path):
                        seen.add(path)
                        paths.append(path)
        return sorted(paths, key=lambda p: (p.name != name, str(p)))

    def _copy_source(self, path: Path, expected_md5=None, expected_size=None,
                     max_bytes=None, deadline=None) -> tuple[str, str]:
        """Stream large files with a stable descriptor and bounded memory."""
        before = path.lstat()
        if not self._safe_source(path):
            raise MediaUnavailable("unsafe_cached_media")
        if expected_size is not None and before.st_size != expected_size:
            raise MediaUnavailable("cached_file_size_mismatch")
        if max_bytes is not None and before.st_size > max_bytes:
            raise MediaUnavailable("cached_file_size_limit")
        staging = _private_directory(self.media_dir / "raw")
        fd, temporary_name = tempfile.mkstemp(prefix=".copy-", dir=staging)
        temporary = Path(temporary_name)
        sha, md5 = hashlib.sha256(), hashlib.md5()
        try:
            os.fchmod(fd, 0o600)
            source_fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            with os.fdopen(source_fd, "rb") as source, os.fdopen(fd, "wb") as destination:
                fd = -1
                opened = os.fstat(source.fileno())
                if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                    raise MediaUnavailable("cached_media_changed")
                copied = 0
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    copied += len(chunk)
                    if max_bytes is not None and copied > max_bytes:
                        raise MediaUnavailable("cached_file_size_limit")
                    if deadline is not None and time.monotonic() > deadline:
                        raise MediaUnavailable("cached_file_copy_timeout")
                    sha.update(chunk); md5.update(chunk); destination.write(chunk)
                after = os.fstat(source.fileno())
                current = path.lstat()
                signature = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)
                if signature(before) != signature(after) or signature(before) != signature(current):
                    raise MediaUnavailable("cached_media_changed")
                destination.flush(); os.fsync(destination.fileno())
            if expected_md5 and md5.hexdigest() != expected_md5:
                raise MediaUnavailable("cached_file_md5_mismatch")
            digest = sha.hexdigest()
            relative = f"raw/{digest[:2]}/{digest}.bin"
            target = _blob_path(self.media_dir, relative)
            _private_directory(target.parent)
            if target.exists() and not target.is_symlink() and _sha256_file(target) == digest:
                temporary.unlink()
            else:
                os.replace(temporary, target)
                target.chmod(0o600)
            return relative, digest
        finally:
            if fd >= 0:
                os.close(fd)
            temporary.unlink(missing_ok=True)

    def _material_path(self, raw_sha: str) -> tuple[str, Path, str]:
        account_ref = hashlib.sha256(self.account_dir.name.encode("utf-8")).hexdigest()
        reference = "mat_" + hashlib.sha256(
            b"chatlog-archive-image-v1\0" + account_ref.encode() + b"\0" + raw_sha.encode()
        ).hexdigest()
        return reference, self.state_dir / "secrets/archive_media" / f"{reference}.json", account_ref

    def _saved_material(self, raw_path: Path, raw_sha: str):
        reference, path, account_ref = self._material_path(raw_sha)
        text = read_secret_text(path)
        if text is None:
            return None
        try:
            saved = json.loads(text)
            if (saved.get("version") != 1 or saved.get("account_ref") != account_ref
                    or saved.get("raw_sha256") != raw_sha
                    or not re.fullmatch(r"[0-9a-f]{32}", saved.get("aes_key_hex", ""))
                    or type(saved.get("xor_key")) is not int or not 0 <= saved["xor_key"] <= 255):
                return None
            key, xor = bytes.fromhex(saved["aes_key_hex"]), saved["xor_key"]
            if not wechat_image._validate_v2_key(key, raw_path):
                return None
            return key, xor, reference
        except (TypeError, ValueError, AttributeError):
            return None

    def _preserve_material(self, raw_path: Path, raw_sha: str, key: bytes, xor: int) -> str:
        """Publish verified material once; an existing file is never overwritten."""
        reference, path, account_ref = self._material_path(raw_sha)
        saved = self._saved_material(raw_path, raw_sha)
        if path.exists() or path.is_symlink():
            if saved and saved[:2] == (key, xor):
                return reference
            raise MediaUnavailable("archive_material_conflict")
        _private_directory(self.state_dir / "secrets")
        _private_directory(path.parent)
        payload = json.dumps({"version": 1, "account_ref": account_ref,
                              "raw_sha256": raw_sha, "aes_key_hex": key.hex(),
                              "xor_key": xor}, sort_keys=True).encode() + b"\n"
        fd, temporary_name = tempfile.mkstemp(prefix=".material-", dir=path.parent)
        temporary = Path(temporary_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as handle:
                fd = -1
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                # Atomic create-only publication: even a racing writer cannot
                # replace the old credentials for this exact raw cache object.
                os.link(temporary, path, follow_symlinks=False)
            except FileExistsError:
                saved = self._saved_material(raw_path, raw_sha)
                if not saved or saved[:2] != (key, xor):
                    raise MediaUnavailable("archive_material_conflict") from None
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if fd >= 0:
                os.close(fd)
            temporary.unlink(missing_ok=True)
        return reference

    def _decode(self, raw_path: Path, data: bytes):
        """Resolve material from this archive's state, not another process env."""
        material = None
        if data.startswith((b"\xff\xd8\xff", b"\x89PNG\r\n\x1a\n", b"GIF8")) or (
                data.startswith(b"RIFF") and data[8:12] == b"WEBP"):
            decoded, encryption = data, "plaintext"
        elif data.startswith(b"wxgf"):
            decoded, encryption = data, "plaintext"
        else:
            encryption = wechat_image._detect_encrypt_variant(data[:16])
            if encryption == "v1":
                decoded = wechat_image._decrypt_v1v2(data, wechat_image._V1_FIXED_AES_KEY)
            elif encryption == "legacy_xor":
                key = wechat_image._derive_v1_key(data[:16])
                decoded = wechat_image._xor_decrypt(data, key) if key is not None else None
            elif encryption == "v2":
                decoded = None
                saved = self._saved_material(raw_path, hashlib.sha256(data).hexdigest())
                if saved:
                    key, xor, _ = saved
                    decoded = wechat_image._decrypt_v1v2(data, key, xor)
                    if decoded:
                        material = (key, xor)
                uin = image_key.load_cached_uin(self.account_dir, state=self.state_dir)
                if decoded is None and uin is not None:
                    for name in image_key.normalized_account_names(self.account_dir.name):
                        key = wechat_image.derive_v2_key_from_uin(uin, name)
                        if wechat_image._validate_v2_key(key, raw_path):
                            decoded = wechat_image._decrypt_v1v2(data, key, uin & 255)
                            if decoded:
                                material = (key, uin & 255)
                            break
                if decoded is None and wechat_image._parse_v1v2_header(data)[2] == 0:
                    cached = read_secret_text(self.state_dir / "secrets" / "wechat_v2_image_aes.key", max_bytes=128)
                    if cached:
                        text = cached.strip()
                        key = text.encode("ascii") if len(text) == 16 and text.isascii() else (
                            bytes.fromhex(text) if re.fullmatch(r"[0-9a-fA-F]{32}", text) else b"")
                        if wechat_image._validate_v2_key(key, raw_path):
                            decoded = wechat_image._decrypt_v1v2(data, key, 0)
                            if decoded:
                                material = (key, 0)
            else:
                decoded = None
            if not decoded:
                raise MediaUnavailable("image_key_or_codec_unavailable")
        codec = "wxgf" if decoded.startswith(b"wxgf") else "image"
        if codec == "wxgf":
            decoded = wechat_image.wxgf_to_jpeg(decoded)
            if not decoded:
                raise MediaUnavailable("wxgf_transcode_failed")
        return decoded, encryption, codec, material

    def _complete_ready_material(self, variant: dict) -> dict:
        if variant.get("encryption_format") != "v2":
            return variant
        raw_path = _blob_path(self.media_dir, variant["raw_path"])
        saved = self._saved_material(raw_path, variant["raw_sha256"])
        if saved:
            return {**{key: value for key, value in variant.items() if key != "decode_reason"},
                    "material_ref": saved[2], "material_status": "preserved"}
        # Ready images from an older installation need a one-time material
        # backfill. Full validation is repeated before publishing credentials.
        old = {key: value for key, value in variant.items()
               if key not in {"material_ref", "material_status"}}
        result = self._decode_variant(old)
        if not result.get("material_ref"):
            result["material_status"] = "unavailable"
        return result

    def _archive_image(self, source: Path, previous: dict | None) -> dict:
        value = source.lstat()
        signature = [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns]
        if previous and previous.get("source_signature") == signature and previous.get("decoded_path"):
            try:
                _read_blob(self.media_dir, previous["decoded_path"], previous.get("decoded_sha256"))
                return self._complete_ready_material(dict(previous))
            except MediaUnavailable:
                pass
        raw_path, raw_sha = self._copy_source(source)
        variant = {"raw_path": raw_path, "raw_sha256": raw_sha,
                   "source_filename": source.name,
                   "source_signature": signature,
                   "tier": "thumbnail" if source.stem.endswith("_t") else "full_cache",
                   "original_proven": False}
        if previous and previous.get("raw_sha256") == raw_sha and previous.get("decoded_path"):
            try:
                _read_blob(self.media_dir, previous["decoded_path"], previous.get("decoded_sha256"))
                return self._complete_ready_material({**previous, **variant})
            except MediaUnavailable:
                pass
        return self._decode_variant(variant)

    def _decode_variant(self, variant: dict) -> dict:
        """A previously copied raw blob may be retried even after source cleanup."""
        from PIL import Image
        variant = {key: value for key, value in variant.items() if key != "decode_reason"}
        archived = _blob_path(self.media_dir, variant["raw_path"])
        if archived.stat().st_size > _MAX_IMAGE_BYTES:
            return {**variant, "decode_reason": "cached_image_exceeds_decode_limit"}
        raw = _read_blob(self.media_dir, variant["raw_path"], variant.get("raw_sha256"))
        variant["encryption_format"] = wechat_image._detect_encrypt_variant(raw[:16])
        try:
            decoded, encryption, codec, material = self._decode(archived, raw)
            with Image.open(io.BytesIO(decoded)) as image:
                fmt, width, height = image.format, *image.size
                if width <= 0 or height <= 0 or width * height > _MAX_IMAGE_PIXELS:
                    raise MediaUnavailable("decoded_image_dimensions_out_of_bounds")
                image.verify()
            with Image.open(io.BytesIO(decoded)) as image:
                image.load()
                frame_count = int(getattr(image, "n_frames", 1))
                animated = frame_count > 1
                if fmt in {"GIF", "WEBP"} or animated:
                    image.seek(0)
                    preview = io.BytesIO()
                    image.convert("RGBA").save(preview, format="PNG")
                    preview_bytes = preview.getvalue()
            extension = {"JPEG": "jpg", "PNG": "png", "GIF": "gif", "WEBP": "webp"}.get(fmt)
            if extension is None:
                raise MediaUnavailable("decoded_image_format_unsupported")
            decoded_path, decoded_sha = self._put_bytes("decoded", decoded, extension)
            animation = {"animated": animated, "frame_count": frame_count,
                         "selected_frame": 0}
            mime = Image.MIME[fmt]
            if fmt in {"GIF", "WEBP"} or animated:
                animation.update(original_decoded_path=decoded_path,
                                 original_decoded_sha256=decoded_sha,
                                 original_mime=mime, original_animation_preserved=True)
                decoded_path, decoded_sha = self._put_bytes("decoded", preview_bytes, "png")
                mime = "image/png"
            result = {**variant, "decoded_path": decoded_path, "decoded_sha256": decoded_sha,
                    "mime": mime, "width": width, "height": height, **animation,
                    "encryption_format": encryption, "codec": codec,
                    "full_image_load_verified": True}
            if material is not None:
                result.update(material_ref=self._preserve_material(
                    archived, variant["raw_sha256"], *material), material_status="preserved")
            return result
        except MediaUnavailable as exc:
            return {**variant, "decode_reason": exc.code}
        except (OSError, ValueError, KeyError, Image.DecompressionBombError):
            return {**variant, "decode_reason": "decoded_image_invalid"}

    def _process_one(self, record: dict) -> dict:
        attachment = _attachment(record)
        kind = str(record.get("kind") or attachment.get("kind") or "")
        if kind == "file":
            return self._process_file(record, attachment)
        if kind == "voice" and record.get("voice_asset_ref"):
            return self._process_archived_voice(record)
        if kind == "sticker" or (kind == "image" and (record.get("forward_ref") or record.get("component_ref"))):
            return self._process_linked_image(record, attachment, kind)
        existing = _variants(record)
        try:
            paths = self._candidates(record, attachment, kind)
        except MediaUnavailable as exc:
            if any(v.get("decoded_path") for v in existing):
                return {"status": "ready", "source_status": exc.code}
            return {"status": "unsupported" if exc.code == "media_kind_not_supported" else "unresolved",
                    "reason": exc.code}
        variants = list(existing)
        by_name = {v.get("source_filename"): v for v in existing}
        copy_errors = []
        attempted = set()
        for path in paths:
            if kind == "image":
                variant = self._archive_image(path, by_name.get(path.name))
                attempted.add(variant.get("raw_sha256"))
            else:
                previous = by_name.get(path.name)
                value = path.lstat()
                signature = [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns]
                if previous and previous.get("source_signature") == signature and previous.get("raw_path"):
                    previous_path = _blob_path(self.media_dir, previous["raw_path"])
                    if previous_path.is_file() and previous_path.stat().st_size == previous.get("raw_size"):
                        continue
                expected = str(attachment.get("md5") or "").lower()
                try:
                    raw_path, raw_sha = self._copy_source(path, expected_md5=expected)
                except MediaUnavailable as exc:
                    copy_errors.append(exc.code)
                    continue
                variant = {"raw_path": raw_path, "raw_sha256": raw_sha,
                           "source_signature": signature, "raw_size": value.st_size,
                           "source_filename": path.name, "tier": "full_cache",
                           "mime": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                           "codec": "native", "original_proven": False}
            variants = [old for old in variants if old.get("raw_sha256") != variant.get("raw_sha256")]
            variants.append(variant)
        if kind == "image":
            # New cache credentials can unlock raw files archived by an earlier
            # pass, even when WeChat has since deleted the original cache.
            completed = []
            for variant in variants:
                if variant.get("decoded_path"):
                    variant = self._complete_ready_material(variant)
                elif variant.get("raw_path") and variant.get("raw_sha256") not in attempted:
                    variant = self._decode_variant(variant)
                completed.append(variant)
            variants = completed
        usable = [v for v in variants if v.get("decoded_path") or (kind != "image" and v.get("raw_path"))]
        if usable:
            best = min(usable, key=_rank)
            return {**best, "status": "ready", "reason": None, "variants": variants,
                    "kind": kind, "source_status": "cached" if paths else "not_cached"}
        if variants:
            best = min(variants, key=_rank)
            return {**best, "status": "failed", "reason": best.get("decode_reason", "media_decode_failed"),
                    "variants": variants, "kind": kind}
        if copy_errors:
            return {"status": "unresolved", "reason": copy_errors[0], "kind": kind}
        return {"status": "missing", "reason": "media_not_cached", "kind": kind}

    def _process_archived_voice(self, record: dict) -> dict:
        """VoiceInfo has already authenticated the source message association."""
        try:
            payload = _read_blob(self.media_dir, record.get("raw_path"),
                                 record.get("raw_sha256"), max_bytes=_MAX_IMAGE_BYTES)
            if not payload.startswith((b"#!SILK_V3", b"\x02#!SILK_V3")):
                raise MediaUnavailable("archived_voice_format_invalid")
            if record.get("raw_size") is not None and record["raw_size"] != len(payload):
                raise MediaUnavailable("archive_blob_corrupt")
        except MediaUnavailable as exc:
            return {"status": "failed", "reason": exc.code, "kind": "voice"}
        return {"status": "ready", "reason": None, "kind": "voice",
                "raw_path": record["raw_path"], "raw_sha256": record["raw_sha256"],
                "raw_size": len(payload), "mime": "audio/silk", "codec": "silk_v3",
                "source_status": "authenticated_media_database", "tier": "full_cache",
                "original_proven": False}

    def _linked_variant(self, variant: dict, expected: dict) -> dict | None:
        """Recheck the archived original bytes against current component hashes."""
        candidate = dict(variant)
        if not candidate.get("decoded_path") and candidate.get("raw_path"):
            candidate = self._decode_variant(candidate)
        if not candidate.get("decoded_path"):
            return None
        path = candidate.get("original_decoded_path") or candidate["decoded_path"]
        sha = candidate.get("original_decoded_sha256") or candidate.get("decoded_sha256")
        try:
            data = _read_blob(self.media_dir, path, sha, max_bytes=_MAX_IMAGE_BYTES)
        except MediaUnavailable:
            return None
        digest = hashlib.md5(data).hexdigest()
        if digest not in expected:
            return None
        return {**candidate, "tier": expected[digest], "content_md5": digest,
                "association_evidence": "decoded_content_md5", "original_proven": False}

    def _process_linked_image(self, record: dict, attachment: dict, kind: str) -> dict:
        full = str(attachment.get("file_md5") or attachment.get("md5") or "").lower()
        thumb = str(attachment.get("thumbnail_md5") or "").lower()
        expected = {}
        if _FILE_ID.fullmatch(thumb):
            expected[thumb] = "thumbnail"
        if _FILE_ID.fullmatch(full):
            expected[full] = "full_cache"
        if not expected:
            return {"status": "unresolved", "reason": f"{kind}_link_unresolved", "kind": kind,
                    "variants": []}
        verified = []
        for previous in _variants(record):
            value = self._linked_variant(previous, expected)
            if value is not None:
                verified.append(value)
        # A complete archive remains usable when source caches are cleared.
        if any(v["tier"] == "full_cache" for v in verified):
            best = min(verified, key=_rank)
            return {**best, "status": "ready", "reason": None, "kind": kind,
                    "variants": verified, "source_status": "archived_copy_verified"}
        index, encrypted, limited = self._linked_image_index(
            _month(record.get("create_time", record.get("timestamp"))), kind == "sticker")
        candidates = [(path, digest) for digest in expected for path in index.get(digest, [])]
        # A matching basename only selects a bounded decode attempt. Full
        # decoded-content MD5 is still mandatory; names never establish a link.
        if kind != "sticker":
            candidates.extend((path, None) for path in encrypted
                              if path.stem.split("_", 1)[0] in expected)
        seen = {value.get("raw_sha256") for value in verified}
        deadline = time.monotonic() + _IMAGE_SCAN_SECONDS
        for path, expected_raw_md5 in candidates[:32]:
            if time.monotonic() > deadline:
                limited = True
                break
            try:
                data = self._read_source_image(path, deadline=deadline)
                if expected_raw_md5 and hashlib.md5(data).hexdigest() != expected_raw_md5:
                    continue
                decoded, _, _, _ = self._decode(path, data)
                digest = hashlib.md5(decoded).hexdigest()
                if digest not in expected:
                    continue
                raw_path, raw_sha = self._copy_source(path, expected_md5=hashlib.md5(data).hexdigest(),
                    max_bytes=_MAX_IMAGE_BYTES, deadline=deadline)
                variant = self._decode_variant({"raw_path": raw_path, "raw_sha256": raw_sha,
                    "source_filename": path.name, "original_proven": False})
                value = self._linked_variant(variant, expected)
                if value is not None and raw_sha not in seen:
                    verified.append(value)
                    seen.add(raw_sha)
            except (OSError, MediaUnavailable):
                continue
        if verified:
            best = min(verified, key=_rank)
            return {**best, "status": "ready", "reason": None, "kind": kind,
                    "variants": verified, "source_status": "cached_content_verified"}
        return {"status": "unresolved", "reason": f"{kind}_content_link_unverified",
                "kind": kind, "variants": [], "source_status": "not_verified",
                "candidate_scan_limited": limited}

    def _process_file(self, record: dict, attachment: dict) -> dict:
        expected = str(attachment.get("md5") or attachment.get("file_md5") or "").lower()
        if _FILE_ID.fullmatch(expected) is None:
            return {"status": "unresolved", "reason": "file_link_unresolved", "kind": "file"}
        expected_size = attachment.get("total_bytes")
        if expected_size is not None and (type(expected_size) is not int or expected_size < 0):
            return {"status": "unresolved", "reason": "file_size_metadata_invalid", "kind": "file"}
        if expected_size is not None and expected_size > _MAX_FILE_BYTES:
            return {"status": "unresolved", "reason": "cached_file_size_limit", "kind": "file"}
        filename = attachment.get("filename") or ""
        evidence = "content_md5_and_size" if expected_size is not None else "content_md5"
        deadline = time.monotonic() + _FILE_COPY_SECONDS
        errors = []
        # A verified copy remains useful after WeChat clears its source cache.
        for previous in _variants(record):
            if not previous.get("raw_path"):
                continue
            try:
                blob = _blob_path(self.media_dir, previous["raw_path"])
                value = blob.lstat()
                if not stat.S_ISREG(value.st_mode) or value.st_uid != os.geteuid():
                    raise MediaUnavailable("unsafe_archive_blob")
                if expected_size is not None and value.st_size != expected_size:
                    raise MediaUnavailable("cached_file_size_mismatch")
                if value.st_size > _MAX_FILE_BYTES:
                    raise MediaUnavailable("cached_file_size_limit")
                sha, md5 = hashlib.sha256(), hashlib.md5()
                with blob.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        if time.monotonic() > deadline:
                            raise MediaUnavailable("cached_file_copy_timeout")
                        sha.update(chunk); md5.update(chunk)
                if sha.hexdigest() != blob.stem or sha.hexdigest() != previous.get("raw_sha256"):
                    raise MediaUnavailable("archive_blob_corrupt")
                if md5.hexdigest() != expected:
                    raise MediaUnavailable("cached_file_md5_mismatch")
                verified = {**previous, "raw_size": value.st_size, "content_md5": expected,
                            "association_evidence": evidence, "expected_size": expected_size,
                            "filename": filename}
                return {**verified, "status": "ready", "reason": None, "kind": "file",
                        "variants": [verified], "source_status": "archived_copy_verified"}
            except (MediaUnavailable, OSError) as exc:
                errors.append(exc.code if isinstance(exc, MediaUnavailable) else "archive_blob_missing")
        try:
            paths = self._candidates(record, attachment, "file")
        except MediaUnavailable as exc:
            return {"status": "unresolved", "reason": exc.code, "kind": "file"}
        scanned_bytes = 0
        for path in paths:
            try:
                value = path.lstat()
                if expected_size is not None and value.st_size != expected_size:
                    errors.append("cached_file_size_mismatch")
                    continue
                if path.name != filename:
                    if scanned_bytes + value.st_size > _MAX_FILE_SCAN_BYTES:
                        errors.append("file_scan_byte_limit")
                        continue
                    scanned_bytes += value.st_size
                raw_path, raw_sha = self._copy_source(path, expected_md5=expected,
                    expected_size=expected_size, max_bytes=_MAX_FILE_BYTES, deadline=deadline)
                variant = {"raw_path": raw_path, "raw_sha256": raw_sha, "raw_size": value.st_size,
                           "source_signature": [value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns],
                           "source_filename": path.name, "filename": filename, "tier": "full_cache",
                           "mime": mimetypes.guess_type(filename)[0] or "application/octet-stream",
                           "mime_source": "filename_extension", "codec": "native", "original_proven": False,
                           "content_md5": expected, "association_evidence": evidence, "expected_size": expected_size}
                return {**variant, "status": "ready", "reason": None, "kind": "file",
                        "variants": [variant], "source_status": "cached"}
            except (MediaUnavailable, OSError) as exc:
                errors.append(exc.code if isinstance(exc, MediaUnavailable) else "cached_media_unavailable")
                if errors[-1] == "cached_file_copy_timeout":
                    break
        if getattr(self, "_file_scan_limited", False):
            errors.append("file_candidate_limit")
        reason = next((code for code in ("cached_file_copy_timeout", "file_candidate_limit", "file_scan_byte_limit")
                       if code in errors), errors[0] if errors else "media_not_cached")
        return {"status": "unresolved" if errors else "missing", "kind": "file",
                "reason": reason}

    def process_pending(self, limit=100, time_budget=10.0) -> dict:
        """Process a bounded batch; an in-flight codec keeps its own timeout."""
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 1000:
            raise ValueError("media limit must be 1..1000")
        if (not isinstance(time_budget, (int, float)) or isinstance(time_budget, bool)
                or not math.isfinite(time_budget) or time_budget < 0):
            raise ValueError("media time budget must be finite and non-negative")
        deadline = time.monotonic() + time_budget
        summary = {"processed": 0, "ready": 0, "missing": 0, "failed": 0,
                   "unsupported": 0, "unresolved": 0, "budget_exhausted": False}
        for value in self.store.pending_media(limit):
            if time.monotonic() >= deadline:
                summary["budget_exhausted"] = True
                break
            record = dict(value)
            if record.get("account_id") not in (None, "", self.account_dir.name):
                self.store.update_media(record["media_ref"], source_status="different_account_not_scanned")
                continue
            try:
                fields = self._process_one(record)
            except MediaUnavailable as exc:
                fields = {"status": "failed", "reason": exc.code}
            except Exception:  # A corrupt media item must not stop other queued messages.
                fields = {"status": "failed", "reason": "media_archive_failed"}
            # Even unchanged ready rows update last_checked for queue fairness.
            self.store.update_media(record["media_ref"], **fields)
            summary["processed"] += 1
            summary[fields["status"]] = summary.get(fields["status"], 0) + 1
        return summary


class MediaService:
    """Read only archive blobs through opaque IDs, independently of source keys."""

    def __init__(self, store):
        self.store = store
        self.media_dir = _media_root(store)

    def get_media(self, reference: str, quality="best") -> dict:
        if not isinstance(reference, str) or _REFERENCE.fullmatch(reference) is None:
            raise MediaUnavailable("invalid_media_reference")
        if quality not in {"best", "full", "thumbnail", "original"}:
            raise MediaUnavailable("invalid_media_quality")
        records = self.store.get_media_records(media_ref=reference)
        if not records:
            records = self.store.get_media_records(message_ref=reference)
        if not records:
            raise MediaUnavailable("media_not_found")
        choices = []
        for value in records:
            record = dict(value)
            kind = record.get("kind") or _attachment(record).get("kind")
            for variant in _variants(record):
                if kind in {"image", "sticker"} and not variant.get("decoded_path"):
                    continue
                if not variant.get("decoded_path") and not variant.get("raw_path"):
                    continue
                if quality == "full" and variant.get("tier") == "thumbnail":
                    continue
                if quality == "thumbnail" and variant.get("tier") != "thumbnail":
                    continue
                if quality == "original" and variant.get("original_proven") is not True:
                    continue
                choices.append((record, variant))
        if not choices:
            raise MediaUnavailable("original_not_verified" if quality == "original" else "requested_media_quality_unavailable")
        record, variant = min(choices, key=lambda item: _rank(item[1]))
        decoded = bool(variant.get("decoded_path"))
        key = "decoded" if decoded else "raw"
        data = _read_blob(self.media_dir, variant[f"{key}_path"], variant.get(f"{key}_sha256"),
                          max_bytes=_MAX_INLINE_BYTES)
        metadata = {"media_ref": record["media_ref"], "message_ref": record["message_ref"],
                    "forward_ref": record.get("forward_ref"), "item_ref": record.get("component_ref"),
                    "status": "ready", "archive_updated_at": record.get("updated_at"),
                    "kind": record.get("kind") or _attachment(record).get("kind"),
                    "tier": variant.get("tier"), "width": variant.get("width"),
                    "height": variant.get("height"), "codec": variant.get("codec"),
                    "encryption_format": variant.get("encryption_format"),
                    "original_proven": variant.get("original_proven") is True,
                    "animated": variant.get("animated", False),
                    "frame_count": variant.get("frame_count", 1),
                    "selected_frame": variant.get("selected_frame", 0),
                    "original_animation_preserved": variant.get("original_animation_preserved", False),
                    "association_evidence": variant.get("association_evidence"),
                    "sha256": hashlib.sha256(data).hexdigest()}
        return {"metadata": metadata, "data": data,
                "mime_type": variant.get("mime") or "application/octet-stream"}
