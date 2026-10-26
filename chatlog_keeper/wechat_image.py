"""Local Linux WeChat image decoding: legacy XOR, V1/V2 AES, and WXGF.

V2 image keys are derived from a private, account-matched UIN cache and the
normalized account name. A real encrypted image validates every candidate.
Database keys and XML/CDN AES keys are unrelated to this local image key.
No process scanning, UI automation, network requests, or OCR is performed.
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
import struct
from typing import Optional

from chatlog_keeper import image_key
from chatlog_keeper.core._secrets import read_secret_text, write_secret_text

logger = logging.getLogger(__name__)
_JPEG_HEAD = b"\xff\xd8\xff"
_PNG_HEAD = b"\x89PNG"
_GIF_HEAD = b"GIF8"
_WEBP_HEAD = b"RIFF"
_WXGF_HEAD = b"wxgf"
_V1_SIG = b"\x07\x08V1\x08\x07"
_V2_SIG = b"\x07\x08V2\x08\x07"
_V1_FIXED_AES_KEY = b"cfcd208495d565ef"
_DEFAULT_XOR_KEY = 0x88
_MAX_DAT_BYTES = 64 * 1024 * 1024
_BLANK_NONWHITE_RATIO = 0.01

# Retain these private hooks for the existing narrow PoC harness. Persistent
# state now uses the package's per-user data directory, never the source tree.
_V2_UIN_CACHE_PATH_FN = lambda: image_key.secret_path("wechat_v2_uin.txt")
_V2_KEY_CACHE_PATH_FN = lambda: image_key.secret_path("wechat_v2_image_aes.key")
_V2_KEY_CACHE: Optional[bytes] = None
_V2_XOR_KEY_OVERRIDE: Optional[int] = None


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _derive_v1_key(head: bytes) -> Optional[int]:
    if len(head) < 4:
        return None
    for magic in (_JPEG_HEAD, _PNG_HEAD, _GIF_HEAD):
        key = head[0] ^ magic[0]
        if bytes(value ^ key for value in head[:len(magic)]) == magic:
            return key
    return None


def _detect_encrypt_variant(head: bytes) -> str:
    if head.startswith(_V1_SIG):
        return "v1"
    if head.startswith(_V2_SIG):
        return "v2"
    return "legacy_xor" if _derive_v1_key(head) is not None else "unknown"


def _xor_decrypt(data: bytes, key: int) -> bytes:
    return bytes(value ^ key for value in data)


def _aligned_aes_size(aes_size: int) -> int:
    # PKCS7 always pads, including an extra block when already aligned.
    return aes_size + (16 - aes_size % 16)


def _parse_v1v2_header(data: bytes):
    if len(data) < 15:
        return None, 0, 0
    aes_size, xor_size = struct.unpack_from("<LL", data, 6)
    return data[:6], aes_size, xor_size


def _decrypt_v1v2(data: bytes, aes_key: bytes, xor_key: int = _DEFAULT_XOR_KEY) -> Optional[bytes]:
    """Decode the 15-byte header + padded AES-ECB + raw + XOR layout."""
    from Crypto.Cipher import AES
    from Crypto.Util.Padding import unpad
    sig, aes_size, xor_size = _parse_v1v2_header(data)
    if sig not in (_V1_SIG, _V2_SIG) or not 0 < aes_size <= _MAX_DAT_BYTES:
        return None
    if len(aes_key) != 16 or not isinstance(xor_key, int) or not 0 <= xor_key <= 255:
        return None
    aligned = _aligned_aes_size(aes_size)
    end_aes = 15 + aligned
    if end_aes > len(data) or xor_size > len(data) - end_aes:
        return None
    try:
        prefix = unpad(AES.new(bytes(aes_key), AES.MODE_ECB).decrypt(data[15:end_aes]), 16)
    except (ValueError, KeyError):
        return None
    if len(prefix) != aes_size:
        return None
    raw_end = len(data) - xor_size
    return prefix + data[end_aes:raw_end] + _xor_decrypt(data[raw_end:], xor_key)


def derive_v2_key_from_uin(uin: int, wxid: str) -> bytes:
    """Derive a candidate; validate it against the selected real V2 image."""
    return hashlib.md5(f"{uin}{wxid}".encode("utf-8")).hexdigest()[:16].encode("ascii")


def _validate_v2_key(candidate: bytes, v2_dat_path: Path) -> bool:
    from Crypto.Cipher import AES
    try:
        with Path(v2_dat_path).open("rb") as handle:
            data = handle.read(15 + 16)
        sig, aes_size, _ = _parse_v1v2_header(data)
        if sig != _V2_SIG or aes_size < 16 or len(data) < 31 or len(candidate) != 16:
            return False
        decoded = AES.new(candidate, AES.MODE_ECB).decrypt(data[15:31])
    except (OSError, ValueError, TypeError):
        return False
    return (decoded.startswith((_JPEG_HEAD, _PNG_HEAD, _GIF_HEAD, _WXGF_HEAD))
            or (decoded.startswith(_WEBP_HEAD) and decoded[8:12] == b"WEBP"))


def load_cached_v2_key() -> Optional[bytes]:
    text = read_secret_text(_V2_KEY_CACHE_PATH_FN(), max_bytes=128)
    if text is None:
        return None
    text = text.strip().lstrip("\ufeff")
    if len(text) == 16 and text.isascii() and text.isalnum():
        return text.encode("ascii")
    if len(text) == 32 and all(c in "0123456789abcdefABCDEF" for c in text):
        return bytes.fromhex(text)
    return None


def save_cached_v2_key(key: bytes) -> bool:
    if not isinstance(key, (bytes, bytearray)) or len(key) != 16:
        return False
    key = bytes(key)
    text = key.decode("ascii") if key.isalnum() and key.isascii() else key.hex()
    return write_secret_text(_V2_KEY_CACHE_PATH_FN(), text)


def _attach_root_for_image(path: Path) -> Optional[Path]:
    # For .../<account>/msg/attach/<chat>/<month>/Img/file.dat the attach root
    # is four parents up, not three. Find named components rather than count.
    for parent in Path(path).parents:
        if parent.name == "attach" and parent.parent.name == "msg":
            return parent
    return None


def derive_v2_xor_key(attach_root: Path) -> Optional[int]:
    """Read private UIN material, preserving zero; never guess from other images."""
    if _V2_XOR_KEY_OVERRIDE is not None:
        return _V2_XOR_KEY_OVERRIDE
    root = Path(attach_root)
    account = root.parent.parent if root.name == "attach" and root.parent.name == "msg" else None
    uin = image_key.load_cached_uin(account, cache_path=_V2_UIN_CACHE_PATH_FN())
    return uin & 0xff if uin is not None else None


def _nonwhite_ratio(jpeg_bytes: bytes) -> float:
    """Fraction of non-white pixels in a JPEG (downsampled grayscale).

    Returns -1.0 if PIL is unavailable / decode fails (caller treats as
    "unknown" → keeps single-frame behavior, no regression). A pure-white
    placeholder frame returns ~0.0; a real screenshot returns >> 0.01.
    """
    try:
        import io as _io
        from PIL import Image
        im = Image.open(_io.BytesIO(jpeg_bytes)).convert("L")
        im.thumbnail((128, 128))
        pixels = im.get_flattened_data() if hasattr(im, "get_flattened_data") else im.getdata()
        px = list(pixels)
        if not px:
            return 0.0
        return sum(1 for p in px if p < 240) / len(px)
    except Exception:
        return -1.0


def wxgf_to_jpeg(wxgf_bytes: bytes, timeout_s: float = 15.0) -> Optional[bytes]:
    """Convert wxgf (WeChat custom HEVC Main Still Picture) to JPEG via ffmpeg.

    wxgf layout (reverse-engineered):
      [4B "wxgf"] [variable-len wechat metadata, can be 16B-1KB+]
      [HEVC NAL bitstream, may start with 4-byte (00 00 00 01) or 3-byte
       (00 00 01) Annex-B start code]

    Strategy (full coverage):
      1) Search the NAL start code in the first 8KB (metadata routinely runs
         > 256B, so a 256B window only transcoded ~60% of wxgf).
      2) Try both 4-byte and 3-byte Annex-B markers.
      3) Fallback A: feed entire wxgf (post-"wxgf" magic) to ffmpeg and let
         ffmpeg's stream-detect figure it out.
      4) Fallback B: append a synthesized HEVC SPS/PPS header (last-resort).
    """
    if wxgf_bytes[:4] != _WXGF_HEAD:
        return None

    # Strategy 1+2: scan first 8KB for NAL start code (4-byte or 3-byte)
    nal_off = -1
    head_search = wxgf_bytes[:8192]
    for i in range(4, len(head_search) - 4):
        if head_search[i:i+4] == b"\x00\x00\x00\x01":
            nal_off = i
            break
        if head_search[i:i+3] == b"\x00\x00\x01":
            nal_off = i
            break

    import subprocess
    import shutil
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        try:
            import imageio_ffmpeg
            ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
        except (ImportError, RuntimeError, OSError):
            logger.warning("wxgf_to_jpeg: no FFmpeg executable available")
            return None

    def _try_ffmpeg(stream: bytes) -> Optional[bytes]:
        if not stream:
            return None
        try:
            r = subprocess.run(
                [ffmpeg, "-loglevel", "error", "-y",
                 "-f", "hevc", "-i", "pipe:0",
                 "-vframes", "1", "-q:v", "2",
                 "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"],
                input=stream,
                capture_output=True,
                timeout=timeout_s,
            )
            if (r.returncode == 0 and len(r.stdout) > 1000
                    and r.stdout[:3] == _JPEG_HEAD):
                return r.stdout
            return None
        except (subprocess.TimeoutExpired, OSError):
            return None

    def _try_ffmpeg_multiframe(stream: bytes, n: int = 8) -> Optional[bytes]:
        # Some wxgf are 2-frame grayscale HEVC where frame-1 is a pure-white
        # placeholder and frame-2 carries the real content (e.g. some
        # screenshots). `-vframes 1` grabbed the white frame → vision saw blank.
        # Extract up to n frames and return the one with the most non-white
        # pixels. Single-frame wxgf never reaches here (frame-1 not blank).
        if not stream:
            return None
        import glob as _glob
        import os as _os
        import tempfile as _tf
        try:
            with _tf.TemporaryDirectory() as td:
                pat = _os.path.join(td, "f_%03d.jpg")
                subprocess.run(
                    [ffmpeg, "-loglevel", "error", "-y",
                     "-f", "hevc", "-i", "pipe:0",
                     "-frames:v", str(n), "-q:v", "2", pat],
                    input=stream, capture_output=True, timeout=timeout_s,
                )
                best, best_r = None, -1.0
                for fp in sorted(_glob.glob(_os.path.join(td, "f_*.jpg"))):
                    try:
                        b = open(fp, "rb").read()
                    except OSError:
                        continue
                    if len(b) <= 1000 or b[:3] != _JPEG_HEAD:
                        continue
                    rr = _nonwhite_ratio(b)
                    if rr > best_r:
                        best, best_r = b, rr
                return best
        except (subprocess.TimeoutExpired, OSError):
            return None

    def _extract(stream: bytes) -> Optional[bytes]:
        """Single-frame first (byte-identical to legacy for normal images);
        if that frame is blank-white, recover the best content frame."""
        out = _try_ffmpeg(stream)
        if not out:
            return None
        r = _nonwhite_ratio(out)
        if 0.0 <= r < _BLANK_NONWHITE_RATIO:  # frame-1 white (PIL ok) → try frame-2+
            better = _try_ffmpeg_multiframe(stream)
            if better is not None and _nonwhite_ratio(better) > r:
                return better
        return out  # r==-1 (no PIL) or non-blank → legacy behavior, zero regression

    # Attempt 1: stream from found NAL marker
    if nal_off >= 0:
        out = _extract(wxgf_bytes[nal_off:])
        if out:
            return out

    # Fallback A: feed entire payload after "wxgf" magic (skip 4-byte header)
    # — ffmpeg's HEVC demuxer may auto-detect later NAL markers in big files.
    out = _extract(wxgf_bytes[4:])
    if out:
        return out

    # Fallback B: try various metadata-size guesses (16/32/64 byte headers)
    for skip in (16, 32, 64, 128, 256, 512, 1024):
        if skip >= len(wxgf_bytes):
            break
        out = _extract(wxgf_bytes[skip:])
        if out:
            return out

    logger.debug(
        f"wxgf_to_jpeg: all fallbacks failed (size={len(wxgf_bytes)} "
        f"nal_off={nal_off} head_hex={wxgf_bytes[:32].hex()})"
    )
    return None



def decode_image(path: Path, account_dir: Path | None = None) -> Optional[bytes]:
    """Decode one explicitly selected local image using private cached material.

    Return JPEG/PNG/GIF/WEBP bytes, or raw WXGF when FFmpeg cannot transcode.
    Return None for unavailable/unverified key material or malformed input.
    Callers should fully load the image before reporting successful rendering.
    This function neither downloads missing media nor discovers a UIN.
    """
    global _V2_KEY_CACHE
    path = Path(path)
    try:
        if path.is_symlink() or path.stat().st_size > _MAX_DAT_BYTES:
            return None
        data = path.read_bytes()
    except OSError:
        return None
    variant = _detect_encrypt_variant(data[:16])
    if variant == "legacy_xor":
        key = _derive_v1_key(data[:16])
        return _xor_decrypt(data, key) if key is not None else None
    if variant == "v1":
        return _decrypt_v1v2(data, _V1_FIXED_AES_KEY, _DEFAULT_XOR_KEY)
    if variant != "v2":
        return None
    attach_root = _attach_root_for_image(path)
    account = Path(account_dir) if account_dir is not None else (attach_root.parent.parent if attach_root else None)
    uin = image_key.load_cached_uin(account, cache_path=_V2_UIN_CACHE_PATH_FN())
    key, xor = None, None
    if uin is not None and account is not None:
        for name in image_key.normalized_account_names(account.name):
            candidate = derive_v2_key_from_uin(uin, name)
            if _validate_v2_key(candidate, path):
                key, xor = candidate, uin & 0xff
                break
    if key is None:
        candidate = load_cached_v2_key()
        if candidate is None and _V2_KEY_CACHE is not None:
            candidate = _V2_KEY_CACHE
        if candidate is not None and _validate_v2_key(candidate, path):
            key = candidate
            # A UIN which failed to derive the validated AES candidate must
            # not supply an unverified XOR byte through this fallback.
            if _parse_v1v2_header(data)[2] == 0:
                xor = 0
            elif _V2_XOR_KEY_OVERRIDE is not None:
                xor = _V2_XOR_KEY_OVERRIDE
    if key is None or xor is None:
        return None
    result = _decrypt_v1v2(data, key, xor)
    if result is None:
        return None
    _V2_KEY_CACHE = key
    if result.startswith(_WXGF_HEAD):
        return wxgf_to_jpeg(result) or result
    return result


def decrypt_wechat_dat(path: Path) -> Optional[bytes]:
    """Compatibility wrapper for decode_image; infer canonical account layout."""
    return decode_image(path)
