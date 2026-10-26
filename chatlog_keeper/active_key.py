"""Linux WeChat active-key entry point.

The implementation lives in linux_key: a verified GDB hardware breakpoint,
with a fixed LD_PRELOAD observer or read-only child scan as compatible fallbacks.
No alternate-platform launchers or implicit privilege changes are provided.
"""
from __future__ import annotations

from typing import Callable, Optional

from chatlog_keeper import linux_key


def extract_wechat_key_active(
    *,
    db_path: Optional[str] = None,
    analyze_only: bool = False,
    timeout: int = 600,
    _cancel_requested: Optional[Callable[[], bool]] = None,
) -> Optional[bytes]:
    """Return a locally HMAC-verified database key, or ``None``.

    The daily client must be closed before active extraction. Only the official
    client launched by this operation is terminated on completion or cancellation.
    ``analyze_only`` inspects compatibility and prepares local helpers without
    launching WeChat. Call ``linux_key.last_error()`` for a path-free error code.
    """
    return linux_key.extract_wechat_key_active(
        db_path=db_path,
        analyze_only=analyze_only,
        timeout=timeout,
        cancel_requested=_cancel_requested,
    )


__all__ = ["extract_wechat_key_active"]
