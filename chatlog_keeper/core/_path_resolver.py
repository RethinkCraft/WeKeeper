"""Resolve local writable state for the Linux WeChat reader."""
from __future__ import annotations

import os
from pathlib import Path


def data_dir() -> Path:
    """Use CHATLOG_KEEPER_DATA_DIR or the current user's XDG data directory."""
    override = os.environ.get("CHATLOG_KEEPER_DATA_DIR")
    if override:
        directory = Path(override).expanduser()
    else:
        xdg = os.environ.get("XDG_DATA_HOME")
        base = Path(xdg) if xdg else Path.home() / ".local" / "share"
        directory = base / "chatlog-keeper"
    directory.mkdir(parents=True, exist_ok=True)
    return directory
