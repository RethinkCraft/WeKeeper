"""Read-only Linux filesystem wakeups; durable cursors remain the authority."""
from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
import select
import struct
import time


class NativeFileWatcher:
    """Best-effort inotify hints with bounded coalescing and polling fallback.

    Watch directories rather than file inodes: SQLite replaces/removes WALs,
    and WeChat can rotate whole database files. No event carries chat content.
    A queue overflow is itself a dirty hint; callers must rescan their fences.
    """

    MASK = 0x00000002 | 0x00000004 | 0x00000008 | 0x00000040 | 0x00000080 | 0x00000100 | 0x00000200 | 0x00000400 | 0x00000800
    OVERFLOW = 0x00004000
    IGNORED = 0x00008000
    ISDIR = 0x40000000

    def __init__(self):
        self.fd = None
        self._watches = {}
        self.observed_at = None
        self.observed_monotonic = None
        self.overflow = False
        self.dirty_paths = set()
        self.rescan_all = False
        try:
            self._lib = ctypes.CDLL(None, use_errno=True)
            self._lib.inotify_init1.argtypes = [ctypes.c_int]
            self._lib.inotify_init1.restype = ctypes.c_int
            self._lib.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
            self._lib.inotify_add_watch.restype = ctypes.c_int
            self._lib.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
            fd = self._lib.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
            if fd >= 0:
                self.fd = fd
        except (AttributeError, OSError):
            pass

    @property
    def available(self):
        return self.fd is not None

    def configure(self, account_dir):
        if self.fd is None or account_dir is None:
            return
        root = Path(account_dir)
        candidates = [root, root / "db_storage", root / "db_storage/message", root / "db_storage/contact"]
        wanted = {}
        for path in candidates:
            try:
                stat = path.stat()
                if path.is_dir():
                    wanted[path] = (stat.st_dev, stat.st_ino)
            except OSError:
                pass
        for descriptor, (path, identity) in list(self._watches.items()):
            if wanted.get(path) != identity:
                self._lib.inotify_rm_watch(self.fd, descriptor)
                self._watches.pop(descriptor, None)
        existing = {path for path, _ in self._watches.values()}
        for path, identity in wanted.items():
            if path not in existing:
                descriptor = self._lib.inotify_add_watch(self.fd, os.fsencode(path), self.MASK)
                if descriptor >= 0:
                    self._watches[descriptor] = (path, identity)

    def _drain(self):
        dirty = False
        reads = 0
        while self.fd is not None and reads < 8:
            try:
                data = os.read(self.fd, 65536)
            except BlockingIOError:
                break
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                self.close()
                break
            if not data:
                break
            reads += 1
            offset = 0
            while offset + 16 <= len(data):
                descriptor, mask, _, length = struct.unpack_from("iIII", data, offset)
                raw_name = data[offset + 16:offset + 16 + length].split(b'\0', 1)[0]
                offset += 16 + length
                dirty = True
                self._record_hint(descriptor, mask, raw_name)
                if mask & self.OVERFLOW:
                    self.overflow = True
                    # Re-enumerate descriptors in configure() on the next pass.
                    for watched in list(self._watches):
                        self._lib.inotify_rm_watch(self.fd, watched)
                    self._watches.clear()
                if mask & self.IGNORED:
                    self._watches.pop(descriptor, None)
        return dirty

    def _record_hint(self, descriptor, mask, raw_name):
        """Retain bounded shard hints; an unfamiliar event requires all fences.

        File names only select candidate work. They never authorize a source,
        supply a cursor, or replace authenticated snapshot validation.
        """
        watched = self._watches.get(descriptor)
        if (watched is None or mask & (self.OVERFLOW | self.IGNORED | self.ISDIR)
                or not raw_name or b'/' in raw_name):
            self.rescan_all = True
            return
        directory = watched[0]
        name = os.fsdecode(raw_name)
        suffix = '-wal' if name.endswith('-wal') else '-shm' if name.endswith('-shm') else ''
        base = name[:-len(suffix)] if suffix else name
        if (directory.name != 'message' or directory.parent.name != 'db_storage'
                or not base.startswith('message_') or not base.endswith('.db')
                or any(skip in base for skip in ('fts', 'resource', 'media'))):
            self.rescan_all = True
            return
        self.dirty_paths.add(directory / base)
        # Main-file lifecycle/metadata changes can replace its identity or salt.
        # WAL/SHM creation and removal are ordinary writes to a known family.
        if not suffix and mask & (0x00000004 | 0x00000040 | 0x00000080 | 0x00000100 | 0x00000200):
            self.rescan_all = True

    def wait(self, timeout, *, coalesce=.02):
        self.overflow = False
        self.dirty_paths = set()
        self.rescan_all = False
        if self.fd is None:
            select.select([], [], [], timeout)
            return False
        try:
            if not select.select([self.fd], [], [], timeout)[0]:
                return False
            self.observed_at = time.time()
            self.observed_monotonic = time.monotonic()
            dirty = self._drain()
            # One deadline measured from the first hint, never extended by a
            # write storm. Additional writes cannot indefinitely defer input.
            deadline = self.observed_monotonic + coalesce
            while self.fd is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not select.select([self.fd], [], [], remaining)[0]:
                    break
                dirty = self._drain() or dirty
            return dirty
        except (OSError, ValueError):
            self.close()
            return False

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self._watches.clear()
