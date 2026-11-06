"""Explicit, offline cleanup for the removed automatic-summary feature."""
from __future__ import annotations

from contextlib import ExitStack
import fcntl
import os
from pathlib import Path

from .inference_state import InferenceState
from .usage import UsageLedger

SUMMARY_FILES = ("summary.sqlite", "summary.sqlite-wal", "summary.sqlite-shm", "summary.sqlite-journal",
                 "summary-worker.lock", "summary-scheduler.lock", "summary-wake", "chat-waiting", "bot-chat-waiting")
WORKER_LOCKS = ("sync.lock", "agent.lock", "agent-turn.lock", "summary-worker.lock", "summary-scheduler.lock",
                "inference.lock", "bot-inference-0.lock", "bot-inference-1.lock")


def remove_automatic_summaries(directory):
    """Migrate shared pause state, then discard only summary-owned files.

    Call after stopping the old summary, sync, agent and SIWC bot workers.
    Existing archive, credentials, conversations and actual usage are preserved.
    """
    root = Path(directory).expanduser()
    if root.is_symlink():
        raise ValueError("unsafe_dashboard_state")
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    for name in (*SUMMARY_FILES, *WORKER_LOCKS):
        if (root / name).is_symlink():
            raise ValueError("unsafe_summary_cleanup_path")
    removed = []
    with ExitStack() as stack:
        for name in WORKER_LOCKS:
            fd = os.open(root / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            stack.callback(os.close, fd)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError("dashboard_workers_must_be_stopped") from None
        state = InferenceState(root)
        try:
            pause = state.status()
        finally:
            state.close()
        ledger = UsageLedger(root)
        try:
            for run in ledger.list_running():
                if run["kind"] == "summary":
                    ledger.finish_run(run["run_id"], status="interrupted", error_code="summary_feature_removed")
        finally:
            ledger.close()
        for name in SUMMARY_FILES:
            path = root / name
            if path.exists():
                path.unlink()
                removed.append(name)
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return {"removed": removed, "inference": pause}
