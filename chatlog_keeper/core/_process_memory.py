"""Path-free errors shared by process-memory readers and their callers."""

PROCESS_ACCESS_DENIED = "process_access_denied"


class ProcessMemoryAccessDenied(RuntimeError):
    """An explicit process permission denial without a PID or raw message."""

    code = PROCESS_ACCESS_DENIED

    def __init__(self) -> None:
        super().__init__(self.code)


__all__ = ["PROCESS_ACCESS_DENIED", "ProcessMemoryAccessDenied"]
