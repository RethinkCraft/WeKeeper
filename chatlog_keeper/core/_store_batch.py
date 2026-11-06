"""One durable source transaction around legacy ArchiveStore method contexts.

The schema-4 archive remains untouched. Its nested ``with self.conn`` blocks
delegate commit/rollback to this owner, while the store lock excludes every
other user of the temporarily installed proxy.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import sqlite3
import time


class StoreBatchAborted(RuntimeError):
    pass


@dataclass
class StoreBatch:
    active: bool = False
    committed_at: float | None = None
    commit_seconds: float = 0
    writer_wait_seconds: float = 0


class _Connection:
    def __init__(self, connection):
        self.raw = connection
        self.failed = False

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        if kind is not None:
            self.failed = True
        return False

    def execute(self, sql, *args, **kwargs):
        if sql.lstrip().split(None, 1)[0].upper() in {
                'BEGIN', 'COMMIT', 'END', 'ROLLBACK', 'SAVEPOINT', 'RELEASE'}:
            self.commit()
        return self.raw.execute(sql, *args, **kwargs)

    def commit(self, *args, **kwargs):
        self.failed = True
        raise StoreBatchAborted('archive_batch_explicit_boundary_forbidden')

    rollback = commit
    close = commit
    executescript = commit

    def __getattr__(self, name):
        return getattr(self.raw, name)


@contextmanager
def source_batch(store, *, enabled):
    """Commit live cursors, rows and source availability exactly once.

    Pure-Python test stores and ordinary collection retain their old contract.
    A real connection may not enter with an unrelated transaction in progress.
    An exception from a nested legacy context poisons the batch even if a
    caller catches it: partial writes must never become a successful source.
    """
    connection = getattr(store, 'conn', None)
    lock = getattr(store, '_lock', None)
    result = StoreBatch()
    if not enabled or not isinstance(connection, sqlite3.Connection) or lock is None:
        yield result
        return
    waiting = time.perf_counter()
    with lock:
        if store.conn is not connection or connection.in_transaction:
            raise StoreBatchAborted('archive_batch_existing_transaction')
        proxy = _Connection(connection)
        connection.execute('BEGIN IMMEDIATE')
        result.writer_wait_seconds = time.perf_counter() - waiting
        result.active = True
        store.conn = proxy
        try:
            yield result
            if proxy.failed:
                raise StoreBatchAborted('archive_batch_nested_operation_failed')
            started = time.perf_counter()
            connection.commit()
            result.commit_seconds = time.perf_counter() - started
            result.committed_at = time.time()
        except BaseException:
            try:
                connection.rollback()
            except BaseException:
                # A connection whose rollback failed cannot be used by the
                # outer collector's status handler to commit a partial batch.
                try:
                    connection.close()
                except BaseException:
                    pass
            raise
        finally:
            store.conn = connection
