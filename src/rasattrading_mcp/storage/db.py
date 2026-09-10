"""SQLite connection management: WAL, a single write queue, and parallel readers.

Concurrency model:
- ALL writes go through one writer task in the daemon (serialized commits).
- The writer connection lives on ONE thread (max_workers=1 executor); SQLite
  connections are thread-affine, so writes always run on the same thread.
- WAL lets reads run alongside writes; each read opens/uses/closes its own
  connection in one thread call on the default executor pool.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, TypeVar

logger = logging.getLogger("rasattrading.storage")

T = TypeVar("T")

WriteFn = Callable[[sqlite3.Connection], T]
ReadFn = Callable[[sqlite3.Connection], T]

_READER_CONNECT_TIMEOUT = 30


class Database:
    """SQLite wrapper with one serialized writer thread and parallel reads."""

    def __init__(self, path: Path, busy_timeout_ms: int = 30_000) -> None:
        self.path = path
        self._busy_timeout_ms = busy_timeout_ms
        self._queue: asyncio.Queue[tuple[WriteFn, asyncio.Future] | None] = asyncio.Queue()
        self._writer_task: asyncio.Task | None = None
        self._writer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbwriter")
        self._closed = asyncio.Event()

    # ---------- connections ----------

    def connect(self) -> sqlite3.Connection:
        """Create a new SQLite connection (WAL, FK, busy_timeout)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=_READER_CONNECT_TIMEOUT)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    # ---------- write queue ----------

    async def start(self) -> None:
        self._closed.clear()
        self._writer_task = asyncio.create_task(self._writer_loop())

    async def _writer_loop(self) -> None:
        loop = asyncio.get_running_loop()
        # Open the connection on the SAME thread as write operations (single-thread executor).
        conn = await loop.run_in_executor(self._writer_executor, self.connect)
        try:
            while True:
                item = await self._queue.get()
                if item is None:
                    break
                fn, fut = item
                try:
                    result = await loop.run_in_executor(self._writer_executor, self._run_write, conn, fn)
                    if not fut.done():
                        fut.set_result(result)
                except Exception as exc:  # noqa: BLE001
                    if not fut.done():
                        fut.set_exception(exc)
                finally:
                    self._queue.task_done()
        finally:
            try:
                await loop.run_in_executor(self._writer_executor, conn.close)
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    def _run_write(conn: sqlite3.Connection, fn: WriteFn) -> Any:
        with conn:  # one transaction
            return fn(conn)

    async def write(self, fn: WriteFn) -> Any:
        """Queue fn for writing, commit serially, and return its result."""
        if self._closed.is_set():
            raise RuntimeError("database is closed")
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._queue.put((fn, fut))
        return await fut

    # ---------- reads ----------

    async def read(self, fn: ReadFn) -> Any:
        """Open a short-lived read connection (WAL prevents write conflicts)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._read_impl, fn)

    def _read_impl(self, fn: ReadFn) -> Any:
        conn = self.connect()
        try:
            return fn(conn)
        finally:
            conn.close()

    # ---------- shutdown ----------

    async def stop(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        if self._writer_task is not None:
            await self._queue.put(None)
            await asyncio.gather(self._writer_task, return_exceptions=True)
            self._writer_task = None
        self._writer_executor.shutdown(wait=True)
