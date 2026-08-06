"""SQLite bağlantı yönetimi: WAL + tek yazma kuyruğu + paralel okuyucular.

Eşzamanlılık modeli:
- TÜM yazmalar daemon içindeki tek bir yazıcı görevi üzerinden geçer (sıralı commit).
- Yazıcı bağlantısı TEK bir thread'te yaşar (max_workers=1 executor) — SQLite bağlantıları
  thread-affine olduğu için yazma operasyonları hep aynı thread'te çalışır.
- Okumalar WAL sayesinde yazmalarla paraleldir; her okuma kendi bağlantısını açar,
  açar-kullanır-kapatır (aynı thread call içinde), varsayılan executor pool'unda koşar.
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
    """Tek yazıcı (serialized, tek thread) + paralel okuma sunan SQLite sarmalayıcı."""

    def __init__(self, path: Path, busy_timeout_ms: int = 30_000) -> None:
        self.path = path
        self._busy_timeout_ms = busy_timeout_ms
        self._queue: asyncio.Queue[tuple[WriteFn, asyncio.Future] | None] = asyncio.Queue()
        self._writer_task: asyncio.Task | None = None
        self._writer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbwriter")
        self._closed = asyncio.Event()

    # ---------- bağlantılar ----------

    def connect(self) -> sqlite3.Connection:
        """Yeni bir SQLite bağlantısı (WAL, FK, busy_timeout)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=_READER_CONNECT_TIMEOUT)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    # ---------- yazma kuyruğu ----------

    async def start(self) -> None:
        self._closed.clear()
        self._writer_task = asyncio.create_task(self._writer_loop())

    async def _writer_loop(self) -> None:
        loop = asyncio.get_running_loop()
        # Bağlantı, yazma operasyonlarıyla AYNI thread'te (tek thread'lik executor) açılır
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
        with conn:  # tek transaction
            return fn(conn)

    async def write(self, fn: WriteFn) -> Any:
        """fn'i yazma kuyruğuna koyar, sıralı commit edilir, sonucu döner."""
        if self._closed.is_set():
            raise RuntimeError("database kapatıldı")
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        await self._queue.put((fn, fut))
        return await fut

    # ---------- okuma ----------

    async def read(self, fn: ReadFn) -> Any:
        """Kısa ömürlü bir okuma bağlantısı açar (WAL sayesinde yazma ile çakışmaz)."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._read_impl, fn)

    def _read_impl(self, fn: ReadFn) -> Any:
        conn = self.connect()
        try:
            return fn(conn)
        finally:
            conn.close()

    # ---------- kapanış ----------

    async def stop(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        if self._writer_task is not None:
            await self._queue.put(None)
            await asyncio.gather(self._writer_task, return_exceptions=True)
            self._writer_task = None
        self._writer_executor.shutdown(wait=True)
