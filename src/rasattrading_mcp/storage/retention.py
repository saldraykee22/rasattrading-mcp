"""Retention/compaction: candles ve futures_context sınırsız büyümesin."""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Mapping

from .db import Database

logger = logging.getLogger("rasattrading.storage.retention")


async def prune_candles(
    db: Database,
    retention_days: Mapping[str, int],
    now: float | None = None,
) -> dict[str, int]:
    """Her timeframe için cutoff öncesi mumları siler. Silinen satır sayısını döner."""
    now = now if now is not None else time.time()

    def _prune(conn: sqlite3.Connection) -> dict[str, int]:
        removed: dict[str, int] = {}
        for timeframe, days in retention_days.items():
            cutoff = int(now) - int(days * 86400)
            cur = conn.execute(
                "DELETE FROM candles WHERE timeframe = ? AND open_time < ?",
                (timeframe, cutoff),
            )
            if cur.rowcount:
                removed[timeframe] = cur.rowcount
        return removed

    result = await db.write(_prune)
    if result:
        logger.info("candles retention: %s", result)
    return result


async def prune_futures_context(db: Database, max_days: int = 30, now: float | None = None) -> int:
    """Eski futures_context kayıtlarını budar."""
    now = now if now is not None else time.time()
    cutoff = int(now) - int(max_days * 86400)

    def _prune(conn: sqlite3.Connection) -> int:
        cur = conn.execute("DELETE FROM futures_context WHERE event_time < ?", (cutoff,))
        return cur.rowcount

    removed = await db.write(_prune)
    if removed:
        logger.info("futures_context retention: %s satır", removed)
    return removed
