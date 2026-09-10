"""Retention/compaction: keep candles and futures_context from growing without bound."""

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
    """Delete candles before the cutoff for each timeframe and return deleted row counts."""
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
    """Prune old futures_context records."""
    now = now if now is not None else time.time()
    cutoff = int(now) - int(max_days * 86400)

    def _prune(conn: sqlite3.Connection) -> int:
        cur = conn.execute("DELETE FROM futures_context WHERE event_time < ?", (cutoff,))
        return cur.rowcount

    removed = await db.write(_prune)
    if removed:
        logger.info("futures_context retention: %s rows", removed)
    return removed
