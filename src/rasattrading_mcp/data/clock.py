"""Adjustable clock based on Binance server time (bounded server-time offset).

Closed-candle and freshness decisions cannot depend on the local host clock: if
the host clock drifts, a forming bar may be treated as "closed" or a current bar
as "stale". This clock calculates the server-client offset using `/api/v3/time`,
keeps it bounded (an excessively large offset is considered unreliable), and
returns reliable server time from `server_now()`.

Fail-closed contract: if the offset was never obtained or has not been refreshed
within `max_offset_age_seconds`, `server_now()` returns `None`; callers cannot
declare a bar "closed" or mark data "fresh" (`klines._store` does not store it,
and `freshness_for` returns `stale`).
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..errors import ErrorCode, RasatError
from ..timeutil import to_epoch_seconds
from .binance_client import BinanceREST

logger = logging.getLogger("rasattrading.data.clock")


class BinanceClock:
    """Injectable clock with a Binance server-time offset.

    `sync()` fetches `/api/v3/time` and stores the `server - local` offset. It is
    refreshed periodically in the background (`start()`/`stop()`); if it cannot
    be refreshed, `server_now()` returns `None` (fail closed).
    """

    def __init__(
        self,
        rest: BinanceREST,
        *,
        refresh_seconds: float = 60.0,
        max_offset_age_seconds: float = 300.0,
        max_offset_seconds: float = 3600.0,
    ) -> None:
        self._rest = rest
        self._refresh_seconds = refresh_seconds
        self._max_offset_age_seconds = max_offset_age_seconds
        self._max_offset_seconds = max_offset_seconds
        self._offset: float | None = None
        self._synced_at: float = 0.0
        self._last_error: str | None = None
        self._sync_lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    async def sync(self) -> bool:
        """Fetch server time and update the offset; return success (never raise)."""
        async with self._sync_lock:
            try:
                data = await self._rest.get("/api/v3/time", weight=1)
                server_ms = data.get("serverTime")
                if not server_ms:
                    raise RasatError(ErrorCode.INTERNAL_ERROR, "serverTime field is missing")
                server = to_epoch_seconds(int(server_ms))
                offset = server - time.time()
                if abs(offset) > self._max_offset_seconds:
                    self._offset = None
                    self._last_error = f"host clock drift is excessive (offset={offset:.0f}s)"
                    logger.warning("server clock offset is outside the safe bound: %.0fs", offset)
                    return False
                self._offset = offset
                self._synced_at = time.time()
                self._last_error = None
                return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("server clock synchronization failed: %s", exc)
                return False

    def server_now(self) -> float | None:
        """Reliable server time in seconds; return None if the offset is missing or stale."""
        if self._offset is None:
            return None
        if time.time() - self._synced_at > self._max_offset_age_seconds:
            return None
        return time.time() + self._offset

    @property
    def offset(self) -> float | None:
        """Current (fresh) offset; return None if missing or stale."""
        if self.server_now() is None:
            return None
        return self._offset

    @property
    def available(self) -> bool:
        return self.server_now() is not None

    @property
    def last_error(self) -> str | None:
        return self._last_error

    def start(self) -> None:
        """Start the periodic refresh loop (idempotent)."""
        if self._task is None:
            self._task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._refresh_seconds)
                await self.sync()
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.warning("clock refresh failed", exc_info=True)
