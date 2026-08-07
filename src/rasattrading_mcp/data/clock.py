"""Binance sunucu saatine göre ayarlanabilir clock (bounded server-time offset).

Kapalı mum kapanışı ve freshness kararları yerel host saatine bağlanamaz: host
saati kayarsa oluşmakta olan bar "kapanmış" sanılabilir veya güncel bar "stale"
sayılabilir. Bu clock `/api/v3/time` ile sunucu-istemci offset'ini hesaplar,
bounded tutar (offset aşırı büyükse güvenilmez sayılır) ve `server_now()`
güvenilir sunucu zamanını döndürür.

Fail-closed sözleşmesi: offset hiç alınamadıysa veya `max_offset_age_seconds`
içinde tazelenmediyse `server_now()` `None` döner — arayan barı "kapanmış"
sayamaz / veriyi "fresh" işaretleyemez (klines._store saklamaz, freshness_for
`stale` döner).
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
    """Binance server time offset'li injectable clock.

    `sync()` `/api/v3/time` çeker ve `server - local` offset'ini saklar. Arka
    planda periyodik tazelenir (`start()`/`stop()`); tazelenemezse `server_now()`
    `None` döner (fail-closed).
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
        """Sunucu saatini çekip offset'i günceller; başarı/sonuç döner (asla fırlatmaz)."""
        async with self._sync_lock:
            try:
                data = await self._rest.get("/api/v3/time", weight=1)
                server_ms = data.get("serverTime")
                if not server_ms:
                    raise RasatError(ErrorCode.INTERNAL_ERROR, "serverTime alanı yok")
                server = to_epoch_seconds(int(server_ms))
                offset = server - time.time()
                if abs(offset) > self._max_offset_seconds:
                    self._offset = None
                    self._last_error = f"host saat kayması aşırı (offset={offset:.0f}s)"
                    logger.warning("server clock offset'i güvenli sınırın dışında: %.0fs", offset)
                    return False
                self._offset = offset
                self._synced_at = time.time()
                self._last_error = None
                return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._last_error = str(exc)
                logger.warning("server clock senkronu başarısız: %s", exc)
                return False

    def server_now(self) -> float | None:
        """Güvenilir sunucu zamanı (saniye); offset yoksa veya stale ise None."""
        if self._offset is None:
            return None
        if time.time() - self._synced_at > self._max_offset_age_seconds:
            return None
        return time.time() + self._offset

    @property
    def offset(self) -> float | None:
        """Geçerli (taze) offset; yoksa veya stale ise None."""
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
        """Periyodik tazeleme döngüsünü başlatır (idempotent)."""
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
                logger.warning("clock refresh hatası", exc_info=True)
