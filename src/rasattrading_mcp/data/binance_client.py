"""Binance REST istemcisi: weight bütçesi + 429/418 exponential backoff.

401/403 → RasatError(UNAUTHORIZED), 400/404 → RasatError(INVALID_REQUEST)
(bilinmeyen sembol gibi — futures'ta olmayan spot çifti), 429/418 → backoff ile
retry, diğer hatalar → RasatError(INTERNAL_ERROR).
Test edilebilirlik için `session` enjekte edilebilir (FakeSession ile).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp

from ..errors import ErrorCode, RasatError
from .rate_limit import RateLimitBudget, backoff_delay

logger = logging.getLogger("rasattrading.data.binance")

SPOT_REST_BASE = "https://api.binance.com"
FUTURES_REST_BASE = "https://fapi.binance.com"


def kline_weight(limit: int) -> int:
    """GET /klines weight: limit<=100 → 1, <=500 → 2, <=1000 → 5."""
    if limit <= 100:
        return 1
    if limit <= 500:
        return 2
    return 5


class BinanceREST:
    """Weight-bütçeli Binance REST istemcisi (signed istek yok — public market data)."""

    def __init__(
        self,
        base_url: str,
        budget: RateLimitBudget,
        session: aiohttp.ClientSession | None = None,
        retries: int = 4,
        timeout_seconds: float = 20.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._budget = budget
        self._session = session
        self._retries = retries
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._own_session = session is None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def get(self, path: str, params: dict | None = None, weight: int = 1) -> Any:
        """GET isteği — bütçeyi kullanır, 429'da backoff, hataları RasatError'a çevirir."""
        await self._budget.acquire(weight)
        session = await self._get_session()
        last_exc: RasatError | None = None

        for attempt in range(self._retries):
            try:
                async with session.get(
                    f"{self.base_url}{path}", params=params, timeout=self._timeout
                ) as resp:
                    used = resp.headers.get("x-mbx-used-weight-1m")
                    if used and used.isdigit():
                        self._budget.note_used(int(used))
                    if resp.status in (429, 418):
                        last_exc = RasatError(ErrorCode.RATE_LIMITED, f"Binance rate limit ({path})")
                        await asyncio.sleep(backoff_delay(attempt))
                        continue
                    if resp.status in (401, 403):
                        raise RasatError(
                            ErrorCode.UNAUTHORIZED,
                            f"Binance {resp.status} — API key gerekiyor olabilir ({path})",
                        )
                    if resp.status in (400, 404):
                        raise RasatError(
                            ErrorCode.INVALID_REQUEST,
                            f"Binance {resp.status} — geçersiz istek ({path})",
                        )
                    resp.raise_for_status()
                    ctype = resp.headers.get("Content-Type", "")
                    if "json" in ctype.lower():
                        return await resp.json()
                    return await resp.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = RasatError(ErrorCode.INTERNAL_ERROR, f"Binance istek hatası ({path}): {exc}")
                if attempt < self._retries - 1:
                    await asyncio.sleep(backoff_delay(attempt, base=0.5))
            except RasatError:
                raise

        raise last_exc or RasatError(ErrorCode.INTERNAL_ERROR, f"Binance istek başarısız: {path}")

    async def close(self) -> None:
        if self._own_session and self._session is not None and not self._session.closed:
            await self._session.close()
