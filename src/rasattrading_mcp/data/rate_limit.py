"""Binance REST weight bütçesi.

Her endpoint'in statik weight'i bilinir; `acquire(weight)` istek öncesi planlanan
kullanımı ayırır, limit doluyken pencere sıfırlanana kadar bekler (kuyruklama).
Sunucunun `x-mbx-used-weight-1m` başlığı geldikçe `note_used` ile gerçek kullanıma
güncellenir. Aşım durumunda istekler hata fırlatmaz, bekler — sistem durmaz.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time

logger = logging.getLogger("rasattrading.data.rate_limit")


class RateLimitBudget:
    """Pencere başına weight bütçesi (varsayılan 60sn / 6000 weight)."""

    def __init__(self, max_weight: int, window_seconds: float = 60.0) -> None:
        self.max_weight = max_weight
        self.window_seconds = window_seconds
        self._used: float = 0.0
        self._window_start = time.monotonic()
        self._lock = asyncio.Lock()
        self.total_waits = 0

    async def acquire(self, weight: int) -> None:
        """weight'lik bir istek için bütçe ayırır; gerekirse pencere sıfırlanana dek bekler."""
        async with self._lock:
            while True:
                now = time.monotonic()
                if now - self._window_start >= self.window_seconds:
                    self._used = 0.0
                    self._window_start = now
                if self._used + weight <= self.max_weight:
                    self._used += weight
                    return
                self.total_waits += 1
                wait = (self._window_start + self.window_seconds) - now
                # Küçük jitter: aynı anda bekleyenler aynı anda patlamasın
                await asyncio.sleep(min(wait, 5.0) + random.uniform(0, 0.2))

    def note_used(self, used_weight: int) -> None:
        """Sunucudan gelen gerçek kullanım (`x-mbx-used-weight-1m`)."""
        # Pencere kayması nedeniyle yaklaşıktır; güvenli yöne (fazla) sapar.
        if used_weight > self._used:
            self._used = float(used_weight)

    def snapshot(self) -> dict:
        now = time.monotonic()
        elapsed = now - self._window_start
        remaining = max(0.0, self.window_seconds - elapsed)
        return {
            "used_weight": round(self._used, 1),
            "max_weight": self.max_weight,
            "window_remaining_s": round(remaining, 1),
            "total_waits": self.total_waits,
        }


def backoff_delay(attempt: int, base: float = 1.0, max_delay: float = 60.0, jitter: bool = True) -> float:
    """429/418 için exponential backoff (saniye)."""
    delay = min(base * (2 ** attempt), max_delay)
    if jitter:
        delay *= random.uniform(0.8, 1.2)
    return delay
