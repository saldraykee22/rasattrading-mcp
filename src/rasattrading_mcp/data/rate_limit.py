"""Binance REST weight budget.

Each endpoint has a known static weight; `acquire(weight)` reserves the planned
usage before a request and queues while the limit is full until the window resets.
As the server's `x-mbx-used-weight-1m` header arrives, update actual usage through
`note_used`. Requests wait rather than raising on exhaustion, so the system continues.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time

logger = logging.getLogger("rasattrading.data.rate_limit")


class RateLimitBudget:
    """Per-window weight budget (default 60s / 6000 weight)."""

    def __init__(self, max_weight: int, window_seconds: float = 60.0) -> None:
        self.max_weight = max_weight
        self.window_seconds = window_seconds
        self._used: float = 0.0
        self._window_start = time.monotonic()
        self._lock = asyncio.Lock()
        self.total_waits = 0

    async def acquire(self, weight: int) -> None:
        """Reserve budget for a request of the given weight; wait until reset if needed."""
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
                # Small jitter prevents all waiters from waking at once.
                await asyncio.sleep(min(wait, 5.0) + random.uniform(0, 0.2))

    def note_used(self, used_weight: int) -> None:
        """Actual usage reported by the server (`x-mbx-used-weight-1m`)."""
        # Approximate because the window may shift; err on the safe (higher) side.
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
    """Exponential backoff in seconds for 429/418."""
    delay = min(base * (2 ** attempt), max_delay)
    if jitter:
        delay *= random.uniform(0.8, 1.2)
    return delay
