"""Symbol-universe synchronization.

Binance spot `exchangeInfo` → all pairs with `status=TRADING` and `quoteAsset=USDT`.
Periodic resynchronization tracks newly listed and delisted symbols. If
synchronization fails, preserve the last known universe and mark the state `stale`/`error`.
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config
from .binance_client import BinanceREST

logger = logging.getLogger("rasattrading.data.universe")


class UniverseService:
    def __init__(self, rest: BinanceREST, config: Config) -> None:
        self._rest = rest
        self._refresh_seconds = config.universe_refresh_seconds
        self._symbols: list[str] = []
        self._info: dict[str, dict] = {}
        self._status = "unknown"  # unknown | ok | stale | error
        self._last_sync: float = 0.0
        self._last_error: str | None = None

    async def sync(self) -> int:
        """Fetch exchangeInfo and update the USDT/TRADING universe and symbol filters."""
        try:
            data = await self._rest.get("/api/v3/exchangeInfo", weight=20)
            entries = {s["symbol"]: s for s in data.get("symbols", [])}
            symbols = [
                s
                for s in entries.values()
                if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
            ]
            symbols.sort(key=lambda s: s["symbol"])
            self._info = entries
            self._symbols = [s["symbol"] for s in symbols]
            self._last_sync = time.time()
            self._status = "ok"
            self._last_error = None
            logger.info("symbol universe synchronized: %d USDT pairs", len(self._symbols))
            return len(self._symbols)
        except Exception as exc:  # noqa: BLE001
            self._status = "error" if not self._symbols else "stale"
            self._last_error = str(exc)
            logger.warning("could not fetch exchangeInfo (%s) — preserving universe of %d symbols", exc, len(self._symbols))
            raise

    def contains(self, symbol: str) -> bool:
        return symbol in self._symbols

    def snapshot(self) -> list[str]:
        return list(self._symbols)

    def symbol_info(self, symbol: str) -> dict | None:
        """Raw exchangeInfo symbol record (including filters); None if unknown."""
        return self._info.get(symbol)

    @property
    def status(self) -> str:
        return self._status

    @property
    def last_sync(self) -> float:
        return self._last_sync

    def last_error(self) -> str | None:
        return self._last_error

    async def ensure_contains(self, symbol: str) -> bool:
        """Check whether a symbol is in the universe; refresh it if unknown, returning False if absent."""
        if symbol in self._symbols:
            return True
        if time.time() - self._last_sync > self._refresh_seconds:
            try:
                await self.sync()
            except Exception:  # noqa: BLE001
                pass
        return symbol in self._symbols

    async def run_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._refresh_seconds)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await self.sync()
            except Exception:  # noqa: BLE001
                pass

    def status_dict(self) -> dict:
        return {
            "symbols": len(self._symbols),
            "status": self._status,
            "last_sync": self._last_sync,
            "last_error": self._last_error,
        }
