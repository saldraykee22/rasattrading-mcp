"""Veri toplama pipeline'ı: universe + miniTicker WS + kline scheduler + futures poll + retention."""

from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config
from ..storage.db import Database
from ..storage.retention import prune_candles, prune_futures_context
from .binance_client import BinanceREST
from .futures import FuturesContextPoller
from .klines import KlineService
from .miniticker import MiniTickerClient, TickerCache
from .rate_limit import RateLimitBudget
from .universe import UniverseService

logger = logging.getLogger("rasattrading.data.pipeline")

RETENTION_INTERVAL_SECONDS = 6 * 3600


class DataPipeline:
    """Tüm veri toplama bileşenlerini başlatır/durdurur ve sağlık durumu sunar."""

    def __init__(
        self,
        config: Config,
        db: Database,
        rest: BinanceREST | None = None,
        futures_rest: BinanceREST | None = None,
    ) -> None:
        self.config = config
        self.db = db
        self.budget = RateLimitBudget(
            max_weight=config.rate_limit_max_weight,
            window_seconds=config.rate_limit_window_seconds,
        )
        self.rest = rest or BinanceREST(config.rest_spot_base, self.budget)
        self.futures_rest = futures_rest or BinanceREST(config.rest_futures_base, self.budget)
        self.universe = UniverseService(self.rest, config)
        self.ticker_cache = TickerCache()
        self.miniticker = MiniTickerClient(config.ws_all_miniticker_url, self.ticker_cache)
        self.klines = KlineService(self.rest, db, self.universe, config)
        self.futures = FuturesContextPoller(self.futures_rest, db, self.universe, config)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self.started_at: float | None = None

    async def start(self) -> None:
        self.started_at = time.time()
        # Sembol evreni önce — her şey buna bağlı. Başarısızlık durumu aşağıda görünür kalır.
        try:
            await self.universe.sync()
        except Exception:  # noqa: BLE001
            logger.warning("ilk universe senkronizasyonu başarısız — arka planda tekrar denenir")

        self._tasks.append(asyncio.create_task(self.miniticker.run(self._stop)))
        await self.klines.start()
        self._tasks.append(asyncio.create_task(self.universe.run_loop(self._stop)))
        self._tasks.append(asyncio.create_task(self.futures.run_loop(self._stop)))
        self._tasks.append(asyncio.create_task(self._retention_loop()))

    async def _retention_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=RETENTION_INTERVAL_SECONDS)
                break
            except asyncio.TimeoutError:
                pass
            try:
                await prune_candles(self.db, self.config.candles_retention_days)
                await prune_futures_context(self.db)
            except Exception:  # noqa: BLE001
                logger.exception("retention budama hatası")

    async def stop(self) -> None:
        self._stop.set()
        await self.klines.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.rest.close()
        await self.futures_rest.close()

    # ---------- okuma (handler'lar için) ----------

    def get_ticker(self, symbol: str) -> dict | None:
        return self.ticker_cache.get(symbol)

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300, source: str = "spot") -> list[dict]:
        return await self.klines.get_candles(symbol, timeframe, limit, source)

    async def ensure_symbol(self, symbol: str) -> bool:
        return await self.universe.ensure_contains(symbol)

    def candle_freshness(self, symbol: str, timeframe: str, rows: list[dict]) -> str:
        return self.klines.freshness_for(symbol, timeframe, rows)

    def status(self) -> dict:
        return {
            "universe": self.universe.status_dict(),
            "ws": self.ticker_cache.health(),
            "rate_limit": self.budget.snapshot(),
            "klines": self.klines.health(),
            "futures": self.futures.status(),
        }
