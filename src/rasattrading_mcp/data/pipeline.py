"""Data-collection pipeline: universe + miniTicker WS + kline scheduler + futures poll + liquidation WS + retention."""

from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config
from ..storage.db import Database
from ..storage.retention import prune_candles, prune_futures_context
from .binance_client import BinanceREST
from .clock import BinanceClock
from .futures import FuturesContextPoller
from .klines import KlineService
from .liquidation_ws import LiquidationWSClient
from .miniticker import MiniTickerClient, TickerCache
from .rate_limit import RateLimitBudget
from .universe import UniverseService

logger = logging.getLogger("rasattrading.data.pipeline")

RETENTION_INTERVAL_SECONDS = 6 * 3600


class DataPipeline:
    """Start/stop all data-collection components and expose health status."""

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
        self.clock = BinanceClock(self.rest)
        self.ticker_cache = TickerCache()
        self.miniticker = MiniTickerClient(config.ws_all_miniticker_url, self.ticker_cache)
        self.liquidation_ws = LiquidationWSClient(config.ws_force_order_url, db)
        self.klines = KlineService(self.rest, self.futures_rest, db, self.universe, config, self.clock)
        self.futures = FuturesContextPoller(self.futures_rest, db, self.universe, config)
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self.started_at: float | None = None

    async def start(self) -> None:
        self.started_at = time.time()
        # Symbol universe first — everything depends on it. Preserve failure status below.
        try:
            await self.universe.sync()
        except Exception:  # noqa: BLE001
            logger.warning("initial universe synchronization failed — retrying in the background")
        # Server clock: basis for close/freshness decisions. If the initial sync fails,
        # kline storage fails closed (stale); the background loop retries.
        if not await self.clock.sync():
            logger.warning("initial server clock synchronization failed — kline data remains stale fail closed")
        self.clock.start()

        self._tasks.append(asyncio.create_task(self.miniticker.run(self._stop)))
        self._tasks.append(asyncio.create_task(self.liquidation_ws.run(self._stop)))
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
                logger.exception("retention pruning failed")

    async def stop(self) -> None:
        self._stop.set()
        await self.klines.stop()
        await self.clock.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.rest.close()
        await self.futures_rest.close()

    # ---------- reads (for handlers) ----------

    def get_ticker(self, symbol: str) -> dict | None:
        return self.ticker_cache.get(symbol)

    def symbol_info(self, symbol: str) -> dict | None:
        return self.universe.symbol_info(symbol)

    def universe_snapshot(self) -> list[str]:
        return self.universe.snapshot()

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300, source: str = "spot") -> list[dict]:
        return await self.klines.get_candles(symbol, timeframe, limit, source)

    async def ensure_symbol(self, symbol: str) -> bool:
        return await self.universe.ensure_contains(symbol)

    def candle_freshness(self, symbol: str, timeframe: str, rows: list[dict]) -> str:
        return self.klines.freshness_for(symbol, timeframe, rows)

    def status(self) -> dict:
        # REST liquidation polling was removed; derive state from the WS client
        # (connected/disconnected) and write it to the poller's `_last_status["liquidation"]`
        # when reading.
        self.futures.set_liquidation_status(self.liquidation_ws.status)
        return {
            "universe": self.universe.status_dict(),
            "ws": self.ticker_cache.health(),
            "rate_limit": self.budget.snapshot(),
            "klines": self.klines.health(),
            "futures": self.futures.status(),
        }
