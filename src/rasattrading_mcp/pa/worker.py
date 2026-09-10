"""2.8 — Background PA computation worker.

Plan Section 2.3: continuously keep the fixed timeframe set (15m/1h/4h/1d) current.
When a timeframe's closed bar updates, this worker recomputes the PA chain for
universe symbols automatically, without waiting for the agent's
`get_market_structure` call. If candle data has not reached the target closed bar
(stale / kline scheduler has not finished), skip computation and process it on the
next pass after refresh. Limit concurrency with a semaphore.

2.18: **Persistent insufficient data** (newly listed symbols such as tokenized
stocks lacking `PA_LOOKBACK` closed candles) does not block a pass: mark those
symbols `insufficient`, advance `_last_processed`, and retry when a new closed bar
arrives. Otherwise one blocking symbol (e.g. SMCIBUSDT with two 1d candles) would
reprocess all 489 symbols on every loop.
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config, TIMEFRAME_SECONDS
from ..envelope import FRESHNESS_FRESH
from ..errors import ErrorCode, RasatError
from .analysis import PAEngine, PA_LOOKBACK

logger = logging.getLogger("rasattrading.pa.worker")


class PAWorker:
    def __init__(self, engine: PAEngine, universe, config: Config) -> None:
        self.engine = engine
        self.universe = universe
        self.config = config
        self._last_processed: dict[str, int] = {}
        self._sem = asyncio.Semaphore(config.pa_worker_concurrency)

    @staticmethod
    def latest_closed(tf: str, now: float | None = None) -> int:
        """`open_time` of the timeframe's last closed candle (server clock, T2).

        If `now` is omitted, fall back to local time; `PAWorker` passes `engine._now()`
        so passes remain consistent with the server clock.
        """
        period = TIMEFRAME_SECONDS[tf]
        if now is None:
            now = time.time()
        return int(now // period) * period - period

    async def run(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.pa_check_seconds)
                await self.check_and_process()
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("PA worker loop failed")

    async def check_and_process(self) -> int:
        """One pass: recompute PA for timeframes with a new closed bar.

        Return the number of processed (successful) symbols (observability/diagnostics).
        Process every timeframe on the first pass (cold-universe warm-up); `_last_processed`
        is empty. Advance the marker only when a timeframe pass is COMPLETELY successful:
        retry the same closed bar on the next pass if any symbol is stale/failed/empty
        (2.12 fix). **Persistent insufficient-data symbols (`insufficient`, 2.18) do
        not block a pass**; retry them when a new closed bar arrives.
        """
        processed = 0
        now = self.engine._now()
        for tf in self.config.kline_intervals:
            latest_closed = self.latest_closed(tf, now=now)
            if self._last_processed.get(tf) == latest_closed:
                continue
            symbols = sorted(self.universe.snapshot())
            if not symbols:
                continue
            logger.info("PA recomputation pass: %s (%d symbols)", tf, len(symbols))
            results = await asyncio.gather(
                *(self._process(symbol, tf) for symbol in symbols), return_exceptions=True
            )
            successes = sum(1 for r in results if r is True)
            insufficient = sum(1 for r in results if r == "insufficient")
            processed += successes
            if successes + insufficient == len(symbols):
                self._last_processed[tf] = latest_closed
        return processed

    async def _process(self, symbol: str, tf: str) -> bool | str:
        """Process a symbol. `True` success, `"insufficient"` persistent insufficient data,
        `False` transient failure (retry on the next pass)."""
        async with self._sem:
            try:
                candles = await self.engine._load_candles(symbol, tf, PA_LOOKBACK)
            except Exception as exc:  # noqa: BLE001
                logger.debug("could not load PA candles (skipped): %s %s — %r", symbol, tf, exc)
                return False
            if not candles:
                return False
            if self.engine.freshness(tf, candles[-1]["open_time"]) != FRESHNESS_FRESH:
                # Candle data has not reached the target closed bar → process after kline scheduler catches up.
                return False
            try:
                await self.engine.analyze(symbol, tf)
                return True
            except RasatError as exc:
                if exc.code == ErrorCode.STALE_DATA:
                    # Persistent insufficient data (e.g. too few 1d candles for a
                    # tokenized stock): do not block the pass; retry on a new closed bar.
                    logger.info("insufficient PA data (skipped): %s %s — %s", symbol, tf, exc.message)
                    return "insufficient"
                logger.warning("PA computation failed: %s %s — %s", symbol, tf, exc.message)
                return False
            except Exception as exc:  # noqa: BLE001
                logger.warning("PA computation failed: %s %s — %r", symbol, tf, exc)
                return False
