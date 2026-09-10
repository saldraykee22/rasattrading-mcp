"""REST kline scheduler plus warm-up prioritization (spot/futures source separation).

- The fixed set (15m/1h/4h/1d) is updated continuously in the background: when a
  closed candle is detected, the last N bars are fetched for the whole universe;
  cold (symbol,timeframe) pairs are backfilled at low priority.
- The symbol/timeframe currently requested by the agent is filled **first** (lazy,
  prioritized warm-up).
- All REST calls use the weight budget; when the limit is reached they queue and the system continues.
- Data is upserted into the `candles` table in batches.
- **Closed-candle rule (plans 2.7/1.4, 1.6):** Binance `/klines` also returns the
  forming bar; do not store it. If a partially formed bar were stored, catch-up
  would not trigger after it closed because `MAX(open_time) == last_closed`, leaving
  the last "closed" candle with partial volume. `_store` writes only closed bars.
- **Source separation (T05):** spot `/api/v3/klines` and futures `/fapi/v1/klines`
  use separate REST clients. `source` is part of the key for in-flight dedup,
  warm-map, scheduler/catch-up, and read/write queries. Futures requests are
  validated against the independent fapi universe; spot data cannot warm futures.
- **Server clock (T05):** close/freshness decisions use `BinanceClock` with the
  `/api/v3/time` offset, not the local clock. If the clock is missing/stale, fail
  closed: do not store candles and mark freshness as `stale`.
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config, TIMEFRAME_SECONDS
from ..envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from ..errors import ErrorCode, RasatError
from ..storage.db import Database
from ..timeutil import to_epoch_seconds
from .binance_client import BinanceREST, kline_weight
from .clock import BinanceClock
from .universe import UniverseService

logger = logging.getLogger("rasattrading.data.klines")

PRIORITY_ONDEMAND = 0
PRIORITY_CLOSED_BAR = 5
PRIORITY_BACKFILL = 10

SPOT_KLINES_PATH = "/api/v3/klines"
FUTURES_KLINES_PATH = "/fapi/v1/klines"


def parse_klines(raw: list) -> list[dict]:
    """Convert Binance kline arrays to a list of dicts (normalize open_time to seconds).

    Spot and futures kline array schemas are identical: [openTime, open, high, low, close,
    volume, closeTime, quoteVolume, trades, takerBuyBase, takerBuyQuote, ignore].
    """
    rows = []
    for item in raw:
        try:
            rows.append(
                {
                    "open_time": to_epoch_seconds(item[0]),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "quote_volume": float(item[7]),
                    "trades": int(item[8]),
                }
            )
        except (TypeError, ValueError, IndexError):
            continue
    return rows


class KlineService:
    def __init__(
        self,
        spot_rest: BinanceREST,
        futures_rest: BinanceREST,
        db: Database,
        universe: UniverseService,
        config: Config,
        clock: BinanceClock,
    ) -> None:
        self._rest = spot_rest
        self._futures_rest = futures_rest
        self._db = db
        self._universe = universe
        self._config = config
        self._clock = clock
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._seq = 0
        self._in_flight: dict[tuple[str, str, str], asyncio.Future] = {}
        self._workers: list[asyncio.Task] = []
        self._scheduler_task: asyncio.Task | None = None
        self._backfill_task: asyncio.Task | None = None
        self._futures_symbols: set[str] | None = None
        self._futures_sync_at: float = 0.0

    # ---------- lifecycle ----------

    async def start(self) -> None:
        for _ in range(self._config.kline_workers):
            self._workers.append(asyncio.create_task(self._worker_loop()))
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        self._backfill_task = asyncio.create_task(self._backfill_loop())

    async def stop(self) -> None:
        for t in self._workers:
            t.cancel()
        for t in (self._scheduler_task, self._backfill_task):
            if t is not None:
                t.cancel()
        await asyncio.gather(*self._workers, self._scheduler_task, self._backfill_task, return_exceptions=True)

    # ---------- work queue (source is part of the key) ----------

    def _enqueue(self, symbol: str, tf: str, limit: int, priority: int, source: str = "spot") -> asyncio.Future:
        key = (symbol, tf, source)
        existing = self._in_flight.get(key)
        if existing is not None and not existing.done():
            return existing
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._in_flight[key] = fut
        fut.add_done_callback(self._on_job_done(key))
        self._seq += 1
        self._queue.put_nowait((priority, self._seq, (symbol, tf, limit, source, fut)))
        return fut

    def _on_job_done(self, key: tuple[str, str, str]):
        def _cb(fut: asyncio.Future) -> None:
            self._in_flight.pop(key, None)
            if fut.cancelled():
                return
            exc = fut.exception()
            if exc is not None:
                logger.debug("background kline job failed (%s): %s", key, exc)

        return _cb

    async def _worker_loop(self) -> None:
        while True:
            try:
                _, _, (symbol, tf, limit, source, fut) = await self._queue.get()
            except asyncio.CancelledError:
                return
            try:
                rows = await self._fetch(symbol, tf, limit, source)
                if rows:
                    await self._store(symbol, tf, source, rows)
                if not fut.done():
                    fut.set_result(len(rows))
            except asyncio.CancelledError:
                if not fut.done():
                    fut.cancel()
                return
            except Exception as exc:  # noqa: BLE001
                if not fut.done():
                    fut.set_exception(exc)
            finally:
                self._queue.task_done()

    async def _fetch(self, symbol: str, tf: str, limit: int, source: str) -> list[dict]:
        if source == "futures":
            rest = self._futures_rest
            path = FUTURES_KLINES_PATH
        else:
            rest = self._rest
            path = SPOT_KLINES_PATH
        data = await rest.get(
            path,
            params={"symbol": symbol, "interval": tf, "limit": limit},
            weight=kline_weight(limit),
        )
        return parse_klines(data)

    @staticmethod
    def _upsert_sql() -> str:
        return (
            "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, quote_volume, trades, source, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(symbol, timeframe, open_time, source) DO UPDATE SET "
            "open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, "
            "volume=excluded.volume, quote_volume=excluded.quote_volume, trades=excluded.trades, updated_at=excluded.updated_at"
        )

    @staticmethod
    def _closed_only(rows: list[dict], tf: str, now: float) -> list[dict]:
        """Discard the last still-forming bar (closed-candle rule).

        `now` is reliable server time (server clock), not local time. Binance
        `/klines` also returns the forming bar; do not store it with partial volume.
        """
        period = TIMEFRAME_SECONDS[tf]
        latest_closed = int(now // period) * period - period
        return [r for r in rows if r["open_time"] <= latest_closed]

    async def _store(self, symbol: str, tf: str, source: str, rows: list[dict]) -> None:
        """Closed-candle rule plus server clock. Write nothing if the clock is missing/stale (fail closed)."""
        now = self._clock.server_now()
        if now is None:
            logger.warning(
                "server clock unavailable/stale — did not store %s/%s (%s) (fail closed)",
                symbol, tf, source,
            )
            return
        rows = self._closed_only(rows, tf, now)
        if not rows:
            return
        now_sec = int(now)
        sql = self._upsert_sql()

        def _write(conn) -> None:
            params = [
                (
                    symbol, tf, r["open_time"], r["open"], r["high"], r["low"], r["close"],
                    r["volume"], r["quote_volume"], r["trades"], source, now_sec,
                )
                for r in rows
            ]
            conn.executemany(sql, params)

        await self._db.write(_write)

    # ---------- warm state (per source) ----------

    async def _expected_latest_closed(self, tf: str) -> int | None:
        """Open time of the last closed bar by server time; None if the clock is missing (fail closed)."""
        now = self._clock.server_now()
        if now is None:
            return None
        period = TIMEFRAME_SECONDS[tf]
        return int(now // period) * period - period

    async def _warm_map(self, source: str) -> dict[tuple[str, str], int]:
        """Source-specific (symbol, timeframe) → latest open_time map (one query)."""

        def _q(conn):
            rows = conn.execute(
                "SELECT symbol, timeframe, MAX(open_time) AS m FROM candles "
                "WHERE source=? GROUP BY symbol, timeframe",
                (source,),
            ).fetchall()
            return {(r["symbol"], r["timeframe"]): int(r["m"]) for r in rows}

        return await self._db.read(_q)

    async def _is_warm(self, warm_map: dict, symbol: str, tf: str) -> bool:
        latest_closed = await self._expected_latest_closed(tf)
        if latest_closed is None:
            return False  # No clock → cannot decide warm state (fail closed).
        return warm_map.get((symbol, tf), 0) >= latest_closed

    # ---------- futures symbol validation ----------

    async def _futures_symbol_set(self) -> set[str]:
        """Return the TRADING USDT pair set from fapi exchangeInfo with TTL, as in FuturesContextPoller."""
        now = time.time()
        if self._futures_symbols is None or now - self._futures_sync_at >= self._config.futures_universe_ttl_seconds:
            data = await self._futures_rest.get("/fapi/v1/exchangeInfo", weight=1)
            symbols = {
                s["symbol"]
                for s in data.get("symbols", [])
                if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
            }
            self._futures_symbols = symbols
            self._futures_sync_at = now
            logger.info("futures kline universe synchronized: %d pairs", len(symbols))
        return self._futures_symbols

    async def _ensure_futures_symbol(self, symbol: str) -> bool:
        """Check whether a symbol is in the futures universe; fail closed if it cannot be loaded."""
        if self._futures_symbols is None or time.time() - self._futures_sync_at >= self._config.futures_universe_ttl_seconds:
            try:
                await self._futures_symbol_set()
            except Exception as exc:  # noqa: BLE001
                if self._futures_symbols is None:
                    raise RasatError(
                        ErrorCode.INVALID_SYMBOL,
                        f"could not load futures universe — could not validate symbol: {symbol}",
                    ) from exc
                logger.warning(
                    "could not refresh futures universe; using cache (%d symbols): %s",
                    len(self._futures_symbols), exc,
                )
        return symbol in (self._futures_symbols or set())

    # ---------- prioritized reads (agent request) ----------

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300, source: str = "spot") -> list[dict]:
        """Return a symbol's candles, performing prioritized warm-up first if cold."""
        if timeframe not in TIMEFRAME_SECONDS:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"invalid timeframe: {timeframe}")
        if limit < 1 or limit > 1000:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"limit must be between 1 and 1000 (given: {limit})")
        if source not in ("spot", "futures"):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"invalid source: {source}")
        if source == "futures":
            if not await self._ensure_futures_symbol(symbol):
                raise RasatError(ErrorCode.INVALID_SYMBOL, f"unknown symbol in futures universe: {symbol}")
        elif not await self._universe.ensure_contains(symbol):
            raise RasatError(ErrorCode.INVALID_SYMBOL, f"unknown symbol in universe: {symbol}")

        if timeframe in self._config.kline_intervals:
            # Fixed set → prioritized warm-up (source-specific warm map).
            warm_map = await self._warm_map(source)
            if not await self._is_warm(warm_map, symbol, timeframe):
                fut = self._enqueue(symbol, timeframe, limit, PRIORITY_ONDEMAND, source)
                try:
                    await fut
                except RasatError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    raise RasatError(ErrorCode.STALE_DATA, f"could not retrieve candle data: {exc}") from exc
        else:
            # Outside the fixed set → fetch live on request (not monitored in the background).
            rows = await self._fetch(symbol, timeframe, limit, source)
            if rows:
                await self._store(symbol, timeframe, source, rows)

        return await self._read_candles(symbol, timeframe, limit, source)

    async def _read_candles(self, symbol: str, tf: str, limit: int, source: str) -> list[dict]:
        def _q(conn):
            rows = conn.execute(
                "SELECT open_time, open, high, low, close, volume, quote_volume, trades, source "
                "FROM candles WHERE symbol=? AND timeframe=? AND source=? "
                "ORDER BY open_time DESC LIMIT ?",
                (symbol, tf, source, limit),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]

        return await self._db.read(_q)

    # ---------- background loops ----------

    async def _scheduler_loop(self) -> None:
        """Catch closed candles: fetch for the spot universe when the latest closed bar is delayed by timeframe."""
        while True:
            try:
                await asyncio.sleep(20)
                now = self._clock.server_now()
                if now is None:
                    # Without a clock, the close target cannot be determined; fail closed and skip this pass.
                    continue
                for tf in self._config.kline_intervals:
                    period = TIMEFRAME_SECONDS[tf]
                    last_closed = int(now // period) * period - period
                    if await self._needs_catchup(tf, last_closed, "spot"):
                        await self._enqueue_closed_bar_pass(tf, last_closed, "spot")
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("kline scheduler failed")

    async def _needs_catchup(self, tf: str, last_closed: int, source: str) -> bool:
        """Determine the need for symbol-level catch-up (T2).

        The old behavior checked one `MAX(open_time)` for the whole timeframe;
        once one symbol reached the last closed bar, catch-up was skipped for the
        whole timeframe and lagging symbols could be missed. Now each symbol's own
        `MAX(open_time)` is compared with the target closed bar: return True when at
        least one lags (in-flight dedup already prevents duplicate fetches).
        """
        if source == "futures":
            symbols = await self._futures_symbol_set()
        else:
            symbols = self._universe.snapshot()
        if not symbols:
            return False

        def _q(conn):
            rows = conn.execute(
                "SELECT symbol, MAX(open_time) AS m FROM candles WHERE timeframe=? AND source=? GROUP BY symbol",
                (tf, source),
            ).fetchall()
            return {r["symbol"]: int(r["m"]) for r in rows}

        warm_map = await self._db.read(_q)
        return any(warm_map.get(s, 0) < last_closed for s in symbols)

    async def _enqueue_closed_bar_pass(self, tf: str, last_closed: int, source: str = "spot") -> None:
        for symbol in self._universe.snapshot():
            self._enqueue(symbol, tf, self._config.kline_catchup_bars, PRIORITY_CLOSED_BAR, source)
        logger.info(
            "closed-candle catch-up: %s (%s, target open_time=%s, %d symbols)",
            tf, source, last_closed, len(self._universe.snapshot()),
        )

    async def _backfill_loop(self) -> None:
        """Fill cold (symbol,timeframe) pairs at low priority (spot).

        Sleep before the first pass so startup backfill does not consume the budget
        before the agent's prioritized requests enter the queue. Without a clock,
        warm state cannot be decided; skip this pass fail closed.
        """
        while True:
            try:
                await asyncio.sleep(30)
                if self._clock.server_now() is None:
                    continue
                symbols = self._universe.snapshot()
                if symbols:
                    warm_map = await self._warm_map("spot")
                    enqueued = 0
                    for symbol in symbols:
                        for tf in self._config.kline_intervals:
                            if not await self._is_warm(warm_map, symbol, tf):
                                self._enqueue(symbol, tf, self._config.kline_backfill_bars, PRIORITY_BACKFILL, "spot")
                                enqueued += 1
                    if enqueued:
                        logger.info("backfill queue: %d (symbol,timeframe) pairs", enqueued)
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("backfill failed")

    # ---------- helper ----------

    def freshness_for(self, symbol: str, tf: str, rows: list[dict]) -> str:
        """Freshness based on the last bar's recency (server clock; no tolerance).

        `fresh` means the last candle in the DB is the timeframe's last closed
        candle (`last_open == latest_closed`). If the last closed candle has not
        been stored, the clock is missing, or data is "in the future" (forming)
        relative to the server, return `stale`; fail closed so old/forming data is
        not treated as fresh.
        """
        if not rows:
            return FRESHNESS_STALE
        now = self._clock.server_now()
        if now is None:
            return FRESHNESS_STALE
        period = TIMEFRAME_SECONDS[tf]
        last_open = rows[-1]["open_time"]
        expected = int(now // period) * period - period
        if last_open == expected:
            return FRESHNESS_FRESH
        return FRESHNESS_STALE

    def health(self) -> dict:
        return {
            "queue_size": self._queue.qsize(),
            "in_flight": len(self._in_flight),
            "clock_available": self._clock.available,
        }
