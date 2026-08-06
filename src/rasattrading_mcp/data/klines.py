"""REST kline scheduler + warm-up önceliklendirme.

- Sabit set (15m/1h/4h/1d) arka planda sürekli güncellenir: kapalı mum tespit edilince
  tüm evren için son N bar çekilir; soğuk (symbol,timeframe) çiftleri düşük öncelikle backfill edilir.
- Agent'ın o an istediği symbol/timeframe **öncelikli** doldurulur (lazy/öncelikli warm-up).
- Tüm REST çağrıları weight bütçesinden geçer; limit dolarsa kuyruklanır, sistem durmaz.
- Veri `candles` tablosuna upsert edilir (batch).
- **Kapalı mum kuralı (plan 2.7/1.4, 1.6):** Binance `/klines` oluşmakta olan barı da
  döndürür; o bar saklanmaz. Kısmi hacimli forming bar kaydedilseydi, kapanınca
  `MAX(open_time) == last_closed` olduğu için catchup tetiklenmez ve son "kapalı" mum
  kısmi hacimle kalırdı. `_store` yalnızca kapanmış barları yazar.
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
from .universe import UniverseService

logger = logging.getLogger("rasattrading.data.klines")

PRIORITY_ONDEMAND = 0
PRIORITY_CLOSED_BAR = 5
PRIORITY_BACKFILL = 10


def parse_klines(raw: list) -> list[dict]:
    """Binance kline dizisini dict listesine çevirir (open_time saniyeye normalize)."""
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
    def __init__(self, rest: BinanceREST, db: Database, universe: UniverseService, config: Config) -> None:
        self._rest = rest
        self._db = db
        self._universe = universe
        self._config = config
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._seq = 0
        self._in_flight: dict[tuple[str, str], asyncio.Future] = {}
        self._workers: list[asyncio.Task] = []
        self._scheduler_task: asyncio.Task | None = None
        self._backfill_task: asyncio.Task | None = None

    # ---------- yaşam döngüsü ----------

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

    # ---------- iş kuyruğu ----------

    def _enqueue(self, symbol: str, tf: str, limit: int, priority: int, source: str = "spot") -> asyncio.Future:
        key = (symbol, tf)
        existing = self._in_flight.get(key)
        if existing is not None and not existing.done():
            return existing
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._in_flight[key] = fut
        fut.add_done_callback(self._on_job_done(key))
        self._seq += 1
        self._queue.put_nowait((priority, self._seq, (symbol, tf, limit, source, fut)))
        return fut

    def _on_job_done(self, key: tuple[str, str]):
        def _cb(fut: asyncio.Future) -> None:
            self._in_flight.pop(key, None)
            if fut.cancelled():
                return
            exc = fut.exception()
            if exc is not None:
                logger.debug("arka plan kline işi başarısız (%s): %s", key, exc)

        return _cb

    async def _worker_loop(self) -> None:
        while True:
            try:
                _, _, (symbol, tf, limit, source, fut) = await self._queue.get()
            except asyncio.CancelledError:
                return
            try:
                rows = await self._fetch(symbol, tf, limit)
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

    async def _fetch(self, symbol: str, tf: str, limit: int) -> list[dict]:
        data = await self._rest.get(
            "/api/v3/klines",
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
    def _closed_only(rows: list[dict], tf: str) -> list[dict]:
        """Hâlâ oluşmakta olan son barı atar (kapalı mum kuralı).

        Binance `/klines` oluşmakta olan barı da döndürür; kısmi hacimle
        saklanmamalı (1.6: forming bar kaydedilirse kapanınca catchup tetiklenmez
        ve son "kapalı" mum kısmi hacimle kalır).
        """
        period = TIMEFRAME_SECONDS[tf]
        latest_closed = int(time.time() // period) * period - period
        return [r for r in rows if r["open_time"] <= latest_closed]

    async def _store(self, symbol: str, tf: str, source: str, rows: list[dict]) -> None:
        rows = self._closed_only(rows, tf)
        if not rows:
            return
        now = int(time.time())
        sql = self._upsert_sql()

        def _write(conn) -> None:
            params = [
                (
                    symbol, tf, r["open_time"], r["open"], r["high"], r["low"], r["close"],
                    r["volume"], r["quote_volume"], r["trades"], source, now,
                )
                for r in rows
            ]
            conn.executemany(sql, params)

        await self._db.write(_write)

    # ---------- warm durumu ----------

    async def _expected_latest_closed(self, tf: str) -> int:
        period = TIMEFRAME_SECONDS[tf]
        now = time.time()
        return int(now // period) * period - period

    async def _warm_map(self) -> set[tuple[str, str]]:
        """source='spot' için (symbol, timeframe) → en güncel open_time haritası (tek sorgu)."""

        def _q(conn):
            rows = conn.execute(
                "SELECT symbol, timeframe, MAX(open_time) AS m FROM candles "
                "WHERE source='spot' GROUP BY symbol, timeframe"
            ).fetchall()
            return {(r["symbol"], r["timeframe"]): int(r["m"]) for r in rows}

        return await self._db.read(_q)

    async def _is_warm(self, warm_map: dict, symbol: str, tf: str) -> bool:
        latest_closed = await self._expected_latest_closed(tf)
        return warm_map.get((symbol, tf), 0) >= latest_closed

    # ---------- öncelikli okuma (agent isteği) ----------

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300, source: str = "spot") -> list[dict]:
        """Sembolün mumlarını döndürür; soğuksa önce öncelikli warm-up yapar."""
        if timeframe not in TIMEFRAME_SECONDS:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"geçersiz timeframe: {timeframe}")
        if limit < 1 or limit > 1000:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"limit 1-1000 arası olmalı (verildi: {limit})")
        if source not in ("spot", "futures"):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"geçersiz source: {source}")
        if not await self._universe.ensure_contains(symbol):
            raise RasatError(ErrorCode.INVALID_SYMBOL, f"evrende bilinmeyen sembol: {symbol}")

        if timeframe in self._config.kline_intervals:
            # Sabit set → öncelikli warm-up
            warm_map = await self._warm_map()
            if not await self._is_warm(warm_map, symbol, timeframe):
                fut = self._enqueue(symbol, timeframe, limit, PRIORITY_ONDEMAND, source)
                try:
                    await fut
                except RasatError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    raise RasatError(ErrorCode.STALE_DATA, f"mum verisi alınamadı: {exc}") from exc
        else:
            # Sabit set dışı → istek anında canlı hesapla (arka planda izlenmez)
            rows = await self._fetch(symbol, timeframe, limit)
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

    # ---------- arka plan döngüleri ----------

    async def _scheduler_loop(self) -> None:
        """Kapalı mumları yakalar: timeframe bazında son kapalı bar gecikmeli ise tüm evren için çeker."""
        while True:
            try:
                await asyncio.sleep(20)
                now = time.time()
                for tf in self._config.kline_intervals:
                    period = TIMEFRAME_SECONDS[tf]
                    last_closed = int(now // period) * period - period
                    if await self._needs_catchup(tf, last_closed):
                        await self._enqueue_closed_bar_pass(tf, last_closed)
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("kline scheduler hatası")

    async def _needs_catchup(self, tf: str, last_closed: int) -> bool:
        def _q(conn):
            row = conn.execute(
                "SELECT MAX(open_time) AS m FROM candles WHERE timeframe=? AND source='spot'", (tf,)
            ).fetchone()
            return int(row["m"]) if row["m"] is not None else None

        max_open = await self._db.read(_q)
        return max_open is None or max_open < last_closed

    async def _enqueue_closed_bar_pass(self, tf: str, last_closed: int) -> None:
        for symbol in self._universe.snapshot():
            self._enqueue(symbol, tf, self._config.kline_catchup_bars, PRIORITY_CLOSED_BAR)
        logger.info("kapalı mum yakalama: %s (hedef open_time=%s, %d sembol)", tf, last_closed, len(self._universe.snapshot()))

    async def _backfill_loop(self) -> None:
        """Soğuk (symbol,timeframe) çiftlerini düşük öncelikle doldurur."""
        while True:
            try:
                symbols = self._universe.snapshot()
                if symbols:
                    warm_map = await self._warm_map()
                    enqueued = 0
                    for symbol in symbols:
                        for tf in self._config.kline_intervals:
                            if not await self._is_warm(warm_map, symbol, tf):
                                self._enqueue(symbol, tf, self._config.kline_backfill_bars, PRIORITY_BACKFILL)
                                enqueued += 1
                    if enqueued:
                        logger.info("backfill kuyruğu: %d (symbol,timeframe) çifti", enqueued)
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("backfill hatası")
                await asyncio.sleep(30)

    # ---------- helper ----------

    def freshness_for(self, symbol: str, tf: str, rows: list[dict]) -> str:
        """Son barın güncelliğine göre freshness (2.15: fazladan bir period toleransı yok).

        `fresh`, DB'deki son mumun timeframe'in son kapanmış mumu olduğu anlamına
        gelir (`last_open >= latest_closed`). Bir mum eksikse (son kapanmış mum
        henüz saklanmadıysa) `stale` — fail-closed, eski veri taze sanılmaz.
        """
        if not rows:
            return FRESHNESS_STALE
        period = TIMEFRAME_SECONDS[tf]
        last_open = rows[-1]["open_time"]
        expected = int(time.time() // period) * period - period
        if last_open >= expected:
            return FRESHNESS_FRESH
        return FRESHNESS_STALE

    def health(self) -> dict:
        return {"queue_size": self._queue.qsize(), "in_flight": len(self._in_flight)}
