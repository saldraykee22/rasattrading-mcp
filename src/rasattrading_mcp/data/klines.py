"""REST kline scheduler + warm-up önceliklendirme (spot/futures kaynak ayrımı).

- Sabit set (15m/1h/4h/1d) arka planda sürekli güncellenir: kapalı mum tespit edilince
  tüm evren için son N bar çekilir; soğuk (symbol,timeframe) çiftleri düşük öncelikle backfill edilir.
- Agent'ın o an istediği symbol/timeframe **öncelikli** doldurulur (lazy/öncelikli warm-up).
- Tüm REST çağrıları weight bütçesinden geçer; limit dolarsa kuyruklanır, sistem durmaz.
- Veri `candles` tablosuna upsert edilir (batch).
- **Kapalı mum kuralı (plan 2.7/1.4, 1.6):** Binance `/klines` oluşmakta olan barı da
  döndürür; o bar saklanmaz. Kısmi hacimli forming bar kaydedilseydi, kapanınca
  `MAX(open_time) == last_closed` olduğu için catchup tetiklenmez ve son "kapalı" mum
  kısmi hacimle kalırdı. `_store` yalnızca kapanmış barları yazar.
- **Kaynak ayrımı (T05):** spot `/api/v3/klines`, futures `/fapi/v1/klines` (ayrı REST
  client). `source` — in-flight dedup, warm-map, scheduler/catch-up ve read/write
  sorgularının — anahtarının parçasıdır. Futures istekleri spot evreninden bağımsız
  fapi universe ile doğrulanır; spot verisi futures'ı warm kabul ettirmez.
- **Server clock (T05):** kapanış/freshness kararları yerel saat değil, Binance
  `/api/v3/time` offset'li `BinanceClock` üzerinden yapılır. Clock yoksa/stale ise
  fail-closed davranılır: mum saklanmaz, freshness `stale` sayılır.
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
    """Binance kline dizisini dict listesine çevirir (open_time saniyeye normalize).

    Spot ve futures kline dizi şeması aynıdır: [openTime, open, high, low, close,
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

    # ---------- iş kuyruğu (source anahtarın parçasıdır) ----------

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
                logger.debug("arka plan kline işi başarısız (%s): %s", key, exc)

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
        """Hâlâ oluşmakta olan son barı atar (kapalı mum kuralı).

        `now` güvenilir sunucu zamanıdır (server clock); yerel saat değil. Binance
        `/klines` oluşmakta olan barı da döndürür; kısmi hacimle saklanmamalı.
        """
        period = TIMEFRAME_SECONDS[tf]
        latest_closed = int(now // period) * period - period
        return [r for r in rows if r["open_time"] <= latest_closed]

    async def _store(self, symbol: str, tf: str, source: str, rows: list[dict]) -> None:
        """Kapalı mum kuralı + server clock. Clock yoksa/stale ise hiçbir şey yazmaz (fail-closed)."""
        now = self._clock.server_now()
        if now is None:
            logger.warning(
                "server clock erişilemez/stale — %s/%s (%s) saklanmadı (fail-closed)",
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

    # ---------- warm durumu (source bazlı) ----------

    async def _expected_latest_closed(self, tf: str) -> int | None:
        """Server saatine göre son kapanmış barın open_time'ı; clock yoksa None (fail-closed)."""
        now = self._clock.server_now()
        if now is None:
            return None
        period = TIMEFRAME_SECONDS[tf]
        return int(now // period) * period - period

    async def _warm_map(self, source: str) -> dict[tuple[str, str], int]:
        """source'a özel (symbol, timeframe) → en güncel open_time haritası (tek sorgu)."""

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
            return False  # clock yok → warm kararı verilemez (fail-closed)
        return warm_map.get((symbol, tf), 0) >= latest_closed

    # ---------- futures sembol doğrulama ----------

    async def _futures_symbol_set(self) -> set[str]:
        """fapi exchangeInfo'dan TRADING USDT çifti kümesi (TTL'li, FuturesContextPoller gibi)."""
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
            logger.info("futures kline evreni senkronize: %d çift", len(symbols))
        return self._futures_symbols

    async def _ensure_futures_symbol(self, symbol: str) -> bool:
        """Sembol futures evreninde mi? Evren yüklenemezse fail-closed (sessizce kabul etme)."""
        if self._futures_symbols is None or time.time() - self._futures_sync_at >= self._config.futures_universe_ttl_seconds:
            try:
                await self._futures_symbol_set()
            except Exception as exc:  # noqa: BLE001
                if self._futures_symbols is None:
                    raise RasatError(
                        ErrorCode.INVALID_SYMBOL,
                        f"futures evreni yüklenemedi — sembol doğrulanamadı: {symbol}",
                    ) from exc
                logger.warning(
                    "futures evreni tazelenemedi; önbellek kullanılıyor (%d sembol): %s",
                    len(self._futures_symbols), exc,
                )
        return symbol in (self._futures_symbols or set())

    # ---------- öncelikli okuma (agent isteği) ----------

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300, source: str = "spot") -> list[dict]:
        """Sembolün mumlarını döndürür; soğuksa önce öncelikli warm-up yapar."""
        if timeframe not in TIMEFRAME_SECONDS:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"geçersiz timeframe: {timeframe}")
        if limit < 1 or limit > 1000:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"limit 1-1000 arası olmalı (verildi: {limit})")
        if source not in ("spot", "futures"):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"geçersiz source: {source}")
        if source == "futures":
            if not await self._ensure_futures_symbol(symbol):
                raise RasatError(ErrorCode.INVALID_SYMBOL, f"futures evreninde bilinmeyen sembol: {symbol}")
        elif not await self._universe.ensure_contains(symbol):
            raise RasatError(ErrorCode.INVALID_SYMBOL, f"evrende bilinmeyen sembol: {symbol}")

        if timeframe in self._config.kline_intervals:
            # Sabit set → öncelikli warm-up (source'a özel warm haritası)
            warm_map = await self._warm_map(source)
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

    # ---------- arka plan döngüleri ----------

    async def _scheduler_loop(self) -> None:
        """Kapalı mumları yakalar: timeframe bazında son kapalı bar gecikmeli ise spot evreni için çeker."""
        while True:
            try:
                await asyncio.sleep(20)
                now = self._clock.server_now()
                if now is None:
                    # Clock yoksa kapanış hedefi belirlenemez — fail-closed, bu turu atla.
                    continue
                for tf in self._config.kline_intervals:
                    period = TIMEFRAME_SECONDS[tf]
                    last_closed = int(now // period) * period - period
                    if await self._needs_catchup(tf, last_closed, "spot"):
                        await self._enqueue_closed_bar_pass(tf, last_closed, "spot")
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("kline scheduler hatası")

    async def _needs_catchup(self, tf: str, last_closed: int, source: str) -> bool:
        """Sembol bazlı catchup ihtiyacı (T2).

        Eski davranış timeframe genelinde tek `MAX(open_time)` kontrolü yapıyordu;
        bir sembol son kapalı bara ulaşınca tüm timeframe için catchup atlanıyor,
        geride kalan semboller kaçabiliyordu. Artık evrendeki her sembolün kendi
        `MAX(open_time)`'ı hedef kapalı bara bakılır: en az biri geride kalınca
        True döner (in-flight dedup çift çekimi zaten önler).
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
            "kapalı mum yakalama: %s (%s, hedef open_time=%s, %d sembol)",
            tf, source, last_closed, len(self._universe.snapshot()),
        )

    async def _backfill_loop(self) -> None:
        """Soğuk (symbol,timeframe) çiftlerini düşük öncelikle doldurur (spot).

        İlk tur önce uyur: başlangıçta agent'ın öncelikli istekleri kuyruğa
        girmeden backfill bütçeyi doldurmasın. Clock yoksa warm kararı verilemez
        — bu tur fail-closed atlanır.
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
                        logger.info("backfill kuyruğu: %d (symbol,timeframe) çifti", enqueued)
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("backfill hatası")

    # ---------- helper ----------

    def freshness_for(self, symbol: str, tf: str, rows: list[dict]) -> str:
        """Son barın güncelliğine göre freshness (server clock; toleranssız).

        `fresh`, DB'deki son mumun timeframe'in son kapanmış mumu olduğu anlamına
        gelir (`last_open == latest_closed`). Son kapanmış mum henüz saklanmadıysa,
        clock yoksa veya veri server'a göre "gelecekte" (forming) ise `stale` —
        fail-closed, eski/oluşmakta olan veri taze sanılmaz.
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
