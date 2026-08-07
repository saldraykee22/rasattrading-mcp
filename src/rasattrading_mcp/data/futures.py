"""Futures bağlam verisi (salt-okunur): funding rate, open interest.

REST periyodik çekim. Her kayıt Binance'in `event_time`'ı ve daemon'un `fetched_at`'iyle
ayrı ayrı saklanır — ikisi arasındaki fark gecikmeyi gösterir. `freshness` alanı
fresh|stale|unknown olabilir; `unknown` sembol girdisi likidite skoruna katılmaz.

Liquidation REST ile çekilmez: `/fapi/v1/forceOrders` imzalı USER_DATA endpoint'idir
(piyasa geneli değil, yalnızca o hesabın likidasyonları) ve imzasız çağrı hep 401
dönerdi. Piyasa geneli likidasyonlar public `!forceOrder@arr` WebSocket stream'i ile
alınır — bkz. `data/liquidation_ws.py`. Poller'ın `_last_status["liquidation"]`'ı
artık pipeline tarafından WS client durumundan (`connected`/`disconnected`) türetilir.
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config
from ..errors import ErrorCode, RasatError
from ..storage.db import Database
from ..timeutil import to_epoch_seconds
from .binance_client import BinanceREST
from .universe import UniverseService

logger = logging.getLogger("rasattrading.data.futures")


class FuturesContextPoller:
    def __init__(self, rest: BinanceREST, db: Database, universe: UniverseService, config: Config) -> None:
        self._rest = rest
        self._db = db
        self._universe = universe
        self._config = config
        self._last_status: dict[str, str] = {}
        self._futures_symbols: set[str] | None = None
        self._futures_sync_at: float = 0.0

    async def _futures_symbol_set(self) -> set[str]:
        """fapi exchangeInfo'dan TRADING USDT çifti kümesi (TTL'li).

        Spot evrenindeki her sembol futures'ta yoktur (tokenized hisse senetleri,
        spot-only çiftler). OI poll'ü yalnızca bu kümedeki sembollere istek atar —
        aksi halde her turda ~100+ gereksiz 400 hatası + log spam üretir.
        """
        now = time.time()
        if self._futures_symbols is None or now - self._futures_sync_at >= self._config.futures_universe_ttl_seconds:
            data = await self._rest.get("/fapi/v1/exchangeInfo", weight=1)
            symbols = {
                s["symbol"]
                for s in data.get("symbols", [])
                if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT"
            }
            self._futures_symbols = symbols
            self._futures_sync_at = now
            logger.info("futures sembol evreni senkronize: %d çift", len(symbols))
        return self._futures_symbols

    async def poll_funding(self) -> int:
        data = await self._rest.get("/fapi/v1/premiumIndex", weight=1)
        rows: list[tuple] = []
        for item in data:
            try:
                rate = item.get("lastFundingRate")
                if rate is None:
                    continue
                rows.append(
                    (
                        item["symbol"],
                        "funding_rate",
                        float(rate),
                        to_epoch_seconds(item.get("time", 0)),
                        {"next_funding_time": item.get("nextFundingTime")},
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        await self._store(rows)
        self._last_status["funding"] = "ok"
        return len(rows)

    async def poll_open_interest(self) -> int:
        total = 0
        clean = True
        futures_symbols = await self._futures_symbol_set()
        for symbol in self._universe.snapshot():
            if symbol not in futures_symbols:
                continue
            try:
                data = await self._rest.get(
                    "/fapi/v1/openInterest", params={"symbol": symbol}, weight=1
                )
                rows = [
                    (
                        symbol,
                        "open_interest",
                        float(data.get("openInterest", 0)),
                        to_epoch_seconds(data.get("time", 0)),
                        {},
                    )
                ]
                await self._store(rows)
                total += 1
            except RasatError as exc:
                if exc.code == ErrorCode.RATE_LIMITED:
                    self._last_status["open_interest"] = "rate_limited"
                    clean = False
                    break  # bütçe dolu — bu turu bırak, sonraki turda dene
                if exc.code == ErrorCode.INVALID_REQUEST:
                    # fapi'de olmayan sembol (400): kümeden düşür, her turda tekrarlama
                    if self._futures_symbols is not None and symbol in self._futures_symbols:
                        self._futures_symbols.discard(symbol)
                        logger.info("fapi'de olmayan sembol OI kümesinden düşürüldü: %s", symbol)
                    continue
                logger.warning("openInterest başarısız %s: %s", symbol, exc.message)
                self._last_status["open_interest"] = "error"
                clean = False
        # "ok" yalnızca döngü hatasız/kesintisiz tamamlandığında yazılır (T2);
        # aksi halde rate_limited/error gibi gerçek durum korunur.
        if clean:
            self._last_status["open_interest"] = "ok"
        return total

    async def _store(self, rows: list[tuple]) -> None:
        if not rows:
            return
        fetched_at = int(time.time())
        # row formatı: (symbol, type, value, event_time, extra)
        import json

        def _write(conn) -> None:
            sql = (
                "INSERT INTO futures_context (symbol, type, event_time, value, extra, fetched_at, freshness) "
                "VALUES (?,?,?,?,?,?,'fresh') "
                "ON CONFLICT(symbol, type, event_time) DO UPDATE SET "
                "value=excluded.value, extra=excluded.extra, fetched_at=excluded.fetched_at, freshness='fresh'"
            )
            conn.executemany(
                sql,
                [(r[0], r[1], r[3], r[2], json.dumps(r[4], default=str), fetched_at) for r in rows],
            )

        await self._db.write(_write)

    async def age_stale_rows(self, now: float | None = None) -> int:
        """`fresh` satırları yaşlandırır: `futures_stale_after_seconds` süredir
        yenilenmeyenler `stale` olur.

        Poll başarısız olsa bile çalışır — eski futures verisi süresiz `fresh`
        kalamaz (2.10 fix). Dönen değer yaşlandırılan satır sayısıdır.
        """
        now = now if now is not None else time.time()
        cutoff = int(now) - int(self._config.futures_stale_after_seconds)

        def _w(conn) -> int:
            cur = conn.execute(
                "UPDATE futures_context SET freshness='stale' WHERE freshness='fresh' AND fetched_at < ?",
                (cutoff,),
            )
            return cur.rowcount

        aged = await self._db.write(_w)
        if aged:
            logger.info("futures_context yaşlandırıldı: %d satır", aged)
        return aged

    async def run_loop(self, stop: asyncio.Event) -> None:
        """Funding + OI poll'ü her `liquidation_poll_seconds`'ta çalıştırır.

        Liquidation bu döngüden ayrıdır — `data/liquidation_ws.py` WebSocket akışı
        tarafından beslenir (`_last_status["liquidation"]`'ı pipeline, WS client
        durumundan set eder).
        """
        while not stop.is_set():
            try:
                try:
                    await self.age_stale_rows()
                except Exception:  # noqa: BLE001
                    logger.exception("futures aging hatası")
                try:
                    await self.poll_funding()
                except RasatError as exc:
                    logger.warning("funding poll başarısız: %s", exc.message)
                    self._last_status["funding"] = "error"
                try:
                    await self.poll_open_interest()
                except RasatError as exc:
                    logger.warning("OI poll başarısız: %s", exc.message)
                    self._last_status["open_interest"] = "error"
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("futures poll döngüsü hatası")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._config.liquidation_poll_seconds)
            except asyncio.TimeoutError:
                pass

    def set_liquidation_status(self, status: str) -> None:
        """Pipeline, WS client durumunu (`connected`/`disconnected`) buraya yazar.

        REST liquidation poll kaldırıldığı için durum artık `data/liquidation_ws.py`
        tarafından beslenen public WebSocket stream'inden türetilir.
        """
        self._last_status["liquidation"] = status

    def status(self) -> dict:
        return dict(self._last_status)
