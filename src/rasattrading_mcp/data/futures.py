"""Futures bağlam verisi (salt-okunur): funding rate, open interest, liquidation.

REST periyodik çekim. Her kayıt Binance'in `event_time`'ı ve daemon'un `fetched_at`'iyle
ayrı ayrı saklanır — ikisi arasındaki fark gecikmeyi gösterir. `freshness` alanı
fresh|stale|unknown olabilir; `unknown` sembol girdisi likidite skoruna katılmaz.

Liquidation (`/fapi/v1/forceOrders`, signed USER_DATA) API key ister; anahtar yoksa
Binance 401 döner → `_liquidation_available=False` ile sessizce devre dışı kalır ve
durum `not_configured` olur (hata değil — veri eksikliği `unknown` olarak görünür,
sistemi durdurmaz). 1.6: eski path `/fapi/v1/allForceOrders` canlı API'de 404
dönüyordu (path mevcut değil) → kalıcı `error`; doğru path `/fapi/v1/forceOrders`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

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
        self._last_liquidation_ts: int = 0
        self._liquidation_available: bool = True
        self._last_status: dict[str, str] = {}

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
        for symbol in self._universe.snapshot():
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
                    break  # bütçe dolu — bu turu bırak, sonraki turda dene
                logger.warning("openInterest başarısız %s: %s", symbol, exc.message)
        self._last_status["open_interest"] = "ok"
        return total

    async def poll_liquidations(self) -> int:
        if not self._liquidation_available:
            return 0
        params: dict[str, Any] = {"limit": 1000}
        if self._last_liquidation_ts:
            params["startTime"] = self._last_liquidation_ts
        try:
            data = await self._rest.get("/fapi/v1/forceOrders", params=params, weight=10)
        except RasatError as exc:
            if exc.code == ErrorCode.UNAUTHORIZED:
                self._liquidation_available = False
                self._last_status["liquidation"] = "not_configured"
                logger.info("liquidation verisi API key gerektiriyor — devre dışı (not_configured)")
                return 0
            raise

        rows: list[tuple] = []
        for o in data:
            try:
                # `_last_liquidation_ts` API `startTime` paramı için ms tutulur;
                # DB'ye saniye yazılır (tek birim standardı).
                raw_ms = int(o.get("time", 0))
                event_time = to_epoch_seconds(raw_ms)
                price = float(o.get("price", 0))
                qty = float(o.get("origQty", 0))
                rows.append(
                    (
                        o.get("symbol", "?"),
                        "liquidation",
                        price * qty,
                        event_time,
                        {"side": o.get("side"), "price": price, "qty": qty},
                    )
                )
                if raw_ms > self._last_liquidation_ts:
                    self._last_liquidation_ts = raw_ms
            except (TypeError, ValueError):
                continue
        if rows:
            await self._store(rows)
        self._last_status["liquidation"] = "ok"
        return len(rows)

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
        """Funding + OI `futures_poll_seconds`'ta, liquidation `liquidation_poll_seconds`'ta."""
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
                try:
                    await self.poll_liquidations()
                except RasatError as exc:
                    logger.warning("liquidation poll başarısız: %s", exc.message)
                    self._last_status["liquidation"] = "error"
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("futures poll döngüsü hatası")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._config.liquidation_poll_seconds)
            except asyncio.TimeoutError:
                pass

    def status(self) -> dict:
        return dict(self._last_status)
