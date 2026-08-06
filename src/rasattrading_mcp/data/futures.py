"""Futures bağlam verisi (salt-okunur): funding rate, open interest, liquidation.

REST periyodik çekim. Her kayıt Binance'in `event_time`'ı ve daemon'un `fetched_at`'iyle
ayrı ayrı saklanır — ikisi arasındaki fark gecikmeyi gösterir. `freshness` alanı
fresh|stale|unknown olabilir; `unknown` sembol girdisi likidite skoruna katılmaz.

Liquidation (allForceOrders) API key isteyebilir; 401 alınırsa `_liquidation_available=False`
ile sessizce devre dışı kalır (veri eksikliği `unknown` olarak görünür, sistemi durdurmaz).
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from ..config import Config
from ..errors import ErrorCode, RasatError
from ..storage.db import Database
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
                        int(item.get("time", 0)),
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
                        int(data.get("time", 0)),
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
            data = await self._rest.get("/fapi/v1/allForceOrders", params=params, weight=10)
        except RasatError as exc:
            if exc.code == ErrorCode.UNAUTHORIZED:
                self._liquidation_available = False
                self._last_status["liquidation"] = "unavailable"
                logger.info("liquidation verisi API key gerektiriyor — devre dışı (v1 kabul)")
                return 0
            raise

        rows: list[tuple] = []
        for o in data:
            try:
                event_time = int(o.get("time", 0))
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
                if event_time > self._last_liquidation_ts:
                    self._last_liquidation_ts = event_time
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

    async def run_loop(self, stop: asyncio.Event) -> None:
        """Funding + OI `futures_poll_seconds`'ta, liquidation `liquidation_poll_seconds`'ta."""
        while not stop.is_set():
            try:
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
