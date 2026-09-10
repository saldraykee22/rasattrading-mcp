"""Read-only futures context data: funding rate and open interest.

Fetched periodically through REST. Each record stores Binance's `event_time` and
the daemon's `fetched_at` separately; their difference shows the delay.
`freshness` can be fresh|stale|unknown; an `unknown` symbol entry is excluded from
the liquidity score.

Liquidations are not fetched through REST: `/fapi/v1/forceOrders` is a signed
USER_DATA endpoint (it returns only that account's liquidations, not market-wide
data), and unsigned calls always returned 401. Market-wide liquidations are
received through the public `!forceOrder@arr` WebSocket stream; see
`data/liquidation_ws.py`. The poller's `_last_status["liquidation"]` is now derived
by the pipeline from the WS client's (`connected`/`disconnected`) state.
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
        """Return the set of TRADING USDT pairs from fapi exchangeInfo (with TTL).

        Not every symbol in the spot universe exists in futures (tokenized stocks,
        spot-only pairs). The OI poll requests only this set; otherwise every pass
        would produce 100+ unnecessary 400 errors and log spam.
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
            logger.info("futures symbol universe synchronized: %d pairs", len(symbols))
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
                    break  # Budget exhausted; stop this pass and retry on the next one.
                if exc.code == ErrorCode.INVALID_REQUEST:
                    # Symbol unavailable in fapi (400): remove it from the set and do not repeat every pass.
                    if self._futures_symbols is not None and symbol in self._futures_symbols:
                        self._futures_symbols.discard(symbol)
                        logger.info("removed symbol unavailable in fapi from OI set: %s", symbol)
                    continue
                logger.warning("openInterest failed for %s: %s", symbol, exc.message)
                self._last_status["open_interest"] = "error"
                clean = False
        # Write "ok" only when the loop completes without interruption (T2);
        # otherwise preserve the real state such as rate_limited/error.
        if clean:
            self._last_status["open_interest"] = "ok"
        return total

    async def _store(self, rows: list[tuple]) -> None:
        if not rows:
            return
        fetched_at = int(time.time())
        # Row format: (symbol, type, value, event_time, extra).
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
        """Age `fresh` rows: rows not refreshed for `futures_stale_after_seconds`
        become `stale`.

        This runs even when polling fails; old futures data cannot remain `fresh`
        indefinitely (2.10 fix). Return the number of aged rows.
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
            logger.info("aged futures_context: %d rows", aged)
        return aged

    async def run_loop(self, stop: asyncio.Event) -> None:
        """Run the funding and OI poll every `liquidation_poll_seconds`.

        Liquidations are separate from this loop and are fed by the WebSocket
        stream in `data/liquidation_ws.py` (the pipeline sets
        `_last_status["liquidation"]` from the WS client state).
        """
        while not stop.is_set():
            try:
                try:
                    await self.age_stale_rows()
                except Exception:  # noqa: BLE001
                    logger.exception("futures aging failed")
                try:
                    await self.poll_funding()
                except RasatError as exc:
                    logger.warning("funding poll failed: %s", exc.message)
                    self._last_status["funding"] = "error"
                try:
                    await self.poll_open_interest()
                except RasatError as exc:
                    logger.warning("OI poll failed: %s", exc.message)
                    self._last_status["open_interest"] = "error"
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("futures poll loop failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._config.liquidation_poll_seconds)
            except asyncio.TimeoutError:
                pass

    def set_liquidation_status(self, status: str) -> None:
        """The pipeline writes the WS client state (`connected`/`disconnected`) here.

        Since REST liquidation polling was removed, the state is derived from the
        public WebSocket stream provided by `data/liquidation_ws.py`.
        """
        self._last_status["liquidation"] = status

    def status(self) -> dict:
        return dict(self._last_status)
