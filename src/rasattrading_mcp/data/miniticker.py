"""Single market-wide miniTicker WebSocket.

Price/volume for all symbols are written to an in-memory cache (sufficient for
screener filters; not written to the DB). On WS disconnect or delay, mark data
`stale`; do not silently return old data as `fresh`. Reconnect with exponential
backoff, and let the watchdog close stalled connections.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass

import websockets

from ..envelope import FRESHNESS_FRESH, FRESHNESS_STALE

logger = logging.getLogger("rasattrading.data.miniticker")


@dataclass
class TickerUpdate:
    symbol: str
    last: float
    open24h: float
    high24h: float
    low24h: float
    volume: float
    quote_volume: float
    price_change_pct: float
    event_time: float


def parse_miniticker_arr(raw: str | bytes) -> list[TickerUpdate]:
    """Convert an `!miniTicker@arr` payload to a list of TickerUpdate objects.

    Binance's combined stream (`/stream?streams=!miniTicker@arr`) wraps each
    message as `{"stream": "<name>", "data": [...]}`; also accept the plain array
    form (single stream).
    """
    data = None
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return []
    if isinstance(data, dict):
        data = data.get("data", [])
    if not isinstance(data, list):
        return []
    updates: list[TickerUpdate] = []
    for item in data:
        try:
            close = float(item["c"])
            open24h = float(item["o"])
            pct = item.get("P")
            # `!miniTicker@arr` items do not carry `P` (price change %); calculate it from o/c.
            # `P` exists only in the 24hr ticker stream (accept it for compatibility).
            price_change_pct = float(pct) if pct is not None else ((close - open24h) / open24h * 100.0 if open24h else 0.0)
            updates.append(
                TickerUpdate(
                    symbol=str(item["s"]),
                    last=close,
                    open24h=open24h,
                    high24h=float(item["h"]),
                    low24h=float(item["l"]),
                    volume=float(item["v"]),
                    quote_volume=float(item["q"]),
                    price_change_pct=price_change_pct,
                    event_time=float(item.get("E", 0)) / 1000.0,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return updates


class TickerCache:
    """Symbol → latest 24h ticker plus WS health (fresh/stale)."""

    def __init__(self, stale_after: float = 30.0) -> None:
        self._data: dict[str, TickerUpdate] = {}
        self._stale_after = stale_after
        self._last_message_at: float = 0.0
        self._status = "disconnected"  # disconnected | connected
        self._status_reason: str | None = None

    def apply_updates(self, updates: list[TickerUpdate]) -> None:
        for u in updates:
            self._data[u.symbol] = u
        if updates:
            self._last_message_at = time.time()
            self.mark_connected()

    def mark_connected(self) -> None:
        """Called when the WS connection is established, even before a message arrives,
        so the watchdog can monitor staleness (a connection with no messages is also reconnected)."""
        self._status = "connected"
        self._status_reason = None

    def mark_stale(self, reason: str) -> None:
        self._status = "disconnected"
        self._status_reason = reason

    @property
    def status(self) -> str:
        return self._status

    @property
    def status_reason(self) -> str | None:
        return self._status_reason

    @property
    def last_message_at(self) -> float:
        return self._last_message_at

    def is_fresh(self) -> bool:
        return self._status == "connected" and (time.time() - self._last_message_at) <= self._stale_after

    def freshness_for(self, symbol: str) -> str:
        if symbol in self._data and self.is_fresh():
            return FRESHNESS_FRESH
        return FRESHNESS_STALE

    def get(self, symbol: str) -> dict | None:
        u = self._data.get(symbol)
        if u is None:
            return None
        return {
            "symbol": u.symbol,
            "last": u.last,
            "open_24h": u.open24h,
            "high_24h": u.high24h,
            "low_24h": u.low24h,
            "volume_24h": u.volume,
            "quote_volume_24h": u.quote_volume,
            "price_change_pct_24h": u.price_change_pct,
            "event_time": u.event_time,
            "freshness": self.freshness_for(symbol),
        }

    def snapshot(self) -> dict[str, dict]:
        return {s: self.get(s) for s in self._data}

    def health(self) -> dict:
        return {
            "status": self._status,
            "reason": self._status_reason,
            "last_message_at": self._last_message_at,
            "is_fresh": self.is_fresh(),
            "symbols": len(self._data),
        }


class MiniTickerClient:
    """miniTicker WS connection: reconnect backoff plus stale watchdog."""

    def __init__(self, url: str, cache: TickerCache, stale_after: float = 30.0) -> None:
        self._url = url
        self._cache = cache
        self._stale_after = stale_after

    async def _connect_loop(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with websockets.connect(self._url, ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024) as ws:
                    logger.info("miniTicker WS connected")
                    self._cache.mark_connected()
                    backoff = 1.0
                    async for raw in ws:
                        updates = parse_miniticker_arr(raw)
                        self._cache.apply_updates(updates)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self._cache.mark_stale(f"WS error: {exc}")
                logger.warning("miniTicker WS disconnected: %s (backoff=%ss)", exc, round(backoff, 1))
            if stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def run(self, stop: asyncio.Event) -> None:
        """Connection plus watchdog: cancel and rebuild the connection when the message stream stops."""
        check_interval = max(self._stale_after / 2, 5.0)
        while not stop.is_set():
            conn_task = asyncio.create_task(self._connect_loop(stop))
            try:
                while not conn_task.done():
                    await asyncio.sleep(check_interval)
                    if self._cache.status == "connected" and not self._cache.is_fresh():
                        self._cache.mark_stale("message stream stopped — reconnect")
                        conn_task.cancel()
            except asyncio.CancelledError:
                conn_task.cancel()
                raise
            finally:
                conn_task.cancel()
                await asyncio.gather(conn_task, return_exceptions=True)
