"""Market-wide liquidation stream (`!forceOrder@arr`) WebSocket.

`/fapi/v1/forceOrders` is a signed USER_DATA endpoint; it returns only the
liquidations for the account associated with that API key, NOT market-wide data.
For market-wide liquidations, use Binance's PUBLIC futures WebSocket stream
`wss://fstream.binance.com/ws/!forceOrder@arr` (no signature required; it broadcasts
liquidation orders for all symbols).

Each event is written directly to the `futures_context` table as
`type='liquidation'` (the same INSERT pattern as `FuturesContextPoller._store`,
with `value = price*qty` and `extra` containing side/price/qty). On WS disconnect
or delay, mark the state `disconnected`; do not silently return old data as
`fresh`. Reconnect with exponential backoff, and let the watchdog close stalled
connections (the miniticker.py pattern).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass

import websockets

from ..timeutil import to_epoch_seconds

logger = logging.getLogger("rasattrading.data.liquidation_ws")


@dataclass
class LiquidationEvent:
    symbol: str
    side: str
    price: float
    qty: float
    event_time: int


def parse_force_order_arr(raw: str | bytes) -> list[LiquidationEvent]:
    """Convert an `!forceOrder@arr` payload to a list of LiquidationEvent objects.

    A Binance futures forceOrder event has the form
    `{"e":"forceOrder","E":<eventTime>,"o":{...}}` (`o` carries symbol/side/price/quantity).
    The combined stream (`/stream?streams=!forceOrder@arr`) wraps each message as
    `{"stream":..., "data":...}`; accept both forms and, defensively, a list form.
    Skip events with missing or malformed fields.
    """
    data = None
    try:
        data = json.loads(raw)
    except (ValueError, TypeError):
        return []
    if isinstance(data, dict):
        data = data.get("data", data)
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        return []
    events: list[LiquidationEvent] = []
    for item in data:
        try:
            if not isinstance(item, dict):
                continue
            order = item.get("o")
            if not isinstance(order, dict):
                continue
            # `E` (event time) is in ms; fall back to `T` (trade time) if absent.
            # Both are converted to seconds by `to_epoch_seconds` for one standard unit.
            event_time = to_epoch_seconds(item.get("E") or order.get("T") or 0)
            events.append(
                LiquidationEvent(
                    symbol=str(order["s"]),
                    side=str(order.get("S", "?")),
                    price=float(order["p"]),
                    qty=float(order["q"]),
                    event_time=event_time,
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return events


class LiquidationWSClient:
    """forceOrder WS connection: reconnect backoff, stale watchdog, and direct DB writes.

    The default `stale_after` is longer than miniTicker's: `!forceOrder@arr` is
    low-frequency (no events may arrive for minutes during quiet periods), so a
    short timeout would incorrectly mark a live connection as `stale`. A broken
    connection is already detected quickly by `ping_interval/ping_timeout` (20s).
    """

    def __init__(self, url: str, db, stale_after: float = 300.0) -> None:
        self._url = url
        self._db = db
        self._stale_after = stale_after
        self._status = "disconnected"  # disconnected | connected
        self._status_reason: str | None = None
        self._last_message_at: float = 0.0
        self._events_written = 0

    def mark_connected(self) -> None:
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

    @property
    def events_written(self) -> int:
        return self._events_written

    def is_fresh(self) -> bool:
        return self._status == "connected" and (time.time() - self._last_message_at) <= self._stale_after

    def health(self) -> dict:
        return {
            "status": self._status,
            "reason": self._status_reason,
            "last_message_at": self._last_message_at,
            "is_fresh": self.is_fresh(),
            "events_written": self._events_written,
        }

    async def _store(self, rows: list[tuple]) -> None:
        """Write `type='liquidation'` rows to `futures_context` (poller pattern).

        Row format: (symbol, "liquidation", value=price*qty, event_time, extra).
        """
        if not rows:
            return
        fetched_at = int(time.time())

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

    async def _handle_raw(self, raw: str | bytes) -> None:
        """Parse one WS message and write it to the DB. DB errors do not drop the connection."""
        events = parse_force_order_arr(raw)
        if events:
            rows = [
                (
                    e.symbol,
                    "liquidation",
                    e.price * e.qty,
                    e.event_time,
                    {"side": e.side, "price": e.price, "qty": e.qty},
                )
                for e in events
            ]
            try:
                await self._store(rows)
                self._events_written += len(rows)
            except Exception:  # noqa: BLE001
                logger.exception("liquidation DB write failed")
        self._last_message_at = time.time()
        self.mark_connected()

    async def _connect_loop(self, stop: asyncio.Event) -> None:
        backoff = 1.0
        while not stop.is_set():
            try:
                async with websockets.connect(self._url, ping_interval=20, ping_timeout=20, max_size=16 * 1024 * 1024) as ws:
                    logger.info("forceOrder WS connected")
                    self.mark_connected()
                    backoff = 1.0
                    async for raw in ws:
                        await self._handle_raw(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.mark_stale(f"WS error: {exc}")
                logger.warning("forceOrder WS disconnected: %s (backoff=%ss)", exc, round(backoff, 1))
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
                    if self.status == "connected" and not self.is_fresh():
                        self.mark_stale("message stream stopped — reconnect")
                        conn_task.cancel()
            except asyncio.CancelledError:
                conn_task.cancel()
                raise
            finally:
                conn_task.cancel()
                await asyncio.gather(conn_task, return_exceptions=True)
