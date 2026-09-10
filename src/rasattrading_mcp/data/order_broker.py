"""Order broker (ticket 3.4): Binance signed orders/balances plus an abstract interface.

`OrderBroker` is a Protocol; tests use `FakeOrderBroker`, while production uses
`BinanceOrderBroker`.

- `place_order` → submit an order (with clientOrderId) and return the real Binance state.
- `query_order` → query one order by `clientOrderId` for reconcile-before-retry.
- `query_oco` → query an OCO list by `listClientOrderId`.
- `cancel_oco` → cancel an OCO list by `listClientOrderId` (kill switch).
- `get_balance` → the daemon's own fresh balance snapshot (do not trust agent figures).

Status values mirror Binance's real state machine:
NEW | PARTIALLY_FILLED | FILLED | CANCELED | REJECTED | EXPIRED | UNKNOWN.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

import aiohttp
import yarl

from ..errors import ErrorCode, RasatError
from .clock import BinanceClock
from .rate_limit import RateLimitBudget

logger = logging.getLogger("rasattrading.data.orders")

ORDER_STATUSES = ("NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED", "UNKNOWN")
TERMINAL_STATUSES = ("FILLED", "CANCELED", "REJECTED", "EXPIRED")
OPEN_STATUSES = ("NEW", "PARTIALLY_FILLED")

CredentialsProvider = Callable[[str], Awaitable[tuple[str, str]]]


@dataclass(frozen=True)
class OrderResult:
    """Order status returned by the broker (canonical, not an exact Binance copy)."""

    status: str
    exchange_order_id: str | None = None
    executed_qty: float = 0.0
    avg_price: float = 0.0
    raw: dict | None = None

    @classmethod
    def unknown(cls, raw: dict | None = None) -> "OrderResult":
        return cls(status="UNKNOWN", raw=raw)


class OrderBroker(Protocol):
    async def place_order(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        client_order_id: str,
    ) -> OrderResult: ...

    async def query_order(
        self,
        *,
        account_id: str,
        symbol: str,
        client_order_id: str,
    ) -> OrderResult | None: ...

    async def query_oco(
        self,
        *,
        account_id: str,
        list_client_order_id: str,
    ) -> OrderResult | None: ...

    async def cancel_order(
        self,
        *,
        account_id: str,
        symbol: str,
        client_order_id: str,
    ) -> OrderResult | None: ...

    async def cancel_oco(
        self,
        *,
        account_id: str,
        symbol: str,
        list_client_order_id: str,
    ) -> OrderResult | None: ...

    async def get_all_open_orders(self, *, account_id: str) -> list[dict]: ...

    async def cancel_all_open_orders(self, *, account_id: str, symbol: str) -> int: ...

    async def get_balance(self, *, account_id: str) -> dict[str, float]: ...

    async def get_balance_detail(self, *, account_id: str) -> dict[str, dict[str, float]]: ...


def to_client_order_id(idempotency_key: str) -> str:
    """Generate a safe, deterministic Binance clientOrderId from an idempotency_key.

    Binance clientOrderId: max 36 chars, [A-Za-z0-9._-]. Because it is deterministic,
    retries with the same idempotency_key use the same clientOrderId → reconciliation works.
    """
    allowed = "".join(ch for ch in idempotency_key if ch.isalnum() or ch in "._-")
    if not allowed:
        allowed = "idem"
    if len(allowed) > 24:
        allowed = allowed[:24]
    digest = hashlib.sha1(idempotency_key.encode("utf-8")).hexdigest()[:11]
    return f"{allowed}-{digest}"


class BinanceOrderBroker:
    """Binance spot signed REST client (HMAC-SHA256). Use in production only outside tests."""

    def __init__(
        self,
        base_url: str,
        credentials: CredentialsProvider,
        budget: RateLimitBudget | None = None,
        session: aiohttp.ClientSession | None = None,
        recv_window_ms: int = 10_000,
        timeout_seconds: float = 20.0,
        clock: BinanceClock | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._credentials = credentials
        self._budget = budget
        self._session = session
        self._own_session = session is None
        self._recv_window_ms = recv_window_ms
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        #: T1 coordination: BinanceClock with a server-time offset for signed-request
        #: timestamps. If supplied, `_signed_request_url` uses `clock.server_now()`;
        #: otherwise (or when the clock fails closed → None), retain the old
        #: `time.time()` fallback for backward compatibility.
        self._clock = clock

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    def _now_ms(self) -> int:
        """Timestamp for a signed request (ms).

        If `clock` is supplied and `server_now()` returns reliable server time,
        use it (protects against host-clock drift and -1021/1022); if no clock is
        available or it fails closed (None), use the old local `time.time()` fallback.
        """
        if self._clock is not None:
            server_now = self._clock.server_now()
            if server_now is not None:
                return int(server_now * 1000)
        return int(time.time() * 1000)

    async def _signed_request_url(self, account_id: str, path: str, params: dict) -> tuple[str, str]:
        """Build the (api_key, full URL) pair for a signed request.

        Compute the signature over exactly the query string yarl will generate
        (Binance verifies the raw query in the order sent). If the params dict is
        passed to aiohttp, yarl chooses its own insertion order, which does not
        match the sorted signed string and produces -1022 on the live API.
        """
        api_key, api_secret = await self._credentials(account_id)
        base = dict(params)
        now = self._now_ms()
        base["timestamp"] = now
        base["recvWindow"] = self._recv_window_ms
        url = yarl.URL(f"{self._base_url}{path}").with_query(base)
        signature = hmac.new(api_secret.encode("utf-8"), url.query_string.encode("utf-8"), hashlib.sha256).hexdigest()
        signed_url = str(url.with_query({**base, "signature": signature}))
        return api_key, signed_url

    async def _request(self, method: str, path: str, account_id: str, params: dict) -> dict:
        if self._budget is not None:
            weight = 5 if path == "/api/v3/account" else 1
            await self._budget.acquire(weight)
        api_key, signed_url = await self._signed_request_url(account_id, path, params)
        session = await self._get_session()
        headers = {"X-MBX-APIKEY": api_key}
        try:
            async with session.request(method, signed_url, headers=headers, timeout=self._timeout) as resp:
                used = resp.headers.get("x-mbx-used-weight-1m")
                if used and used.isdigit() and self._budget is not None:
                    self._budget.note_used(int(used))
                if resp.status in (401, 403):
                    raise RasatError(ErrorCode.UNAUTHORIZED, f"Binance {resp.status} — invalid API key ({path})")
                if resp.status >= 400:
                    try:
                        body = await resp.json()
                    except Exception:  # noqa: BLE001
                        body = {}
                    code = body.get("code") if isinstance(body, dict) else None
                    msg = body.get("msg") or f"Binance {resp.status}"
                    raise RasatError(ErrorCode.ORDER_REJECTED, f"{msg} ({path})", details={"binance_code": code})
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, RasatError) as exc:
            if isinstance(exc, RasatError):
                raise
            # T01: do not let asyncio.TimeoutError escape; map it to canonical TIMEOUT.
            # OrderService enters the single reconciliation path for this code; no blind retry.
            raise RasatError(ErrorCode.TIMEOUT, f"Binance request timed out/failed ({path}): {exc}") from exc

    @staticmethod
    def _order_result(data: dict) -> OrderResult:
        status = str(data.get("status") or "UNKNOWN").upper()
        if status not in ORDER_STATUSES:
            status = "UNKNOWN"
        executed_qty = 0.0
        avg_price = 0.0
        try:
            executed_qty = float(data.get("executedQty", 0))
            cum_quote = float(data.get("cummulativeQuoteQty", 0))
            avg_price = cum_quote / executed_qty if executed_qty else 0.0
        except (TypeError, ValueError):
            pass
        return OrderResult(
            status=status,
            exchange_order_id=str(data.get("orderId")) if data.get("orderId") is not None else None,
            executed_qty=executed_qty,
            avg_price=avg_price,
            raw=data,
        )

    async def place_order(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        client_order_id: str,
        stop_price: float | None = None,
    ) -> OrderResult:
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type.upper(),
            "quantity": str(quantity),
        }
        if order_type.upper() == "LIMIT":
            if price is None:
                raise RasatError(ErrorCode.INVALID_REQUEST, "price is required for a limit order")
            params["price"] = str(price)
            params["timeInForce"] = "GTC"
        elif order_type.upper() == "STOP_LOSS_LIMIT":
            # Spot stop protection: reaching stopPrice triggers the LIMIT sale.
            if stop_price is None:
                raise RasatError(ErrorCode.INVALID_REQUEST, "stop_price is required for a stop order")
            if price is None:
                raise RasatError(ErrorCode.INVALID_REQUEST, "price is required for a stop order")
            params["stopPrice"] = str(stop_price)
            params["price"] = str(price)
            params["timeInForce"] = "GTC"
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        data = await self._request("POST", "/api/v3/order", account_id, params)
        return self._order_result(data)

    async def place_oco(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        stop_price: float,
        stop_limit_price: float,
        client_order_id: str,
    ) -> OrderResult:
        """Spot OCO order (`/api/v3/orderList/oco`): LIMIT_MAKER + STOP_LOSS_LIMIT in one request.

        When one fills, the exchange automatically cancels the other (true OCO guarantee).
        SELL (close long): `above` = profit target (LIMIT_MAKER, above the price),
        `below` = stop (STOP_LOSS_LIMIT, below the price).
        BUY (close short): `above` = stop, `below` = profit target.

        Binance `/orderList/oco` requires `aboveType`/`belowType` (2026-04+);
        the old flat `price`/`stopPrice`/`stopLimitPrice` form is rejected with
        "Mandatory parameter 'aboveType' was not sent" — verified live (07.08).
        `price` = profit-target limit, `stop_price` = stop trigger,
        `stop_limit_price` = limit price sold when the stop triggers.
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "quantity": str(quantity),
            "listClientOrderId": client_order_id if client_order_id else None,
        }
        if side.upper() == "BUY":
            params["aboveType"] = "STOP_LOSS_LIMIT"
            params["aboveStopPrice"] = str(stop_price)
            params["abovePrice"] = str(stop_limit_price)
            params["aboveTimeInForce"] = "GTC"
            params["belowType"] = "LIMIT_MAKER"
            params["belowPrice"] = str(price)
        else:
            params["aboveType"] = "LIMIT_MAKER"
            params["abovePrice"] = str(price)
            params["belowType"] = "STOP_LOSS_LIMIT"
            params["belowStopPrice"] = str(stop_price)
            params["belowPrice"] = str(stop_limit_price)
            params["belowTimeInForce"] = "GTC"
        data = await self._request("POST", "/api/v3/orderList/oco", account_id, params)
        # OCO responses carry orderListId (not orderId); store the list as the trace.
        return OrderResult(
            status="NEW",
            exchange_order_id=str(data.get("orderListId")) if data.get("orderListId") is not None else None,
            executed_qty=0.0,
            avg_price=0.0,
            raw=data,
        )

    async def query_order(self, *, account_id: str, symbol: str, client_order_id: str) -> OrderResult | None:
        try:
            data = await self._request(
                "GET",
                "/api/v3/order",
                account_id,
                {"symbol": symbol, "origClientOrderId": client_order_id},
            )
        except RasatError as exc:
            # -2013 order not found → None (safe to resubmit).
            if exc.code == ErrorCode.ORDER_REJECTED and exc.details and exc.details.get("binance_code") == -2013:
                return None
            raise
        return self._order_result(data)

    async def query_oco(self, *, account_id: str, list_client_order_id: str) -> OrderResult | None:
        """Query the OCO by `listClientOrderId` (`GET /api/v3/orderList`).

        Unlike `query_order`, OCO legs carry clientOrderIds generated by Binance;
        our `listClientOrderId` can only be queried at the order-list level and
        cannot be found through individual `/api/v3/order` (which previously
        caused everything to become UNKNOWN).
        """
        try:
            data = await self._request(
                "GET",
                "/api/v3/orderList",
                account_id,
                {"origClientOrderId": list_client_order_id},
            )
        except RasatError as exc:
            if exc.code == ErrorCode.ORDER_REJECTED and exc.details and exc.details.get("binance_code") == -2013:
                return None
            raise
        list_status = str(data.get("listOrderStatus") or "").upper()
        # EXECUTING = OCO is still active/alive on the exchange (protection in place) → NEW.
        # ALL_DONE = one leg filled/canceled and the list completed → terminal.
        # Other/unknown values become UNKNOWN (never blindly assume "alive").
        if list_status == "EXECUTING":
            status = "NEW"
        elif list_status == "ALL_DONE":
            status = "FILLED"
        else:
            status = "UNKNOWN"
        return OrderResult(
            status=status,
            exchange_order_id=str(data.get("orderListId")) if data.get("orderListId") is not None else None,
            raw=data,
        )

    async def cancel_order(self, *, account_id: str, symbol: str, client_order_id: str) -> OrderResult | None:
        try:
            data = await self._request(
                "DELETE",
                "/api/v3/order",
                account_id,
                {"symbol": symbol, "origClientOrderId": client_order_id},
            )
        except RasatError as exc:
            # -2011 order already canceled/filled → None.
            if exc.code == ErrorCode.ORDER_REJECTED and exc.details and exc.details.get("binance_code") == -2011:
                return None
            raise
        return self._order_result(data)

    async def cancel_oco(
        self, *, account_id: str, symbol: str, list_client_order_id: str
    ) -> OrderResult | None:
        """Cancel the OCO list by `listClientOrderId` (`DELETE /api/v3/orderList`).

        Because OCO legs carry clientOrderIds generated by Binance, they cannot be
        canceled with individual `cancel_order`; the kill switch must cancel OCO
        rows by the `listClientOrderId` matched through `query_oco`.
        The response carries `listOrderStatus`: ALL_DONE → CANCELED, EXECUTING → NEW,
        unknown → UNKNOWN (never blindly assume "canceled").
        """
        try:
            data = await self._request(
                "DELETE",
                "/api/v3/orderList",
                account_id,
                {"symbol": symbol, "listClientOrderId": list_client_order_id},
            )
        except RasatError as exc:
            # -2011 list already canceled/filled → None.
            if exc.code == ErrorCode.ORDER_REJECTED and exc.details and exc.details.get("binance_code") == -2011:
                return None
            raise
        list_status = str(data.get("listOrderStatus") or "").upper()
        if list_status == "ALL_DONE":
            status = "CANCELED"
        elif list_status == "EXECUTING":
            status = "NEW"
        else:
            status = "UNKNOWN"
        return OrderResult(
            status=status,
            exchange_order_id=str(data.get("orderListId")) if data.get("orderListId") is not None else None,
            raw=data,
        )

    async def get_all_open_orders(self, *, account_id: str) -> list[dict]:
        """Return all open spot orders (global, not symbol-specific)."""
        data = await self._request("GET", "/api/v3/openOrders", account_id, {})
        if not isinstance(data, list):
            return []
        return [
            {
                "symbol": str(o.get("symbol", "")),
                "order_id": str(o.get("orderId")) if o.get("orderId") is not None else None,
                "client_order_id": str(o.get("clientOrderId")) if o.get("clientOrderId") else None,
                "side": str(o.get("side", "")),
                "quantity": float(o.get("origQty", 0) or 0),
                "raw": o,
            }
            for o in data
        ]

    async def cancel_all_open_orders(self, *, account_id: str, symbol: str) -> int:
        """Cancel all open orders for a symbol and return the number canceled."""
        try:
            data = await self._request(
                "DELETE",
                "/api/v3/openOrders",
                account_id,
                {"symbol": symbol},
            )
        except RasatError as exc:
            # -2011 no open orders → 0.
            if exc.code == ErrorCode.ORDER_REJECTED and exc.details and exc.details.get("binance_code") == -2011:
                return 0
            raise
        return len(data) if isinstance(data, list) else 0

    async def get_balance(self, *, account_id: str) -> dict[str, float]:
        detail = await self.get_balance_detail(account_id=account_id)
        return {asset: b["free"] for asset, b in detail.items() if b["free"]}

    async def get_balance_detail(self, *, account_id: str) -> dict[str, dict[str, float]]:
        """Full balance including free + locked; `locked` is held by open orders (3.21)."""
        data = await self._request("GET", "/api/v3/account", account_id, {})
        balances: dict[str, dict[str, float]] = {}
        for b in data.get("balances", []):
            balances[str(b["asset"])] = {
                "free": float(b.get("free", 0) or 0),
                "locked": float(b.get("locked", 0) or 0),
            }
        return balances

    async def close(self) -> None:
        if self._own_session and self._session is not None and not self._session.closed:
            await self._session.close()
