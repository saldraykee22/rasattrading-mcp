"""Emir broker'ı (ticket 3.4): Binance signed order/balance + soyut arayüz.

`OrderBroker` bir Protocol'dür; testler `FakeOrderBroker` kullanır, üretim
`BinanceOrderBroker` ile çalışır.

- `place_order` → emir gönderir (clientOrderId ile), gerçek Binance state'i döner.
- `query_order` → reconcile-before-retry için emri `clientOrderId` ile sorgular.
- `get_balance` → daemon'ın kendi taze bakiye snapshot'ı (agent rakamlarına güvenilmez).

Status değerleri Binance'in gerçek state machine'ini yansıtır:
NEW | PARTIALLY_FILLED | FILLED | CANCELED | REJECTED | EXPIRED | UNKNOWN.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

import aiohttp
import yarl

from ..errors import ErrorCode, RasatError
from .rate_limit import RateLimitBudget

logger = logging.getLogger("rasattrading.data.orders")

ORDER_STATUSES = ("NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED", "UNKNOWN")
TERMINAL_STATUSES = ("FILLED", "CANCELED", "REJECTED", "EXPIRED")
OPEN_STATUSES = ("NEW", "PARTIALLY_FILLED")

CredentialsProvider = Callable[[str], Awaitable[tuple[str, str]]]


@dataclass(frozen=True)
class OrderResult:
    """Broker'dan dönen emir durumu (canonical, Binance'e birebir değil)."""

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

    async def cancel_order(
        self,
        *,
        account_id: str,
        symbol: str,
        client_order_id: str,
    ) -> OrderResult | None: ...

    async def get_all_open_orders(self, *, account_id: str) -> list[dict]: ...

    async def cancel_all_open_orders(self, *, account_id: str, symbol: str) -> int: ...

    async def get_balance(self, *, account_id: str) -> dict[str, float]: ...

    async def get_balance_detail(self, *, account_id: str) -> dict[str, dict[str, float]]: ...


def to_client_order_id(idempotency_key: str) -> str:
    """idempotency_key'den güvenli, deterministic bir Binance clientOrderId üretir.

    Binance clientOrderId: max 36 char, [A-Za-z0-9._-]. Deterministic olduğu için
    aynı idempotency_key retry'i aynı clientOrderId'ye bağlanır → reconcile çalışır.
    """
    allowed = "".join(ch for ch in idempotency_key if ch.isalnum() or ch in "._-")
    if not allowed:
        allowed = "idem"
    if len(allowed) > 24:
        allowed = allowed[:24]
    digest = hashlib.sha1(idempotency_key.encode("utf-8")).hexdigest()[:11]
    return f"{allowed}-{digest}"


class BinanceOrderBroker:
    """Binance spot signed REST client (HMAC-SHA256). Test dışında üretim kullanımı."""

    def __init__(
        self,
        base_url: str,
        credentials: CredentialsProvider,
        budget: RateLimitBudget | None = None,
        session: aiohttp.ClientSession | None = None,
        recv_window_ms: int = 10_000,
        timeout_seconds: float = 20.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._credentials = credentials
        self._budget = budget
        self._session = session
        self._own_session = session is None
        self._recv_window_ms = recv_window_ms
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def _signed_request_url(self, account_id: str, path: str, params: dict) -> tuple[str, str]:
        """İmzalı isteğin (api_key, tam URL) çiftini üretir.

        İmza, yarl'ın üreteceği query string'in BİREBİR aynısı üzerinden
        hesaplanır (Binance, gönderilen ham query sırasına göre doğrular).
        aiohttp'e params= dict'i bırakılırsa yarl kendi ekleme sırasını kullanır
        ve sorted() imzalı string'le uyuşmaz — canlı API'de -1022 üretir.
        """
        api_key, api_secret = await self._credentials(account_id)
        base = dict(params)
        base["timestamp"] = int(time.time() * 1000)
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
                    raise RasatError(ErrorCode.UNAUTHORIZED, f"Binance {resp.status} — geçersiz API key ({path})")
                if resp.status >= 400:
                    try:
                        body = await resp.json()
                    except Exception:  # noqa: BLE001
                        body = {}
                    code = body.get("code") if isinstance(body, dict) else None
                    msg = body.get("msg") or f"Binance {resp.status}"
                    raise RasatError(ErrorCode.ORDER_REJECTED, f"{msg} ({path})", details={"binance_code": code})
                return await resp.json()
        except (aiohttp.ClientError, RasatError) as exc:
            if isinstance(exc, RasatError):
                raise
            raise RasatError(ErrorCode.TIMEOUT, f"Binance istek zaman aşımı/hatası ({path}): {exc}") from exc

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
                raise RasatError(ErrorCode.INVALID_REQUEST, "limit emirde price zorunlu")
            params["price"] = str(price)
            params["timeInForce"] = "GTC"
        elif order_type.upper() == "STOP_LOSS_LIMIT":
            # Spot stop koruması: stopPrice'a ulaşınca LIMIT satış tetiklenir.
            if stop_price is None:
                raise RasatError(ErrorCode.INVALID_REQUEST, "stop emirde stop_price zorunlu")
            if price is None:
                raise RasatError(ErrorCode.INVALID_REQUEST, "stop emirde price zorunlu")
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
        """Spot OCO emri (`/api/v3/orderList/oco`): LIMIT + STOP_LOSS_LIMIT tek istekte.

        Biri dolunca diğeri borsada otomatik iptal olur (true OCO garantisi).
        `price` = kâr hedefi (limit), `stop_price` = stop tetikleme,
        `stop_limit_price` = stop tetiklenince satılacak limit fiyatı.
        Aynı pozisyon için ayrı ayrı SL+TP emri bakiyeyi birbirinden çaldığı için
        mümkün değildi; bu çağrı ikisini tek emir listesinde taşır.
        """
        params: dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "quantity": str(quantity),
            "price": str(price),
            "stopPrice": str(stop_price),
            "stopLimitPrice": str(stop_limit_price),
            "stopLimitTimeInForce": "GTC",
        }
        if client_order_id:
            params["listClientOrderId"] = client_order_id
        data = await self._request("POST", "/api/v3/orderList/oco", account_id, params)
        # OCO yanıtı orderListId taşır (orderId değil) — listeyi iz olarak sakla.
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
            # -2013 emir yok → None (yeniden gönderim güvenli)
            if exc.code == ErrorCode.ORDER_REJECTED and exc.details and exc.details.get("binance_code") == -2013:
                return None
            raise
        return self._order_result(data)

    async def cancel_order(self, *, account_id: str, symbol: str, client_order_id: str) -> OrderResult | None:
        try:
            data = await self._request(
                "DELETE",
                "/api/v3/order",
                account_id,
                {"symbol": symbol, "origClientOrderId": client_order_id},
            )
        except RasatError as exc:
            # -2011 emir zaten iptal/dolmuş → None
            if exc.code == ErrorCode.ORDER_REJECTED and exc.details and exc.details.get("binance_code") == -2011:
                return None
            raise
        return self._order_result(data)

    async def get_all_open_orders(self, *, account_id: str) -> list[dict]:
        """Spot'taki tüm açık emirleri döner (sembol bazlı değil, global)."""
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
        """Bir semboldeki tüm açık emirleri iptal eder; iptal edilen sayıyı döner."""
        try:
            data = await self._request(
                "DELETE",
                "/api/v3/openOrders",
                account_id,
                {"symbol": symbol},
            )
        except RasatError as exc:
            # -2011 açık emir yok → 0
            if exc.code == ErrorCode.ORDER_REJECTED and exc.details and exc.details.get("binance_code") == -2011:
                return 0
            raise
        return len(data) if isinstance(data, list) else 0

    async def get_balance(self, *, account_id: str) -> dict[str, float]:
        detail = await self.get_balance_detail(account_id=account_id)
        return {asset: b["free"] for asset, b in detail.items() if b["free"]}

    async def get_balance_detail(self, *, account_id: str) -> dict[str, dict[str, float]]:
        """free + locked içeren tam bakiye; `locked` açık emirlerde kilitli (3.21)."""
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
