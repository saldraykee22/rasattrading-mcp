"""Order execution service (ticket 3.4).

`execute_on_accounts` / `place_order`:

- **Idempotency:** the `orders` table has a UNIQUE (account_id, idempotency_key).
  A retry with the same key does not create a duplicate order; the existing order
  is returned / its state is reconciled.
- **Reconcile-before-retry:** after a network timeout, Binance is queried for the
  real state using `client_order_id`; if found, that state is used, otherwise
  UNKNOWN is returned. Blind resubmission is NEVER performed.
- **Per-account serialization:** an `asyncio.Lock` per account prevents two
  concurrent `execute_on_accounts` calls from racing on the same account.
- **Aggregate exposure:** open-order notional + partial fills + base balance
  (the daemon's fresh balance/price snapshot) are checked against the policy cap;
  a one-time override is consumed if the cap would be exceeded.
- **Partial success:** bulk calls return a separate result for each account.
- Basic correctness checks (3.3) always run first; an override never bypasses them.
"""

from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
import time
import uuid
from typing import Any, Protocol

from ..accuracy import check_price_fresh, check_stop_direction, check_symbol_valid
from ..errors import ErrorCode, RasatError
from ..envelope import FRESHNESS_FRESH
from ..position_sizing import SymbolFilters, calculate_position_size
from .accounts import AccountService
from .order_validation import normalize_order_type, validate_execution_order
from .audit import AuditLog
from .db import Database
from .risk_policy import RiskPolicyService

logger = logging.getLogger("rasattrading.storage.orders")

#: Binance state machine + local PAPER (paper account simulation).
STATUS_PAPER = "paper"

_ORDER_COLUMNS = (
    "order_id",
    "account_id",
    "idempotency_key",
    "symbol",
    "side",
    "order_type",
    "quantity",
    "price",
    "stop_price",
    "stop_limit_price",
    "status",
    "exchange_order_id",
    "client_order_id",
    "executed_qty",
    "avg_price",
    "fee",
    "notional",
    "reference_price",
    "equity_snapshot",
    "error_code",
    "error_message",
    "created_at",
    "updated_at",
)


class MarketFeed(Protocol):
    """The daemon's fresh market snapshot (agent-provided figures are untrusted)."""

    async def symbol_valid(self, symbol: str) -> bool: ...
    async def price(self, symbol: str) -> float | None: ...
    async def filters(self, symbol: str) -> SymbolFilters | None: ...


class PipelineMarketFeed:
    """Adapt DataPipeline to the MarketFeed interface."""

    def __init__(self, pipeline: Any) -> None:
        self._pipeline = pipeline

    async def symbol_valid(self, symbol: str) -> bool:
        return await self._pipeline.ensure_symbol(symbol)

    async def price(self, symbol: str) -> float | None:
        ticker = self._pipeline.get_ticker(symbol)
        if ticker is None or ticker.get("freshness") != FRESHNESS_FRESH:
            return None
        return float(ticker["last"])

    async def filters(self, symbol: str) -> SymbolFilters | None:
        info = self._pipeline.symbol_info(symbol)
        if info is None:
            return None
        return SymbolFilters.from_exchange_info(info)


class OrderService:
    MIN_UNPROTECTED_VALUE_USD = 10.0

    def __init__(
        self,
        db: Database,
        accounts: AccountService,
        risk: RiskPolicyService,
        broker: Any,
        market: MarketFeed,
        audit: AuditLog | None = None,
        default_fee_rate: float = 0.001,
    ) -> None:
        self.db = db
        self.accounts = accounts
        self.risk = risk
        self.broker = broker
        self.market = market
        self.audit = audit
        self.default_fee_rate = default_fee_rate
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, account_id: str) -> asyncio.Lock:
        lock = self._locks.get(account_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[account_id] = lock
        return lock

    # ---------- target resolution ----------

    @staticmethod
    def _require_number(value: Any, name: str) -> float:
        """Required numeric parameter: missing/invalid/non-finite values are INVALID_REQUEST.

        `float(None)` raised TypeError and was swallowed as UNKNOWN below; a
        missing required parameter must return canonical INVALID_REQUEST. NaN and
        Infinity (from non-standard JSON parsing) must not bypass comparisons—T01.
        """
        if value is None or isinstance(value, bool):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} is required (number)")
        try:
            num = float(value)
        except (TypeError, ValueError):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} must be a number")
        if not math.isfinite(num):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} must be a finite number")
        return num

    @staticmethod
    def _require_string(value: Any, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} is required (string)")
        return value.strip()

    async def _resolve_targets(self, account_ids: Any, tags: Any) -> list[dict]:
        if not account_ids and not tags:
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_ids or tags is required (neither can be empty)")
        id_set = {str(a).strip() for a in (account_ids or []) if a}
        tag_list = [str(t).strip() for t in (tags or []) if t]
        listed = await self.accounts.list_accounts()
        accounts = listed["accounts"]
        missing = [a for a in id_set if not any(acc["account_id"] == a for acc in accounts)]
        if missing:
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account not found: {missing[0]}")
        selected = [
            acc
            for acc in accounts
            if acc["account_id"] in id_set or (tag_list and any(t in acc["tags"] for t in tag_list))
        ]
        return selected

    # ---------- aggregate exposure ----------

    @staticmethod
    def _open_order_notional(conn: sqlite3.Connection, account_id: str) -> float:
        # 3.17: Include UNKNOWN in exposure conservatively (assume it is filled).
        row = conn.execute(
            "SELECT COALESCE(SUM(notional), 0) AS total FROM orders "
            "WHERE account_id = ? AND status IN ('NEW', 'PARTIALLY_FILLED', 'UNKNOWN')",
            (account_id,),
        ).fetchone()
        return float(row["total"]) if row else 0.0

    async def _current_exposure(self, account_id: str, quote_asset: str) -> float:
        """Open orders + partial fills + (base) balance value—the daemon's fresh data."""

        def _open(conn: sqlite3.Connection) -> float:
            return self._open_order_notional(conn, account_id)

        open_notional = await self.db.read(_open)
        balances = await self.broker.get_balance(account_id=account_id)
        held_value = 0.0
        for asset, free in balances.items():
            if asset == quote_asset:
                continue
            if free <= 0:
                continue
            symbol = f"{asset}{quote_asset}"
            price = await self.market.price(symbol)
            if price is None:
                raise RasatError(ErrorCode.STALE_DATA, f"could not obtain exposure price: {symbol}")
            held_value += free * price
        return open_notional + held_value

    async def _equity_from_balances(self, balances: dict[str, float], quote_asset: str) -> float:
        """Shared equity calculation (3.3 logic): free quote + market value of base assets.

        Only free amounts are used; amounts locked in open orders are excluded
        from equity for the balance check (3.8), because that check uses the
        "free amount available to send to the exchange." Assets with unknown
        prices are excluded from the value calculation.
        """
        equity = 0.0
        for asset, free in balances.items():
            if asset == quote_asset:
                equity += free
                continue
            asset_price = await self.market.price(f"{asset}{quote_asset}")
            if asset_price is not None:
                equity += free * asset_price
        return equity

    async def _account_balance_breakdown(self, account_id: str, quote_asset: str) -> dict[str, Any]:
        """Full account balance breakdown (3.21): free / locked / holdings_value / total.

        - `free` → free (available) quote asset amount.
        - `locked` → market value of amounts locked in open orders (quote + base).
        - `holdings_value` → current market value of held base assets (free).
        - `total`/`equity` → free + locked + holdings_value (true total account value).
        - `assets` → per-asset details (free/locked/value).
        """
        detail = await self.broker.get_balance_detail(account_id=account_id)
        free = 0.0
        locked = 0.0
        holdings_value = 0.0
        assets: list[dict[str, Any]] = []
        for asset, bal in detail.items():
            free_qty = float(bal.get("free", 0) or 0)
            locked_qty = float(bal.get("locked", 0) or 0)
            if asset == quote_asset:
                free += free_qty
                locked += locked_qty
                assets.append({"asset": asset, "free": free_qty, "locked": locked_qty, "value": free_qty})
                continue
            symbol = f"{asset}{quote_asset}"
            price = await self.market.price(symbol)
            if price is None:
                # Assets with unknown prices are excluded from value; their amounts are still carried through.
                assets.append({"asset": asset, "free": free_qty, "locked": locked_qty, "value": None})
                continue
            holdings_value += free_qty * price
            locked += locked_qty * price
            assets.append({"asset": asset, "free": free_qty, "locked": locked_qty, "value": (free_qty + locked_qty) * price})
        total = free + locked + holdings_value
        return {
            "account_id": account_id,
            "quote_asset": quote_asset,
            "free": free,
            "locked": locked,
            "holdings_value": holdings_value,
            "total": total,
            "equity": total,
            "assets": assets,
        }

    # ---------- order record ----------

    def _insert_order(
        self,
        conn: sqlite3.Connection,
        *,
        account_id: str,
        idempotency_key: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        notional: float,
        reference_price: float,
        equity_snapshot: float,
        status: str,
        client_order_id: str,
        stop_price: float | None = None,
        stop_limit_price: float | None = None,
    ) -> dict:
        now = int(time.time())
        order_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO orders "
            "(order_id, account_id, idempotency_key, symbol, side, order_type, quantity, price, stop_price, "
            " stop_limit_price, status, "
            " client_order_id, executed_qty, avg_price, fee, notional, reference_price, equity_snapshot, "
            " created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,?,?,?,?,?)",
            (
                order_id,
                account_id,
                idempotency_key,
                symbol,
                side,
                order_type,
                quantity,
                price,
                stop_price,
                stop_limit_price,
                status,
                client_order_id,
                notional,
                reference_price,
                equity_snapshot,
                now,
                now,
            ),
        )
        return {
            "order_id": order_id,
            "account_id": account_id,
            "idempotency_key": idempotency_key,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "quantity": quantity,
            "price": price,
            "stop_price": stop_price,
            "stop_limit_price": stop_limit_price,
            "status": status,
            "exchange_order_id": None,
            "client_order_id": client_order_id,
            "executed_qty": 0.0,
            "avg_price": 0.0,
            "fee": 0.0,
            "notional": notional,
            "reference_price": reference_price,
            "equity_snapshot": equity_snapshot,
            "error_code": None,
            "error_message": None,
            "created_at": now,
            "updated_at": now,
        }

    def _update_order(self, conn: sqlite3.Connection, order_id: str, **fields: Any) -> None:
        now = int(time.time())
        sets = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(f"UPDATE orders SET {sets}, updated_at = ? WHERE order_id = ?", (*fields.values(), now, order_id))

    def _load_order(self, conn: sqlite3.Connection, account_id: str, idempotency_key: str) -> dict | None:
        row = conn.execute(
            "SELECT " + ", ".join(_ORDER_COLUMNS) + " FROM orders WHERE account_id = ? AND idempotency_key = ?",
            (account_id, idempotency_key),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    @staticmethod
    def _load_open_close(conn: sqlite3.Connection, account_id: str, symbol: str) -> dict | None:
        """Return an in-flight/unknown close SELL for this account and symbol, if any.

        3.14: because a run nonce is added to the idem key, the same-key search
        does not match across runs; to prevent duplicate sells, an open/unknown
        close order is searched by symbol and reconciled with the broker's real state.
        """
        row = conn.execute(
            "SELECT " + ", ".join(_ORDER_COLUMNS)
            + " FROM orders WHERE account_id = ? AND symbol = ? AND side = 'SELL'"
            + " AND idempotency_key LIKE 'close-%' AND status IN ('NEW', 'PARTIALLY_FILLED', 'UNKNOWN')"
            + " ORDER BY created_at DESC LIMIT 1",
            (account_id, symbol),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    @staticmethod
    def _order_to_result(order: dict, *, position_size: float | None = None, error: dict | None = None) -> dict:
        result = {
            "account_id": order["account_id"],
            "status": order["status"],
            "symbol": order["symbol"],
            "side": order["side"],
            "order_type": order["order_type"],
            "quantity": order["quantity"],
            "price": order.get("price"),
            "stop_price": order.get("stop_price"),
            "stop_limit_price": order.get("stop_limit_price"),
            "notional": order["notional"],
            "order_id": order.get("order_id"),
            "exchange_order_id": order.get("exchange_order_id"),
            "executed_qty": order.get("executed_qty", 0.0),
            "avg_price": order.get("avg_price", 0.0),
            "equity_snapshot": order.get("equity_snapshot"),
        }
        if position_size is not None:
            result["position_size"] = position_size
        if error:
            result["error"] = error
        return result

    # ---------- main flow ----------

    async def execute_on_accounts(
        self,
        *,
        account_ids: Any = None,
        tags: Any = None,
        symbol: str,
        side: str,
        entry: float,
        stop_loss: float,
        risk_pct: float,
        idempotency_key: str,
        order_type: str = "MARKET",
        actor: str = "mcp-agent",
    ) -> dict[str, Any]:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "idempotency_key is required (string)")
        # 3.20 M4: required parameters are validated before preflight—a missing
        # value must return INVALID_REQUEST instead of float(None) TypeError → UNKNOWN.
        symbol = self._require_string(symbol, "symbol")
        side = self._require_string(side, "side")
        entry = self._require_number(entry, "entry")
        stop_loss = self._require_number(stop_loss, "stop_loss")
        risk_pct = self._require_number(risk_pct, "risk_pct")
        order_type = normalize_order_type(order_type)
        targets = await self._resolve_targets(account_ids, tags)
        results = []
        for account in targets:
            lock = self._lock(account["account_id"])
            async with lock:
                try:
                    result = await self._execute_one_sized(
                        account,
                        symbol=symbol,
                        side=side,
                        entry=entry,
                        stop_loss=stop_loss,
                        risk_pct=risk_pct,
                        idempotency_key=idempotency_key.strip(),
                        order_type=order_type,
                        actor=actor or "mcp-agent",
                    )
                    results.append(result)
                except RasatError as exc:
                    results.append(
                        {
                            "account_id": account["account_id"],
                            "status": "REJECTED",
                            "symbol": symbol,
                            "error": {"code": exc.code, "message": exc.message},
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("account order failed: %s", account["account_id"])
                    results.append(
                        {
                            "account_id": account["account_id"],
                            "status": "UNKNOWN",
                            "symbol": symbol,
                            "error": {"code": ErrorCode.INTERNAL_ERROR, "message": str(exc)},
                        }
                    )
        succeeded = sum(1 for r in results if r.get("status") not in ("REJECTED", "UNKNOWN"))
        return {
            "results": results,
            "count": len(results),
            "succeeded": succeeded,
            "failed": len(results) - succeeded,
        }

    async def place_order(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None = None,
        stop_price: float | None = None,
        idempotency_key: str,
        actor: str = "mcp-agent",
    ) -> dict[str, Any]:
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id is required (string)")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "idempotency_key is required (string)")
        # 3.20 M4: missing/invalid required parameters return INVALID_REQUEST
        # instead of TypeError → UNKNOWN.
        symbol = self._require_string(symbol, "symbol")
        side = self._require_string(side, "side")
        quantity = self._require_number(quantity, "quantity")
        if price is not None:
            price = self._require_number(price, "price")
        if stop_price is not None:
            stop_price = self._require_number(stop_price, "stop_price")
        # T01: shared validator before broker/lock—enum + conditional fields + finite values.
        validated = validate_execution_order(
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
        )
        side = validated["side"]
        order_type = validated["order_type"]
        lock = self._lock(account_id.strip())
        async with lock:
            return await self._execute_one_direct(
                account_id.strip(),
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                stop_price=stop_price,
                idempotency_key=idempotency_key.strip(),
                actor=actor or "mcp-agent",
            )

    # ---------- OCO orders (2.21) ----------

    async def place_oco_order(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        stop_price: float,
        stop_limit_price: float,
        idempotency_key: str,
        actor: str = "mcp-agent",
    ) -> dict[str, Any]:
        """Spot OCO: profit target (LIMIT) + stop (STOP_LOSS_LIMIT) in one order list.

        When one fills, the other is automatically canceled by the exchange.
        Separate SL+TP orders for the same position competed for the balance;
        this call sends both as one `orderList`.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id is required (string)")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "idempotency_key is required (string)")
        symbol = self._require_string(symbol, "symbol")
        side = self._require_string(side, "side")
        quantity = self._require_number(quantity, "quantity")
        price = self._require_number(price, "price")
        stop_price = self._require_number(stop_price, "stop_price")
        stop_limit_price = self._require_number(stop_limit_price, "stop_limit_price")
        # T01: shared validator—OCO geometry/conditional fields are rejected
        # before reaching the service and account (no market/filters access needed).
        validated = validate_execution_order(
            side=side,
            order_type="OCO",
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            stop_limit_price=stop_limit_price,
        )
        side = validated["side"]
        lock = self._lock(account_id.strip())
        async with lock:
            return await self._execute_oco_direct(
                account_id.strip(),
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                stop_price=stop_price,
                stop_limit_price=stop_limit_price,
                idempotency_key=idempotency_key.strip(),
                actor=actor or "mcp-agent",
            )

    async def _execute_oco_direct(
        self,
        account_id: str,
        *,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        stop_price: float,
        stop_limit_price: float,
        idempotency_key: str,
        actor: str,
    ) -> dict:
        account = await self.accounts.get_account(account_id)
        existing = await self.db.read(lambda conn: self._load_order(conn, account_id, idempotency_key))
        if existing is not None:
            return await self._handle_existing(account, existing)

        is_real = await self._is_real(account)
        if not await self.market.symbol_valid(symbol):
            raise RasatError(ErrorCode.INVALID_SYMBOL, f"unknown symbol in universe: {symbol}")
        market_price = await self.market.price(symbol)
        check_price_fresh(FRESHNESS_FRESH if market_price is not None else None, symbol)
        filters = await self.market.filters(symbol)
        if filters is None:
            raise RasatError(ErrorCode.FILTER_VIOLATION, f"exchangeInfo filters unavailable: {symbol}")
        # T01: shared validator—direction/geometry + SymbolFilters before the broker.
        validated = validate_execution_order(
            side=side,
            order_type="OCO",
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            stop_limit_price=stop_limit_price,
            filters=filters,
            market_price=market_price,
        )
        side = validated["side"]
        notional = validated["notional"]

        # 3.8: free-balance gate—an OCO locks both orders on the exchange, so free balance must be sufficient.
        base_asset = filters.base_asset
        balances = await self.broker.get_balance(account_id=account_id)
        self._check_available_balance(
            balances=balances, side=(side or "BUY").upper(), symbol=symbol,
            base_asset=base_asset, quote_asset="USDT",
            quantity=quantity, notional=notional, fee=notional * self.default_fee_rate,
        )

        policy = await self.risk.get_policy(account_id)
        exposure_after = await self._current_exposure(account_id, "USDT") + notional
        await self._enforce_caps_or_override(account, policy, symbol, notional, exposure_after, idempotency_key)

        equity = await self._equity_from_balances(balances, "USDT")

        from ..data.order_broker import to_client_order_id

        client_order_id = to_client_order_id(idempotency_key)

        if not is_real:
            def _insert(conn: sqlite3.Connection) -> dict:
                order = self._insert_order(
                    conn,
                    account_id=account_id,
                    idempotency_key=idempotency_key,
                    symbol=symbol,
                    side=side,
                    order_type="OCO",
                    quantity=quantity,
                    price=price,
                    notional=notional,
                    reference_price=market_price,
                    equity_snapshot=equity,
                    status=STATUS_PAPER,
                    client_order_id=client_order_id,
                    stop_price=stop_price,
                    stop_limit_price=stop_limit_price,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor,
                        action="place_oco_paper",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": symbol,
                            "side": side,
                            "quantity": quantity,
                            "price": price,
                            "stop_price": stop_price,
                            "stop_limit_price": stop_limit_price,
                        },
                    )
                return order

            order = await self.db.write(_insert)
            return self._order_to_result(order, position_size=quantity)

        def _insert(conn: sqlite3.Connection) -> dict:
            return self._insert_order(
                conn,
                account_id=account_id,
                idempotency_key=idempotency_key,
                symbol=symbol,
                side=side,
                order_type="OCO",
                quantity=quantity,
                price=price,
                notional=notional,
                reference_price=market_price,
                equity_snapshot=equity,
                status="NEW",
                client_order_id=client_order_id,
                stop_price=stop_price,
                stop_limit_price=stop_limit_price,
            )

        order = await self.db.write(_insert)
        try:
            result = await self.broker.place_oco(
                account_id=account_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                price=price,
                stop_price=stop_price,
                stop_limit_price=stop_limit_price,
                client_order_id=client_order_id,
            )
        except (RasatError, asyncio.TimeoutError) as exc:
            if isinstance(exc, asyncio.TimeoutError):
                exc = RasatError(ErrorCode.TIMEOUT, "OCO submission timed out")
            if exc.code == ErrorCode.TIMEOUT:
                return await self._reconcile_after_timeout(
                    account, order, symbol, client_order_id, actor
                )
            status = "REJECTED" if exc.code == ErrorCode.ORDER_REJECTED else "UNKNOWN"

            def _fail(conn: sqlite3.Connection) -> None:
                self._update_order(
                    conn,
                    order["order_id"],
                    status=status,
                    error_code=exc.code,
                    error_message=exc.message,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor,
                        action="place_oco_failed",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": symbol,
                            "error_code": exc.code,
                        },
                    )

            await self.db.write(_fail)
            order = {**order, "status": status, "error_code": exc.code, "error_message": exc.message}
            return self._order_to_result(order, position_size=quantity)

        def _fill(conn: sqlite3.Connection) -> dict:
            self._update_order(
                conn,
                order["order_id"],
                status=result.status,
                exchange_order_id=result.exchange_order_id,
                executed_qty=result.executed_qty,
                avg_price=result.avg_price,
            )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor,
                    action="place_oco",
                    details={
                        "account_id": account_id,
                        "order_id": order["order_id"],
                        "symbol": symbol,
                        "side": side,
                        "quantity": quantity,
                        "price": price,
                        "stop_price": stop_price,
                        "stop_limit_price": stop_limit_price,
                    },
                )
            return order

        order = await self.db.write(_fill)
        return self._order_to_result(order, position_size=quantity)

    # ---------- individual orders ----------

    async def _is_real(self, account: dict) -> bool:
        """Check whether the account is in real mode and validate credentials (fail closed otherwise)."""
        if str(account.get("trading_lock", "paper")) != "real":
            return False
        await self.accounts.get_credentials(account["account_id"])
        return True

    async def _accuracy_preflight(self, symbol: str, side: str, entry: float, stop_loss: float) -> None:
        if not await self.market.symbol_valid(symbol):
            raise RasatError(ErrorCode.INVALID_SYMBOL, f"unknown symbol in universe: {symbol}")
        price = await self.market.price(symbol)
        check_price_fresh(FRESHNESS_FRESH if price is not None else None, symbol)
        check_stop_direction(side, entry, stop_loss)

    @staticmethod
    def _check_available_balance(
        *,
        balances: dict,
        side: str,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        quantity: float,
        notional: float,
        fee: float,
    ) -> None:
        """Check the FREE balance required by the spot order (3.8).

        - BUY  → free quote-asset (USDT) >= notional + fee
        - SELL → free base-asset >= quantity

        The broker's `free` balance is used instead of equity: for an account
        holding base assets, equity exceeds the available USDT, so an equity-based
        check cannot guarantee "insufficient balance → do not send" (review H1).
        Always raise `INSUFFICIENT_BALANCE` when the balance is insufficient.
        """
        side_norm = (side or "BUY").upper()
        if side_norm == "BUY":
            available = float(balances.get(quote_asset, 0) or 0)
            required = notional + fee
            if required > available:
                raise RasatError(
                    ErrorCode.INSUFFICIENT_BALANCE,
                    f"insufficient balance: BUY requires {required:.8f} {quote_asset} "
                    f"> free {available:.8f} ({symbol})",
                )
        elif side_norm == "SELL":
            available = float(balances.get(base_asset, 0) or 0)
            if quantity > available:
                raise RasatError(
                    ErrorCode.INSUFFICIENT_BALANCE,
                    f"insufficient balance: SELL requires {quantity:.8f} {base_asset} "
                    f"> free {available:.8f} ({symbol})",
                )
        else:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"invalid side: {side}")

    async def _execute_one_sized(
        self,
        account: dict,
        *,
        symbol: str,
        side: str,
        entry: float,
        stop_loss: float,
        risk_pct: float,
        idempotency_key: str,
        order_type: str,
        actor: str,
    ) -> dict:
        # 3.12: check idempotency BEFORE preflight. A retry returns the stored
        # result even if the market is stale or credentials have a problem
        # (same key = same result); preflight is not run again.
        existing = await self.db.read(lambda conn: self._load_order(conn, account["account_id"], idempotency_key))
        if existing is not None:
            return await self._handle_existing(account, existing)

        is_real = await self._is_real(account)
        await self._accuracy_preflight(symbol, side, entry, stop_loss)
        quote_asset = "USDT"
        price = await self.market.price(symbol)

        # 1) Daemon's own fresh balance snapshot + equity + exchange filters
        balances = await self.broker.get_balance(account_id=account["account_id"])
        equity = await self._equity_from_balances(balances, quote_asset)

        filters = await self.market.filters(symbol)
        if filters is None:
            raise RasatError(ErrorCode.FILTER_VIOLATION, f"exchangeInfo filters unavailable: {symbol}")

        # 3.8: side-aware sizing input—free USDT for BUY (not equity), equity for
        # SELL (the base gate is applied after sizing). A zero balance returns
        # INSUFFICIENT_BALANCE, not INVALID_REQUEST.
        side_norm = (side or "BUY").upper()
        if side_norm == "BUY":
            available_quote = float(balances.get(quote_asset, 0) or 0)
            if available_quote <= 0:
                raise RasatError(
                    ErrorCode.INSUFFICIENT_BALANCE,
                    f"insufficient balance: free {quote_asset} is 0 ({symbol})",
                )
            sizing_balance = available_quote
        else:
            sizing_balance = equity
            if sizing_balance <= 0:
                raise RasatError(
                    ErrorCode.INSUFFICIENT_BALANCE,
                    f"insufficient balance: account balance is 0 ({symbol})",
                )

        # 2) Position sizing (compatible with exchange filters, fee-aware)
        sized = calculate_position_size(
            symbol=symbol,
            account_balance=sizing_balance,
            risk_pct=risk_pct,
            entry=entry,
            stop_loss=stop_loss,
            filters=filters,
            side=side,
            fee_rate=self.default_fee_rate,
        )
        quantity = sized["quantity"]
        notional = quantity * price

        # T01: shared validator—enum/finite values + SymbolFilters. For
        # LIMIT/STOP_LOSS_LIMIT, price is entry; for STOP_LOSS_LIMIT, stop_price
        # is stop_loss. Otherwise STOP_LOSS_LIMIT would always be rejected with
        # "price required".
        validated = validate_execution_order(
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=(entry if order_type in ("LIMIT", "STOP_LOSS_LIMIT") else None),
            stop_price=(stop_loss if order_type == "STOP_LOSS_LIMIT" else None),
            filters=filters,
            market_price=price,
        )
        side = validated["side"]
        order_type = validated["order_type"]

        # 3) Free-balance gate (3.8): BUY → free USDT, SELL → free base.
        #    If the market price is above entry, sizing's entry-based cap may be
        #    insufficient; this gate rejects using final notional/fee before sending.
        self._check_available_balance(
            balances=balances, side=side_norm, symbol=symbol,
            base_asset=filters.base_asset, quote_asset=quote_asset,
            quantity=quantity, notional=notional, fee=notional * self.default_fee_rate,
        )

        # 4) Risk-policy caps (strict when there is no override)
        policy = await self.risk.get_policy(account["account_id"])
        exposure_after = await self._current_exposure(account["account_id"], quote_asset) + notional
        await self._enforce_caps_or_override(
            account, policy, symbol, notional, exposure_after, idempotency_key
        )

        # 5) Send the order—STOP_LOSS_LIMIT also passes stop_price to the
        #    chokepoint validator (otherwise _place_and_record would again say
        #    "stop_price is required").
        return await self._place_and_record(
            account, is_real, symbol, side, order_type, quantity, entry, notional, price,
            idempotency_key, equity, actor,
            stop_price=(stop_loss if order_type == "STOP_LOSS_LIMIT" else None),
        )

    async def _execute_one_direct(
        self,
        account_id: str,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        idempotency_key: str,
        actor: str,
        stop_price: float | None = None,
    ) -> dict:
        account = await self.accounts.get_account(account_id)

        # 3.12: check idempotency before preflight—a retry returns the stored
        # result even when the market is stale (same key = same result).
        existing = await self.db.read(lambda conn: self._load_order(conn, account_id, idempotency_key))
        if existing is not None:
            return await self._handle_existing(account, existing)

        is_real = await self._is_real(account)
        if not await self.market.symbol_valid(symbol):
            raise RasatError(ErrorCode.INVALID_SYMBOL, f"unknown symbol in universe: {symbol}")
        market_price = await self.market.price(symbol)
        check_price_fresh(FRESHNESS_FRESH if market_price is not None else None, symbol)
        # T01: LIMIT/STOP no longer fall back to the market price—an explicit price is required.
        # The shared validator rejects enum/conditional-field/SymbolFilters errors before the broker.
        filters = await self.market.filters(symbol)
        if filters is None:
            raise RasatError(ErrorCode.FILTER_VIOLATION, f"exchangeInfo filters unavailable: {symbol}")
        validated = validate_execution_order(
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            filters=filters,
            market_price=market_price,
        )
        side = validated["side"]
        order_type = validated["order_type"]
        price = validated["price"]
        stop_price = validated["stop_price"]
        quantity = validated["quantity"]
        notional = validated["notional"]

        # 3.8: free-balance gate (BUY → USDT, SELL → base)—not equity.
        base_asset = filters.base_asset
        balances = await self.broker.get_balance(account_id=account_id)
        self._check_available_balance(
            balances=balances, side=(side or "BUY").upper(), symbol=symbol,
            base_asset=base_asset, quote_asset="USDT",
            quantity=quantity, notional=notional, fee=notional * self.default_fee_rate,
        )

        policy = await self.risk.get_policy(account_id)
        exposure_after = await self._current_exposure(account_id, "USDT") + notional
        await self._enforce_caps_or_override(account, policy, symbol, notional, exposure_after, idempotency_key)

        equity = await self._equity_from_balances(balances, "USDT")
        return await self._place_and_record(
            account, is_real, symbol, side, order_type, quantity, price, notional, market_price,
            idempotency_key, equity, actor, stop_price=stop_price,
        )

    async def _enforce_caps_or_override(
        self,
        account: dict,
        policy: dict,
        symbol: str,
        notional: float,
        exposure_after: float,
        idempotency_key: str,
    ) -> None:
        """Enforce caps; try a one-time override if a cap would be exceeded.

        An override bypasses only user-defined policy caps—basic correctness checks
        (3.3) are outside this function and always run first.
        """
        from ..risk import enforce_policy_caps

        try:
            enforce_policy_caps(
                symbol=symbol,
                notional=notional,
                policy=policy,
                aggregate_exposure=exposure_after,
            )
            return
        except RasatError as exc:
            if exc.code not in (ErrorCode.RISK_LIMIT_EXCEEDED, ErrorCode.SYMBOL_NOT_ALLOWED):
                raise
            override = await self.risk.consume_override(
                account["account_id"],
                policy_version=policy["policy_version"],
                consumed_by_idem=idempotency_key,
            )
            if override is None:
                raise exc
            logger.info(
                "override consumed (account=%s, policy_version=%s): %s",
                account["account_id"],
                policy["policy_version"],
                idempotency_key,
            )

    async def _place_and_record(
        self,
        account: dict,
        is_real: bool,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        notional: float,
        reference_price: float,
        idempotency_key: str,
        equity_snapshot: float,
        actor: str,
        stop_price: float | None = None,
    ) -> dict:
        account_id = account["account_id"]
        from ..data.order_broker import to_client_order_id

        # T01: defense in depth at the single broker chokepoint—enum/conditional/finite.
        validate_execution_order(
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            market_price=reference_price,
        )
        client_order_id = to_client_order_id(idempotency_key)

        if not is_real:
            # PAPER account: simulated order; it does not go to the broker.
            def _insert(conn: sqlite3.Connection) -> dict:
                order = self._insert_order(
                    conn,
                    account_id=account_id,
                    idempotency_key=idempotency_key,
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    notional=notional,
                    reference_price=reference_price,
                    equity_snapshot=equity_snapshot,
                    status=STATUS_PAPER,
                    client_order_id=client_order_id,
                    stop_price=stop_price,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor,
                        action="place_order_paper",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": symbol,
                            "side": side,
                            "quantity": quantity,
                            "notional": notional,
                            "stop_price": stop_price,
                        },
                    )
                return order

            order = await self.db.write(_insert)
            return self._order_to_result(order, position_size=quantity)

        # REAL account
        # Create the record first (trace after a crash), then call the broker.
        def _insert(conn: sqlite3.Connection) -> dict:
            return self._insert_order(
                conn,
                account_id=account_id,
                idempotency_key=idempotency_key,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                notional=notional,
                reference_price=reference_price,
                equity_snapshot=equity_snapshot,
                status="NEW",
                client_order_id=client_order_id,
                stop_price=stop_price,
            )

        order = await self.db.write(_insert)
        try:
            result = await self.broker.place_order(
                account_id=account_id,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                client_order_id=client_order_id,
                stop_price=stop_price,
            )
        except (RasatError, asyncio.TimeoutError) as exc:
            if isinstance(exc, asyncio.TimeoutError):
                exc = RasatError(ErrorCode.TIMEOUT, "order submission timed out")
            if exc.code == ErrorCode.TIMEOUT:
                # Reconcile-before-retry: query Binance for the real state.
                return await self._reconcile_after_timeout(
                    account, order, symbol, client_order_id, actor
                )
            status = "REJECTED" if exc.code == ErrorCode.ORDER_REJECTED else "UNKNOWN"

            def _fail(conn: sqlite3.Connection) -> None:
                self._update_order(
                    conn,
                    order["order_id"],
                    status=status,
                    error_code=exc.code,
                    error_message=exc.message,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor,
                        action="place_order_failed",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": symbol,
                            "error_code": exc.code,
                        },
                    )

            await self.db.write(_fail)
            order = {**order, "status": status, "error_code": exc.code, "error_message": exc.message}
            return self._order_to_result(order, position_size=quantity)

        def _fill(conn: sqlite3.Connection) -> dict:
            self._update_order(
                conn,
                order["order_id"],
                status=result.status,
                exchange_order_id=result.exchange_order_id,
                executed_qty=result.executed_qty,
                avg_price=result.avg_price,
            )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor,
                    action="place_order",
                    details={
                        "account_id": account_id,
                        "order_id": order["order_id"],
                        "symbol": symbol,
                        "side": side,
                        "quantity": quantity,
                        "status": result.status,
                        "exchange_order_id": result.exchange_order_id,
                    },
                )
            return {**order, "status": result.status, "exchange_order_id": result.exchange_order_id,
                    "executed_qty": result.executed_qty, "avg_price": result.avg_price}

        order = await self.db.write(_fill)
        return self._order_to_result(order, position_size=quantity)

    async def _reconcile_after_timeout(
        self,
        account: dict,
        order: dict,
        symbol: str,
        client_order_id: str,
        actor: str,
    ) -> dict:
        account_id = account["account_id"]
        try:
            if order["order_type"] == "OCO":
                found = await self.broker.query_oco(
                    account_id=account_id, list_client_order_id=client_order_id
                )
            else:
                found = await self.broker.query_order(
                    account_id=account_id, symbol=symbol, client_order_id=client_order_id
                )
        except RasatError:
            found = None
        if found is not None:
            def _apply(conn: sqlite3.Connection) -> None:
                self._update_order(
                    conn,
                    order["order_id"],
                    status=found.status,
                    exchange_order_id=found.exchange_order_id,
                    executed_qty=found.executed_qty,
                    avg_price=found.avg_price,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor,
                        action="order_reconciled",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": symbol,
                            "status": found.status,
                            "exchange_order_id": found.exchange_order_id,
                        },
                    )

            await self.db.write(_apply)
            return self._order_to_result({**order, "status": found.status,
                                          "exchange_order_id": found.exchange_order_id,
                                          "executed_qty": found.executed_qty,
                                          "avg_price": found.avg_price})
        # Not found → state is uncertain; UNKNOWN, with no blind retry.
        def _unknown(conn: sqlite3.Connection) -> None:
            self._update_order(conn, order["order_id"], status="UNKNOWN", error_code=ErrorCode.ORDER_UNKNOWN,
                               error_message="network timeout; order state could not be verified")
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor,
                    action="order_unknown",
                    details={"account_id": account_id, "order_id": order["order_id"], "symbol": symbol},
                )

        await self.db.write(_unknown)
        return self._order_to_result(
            {**order, "status": "UNKNOWN", "error_code": ErrorCode.ORDER_UNKNOWN,
             "error_message": "network timeout; order state could not be verified"},
            error={"code": ErrorCode.ORDER_UNKNOWN, "message": "network timeout; order state could not be verified"},
        )

    async def _handle_existing(self, account: dict, existing: dict) -> dict:
        """Retry with the same idempotency_key: does not create a duplicate order.

        - For a terminal state, return the stored result.
        - For an open/NEW state, reconcile (query the real state) and return it.
        """
        # 3.17: UNKNOWN is queried again—it may have actually become FILLED after
        # the timeout; do not blindly return the stored UNKNOWN.
        if existing["status"] not in ("NEW", "PARTIALLY_FILLED", "UNKNOWN"):
            return self._order_to_result(existing)

        if str(account.get("trading_lock", "paper")) != "real":
            # paper account: do not repeat the simulation; return the stored result
            return self._order_to_result(existing)

        try:
            # OCO legs carry Binance's own clientOrderIds; individual
            # `query_order` always returns -2013. Reconcile the OCO record through
            # `query_oco` using listClientOrderId (see `_reconcile_after_timeout`).
            if existing["order_type"] == "OCO":
                found = await self.broker.query_oco(
                    account_id=account["account_id"],
                    list_client_order_id=existing["client_order_id"],
                )
            else:
                found = await self.broker.query_order(
                    account_id=account["account_id"],
                    symbol=existing["symbol"],
                    client_order_id=existing["client_order_id"],
                )
        except RasatError:
            found = None
        if found is None:
            return self._order_to_result(existing)

        def _apply(conn: sqlite3.Connection) -> None:
            self._update_order(
                conn,
                existing["order_id"],
                status=found.status,
                exchange_order_id=found.exchange_order_id,
                executed_qty=found.executed_qty,
                avg_price=found.avg_price,
            )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor="system",
                    action="order_reconciled",
                    details={
                        "account_id": account["account_id"],
                        "order_id": existing["order_id"],
                        "status": found.status,
                    },
                )

        await self.db.write(_apply)
        return self._order_to_result(
            {**existing, "status": found.status, "exchange_order_id": found.exchange_order_id,
             "executed_qty": found.executed_qty, "avg_price": found.avg_price}
        )

    # ---------- startup reconcile (3.13) ----------

    async def reconcile_open_orders(self) -> dict[str, Any]:
        """Verify orders stuck in NEW/PARTIALLY_FILLED/UNKNOWN at daemon startup.

        If the process crashes between writing NEW to the DB and calling the
        broker, an orphan NEW record remains and inflates exposure
        (`_current_exposure`/`_open_order_notional`) forever. Each record is
        brought to its real state with Binance `query_order`; records that cannot
        be verified on the exchange become `UNKNOWN` (no blind resubmission—the
        reconcile-before-retry contract). OCO lists use `query_oco`; individual
        orders use `query_order`.

        UNKNOWN records are also scanned: they may have become FILLED after a
        timeout, so they are queried again at startup and moved to a terminal state.
        """
        account_ids = [
            acc["account_id"]
            for acc in (await self.accounts.list_accounts())["accounts"]
            if str(acc.get("trading_lock", "paper")) == "real"
        ]
        if not account_ids:
            return {"scanned": 0, "reconciled": 0, "unchanged": 0, "errors": []}

        def _stuck(conn: sqlite3.Connection) -> list[dict]:
            marks = ",".join("?" for _ in account_ids)
            rows = conn.execute(
                "SELECT " + ", ".join(_ORDER_COLUMNS)
                + f" FROM orders WHERE account_id IN ({marks}) AND status IN ('NEW', 'PARTIALLY_FILLED', 'UNKNOWN')",
                tuple(account_ids),
            ).fetchall()
            return [dict(r) for r in rows]

        stuck = await self.db.read(_stuck)
        reconciled = 0
        unchanged = 0
        errors: list[dict] = []
        for order in stuck:
            try:
                if order["order_type"] == "OCO":
                    found = await self.broker.query_oco(
                        account_id=order["account_id"],
                        list_client_order_id=order["client_order_id"],
                    )
                else:
                    found = await self.broker.query_order(
                        account_id=order["account_id"],
                        symbol=order["symbol"],
                        client_order_id=order["client_order_id"],
                    )
            except RasatError as exc:
                errors.append(
                    {"order_id": order["order_id"], "symbol": order["symbol"],
                     "error": {"code": exc.code, "message": exc.message}}
                )
                continue
            if found is None or found.status == "UNKNOWN":
                def _unknown(conn: sqlite3.Connection, oid: str = order["order_id"]) -> None:
                    self._update_order(
                        conn, oid, status="UNKNOWN", error_code=ErrorCode.ORDER_UNKNOWN,
                        error_message="startup reconcile: order could not be verified on Binance",
                    )
                    if self.audit is not None:
                        self.audit.append_in_connection(
                            conn, actor="system", action="order_reconciled_unknown",
                            details={"account_id": order["account_id"], "order_id": order["order_id"],
                                     "symbol": order["symbol"]},
                        )

                await self.db.write(_unknown)
                reconciled += 1
                continue
            # 3.18: "unchanged" means all mutable fields match, not only status.
            # executed_qty/avg_price may advance under the same status (for example,
            # PARTIALLY_FILLED); sync the real fill fields even when status matches.
            same = (
                found.status == order["status"]
                and found.exchange_order_id == order["exchange_order_id"]
                and found.executed_qty == order["executed_qty"]
                and found.avg_price == order["avg_price"]
            )
            if same:
                unchanged += 1
                continue

            def _apply(conn: sqlite3.Connection, oid: str = order["order_id"], f: Any = found) -> None:
                self._update_order(
                    conn, oid, status=f.status, exchange_order_id=f.exchange_order_id,
                    executed_qty=f.executed_qty, avg_price=f.avg_price,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn, actor="system", action="order_reconciled",
                        details={"account_id": order["account_id"], "order_id": order["order_id"],
                                 "symbol": order["symbol"], "status": f.status,
                                 "executed_qty": f.executed_qty, "avg_price": f.avg_price},
                    )

            await self.db.write(_apply)
            reconciled += 1

        return {"scanned": len(stuck), "reconciled": reconciled, "unchanged": unchanged, "errors": errors}

    # ---------- kill switch: close_all_positions (3.5) ----------

    async def close_all_positions(self, *, account_id: str, actor: str = "mcp-agent") -> dict[str, Any]:
        """Cancel open orders and sell base-asset balances at market price.

        - If `account_id == "all"`, use all accounts; otherwise use that account.
        - Spot long-only: "close position" means selling held base-asset balances.
        - Partial success: return a separate result for each account and clearly
          report which accounts did or did not close.
        - For paper accounts, do not query or sell real balances; cancel only local
          open orders and return `closed=False, simulated=True`.
        - 3.14: each `_close_one` call creates its own close-run nonce; the idem
          key combines `account+symbol+qty+run`. This creates a new SELL even for
          an equal-quantity rebuy (previously FILLED dedup skipped it) and avoids
          UNIQUE constraint failures after REJECTED/CANCELED retries.
        - Reconcile-before-resend: if an in-flight/unknown close order exists,
          query its real broker state first; NEVER perform a blind duplicate sell.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id is required (string|'all')")
        account_id = account_id.strip()

        if account_id == "all":
            accounts = (await self.accounts.list_accounts())["accounts"]
        else:
            accounts = [await self.accounts.get_account(account_id)]

        results = []
        for account in accounts:
            lock = self._lock(account["account_id"])
            async with lock:
                try:
                    result = await self._close_one(account, actor=actor or "mcp-agent")
                    results.append(result)
                except RasatError as exc:
                    results.append(
                        {
                            "account_id": account["account_id"],
                            "closed": False,
                            "error": {"code": exc.code, "message": exc.message},
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("position close failed: %s", account["account_id"])
                    results.append(
                        {
                            "account_id": account["account_id"],
                            "closed": False,
                            "error": {
                                "code": ErrorCode.INTERNAL_ERROR,
                                "message": "position close failed",
                            },
                        }
                    )

        closed = sum(1 for r in results if r.get("closed"))
        return {"results": results, "count": len(results), "closed": closed, "failed": len(results) - closed}

    async def _close_one(self, account: dict, actor: str) -> dict:
        from ..data.order_broker import to_client_order_id

        account_id = account["account_id"]
        is_real = await self._is_real(account)
        cancelled: list[str] = []
        cancel_errors: list[dict] = []
        sold: list[dict] = []

        # 1) Cancel open orders (T01): for real accounts, query ALL open exchange
        #    orders—including orphan orders absent from the local DB—and sync local
        #    rows with broker results. If cancellation is uncertain, use UNKNOWN
        #    and closed=False; audit every transition.
        def _open_orders(conn: sqlite3.Connection) -> list[dict]:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT " + ", ".join(_ORDER_COLUMNS)
                    + " FROM orders WHERE account_id = ? AND status IN ('NEW', 'PARTIALLY_FILLED')",
                    (account_id,),
                ).fetchall()
            ]

        open_orders = await self.db.read(_open_orders)
        local_by_cid = {o["client_order_id"]: o for o in open_orders}

        exchange_open: list[dict] = []
        exchange_by_cid: dict[str, dict] = {}
        if is_real:
            exchange_open = await self.broker.get_all_open_orders(account_id=account_id) or []
            exchange_by_cid = {
                str(o["client_order_id"]): o for o in exchange_open if o.get("client_order_id")
            }

        async def _query_safe(symbol: str, cid: str, is_oco: bool = False):
            try:
                if is_oco:
                    return await self.broker.query_oco(
                        account_id=account_id, list_client_order_id=cid
                    )
                return await self.broker.query_order(
                    account_id=account_id, symbol=symbol, client_order_id=cid
                )
            except asyncio.TimeoutError:
                return RasatError(ErrorCode.TIMEOUT, "cancel state query timed out")
            except RasatError:
                return None

        def _write_cancel(order: dict):
            def _apply(conn: sqlite3.Connection, oid: str = order["order_id"]) -> None:
                self._update_order(conn, oid, status="CANCELED")
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn, actor=actor, action="order_canceled",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": order["symbol"],
                            "client_order_id": order["client_order_id"],
                        },
                    )

            return _apply

        def _write_unknown(order: dict, exc: RasatError):
            def _apply(conn: sqlite3.Connection, oid: str = order["order_id"], cerr: RasatError = exc) -> None:
                self._update_order(
                    conn, oid, status="UNKNOWN",
                    error_code=cerr.code, error_message=cerr.message,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn, actor=actor, action="order_cancel_failed",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": order["symbol"],
                            "error_code": cerr.code,
                        },
                    )

            return _apply

        def _write_sync(order: dict, found: Any):
            def _apply(conn: sqlite3.Connection, oid: str = order["order_id"], f: Any = found) -> None:
                self._update_order(
                    conn, oid, status=f.status, exchange_order_id=f.exchange_order_id,
                    executed_qty=f.executed_qty, avg_price=f.avg_price,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn, actor=actor, action="order_reconciled",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": order["symbol"],
                            "status": f.status,
                        },
                    )

            return _apply

        async def _cancel_one(cid: str, symbol: str, order: dict | None) -> None:
            """Try to cancel one client_order_id and sync a local row when present.

            For OCO rows, use `cancel_oco` with `listClientOrderId`
            (individual `cancel_order` cannot find legs using Binance's own cids);
            verify cancellation afterward with `query_oco`.
            """
            is_oco = order is not None and order.get("order_type") == "OCO"
            try:
                if is_oco:
                    res = await self.broker.cancel_oco(
                        account_id=account_id, symbol=symbol, list_client_order_id=cid
                    )
                else:
                    res = await self.broker.cancel_order(
                        account_id=account_id, symbol=symbol, client_order_id=cid
                    )
            except (RasatError, asyncio.TimeoutError) as exc:
                # 3.9: cancellation failed → do not silently mark CANCELED. The
                # real state is unknown; set UNKNOWN and report it explicitly.
                if isinstance(exc, asyncio.TimeoutError):
                    exc = RasatError(ErrorCode.TIMEOUT, "cancel request timed out")
                if order is not None:
                    await self.db.write(_write_unknown(order, exc))
                cancel_errors.append(
                    {
                        "symbol": symbol,
                        "order_id": order["order_id"] if order else None,
                        "client_order_id": cid,
                        "error": {"code": exc.code, "message": exc.message},
                    }
                )
                return
            if res is None:
                # -2011: not on the exchange (may be canceled/filled) → query the real state.
                found = await _query_safe(symbol, cid, is_oco=is_oco)
                if isinstance(found, RasatError):
                    if order is not None:
                        await self.db.write(_write_unknown(order, found))
                    cancel_errors.append(
                        {
                            "symbol": symbol,
                            "order_id": order["order_id"] if order else None,
                            "client_order_id": cid,
                            "error": {"code": found.code, "message": found.message},
                        }
                    )
                    return
                if found is None:
                    if order is not None:
                        await self.db.write(_write_cancel(order))
                    if symbol not in cancelled:
                        cancelled.append(symbol)
                elif found.status in ("NEW", "PARTIALLY_FILLED", "UNKNOWN"):
                    unknown = RasatError(ErrorCode.ORDER_UNKNOWN, "cancel state could not be verified")
                    if order is not None:
                        await self.db.write(_write_unknown(order, unknown))
                    cancel_errors.append(
                        {
                            "symbol": symbol,
                            "order_id": order["order_id"] if order else None,
                            "client_order_id": cid,
                            "error": {"code": ErrorCode.ORDER_UNKNOWN, "message": unknown.message},
                        }
                    )
                else:
                    if order is not None:
                        await self.db.write(_write_sync(order, found))
                    if found.status == "CANCELED" and symbol not in cancelled:
                        cancelled.append(symbol)
                return
            if res.status == "CANCELED":
                if order is not None:
                    await self.db.write(_write_cancel(order))
                if symbol not in cancelled:
                    cancelled.append(symbol)
                return
            if res.status in ("NEW", "PARTIALLY_FILLED", "UNKNOWN"):
                unknown = RasatError(ErrorCode.ORDER_RECONCILE_REQUIRED, "cancel result is not terminal")
                if order is not None:
                    await self.db.write(_write_unknown(order, unknown))
                cancel_errors.append(
                    {
                        "symbol": symbol,
                        "order_id": order["order_id"] if order else None,
                        "client_order_id": cid,
                        "error": {"code": unknown.code, "message": unknown.message},
                    }
                )
                return
            # Other terminal states (for example, FILLED) → no open order remains; sync locally.
            if order is not None:
                await self.db.write(_write_sync(order, res))

        # Cancellation targets = local open orders ∪ exchange open orders
        targets: dict[str, tuple[str, dict | None]] = {}
        for cid, order in local_by_cid.items():
            targets[cid] = (order["symbol"], order)
        for cid, ex in exchange_by_cid.items():
            if cid not in targets:
                targets[cid] = (str(ex.get("symbol") or ""), None)
        # Exchange orders without client_order_id → bulk cancellation by symbol
        for ex in exchange_open:
            if not ex.get("client_order_id"):
                symbol = str(ex.get("symbol") or "")
                if not symbol:
                    continue
                try:
                    await self.broker.cancel_all_open_orders(account_id=account_id, symbol=symbol)
                    if self.audit is not None:
                        await self.db.write(
                            lambda conn, ex_order=ex, sym=symbol: self.audit.append_in_connection(
                                conn,
                                actor=actor,
                                action="order_canceled",
                                details={
                                    "account_id": account_id,
                                    "order_id": ex_order.get("order_id"),
                                    "symbol": sym,
                                    "client_order_id": None,
                                    "scope": "exchange_open_order_without_client_id",
                                },
                            )
                        )
                    if symbol not in cancelled:
                        cancelled.append(symbol)
                except (RasatError, asyncio.TimeoutError) as exc:
                    if isinstance(exc, asyncio.TimeoutError):
                        exc = RasatError(ErrorCode.TIMEOUT, "bulk cancellation request timed out")
                    if self.audit is not None:
                        await self.db.write(
                            lambda conn, ex_order=ex, sym=symbol, cerr=exc: self.audit.append_in_connection(
                                conn,
                                actor=actor,
                                action="order_cancel_failed",
                                details={
                                    "account_id": account_id,
                                    "order_id": ex_order.get("order_id"),
                                    "symbol": sym,
                                    "error_code": cerr.code,
                                    "scope": "exchange_open_order_without_client_id",
                                },
                            )
                        )
                    cancel_errors.append(
                        {
                            "symbol": symbol,
                            "order_id": ex.get("order_id"),
                            "error": {"code": exc.code, "message": exc.message},
                        }
                    )

        for cid, (symbol, order) in targets.items():
            if not symbol:
                continue
            if is_real:
                await _cancel_one(cid, symbol, order)
            elif order is not None:
                await self.db.write(_write_cancel(order))
                if symbol not in cancelled:
                    cancelled.append(symbol)

        # 2) Sell base-asset balances
        if not is_real:
            # Because paper accounts do not read exchange balances, do not claim
            # that the position actually closed. Local open orders may have been
            # canceled; callers can distinguish this using `simulated` and
            # `position_close_supported`.
            return {
                "account_id": account_id,
                "closed": False,
                "mode": "paper",
                "simulated": True,
                "position_close_supported": False,
                "reason": "only local open orders were canceled for the paper account; real balances were not sold",
                "cancelled": cancelled,
                "cancel_errors": cancel_errors,
                "sold": sold,
            }

        # 3.14: each close run has its own nonce—even the same (account, symbol, qty)
        # on a rebuy gets a new idem key → new SELL; REJECTED retries avoid UNIQUE conflicts.
        run_nonce = uuid.uuid4().hex[:12]

        balances = await self.broker.get_balance(account_id=account_id)
        quote_asset = "USDT"
        for asset, free in balances.items():
            if asset == quote_asset or free <= 0:
                continue
            symbol = f"{asset}{quote_asset}"
            if not await self.market.symbol_valid(symbol):
                continue
            price = await self.market.price(symbol)
            if price is None:
                sold.append({"symbol": symbol, "skipped": "stale price"})
                continue
            filters = await self.market.filters(symbol)
            if filters is None:
                sold.append({"symbol": symbol, "skipped": "no filters"})
                continue
            from ..position_sizing import round_down_to_step

            qty = round_down_to_step(float(free), filters.step_size)
            if qty < filters.min_qty:
                sold.append({"symbol": symbol, "skipped": "below min_qty"})
                continue

            # 3.14: reconcile-before-resend—if this symbol has an in-flight/unknown
            # close SELL, query its real broker state first; do not blind-sell twice.
            # For terminal REJECTED/CANCELED, retry with a new run.
            open_close = await self.db.read(lambda conn: self._load_open_close(conn, account_id, symbol))
            if open_close is not None:
                try:
                    found = await self.broker.query_order(
                        account_id=account_id, symbol=symbol,
                        client_order_id=open_close["client_order_id"],
                    )
                except RasatError:
                    found = None
                if found is not None:
                    def _sync(conn: sqlite3.Connection, oid: str = open_close["order_id"], f: Any = found) -> None:
                        self._update_order(
                            conn, oid, status=f.status, exchange_order_id=f.exchange_order_id,
                            executed_qty=f.executed_qty, avg_price=f.avg_price,
                        )

                    await self.db.write(_sync)
                    if found.status in ("NEW", "PARTIALLY_FILLED", "UNKNOWN", "FILLED"):
                        # still in-flight/unknown/filled → do not send a new SELL
                        sold.append({"symbol": symbol, "quantity": open_close["quantity"],
                                     "status": found.status, "order_id": open_close["order_id"]})
                        continue
                    # REJECTED/CANCELED/EXPIRED → previous attempt failed; new SELL

            idem = f"close-{account_id}-{symbol}-{qty}-{run_nonce}"
            result = await self._place_and_record(
                account, True, symbol, "SELL", "MARKET", qty, None, qty * price, price,
                idem, 0.0, actor,
            )
            sold.append({"symbol": symbol, "quantity": result["quantity"],
                         "status": result["status"], "order_id": result["order_id"]})

        # 3.19: closed is True only when everything was actually sold (FILLED) and
        # there were no cancellation errors. UNKNOWN/REJECTED/NEW/skipped sales
        # may mean the position is still open.
        sold_ok = all(s.get("status") == "FILLED" for s in sold)
        return {"account_id": account_id, "closed": (not cancel_errors) and sold_ok, "mode": "real",
                "cancelled": cancelled, "cancel_errors": cancel_errors, "sold": sold}

    # ---------- exposure + audit (3.5) ----------

    async def _exposure_by_symbol(self, account_id: str) -> dict[str, float]:
        """Account exposure by symbol: open-order notional + base balance value."""

        def _open(conn: sqlite3.Connection) -> dict[str, float]:
            # 3.17: Include UNKNOWN in exposure conservatively (it may be filled in reality).
            rows = conn.execute(
                "SELECT symbol, SUM(notional) AS n FROM orders "
                "WHERE account_id = ? AND status IN ('NEW', 'PARTIALLY_FILLED', 'UNKNOWN') GROUP BY symbol",
                (account_id,),
            ).fetchall()
            return {str(r["symbol"]): float(r["n"]) for r in rows}

        by_symbol = await self.db.read(_open)
        balances = await self.broker.get_balance(account_id=account_id)
        for asset, free in balances.items():
            if asset == "USDT":
                continue
            if free <= 0:
                continue
            symbol = f"{asset}USDT"
            price = await self.market.price(symbol)
            if price is None:
                raise RasatError(ErrorCode.STALE_DATA, f"could not obtain exposure price: {symbol}")
            by_symbol[symbol] = by_symbol.get(symbol, 0.0) + free * price
        return by_symbol

    async def get_total_exposure(self) -> dict[str, Any]:
        """Total exposure across all accounts: a symbol-level risk view.

        T01: account errors are not silently swallowed. If an account balance/price
        query fails, `{account_id, error}` is included in `errors` and
        `complete=false` is returned; incomplete exposure is never presented as a full total.
        """
        accounts = (await self.accounts.list_accounts())["accounts"]
        by_symbol: dict[str, float] = {}
        per_account: dict[str, float] = {}
        errors: list[dict] = []
        for account in accounts:
            try:
                per = await self._exposure_by_symbol(account["account_id"])
            except RasatError as exc:
                errors.append(
                    {
                        "account_id": account["account_id"],
                        "error": {"code": exc.code, "message": exc.message},
                    }
                )
                continue
            except Exception as exc:  # noqa: BLE001
                logger.warning("exposure calculation failed: %s", account["account_id"], exc_info=True)
                errors.append(
                    {
                        "account_id": account["account_id"],
                        "error": {"code": ErrorCode.INTERNAL_ERROR, "message": "could not calculate account exposure"},
                    }
                )
                continue
            per_account[account["account_id"]] = sum(per.values())
            for symbol, value in per.items():
                by_symbol[symbol] = by_symbol.get(symbol, 0.0) + value
        ordered = dict(sorted(by_symbol.items(), key=lambda kv: -kv[1]))
        return {
            "total": sum(by_symbol.values()),
            "by_symbol": ordered,
            "per_account": per_account,
            "account_count": len(accounts),
            "complete": not errors,
            "errors": errors,
        }

    async def get_account_balance(self, *, account_id: str) -> dict[str, Any]:
        """Full account balance view: free + locked + holdings value + total (3.21).

        The previous behavior returned only free balance and did not include
        amounts locked in open orders or the value of held base assets; therefore
        total account value appeared incomplete when the account had an open
        position/order. It now returns `free`/`locked`/`holdings_value`/`total`
        separately; `total` = free + locked + holdings_value.
        """
        account_id = self._require_string(account_id, "account_id")
        account = await self.accounts.get_account(account_id)
        if not account["credentials_configured"]:
            # A Binance balance query is not available for public/read-only accounts.
            raise RasatError(
                ErrorCode.ACCOUNT_NO_CREDENTIALS,
                "an authenticated account with credentials is required to query balances",
            )
        return await self._account_balance_breakdown(account_id, "USDT")

    async def get_open_orders(self, *, account_id: str) -> dict[str, Any]:
        """Return real open orders on the exchange (Binance)—different from the
        MCP's approval queue (`get_pending_orders`) or audit log: these are the
        orders actually waiting on the exchange (the source of locked balance)."""
        account_id = self._require_string(account_id, "account_id")
        account = await self.accounts.get_account(account_id)
        if not account["credentials_configured"]:
            raise RasatError(
                ErrorCode.ACCOUNT_NO_CREDENTIALS,
                "an authenticated account with credentials is required to query open orders",
            )
        raw_orders = await self.broker.get_all_open_orders(account_id=account_id) or []
        orders = []
        for o in raw_orders:
            raw = o.get("raw") or {}
            orders.append(
                {
                    "symbol": o.get("symbol"),
                    "order_id": o.get("order_id"),
                    "client_order_id": o.get("client_order_id"),
                    "order_list_id": str(raw.get("orderListId"))
                    if raw.get("orderListId") not in (None, -1)
                    else None,
                    "side": o.get("side"),
                    "type": raw.get("type"),
                    "status": raw.get("status"),
                    "price": float(raw["price"]) if raw.get("price") not in (None, "") else None,
                    "stop_price": float(raw["stopPrice"])
                    if raw.get("stopPrice") not in (None, "", "0.00000000")
                    else None,
                    "quantity": o.get("quantity"),
                    "time": raw.get("time"),
                }
            )
        return {"orders": orders, "count": len(orders)}

    async def find_unprotected_positions(self) -> dict[str, Any]:
        """Find base-asset balances above dust with no open SELL order on real accounts.

        Unlike ``get_open_orders``, scan all real accounts and automatically
        cross-reference balances with open orders—answering "which positions are
        unprotected" in one call.

        Account access errors are not silently swallowed: if an account balance or
        open-order query fails, `{account_id, error}` is included in `errors` and
        `complete=false` is returned. The failed account is excluded without
        presenting a misleading complete table that says there are "no
        unprotected positions" (see `get_total_exposure`).
        """
        accounts = [
            acc
            for acc in (await self.accounts.list_accounts())["accounts"]
            if str(acc.get("trading_lock", "paper")) == "real" and acc.get("credentials_configured")
        ]
        unprotected: list[dict[str, Any]] = []
        errors: list[dict] = []
        for acc in accounts:
            account_id = acc["account_id"]
            try:
                balance_detail = await self.broker.get_balance_detail(account_id=account_id)
                open_orders = await self.broker.get_all_open_orders(account_id=account_id) or []
            except RasatError as exc:
                errors.append(
                    {
                        "account_id": account_id,
                        "error": {"code": exc.code, "message": exc.message},
                    }
                )
                continue
            except Exception as exc:  # noqa: BLE001
                logger.warning("unprotected position scan failed: %s", account_id, exc_info=True)
                errors.append(
                    {
                        "account_id": account_id,
                        "error": {"code": ErrorCode.INTERNAL_ERROR, "message": "could not scan account"},
                    }
                )
                continue
            sell_symbols = {
                o.get("symbol") for o in open_orders if str(o.get("side", "")).upper() == "SELL"
            }
            for asset, bal in balance_detail.items():
                if asset == "USDT":
                    continue
                qty = float(bal.get("free", 0) or 0) + float(bal.get("locked", 0) or 0)
                if qty <= 0:
                    continue
                symbol = f"{asset}USDT"
                price = await self.market.price(symbol)
                if price is None:
                    continue
                value = qty * price
                if value < self.MIN_UNPROTECTED_VALUE_USD:
                    continue
                if symbol in sell_symbols:
                    continue
                unprotected.append(
                    {
                        "account_id": account_id,
                        "account_label": acc.get("label"),
                        "symbol": symbol,
                        "quantity": qty,
                        "value_usd": value,
                    }
                )
        return {
            "unprotected": unprotected,
            "count": len(unprotected),
            "account_count": len(accounts),
            "complete": not errors,
            "errors": errors,
        }

    async def get_audit_log(self, *, limit: int = 50) -> dict[str, Any]:
        """Query the hash-chain-verified audit log (3.5)."""
        if self.audit is None:
            raise RasatError(ErrorCode.NOT_IMPLEMENTED, "audit log is unavailable in this context")
        if not isinstance(limit, int) or limit < 1 or limit > 500:
            raise RasatError(ErrorCode.INVALID_REQUEST, "limit must be between 1 and 500")
        broken = await self.audit.verify()
        tail = await self.audit.tail(limit)
        return {
            "verified": len(broken) == 0,
            "broken": broken,
            "tail": tail,
            "count": len(tail),
        }
