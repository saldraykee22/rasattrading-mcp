"""Shared execution validator (T01).

`place_order` / `place_oco_order` / `execute_on_accounts` use this validator
BEFORE calling the broker; the JSON schema is not trusted. This layer produces
canonical `INVALID_REQUEST` or `FILTER_VIOLATION`, and the broker is never called.

All checks are centralized here:
- finite numbers (NaN/Infinity rejected);
- side/order_type allowlist;
- MARKET/LIMIT/STOP_LOSS_LIMIT/OCO conditional fields;
- stop direction (relative to the market) and OCO price geometry;
- SymbolFilters: min/max quantity, step size, min notional, price tick/min/max.
"""

from __future__ import annotations

import math

from ..errors import ErrorCode, RasatError
from ..numeric import require_positive
from ..position_sizing import SymbolFilters

SIDES = ("BUY", "SELL")
ORDER_TYPES = ("MARKET", "LIMIT", "STOP_LOSS_LIMIT", "OCO")

_ALIGN_EPS = 1e-6


def normalize_side(side) -> str:
    if not isinstance(side, str) or not side.strip():
        raise RasatError(ErrorCode.INVALID_REQUEST, "side is required (BUY|SELL)")
    side_n = side.strip().upper()
    if side_n not in SIDES:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"invalid side: {side!r} (BUY|SELL)")
    return side_n


def normalize_order_type(order_type) -> str:
    if not isinstance(order_type, str) or not order_type.strip():
        raise RasatError(ErrorCode.INVALID_REQUEST, "order_type is required")
    ot = order_type.strip().upper()
    if ot not in ORDER_TYPES:
        raise RasatError(
            ErrorCode.INVALID_REQUEST,
            f"invalid order_type: {order_type!r} ({'/'.join(ORDER_TYPES)})",
        )
    return ot


def _require_positive_optional(value, name: str) -> float | None:
    if value is None:
        return None
    return require_positive(value, name)


def _check_filters_finite(filters: SymbolFilters) -> None:
    for field in (
        "step_size",
        "min_qty",
        "max_qty",
        "min_notional",
        "tick_size",
        "min_price",
        "max_price",
    ):
        value = getattr(filters, field)
        if value is None or not math.isfinite(float(value)):
            raise RasatError(
                ErrorCode.FILTER_VIOLATION,
                f"{filters.symbol} {field} is invalid (not finite)",
            )


def validate_execution_order(
    *,
    side,
    order_type,
    quantity,
    price: float | None = None,
    stop_price: float | None = None,
    stop_limit_price: float | None = None,
    filters: SymbolFilters | None = None,
    market_price: float | None = None,
) -> dict:
    """Shared validation; returns normalized fields.

    Returns:
        {side, order_type, quantity, price, stop_price, stop_limit_price, notional}

    `notional` = quantity * price; when price is absent (MARKET), quantity * market_price.
    When `filters`/`market_price` are absent, those checks are skipped (for
    pre-validation calls); the full path passes both.
    """
    side_n = normalize_side(side)
    ot = normalize_order_type(order_type)

    qty = require_positive(quantity, "quantity")
    price = _require_positive_optional(price, "price")
    stop_price = _require_positive_optional(stop_price, "stop_price")
    stop_limit_price = _require_positive_optional(stop_limit_price, "stop_limit_price")
    market_price = _require_positive_optional(market_price, "market_price")

    # Conditional fields
    if ot == "LIMIT":
        if price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "price is required for LIMIT orders")
    elif ot == "STOP_LOSS_LIMIT":
        if price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "price is required for STOP_LOSS_LIMIT orders")
        if stop_price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "stop_price is required for STOP_LOSS_LIMIT orders")
        if market_price is not None:
            if side_n == "SELL" and stop_price >= market_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "SELL stop_price must be below the market price",
                )
            if side_n == "BUY" and stop_price <= market_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "BUY stop_price must be above the market price",
                )
    elif ot == "OCO":
        if price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "price (profit target) is required for OCO orders")
        if stop_price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "stop_price is required for OCO orders")
        if stop_limit_price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "stop_limit_price is required for OCO orders")
        if side_n == "SELL":
            if stop_limit_price >= stop_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "SELL OCO stop_limit_price must be below stop_price",
                )
            if price <= stop_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "SELL OCO price (profit target) must be above stop_price",
                )
        else:
            if stop_limit_price <= stop_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "BUY OCO stop_limit_price must be above stop_price",
                )
            if price >= stop_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "BUY OCO price (profit target) must be below stop_price",
                )
        if market_price is not None:
            if side_n == "SELL" and stop_price >= market_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "SELL OCO stop_price must be below the entry price",
                )
            if side_n == "BUY" and stop_price <= market_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "BUY OCO stop_price must be above the entry price",
                )

    reference = price if price is not None else market_price
    notional = qty * reference if reference is not None else None

    if filters is not None:
        _check_filters_finite(filters)
        if qty < filters.min_qty:
            raise RasatError(
                ErrorCode.FILTER_VIOLATION,
                f"quantity is below LOT_SIZE minQty: {qty} < {filters.min_qty} ({filters.symbol})",
            )
        if qty > filters.max_qty:
            raise RasatError(
                ErrorCode.FILTER_VIOLATION,
                f"quantity is above LOT_SIZE maxQty: {qty} > {filters.max_qty} ({filters.symbol})",
            )
        if filters.step_size and filters.step_size > 0:
            steps = qty / filters.step_size
            if abs(steps - round(steps)) > _ALIGN_EPS:
                raise RasatError(
                    ErrorCode.FILTER_VIOLATION,
                    f"quantity is not a multiple of LOT_SIZE stepSize: {qty} (step {filters.step_size}, {filters.symbol})",
                )
        for pname, pval in (
            ("price", price),
            ("stop_price", stop_price),
            ("stop_limit_price", stop_limit_price),
        ):
            if pval is None:
                continue
            if pval < filters.min_price or pval > filters.max_price:
                raise RasatError(
                    ErrorCode.FILTER_VIOLATION,
                    f"{pname} is outside PRICE_FILTER: {pval} ([{filters.min_price}, {filters.max_price}])",
                )
            if filters.tick_size and filters.tick_size > 0:
                ticks = (pval - filters.min_price) / filters.tick_size
                if abs(ticks - round(ticks)) > _ALIGN_EPS:
                    raise RasatError(
                        ErrorCode.FILTER_VIOLATION,
                        f"{pname} is not a multiple of PRICE_FILTER tickSize: {pval} (tick {filters.tick_size})",
                    )
        if notional is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"price/reference price is required for {ot} orders")
        if notional < filters.min_notional:
            raise RasatError(
                ErrorCode.FILTER_VIOLATION,
                f"notional is below MIN_NOTIONAL: {notional} < {filters.min_notional} ({filters.symbol})",
            )

    return {
        "side": side_n,
        "order_type": ot,
        "quantity": qty,
        "price": price,
        "stop_price": stop_price,
        "stop_limit_price": stop_limit_price,
        "notional": notional,
    }
