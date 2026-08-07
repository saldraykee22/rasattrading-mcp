"""Ortak execution validator (T01).

`place_order` / `place_oco_order` / `execute_on_accounts` broker çağrısından
ÖNCE bu validator'ı kullanır; JSON schema'ya güvenilmez. Bu katman canonical
`INVALID_REQUEST` veya `FILTER_VIOLATION` üretir ve broker asla çağrılmaz.

Kontroller tek yerde:
- sonlu sayılar (NaN/Infinity reddi);
- side/order_type allowlist;
- MARKET/LIMIT/STOP_LOSS_LIMIT/OCO koşullu alanları;
- stop yönü (market'e göre) ve OCO fiyat geometrisi;
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
        raise RasatError(ErrorCode.INVALID_REQUEST, "side zorunlu (BUY|SELL)")
    side_n = side.strip().upper()
    if side_n not in SIDES:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"geçersiz side: {side!r} (BUY|SELL)")
    return side_n


def normalize_order_type(order_type) -> str:
    if not isinstance(order_type, str) or not order_type.strip():
        raise RasatError(ErrorCode.INVALID_REQUEST, "order_type zorunlu")
    ot = order_type.strip().upper()
    if ot not in ORDER_TYPES:
        raise RasatError(
            ErrorCode.INVALID_REQUEST,
            f"geçersiz order_type: {order_type!r} ({'/'.join(ORDER_TYPES)})",
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
                f"{filters.symbol} {field} geçersiz (sonlu değil)",
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
    """Ortak doğrulama; normalize edilmiş alanları döner.

    Returns:
        {side, order_type, quantity, price, stop_price, stop_limit_price, notional}

    `notional` = quantity * price; price yoksa (MARKET) quantity * market_price.
    `filters`/`market_price` verilmediğinde o kontroller atlanır (pre-validation
    çağrıları için); tam yol her ikisini de geçirir.
    """
    side_n = normalize_side(side)
    ot = normalize_order_type(order_type)

    qty = require_positive(quantity, "quantity")
    price = _require_positive_optional(price, "price")
    stop_price = _require_positive_optional(stop_price, "stop_price")
    stop_limit_price = _require_positive_optional(stop_limit_price, "stop_limit_price")
    market_price = _require_positive_optional(market_price, "market_price")

    # Koşullu alanlar
    if ot == "LIMIT":
        if price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "LIMIT emirde price zorunlu")
    elif ot == "STOP_LOSS_LIMIT":
        if price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "STOP_LOSS_LIMIT emirde price zorunlu")
        if stop_price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "STOP_LOSS_LIMIT emirde stop_price zorunlu")
        if market_price is not None:
            if side_n == "SELL" and stop_price >= market_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "SELL stop emrinde stop_price piyasa fiyatının altında olmalı",
                )
            if side_n == "BUY" and stop_price <= market_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "BUY stop emrinde stop_price piyasa fiyatının üstünde olmalı",
                )
    elif ot == "OCO":
        if price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "OCO emirde price (kâr hedefi) zorunlu")
        if stop_price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "OCO emirde stop_price zorunlu")
        if stop_limit_price is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, "OCO emirde stop_limit_price zorunlu")
        if side_n == "SELL":
            if stop_limit_price >= stop_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "SELL OCO'da stop_limit_price stop_price'dan düşük olmalı",
                )
            if price <= stop_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "SELL OCO'da price (kâr hedefi) stop_price'ın üstünde olmalı",
                )
        else:
            if stop_limit_price <= stop_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "BUY OCO'da stop_limit_price stop_price'dan yüksek olmalı",
                )
            if price >= stop_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "BUY OCO'da price (kâr hedefi) stop_price'ın altında olmalı",
                )
        if market_price is not None:
            if side_n == "SELL" and stop_price >= market_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "SELL OCO'da stop_price giriş fiyatının altında olmalı",
                )
            if side_n == "BUY" and stop_price <= market_price:
                raise RasatError(
                    ErrorCode.INVALID_REQUEST,
                    "BUY OCO'da stop_price giriş fiyatının üstünde olmalı",
                )

    reference = price if price is not None else market_price
    notional = qty * reference if reference is not None else None

    if filters is not None:
        _check_filters_finite(filters)
        if qty < filters.min_qty:
            raise RasatError(
                ErrorCode.FILTER_VIOLATION,
                f"miktar LOT_SIZE minQty altında: {qty} < {filters.min_qty} ({filters.symbol})",
            )
        if qty > filters.max_qty:
            raise RasatError(
                ErrorCode.FILTER_VIOLATION,
                f"miktar LOT_SIZE maxQty üstünde: {qty} > {filters.max_qty} ({filters.symbol})",
            )
        if filters.step_size and filters.step_size > 0:
            steps = qty / filters.step_size
            if abs(steps - round(steps)) > _ALIGN_EPS:
                raise RasatError(
                    ErrorCode.FILTER_VIOLATION,
                    f"miktar LOT_SIZE stepSize katı değil: {qty} (step {filters.step_size}, {filters.symbol})",
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
                    f"{pname} PRICE_FILTER dışında: {pval} ([{filters.min_price}, {filters.max_price}])",
                )
            if filters.tick_size and filters.tick_size > 0:
                ticks = (pval - filters.min_price) / filters.tick_size
                if abs(ticks - round(ticks)) > _ALIGN_EPS:
                    raise RasatError(
                        ErrorCode.FILTER_VIOLATION,
                        f"{pname} PRICE_FILTER tickSize katı değil: {pval} (tick {filters.tick_size})",
                    )
        if notional is None:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{ot} emir için fiyat/referans fiyatı gerekli")
        if notional < filters.min_notional:
            raise RasatError(
                ErrorCode.FILTER_VIOLATION,
                f"tutar MIN_NOTIONAL altında: {notional} < {filters.min_notional} ({filters.symbol})",
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
