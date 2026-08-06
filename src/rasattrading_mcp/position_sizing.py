"""Pozisyon boyutlandırma + Binance exchangeInfo filtreleri (ticket 3.3).

`calculate_position_size` risk-bazlı, fee-aware boyutlandırır ve Binance spot
filtrelerine (`LOT_SIZE`, `MIN_NOTIONAL`, `PRICE_FILTER`) göre yuvarlar — borsa
filtrelerine uymayan emir gönderilmez (3.4 bu çıktıyı kullanır).

Cap'lere tolerans UYGULANMAZ (3.2 tolerans ilkesi): `quantity` her zaman
`max_qty` altında ve `min_qty`/`min_notional`'ı karşılayacak şekilde aşağı
yuvarlanır; karşılanamıyorsa `FILTER_VIOLATION` ile fail-closed reddedilir.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from .accuracy import check_stop_direction, check_sufficient_balance
from .errors import ErrorCode, RasatError

DEFAULT_FEE_RATE = 0.001  # %0.1 (Binance spot default)

_EPS = 1e-9


@dataclass(frozen=True)
class SymbolFilters:
    """Bir sembolün exchangeInfo filter özeti (sayısal)."""

    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    step_size: float
    min_qty: float
    max_qty: float
    min_notional: float
    tick_size: float
    min_price: float
    max_price: float

    @classmethod
    def from_exchange_info(cls, entry: dict) -> "SymbolFilters":
        filters = {f.get("filterType"): f for f in entry.get("filters", [])}

        def _num(fname: str, key: str, default: float = 0.0) -> float:
            f = filters.get(fname) or {}
            raw = f.get(key)
            if raw is None or raw == "":
                return default
            return float(raw)

        return cls(
            symbol=str(entry.get("symbol", "")),
            base_asset=str(entry.get("baseAsset", "")),
            quote_asset=str(entry.get("quoteAsset", "")),
            status=str(entry.get("status", "")),
            step_size=_num("LOT_SIZE", "stepSize"),
            min_qty=_num("LOT_SIZE", "minQty"),
            max_qty=_num("LOT_SIZE", "maxQty", default=math.inf),
            min_notional=_num("MIN_NOTIONAL", "minNotional"),
            tick_size=_num("PRICE_FILTER", "tickSize"),
            min_price=_num("PRICE_FILTER", "minPrice"),
            max_price=_num("PRICE_FILTER", "maxPrice", default=math.inf),
        )

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "status": self.status,
            "filters": {
                "LOT_SIZE": {
                    "step_size": self.step_size,
                    "min_qty": self.min_qty,
                    "max_qty": None if math.isinf(self.max_qty) else self.max_qty,
                },
                "MIN_NOTIONAL": {"min_notional": self.min_notional},
                "PRICE_FILTER": {
                    "tick_size": self.tick_size,
                    "min_price": self.min_price,
                    "max_price": None if math.isinf(self.max_price) else self.max_price,
                },
            },
        }


def round_down_to_step(value: float, step_size: float) -> float:
    """`value`'yu `step_size`'ın tam katına AŞAĞI yuvarlar (LOT_SIZE)."""
    if step_size <= 0:
        return value
    return math.floor(value / step_size + _EPS) * step_size


def calculate_position_size(
    *,
    symbol: str,
    account_balance: float,
    risk_pct: float,
    entry: float,
    stop_loss: float,
    filters: SymbolFilters,
    side: str = "BUY",
    fee_rate: float = DEFAULT_FEE_RATE,
) -> dict[str, Any]:
    """Risk-bazlı, fee-aware boyut hesaplar; filtre uyumsuzluğunda fail-closed.

    Mantık:
    1. Doğruluk: stop yönü (long: stop < entry) her zaman kontrol edilir.
    2. `risk_amount = account_balance * risk_pct`; `risk_per_unit = |entry - stop|`.
    3. `raw_qty = risk_amount / risk_per_unit` (base asset).
    4. Fee düşüldükten sonra bakiye sınırı: `qty <= balance / (entry*(1+fee_rate))`.
    5. `LOT_SIZE.stepSize`'a aşağı yuvarlanır; `max_qty` aşılmaz.
    6. `min_qty` / `min_notional` (entry fiyatıyla) karşılanamıyorsa `FILTER_VIOLATION`.
    7. `PRICE_FILTER` (entry `min_price..max_price` içinde) doğrulanır.
    """
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_REQUEST, "symbol zorunlu (string)")
    check_stop_direction(side, entry, stop_loss)
    if risk_pct <= 0 or risk_pct > 1:
        raise RasatError(ErrorCode.INVALID_REQUEST, "risk_pct (0,1] aralığında olmalı")
    if account_balance <= 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, "account_balance sıfırdan büyük olmalı")
    if entry <= 0 or stop_loss <= 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, "entry ve stop_loss sıfırdan büyük olmalı")
    if fee_rate < 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, "fee_rate negatif olamaz")

    if entry < filters.min_price or entry > filters.max_price:
        raise RasatError(
            ErrorCode.FILTER_VIOLATION,
            f"entry fiyatı PRICE_FILTER dışında: {entry} ([{filters.min_price}, {filters.max_price}])",
        )

    risk_amount = account_balance * risk_pct
    risk_per_unit = abs(entry - stop_loss)
    raw_qty = risk_amount / risk_per_unit

    # Fee-aware bakiye sınırı: qty*entry*(1+fee_rate) <= account_balance
    balance_cap_qty = account_balance / (entry * (1 + fee_rate))
    qty = min(raw_qty, balance_cap_qty)
    qty = round_down_to_step(qty, filters.step_size)

    notional = qty * entry
    fee = notional * fee_rate

    if qty <= 0 or qty < filters.min_qty:
        raise RasatError(
            ErrorCode.FILTER_VIOLATION,
            f"miktar LOT_SIZE minQty altında: {qty} < {filters.min_qty} ({symbol})",
        )
    if qty > filters.max_qty:
        raise RasatError(
            ErrorCode.FILTER_VIOLATION,
            f"miktar LOT_SIZE maxQty üstünde: {qty} > {filters.max_qty} ({symbol})",
        )
    if notional < filters.min_notional:
        raise RasatError(
            ErrorCode.FILTER_VIOLATION,
            f"tutar MIN_NOTIONAL altında: {notional} < {filters.min_notional} ({symbol})",
        )

    # Temel doğruluk: nihai gereken tutar (notional + fee) bakiyeyi aşamaz.
    check_sufficient_balance(account_balance, notional + fee, symbol)

    return {
        "symbol": symbol,
        "side": (side or "BUY").upper(),
        "account_balance": account_balance,
        "risk_pct": risk_pct,
        "risk_amount": risk_amount,
        "risk_per_unit": risk_per_unit,
        "entry": entry,
        "stop_loss": stop_loss,
        "quantity": qty,
        "notional": notional,
        "estimated_fee": fee,
        "total_required": notional + fee,
        "fee_rate": fee_rate,
        "filters": filters.to_dict(),
    }
