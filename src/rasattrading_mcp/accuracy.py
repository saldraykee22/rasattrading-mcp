"""Core correctness checks (ticket 3.3).

These checks are independent of the risk policy and overrides, and are mandatory
and non-bypassable for every order. They guarantee that no garbage orders are sent:
- insufficient balance
- price freshness
- stop-direction logic (stop < entry for long positions)
- symbol validity / TRADING status

They are pure functions and receive their data from the caller (pipeline/universe/handler).
3.4 runs these checks before sending an order; overrides never bypass them.
"""

from __future__ import annotations

from .envelope import FRESHNESS_FRESH
from .errors import ErrorCode, RasatError


def check_symbol_valid(symbol: str, universe: set[str] | list[str] | None) -> None:
    """Check whether the symbol is in the universe (status=TRADING and USDT universe)."""
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_SYMBOL, "symbol is required (string)")
    if universe is None:
        raise RasatError(ErrorCode.INVALID_SYMBOL, f"symbol universe is unavailable — could not validate {symbol}")
    if symbol not in universe:
        raise RasatError(ErrorCode.INVALID_SYMBOL, f"unknown or non-TRADING symbol in universe: {symbol}")


def check_price_fresh(freshness: str | None, symbol: str) -> None:
    """Reject the order if the price data is not `fresh` (do not trade on stale data)."""
    if freshness != FRESHNESS_FRESH:
        raise RasatError(ErrorCode.STALE_DATA, f"price data is stale — order rejected: {symbol}")


def check_stop_direction(side: str, entry: float, stop_loss: float) -> None:
    """Stop-direction logic: stop < entry for long positions and stop > entry for short positions."""
    normalized = (side or "BUY").upper()
    if normalized == "BUY":
        if stop_loss >= entry:
            raise RasatError(ErrorCode.INVALID_REQUEST, "stop_loss must be below entry for a long position")
    elif normalized == "SELL":
        if stop_loss <= entry:
            raise RasatError(ErrorCode.INVALID_REQUEST, "stop_loss must be above entry for a short position")
    else:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"invalid side: {side}")


def check_sufficient_balance(available_quote: float, required_quote: float, symbol: str) -> None:
    """Reject the spot order if its amount (notional plus estimated fee) exceeds the balance."""
    if required_quote > available_quote:
        raise RasatError(
            ErrorCode.INSUFFICIENT_BALANCE,
            f"insufficient balance: required {required_quote:.8f} > available {available_quote:.8f} ({symbol})",
        )
