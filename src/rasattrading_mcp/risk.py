"""Risk-policy helpers (ticket 3.2).

Tolerance principle (user decision):
- Use a tolerance band (`within_tolerance`) only for "soft" signal thresholds
  such as RR/stop distance. This prevents a good setup from being rejected due
  to strict equality.
- Never apply positive tolerance to the safety ceilings
  `max_notional_per_order` / `max_aggregate_exposure` (`enforce_policy_caps`):
  the final value must always be `<= cap`. Flexibility above a cap requires an
  explicit, audited override (3.2 `risk_override`).

Core correctness checks (balance/stale/stop direction/symbol validity) are OUTSIDE
this module's SCOPE — they belong to ticket 3.3 and remain mandatory for every
order independently of overrides.
"""

from __future__ import annotations

import logging

from .errors import ErrorCode, RasatError
from .numeric import require_finite

logger = logging.getLogger("rasattrading.risk")

#: Default tolerance band for soft thresholds (RR/stop distance; not caps).
DEFAULT_TOLERANCE_PCT = 0.02


def within_tolerance(value: float, target: float, tolerance_pct: float = DEFAULT_TOLERANCE_PCT) -> bool:
    """Return True when `value` is within the tolerance band of the target.

    Use only for signal-quality thresholds. `tolerance_pct` is relative (a
    percentage of target). Example: RR target 2.0 with 2% tolerance accepts 1.96+.
    """
    # T01: finite-validate non-numeric/None tolerance before comparison;
    # otherwise values such as "abc" could escape as TypeError.
    value = require_finite(value, "value")
    target = require_finite(target, "target")
    tolerance_pct = require_finite(tolerance_pct, "tolerance_pct")
    if tolerance_pct < 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, "tolerance_pct cannot be negative")
    if target == 0:
        return value == 0
    band = abs(target) * tolerance_pct
    return abs(value - target) <= band


def enforce_policy_caps(
    *,
    symbol: str,
    notional: float,
    policy: dict,
    aggregate_exposure: float | None = None,
) -> None:
    """Apply the user-defined risk policy STRICTLY.

    Raise `RISK_LIMIT_EXCEEDED` when `notional` (final order amount after rounding)
    or optional `aggregate_exposure` exceeds a cap. Raise `SYMBOL_NOT_ALLOWED` when
    the symbol is not in `allowed_symbols`. NO tolerance: the final value must always be `<= cap`.

    `policy` is a dict: {max_notional_per_order?, max_aggregate_exposure?,
    allowed_symbols?}. `None`/empty = unlimited/free.
    """
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_REQUEST, "symbol is required (string)")
    notional = require_finite(notional, "notional")
    if notional <= 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, "notional must be a positive number")

    allowed = policy.get("allowed_symbols")
    if allowed:
        if symbol not in allowed:
            raise RasatError(ErrorCode.SYMBOL_NOT_ALLOWED, f"symbol is not allowed by risk policy: {symbol}")

    cap = policy.get("max_notional_per_order")
    if cap is not None:
        cap = require_finite(cap, "max_notional_per_order")
        if notional > cap:
            raise RasatError(
                ErrorCode.RISK_LIMIT_EXCEEDED,
                f"order notional exceeds cap: {notional} > {cap} (max_notional_per_order, no tolerance)",
            )

    if aggregate_exposure is not None:
        aggregate_exposure = require_finite(aggregate_exposure, "aggregate_exposure")
        agg_cap = policy.get("max_aggregate_exposure")
        if agg_cap is not None:
            agg_cap = require_finite(agg_cap, "max_aggregate_exposure")
            if aggregate_exposure > agg_cap:
                raise RasatError(
                    ErrorCode.RISK_LIMIT_EXCEEDED,
                    f"aggregate exposure exceeds cap: {aggregate_exposure} > {agg_cap} (max_aggregate_exposure, no tolerance)",
                )
