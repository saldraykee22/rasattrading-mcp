"""Finite-number validation helpers (T01).

Python's JSON decoder accepts non-standard NaN/Infinity; internal Python callers
can also pass `float("nan")`. Because `NaN` bypasses every comparison (both NaN > x
and NaN <= x are False), numbers on the risk/order path must be rejected with the
canonical INVALID_REQUEST before reaching the broker or cap comparison.
"""

from __future__ import annotations

import math

from .errors import ErrorCode, RasatError


def require_finite(value, name: str) -> float:
    """Require `value` to be a finite float; otherwise raise INVALID_REQUEST."""
    if value is None or isinstance(value, bool):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} is required (number)")
    try:
        num = float(value)
    except (TypeError, ValueError):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} must be a number")
    if not math.isfinite(num):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} must be a finite number")
    return num


def require_positive(value, name: str) -> float:
    """Require a finite number greater than zero (INVALID_REQUEST)."""
    num = require_finite(value, name)
    if num <= 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} must be greater than zero")
    return num
