"""Sonlu sayı doğrulama yardımcıları (T01).

Python'un JSON decoder'ı non-standard NaN/Infinity kabul eder; ayrıca dahili
Python caller'ları `float("nan")` geçebilir. `NaN` her karşılaştırmayı bypass
ettiği için (NaN > x ve NaN <= x ikisi de False) risk/emir yolundaki sayılar
broker'a veya cap karşılaştırmasına girmeden canonical INVALID_REQUEST ile
kesilmelidir.
"""

from __future__ import annotations

import math

from .errors import ErrorCode, RasatError


def require_finite(value, name: str) -> float:
    """`value` sonlu bir float olmalı; değilse INVALID_REQUEST fırlatır."""
    if value is None or isinstance(value, bool):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} zorunlu (sayı)")
    try:
        num = float(value)
    except (TypeError, ValueError):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} sayı olmalı")
    if not math.isfinite(num):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} sonlu (finite) bir sayı olmalı")
    return num


def require_positive(value, name: str) -> float:
    """Sonlu ve sıfırdan büyük sayı zorunludur (INVALID_REQUEST)."""
    num = require_finite(value, name)
    if num <= 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} sıfırdan büyük olmalı")
    return num
