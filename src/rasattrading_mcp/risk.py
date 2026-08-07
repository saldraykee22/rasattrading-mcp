"""Risk politikası yardımcıları (ticket 3.2).

Tolerans ilkesi (kullanıcı kararı):
- Yalnızca RR/stop-mesafesi gibi "yumuşak" sinyal eşikleri tolerans bandıyla
  değerlendirilir (`within_tolerance`). Bu, katı eşitlik yüzünden iyi bir setup'ın
  reddedilmesini önler.
- `max_notional_per_order` / `max_aggregate_exposure` güvenlik tavanlarına asla
  pozitif tolerans uygulanmaz (`enforce_policy_caps`): nihai değer her zaman
  `<= cap` olmalıdır. Cap üzerine esneklik ancak açık, audit'lenen bir override
  (3.2 `risk_override`) ile yapılır.

Temel doğruluk kontrolleri (bakiye/stale/stop-yönü/sembol-geçerliliği) bu modülün
KAPSAMI DIŞINDADIR — 3.3 ticket'ına aittir ve override'dan bağımsız olarak her
emirde zorunlu kalır.
"""

from __future__ import annotations

import logging

from .errors import ErrorCode, RasatError
from .numeric import require_finite

logger = logging.getLogger("rasattrading.risk")

#: Soft eşikler için varsayılan tolerans bandı (RR/stop-mesafesi; cap değil).
DEFAULT_TOLERANCE_PCT = 0.02


def within_tolerance(value: float, target: float, tolerance_pct: float = DEFAULT_TOLERANCE_PCT) -> bool:
    """`value` hedefe tolerans bandı içinde yakınsa True.

    Sadece sinyal kalitesi eşikleri için kullanılmalıdır. `tolerance_pct`
    görecelidir (target'ın yüzdesi). Örnek: RR hedef 2.0, tolerans %2
    -> 1.96 ve üzeri kabul.
    """
    if tolerance_pct is None or tolerance_pct < 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, "tolerance_pct negatif olamaz")
    value = require_finite(value, "value")
    target = require_finite(target, "target")
    tolerance_pct = require_finite(tolerance_pct, "tolerance_pct")
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
    """Kullanıcı-tanımlı risk politikasını KATI biçimde uygular.

    `notional` (yuvarlama sonrası nihai emir tutarı) ve isteğe bağlı
    `aggregate_exposure` cap'leri aşıldığında `RISK_LIMIT_EXCEEDED` fırlatır.
    Sembol `allowed_symbols`'da değilse `SYMBOL_NOT_ALLOWED`. Tolerans YOK:
    nihai değer her zaman `<= cap` olmalıdır.

    `policy` bir dict'tir: {max_notional_per_order?, max_aggregate_exposure?,
    allowed_symbols?}. `None`/boş = sınırsız/serbest.
    """
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_REQUEST, "symbol zorunlu (string)")
    notional = require_finite(notional, "notional")
    if notional <= 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, "notional pozitif bir sayı olmalı")

    allowed = policy.get("allowed_symbols")
    if allowed:
        if symbol not in allowed:
            raise RasatError(ErrorCode.SYMBOL_NOT_ALLOWED, f"sembol risk politikasında yok: {symbol}")

    cap = policy.get("max_notional_per_order")
    if cap is not None:
        cap = require_finite(cap, "max_notional_per_order")
        if notional > cap:
            raise RasatError(
                ErrorCode.RISK_LIMIT_EXCEEDED,
                f"emir tutarı cap'i aşıyor: {notional} > {cap} (max_notional_per_order, tolerans uygulanmaz)",
            )

    if aggregate_exposure is not None:
        aggregate_exposure = require_finite(aggregate_exposure, "aggregate_exposure")
        agg_cap = policy.get("max_aggregate_exposure")
        if agg_cap is not None:
            agg_cap = require_finite(agg_cap, "max_aggregate_exposure")
            if aggregate_exposure > agg_cap:
                raise RasatError(
                    ErrorCode.RISK_LIMIT_EXCEEDED,
                    f"toplam exposure cap'i aşıyor: {aggregate_exposure} > {agg_cap} (max_aggregate_exposure, tolerans uygulanmaz)",
                )
