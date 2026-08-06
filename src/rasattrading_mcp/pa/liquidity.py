"""2.2 — Likidite bölgeleri + futures context (saf + DB okuma).

Kurallar (sürüm `liquidity-v1`):
- **Equal highs/lows:** Aynı türden swing seviyeleri, fiyat farkı `tolerans`
  (varsayılan `EQUAL_LEVEL_TOLERANCE_PCT`) içindeyse aynı likidite bölgesinde
  kümeleşir. En az 2 swing gereklidir; tek seviye bölge üretmez.
- **Sweep/mitigasyon:** Bölge oluştuktan (`formed_at`) sonra fiyat bölge
  bandını aşarsa (equal_highs: high > band.high; equal_lows: low < band.low)
  o seviyedeki likidite alınmış sayılır → `mitigated=true`. `tested` = fiyat
  banda değdi ama aşmadı.
- **Futures context (salt-okunur):** `futures_context` tablosundaki funding
  rate / OI / liquidation yalnızca `fresh` ise likidite skoruna girer.
  `stale`/`unknown` girişler skora dahil EDİLMEZ ama çıktıda açıkça
  `included: false` + durum olarak görünür (sessizce 0 sayılmaz).
"""

from __future__ import annotations

import logging
from typing import Any

from ..storage.db import Database
from .params import (
    EQUAL_LEVEL_TOLERANCE_PCT,
    LIQUIDITY_ALGO_VERSION,
    LIQUIDITY_WEIGHTS,
    SWEEP_EXCEED_PCT,
)

logger = logging.getLogger("rasattrading.pa.liquidity")

FUTURES_TYPES = ("funding_rate", "open_interest", "liquidation")


def _cluster_swings(swings: list[dict], pivot_kind: str, tolerance_pct: float) -> list[list[dict]]:
    """Aynı türdeki swing seviyelerini tolerans içinde kümeler; en az 2'si olanları döner."""
    items = sorted((s for s in swings if s["kind"] == pivot_kind), key=lambda s: s["price"])
    clusters: list[list[dict]] = []
    for s in items:
        placed = False
        for cl in clusters:
            mid = sum(x["price"] for x in cl) / len(cl)
            if abs(s["price"] - mid) <= (mid * tolerance_pct / 100.0):
                cl.append(s)
                placed = True
                break
        if not placed:
            clusters.append([s])
    return [c for c in clusters if len(c) >= 2]


def _make_zone(cluster: list[dict], kind: str) -> dict[str, Any]:
    prices = [s["price"] for s in cluster]
    lo, hi = min(prices), max(prices)
    indices = [s["index"] for s in cluster]
    zone: dict[str, Any] = {
        "zone_id": f"{kind}:{lo:.8g}:{hi:.8g}",
        "kind": kind,
        "range": {"low": lo, "high": hi},
        "swing_count": len(cluster),
        "formed_at": max(indices),
        "swept_at": None,
        "mitigated": False,
        "tested": False,
    }
    return zone


def compute_liquidity_zones(
    candles: list[dict],
    structure: dict,
    futures: dict[str, Any] | None = None,
    algo_version: str = LIQUIDITY_ALGO_VERSION,
    tolerance_pct: float = EQUAL_LEVEL_TOLERANCE_PCT,
) -> dict[str, Any]:
    """Swing yapısından likidite bölgeleri + futures tabanlı skor üretir."""
    swings = structure.get("swings", [])
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    n = len(candles)

    zones: list[dict[str, Any]] = []
    for kind, pivot_kind, sweep_dir in (
        ("equal_highs", "high", "high"),
        ("equal_lows", "low", "low"),
    ):
        for cluster in _cluster_swings(swings, pivot_kind, tolerance_pct):
            zone = _make_zone(cluster, kind)
            _mark_status(zone, sweep_dir, highs, lows, n)
            zones.append(zone)

    score = liquidity_score(zones, futures)
    return {"algo_version": algo_version, "zones": zones, "score": score}


def _mark_status(zone: dict, sweep_dir: str, highs: list[float], lows: list[float], n: int) -> None:
    formed = zone["formed_at"]
    band = zone["range"]
    exceed = 1.0 + SWEEP_EXCEED_PCT / 100.0
    for i in range(formed + 1, n):
        if sweep_dir == "high":
            if zone["swept_at"] is None and highs[i] > band["high"] * exceed:
                zone["swept_at"] = i
                zone["mitigated"] = True
            if highs[i] >= band["low"]:
                zone["tested"] = True
        else:
            if zone["swept_at"] is None and lows[i] < band["low"] * (1.0 / exceed):
                zone["swept_at"] = i
                zone["mitigated"] = True
            if lows[i] <= band["high"]:
                zone["tested"] = True


def _norm_funding(rate: float) -> float:
    """|funding rate|/0.001 → 0..1 (0.001 = %0.1 perp fonlama eşiği)."""
    return min(abs(rate) / 0.001, 1.0)


def _norm_liquidation(count: float) -> float:
    """Son N likidasyon sayısı/5 → 0..1."""
    return min(max(count, 0.0), 5.0) / 5.0


def liquidity_score(zones: list[dict], futures: dict[str, Any] | None) -> dict[str, Any]:
    """0-100 likidite skoru. Futures girdileri sadece `fresh` ise katkı verir."""
    futures = futures or {}
    w = LIQUIDITY_WEIGHTS

    eq_count = sum(1 for z in zones if z["kind"] in ("equal_highs", "equal_lows"))
    components: dict[str, Any] = {
        "equal_levels": {
            "status": "fresh",
            "zones": eq_count,
            "included": True,
            "points": round(min(eq_count, 10) / 10.0 * w["equal_levels"], 1),
        }
    }

    specs = (
        ("open_interest", w["open_interest"], lambda v: 1.0 if v and v > 0 else 0.0),
        ("funding_rate", w["funding_rate"], _norm_funding),
        ("liquidation", w["liquidation"], _norm_liquidation),
    )
    futures_available = False
    for key, weight, norm in specs:
        item = futures.get(key)
        if item is None:
            components[key] = {"status": "unknown", "included": False, "points": 0, "note": "veri yok"}
            continue
        status = item.get("freshness", "unknown")
        if status != "fresh":
            components[key] = {"status": status, "included": False, "points": 0, "note": "skora katılmadı"}
            continue
        value = item.get("value")
        points = round(weight * norm(value) if value is not None else 0.0, 1)
        components[key] = {"status": "fresh", "value": value, "included": True, "points": points}
        futures_available = True

    total = round(sum(c["points"] for c in components.values()), 1)
    return {
        "score": total,
        "components": components,
        "futures_available": futures_available,
        "algo_version": LIQUIDITY_ALGO_VERSION,
    }


async def load_futures_context(db: Database, symbol: str) -> dict[str, dict]:
    """Sembolün her türü için EN SON futures_context kaydını döndürür.

    Çıktı: {funding_rate: {type, value, event_time, freshness, fetched_at}, ...}
    — yalnızca tabloda kayıt varsa anahtar bulunur; yoksa o tür `None`.
    """

    def _q(conn):
        placeholders = ",".join("?" for _ in FUTURES_TYPES)
        rows = conn.execute(
            f"SELECT type, value, event_time, freshness, fetched_at FROM futures_context "
            f"WHERE symbol=? AND type IN ({placeholders}) ORDER BY event_time DESC",
            (symbol, *FUTURES_TYPES),
        ).fetchall()
        latest: dict[str, dict] = {}
        for r in rows:
            t = r["type"]
            if t not in latest:
                latest[t] = dict(r)
        return latest

    return await db.read(_q)
