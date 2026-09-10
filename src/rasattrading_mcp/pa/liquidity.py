"""2.2 — Liquidity zones plus futures context (pure computation and DB reads).

Rules (version `liquidity-v1`):
- **Equal highs/lows:** Swing levels of the same type cluster into one liquidity
  zone when their price difference is within `EQUAL_LEVEL_TOLERANCE_PCT`. At least
  two swings are required; a single level does not create a zone.
- **Sweep/mitigation:** After a zone is formed (`formed_at`), if price exceeds
  the zone band (equal_highs: high > band.high; equal_lows: low < band.low), the
  liquidity at that level is considered taken → `mitigated=true`. `tested` means
  price touched but did not exceed the band.
- **Futures context (read-only):** funding rate / OI / liquidation from
  `futures_context` contribute to the liquidity score only when `fresh`.
  `stale`/`unknown` entries are excluded but explicitly shown with
  `included: false` and their status (never silently counted as zero).
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
    """Cluster swing levels of the same type within tolerance; return clusters with at least two."""
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
        "swept_at_time": None,
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
    """Build liquidity zones from swing structure and a futures-based score."""
    swings = structure.get("swings", [])
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    times = [c["open_time"] for c in candles]
    n = len(candles)

    zones: list[dict[str, Any]] = []
    for kind, pivot_kind, sweep_dir in (
        ("equal_highs", "high", "high"),
        ("equal_lows", "low", "low"),
    ):
        for cluster in _cluster_swings(swings, pivot_kind, tolerance_pct):
            zone = _make_zone(cluster, kind)
            _mark_status(zone, sweep_dir, times, highs, lows, n)
            zones.append(zone)

    score = liquidity_score(zones, futures)
    return {"algo_version": algo_version, "zones": zones, "score": score}


def _mark_status(zone: dict, sweep_dir: str, times: list[int], highs: list[float], lows: list[float], n: int) -> None:
    formed = zone["formed_at"]
    band = zone["range"]
    exceed = 1.0 + SWEEP_EXCEED_PCT / 100.0
    for i in range(formed + 1, n):
        if sweep_dir == "high":
            if zone["swept_at"] is None and highs[i] > band["high"] * exceed:
                zone["swept_at"] = i
                zone["swept_at_time"] = times[i]
                zone["mitigated"] = True
            if highs[i] >= band["low"]:
                zone["tested"] = True
        else:
            if zone["swept_at"] is None and lows[i] < band["low"] * (1.0 / exceed):
                zone["swept_at"] = i
                zone["swept_at_time"] = times[i]
                zone["mitigated"] = True
            if lows[i] <= band["high"]:
                zone["tested"] = True


def _norm_funding(rate: float) -> float:
    """Normalize |funding rate|/0.001 → 0..1 (0.001 = 0.1% perp funding threshold)."""
    return min(abs(rate) / 0.001, 1.0)


def _norm_liquidation(count: float) -> float:
    """Normalize the last N liquidation count/5 → 0..1."""
    return min(max(count, 0.0), 5.0) / 5.0


def liquidity_score(zones: list[dict], futures: dict[str, Any] | None) -> dict[str, Any]:
    """0-100 liquidity score. Futures inputs contribute only when `fresh`.

    2.15 fix — `equal_levels` points are based on active (unmitigated) zone count;
    mitigated zones are "used liquidity" and earn no points (if 7 of 10 zones are
    mitigated, do not award full points). The funding component carries direction
    information: `bias: long_crowded|short_crowded` (also exposed as `funding_bias`).
    """
    futures = futures or {}
    w = LIQUIDITY_WEIGHTS

    eq_zones = [z for z in zones if z["kind"] in ("equal_highs", "equal_lows")]
    eq_count = len(eq_zones)
    eq_mitigated = sum(1 for z in eq_zones if z.get("mitigated"))
    eq_active = eq_count - eq_mitigated
    components: dict[str, Any] = {
        "equal_levels": {
            "status": "fresh",
            "zones": eq_count,
            "active_zones": eq_active,
            "mitigated_zones": eq_mitigated,
            "note": (
                f"equal-level zone count is {eq_count}; {eq_active} active (unmitigated), "
                f"{eq_mitigated} mitigated — only active zones appear in the default list; "
                f"points are based on active zone count"
            ),
            "included": True,
            "points": round(min(eq_active, 10) / 10.0 * w["equal_levels"], 1),
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
            components[key] = {"status": "unknown", "included": False, "points": 0, "note": "data unavailable"}
            continue
        status = item.get("freshness", "unknown")
        if status != "fresh":
            components[key] = {"status": status, "included": False, "points": 0, "note": "excluded from score"}
            continue
        value = item.get("value")
        points = round(weight * norm(value) if value is not None else 0.0, 1)
        comp: dict[str, Any] = {"status": "fresh", "value": value, "included": True, "points": points}
        if key == "funding_rate" and value is not None:
            # Positive funding → longs pay (crowded long); negative → crowded shorts.
            comp["bias"] = "long_crowded" if value >= 0 else "short_crowded"
        components[key] = comp
        futures_available = True

    funding = components.get("funding_rate") or {}
    result: dict[str, Any] = {
        "score": round(sum(c["points"] for c in components.values()), 1),
        "components": components,
        "futures_available": futures_available,
        "algo_version": LIQUIDITY_ALGO_VERSION,
    }
    if funding.get("included") and "bias" in funding:
        result["funding_bias"] = funding["bias"]
    return result


async def load_futures_context(db: Database, symbol: str) -> dict[str, dict]:
    """Return the LATEST futures_context record for each type of a symbol.

    Output: {funding_rate: {type, value, event_time, freshness, fetched_at}, ...}
    — a key exists only when the table has a record; otherwise that type is absent.
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


async def load_futures_series(db: Database, symbol: str, ftype: str, limit: int = 20) -> list[dict]:
    """Return the newest `limit` records for a type in chronological order (oldest→newest).

    Filters such as `oi_change` need the type's history; return an empty list (not
    an error) for an unknown type. Select the newest records (ASC+LIMIT returned
    the oldest — 2.10 fix), then reverse them into chronological order.
    """

    def _q(conn):
        rows = conn.execute(
            "SELECT type, value, event_time, freshness, fetched_at FROM futures_context "
            "WHERE symbol=? AND type=? ORDER BY event_time DESC LIMIT ?",
            (symbol, ftype, limit),
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    return await db.read(_q)
