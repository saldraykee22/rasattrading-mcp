"""2.3 — Order Block / FVG detection (pure, deterministic).

Rules (version `obfvg-v1`):
- **Order Block:** The LAST opposite-colored candle before a BOS/CHoCH event is
  that move's order block (bullish event → last red candle; bearish event → last
  green candle). The zone is that candle's [low, high] range. Search only closed
  candles to the left of the event bar.
- **Mitigation:** If price returns to the zone after formation (bullish OB: a
  candle's low ≤ zone.high; bearish OB: high ≥ zone.low) → `mitigated=true`,
  `zone_type=mitigation_block`.
- **Breaker:** If price fully exceeds the zone and closes beyond the opposite edge
  (bullish OB: close < zone.low; bearish OB: close > zone.high) →
  `zone_type=breaker`. A breaker is an OB broken/used by a close: 2.15 fix — it is
  no longer a valid active zone, so it carries `mitigated=true` (previously it
  remained `false` and was treated as active).
- **Dedup (2.15):** Merge OBs covering the same or a very close price range into
  one logical zone. Multiple BOS/CHoCH events may select the same candle as the
  "last opposite-colored candle", producing separate records and inflating the
  active count. Preserve the first (earliest event) record because its mitigation
  scan is most complete and carries the zone's real state.
- **FVG:** `candle[i].high < candle[i+2].low` → bullish;
  `candle[i].low > candle[i+2].high` → bearish. The zone is the gap range.
  Do not create gaps `< FVG_MIN_GAP_PCT`. Price entering an FVG → mitigated.
"""

from __future__ import annotations

from typing import Any

from .params import FVG_MIN_GAP_PCT, OB_DEDUP_TOLERANCE_PCT, OBFVG_ALGO_VERSION


def _last_opposite_candle(candles: list[dict], upto_index: int, want_red: bool) -> int | None:
    """Return the index of the first requested-color candle searching backward from `upto_index`."""
    for i in range(upto_index - 1, -1, -1):
        c = candles[i]
        red = c["close"] < c["open"]
        if red == want_red:
            return i
    return None


def _mark_order_block(zone: dict, candles: list[dict]) -> None:
    n = len(candles)
    direction = zone["direction"]
    # Scan from AFTER the BOS/CHoCH event bar; since the event bar creates the OB,
    # it does not count as a "return".
    start = zone["event_index"] + 1
    for i in range(start, n):
        c = candles[i]
        if direction == "bullish":
            if c["close"] < zone["range"]["low"]:
                # Breaker: the OB was broken by a close beyond the opposite edge;
                # it is no longer valid, so mitigated=true (2.15 fix; was false).
                zone["zone_type"] = "breaker"
                zone["mitigated"] = True
                return
            if not zone["mitigated"] and c["low"] <= zone["range"]["high"]:
                zone["mitigated"] = True
                zone["zone_type"] = "mitigation_block"
        else:
            if c["close"] > zone["range"]["high"]:
                zone["zone_type"] = "breaker"
                zone["mitigated"] = True
                return
            if not zone["mitigated"] and c["high"] >= zone["range"]["low"]:
                zone["mitigated"] = True
                zone["zone_type"] = "mitigation_block"


def _compute_fvgs(candles: list[dict], min_gap_pct: float) -> list[dict]:
    n = len(candles)
    fvgs: list[dict] = []
    for i in range(n - 2):
        a, b, c = candles[i], candles[i + 1], candles[i + 2]
        if a["high"] < c["low"]:
            lo, hi, direction = a["high"], c["low"], "bullish"
        elif a["low"] > c["high"]:
            lo, hi, direction = c["high"], a["low"], "bearish"
        else:
            continue
        ref = (a["high"] + a["low"]) / 2.0
        if ref > 0 and (hi - lo) / ref * 100.0 < min_gap_pct:
            continue
        zone: dict[str, Any] = {
            "zone_id": f"fvg:{i}:{i + 2}",
            "zone_type": "fvg",
            "direction": direction,
            "range": {"low": lo, "high": hi},
            "formed_at": i + 2,
            "mitigated": False,
        }
        _mark_fvg(zone, candles)
        fvgs.append(zone)
    return fvgs


def _mark_fvg(zone: dict, candles: list[dict]) -> None:
    n = len(candles)
    direction = zone["direction"]
    for i in range(zone["formed_at"] + 1, n):
        c = candles[i]
        if direction == "bullish":
            if c["low"] <= zone["range"]["high"]:
                zone["mitigated"] = True
                return
        else:
            if c["high"] >= zone["range"]["low"]:
                zone["mitigated"] = True
                return


def _dedup_order_blocks(order_blocks: list[dict], tolerance_pct: float) -> list[dict]:
    """Merge OBs covering the same or a very close price range into one logical zone.

    Multiple BOS/CHoCH events may select the same candle as the "last opposite-colored
    candle", producing separate records for the same price range (2.15 fix — inflated
    count). When ranges overlap within tolerance, preserve the **first (earliest event)
    record**: its mitigation scan is most complete and carries the zone's real state;
    remove duplicates.
    """
    kept: list[dict] = []
    for ob in order_blocks:
        lo, hi = ob["range"]["low"], ob["range"]["high"]
        ref = (lo + hi) / 2.0
        tol = ref * tolerance_pct / 100.0
        dup = False
        for k in kept:
            if k["direction"] != ob["direction"]:
                continue
            klo, khi = k["range"]["low"], k["range"]["high"]
            if not (hi + tol < klo or khi + tol < lo):
                dup = True
                break
        if not dup:
            kept.append(ob)
    return kept


def compute_order_blocks(
    candles: list[dict],
    structure: dict,
    algo_version: str = OBFVG_ALGO_VERSION,
    min_gap_pct: float = FVG_MIN_GAP_PCT,
    dedup_tolerance_pct: float = OB_DEDUP_TOLERANCE_PCT,
) -> dict[str, Any]:
    """Build order blocks from BOS/CHoCH events and compute all FVGs."""
    events = structure.get("events", [])
    order_blocks: list[dict] = []
    for ev in events:
        if ev["type"] in ("bos_bullish", "choch_bullish"):
            direction, want_red = "bullish", True
        else:
            direction, want_red = "bearish", False
        ob_idx = _last_opposite_candle(candles, ev["index"], want_red=want_red)
        if ob_idx is None:
            continue
        c = candles[ob_idx]
        zone: dict[str, Any] = {
            "zone_id": f"ob:{ev['index']}:{ob_idx}",
            "zone_type": "order_block",
            "direction": direction,
            "range": {"low": c["low"], "high": c["high"]},
            "event_index": ev["index"],
            "candle_index": ob_idx,
            "formed_at": ob_idx,
            "mitigated": False,
        }
        _mark_order_block(zone, candles)
        order_blocks.append(zone)

    order_blocks = _dedup_order_blocks(order_blocks, dedup_tolerance_pct)

    return {
        "algo_version": algo_version,
        "order_blocks": order_blocks,
        "fvgs": _compute_fvgs(candles, min_gap_pct),
    }
