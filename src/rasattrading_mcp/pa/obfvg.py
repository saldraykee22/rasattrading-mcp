"""2.3 — Order Block / FVG tespiti (saf, deterministik).

Kurallar (sürüm `obfvg-v1`):
- **Order Block:** Bir BOS/CHoCH olayından önceki SON karşı renkli mum, o
  hareketin order block'udur (bullish olay → son kırmızı mum; bearish olay →
  son yeşil mum). Bölge = o mumun [low, high] aralığı. Yalnızca kapanmış
  mumlar üzerinden; olay barının solundaki mumlar aranır.
- **Mitigasyon:** Bölge oluştuktan sonra fiyat bölgeye geri dönerse
  (bullish OB: bir mumun low'u ≤ zone.high; bearish OB: high'ı ≥ zone.low)
  → `mitigated=true`, `zone_type=mitigation_block`.
- **Breaker:** Fiyat bölgeyi tamamen aşar ve karşı kenardan kapanırsa
  (bullish OB: close < zone.low; bearish OB: close > zone.high)
  → `zone_type=breaker` (öncelikli).
- **FVG:** `candle[i].high < candle[i+2].low` → bullish; 
  `candle[i].low > candle[i+2].high` → bearish. Bölge = boşluk aralığı.
  Boşluk `< FVG_MIN_GAP_PCT` ise üretilmez. FVG'ye fiyat girişi → mitigated.
"""

from __future__ import annotations

from typing import Any

from .params import FVG_MIN_GAP_PCT, OBFVG_ALGO_VERSION


def _last_opposite_candle(candles: list[dict], upto_index: int, want_red: bool) -> int | None:
    """`upto_index`'ten geriye ilk istenen renkteki mumun indeksini döner."""
    for i in range(upto_index - 1, -1, -1):
        c = candles[i]
        red = c["close"] < c["open"]
        if red == want_red:
            return i
    return None


def _mark_order_block(zone: dict, candles: list[dict]) -> None:
    n = len(candles)
    direction = zone["direction"]
    # BOS/CHoCH olay barının SONRASINDAN itibaren tararız — olay barı OB'nin
    # doğduğu bar olduğu için "geri dönüş" sayılmaz.
    start = zone["event_index"] + 1
    for i in range(start, n):
        c = candles[i]
        if direction == "bullish":
            if c["close"] < zone["range"]["low"]:
                zone["zone_type"] = "breaker"
                zone["mitigated"] = False
                return
            if not zone["mitigated"] and c["low"] <= zone["range"]["high"]:
                zone["mitigated"] = True
                zone["zone_type"] = "mitigation_block"
        else:
            if c["close"] > zone["range"]["high"]:
                zone["zone_type"] = "breaker"
                zone["mitigated"] = False
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


def compute_order_blocks(
    candles: list[dict],
    structure: dict,
    algo_version: str = OBFVG_ALGO_VERSION,
    min_gap_pct: float = FVG_MIN_GAP_PCT,
) -> dict[str, Any]:
    """BOS/CHoCH olaylarından order block üretir + tüm FVG'leri hesaplar."""
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

    return {
        "algo_version": algo_version,
        "order_blocks": order_blocks,
        "fvgs": _compute_fvgs(candles, min_gap_pct),
    }
