"""2.1 — Swing High/Low + BOS/CHoCH (pure, deterministic).

Rules (version `swing-v1`, thresholds in `pa/params.py`):
- **Swing (fractal):** bar `i` is a swing high iff `high[i]` is strictly greater
  than every other bar's high in the `[i-L, i+L]` window (strictly lower for a
  swing low). `L = SWING_LOOKBACK`.
- **Closed-candle rule:** This module processes exactly the input array it receives;
  the caller must exclude the still-forming last bar (`filter_closed_candles` helper).
  Thus computation never looks at a live bar and is deterministic.
- **Structure:** Preserve the latest swing high/low levels while walking the data.
  Emit an event when a bar close crosses a level:
  - while trend is `up`, close > last swing high → `bos_bullish`
  - while trend is `down`, close < last swing low  → `bos_bearish`
  - while trend is `up`, close < last swing low   → `choch_bearish`, trend → down
  - while trend is `down`, close > last swing high → `choch_bullish`, trend → up
- Determine the initial trend from the order of the first two pivots: low then high
  → up; high then low → down.
"""

from __future__ import annotations

from typing import Any

from .params import SWING_ALGO_VERSION, SWING_LOOKBACK


def filter_closed_candles(candles: list[dict], timeframe: str, now: float | None = None) -> list[dict]:
    """Discard the last still-forming (not closed) bar.

    Closed-candle rule: PA calculations use only closed candles. On Binance, a
    bar closes at `open_time + period`; no bar before that time counts as live.

    If `now` is omitted, fall back to local time; for consistency with the data
    layer (klines server clock), callers should pass the server clock through
    `PAEngine._now()` (T2), otherwise host-clock drift affects this decision.
    """
    from ..config import TIMEFRAME_SECONDS

    if timeframe not in TIMEFRAME_SECONDS:
        raise ValueError(f"unknown timeframe: {timeframe}")
    if not candles:
        return candles
    import time as _time

    if now is None:
        now = _time.time()
    period = TIMEFRAME_SECONDS[timeframe]
    latest_closed = int(now // period) * period - period
    # TODO(T2, low priority): epoch-floor close alignment for 1w/1M does not
    # match Binance's Monday/month-start alignment; handle separately if needed.
    return [c for c in candles if c["open_time"] <= latest_closed]


def detect_swings(highs: list[float], lows: list[float], lookback: int = SWING_LOOKBACK) -> list[tuple[int, str, float]]:
    """Detect fractal swing highs/lows. Return (index, 'high'|'low', price) triples."""
    n = len(highs)
    pivots: list[tuple[int, str, float]] = []
    if n < 2 * lookback + 1:
        return pivots
    for i in range(lookback, n - lookback):
        window_hi = highs[i - lookback : i + lookback + 1]
        if highs[i] > max(window_hi[:lookback]) and highs[i] > max(window_hi[lookback + 1 :]):
            pivots.append((i, "high", highs[i]))
        window_lo = lows[i - lookback : i + lookback + 1]
        if lows[i] < min(window_lo[:lookback]) and lows[i] < min(window_lo[lookback + 1 :]):
            pivots.append((i, "low", lows[i]))
    return pivots


def detect_structure(
    candles: list[dict],
    lookback: int = SWING_LOOKBACK,
    algo_version: str = SWING_ALGO_VERSION,
) -> dict[str, Any]:
    """Build swing + BOS/CHoCH structure.

    Output:
      { algo_version, trend: "up"|"down"|None,
        swings: [{index, time, kind: "high"|"low", price, label: "HH"|"LH"|"HL"|"LL"|null}],
        events: [{index, time, type: "bos_bullish"|"bos_bearish"|"choch_bullish"|"choch_bearish",
                  level, direction: "bullish"|"bearish"}] }
    """
    n = len(candles)
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]
    times = [c["open_time"] for c in candles]

    pivots = detect_swings(highs, lows, lookback)

    last_high: float | None = None
    last_low: float | None = None
    prev_high: float | None = None
    prev_low: float | None = None
    trend: str | None = None
    swings_out: list[dict] = []
    events: list[dict] = []
    first_kind: str | None = None

    pidx = 0
    npiv = len(pivots)
    for b in range(n):
        # 1) Check close breaks against the levels at the START of the bar.
        #    Evaluate against the old level before updating a new pivot from the
        #    same bar; this catches a structure break on the swing-formation bar
        #    without double-counting.
        if b > 0 and last_high is not None and last_low is not None:
            prev_close = closes[b - 1]
            c = closes[b]
            if c > last_high and prev_close <= last_high:
                if trend == "down":
                    events.append(
                        {"index": b, "time": times[b], "type": "choch_bullish", "level": last_high, "direction": "bullish"}
                    )
                    trend = "up"
                else:
                    events.append(
                        {"index": b, "time": times[b], "type": "bos_bullish", "level": last_high, "direction": "bullish"}
                    )
            elif c < last_low and prev_close >= last_low:
                if trend == "up":
                    events.append(
                        {"index": b, "time": times[b], "type": "choch_bearish", "level": last_low, "direction": "bearish"}
                    )
                    trend = "down"
                else:
                    events.append(
                        {"index": b, "time": times[b], "type": "bos_bearish", "level": last_low, "direction": "bearish"}
                    )

        # 2) Process pivots confirmed on the same bar → update levels.
        while pidx < npiv and pivots[pidx][0] == b:
            idx, kind, price = pivots[pidx]
            pidx += 1
            if first_kind is None:
                first_kind = kind
            if kind == "high":
                label = "HH" if (prev_high is not None and price > prev_high) else ("LH" if prev_high is not None else None)
                prev_high = price
                last_high = price
            else:
                label = "HL" if (prev_low is not None and price > prev_low) else ("LL" if prev_low is not None else None)
                prev_low = price
                last_low = price
            swings_out.append({"index": idx, "time": times[idx], "kind": kind, "price": price, "label": label})

        if trend is None and first_kind is not None and last_high is not None and last_low is not None:
            trend = "down" if first_kind == "high" else "up"

    return {"algo_version": algo_version, "trend": trend, "swings": swings_out, "events": events}
