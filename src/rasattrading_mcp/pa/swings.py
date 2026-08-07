"""2.1 — Swing High/Low + BOS/CHoCH (saf, deterministik).

Kurallar (sürüm `swing-v1`, eşikler `pa/params.py`):
- **Swing (fractal):** `i` barı swing high'dır ⇔ `high[i]`, `[i-L, i+L]`
  penceresindeki diğer tüm bar'ların high'ından katı biçimde büyüktür
  (swing low için low'un katı biçimde küçük olması). `L = SWING_LOOKBACK`.
- **Kapalı mum kuralı:** Bu modül girdi olarak aldığı dizi neyse onu işler;
  hâlâ oluşmakta olan son barı hariç tutmak çağıranın işidir
  (`filter_closed_candles` yardımcısı). Yani hesaplama asla canlı bara
  bakmaz, deterministiktir.
- **Yapı:** Yürüyüş sırasında en güncel swing high/low seviyeleri korunur.
  Bir barın kapanışı (cross) bir seviyeyi geçince olay üretilir:
  - trend `up` iken close > son swing high → `bos_bullish`
  - trend `down` iken close < son swing low  → `bos_bearish`
  - trend `up` iken close < son swing low   → `choch_bearish`, trend → down
  - trend `down` iken close > son swing high → `choch_bullish`, trend → up
- Başlangıç trendi ilk iki pivotun sırasından belirlenir: önce low sonra high
  → up; önce high sonra low → down.
"""

from __future__ import annotations

from typing import Any

from .params import SWING_ALGO_VERSION, SWING_LOOKBACK


def filter_closed_candles(candles: list[dict], timeframe: str, now: float | None = None) -> list[dict]:
    """Hâlâ oluşmakta olan (kapanmamış) son barı atar.

    Kapalı mum kuralı: PA hesaplamaları yalnızca kapanmış mumlar üzerinden
    yapılır. Binance'te bar `open_time + period` anında kapanır; bu andan
    önceki hiçbir bar canlı sayılmaz.

    `now` verilmezse yerel saate düşülür; veri katmanıyla (klines server clock)
    tutarlılık için çağıranlar `PAEngine._now()` ile server clock'u geçirmeli
    (T2) — host saati kayarsa bu karar kayar.
    """
    from ..config import TIMEFRAME_SECONDS

    if timeframe not in TIMEFRAME_SECONDS:
        raise ValueError(f"bilinmeyen timeframe: {timeframe}")
    if not candles:
        return candles
    import time as _time

    if now is None:
        now = _time.time()
    period = TIMEFRAME_SECONDS[timeframe]
    latest_closed = int(now // period) * period - period
    # TODO(T2, düşük öncelik): 1w/1M için epoch-floor kapanış hizalaması
    # Binance'in Pazartesi/ay-başı hizalamasıyla uyuşmaz — gerekiyorsa ayrıca ele alın.
    return [c for c in candles if c["open_time"] <= latest_closed]


def detect_swings(highs: list[float], lows: list[float], lookback: int = SWING_LOOKBACK) -> list[tuple[int, str, float]]:
    """Fractal swing high/low tespiti. (index, 'high'|'low', price) üçlüleri."""
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
    """Swing + BOS/CHoCH yapısını üretir.

    Çıktı:
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
        # 1) Kapanış kırılımı kontrolü — barın BAŞINDAKİ seviyelere karşı.
        #    (Aynı barda yeni bir pivot seviyeyi güncellemeden önce eski
        #    seviyeye karşı değerlendirilir; böylece swing oluşum barındaki
        #    yapı kırılımı da yakalanır, çift sayım olmaz.)
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

        # 2) Aynı barda doğrulanan pivotları işle → seviyeleri güncelle.
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
