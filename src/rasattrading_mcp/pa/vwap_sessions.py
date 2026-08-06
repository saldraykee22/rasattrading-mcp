"""2.3 — VWAP (oturum/session anchored) ve Session/Killzone seviyeleri.

- **VWAP:** Gün (UTC) başına yeniden çapalanır; `(H+L+C)/3 * volume` kümülatif
  toplam / kümülatif hacim. PA-destekleyici tek indikatör istisnasıdır.
- **Session seviyeleri:** `KILLZONES` (UTC saat aralıkları) içinde kalan
  mumların günlük high/low'u. Sabit timezone: `SESSION_TIMEZONE` (UTC).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .params import SESSION_ALGO_VERSION, SESSION_TIMEZONE, VWAP_ALGO_VERSION, KILLZONES


def compute_vwap(candles: list[dict], algo_version: str = VWAP_ALGO_VERSION) -> dict[str, Any]:
    """UTC gününe çapalanmış VWAP serisi ve güncel değer."""
    cum_tp = 0.0
    cum_vol = 0.0
    current_day: int | None = None
    points: list[dict] = []
    for c in candles:
        day = int(c["open_time"] // 86400)
        if current_day is None:
            current_day = day
        elif day != current_day:
            cum_tp, cum_vol = 0.0, 0.0
            current_day = day
        tp = (c["high"] + c["low"] + c["close"]) / 3.0
        vol = c.get("volume") or 0.0
        cum_tp += tp * vol
        cum_vol += vol
        vwap = round(cum_tp / cum_vol, 8) if cum_vol > 0 else None
        points.append({"time": c["open_time"], "vwap": vwap})

    return {
        "algo_version": algo_version,
        "anchored_at": current_day * 86400 if current_day is not None else None,
        "current": points[-1]["vwap"] if points else None,
        "points": points,
    }


def compute_session_levels(candles: list[dict], algo_version: str = SESSION_ALGO_VERSION) -> dict[str, Any]:
    """Killzone aralıklarına göre günlük high/low seviyeleri."""
    by_zone: dict[str, list[tuple[str, list[dict]]]] = {name: [] for name in KILLZONES}
    for c in candles:
        dt = datetime.fromtimestamp(c["open_time"], tz=timezone.utc)
        day = dt.strftime("%Y-%m-%d")
        hour = dt.hour
        for name, (start, end) in KILLZONES.items():
            if start <= hour < end:
                by_zone[name].append((day, c))

    sessions: list[dict] = []
    for name, (start, end) in KILLZONES.items():
        entries = by_zone[name]
        if not entries:
            continue
        latest_day = max(day for day, _ in entries)
        candles_in = [c for day, c in entries if day == latest_day]
        sessions.append(
            {
                "name": name,
                "day": latest_day,
                "start_hour": start,
                "end_hour": end,
                "high": max(x["high"] for x in candles_in),
                "low": min(x["low"] for x in candles_in),
            }
        )

    return {"algo_version": algo_version, "timezone": SESSION_TIMEZONE, "sessions": sessions}
