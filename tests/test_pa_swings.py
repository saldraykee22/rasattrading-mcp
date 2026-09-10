"""2.1 — Swing High/Low + BOS/CHoCH fixture tests.

Verify expected swing/BOS/CHoCH output for known candle sequences.
If the algorithm changes, `algo_version` must increase—these tests guarantee
deterministic output and the version label for fixed input.

Kurallar (swing-v1):
- A pivot is marked only when a half-window exists on both sides
  (index [lookback, n-lookback)); strict comparison (equals are not pivots).
- Events use the cross rule: close against the level at the start of the bar.
"""

import time

import pytest

from rasattrading_mcp.pa.params import SWING_ALGO_VERSION
from rasattrading_mcp.pa.swings import detect_structure, detect_swings, filter_closed_candles


def mk(rows):
    return [
        {"open_time": i * 100, "open": o, "high": h, "low": l, "close": c}
        for i, (o, h, l, c) in enumerate(rows)
    ]


def events(st):
    return [(e["type"], e["index"]) for e in st["events"]]


UPTREND = [
    (100, 100.5, 99.5, 100),      # 0
    (100, 100.5, 99.5, 100),      # 1
    (99, 100, 98, 99.5),          # 2  swing low 98
    (99.5, 100.5, 99, 100),       # 3
    (100, 102, 99.5, 101),        # 4  swing high 102
    (101, 101.5, 100.5, 101),     # 5
    (101, 101.5, 100.5, 100.5),   # 6
    (100.5, 103, 101, 102.5),     # 7  bos_bullish (102), swing high 103
    (102, 102.5, 101.5, 102),     # 8
    (102, 102.5, 101.5, 102),     # 9
    (102.5, 103.5, 102, 103),     # 10
    (103, 103.5, 102.5, 103),     # 11
    (103, 105, 102.5, 104.5),     # 12 bos_bullish (103), swing high 105
    (104, 104.5, 103.5, 104),     # 13
    (104, 104.5, 103.5, 103.5),   # 14
]

CHOCB = [
    (100, 100.5, 99.5, 100),      # 0
    (100, 100.5, 99.5, 100),      # 1
    (99, 100, 98, 99.5),          # 2  swing low 98
    (99.5, 100.5, 99, 100),       # 3
    (100, 102, 99.5, 101),        # 4  swing high 102
    (101, 101.5, 100.5, 101),     # 5
    (101, 101.5, 100.5, 100.5),   # 6
    (100.5, 101, 99, 100),        # 7  swing low 99
    (100, 101, 99.5, 100.5),      # 8
    (100.5, 101, 100, 100.5),     # 9
    (100.5, 101, 99, 99.5),       # 10
    (99.5, 100, 98.5, 98.5),      # 11 choch_bearish (99), swing low 98.5
]

CHOCU = [
    (100, 100.5, 99.5, 100),      # 0
    (100, 100.5, 99.5, 100),      # 1
    (101, 103, 100, 101.5),       # 2  swing high 103
    (102, 102, 99.5, 101.5),      # 3
    (101.5, 102, 99.5, 101),      # 4
    (101, 101.5, 98, 98.5),       # 5  swing low 98
    (99, 99.5, 99, 99),           # 6
    (99.5, 100, 99.5, 99.5),      # 7
    (99.5, 104, 99.5, 103.5),     # 8  choch_bullish (103), swing high 104
    (103, 103.5, 102.5, 103),     # 9
    (103, 103.5, 102.5, 102.5),   # 10
    (102.5, 105, 102.5, 104.5),   # 11 bos_bullish (104), swing high 105
    (104, 104.5, 103.5, 104),     # 12
    (104, 104.5, 103.5, 103.5),   # 13
]

HOLD = [
    (100, 100.5, 99.5, 100),      # 0
    (100, 100.5, 99.5, 100),      # 1
    (99, 100, 98, 99.5),          # 2  swing low 98
    (99.5, 100.5, 99, 100),       # 3
    (100, 102, 99.5, 101),        # 4  swing high 102
    (101, 101.5, 100.5, 101),     # 5
    (101, 101.5, 100.5, 100.5),   # 6
    (100.5, 103, 101, 102.5),     # 7  bos_bullish (102), swing high 103
    (102.5, 102.5, 102, 102.5),   # 8  remains above — no new event.
    (102.5, 102.5, 102, 102.5),   # 9
    (102, 102.5, 101.5, 102),     # 10
    (102, 102.5, 101.5, 101.5),   # 11
]


def test_swing_fractal_detection():
    pivots = detect_swings([r[1] for r in UPTREND], [r[2] for r in UPTREND])
    assert pivots == [(2, "low", 98.0), (4, "high", 102.0), (7, "high", 103.0), (12, "high", 105.0)]


def test_swing_left_edge_not_pivot():
    rows = [
        (100, 105, 100, 100),
        (99, 100, 98, 99),
        (99.5, 101, 99.5, 100),
        (100, 100.5, 100, 100),
        (100, 100.5, 100, 99.5),
    ]
    pivots = detect_swings([r[1] for r in rows], [r[2] for r in rows])
    assert pivots == []  # Indices 0-1 lack the left half-window → no pivot.


def test_uptrend_bullish_bos():
    st = detect_structure(mk(UPTREND))
    assert st["algo_version"] == SWING_ALGO_VERSION
    assert st["trend"] == "up"
    assert events(st) == [("bos_bullish", 7), ("bos_bullish", 12)]


def test_swing_labels_hh():
    st = detect_structure(mk(UPTREND))
    labels = [(s["kind"], s["index"], s["label"]) for s in st["swings"]]
    assert labels == [
        ("low", 2, None),
        ("high", 4, None),
        ("high", 7, "HH"),
        ("high", 12, "HH"),
    ]


def test_choch_bearish_from_uptrend():
    st = detect_structure(mk(CHOCB))
    assert events(st) == [("choch_bearish", 11)]
    assert st["trend"] == "down"


def test_choch_bullish_from_downtrend():
    st = detect_structure(mk(CHOCU))
    assert events(st) == [("choch_bullish", 8), ("bos_bullish", 11)]
    assert st["trend"] == "up"


def test_initial_trend_first_pivot_high_is_down():
    st = detect_structure(mk(CHOCU))
    # First pivot high → initial trend down; upward break becomes CHoCH.
    first = st["events"][0]
    assert first["type"] == "choch_bullish"
    assert first["level"] == 103.0


def test_no_event_on_same_bar_hold():
    st = detect_structure(mk(HOLD))
    assert events(st) == [("bos_bullish", 7)]


def test_deterministic_same_input():
    assert detect_structure(mk(UPTREND)) == detect_structure(mk(UPTREND))


def test_filter_closed_candles_drops_forming_bar():
    from rasattrading_mcp.config import TIMEFRAME_SECONDS

    tf = "1h"
    period = TIMEFRAME_SECONDS[tf]
    latest_closed = int(time.time() // period) * period - period
    candles = [
        {"open_time": latest_closed - 2 * period, "open": 1, "high": 2, "low": 0.5, "close": 1.5},
        {"open_time": latest_closed - period, "open": 1.5, "high": 2.5, "low": 1, "close": 2},
        {"open_time": latest_closed, "open": 2, "high": 3, "low": 1.5, "close": 2.5},
        {"open_time": latest_closed + period, "open": 2.5, "high": 3.5, "low": 2, "close": 3},
    ]
    closed = filter_closed_candles(candles, tf)
    assert [c["open_time"] for c in closed] == [latest_closed - 2 * period, latest_closed - period, latest_closed]


def test_filter_closed_candles_unknown_timeframe():
    with pytest.raises(ValueError):
        filter_closed_candles([], "99m")
