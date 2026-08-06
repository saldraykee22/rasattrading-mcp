"""2.3 — Order Block/FVG + VWAP + Session seviyeleri testleri."""

from datetime import datetime, timezone

from rasattrading_mcp.pa.obfvg import compute_order_blocks
from rasattrading_mcp.pa.params import OBFVG_ALGO_VERSION, SESSION_ALGO_VERSION, VWAP_ALGO_VERSION
from rasattrading_mcp.pa.swings import detect_structure
from rasattrading_mcp.pa.vwap_sessions import compute_session_levels, compute_vwap


def mk(rows):
    return [
        {"open_time": i * 100, "open": o, "high": h, "low": l, "close": c, "volume": 10.0}
        for i, (o, h, l, c) in enumerate(rows)
    ]


# UPTREND yapısı: bos_bullish@7 ve bos@12; son kırmızı mum idx6 (OB adayı)
OBS = [
    (100, 100.5, 99.5, 100),      # 0
    (100, 100.5, 99.5, 100),      # 1
    (99, 100, 98, 99.5),          # 2  swing low 98
    (99.5, 100.5, 99, 100),       # 3
    (100, 102, 99.5, 101),        # 4  swing high 102
    (101, 101.5, 100.5, 101),     # 5
    (101, 101.5, 100.5, 100.5),   # 6  RED → OB
    (100.5, 103, 101, 102.5),     # 7  bos_bullish@7 (102)
    (102, 102.5, 101.5, 102),     # 8  low 101.5 → OB'ye giriş (mitigated)
    (102, 102.5, 101.5, 102),     # 9
    (102.5, 103.5, 102, 103),     # 10
    (103, 103.5, 102.5, 103),     # 11
    (103, 105, 102.5, 104.5),     # 12 bos_bullish@12 (103)
    (104, 104.5, 103.5, 104),     # 13
    (104, 104.5, 103.5, 103.5),   # 14
]

# OB sonrası fiyat bölgeyi aşar (breaker)
BREAKER = OBS[:8] + [
    (102, 102.5, 101.5, 102),     # 8
    (101.5, 102, 100.4, 100.3),   # 9  close 100.3 < OB.low 100.5 → breaker
    (101, 101.5, 100.5, 101),     # 10
]

FVG = [
    (100, 101, 99, 100.5),        # 0  high 101
    (100.6, 100.8, 100.4, 100.6), # 1  ortadaki mum
    (101.5, 103, 101.3, 102.5),   # 2  low 101.3 > 101 → bullish FVG [101, 101.3]
    (102, 102.5, 100.6, 101.5),   # 3  low 100.6 ≤ 101.3 → mitigated
]


def test_order_block_after_bos():
    st = detect_structure(mk(OBS))
    res = compute_order_blocks(mk(OBS), st)
    assert res["algo_version"] == OBFVG_ALGO_VERSION
    obs = res["order_blocks"]
    assert len(obs) == 2
    ob = next(o for o in obs if o["event_index"] == 7)
    assert ob["direction"] == "bullish"
    assert ob["candle_index"] == 6
    assert ob["range"] == {"low": 100.5, "high": 101.5}
    assert ob["zone_type"] == "mitigation_block"
    assert ob["mitigated"] is True


def test_order_block_becomes_breaker():
    st = detect_structure(mk(BREAKER))
    res = compute_order_blocks(mk(BREAKER), st)
    ob = next(o for o in res["order_blocks"] if o["event_index"] == 7)
    assert ob["zone_type"] == "breaker"
    assert ob["mitigated"] is False


def test_fvg_detection_and_mitigation():
    st = {"events": [], "swings": []}
    res = compute_order_blocks(mk(FVG), st)
    assert len(res["order_blocks"]) == 0  # olay yok → OB yok
    fvgs = res["fvgs"]
    assert len(fvgs) == 1
    fvg = fvgs[0]
    assert fvg["zone_type"] == "fvg"
    assert fvg["direction"] == "bullish"
    assert fvg["range"] == {"low": 101.0, "high": 101.3}
    assert fvg["mitigated"] is True


def test_fvg_bearish():
    rows = [
        (103, 104, 102.5, 103.5),  # 0  low 102.5
        (102.4, 102.6, 102.2, 102.4),  # 1
        (101.6, 102.1, 101.5, 102),    # 2  high 102.1 < 102.5 → bearish FVG [102.1, 102.5]
    ]
    st = {"events": [], "swings": []}
    res = compute_order_blocks(mk(rows), st)
    fvgs = res["fvgs"]
    assert len(fvgs) == 1
    assert fvgs[0]["direction"] == "bearish"
    assert fvgs[0]["range"] == {"low": 102.1, "high": 102.5}


def test_fvg_min_gap_filter():
    rows = [
        (100, 101, 99, 100.5),        # high 101
        (100.6, 101.1, 100.4, 100.6), # middle — ikinci boşluk oluşturmaz
        (101.5, 103, 101.05, 102.5),  # gap 101→101.05 = %0.05
    ]
    st = {"events": [], "swings": []}
    res = compute_order_blocks(mk(rows), st, min_gap_pct=0.1)
    assert res["fvgs"] == []  # küçük boşluk elenir
    res2 = compute_order_blocks(mk(rows), st, min_gap_pct=0.0)
    assert len(res2["fvgs"]) == 1


def test_obfvg_deterministic():
    st = detect_structure(mk(OBS))
    assert compute_order_blocks(mk(OBS), st) == compute_order_blocks(mk(OBS), st)


# ---------------------------------------------------------------------------
# VWAP
# ---------------------------------------------------------------------------


def test_vwap_single_day_series():
    rows = [
        {"open_time": 0, "open": 100, "high": 100, "low": 100, "close": 100, "volume": 10},
        {"open_time": 3600, "open": 110, "high": 110, "low": 110, "close": 110, "volume": 20},
        {"open_time": 7200, "open": 90, "high": 90, "low": 90, "close": 90, "volume": 10},
    ]
    res = compute_vwap(rows)
    assert res["algo_version"] == VWAP_ALGO_VERSION
    assert res["points"][0]["vwap"] == 100.0
    assert abs(res["points"][1]["vwap"] - 106.66666667) < 1e-6
    assert res["points"][2]["vwap"] == 102.5
    assert res["current"] == 102.5
    assert res["anchored_at"] == 0


def test_vwap_resets_at_day_rollover():
    rows = [
        {"open_time": 0, "open": 100, "high": 100, "low": 100, "close": 100, "volume": 10},
        {"open_time": 86400, "open": 50, "high": 50, "low": 50, "close": 50, "volume": 10},
    ]
    res = compute_vwap(rows)
    assert res["points"][-1]["vwap"] == 50.0  # yeni güne çapalanır
    assert res["anchored_at"] == 86400


# ---------------------------------------------------------------------------
# Session seviyeleri
# ---------------------------------------------------------------------------


def _epoch(hour):
    base = datetime(2026, 1, 15, tzinfo=timezone.utc).timestamp()
    return int(base) + hour * 3600


def test_session_levels_per_killzone():
    candles = [
        {"open_time": _epoch(3), "high": 105, "low": 101},   # asian
        {"open_time": _epoch(10), "high": 110, "low": 102},  # london
        {"open_time": _epoch(14), "high": 112, "low": 103},  # newyork
        {"open_time": _epoch(6), "high": 106, "low": 99},    # asian (daha düşük low)
    ]
    res = compute_session_levels(candles)
    assert res["algo_version"] == SESSION_ALGO_VERSION
    assert res["timezone"] == "UTC"
    by_name = {s["name"]: s for s in res["sessions"]}
    assert by_name["asian"]["high"] == 106
    assert by_name["asian"]["low"] == 99
    assert by_name["london"]["high"] == 112  # 14:00 london (7-16) ile newyork'u kapsar
    assert by_name["newyork"]["high"] == 112


def test_session_levels_empty_window_skipped():
    candles = [
        {"open_time": _epoch(18), "high": 112, "low": 103},  # newyork-only saat
    ]
    res = compute_session_levels(candles)
    names = {s["name"] for s in res["sessions"]}
    assert names == {"newyork"}
