"""2.2 — Liquidity zones + futures context tests."""

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.liquidity import compute_liquidity_zones, liquidity_score, load_futures_context
from rasattrading_mcp.pa.params import LIQUIDITY_ALGO_VERSION
from rasattrading_mcp.pa.swings import detect_structure
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations


def mk(rows):
    return [
        {"open_time": i * 100, "open": o, "high": h, "low": l, "close": c}
        for i, (o, h, l, c) in enumerate(rows)
    ]


# Two equal highs (101.00 / 101.04) → then sweep (102).
EQ_HI_SWEPT = [
    (100, 100.5, 99.5, 100),
    (100, 100.5, 99.5, 100),
    (99, 100, 98, 99.5),          # swing low 98
    (99.5, 100.5, 99, 100),
    (100, 101, 99.5, 100.5),      # swing high 101.00
    (100.5, 100.5, 100, 100.5),
    (100.5, 100.5, 100, 100.5),
    (100, 101.04, 99.5, 100.5),   # swing high 101.04 (equal high).
    (100.5, 100.5, 100, 100.5),
    (100.5, 100.5, 100, 100.5),
    (100.5, 102, 100.5, 101.5),   # sweep: 102 > 101.04
]

# Same structure but no sweep (price does not cross the zone).
EQ_HI_UNSWEPT = EQ_HI_SWEPT[:-1]


def _zones(candles, futures=None):
    st = detect_structure(mk(candles))
    return compute_liquidity_zones(mk(candles), st, futures=futures)


def test_equal_highs_zone_formed():
    result = _zones(EQ_HI_SWEPT)
    assert result["algo_version"] == LIQUIDITY_ALGO_VERSION
    zones = result["zones"]
    assert len(zones) == 1
    z = zones[0]
    assert z["kind"] == "equal_highs"
    assert z["swing_count"] == 2
    assert z["range"] == {"low": 101.0, "high": 101.04}
    assert z["formed_at"] == 7


def test_sweep_mitigates_zone():
    result = _zones(EQ_HI_SWEPT)
    z = result["zones"][0]
    assert z["mitigated"] is True
    assert z["swept_at"] == 10
    assert z["tested"] is True


def test_unswept_zone_stays_active():
    result = _zones(EQ_HI_UNSWEPT)
    z = result["zones"][0]
    assert z["mitigated"] is False
    assert z["swept_at"] is None


def test_equal_lows_zone():
    rows = [
        (100, 100.5, 99.5, 100),
        (100, 100.5, 99.5, 100),
        (100, 102, 99.5, 101),      # swing high 102
        (99.5, 100.5, 98, 99.5),    # swing low 98
        (99.5, 100, 98.5, 99.5),
        (99.5, 100.5, 99, 100),
        (100, 100.5, 98.04, 100.5), # swing low 98.04 (equal low).
        (100, 100.5, 99, 100.5),
        (99.5, 100, 98.5, 99.5),
        (99.5, 100.5, 97.9, 99.5),  # sweep: 97.9 < 98.0
    ]
    result = _zones(rows)
    zones = result["zones"]
    lows = [z for z in zones if z["kind"] == "equal_lows"]
    assert len(lows) == 1
    z = lows[0]
    assert z["range"]["low"] == 98.0
    assert z["mitigated"] is True
    assert z["swept_at"] == 9


def test_isolated_high_no_zone():
    rows = [
        (100, 100.5, 99.5, 100),
        (100, 100.5, 99.5, 100),
        (99, 100, 98, 99.5),        # swing low 98
        (99.5, 100.5, 99, 100),
        (100, 101, 99.5, 100.5),    # single swing high 101 (no equal).
        (100, 100, 99.5, 100),
        (100, 100, 99.5, 100),
        (100, 100.5, 99.5, 100),
        (100, 100.5, 99.5, 100),
        (100, 100.5, 99.5, 100),
        (100.5, 101.2, 100, 100.5),  # close but not a pivot (outside range).
    ]
    result = _zones(rows)
    assert [z for z in result["zones"] if z["kind"] == "equal_highs"] == []


def test_liquidity_score_fresh_futures():
    futures = {
        "open_interest": {"freshness": "fresh", "value": 1234.5},
        "funding_rate": {"freshness": "fresh", "value": 0.0005},
        "liquidation": {"freshness": "fresh", "value": 2},
    }
    sc = liquidity_score([{"kind": "equal_highs"}], futures)
    assert sc["futures_available"] is True
    comp = sc["components"]
    assert comp["open_interest"] == {"status": "fresh", "value": 1234.5, "included": True, "points": 35.0}
    assert comp["funding_rate"]["points"] == 7.5
    assert comp["liquidation"]["points"] == 4.0
    assert comp["equal_levels"]["points"] == 4.0
    assert sc["score"] == 50.5


def test_liquidity_score_stale_not_included():
    futures = {
        "open_interest": {"freshness": "stale", "value": 1234.5},
        "funding_rate": None,
        "liquidation": {"freshness": "unknown", "value": 1},
    }
    sc = liquidity_score([], futures)
    assert sc["futures_available"] is False
    oi = sc["components"]["open_interest"]
    assert oi["included"] is False
    assert oi["status"] == "stale"
    assert oi["points"] == 0
    assert sc["components"]["funding_rate"]["status"] == "unknown"
    # Missing data is not hidden: only equal_levels contributes.
    assert sc["score"] == 0.0


def test_liquidity_deterministic():
    a = _zones(EQ_HI_SWEPT)
    b = _zones(EQ_HI_SWEPT)
    assert a == b


# ---------------------------------------------------------------------------
# futures_context tablosundan okuma
# ---------------------------------------------------------------------------


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


@pytest.fixture
async def db(cfg):
    d = Database(cfg.db_path)
    await d.start()
    yield d
    await d.stop()


async def test_load_futures_context_latest_per_type(db):
    await run_migrations(db)

    def _seed(conn):
        conn.executemany(
            "INSERT INTO futures_context (symbol, type, event_time, value, fetched_at, freshness) "
            "VALUES (?,?,?,?,?,?)",
            [
                ("BTCUSDT", "funding_rate", 100, 0.0001, 100, "fresh"),
                ("BTCUSDT", "funding_rate", 200, 0.0002, 200, "fresh"),
                ("BTCUSDT", "open_interest", 150, 500.0, 150, "fresh"),
                ("BTCUSDT", "liquidation", 120, 3, 120, "stale"),
                ("ETHUSDT", "funding_rate", 100, 0.0001, 100, "fresh"),
            ],
        )

    await db.write(_seed)
    data = await load_futures_context(db, "BTCUSDT")
    assert data["funding_rate"]["event_time"] == 200  # Latest record.
    assert data["funding_rate"]["value"] == 0.0002
    assert data["open_interest"]["value"] == 500.0
    assert data["liquidation"]["freshness"] == "stale"  # durum korunur
    empty = await load_futures_context(db, "UNKNOWN")
    assert empty == {}
