"""2.15 FIX — Score/freshness/OB design-weakness regression tests.

Five weaknesses found by an independent critic agent (INJ/SEI/ARB live analysis)
are converted into tests:

- S1 (freshness): `freshness_for` now considers a snapshot one period older than
  the last closed candle `stale` (previously `fresh` with `- period` tolerance);
  PA meta carries `freshness_note`.
- S2 (mitigation weighting): `equal_levels` points use active-zone count—7 of 10
  mitigated zones do not receive full points.
- S3 (funding direction): funding component carries `bias: long_crowded|short_crowded`.
- S4 (breaker): an OB broken on close becomes `zone_type=breaker` + `mitigated=true`.
- S5 (OB dedup): OBs covering the same/very close price range merge into one
  logical zone (multiple BOS/CHoCH events selected the same candle).
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.liquidity import liquidity_score
from rasattrading_mcp.pa.obfvg import compute_order_blocks
from rasattrading_mcp.pa.swings import detect_structure
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1h"
PERIOD = 3600

# Two BOS events (7 and 12) select the same candle (index 6) as an OB candidate → same range.
DUP_EVENTS = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]

# Price crosses the zone on close after the OB → breaker.
BREAKER = DUP_EVENTS[:8] + [
    (102, 102.5, 101.5, 102),
    (101.5, 102, 100.4, 100.3),  # close 100.3 < OB.low 100.5 → breaker
    (101, 101.5, 100.5, 101),
]


def mk(rows):
    return [
        {"open_time": i * 100, "open": o, "high": h, "low": l, "close": c, "volume": 10.0}
        for i, (o, h, l, c) in enumerate(rows)
    ]


# ---------------------------------------------------------------------------
# S1 — freshness
# ---------------------------------------------------------------------------


def test_s1_freshness_strict_no_period_tolerance():
    """A snapshot one period older than the last closed candle is no longer fresh (2.15)."""
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    # Latest: analysis includes the last closed candle → fresh.
    assert PAEngine.freshness_for(TF, latest_closed) == FRESHNESS_FRESH
    # One period behind: last closed candle is missing → stale (previously fresh with tolerance).
    assert PAEngine.freshness_for(TF, latest_closed - PERIOD) == FRESHNESS_STALE
    # Two periods behind: still stale.
    assert PAEngine.freshness_for(TF, latest_closed - 2 * PERIOD) == FRESHNESS_STALE
    assert PAEngine.freshness_for(TF, None) == FRESHNESS_STALE


async def test_s1_pa_meta_carries_freshness_note(db, cfg):
    """PA tool meta carries a note explaining freshness semantics."""
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    times = [latest_closed - (len(DUP_EVENTS) - 1 - i) * PERIOD for i in range(len(DUP_EVENTS))]

    def _w(conn):
        for i, (o, h, l, c) in enumerate(DUP_EVENTS):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("BTCUSDT", TF, times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)
    ctx = {"db": db, "config": cfg, "readiness": None, "pipeline": None, "started_at": time.time()}
    dispatcher = build_dispatcher(ctx)
    data, meta = await dispatcher.dispatch("get_full_analysis", {"symbol": "BTCUSDT", "timeframe": TF}, ctx)
    assert meta.freshness == FRESHNESS_FRESH  # fresh candle data
    assert "freshness_note" in meta.to_dict()
    assert "last closed candle" in meta.to_dict()["freshness_note"]


async def test_s1_klineservice_freshness_strict(db, cfg):
    """KlineService.freshness_for also has no extra period tolerance."""
    from tests.helpers import FakeClock, FakeRest
    from rasattrading_mcp.data.klines import KlineService
    from rasattrading_mcp.data.universe import UniverseService

    fake = FakeRest(["BTCUSDT"])
    uni = UniverseService(fake, cfg)
    svc = KlineService(fake, fake, db, uni, cfg, clock=FakeClock())
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    fresh_rows = [{"open_time": latest_closed - 5 * PERIOD}, {"open_time": latest_closed}]
    stale_rows = [{"open_time": latest_closed - 5 * PERIOD}, {"open_time": latest_closed - PERIOD}]
    assert svc.freshness_for("BTCUSDT", TF, fresh_rows) == FRESHNESS_FRESH
    assert svc.freshness_for("BTCUSDT", TF, stale_rows) == FRESHNESS_STALE


# ---------------------------------------------------------------------------
# S2 — mitigation weighting
# ---------------------------------------------------------------------------


def test_s2_equal_levels_weighted_by_active_zones():
    """Seven of 10 zones are mitigated → no full score (40); use active count."""
    zones = [{"kind": "equal_highs", "mitigated": i < 7} for i in range(10)]
    sc = liquidity_score(zones, None)
    eq = sc["components"]["equal_levels"]
    assert eq["zones"] == 10
    assert eq["active_zones"] == 3
    assert eq["mitigated_zones"] == 7
    assert eq["points"] == 12.0  # 3/10 * 40
    assert "points are based on active zone count" in eq["note"]


def test_s2_all_mitigated_scores_zero():
    sc = liquidity_score([{"kind": "equal_highs", "mitigated": True}], None)
    eq = sc["components"]["equal_levels"]
    assert eq["active_zones"] == 0
    assert eq["points"] == 0.0


# ---------------------------------------------------------------------------
# S3 — funding direction
# ---------------------------------------------------------------------------


def test_s3_funding_bias_long_crowded():
    futures = {"funding_rate": {"freshness": "fresh", "value": 0.0005}}
    sc = liquidity_score([], futures)
    comp = sc["components"]["funding_rate"]
    assert comp["bias"] == "long_crowded"
    assert sc["funding_bias"] == "long_crowded"


def test_s3_funding_bias_short_crowded():
    futures = {"funding_rate": {"freshness": "fresh", "value": -0.0005}}
    sc = liquidity_score([], futures)
    assert sc["components"]["funding_rate"]["bias"] == "short_crowded"
    assert sc["funding_bias"] == "short_crowded"


def test_s3_funding_stale_no_bias():
    futures = {"funding_rate": {"freshness": "stale", "value": 0.0005}}
    sc = liquidity_score([], futures)
    assert "bias" not in sc["components"]["funding_rate"]
    assert "funding_bias" not in sc


# ---------------------------------------------------------------------------
# S4 — breaker → mitigated
# ---------------------------------------------------------------------------


def test_s4_breaker_is_mitigated():
    st = detect_structure(mk(BREAKER))
    res = compute_order_blocks(mk(BREAKER), st)
    ob = next(o for o in res["order_blocks"] if o["event_index"] == 7)
    assert ob["zone_type"] == "breaker"
    assert ob["mitigated"] is True  # Broken on close → not a valid active zone.
    # Breaker must not appear in the default (active) view—visible with include_mitigated.
    active = [o for o in res["order_blocks"] if not o["mitigated"]]
    assert all(o["zone_type"] != "breaker" for o in active)


# ---------------------------------------------------------------------------
# S5 — OB dedup
# ---------------------------------------------------------------------------


def test_s5_duplicate_events_same_candle_collapse():
    """Two BOS events selecting the same candle still produce one logical OB."""
    st = detect_structure(mk(DUP_EVENTS))
    assert [e["type"] for e in st["events"]] == ["bos_bullish", "bos_bullish"]
    res = compute_order_blocks(mk(DUP_EVENTS), st)
    obs = res["order_blocks"]
    assert len(obs) == 1  # Previously produced two separate records.
    assert obs[0]["range"] == {"low": 100.5, "high": 101.5}
    assert obs[0]["event_index"] == 7  # Preserve the first (earliest) record.


def test_s5_distinct_ranges_not_merged():
    """OBs with different price ranges are not merged."""
    rows = DUP_EVENTS[:8] + [
        (102, 102.5, 101.6, 102),     # Price does not return to first OB → remains active.
        (102, 102.5, 101.6, 102),
        (102.5, 103.5, 102, 103),
        (103.5, 103.0, 101.8, 102.6),  # Red candle → new OB candidate (different range).
        (103, 105, 102.4, 104.5),      # bos_bullish@12 → selects this candle as OB.
        (104, 104.5, 103.5, 104),
    ]
    st = detect_structure(mk(rows))
    res = compute_order_blocks(mk(rows), st)
    obs = res["order_blocks"]
    # Two different price ranges → dedup does not merge them.
    assert len(obs) == 2
    ranges = {o["range"]["low"] for o in obs}
    assert 100.5 in ranges and 101.8 in ranges


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


@pytest.fixture
async def db(cfg):
    d = Database(cfg.db_path)
    await d.start()
    await run_migrations(d)
    yield d
    await d.stop()
