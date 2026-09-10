"""2.4 — PA engine (immutable records) + annotation + handler integration."""

import json
import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.pa.analysis import PAEngine, _read_history
from rasattrading_mcp.pa.annotations import AnnotationService
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

BASE = 1_700_000_000
TF = "1h"

# UPTREND-like sequence producing bos_bullish events.
UPTREND = [
    (100, 100.5, 99.5, 100),
    (100, 100.5, 99.5, 100),
    (99, 100, 98, 99.5),          # swing low 98
    (99.5, 100.5, 99, 100),
    (100, 102, 99.5, 101),        # swing high 102
    (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5),   # OB candidate (red).
    (100.5, 103, 101, 102.5),     # bos_bullish@7
    (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102),
    (102.5, 103.5, 102, 103),
    (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5),     # bos_bullish@12
    (104, 104.5, 103.5, 104),
    (104, 104.5, 103.5, 103.5),
]

# Equal highs + sweep (liquidity zone becomes mitigated).
EQ_SWEEP = [
    (100, 100.5, 99.5, 100),
    (100, 100.5, 99.5, 100),
    (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100),
    (100, 101, 99.5, 100.5),      # swing high 101.00
    (100.5, 100.5, 100, 100.5),
    (100.5, 100.5, 100, 100.5),
    (100, 101.04, 99.5, 100.5),   # swing high 101.04
    (100.5, 100.5, 100, 100.5),
    (100.5, 100.5, 100, 100.5),
    (100.5, 102, 100.5, 101.5),   # sweep → mitigated
]


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


async def seed_candles(db, symbol, rows, offset=0):
    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, BASE + (offset + i) * 3600, o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


async def test_analyze_stores_immutable_records(db):
    await seed_candles(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    result = await engine.analyze("BTCUSDT", TF)

    assert result["structure"]["trend"] == "up"
    assert [e["type"] for e in result["structure"]["events"]] == ["bos_bullish", "bos_bullish"]
    assert result["liquidity"]["score"]["algo_version"] == "liquidity-v1"
    assert result["order_blocks"]["algo_version"] == "obfvg-v1"
    assert result["vwap"]["current"] is not None
    assert result["sessions"]["timezone"] == "UTC"

    rows = await _read_history(db, "market_structure", "BTCUSDT", TF)
    assert len(rows) == 1
    assert rows[0]["effective_to"] is None


async def test_analyze_recompute_creates_history(db):
    await seed_candles(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    await engine.analyze("BTCUSDT", TF)
    # Recalculate after a new bar closes → old record closes.
    await seed_candles(db, "BTCUSDT", UPTREND, offset=len(UPTREND))
    await engine.analyze("BTCUSDT", TF)

    rows = await _read_history(db, "market_structure", "BTCUSDT", TF)
    assert len(rows) == 2
    assert rows[0]["effective_to"] == rows[1]["effective_from"] - 1
    assert rows[1]["effective_to"] is None

    # Repeat for same bar → no new row, update existing.
    await engine.analyze("BTCUSDT", TF)
    rows2 = await _read_history(db, "market_structure", "BTCUSDT", TF)
    assert len(rows2) == 2


async def test_get_market_structure(db):
    await seed_candles(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    data = await engine.get_market_structure("BTCUSDT", TF)
    assert data["as_of"] == BASE + (len(UPTREND) - 1) * 3600
    assert len(data["structure"]["swings"]) >= 3


async def test_liquidity_zones_default_excludes_mitigated(db):
    await seed_candles(db, "BTCUSDT", EQ_SWEEP)
    engine = PAEngine(db)
    data = await engine.get_liquidity_zones("BTCUSDT", TF)
    assert data["zones"] == []  # One zone was mitigated → default active list is empty.

    full = await engine.get_liquidity_zones("BTCUSDT", TF, include_mitigated=True)
    assert len(full["zones"]) == 1
    assert full["zones"][0]["mitigated"] is True


async def test_order_blocks_default_and_history(db):
    await seed_candles(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    data = await engine.get_order_blocks("BTCUSDT", TF)
    # In UPTREND, two BOS events (7 and 12) select the same candle as an OB
    # candidate → dedup reduces the same price range to one logical zone (2.15);
    # because this zone is also swept, the default (active) list is empty.
    assert data["order_blocks"] == []

    full = await engine.get_order_blocks("BTCUSDT", TF, include_mitigated=True)
    assert len(full["order_blocks"]) == 1  # One logical zone appears in history.
    assert full["order_blocks"][0]["mitigated"] is True


async def test_get_full_analysis_reasonable_size(db):
    await seed_candles(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    data = await engine.get_full_analysis("BTCUSDT", TF)
    assert {"structure", "liquidity", "order_blocks", "vwap", "sessions"} <= set(data)
    assert len(data["vwap"]["points"]) <= 20  # Keep context compact.
    assert data["liquidity"]["score"]["futures_available"] is False


async def test_annotations_crud(db):
    svc = AnnotationService(db)
    ids = await svc.annotate("BTCUSDT", TF, [{"level": 100.5, "label": "support", "kind": "level"}], created_by="agent-a")
    assert len(ids) == 1
    ids2 = await svc.annotate("BTCUSDT", TF, {"level": 103.0, "label": "resistance"}, created_by="agent-a")
    assert ids2[0] != ids[0]

    items = await svc.get("BTCUSDT", TF)
    assert len(items) == 2
    assert items[0]["data"]["label"] == "support"

    other = await svc.get("BTCUSDT", "4h")
    assert other == []

    removed = await svc.clear("BTCUSDT", TF)
    assert removed == 2
    assert await svc.get("BTCUSDT", TF) == []


async def test_annotate_validation(db):
    svc = AnnotationService(db)
    with pytest.raises(Exception):
        await svc.annotate("", TF, [{"x": 1}])
    with pytest.raises(Exception):
        await svc.annotate("BTCUSDT", TF, ["not-a-dict"])


async def test_handler_dispatch(db, cfg):
    ctx = {"db": db, "config": cfg, "readiness": None, "pipeline": None, "started_at": time.time()}
    dispatcher = build_dispatcher(ctx)

    await seed_candles(db, "BTCUSDT", UPTREND)
    data, meta = await dispatcher.dispatch(
        "get_market_structure", {"symbol": "BTCUSDT", "timeframe": TF}, ctx
    )
    assert data["structure"]["trend"] == "up"
    assert meta.freshness in ("fresh", "stale")

    ann, ann_meta = await dispatcher.dispatch(
        "annotate_chart", {"symbol": "BTCUSDT", "timeframe": TF, "annotations": [{"x": 1}]}, ctx
    )
    assert len(ann["annotation_ids"]) == 1
    got, _ = await dispatcher.dispatch("get_chart_annotations", {"symbol": "BTCUSDT", "timeframe": TF}, ctx)
    assert len(got["annotations"]) == 1
