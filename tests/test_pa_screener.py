"""2.5 — Screener (scan_market): filtre AST güvenliği + değerlendirme."""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.screener import Screener, validate_filters
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

BASE = 1_700_000_000
TF = "1h"

UPTREND = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]

EQ_SWEEP = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 101, 99.5, 100.5), (100.5, 100.5, 100, 100.5),
    (100.5, 100.5, 100, 100.5), (100, 101.04, 99.5, 100.5), (100.5, 100.5, 100, 100.5),
    (100.5, 100.5, 100, 100.5), (100.5, 102, 100.5, 101.5),
]

FALLING = [(105 - i * 0.5, 105.5 - i * 0.5, 104.5 - i * 0.5, 105 - i * 0.5) for i in range(15)]


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


async def seed(db, symbol, rows, vol=10.0):
    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, BASE + i * 3600, o, h, l, c, vol, "spot", int(time.time())),
            )

    await db.write(_w)


async def seed_all(db):
    await seed(db, "BTCUSDT", UPTREND)
    await seed(db, "ETHUSDT", EQ_SWEEP)
    await seed(db, "SOLUSDT", FALLING)


# ---------------------------------------------------------------------------
# AST güvenliği (enjeksiyon reddi)
# ---------------------------------------------------------------------------


def test_validate_unknown_type_rejected():
    with pytest.raises(Exception):
        validate_filters([{"type": "DROP TABLE candles"}])
    with pytest.raises(Exception):
        validate_filters([{"type": "price_change", "sql": "1=1"}])


def test_validate_unknown_key_rejected():
    with pytest.raises(Exception):
        validate_filters([{"type": "price_change", "window_bars": 10, "evil": "x"}])
    with pytest.raises(Exception):
        validate_filters([{"type": "near_order_block", "max_distance_pct": 1, "payload": "x"}])


def test_validate_bad_values_rejected():
    with pytest.raises(Exception):
        validate_filters([{"type": "structure_event", "event": "not_an_event"}])
    with pytest.raises(Exception):
        validate_filters([{"type": "above_below_vwap", "position": "sideways"}])
    with pytest.raises(Exception):
        validate_filters([{"type": "price_change", "min": "big"}])
    with pytest.raises(Exception):
        validate_filters([{"type": "and", "filters": []}])
    with pytest.raises(Exception):
        validate_filters([], combine="XOR")


def test_validate_defaults_applied():
    node = validate_filters([{"type": "price_change", "min": 2}])["filters"][0]
    assert node["window_bars"] == 24
    assert node["min"] == 2


def test_validate_nested_and_or():
    root = validate_filters(
        [{"type": "or", "filters": [{"type": "price_change", "min": 1}, {"type": "volume_change", "min": 5}]}]
    )
    assert root["type"] == "and"
    assert root["filters"][0]["type"] == "or"


# ---------------------------------------------------------------------------
# Değerlendirme
# ---------------------------------------------------------------------------


async def test_scan_price_change(db):
    await seed_all(db)
    screener = Screener(db)
    res = await screener.scan([{"type": "price_change", "window_bars": 10, "min": 2}])
    syms = [s["symbol"] for s in res["symbols"]]
    assert "BTCUSDT" in syms  # %2.5 artış
    assert "SOLUSDT" not in syms  # düşüyor
    assert res["combine"] == "AND"
    assert res["freshness"] == "stale"  # eski damgalı veri


async def test_scan_liquidity_sweep(db):
    await seed_all(db)
    screener = Screener(db)
    res = await screener.scan([{"type": "liquidity_sweep_occurred"}])
    syms = [s["symbol"] for s in res["symbols"]]
    assert "ETHUSDT" in syms  # eşit high sweep edildi
    assert "BTCUSDT" not in syms


async def test_scan_structure_event(db):
    await seed_all(db)
    screener = Screener(db)
    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish"}])
    syms = [s["symbol"] for s in res["symbols"]]
    assert "BTCUSDT" in syms


async def test_scan_near_order_block(db):
    await seed_all(db)
    screener = Screener(db)
    res = await screener.scan([{"type": "near_order_block", "max_distance_pct": 2.0}])
    syms = [s["symbol"] for s in res["symbols"]]
    assert "BTCUSDT" in syms  # aktif OB'ye yakın
    assert "ETHUSDT" not in syms  # OB yok


async def test_scan_above_below_vwap(db):
    await seed_all(db)
    screener = Screener(db)
    above = await screener.scan([{"type": "above_below_vwap", "position": "above"}])
    below = await screener.scan([{"type": "above_below_vwap", "position": "below"}])
    above_syms = {s["symbol"] for s in above["symbols"]}
    below_syms = {s["symbol"] for s in below["symbols"]}
    assert "BTCUSDT" in above_syms
    assert "SOLUSDT" in below_syms
    assert not (above_syms & below_syms)


async def test_scan_funding_rate_fresh_only(db):
    await seed_all(db)

    def _w(conn):
        conn.executemany(
            "INSERT INTO futures_context (symbol, type, event_time, value, fetched_at, freshness) VALUES (?,?,?,?,?,?)",
            [
                ("BTCUSDT", "funding_rate", BASE, 0.0005, BASE, "fresh"),
                ("ETHUSDT", "funding_rate", BASE, 0.0009, BASE, "stale"),
            ],
        )

    await db.write(_w)
    screener = Screener(db)
    res = await screener.scan([{"type": "funding_rate", "min": 0.0001, "max": 0.001}])
    syms = [s["symbol"] for s in res["symbols"]]
    assert "BTCUSDT" in syms  # fresh
    assert "ETHUSDT" not in syms  # stale skora katılmaz


async def test_scan_combine_or(db):
    await seed_all(db)
    screener = Screener(db)
    res = await screener.scan(
        [{"type": "structure_event", "event": "bos_bullish"}, {"type": "above_below_vwap", "position": "below"}],
        combine="OR",
    )
    syms = [s["symbol"] for s in res["symbols"]]
    assert "BTCUSDT" in syms  # bos_bullish
    assert "SOLUSDT" in syms  # below vwap


async def test_scan_pagination(db):
    await seed_all(db)
    screener = Screener(db)
    filters = [
        {"type": "above_below_vwap", "position": "below"},
        {"type": "structure_event", "event": "bos_bullish"},
    ]
    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        res = await screener.scan(filters, combine="OR", limit=1, cursor=cursor)
        seen.append(res["symbols"][0]["symbol"])
        pages += 1
        if res["next_cursor"] is None:
            break
        cursor = res["next_cursor"]
    assert pages > 1
    assert len(seen) == len(set(seen))  # sayfalar örtüşmez


async def test_scan_stale_marking(db):
    await seed_all(db)
    screener = Screener(db)
    res = await screener.scan([{"type": "price_change", "window_bars": 10, "min": 2}])
    btc = next(s for s in res["symbols"] if s["symbol"] == "BTCUSDT")
    assert btc["data_stale"] is True  # eski damgalı mumlar → açıkça işaretli


async def test_scan_and_or_nested(db):
    await seed_all(db)
    screener = Screener(db)
    res = await screener.scan(
        [{"type": "or", "filters": [{"type": "structure_event", "event": "bos_bullish"}, {"type": "liquidity_sweep_occurred"}]}]
    )
    syms = [s["symbol"] for s in res["symbols"]]
    assert "BTCUSDT" in syms
    assert "ETHUSDT" in syms
