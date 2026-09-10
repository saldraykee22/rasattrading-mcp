"""2.10 FIX — futures series ASC/LIMIT (latest record) + stale aging.

Review evidence is converted into tests:
- `load_futures_series(limit=1)` must return the latest record (previously oldest, event_time=100).
- An old futures record must become `stale` over time through `age_stale_rows`.
- Alert/screener OI change must match the latest series, not be reduced to one sample.
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.futures import FuturesContextPoller
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.alarms import AlarmService
from rasattrading_mcp.pa.liquidity import load_futures_series
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1h"
PERIOD = 3600

UPTREND = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
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


async def _seed_futures(db, symbol, ftype, times_values, fetched_at=None):
    now = fetched_at if fetched_at is not None else int(time.time())

    def _w(conn):
        conn.executemany(
            "INSERT INTO futures_context (symbol, type, event_time, value, fetched_at, freshness) VALUES (?,?,?,?,?,?)",
            [(symbol, ftype, t, v, now, "fresh") for t, v in times_values],
        )

    await db.write(_w)


async def _seed_candles(db, symbol, rows):
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD

    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, latest_closed - (len(rows) - 1 - i) * PERIOD, o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


# ---------------------------------------------------------------------------
# Series ordering
# ---------------------------------------------------------------------------


async def test_series_limit_1_returns_latest(db):
    """Review evidence: limit=1 returned the OLDEST record (event_time=100)."""
    await _seed_futures(db, "BTCUSDT", "open_interest", [(100, 10.0), (200, 20.0), (300, 30.0)])
    series = await load_futures_series(db, "BTCUSDT", "open_interest", limit=1)
    assert len(series) == 1
    assert series[0]["event_time"] == 300  # newest


async def test_series_limit_n_latest_chronological(db):
    await _seed_futures(db, "BTCUSDT", "open_interest", [(100, 10.0), (200, 20.0), (300, 30.0), (400, 40.0)])
    series = await load_futures_series(db, "BTCUSDT", "open_interest", limit=2)
    assert [s["event_time"] for s in series] == [300, 400]  # latest two, chronological


async def test_series_all_chronological(db):
    await _seed_futures(db, "BTCUSDT", "open_interest", [(100, 10.0), (200, 20.0), (300, 30.0)])
    series = await load_futures_series(db, "BTCUSDT", "open_interest")
    assert [s["event_time"] for s in series] == [100, 200, 300]


# ---------------------------------------------------------------------------
# Stale aging
# ---------------------------------------------------------------------------


async def test_stale_aging_marks_old_rows(db, cfg):
    now = int(time.time())
    await _seed_futures(db, "BTCUSDT", "open_interest", [(100, 10.0)], fetched_at=now - 7200)  # 2 hours ago.
    await _seed_futures(db, "BTCUSDT", "open_interest", [(200, 20.0)], fetched_at=now)
    from tests.helpers import FakeRest

    poller = FuturesContextPoller(FakeRest(["BTCUSDT"]), db, None, cfg)
    aged = await poller.age_stale_rows(now)
    assert aged == 1  # Only the old row aged.
    series = await load_futures_series(db, "BTCUSDT", "open_interest")
    by_ts = {s["event_time"]: s["freshness"] for s in series}
    assert by_ts[100] == "stale"
    assert by_ts[200] == "fresh"


async def test_fresh_rows_not_aged(db, cfg):
    from tests.helpers import FakeRest

    now = int(time.time())
    await _seed_futures(db, "BTCUSDT", "open_interest", [(200, 20.0)], fetched_at=now)
    poller = FuturesContextPoller(FakeRest(["BTCUSDT"]), db, None, cfg)
    assert await poller.age_stale_rows(now) == 0


# ---------------------------------------------------------------------------
# Alert OI change can now match the series
# ---------------------------------------------------------------------------


async def test_alarm_oi_change_triggers_with_series(db, cfg):
    """Alert OI change uses the latest fresh series (not reduced to one sample)."""
    await _seed_candles(db, "BTCUSDT", UPTREND)
    await _seed_futures(db, "BTCUSDT", "open_interest", [(100, 10.0), (200, 30.0)])  # 200% increase.
    engine = PAEngine(db)
    alarms = AlarmService(db, engine=engine)
    engine.alarm_service = alarms
    await alarms.create_alert("BTCUSDT", TF, [{"type": "oi_change", "min": 100, "window": 1}], cooldown_seconds=0)
    await engine.analyze("BTCUSDT", TF)
    rec = await alarms.get_triggered_alerts()
    assert len(rec["triggered"]) == 1
