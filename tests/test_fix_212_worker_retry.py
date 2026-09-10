"""2.12 FIX — PA worker retry: marker advances after processing completes.

Review evidence is converted into tests (H1, M1):
- After stale/failed/empty calculation, `_last_processed` does not advance; the
  same closed bar is retried on the next cycle.
- Warm-up/backfill candle loading is also included in the `pa_worker_concurrency` semaphore.
"""

import asyncio
import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.analysis import PAEngine, _read_history
from rasattrading_mcp.pa.worker import PAWorker
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1h"
PERIOD = 3600
OLD_BASE = 1_700_000_000

UPTREND = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]


class FakeUniverse:
    def __init__(self, symbols):
        self._symbols = symbols

    def snapshot(self):
        return list(self._symbols)


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False, kline_intervals=("1h",), pa_worker_concurrency=2)


@pytest.fixture
async def db(cfg):
    d = Database(cfg.db_path)
    await d.start()
    await run_migrations(d)
    yield d
    await d.stop()


def _fresh_times(n, now=None):
    now = now if now is not None else time.time()
    latest_closed = int(now // PERIOD) * PERIOD - PERIOD
    return [latest_closed - (n - 1 - i) * PERIOD for i in range(n)]


async def seed_at(db, symbol, rows, open_times):
    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, open_times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


async def test_worker_retries_failed_analysis_same_bar(db, cfg, monkeypatch):
    """H1: if analyze gets a transient error, marker does not advance; retry the same bar next cycle."""
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
    worker = PAWorker(PAEngine(db), FakeUniverse(["BTCUSDT"]), cfg)

    real_analyze = PAEngine.analyze
    calls = {"n": 0}

    async def flaky_analyze(self, symbol, timeframe, lookback=200):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        return await real_analyze(self, symbol, timeframe, lookback)

    monkeypatch.setattr(PAEngine, "analyze", flaky_analyze)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is None

    assert await worker.check_and_process() == 1
    assert worker._last_processed.get(TF) is not None
    assert len(await _read_history(db, "market_structure", "BTCUSDT", TF)) == 1


async def test_worker_retries_stale_bar_when_data_catches_up(db, cfg):
    """H1: stale candle cycle does not advance marker; process when data reaches the same closed bar."""
    await seed_at(db, "BTCUSDT", UPTREND, [OLD_BASE + i * PERIOD for i in range(len(UPTREND))])
    worker = PAWorker(PAEngine(db), FakeUniverse(["BTCUSDT"]), cfg)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is None
    assert await _read_history(db, "market_structure", "BTCUSDT", TF) == []

    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
    assert await worker.check_and_process() == 1
    assert worker._last_processed.get(TF) is not None
    assert len(await _read_history(db, "market_structure", "BTCUSDT", TF)) == 1


async def test_worker_retries_empty_universe_until_symbols_appear(db, cfg):
    """H1: marker does not advance with an empty universe; process the same closed bar when a symbol is added."""
    universe = FakeUniverse([])
    worker = PAWorker(PAEngine(db), universe, cfg)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is None

    universe._symbols = ["BTCUSDT"]
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
    assert await worker.check_and_process() == 1
    assert worker._last_processed.get(TF) is not None


async def test_worker_load_is_semaphore_limited(db, cfg, monkeypatch):
    """M1: warm-up/backfill candle loading is also included in the concurrency limit."""
    symbols = [f"SYM{i}" for i in range(6)]
    for s in symbols:
        await seed_at(db, s, UPTREND, _fresh_times(len(UPTREND)))

    real_load = PAEngine._load_candles
    state = {"active": 0, "max": 0}

    async def tracking_load(self, symbol, timeframe, lookback):
        state["active"] += 1
        state["max"] = max(state["max"], state["active"])
        await asyncio.sleep(0.02)
        try:
            return await real_load(self, symbol, timeframe, lookback)
        finally:
            state["active"] -= 1

    monkeypatch.setattr(PAEngine, "_load_candles", tracking_load)
    worker = PAWorker(PAEngine(db), FakeUniverse(symbols), cfg)
    assert await worker.check_and_process() == len(symbols)
    assert state["max"] <= cfg.pa_worker_concurrency
