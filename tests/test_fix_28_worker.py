"""2.8 FIX — background PA worker: automatic recalculation at bar close.

Review evidence is converted into tests:
- When a timeframe closes, generate/update the PA record automatically without an agent tool call.
- Do not recalculate (or trigger) while candle data is stale.
"""

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


def _worker(db, cfg):
    return PAWorker(PAEngine(db), FakeUniverse(["BTCUSDT"]), cfg)


async def test_worker_produces_pa_without_agent_call(db, cfg):
    """Worker creates the PA record without an agent `get_market_structure` call."""
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
    worker = _worker(db, cfg)
    processed = await worker.check_and_process()
    assert processed == 1
    rows = await _read_history(db, "market_structure", "BTCUSDT", TF)
    assert len(rows) == 1
    assert rows[0]["effective_to"] is None


async def test_worker_skips_stale_data(db, cfg):
    """Do not recalculate while candle data is stale."""
    await seed_at(db, "BTCUSDT", UPTREND, [OLD_BASE + i * PERIOD for i in range(len(UPTREND))])
    worker = _worker(db, cfg)
    processed = await worker.check_and_process()
    assert processed == 0
    assert await _read_history(db, "market_structure", "BTCUSDT", TF) == []


async def test_worker_recomputes_on_new_closed_bar(db, cfg, monkeypatch):
    """New closed bar → PA record for the same symbol updates automatically (old record closes)."""
    import time as _time

    now0 = int(time.time())
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND), now0))
    worker = _worker(db, cfg)
    assert await worker.check_and_process() == 1

    now1 = now0 + PERIOD
    monkeypatch.setattr(_time, "time", lambda: now1)
    await seed_at(db, "BTCUSDT", [(104.5, 106, 104, 105.5)], _fresh_times(1, now1))

    processed = await worker.check_and_process()
    assert processed == 1
    rows = await _read_history(db, "market_structure", "BTCUSDT", TF)
    assert len(rows) == 2
    assert rows[0]["effective_to"] == rows[1]["effective_from"] - 1
    assert rows[1]["effective_to"] is None


async def test_worker_does_not_duplicate_same_bar(db, cfg):
    """Do not create a new record on the second cycle for the same bar (idempotent)."""
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
    worker = _worker(db, cfg)
    assert await worker.check_and_process() == 1
    assert await worker.check_and_process() == 0  # No new closed bar.
    assert len(await _read_history(db, "market_structure", "BTCUSDT", TF)) == 1
