"""2.18 FIX — PA worker separates permanently insufficient data from cycle blocking.

Live observation: tokenized stock pairs (SMCIBUSDT, ALABBUSDT, ...)
have only ~2 closed candles on 1d; when `analyze` raised `STALE_DATA`, the
worker reprocessed all 489 symbols every cycle (1d cycles repeated about every
25 seconds) because `_last_processed` could never advance.

Fix: `STALE_DATA` (insufficient closed candles) counts as `"insufficient"`—it
does not block the cycle and is retried when a new closed bar arrives. Transient
states (stale data, error, empty) preserve 2.12 behavior: block the cycle and retry.
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.analysis import PAEngine, _read_history
from rasattrading_mcp.pa.worker import PAWorker
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1d"
PERIOD = 86400
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
    return Config(data_dir=tmp_path, pipeline_enabled=False, kline_intervals=(TF,), pa_worker_concurrency=2)


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
                "VALUES (?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(symbol, timeframe, open_time, source) DO UPDATE SET "
                "open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, "
                "volume=excluded.volume, updated_at=excluded.updated_at",
                (symbol, TF, open_times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


async def test_insufficient_symbol_does_not_block_round(db, cfg):
    """A mixed healthy + insufficient-data cycle completes; skip the insufficient symbol."""
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
    await seed_at(db, "SMCIBUSDT", UPTREND[:2], _fresh_times(2))  # 2 bar < 2*SWING_LOOKBACK+1
    worker = PAWorker(PAEngine(db), FakeUniverse(["BTCUSDT", "SMCIBUSDT"]), cfg)

    assert await worker.check_and_process() == 1  # Only BTCUSDT counts as successful.
    assert worker._last_processed.get(TF) is not None  # Cycle still completed.
    assert len(await _read_history(db, "market_structure", "BTCUSDT", TF)) == 1
    assert await _read_history(db, "market_structure", "SMCIBUSDT", TF) == []


async def test_all_symbols_insufficient_still_advances(db, cfg):
    """A universe with only insufficient data does not block the cycle (repeated 489-symbol cycles end)."""
    await seed_at(db, "SMCIBUSDT", UPTREND[:2], _fresh_times(2))
    worker = PAWorker(PAEngine(db), FakeUniverse(["SMCIBUSDT"]), cfg)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is not None

    # Do not repeat the cycle for the same closed bar (marker advanced).
    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is not None


async def test_insufficient_symbol_retried_when_new_bar_arrives(db, cfg):
    """Retry an insufficient symbol when a new closed bar arrives (not a permanent skip)."""
    await seed_at(db, "SMCIBUSDT", UPTREND[:2], _fresh_times(2))
    worker = PAWorker(PAEngine(db), FakeUniverse(["SMCIBUSDT"]), cfg)
    assert await worker.check_and_process() == 0
    first_marker = worker._last_processed.get(TF)
    assert first_marker is not None

    # Third closed bar added (still < 5, still insufficient) → cycle starts because it is new.
    times = _fresh_times(3)
    await seed_at(db, "SMCIBUSDT", UPTREND[:3], times)
    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) == times[2]  # new marker


async def test_stale_insufficient_still_blocks_round(db, cfg):
    """Preserve 2.12: if insufficient data is STALE (has not reached target bar), block the cycle."""
    await seed_at(db, "SMCIBUSDT", UPTREND[:2], [OLD_BASE + i * PERIOD for i in range(2)])
    worker = PAWorker(PAEngine(db), FakeUniverse(["SMCIBUSDT"]), cfg)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is None  # stale → retry beklenir
