"""2.13 POLISH — M2 immutable validation + M5 algo_version + K2 alert delay + K3 warm-up budget.

Review findings (integration-glue-review M2/M5/K1/K2/K3) are converted into tests:
- M2: when algo_version changes for the same bar, close the old revision with effective_to.
- M5: PA tool meta carries the real `algo_version` (not None).
- K2: alert loop performs its first evaluation BEFORE sleep (no 30s delay).
- K3: when no stored analysis exists, on-demand `analyze` is limited by per-cycle budget.
"""

import asyncio
import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.pa.analysis import PAEngine, _read_history, _store_payload
from rasattrading_mcp.pa.alarms import AlarmService
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


async def seed(db, symbol, rows):
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    times = [latest_closed - (len(rows) - 1 - i) * PERIOD for i in range(len(rows))]

    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


# ---------------------------------------------------------------------------
# M2 — immutable validation (analysis.py _store_payload)
# ---------------------------------------------------------------------------


async def test_m2_same_bar_version_change_closes_old(db):
    """When algo_version changes at the same effective_from, keep the old revision in history."""
    await _store_payload(db, "market_structure", "BTCUSDT", TF, "swing-v1", 300, {"a": 1})
    await _store_payload(db, "market_structure", "BTCUSDT", TF, "swing-v2", 300, {"a": 2})
    rows = await _read_history(db, "market_structure", "BTCUSDT", TF)
    assert len(rows) == 2  # revision korunur
    closed = [r for r in rows if r["effective_to"] is not None]
    open_rows = [r for r in rows if r["effective_to"] is None]
    assert len(closed) == 1
    assert closed[0]["algo_version"] == "swing-v1"
    assert closed[0]["effective_to"] == 300  # Point range, no overwrite.
    assert len(open_rows) == 1
    assert open_rows[0]["algo_version"] == "swing-v2"


# ---------------------------------------------------------------------------
# M5 — meta.algo_version carries the real value
# ---------------------------------------------------------------------------


async def test_m5_pa_meta_carries_real_algo_version(db, cfg):
    await seed(db, "BTCUSDT", UPTREND)
    ctx = {"db": db, "config": cfg, "readiness": None, "pipeline": None, "started_at": time.time()}
    dispatcher = build_dispatcher(ctx)

    cases = [
        ("get_market_structure", {"symbol": "BTCUSDT", "timeframe": TF}, "swing-v1"),
        ("get_liquidity_zones", {"symbol": "BTCUSDT", "timeframe": TF}, "liquidity-v1"),
        ("get_order_blocks", {"symbol": "BTCUSDT", "timeframe": TF}, "obfvg-v1"),
        ("get_full_analysis", {"symbol": "BTCUSDT", "timeframe": TF}, "swing-v1,liquidity-v1,obfvg-v1,vwap-v1,session-v1"),
    ]
    for tool, params, expected in cases:
        data, meta = await dispatcher.dispatch(tool, params, ctx)
        assert meta.algo_version == expected, f"{tool}: beklenen {expected}, gelen {meta.algo_version}"
        assert data["algo_version"] == expected


# ---------------------------------------------------------------------------
# K2 — alert loop evaluates before sleep
# ---------------------------------------------------------------------------


async def test_k2_alarm_loop_evaluates_before_first_sleep(cfg):
    from rasattrading_mcp.daemon.main import DaemonRunner

    long_cfg = Config(data_dir=cfg.data_dir, pipeline_enabled=False, alarm_eval_seconds=300)

    class StubAlarm:
        def __init__(self):
            self.passes = []

        def begin_evaluation_pass(self):
            self.passes.append("begin")

        async def alert_symbols(self):
            self.passes.append("symbols")
            return [("BTCUSDT", TF)]

        async def evaluate_symbol(self, symbol, timeframe):
            self.passes.append(("eval", symbol))

    runner = DaemonRunner(long_cfg)
    runner.alarm_service = StubAlarm()
    task = asyncio.create_task(runner._alarm_eval_loop())
    deadline = time.time() + 5
    try:
        while time.time() < deadline:
            if ("eval", "BTCUSDT") in runner.alarm_service.passes:
                break
            await asyncio.sleep(0.01)
        # First cycle is immediate because evaluation occurs before the 300s sleep.
        assert "begin" in runner.alarm_service.passes
        assert ("eval", "BTCUSDT") in runner.alarm_service.passes
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# ---------------------------------------------------------------------------
# K3 — on-demand PA calculation is limited by per-cycle budget
# ---------------------------------------------------------------------------


async def test_k3_compute_budget_limits_on_demand_analyze(db):
    await seed(db, "BTCUSDT", UPTREND)
    await seed(db, "SOLUSDT", UPTREND)
    engine = PAEngine(db)
    alarms = AlarmService(db, engine=engine, compute_budget=1)
    engine.alarm_service = alarms
    cond = [{"type": "above_below_vwap", "position": "above"}]
    await alarms.create_alert("BTCUSDT", TF, cond, cooldown_seconds=0)
    await alarms.create_alert("SOLUSDT", TF, cond, cooldown_seconds=0)
    assert await _read_history(db, "market_structure", "BTCUSDT", TF) == []
    assert await _read_history(db, "market_structure", "SOLUSDT", TF) == []

    # Budget 1 → first symbol is analyzed, second is deferred this cycle.
    await alarms.evaluate_symbol("BTCUSDT", TF)
    await alarms.evaluate_symbol("SOLUSDT", TF)
    assert len(await _read_history(db, "market_structure", "BTCUSDT", TF)) == 1
    assert await _read_history(db, "market_structure", "SOLUSDT", TF) == []

    # New cycle (begin_evaluation_pass) → budget resets, SOLUSDT is also analyzed.
    alarms.begin_evaluation_pass()
    await alarms.evaluate_symbol("SOLUSDT", TF)
    assert len(await _read_history(db, "market_structure", "SOLUSDT", TF)) == 1


async def test_k3_budget_zero_defers_all(db):
    await seed(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    alarms = AlarmService(db, engine=engine, compute_budget=0)
    engine.alarm_service = alarms
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)
    triggered = await alarms.evaluate_symbol("BTCUSDT", TF)
    assert triggered == []
    assert await _read_history(db, "market_structure", "BTCUSDT", TF) == []
