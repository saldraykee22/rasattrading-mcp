"""2.11 FIX — AST validation hardening + composite cooldown + cooldown persistence.

Review evidence is converted into tests:
- A bad filter AST returns structured INVALID_REQUEST, not TypeError.
- Composite alert cooldown is also validated (reject negative/non-integer values).
- Cooldown state is stored explicitly/persistently and survives restart.
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.alarms import AlarmService
from rasattrading_mcp.pa.screener import validate_filters
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


# ---------------------------------------------------------------------------
# AST validation (INVALID_REQUEST instead of TypeError)
# ---------------------------------------------------------------------------


def _rejects(filters):
    with pytest.raises(RasatError) as exc:
        validate_filters(filters)
    assert exc.value.code == ErrorCode.INVALID_REQUEST


def test_window_bars_string_rejected():
    _rejects([{"type": "price_change", "window_bars": "x"}])


def test_window_bars_zero_rejected():
    _rejects([{"type": "price_change", "window_bars": 0}])


def test_recent_bars_negative_rejected():
    _rejects([{"type": "volume_change", "recent_bars": -5}])


def test_since_bars_zero_rejected():
    _rejects([{"type": "liquidity_sweep_occurred", "since_bars": 0}])


def test_oi_window_zero_rejected():
    _rejects([{"type": "oi_change", "window": 0}])


def test_max_distance_pct_negative_rejected():
    _rejects([{"type": "near_order_block", "max_distance_pct": -1}])


def test_max_distance_pct_string_rejected():
    _rejects([{"type": "near_order_block", "max_distance_pct": "x"}])


def test_bool_min_rejected():
    _rejects([{"type": "price_change", "min": True}])


def test_and_node_unknown_key_rejected():
    _rejects([{"type": "and", "filters": [{"type": "price_change", "min": 1}], "evil": 1}])


def test_min_greater_than_max_rejected():
    _rejects([{"type": "price_change", "min": 10, "max": 5}])


def test_valid_ast_still_accepted():
    root = validate_filters([{"type": "price_change", "window_bars": 5, "min": 1, "max": 5}])
    assert root["filters"][0]["window_bars"] == 5


# ---------------------------------------------------------------------------
# Composite cooldown validation
# ---------------------------------------------------------------------------


async def test_composite_cooldown_validation(db):
    alarms = AlarmService(db, engine=PAEngine(db))
    clause = [{"symbol": "BTCUSDT", "timeframe": TF, "filters": [{"type": "price_change", "min": 1}]}]
    with pytest.raises(RasatError):
        await alarms.create_composite_alert(clause, cooldown_seconds=-1)
    with pytest.raises(RasatError):
        await alarms.create_composite_alert(clause, cooldown_seconds="300")


# ---------------------------------------------------------------------------
# Cooldown persistence (survives restart)
# ---------------------------------------------------------------------------


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


async def test_cooldown_state_persisted_and_restart(db):
    await _seed_candles(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    alarms = AlarmService(db, engine=engine)
    engine.alarm_service = alarms
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=3600)
    await engine.analyze("BTCUSDT", TF)

    def _q(conn):
        return dict(conn.execute("SELECT state, cooldown_until FROM alerts").fetchone())

    stored = await db.read(_q)
    assert stored["state"] == "cooldown"
    assert stored["cooldown_until"] is not None
    assert stored["cooldown_until"] > int(time.time())

    # Restart (new service) → cooldown state persists.
    fresh_alarms = AlarmService(db, engine=PAEngine(db))
    listed = await fresh_alarms.list_alerts()
    assert listed[0]["state"] == "cooldown"
    assert listed[0]["cooldown_until"] == stored["cooldown_until"]


async def test_cooldown_expires_to_armed(db, monkeypatch):
    import time as _time

    await _seed_candles(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    alarms = AlarmService(db, engine=engine)
    engine.alarm_service = alarms
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=3600)
    await engine.analyze("BTCUSDT", TF)
    assert (await alarms.list_alerts())[0]["state"] == "cooldown"

    later = int(time.time()) + 7200  # Cooldown expired.
    monkeypatch.setattr(_time, "time", lambda: later)
    assert (await alarms.list_alerts())[0]["state"] == "armed"
