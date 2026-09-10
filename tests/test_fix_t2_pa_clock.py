"""T2 FIX — PA server-clock consistency + screener fail-closed + OI state.

Review findings converted to tests (fix-t2-data-pa-clock ticket):
- `pa/swings.filter_closed_candles`, `pa/analysis.freshness_for`, `pa/worker.latest_closed`
  used local time; the data layer (klines) uses the server clock. Host clock skew
  shifted PA decisions → `PAEngine._now()` (pipeline clock, otherwise local time)
  became the shared source for closed-candle/freshness decisions.
- `klines._needs_catchup` used one MAX check for the whole timeframe → per symbol.
- `futures.poll_open_interest` overwrote post-error state with "ok" → preserve real state.
- Screener mixed stale symbols into results (fail-open) → `require_fresh`.
- Screener called `engine.analyze` without a budget in a cold universe → K3 budget pattern.

Scope: pa/swings, pa/analysis, pa/worker, pa/screener, data/klines, data/futures.
"""

import time

import pytest

from rasattrading_mcp.config import Config, TIMEFRAME_SECONDS
from rasattrading_mcp.data.futures import FuturesContextPoller
from rasattrading_mcp.data.klines import KlineService
from rasattrading_mcp.data.universe import UniverseService
from rasattrading_mcp.envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.screener import Screener
from rasattrading_mcp.pa.swings import filter_closed_candles
from rasattrading_mcp.pa.worker import PAWorker
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from tests.helpers import FakeClock, FakeRest

TF = "1h"
PERIOD = 3600

UPTREND = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]


class FakePipeline:
    def __init__(self, clock):
        self.clock = clock


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


# ---------------------------------------------------------------------------
# PA server clock (closed candle + freshness)
# ---------------------------------------------------------------------------


def test_pa_filter_closed_candles_uses_server_clock():
    """When host time is two periods ahead of server, close decisions use server clock.

    The last bar host considers "closed" is still forming according to server—filter it out.
    """
    clock = FakeClock(offset=-2 * PERIOD)
    now = clock.server_now()
    server_latest_closed = int(now // PERIOD) * PERIOD - PERIOD
    host_latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    assert host_latest_closed > server_latest_closed  # senaryo teyidi

    candles = [
        {"open_time": server_latest_closed - PERIOD, "open": 1, "high": 2, "low": 0.5, "close": 1.5},
        {"open_time": server_latest_closed, "open": 1.5, "high": 2.5, "low": 1, "close": 2},
        {"open_time": host_latest_closed, "open": 2, "high": 3, "low": 1.5, "close": 2.5},
    ]
    kept = filter_closed_candles(candles, TF, now=now)
    assert [c["open_time"] for c in kept] == [server_latest_closed - PERIOD, server_latest_closed]
    # Without `now`, use local time—the old behavior is preserved.
    kept_local = filter_closed_candles(candles, TF)
    assert host_latest_closed in [c["open_time"] for c in kept_local]


def test_pa_freshness_uses_server_clock():
    """Freshness is calculated against server clock with the `now` parameter (T2).

    With server clock one `PERIOD` behind host time, the server's last closed
    candle (`server_latest_closed`) is fresh—but with local (host) time the same
    candle would appear `stale` (false-stale risk). The fix ties the decision to server clock.
    """
    clock = FakeClock(offset=-PERIOD)
    server_now = clock.server_now()
    server_latest_closed = int(server_now // PERIOD) * PERIOD - PERIOD

    # Server's last closed candle is fresh with server clock.
    assert PAEngine.freshness_for(TF, server_latest_closed, now=server_now) == FRESHNESS_FRESH
    # With local time (when now is omitted), the same candle appears stale → inconsistent with data layer.
    assert PAEngine.freshness_for(TF, server_latest_closed) == FRESHNESS_STALE


async def test_pa_engine_now_prefers_pipeline_clock(db):
    """`PAEngine._now()` returns server time when pipeline clock exists; otherwise local time."""
    clock = FakeClock(offset=7 * 60)
    engine = PAEngine(db, pipeline=FakePipeline(clock))
    assert engine._now() == pytest.approx(clock.server_now(), abs=1)

    engine_local = PAEngine(db)  # No pipeline → local time.
    assert engine_local._now() == pytest.approx(time.time(), abs=1)


async def test_pa_freshness_instance_uses_server_clock(db):
    """`PAEngine.freshness()` instance method uses server clock (for screener/worker)."""
    clock = FakeClock(offset=-PERIOD)
    engine = PAEngine(db, pipeline=FakePipeline(clock))
    server_latest_closed = int(clock.server_now() // PERIOD) * PERIOD - PERIOD

    assert engine.freshness(TF, server_latest_closed) == FRESHNESS_FRESH
    # The same candle appears stale with local time—it is fresh because the instance method uses server clock.
    assert PAEngine.freshness_for(TF, server_latest_closed) == FRESHNESS_STALE


def test_pa_worker_latest_closed_uses_server_clock():
    """`PAWorker.latest_closed` returns the server's last closed bar using `now`."""
    clock = FakeClock(offset=-PERIOD)
    server_latest_closed = int(clock.server_now() // PERIOD) * PERIOD - PERIOD
    host_latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD

    assert PAWorker.latest_closed(TF, now=clock.server_now()) == server_latest_closed
    assert PAWorker.latest_closed(TF) == host_latest_closed  # Without now, local time.


async def test_worker_processes_when_server_clock_says_closed(db, cfg):
    """The worker processes a bar closed according to server clock even when host time is skewed.

    The old behavior (local time) considered a bar "not closed" by host time,
    marked it stale, and skipped the cycle; server clock keeps it consistent with the data layer.
    """
    clock = FakeClock(offset=-PERIOD)
    engine = PAEngine(db, pipeline=FakePipeline(clock))
    server_latest_closed = int(clock.server_now() // PERIOD) * PERIOD - PERIOD
    host_latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    assert host_latest_closed == server_latest_closed + PERIOD  # senaryo teyidi

    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND), now=clock.server_now()))
    worker = PAWorker(engine, FakeUniverse(["BTCUSDT"]), cfg)

    assert await worker.check_and_process() == 1
    assert worker._last_processed.get(TF) == server_latest_closed


async def test_worker_skips_when_data_behind_server(db, cfg):
    """If data is one period behind the server's last closed bar, the worker skips
    the cycle—fail-closed (2.12 behavior preserved with server clock)."""
    clock = FakeClock(offset=-PERIOD)
    engine = PAEngine(db, pipeline=FakePipeline(clock))
    server_latest_closed = int(clock.server_now() // PERIOD) * PERIOD - PERIOD

    # Last candle is one period behind the server's last closed bar.
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND), now=clock.server_now() - PERIOD))
    worker = PAWorker(engine, FakeUniverse(["BTCUSDT"]), cfg)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is None  # stale → retry beklenir


# ---------------------------------------------------------------------------
# Screener: require_fresh (fail-closed) + analysis budget
# ---------------------------------------------------------------------------


async def _seed_old(db, symbol, rows):
    base = 1_700_000_000
    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, base + i * PERIOD, o, h, l, c, 10.0, "spot", int(time.time())),
            )
    await db.write(_w)


async def test_screener_require_fresh_excludes_stale(db):
    """`require_fresh=True` (default): stale matches are removed from results and
    reported in `stale_symbols` (same fail-closed behavior as alarms)."""
    await _seed_old(db, "BTCUSDT", UPTREND)  # Old timestamp → stale.
    await _seed_old(db, "SOLUSDT", [(105 - i * 0.5, 105.5 - i * 0.5, 104.5 - i * 0.5, 105 - i * 0.5) for i in range(15)])
    screener = Screener(db)

    res = await screener.scan([{"type": "price_change", "window_bars": 10, "min": 2}])
    assert res["symbols"] == []  # All matches stale → empty result (fail-closed).
    assert res["freshness"] == FRESHNESS_FRESH  # No result presented → no stale data.
    stale_syms = {s["symbol"] for s in res["stale_symbols"]}
    assert "BTCUSDT" in stale_syms  # Matched but stale, so separately reported.
    assert "SOLUSDT" not in stale_syms  # Did not match the filter → not in stale list.


async def test_screener_require_fresh_false_keeps_legacy_behavior(db):
    """`require_fresh=False`: stale matches enter results marked as stale (old behavior)."""
    await _seed_old(db, "BTCUSDT", UPTREND)
    screener = Screener(db)

    res = await screener.scan([{"type": "price_change", "window_bars": 10, "min": 2}], require_fresh=False)
    btc = next(s for s in res["symbols"] if s["symbol"] == "BTCUSDT")
    assert btc["data_stale"] is True
    assert res["freshness"] == FRESHNESS_STALE
    assert res["stale_symbols"] == []  # require_fresh=False → stale symbols are marked in results.


async def test_screener_analysis_budget_defers_cold_symbols(db):
    """K3 pattern: when no stored analysis exists, on-demand `analyze` is budgeted;
    symbols over budget are deferred (`deferred_analysis`), so the scan does not take minutes."""
    for sym in ("BTCUSDT", "ETHUSDT"):
        await seed_at(db, sym, UPTREND, _fresh_times(len(UPTREND)))
    screener = Screener(db, compute_budget=1)

    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish"}])
    assert res["deferred_analysis"] == 1  # 1 of 2 symbols analyzed, 1 deferred
    assert res["total_matched"] == 1  # Only the analyzed symbol could be evaluated.


async def test_screener_budget_resets_each_scan(db):
    """Each scan cycle resets the budget—the deferred symbol can be processed next cycle."""
    for sym in ("BTCUSDT", "ETHUSDT"):
        await seed_at(db, sym, UPTREND, _fresh_times(len(UPTREND)))
    screener = Screener(db, compute_budget=1)

    first = await screener.scan([{"type": "structure_event", "event": "bos_bullish"}])
    assert first["deferred_analysis"] == 1  # BTCUSDT analyzed, ETHUSDT deferred
    assert first["total_matched"] == 1

    # Budget reset on the second cycle; BTCUSDT is now stored → ETHUSDT is also analyzed.
    second = await screener.scan([{"type": "structure_event", "event": "bos_bullish"}])
    assert second["deferred_analysis"] == 0
    assert second["total_matched"] == 2


# ---------------------------------------------------------------------------
# klines: per-symbol catch-up
# ---------------------------------------------------------------------------


async def _klines_svc(cfg, db, fake):
    universe = UniverseService(fake, cfg)
    await universe.sync()
    service = KlineService(fake, fake, db, universe, cfg, clock=FakeClock())
    await service.start()
    return universe, service


async def _max_open_for(db, symbol):
    def _q(conn):
        row = conn.execute(
            "SELECT MAX(open_time) AS m FROM candles WHERE symbol=? AND timeframe=?",
            (symbol, TF),
        ).fetchone()
        return int(row["m"]) if row["m"] is not None else None

    return await db.read(_q)


async def test_needs_catchup_symbol_level(cfg, db):
    """Catch-up is not skipped when one symbol is current and another is behind (T2)."""
    fake = FakeRest(["BTCUSDT", "ETHUSDT"])
    _, svc = await _klines_svc(cfg, db, fake)
    try:
        last_closed = int(time.time() // PERIOD) * PERIOD - PERIOD

        # BTCUSDT current (last closed bar), ETHUSDT one period behind.
        await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
        await seed_at(db, "ETHUSDT", UPTREND, _fresh_times(len(UPTREND), now=time.time() - PERIOD))

        assert await svc._needs_catchup(TF, last_closed, "spot") is True

        # Both current → no catch-up needed.
        await seed_at(db, "ETHUSDT", UPTREND, _fresh_times(len(UPTREND)))
        assert await svc._needs_catchup(TF, last_closed, "spot") is False
    finally:
        await svc.stop()


async def test_needs_catchup_no_symbols_false(cfg, db):
    """No catch-up is needed when a symbol is absent from the universe (preserve single-MAX None behavior)."""
    fake = FakeRest(["BTCUSDT"])
    _, svc = await _klines_svc(cfg, db, fake)
    try:
        # BTCUSDT is in the universe but has no data → catch-up is needed.
        last_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
        assert await svc._needs_catchup(TF, last_closed, "spot") is True
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# futures: OI poll state is "ok" only on a clean cycle
# ---------------------------------------------------------------------------


async def _poller(cfg, db, fake):
    universe = UniverseService(fake, cfg)
    await universe.sync()
    return FuturesContextPoller(fake, db, universe, cfg)


async def test_oi_poll_ok_only_on_clean_round(cfg, db):
    """Temiz tur → durum `ok`."""
    fake = FakeRest(["BTCUSDT"])
    poller = await _poller(cfg, db, fake)
    await poller.poll_open_interest()
    assert poller.status()["open_interest"] == "ok"


async def test_oi_poll_rate_limited_status_preserved(cfg, db):
    """On RATE_LIMITED, state remains `rate_limited`; it is not overwritten with `ok` at cycle end."""
    class _OiRateRest(FakeRest):
        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/openInterest":
                raise RasatError(ErrorCode.RATE_LIMITED, "budget exhausted")
            return await super().get(path, params, weight)

    fake = _OiRateRest(["BTCUSDT"])
    poller = await _poller(cfg, db, fake)
    n = await poller.poll_open_interest()
    assert n == 0
    assert poller.status()["open_interest"] == "rate_limited"


async def test_oi_poll_error_status_preserved(cfg, db):
    """A general error (timeout, etc.) sets state to `error`; it does not write `ok`."""
    class _OiFailRest(FakeRest):
        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/openInterest":
                raise RasatError(ErrorCode.INTERNAL_ERROR, "network error")
            return await super().get(path, params, weight)

    fake = _OiFailRest(["BTCUSDT"])
    poller = await _poller(cfg, db, fake)
    await poller.poll_open_interest()
    assert poller.status()["open_interest"] == "error"


async def test_oi_poll_invalid_symbol_400_still_ok(cfg, db):
    """400 (symbol absent from fapi) drops a symbol from the set but completes the cycle cleanly → `ok`."""
    class _Oi400Rest(FakeRest):
        def __init__(self, symbols):
            super().__init__(symbols)
            self.fail_oi_for: set[str] = set()

        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/openInterest" and (params or {}).get("symbol") in self.fail_oi_for:
                raise RasatError(ErrorCode.INVALID_REQUEST, "Binance 400")
            return await super().get(path, params, weight)

    fake = _Oi400Rest(["BTCUSDT", "ETHUSDT"])
    fake.fail_oi_for = {"ETHUSDT"}
    poller = await _poller(cfg, db, fake)
    n = await poller.poll_open_interest()
    assert n == 1
    assert poller.status()["open_interest"] == "ok"
