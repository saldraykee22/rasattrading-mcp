"""T05 FIX — futures kline source separation and server clock.

Review findings converted to tests (rasattrading-mcp-detailed-review P2):
- `source="futures"` fetched from the spot kline endpoint and labeled it futures
  (`_fetch` always called `/api/v3/klines`).
- `source` was missing from the in-flight kline dedup key; concurrent spot/futures
  requests merged, so one call could read empty data or the wrong source.
- Candle closing and freshness depended on local time; host clock skew could accept
  a forming bar or mark a current bar stale.

Scope: spot `/api/v3/klines`, futures `/fapi/v1/klines` (separate REST client),
source-specific dedup/warm-map/scheduler/read-write, symbol validation against the
fapi universe, and an injectable offset-aware `/api/v3/time` clock (fail-closed).
"""

import asyncio
import time

import pytest

from rasattrading_mcp.config import Config, TIMEFRAME_SECONDS
from rasattrading_mcp.data.clock import BinanceClock
from rasattrading_mcp.data.klines import KlineService
from rasattrading_mcp.data.universe import UniverseService
from rasattrading_mcp.envelope import FRESHNESS_STALE
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from tests.helpers import FakeClock, FakeRest

TF = "1h"
PERIOD = 3600


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


async def _svc(cfg, db, fake, clock=None):
    universe = UniverseService(fake, cfg)
    await universe.sync()
    service = KlineService(fake, fake, db, universe, cfg, clock=clock or FakeClock())
    await service.start()
    return universe, service


async def _count_by_source(db, symbol, tf):
    def _q(conn):
        rows = conn.execute(
            "SELECT source, COUNT(*) AS n FROM candles WHERE symbol=? AND timeframe=? GROUP BY source",
            (symbol, tf),
        ).fetchall()
        return {r["source"]: r["n"] for r in rows}

    return await db.read(_q)


# ---------------------------------------------------------------------------
# Endpoint / source separation
# ---------------------------------------------------------------------------


async def test_futures_source_calls_fapi_and_stores_no_spot(cfg, db):
    """source=futures stores `/fapi/v1/klines` and does not create a source=spot row."""
    fake = FakeRest(["BTCUSDT"])
    _, svc = await _svc(cfg, db, fake)
    try:
        rows = await svc.get_candles("BTCUSDT", TF, 50, source="futures")
        assert len(rows) == 50

        fapi_calls = [c for c in fake.calls if c[0] == "/fapi/v1/klines"]
        assert len(fapi_calls) == 1
        assert fapi_calls[0][1] == {"symbol": "BTCUSDT", "interval": TF, "limit": 50}

        spot_calls = [c for c in fake.calls if c[0] == "/api/v3/klines"]
        assert spot_calls == []

        counts = await _count_by_source(db, "BTCUSDT", TF)
        assert counts.get("futures", 0) == 50
        assert "spot" not in counts
    finally:
        await svc.stop()


async def test_spot_source_keeps_api_v3_and_stores_spot(cfg, db):
    """Default spot behavior is preserved: `/api/v3/klines` and a source='spot' row."""
    fake = FakeRest(["BTCUSDT"])
    _, svc = await _svc(cfg, db, fake)
    try:
        rows = await svc.get_candles("BTCUSDT", TF, 50)
        assert len(rows) == 50

        spot_calls = [c for c in fake.calls if c[0] == "/api/v3/klines"]
        assert len(spot_calls) == 1
        assert spot_calls[0][1] == {"symbol": "BTCUSDT", "interval": TF, "limit": 50}

        counts = await _count_by_source(db, "BTCUSDT", TF)
        assert counts.get("spot", 0) == 50
        assert "futures" not in counts
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# Concurrent dedup: source is part of the key
# ---------------------------------------------------------------------------


class _DualRest(FakeRest):
    """Return futures klines with a different close so sources can be distinguished."""

    def __init__(self, symbols):
        super().__init__(symbols)
        self.futures_close = "200.5"

    async def get(self, path, params=None, weight=1):
        params = dict(params or {})
        if path == "/fapi/v1/klines":
            self.calls.append((path, params, weight))
            raw = self._gen_klines(params.get("symbol"), params.get("interval", "1h"), int(params.get("limit", 100)))
            for row in raw:
                row[4] = self.futures_close
            return raw
        return await super().get(path, params, weight)


async def test_concurrent_spot_futures_no_cross_dedup(cfg, db):
    """Concurrent spot/futures requests for the same (symbol,timeframe) do not deduplicate.

    Each call returns its own source's data (spot close=100.5, futures=200.5);
    if merged into one fetch, one would return the other source's data.
    """
    fake = _DualRest(["BTCUSDT"])
    _, svc = await _svc(cfg, db, fake)
    try:
        spot_rows, fut_rows = await asyncio.gather(
            svc.get_candles("BTCUSDT", TF, 100, source="spot"),
            svc.get_candles("BTCUSDT", TF, 100, source="futures"),
        )
        assert len(spot_rows) == 100
        assert len(fut_rows) == 100
        assert all(r["close"] == 100.5 for r in spot_rows)
        assert all(r["close"] == 200.5 for r in fut_rows)

        paths = [c[0] for c in fake.calls]
        assert "/api/v3/klines" in paths
        assert "/fapi/v1/klines" in paths

        counts = await _count_by_source(db, "BTCUSDT", TF)
        assert counts.get("spot", 0) == 100
        assert counts.get("futures", 0) == 100
    finally:
        await svc.stop()


async def test_warm_spot_does_not_warm_futures(cfg, db):
    """Even with warm spot data, a futures request remains cold and fetches again."""
    fake = FakeRest(["BTCUSDT"])
    _, svc = await _svc(cfg, db, fake)
    try:
        await svc.get_candles("BTCUSDT", TF, 100, source="spot")

        # Spot warm—the second spot request must not fetch.
        n_before = len([c for c in fake.calls if c[0] == "/api/v3/klines"])
        await svc.get_candles("BTCUSDT", TF, 100, source="spot")
        n_after = len([c for c in fake.calls if c[0] == "/api/v3/klines"])
        assert n_after == n_before

        # But futures is still cold → fetch /fapi/v1/klines.
        fut_before = len([c for c in fake.calls if c[0] == "/fapi/v1/klines"])
        rows = await svc.get_candles("BTCUSDT", TF, 100, source="futures")
        assert len(rows) == 100
        fut_after = len([c for c in fake.calls if c[0] == "/fapi/v1/klines"])
        assert fut_after > fut_before

        counts = await _count_by_source(db, "BTCUSDT", TF)
        assert counts.get("spot", 0) == 100
        assert counts.get("futures", 0) == 100
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# Futures symbol validation
# ---------------------------------------------------------------------------


async def test_futures_unknown_symbol_rejected(cfg, db):
    """A symbol in the spot universe but not in fapi is rejected for a futures request."""
    fake = FakeRest(["BTCUSDT", "ETHUSDT"])
    fake.fapi_symbols = ["BTCUSDT"]  # ETHUSDT is not in futures.
    _, svc = await _svc(cfg, db, fake)
    try:
        with pytest.raises(RasatError) as exc:
            await svc.get_candles("ETHUSDT", TF, 100, source="futures")
        assert exc.value.code == ErrorCode.INVALID_SYMBOL
        assert "futures" in exc.value.message

        # The same symbol is valid on spot.
        rows = await svc.get_candles("ETHUSDT", TF, 10, source="spot")
        assert len(rows) == 10
    finally:
        await svc.stop()


async def test_futures_symbol_independent_of_spot_universe(cfg, db):
    """Futures validation uses the fapi universe—it does not need the spot universe."""
    fake = FakeRest(["BTCUSDT"])
    fake.fapi_symbols = ["BTCUSDT", "ETHUSDT"]
    _, svc = await _svc(cfg, db, fake)
    try:
        rows = await svc.get_candles("ETHUSDT", TF, 10, source="futures")
        assert len(rows) == 10
    finally:
        await svc.stop()


async def test_futures_universe_unreachable_fail_closed(cfg, db):
    """If fapi exchangeInfo cannot be loaded, the futures symbol is not silently accepted."""
    class _EiFailRest(FakeRest):
        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/exchangeInfo":
                raise RasatError(ErrorCode.INTERNAL_ERROR, "fapi is unreachable")
            return await super().get(path, params, weight)

    fake = _EiFailRest(["BTCUSDT"])
    _, svc = await _svc(cfg, db, fake)
    try:
        with pytest.raises(RasatError) as exc:
            await svc.get_candles("BTCUSDT", TF, 100, source="futures")
        assert exc.value.code == ErrorCode.INVALID_SYMBOL
        assert "could not validate symbol" in exc.value.message
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# Server clock: closed candle and freshness
# ---------------------------------------------------------------------------


async def test_clock_offset_rejects_forming_bar(cfg, db):
    """With an offset clock, reject a bar that local time considers "closed" but the server still considers forming.

    Host time is 2 periods ahead of server (offset=-2P). FakeRest bar set
    The local clock produces through its latest close, but those bars are still
    forming according to server time. Store only through the server's last closed
    bar (`server_latest_closed`).
    """
    fake = FakeRest(["BTCUSDT"])
    clock = FakeClock(offset=-2 * PERIOD)
    _, svc = await _svc(cfg, db, fake, clock=clock)
    try:
        server_now = clock.server_now()
        server_latest_closed = int(server_now // PERIOD) * PERIOD - PERIOD
        local_latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD

        rows = await svc.get_candles("BTCUSDT", TF, 10)
        assert rows[-1]["open_time"] <= server_latest_closed
        assert rows[-1]["open_time"] < local_latest_closed  # Bar locally considered closed was rejected.

        def _q(conn):
            row = conn.execute(
                "SELECT MAX(open_time) AS m FROM candles WHERE symbol='BTCUSDT' AND timeframe=?",
                (TF,),
            ).fetchone()
            return int(row["m"]) if row["m"] is not None else None

        max_open = await db.read(_q)
        assert max_open == server_latest_closed
    finally:
        await svc.stop()


async def test_clock_sync_accepts_forming_bar_drop(cfg, db):
    """Synchronized clock (offset=0): Binance's forming bar is still not stored."""
    from rasattrading_mcp.config import TIMEFRAME_SECONDS

    class _FormingRest(FakeRest):
        async def get(self, path, params=None, weight=1):
            if path in ("/api/v3/klines", "/fapi/v1/klines"):
                raw = self._gen_klines(
                    params.get("symbol"), params.get("interval", "1h"), int(params.get("limit", 100))
                )
                period = TIMEFRAME_SECONDS[params.get("interval", "1h")]
                latest_closed = int(time.time() // period) * period - period
                raw.append([latest_closed + period, "100", "101", "99", "100.5", "90", latest_closed + 2 * period, "9000", 3, "0", "0", "0"])
                return raw
            return await super().get(path, params, weight)

    fake = _FormingRest(["BTCUSDT"])
    _, svc = await _svc(cfg, db, fake, clock=FakeClock())
    try:
        rows = await svc.get_candles("BTCUSDT", TF, 10)
        latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
        assert all(r["open_time"] <= latest_closed for r in rows)
        assert rows[-1]["volume"] == 1000.0  # Partial volume (90) was not stored.

        def _q(conn):
            row = conn.execute(
                "SELECT MAX(open_time) AS m FROM candles WHERE symbol='BTCUSDT' AND timeframe=?",
                (TF,),
            ).fetchone()
            return int(row["m"]) if row["m"] is not None else None

        assert await db.read(_q) == latest_closed
    finally:
        await svc.stop()


async def test_clock_unavailable_fail_closed(cfg, db):
    """Without clock, data is not stored and freshness is fail-closed `stale`."""
    fake = FakeRest(["BTCUSDT"])
    clock = FakeClock(available=False)
    _, svc = await _svc(cfg, db, fake, clock=clock)
    try:
        rows = await svc.get_candles("BTCUSDT", TF, 100)
        assert rows == []  # No row was stored.

        counts = await _count_by_source(db, "BTCUSDT", TF)
        assert counts == {}

        # Even with DB data, freshness is stale without clock (fail-closed).
        assert svc.freshness_for("BTCUSDT", TF, [{"open_time": int(time.time())}]) == FRESHNESS_STALE
    finally:
        await svc.stop()


async def test_freshness_uses_clock_offset(cfg, db):
    """Freshness is calculated against the server clock (with offset)."""
    fake = FakeRest(["BTCUSDT"])
    clock = FakeClock(offset=-PERIOD)  # host 1 period ileride
    _, svc = await _svc(cfg, db, fake, clock=clock)
    try:
        server_now = clock.server_now()
        server_latest_closed = int(server_now // PERIOD) * PERIOD - PERIOD
        # Server's last closed bar → fresh.
        assert svc.freshness_for("BTCUSDT", TF, [{"open_time": server_latest_closed}]) == "fresh"
        # Previous bar → stale.
        assert svc.freshness_for("BTCUSDT", TF, [{"open_time": server_latest_closed - PERIOD}]) == FRESHNESS_STALE
        # Local latest close is still forming according to server → must not be fresh.
        local_latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
        assert svc.freshness_for("BTCUSDT", TF, [{"open_time": local_latest_closed}]) == FRESHNESS_STALE
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# BinanceClock birim testleri
# ---------------------------------------------------------------------------


class _TimeRest:
    """Minimal REST double returning `/api/v3/time`."""

    def __init__(self, server_time_seconds):
        self.server_time_seconds = server_time_seconds
        self.calls = []

    async def get(self, path, params=None, weight=1):
        self.calls.append((path, dict(params or {}), weight))
        assert path == "/api/v3/time"
        return {"serverTime": int(self.server_time_seconds * 1000)}


async def test_binance_clock_sync_sets_offset():
    now = time.time()
    rest = _TimeRest(now + 120)
    clock = BinanceClock(rest, refresh_seconds=60, max_offset_age_seconds=300)
    assert clock.server_now() is None  # Not synchronized yet.
    assert clock.offset is None

    ok = await clock.sync()
    assert ok
    assert clock.offset == pytest.approx(120, abs=2)
    assert clock.server_now() == pytest.approx(now + 120, abs=3)
    assert clock.available
    assert rest.calls[0][0] == "/api/v3/time"


async def test_binance_clock_bounded_offset_rejected():
    """Excessive host clock skew makes the offset untrusted (fail-closed)."""
    now = time.time()
    rest = _TimeRest(now + 100_000)  # ~28 saat kayma
    clock = BinanceClock(rest, max_offset_seconds=3600)
    ok = await clock.sync()
    assert not ok
    assert clock.server_now() is None
    assert clock.last_error


async def test_binance_clock_stale_offset_returns_none():
    now = time.time()
    rest = _TimeRest(now + 30)
    clock = BinanceClock(rest, refresh_seconds=60, max_offset_age_seconds=300)
    ok = await clock.sync()
    assert ok
    clock._synced_at = 0  # Offset is too old → stale.
    assert clock.server_now() is None
    assert clock.offset is None


async def test_binance_clock_sync_failure_keeps_unavailable():
    class _FailRest(_TimeRest):
        async def get(self, path, params=None, weight=1):
            self.calls.append((path, dict(params or {}), weight))
            raise RasatError(ErrorCode.INTERNAL_ERROR, "network error")

    clock = BinanceClock(_FailRest(0), max_offset_age_seconds=300)
    ok = await clock.sync()
    assert not ok
    assert clock.server_now() is None
    assert clock.last_error is not None


async def test_binance_clock_start_stop_refreshes():
    now = time.time()
    rest = _TimeRest(now)
    clock = BinanceClock(rest, refresh_seconds=0.05, max_offset_age_seconds=60)
    clock.start()
    try:
        await asyncio.sleep(0.15)
        assert len(rest.calls) >= 2  # Refreshed several times in the background.
    finally:
        await clock.stop()
