"""2.17 FIX — futures OI polling is filtered by the fapi symbol set.

Live observation: tokenized stock pairs in the spot universe (SMCIBUSDT,
ALABBUSDT, ...) and spot-only symbols are absent from futures (fapi); `poll_open_interest`
when requesting `/fapi/v1/openInterest` for every universe symbol, produced 100+
400 errors per cycle, log spam, and wasted rate-limit budget.

Fix:
- Fetch the TRADING USDT set from fapi `/fapi/v1/exchangeInfo` with a TTL.
- OI polling requests only symbols in that set.
- Remove a symbol returning 400 (absent from fapi despite exchangeInfo) from the set,
  it is not retried in subsequent cycles.
"""

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.futures import FuturesContextPoller
from rasattrading_mcp.data.universe import UniverseService
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from tests.helpers import FakeRest


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


async def _poller(cfg, db, fake, universe_symbols=None):
    universe = UniverseService(fake, cfg)
    if universe_symbols is not None:
        fake.symbols = list(universe_symbols)
    await universe.sync()
    poller = FuturesContextPoller(fake, db, universe, cfg)
    return universe, poller


async def _stored_oi(db):
    def _q(conn):
        rows = conn.execute(
            "SELECT symbol, value FROM futures_context WHERE type='open_interest'"
        ).fetchall()
        return [dict(r) for r in rows]

    return await db.read(_q)


async def test_oi_poll_skips_symbols_not_on_futures(cfg, db):
    """Do not request OI for symbols in spot universe but absent from fapi."""
    fake = FakeRest(["BTCUSDT", "SMCIBUSDT", "ALABBUSDT"])  # spot universe
    fake.fapi_symbols = ["BTCUSDT"]  # Only BTCUSDT is in futures.
    _, poller = await _poller(cfg, db, fake, universe_symbols=["BTCUSDT", "SMCIBUSDT", "ALABBUSDT"])

    n = await poller.poll_open_interest()
    assert n == 1  # Only BTCUSDT.

    oi_calls = [c for c in fake.calls if c[0] == "/fapi/v1/openInterest"]
    assert len(oi_calls) == 1
    assert oi_calls[0][1] == {"symbol": "BTCUSDT"}
    assert all(c[1]["symbol"] != "SMCIBUSDT" for c in oi_calls)

    rows = await _stored_oi(db)
    assert [r["symbol"] for r in rows] == ["BTCUSDT"]


async def test_oi_poll_caches_futures_universe(cfg, db):
    """Fetch fapi exchangeInfo once during the TTL, not on every poll."""
    fake = FakeRest(["BTCUSDT", "ETHUSDT"])
    _, poller = await _poller(cfg, db, fake)

    await poller.poll_open_interest()
    await poller.poll_open_interest()
    ei_calls = [c for c in fake.calls if c[0] == "/fapi/v1/exchangeInfo"]
    assert len(ei_calls) == 1  # Second poll is within TTL → from cache.


async def test_oi_poll_refreshes_after_ttl(cfg, db):
    """When TTL expires, refetch fapi set (newly listed pairs are picked up)."""
    fake = FakeRest(["BTCUSDT"])
    poller = FuturesContextPoller(
        fake, db, UniverseService(fake, cfg), Config(data_dir=cfg.data_dir, futures_universe_ttl_seconds=0)
    )
    await poller.poll_open_interest()
    await poller.poll_open_interest()
    ei_calls = [c for c in fake.calls if c[0] == "/fapi/v1/exchangeInfo"]
    assert len(ei_calls) == 2


async def test_oi_poll_drops_symbol_after_400(cfg, db):
    """A symbol in exchangeInfo that returns 400 from OI is removed from the set."""
    class _FuturesRest(FakeRest):
        def __init__(self, symbols):
            super().__init__(symbols)
            self.fail_oi_for: set[str] = set()

        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/openInterest" and (params or {}).get("symbol") in self.fail_oi_for:
                self.calls.append((path, dict(params or {}), weight))
                raise RasatError(ErrorCode.INVALID_REQUEST, "Binance 400 — invalid request")
            return await super().get(path, params, weight)

    fake = _FuturesRest(["BTCUSDT", "ETHUSDT"])
    fake.fail_oi_for = {"ETHUSDT"}
    _, poller = await _poller(cfg, db, fake)

    n = await poller.poll_open_interest()
    assert n == 1  # ETHUSDT was removed, BTCUSDT was written.

    oi_calls = [c for c in fake.calls if c[0] == "/fapi/v1/openInterest"]
    assert [c[1]["symbol"] for c in oi_calls] == ["BTCUSDT", "ETHUSDT"]

    # Do not retry ETHUSDT on the second cycle (removed from the set).
    fake.fail_oi_for = set()
    n2 = await poller.poll_open_interest()
    assert n2 == 1
    oi_calls = [c for c in fake.calls if c[0] == "/fapi/v1/openInterest"]
    assert [c[1]["symbol"] for c in oi_calls] == ["BTCUSDT", "ETHUSDT", "BTCUSDT"]

    rows = await _stored_oi(db)
    assert [r["symbol"] for r in rows] == ["BTCUSDT"]
