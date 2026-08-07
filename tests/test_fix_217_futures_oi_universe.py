"""2.17 FIX — futures OI poll'ü fapi sembol kümesiyle filtrelenir.

Canlı gözlem: spot evrenindeki tokenized hisse senedi çiftleri (SMCIBUSDT,
ALABBUSDT, ...) ve spot-only semboller futures'ta (fapi) yok; `poll_open_interest`
evrendeki her sembole `/fapi/v1/openInterest` isteği atınca her turda ~100+ 400
hatası + log spam + boşa rate-limit bütçesi üretiyordu.

Fix:
- fapi `/fapi/v1/exchangeInfo`'dan TRADING USDT kümesi TTL'li çekilir.
- OI poll'ü yalnızca o kümedeki sembollere istek atar.
- 400 düşen sembol (exchangeInfo'ya rağmen fapi'de yok) kümeden düşürülür,
  sonraki turlarda tekrar denenmez.
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
    """Spot evreninde olup fapi'de olmayan sembollere OI isteği atılmaz."""
    fake = FakeRest(["BTCUSDT", "SMCIBUSDT", "ALABBUSDT"])  # spot evreni
    fake.fapi_symbols = ["BTCUSDT"]  # futures'ta yalnızca BTCUSDT var
    _, poller = await _poller(cfg, db, fake, universe_symbols=["BTCUSDT", "SMCIBUSDT", "ALABBUSDT"])

    n = await poller.poll_open_interest()
    assert n == 1  # yalnızca BTCUSDT

    oi_calls = [c for c in fake.calls if c[0] == "/fapi/v1/openInterest"]
    assert len(oi_calls) == 1
    assert oi_calls[0][1] == {"symbol": "BTCUSDT"}
    assert all(c[1]["symbol"] != "SMCIBUSDT" for c in oi_calls)

    rows = await _stored_oi(db)
    assert [r["symbol"] for r in rows] == ["BTCUSDT"]


async def test_oi_poll_caches_futures_universe(cfg, db):
    """fapi exchangeInfo her poll'de değil, TTL süresince bir kez çekilir."""
    fake = FakeRest(["BTCUSDT", "ETHUSDT"])
    _, poller = await _poller(cfg, db, fake)

    await poller.poll_open_interest()
    await poller.poll_open_interest()
    ei_calls = [c for c in fake.calls if c[0] == "/fapi/v1/exchangeInfo"]
    assert len(ei_calls) == 1  # ikinci poll TTL içinde → cache'ten


async def test_oi_poll_refreshes_after_ttl(cfg, db):
    """TTL dolunca fapi kümesi yeniden çekilir (yeni listelenen çiftler yakalanır)."""
    fake = FakeRest(["BTCUSDT"])
    poller = FuturesContextPoller(
        fake, db, UniverseService(fake, cfg), Config(data_dir=cfg.data_dir, futures_universe_ttl_seconds=0)
    )
    await poller.poll_open_interest()
    await poller.poll_open_interest()
    ei_calls = [c for c in fake.calls if c[0] == "/fapi/v1/exchangeInfo"]
    assert len(ei_calls) == 2


async def test_oi_poll_drops_symbol_after_400(cfg, db):
    """exchangeInfo'da görünüp OI'da 400 dönen sembol kümeden düşürülür."""
    class _FuturesRest(FakeRest):
        def __init__(self, symbols):
            super().__init__(symbols)
            self.fail_oi_for: set[str] = set()

        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/openInterest" and (params or {}).get("symbol") in self.fail_oi_for:
                self.calls.append((path, dict(params or {}), weight))
                raise RasatError(ErrorCode.INVALID_REQUEST, "Binance 400 — geçersiz istek")
            return await super().get(path, params, weight)

    fake = _FuturesRest(["BTCUSDT", "ETHUSDT"])
    fake.fail_oi_for = {"ETHUSDT"}
    _, poller = await _poller(cfg, db, fake)

    n = await poller.poll_open_interest()
    assert n == 1  # ETHUSDT düştü, BTCUSDT yazıldı

    oi_calls = [c for c in fake.calls if c[0] == "/fapi/v1/openInterest"]
    assert [c[1]["symbol"] for c in oi_calls] == ["BTCUSDT", "ETHUSDT"]

    # İkinci turda ETHUSDT tekrar denenmemeli (kümeden düşürüldü)
    fake.fail_oi_for = set()
    n2 = await poller.poll_open_interest()
    assert n2 == 1
    oi_calls = [c for c in fake.calls if c[0] == "/fapi/v1/openInterest"]
    assert [c[1]["symbol"] for c in oi_calls] == ["BTCUSDT", "ETHUSDT", "BTCUSDT"]

    rows = await _stored_oi(db)
    assert [r["symbol"] for r in rows] == ["BTCUSDT"]
