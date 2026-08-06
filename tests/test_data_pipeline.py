import asyncio
import json
import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.binance_client import BinanceREST, kline_weight
from rasattrading_mcp.data.futures import FuturesContextPoller
from rasattrading_mcp.data.klines import KlineService, parse_klines
from rasattrading_mcp.data.miniticker import MiniTickerClient, TickerCache, parse_miniticker_arr
from rasattrading_mcp.data.pipeline import DataPipeline
from rasattrading_mcp.data.rate_limit import RateLimitBudget, backoff_delay
from rasattrading_mcp.data.universe import UniverseService
from rasattrading_mcp.envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations


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


# ---------- rate limit ----------

async def test_rate_limit_budget_acquires_and_waits():
    budget = RateLimitBudget(max_weight=10, window_seconds=0.3)
    await budget.acquire(5)
    await budget.acquire(5)  # 10/10 dolu
    t0 = time.monotonic()
    await budget.acquire(2)  # pencere sıfırlanana kadar beklemeli
    waited = time.monotonic() - t0
    assert waited >= 0.2
    assert budget.total_waits >= 1


async def test_rate_limit_note_used():
    budget = RateLimitBudget(max_weight=100)
    budget.note_used(80)
    snap = budget.snapshot()
    assert snap["used_weight"] >= 80


def test_backoff_delay_increases():
    d0 = backoff_delay(0, jitter=False)
    d1 = backoff_delay(1, jitter=False)
    d2 = backoff_delay(2, jitter=False)
    assert d0 <= d1 <= d2


# ---------- binance client ----------

def test_kline_weight():
    assert kline_weight(50) == 1
    assert kline_weight(500) == 2
    assert kline_weight(1000) == 5


async def test_binance_rest_401_maps_to_unauthorized():
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    app = web.Application()

    async def handler(request):
        return web.Response(status=401, body=b"{}")

    app.router.add_get("/x", handler)
    async with TestServer(app) as server:
        from rasattrading_mcp.data.binance_client import FUTURES_REST_BASE

        client = BinanceREST(server.make_url("/").human_repr(), RateLimitBudget(1000), retries=1)
        with pytest.raises(RasatError) as exc:
            await client.get("/x")
        assert exc.value.code == ErrorCode.UNAUTHORIZED


# ---------- universe ----------

async def test_universe_sync_filters(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(
        ["BTCUSDT", "ETHUSDT"],
        extra_exchange_entries=[
            {"symbol": "XXXUSD", "status": "TRADING", "quoteAsset": "USD"},  # USDT değil
            {"symbol": "DELETEDUSDT", "status": "BREAK", "quoteAsset": "USDT"},  # TRADING değil
        ],
    )
    uni = UniverseService(fake, cfg)
    n = await uni.sync()
    assert n == 2  # sadece USDT + TRADING
    assert uni.contains("BTCUSDT")
    assert not uni.contains("XXXUSD")
    assert not uni.contains("DELETEDUSDT")


async def test_universe_ensure_contains_unknown(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    uni = UniverseService(fake, cfg)
    await uni.sync()
    assert await uni.ensure_contains("BTCUSDT") is True
    # bilinmeyen sembol → evren taze ise resync yok → False
    assert await uni.ensure_contains("NOPEUSDT") is False


# ---------- miniticker ----------

MINI_PAYLOAD = json.dumps(
    [
        {"e": "24hrMiniTicker", "s": "BTCUSDT", "c": "100.5", "o": "99.0", "h": "101.0", "l": "98.0",
         "v": "1000", "q": "100000", "P": "1.52", "E": 1700000000000}
    ]
)


def test_parse_miniticker():
    updates = parse_miniticker_arr(MINI_PAYLOAD)
    assert len(updates) == 1
    assert updates[0].symbol == "BTCUSDT"
    assert updates[0].last == 100.5
    assert updates[0].price_change_pct == 1.52


def test_ticker_cache_freshness():
    cache = TickerCache(stale_after=30)
    cache.apply_updates(parse_miniticker_arr(MINI_PAYLOAD))
    assert cache.status == "connected"
    assert cache.freshness_for("BTCUSDT") == FRESHNESS_FRESH

    cache.mark_stale("ws kapandı")
    assert cache.freshness_for("BTCUSDT") == FRESHNESS_STALE
    assert cache.get("BTCUSDT")["freshness"] == FRESHNESS_STALE


async def test_miniticker_ws_reconnect_marks_stale():
    import websockets

    cache = TickerCache(stale_after=30)
    frame = [dict(s="BTCUSDT", c="100.5", o="99", h="101", l="98", v="100", q="10000", P="1.5", E=0)]

    async def handler(ws):
        await ws.send(json.dumps(frame))
        await asyncio.sleep(30)

    server = await websockets.serve(handler, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        client = MiniTickerClient(f"ws://127.0.0.1:{port}/stream", cache)
        stop = asyncio.Event()
        task = asyncio.create_task(client.run(stop))
        for _ in range(100):
            if cache.status == "connected":
                break
            await asyncio.sleep(0.05)
        assert cache.status == "connected"
        assert cache.get("BTCUSDT") is not None

        # Bağlantıyı kes → WS kopar → veri stale işaretlenmeli
        server.close()
        await server.wait_closed()
        for _ in range(100):
            if cache.status == "disconnected":
                break
            await asyncio.sleep(0.05)
        assert cache.status == "disconnected"
        assert cache.get("BTCUSDT")["freshness"] == FRESHNESS_STALE

        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    finally:
        server.close()
        await server.wait_closed()


# ---------- klines ----------

async def test_parse_klines():
    raw = [[1700000000000, "1", "2", "0", "1.5", "10", 1700000100000, "100", 5, "0", "0", "0"]]
    rows = parse_klines(raw)
    assert rows[0]["open_time"] == 1700000000000
    assert rows[0]["close"] == 1.5
    assert rows[0]["trades"] == 5


async def _make_klines(cfg, db, fake):
    universe = UniverseService(fake, cfg)
    await universe.sync()
    service = KlineService(fake, db, universe, cfg)
    await service.start()
    return universe, service


async def test_get_candles_ondemand_priority_and_store(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT", "ETHUSDT"])
    universe, service = await _make_klines(cfg, db, fake)

    try:
        rows = await service.get_candles("BTCUSDT", "15m", 300)
        assert len(rows) == 300
        assert rows[-1]["close"] == 100.5
        # Öncelik: backfill (prio 10) önce kuyruğa girse de on-demand (prio 0) önce işlenir
        first_kline_call = next(c for c in fake.calls if c[0] == "/api/v3/klines")
        assert first_kline_call[1] == {"symbol": "BTCUSDT", "interval": "15m", "limit": 300}
    finally:
        await service.stop()


async def test_get_candles_warm_no_refetch(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    universe, service = await _make_klines(cfg, db, fake)
    try:
        await service.get_candles("BTCUSDT", "1h", 100)
        kline_calls = [c for c in fake.calls if c[0] == "/api/v3/klines" and c[1].get("symbol") == "BTCUSDT"]
        n_before = len(kline_calls)
        await service.get_candles("BTCUSDT", "1h", 100)  # warm → yeni fetch yok
        kline_calls = [c for c in fake.calls if c[0] == "/api/v3/klines" and c[1].get("symbol") == "BTCUSDT"]
        assert len(kline_calls) == n_before
    finally:
        await service.stop()


async def test_get_candles_unknown_symbol(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    universe, service = await _make_klines(cfg, db, fake)
    try:
        with pytest.raises(RasatError) as exc:
            await service.get_candles("NOPEUSDT", "15m", 100)
        assert exc.value.code == ErrorCode.INVALID_SYMBOL
    finally:
        await service.stop()


async def test_get_candles_invalid_timeframe(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    universe, service = await _make_klines(cfg, db, fake)
    try:
        with pytest.raises(RasatError) as exc:
            await service.get_candles("BTCUSDT", "7m", 100)
        assert exc.value.code == ErrorCode.INVALID_REQUEST
    finally:
        await service.stop()


async def test_get_candles_fetch_error_raises_stale(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    fake.fail_kline_for.add("BTCUSDT")
    universe, service = await _make_klines(cfg, db, fake)
    try:
        with pytest.raises(RasatError) as exc:
            await service.get_candles("BTCUSDT", "15m", 100)
        assert exc.value.code == ErrorCode.STALE_DATA
    finally:
        await service.stop()


# ---------- futures ----------

async def test_futures_pollers_write_context(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    universe = UniverseService(fake, cfg)
    await universe.sync()
    poller = FuturesContextPoller(fake, db, universe, cfg)

    n = await poller.poll_funding()
    assert n == 1
    await poller.poll_open_interest()
    await poller.poll_liquidations()

    def _q(conn):
        rows = conn.execute("SELECT symbol, type, event_time, freshness, value FROM futures_context").fetchall()
        return [dict(r) for r in rows]

    rows = await db.read(_q)
    types = {r["type"] for r in rows}
    assert {"funding_rate", "open_interest", "liquidation"} <= types
    assert all(r["freshness"] == "fresh" for r in rows)
    # event_time ile fetched_at ayrı saklanır
    liq = next(r for r in rows if r["type"] == "liquidation")
    assert liq["value"] == 90000.0 * 0.5


# ---------- pipeline ----------

async def test_pipeline_status_and_ticker(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    pipeline = DataPipeline(cfg, db, rest=fake, futures_rest=fake)
    pipeline.universe = UniverseService(fake, cfg)
    await pipeline.universe.sync()

    status = pipeline.status()
    assert "universe" in status and "ws" in status and "rate_limit" in status
    assert status["universe"]["status"] == "ok"

    from rasattrading_mcp.data.miniticker import TickerUpdate

    pipeline.ticker_cache.apply_updates(
        [TickerUpdate("BTCUSDT", 100.5, 99, 101, 98, 1000, 100000, 1.5, time.time())]
    )
    t = pipeline.get_ticker("BTCUSDT")
    assert t is not None and t["freshness"] == FRESHNESS_FRESH
    assert await pipeline.ensure_symbol("NOPEUSDT") is False

