import asyncio
import json
import time

import pytest

from rasattrading_mcp.config import Config, TIMEFRAME_SECONDS
from rasattrading_mcp.data.binance_client import BinanceREST, kline_weight
from rasattrading_mcp.data.futures import FuturesContextPoller
from rasattrading_mcp.data.klines import KlineService, parse_klines
from rasattrading_mcp.data.liquidation_ws import LiquidationWSClient, parse_force_order_arr
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
    await budget.acquire(2)  # Must wait until the window resets.
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
        client = BinanceREST(server.make_url("/").human_repr(), RateLimitBudget(1000), retries=1)
        try:
            with pytest.raises(RasatError) as exc:
                await client.get("/x")
            assert exc.value.code == ErrorCode.UNAUTHORIZED
        finally:
            await client.close()


async def test_signed_broker_signature_verifies_against_sent_query():
    """Verify the signature against the exact query string that was sent.

    Binance calculates the signature from the raw query order it receives; if
    the client leaves params as a dict for aiohttp but signs a sorted() string,
    the order mismatch produces -1022 (a bug caught on the live API and real
    account verification after f8fcd2b).
    """
    import hashlib
    import hmac

    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from rasattrading_mcp.data.order_broker import BinanceOrderBroker

    api_key = "TESTKEY0000000000000000000000000000"
    api_secret = "TESTSECRET00000000000000000000000000"
    received: dict = {}

    def _verify(request) -> tuple[bool, dict]:
        qs = request.query_string
        params = dict(request.query)  # Parsed.
        signature = params.pop("signature", None)
        if not signature:
            return False, {"code": -1022, "msg": "Signature for this request is not valid."}
        signed_part = qs[: qs.index("&signature=")]
        expected = hmac.new(api_secret.encode("utf-8"), signed_part.encode("utf-8"), hashlib.sha256).hexdigest()
        if expected != signature:
            return False, {"code": -1022, "msg": "Signature for this request is not valid."}
        return True, {"code": 200}

    app = web.Application()

    async def account_handler(request):
        ok, body = _verify(request)
        received["account_qs"] = request.query_string
        if not ok:
            return web.json_response(body, status=400)
        return web.json_response(
            {"balances": [{"asset": "USDT", "free": "123.45", "locked": "0"}], "canTrade": True}
        )

    async def order_handler(request):
        ok, body = _verify(request)
        received["order_qs"] = request.query_string
        if not ok:
            return web.json_response(body, status=400)
        return web.json_response(
            {
                "orderId": 12345,
                "clientOrderId": "abc-1",
                "status": "FILLED",
                "executedQty": "0.001",
                "cummulativeQuoteQty": "64.5",
                "symbol": "BTCUSDT",
                "side": "BUY",
                "type": "MARKET",
            }
        )

    app.router.add_get("/api/v3/account", account_handler)
    app.router.add_post("/api/v3/order", order_handler)

    async with TestServer(app) as server:
        async def creds(_aid):
            return (api_key, api_secret)

        broker = BinanceOrderBroker(
            server.make_url("/").human_repr(),
            credentials=creds,
            budget=RateLimitBudget(6000),
        )
        try:
            balances = await broker.get_balance(account_id="a1")
            assert balances["USDT"] == 123.45
            assert received["account_qs"].startswith("timestamp=")
            assert "recvWindow" in received["account_qs"]
            detail = await broker.get_balance_detail(account_id="a1")
            assert detail["USDT"] == {"free": 123.45, "locked": 0.0}

            result = await broker.place_order(
                account_id="a1",
                symbol="BTCUSDT",
                side="BUY",
                order_type="MARKET",
                quantity=0.001,
                price=None,
                client_order_id="abc-1",
            )
            assert result.status == "FILLED"
            assert result.exchange_order_id == "12345"
            assert "symbol=BTCUSDT" in received["order_qs"]
        finally:
            await broker.close()


async def test_broker_get_balance_detail_includes_locked():
    """3.21: get_balance_detail carries free + locked separately; get_balance only returns free.

    In the user's real-account test, amounts locked in open orders and the value
    of held base assets were missing—the balance query discarded locked amounts.
    Broker details now return both amounts.
    """
    import hashlib
    import hmac

    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from rasattrading_mcp.data.order_broker import BinanceOrderBroker

    api_key = "TESTKEY0000000000000000000000000000"
    api_secret = "TESTSECRET00000000000000000000000000"

    def _verify(request) -> tuple[bool, dict]:
        qs = request.query_string
        params = dict(request.query)
        signature = params.pop("signature", None)
        if not signature:
            return False, {"code": -1022}
        signed_part = qs[: qs.index("&signature=")]
        expected = hmac.new(api_secret.encode("utf-8"), signed_part.encode("utf-8"), hashlib.sha256).hexdigest()
        if expected != signature:
            return False, {"code": -1022}
        return True, {"code": 200}

    app = web.Application()

    async def account_handler(request):
        ok, body = _verify(request)
        if not ok:
            return web.json_response(body, status=400)
        return web.json_response(
            {
                "balances": [
                    {"asset": "USDT", "free": "100.0", "locked": "23.45"},
                    {"asset": "BTC", "free": "0.5", "locked": "0.25"},
                ],
                "canTrade": True,
            }
        )

    app.router.add_get("/api/v3/account", account_handler)

    async with TestServer(app) as server:
        async def creds(_aid):
            return (api_key, api_secret)

        broker = BinanceOrderBroker(
            server.make_url("/").human_repr(),
            credentials=creds,
            budget=RateLimitBudget(6000),
        )
        try:
            detail = await broker.get_balance_detail(account_id="a1")
            assert detail["USDT"] == {"free": 100.0, "locked": 23.45}
            assert detail["BTC"] == {"free": 0.5, "locked": 0.25}
            free_only = await broker.get_balance(account_id="a1")
            assert free_only == {"USDT": 100.0, "BTC": 0.5}
        finally:
            await broker.close()


# ---------- universe ----------

async def test_universe_sync_filters(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(
        ["BTCUSDT", "ETHUSDT"],
        extra_exchange_entries=[
            {"symbol": "XXXUSD", "status": "TRADING", "quoteAsset": "USD"},  # Not USDT.
            {"symbol": "DELETEDUSDT", "status": "BREAK", "quoteAsset": "USDT"},  # Not TRADING.
        ],
    )
    uni = UniverseService(fake, cfg)
    n = await uni.sync()
    assert n == 2  # USDT + TRADING only
    assert uni.contains("BTCUSDT")
    assert not uni.contains("XXXUSD")
    assert not uni.contains("DELETEDUSDT")


async def test_universe_ensure_contains_unknown(cfg, db):
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    uni = UniverseService(fake, cfg)
    await uni.sync()
    assert await uni.ensure_contains("BTCUSDT") is True
    # Unknown symbol → no resync when universe is fresh → False.
    assert await uni.ensure_contains("NOPEUSDT") is False


# ---------- miniticker ----------

MINI_PAYLOAD = json.dumps(
    [
        {"e": "24hrMiniTicker", "s": "BTCUSDT", "c": "100.5", "o": "99.0", "h": "101.0", "l": "98.0",
         "v": "1000", "q": "100000", "E": 1700000000000}
    ]
)

MINI_PAYLOAD_COMBINED = json.dumps(
    {"stream": "!miniTicker@arr", "data": json.loads(MINI_PAYLOAD)}
)


def test_parse_miniticker():
    """A real miniTicker item does not carry P (price change %)—calculate it from o/c."""
    updates = parse_miniticker_arr(MINI_PAYLOAD)
    assert len(updates) == 1
    assert updates[0].symbol == "BTCUSDT"
    assert updates[0].last == 100.5
    # (100.5 - 99.0) / 99.0 * 100 ≈ 1.5151
    assert abs(updates[0].price_change_pct - 1.5151) < 0.001


def test_parse_miniticker_with_p_field():
    """Accept the P field from the 24hr ticker stream too."""
    payload = json.dumps(
        [{"e": "24hrTicker", "s": "BTCUSDT", "c": "100.5", "o": "99.0", "h": "101.0", "l": "98.0",
          "v": "1000", "q": "100000", "P": "1.52", "E": 1700000000000}]
    )
    updates = parse_miniticker_arr(payload)
    assert updates[0].price_change_pct == 1.52


def test_parse_miniticker_combined_stream_envelope():
    """Parse Binance's combined stream wrapper ({stream, data}) (smoke-test finding)."""
    updates = parse_miniticker_arr(MINI_PAYLOAD_COMBINED)
    assert len(updates) == 1
    assert updates[0].symbol == "BTCUSDT"
    assert updates[0].last == 100.5
    assert updates[0].event_time == 1700000000.0


def test_parse_miniticker_garbage():
    assert parse_miniticker_arr(json.dumps({"stream": "x"})) == []
    assert parse_miniticker_arr("not json") == []


def test_ticker_cache_freshness():
    cache = TickerCache(stale_after=30)
    cache.apply_updates(parse_miniticker_arr(MINI_PAYLOAD))
    assert cache.status == "connected"
    assert cache.freshness_for("BTCUSDT") == FRESHNESS_FRESH

    cache.mark_stale("ws closed")
    assert cache.freshness_for("BTCUSDT") == FRESHNESS_STALE
    assert cache.get("BTCUSDT")["freshness"] == FRESHNESS_STALE


async def test_miniticker_ws_reconnect_marks_stale():
    import websockets

    cache = TickerCache(stale_after=30)
    frame = [dict(s="BTCUSDT", c="100.5", o="99", h="101", l="98", v="100", q="10000", P="1.5", E=0)]

    async def handler(ws):
        # Send a combined-stream wrapper like real Binance.
        await ws.send(json.dumps({"stream": "!miniTicker@arr", "data": frame}))
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

        # Cut the connection → WS disconnects → data must be marked stale.
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
    # Binance raw `open_time` is milliseconds; normalize to seconds for one unit standard.
    raw = [[1700000000000, "1", "2", "0", "1.5", "10", 1700000100000, "100", 5, "0", "0", "0"]]
    rows = parse_klines(raw)
    assert rows[0]["open_time"] == 1700000000
    assert rows[0]["close"] == 1.5
    assert rows[0]["trades"] == 5


async def test_parse_klines_seconds_passthrough():
    # Fixture data already in seconds is preserved as-is (test data such as FakeRest).
    raw = [[1700000000, "1", "2", "0", "1.5", "10", 1700000100, "100", 5, "0", "0", "0"]]
    rows = parse_klines(raw)
    assert rows[0]["open_time"] == 1700000000


async def test_parse_klines_bad_row_skipped():
    raw = [["not-a-number", "1", "2", "0", "1.5", "10"], [1700000000000, "1", "2", "0", "1.5", "10", 1700000100000, "100", 5]]
    rows = parse_klines(raw)
    assert len(rows) == 1
    assert rows[0]["open_time"] == 1700000000


from tests.helpers import FakeClock


async def _make_klines(cfg, db, fake):
    universe = UniverseService(fake, cfg)
    await universe.sync()
    service = KlineService(fake, fake, db, universe, cfg, clock=FakeClock())
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
        # Priority: on-demand (prio 0) is processed before backfill (prio 10) even if queued later.
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
        await service.get_candles("BTCUSDT", "1h", 100)  # Warm → no new fetch.
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


async def test_store_drops_forming_bar(cfg, db):
    """Closed-candle rule (1.6): a forming (unclosed) bar is not written to `candles`.

    Binance `/klines` returns a still-forming bar as the last bar; if stored with
    partial volume, `MAX(open_time)==last_closed` after it closes, so catch-up is
    not triggered and the last "closed" candle remains partial (90 vs 400-870).
    """
    from tests.helpers import FakeRest

    class _FormingRest(FakeRest):
        async def get(self, path, params=None, weight=1):
            if path == "/api/v3/klines":
                raw = self._gen_klines(params.get("symbol"), params.get("interval", "1h"), int(params.get("limit", 100)))
                period = TIMEFRAME_SECONDS[params.get("interval", "1h")]
                latest_closed = int(time.time() // period) * period - period
                forming = latest_closed + period
                raw.append([forming, "100", "101", "99", "100.5", "90", forming + period, "9000", 3, "0", "0", "0"])
                return raw
            return await super().get(path, params, weight)

    period = TIMEFRAME_SECONDS["1h"]
    latest_closed = int(time.time() // period) * period - period
    forming = latest_closed + period  # Currently forming bar.

    fake = _FormingRest(["BTCUSDT"])
    universe, service = await _make_klines(cfg, db, fake)
    try:
        rows = await service.get_candles("BTCUSDT", "1h", 10)
        # Forming bar is not returned—the last bar must be closed.
        assert all(r["open_time"] <= latest_closed for r in rows)
        assert rows[-1]["volume"] == 1000.0  # Partial volume (90) was not stored.

        def _q(conn):
            row = conn.execute(
                "SELECT MAX(open_time) AS m FROM candles WHERE symbol='BTCUSDT' AND timeframe='1h'"
            ).fetchone()
            return int(row["m"]) if row["m"] is not None else None

        max_open = await db.read(_q)
        assert max_open == latest_closed
        assert max_open != forming
    finally:
        await service.stop()


async def test_scheduler_catchup_does_not_store_forming_bar(cfg, db):
    """1.6 race condition: scheduler close trigger must not store a forming bar.

    When close is triggered, Binance returns a limit-sized kline set; the last bar
    may still be forming. `_store` must discard it—otherwise, when it closes,
    `_needs_catchup` incorrectly considers it current and never fetches full volume.
    """
    from tests.helpers import FakeRest

    period = 3600
    latest_closed = int(time.time() // period) * period - period

    fake = FakeRest(["BTCUSDT"])
    universe, service = await _make_klines(cfg, db, fake)
    try:
        # Scheduler closed-candle capture: target open_time = latest_closed.
        await service._enqueue_closed_bar_pass("1h", latest_closed)
        # Wait for workers to finish.
        for _ in range(200):
            if service._queue.qsize() == 0 and not service._in_flight:
                break
            await asyncio.sleep(0.05)

        def _q(conn):
            row = conn.execute(
                "SELECT MAX(open_time) AS m FROM candles WHERE symbol='BTCUSDT' AND timeframe='1h'"
            ).fetchone()
            return int(row["m"]) if row["m"] is not None else None

        max_open = await db.read(_q)
        assert max_open == latest_closed
        assert max_open != latest_closed + period
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

    def _q(conn):
        rows = conn.execute("SELECT symbol, type, event_time, freshness, value FROM futures_context").fetchall()
        return [dict(r) for r in rows]

    rows = await db.read(_q)
    types = {r["type"] for r in rows}
    assert {"funding_rate", "open_interest"} <= types
    assert all(r["freshness"] == "fresh" for r in rows)


# ---------- liquidation ws ----------

FORCE_ORDER_PAYLOAD = json.dumps(
    {
        "e": "forceOrder",
        "E": 1700000000000,
        "o": {
            "s": "BTCUSDT",
            "S": "SELL",
            "o": "LIMIT",
            "f": "IOC",
            "q": "0.5",
            "p": "90000.0",
            "ap": "90000.0",
            "X": "FILLED",
            "l": "0.5",
            "z": "0.5",
            "T": 1700000000000,
        },
    }
)

FORCE_ORDER_PAYLOAD_COMBINED = json.dumps(
    {"stream": "!forceOrder@arr", "data": json.loads(FORCE_ORDER_PAYLOAD)}
)


def test_parse_force_order():
    events = parse_force_order_arr(FORCE_ORDER_PAYLOAD)
    assert len(events) == 1
    e = events[0]
    assert e.symbol == "BTCUSDT"
    assert e.side == "SELL"
    assert e.price == 90000.0
    assert e.qty == 0.5
    # `E` is milliseconds; convert to seconds for one unit standard.
    assert e.event_time == 1700000000


def test_parse_force_order_combined_stream_envelope():
    """Parse Binance's combined stream wrapper ({stream, data})."""
    events = parse_force_order_arr(FORCE_ORDER_PAYLOAD_COMBINED)
    assert len(events) == 1
    assert events[0].symbol == "BTCUSDT"
    assert events[0].event_time == 1700000000


def test_parse_force_order_garbage_and_bad_fields():
    assert parse_force_order_arr("not json") == []
    assert parse_force_order_arr(json.dumps({"stream": "x"})) == []
    # Missing/malformed field → skip event (do not raise an error).
    assert parse_force_order_arr(json.dumps({"o": {"s": "BTCUSDT"}})) == []
    assert parse_force_order_arr(json.dumps({"e": "forceOrder", "o": {"s": "BTCUSDT", "p": "x"}})) == []


async def test_liquidation_ws_writes_db_and_marks_stale(cfg, db):
    """Write an event from the real WS server to the DB; a broken connection must set `disconnected`."""
    import websockets

    frame = json.loads(FORCE_ORDER_PAYLOAD)

    async def handler(ws):
        await ws.send(json.dumps({"stream": "!forceOrder@arr", "data": frame}))
        await asyncio.sleep(30)

    server = await websockets.serve(handler, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        client = LiquidationWSClient(f"ws://127.0.0.1:{port}/ws", db, stale_after=30)
        stop = asyncio.Event()
        task = asyncio.create_task(client.run(stop))
        for _ in range(100):
            if client.status == "connected":
                break
            await asyncio.sleep(0.05)
        assert client.status == "connected"

        def _q(conn):
            rows = conn.execute(
                "SELECT symbol, type, event_time, freshness, value, extra FROM futures_context"
            ).fetchall()
            return [dict(r) for r in rows]

        rows = await db.read(_q)
        liq = [r for r in rows if r["type"] == "liquidation"]
        assert len(liq) == 1
        assert liq[0]["symbol"] == "BTCUSDT"
        assert liq[0]["value"] == 90000.0 * 0.5
        assert liq[0]["freshness"] == "fresh"
        extra = json.loads(liq[0]["extra"])
        assert extra["side"] == "SELL"
        assert client.events_written == 1

        # Cut the connection → WS disconnects → state `disconnected`.
        server.close()
        await server.wait_closed()
        for _ in range(100):
            if client.status == "disconnected":
                break
            await asyncio.sleep(0.05)
        assert client.status == "disconnected"

        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    finally:
        server.close()
        await server.wait_closed()


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


async def test_pipeline_status_derives_liquidation_from_ws(cfg, db):
    """Derive liquidation state from the WS client, not REST polling."""
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    pipeline = DataPipeline(cfg, db, rest=fake, futures_rest=fake)
    pipeline.universe = UniverseService(fake, cfg)

    status = pipeline.status()
    assert status["futures"]["liquidation"] == "disconnected"

    pipeline.liquidation_ws.mark_connected()
    assert pipeline.status()["futures"]["liquidation"] == "connected"
