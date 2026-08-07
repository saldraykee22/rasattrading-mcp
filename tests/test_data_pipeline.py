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
        client = BinanceREST(server.make_url("/").human_repr(), RateLimitBudget(1000), retries=1)
        try:
            with pytest.raises(RasatError) as exc:
                await client.get("/x")
            assert exc.value.code == ErrorCode.UNAUTHORIZED
        finally:
            await client.close()


async def test_signed_broker_signature_verifies_against_sent_query():
    """İmza, gönderilen query string'in birebir aynısı üzerinden doğrulanmalı.

    Binance imzayı alınan ham query sırasına göre hesaplar; istemci aiohttp'e
    params= dict bırakıp sorted() string'i imzalarsa sıra farkı -1022 üretir
    (canlı API'de yakalanan bug, f8fcd2b sonrası gerçek hesap doğrulaması).
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
        params = dict(request.query)  # parse edilmiş
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
    """3.21: get_balance_detail free + locked'ı ayrı taşır; get_balance yalnızca free.

    Kullanıcının gerçek hesap testinde açık emirlerde kilitli (locked) miktarlar
    ve elde tutulan base asset değeri görünmüyordu — bakiye sorgusu locked'ı
    atıyordu. Broker detayı artık her iki miktarı da döndürür.
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
         "v": "1000", "q": "100000", "E": 1700000000000}
    ]
)

MINI_PAYLOAD_COMBINED = json.dumps(
    {"stream": "!miniTicker@arr", "data": json.loads(MINI_PAYLOAD)}
)


def test_parse_miniticker():
    """Gerçek miniTicker öğesi P (price change %) taşımaz — o/c'den hesaplanmalı."""
    updates = parse_miniticker_arr(MINI_PAYLOAD)
    assert len(updates) == 1
    assert updates[0].symbol == "BTCUSDT"
    assert updates[0].last == 100.5
    # (100.5 - 99.0) / 99.0 * 100 ≈ 1.5151
    assert abs(updates[0].price_change_pct - 1.5151) < 0.001


def test_parse_miniticker_with_p_field():
    """24hr ticker stream'inden gelen P alanı da kabul edilir."""
    payload = json.dumps(
        [{"e": "24hrTicker", "s": "BTCUSDT", "c": "100.5", "o": "99.0", "h": "101.0", "l": "98.0",
          "v": "1000", "q": "100000", "P": "1.52", "E": 1700000000000}]
    )
    updates = parse_miniticker_arr(payload)
    assert updates[0].price_change_pct == 1.52


def test_parse_miniticker_combined_stream_envelope():
    """Binance combined stream sarmalı ({stream, data}) ayrıştırılmalı (smoke test bulgusu)."""
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

    cache.mark_stale("ws kapandı")
    assert cache.freshness_for("BTCUSDT") == FRESHNESS_STALE
    assert cache.get("BTCUSDT")["freshness"] == FRESHNESS_STALE


async def test_miniticker_ws_reconnect_marks_stale():
    import websockets

    cache = TickerCache(stale_after=30)
    frame = [dict(s="BTCUSDT", c="100.5", o="99", h="101", l="98", v="100", q="10000", P="1.5", E=0)]

    async def handler(ws):
        # Gerçek Binance gibi combined-stream sarmalı gönder
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
    # Binance ham `open_time` milisaniyedir; tek birim standardı için saniyeye normalize edilir.
    raw = [[1700000000000, "1", "2", "0", "1.5", "10", 1700000100000, "100", 5, "0", "0", "0"]]
    rows = parse_klines(raw)
    assert rows[0]["open_time"] == 1700000000
    assert rows[0]["close"] == 1.5
    assert rows[0]["trades"] == 5


async def test_parse_klines_seconds_passthrough():
    # Zaten saniye olan fixture verisi aynen korunur (FakeRest gibi test verisi).
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


async def test_store_drops_forming_bar(cfg, db):
    """Kapalı mum kuralı (1.6): oluşmakta olan (kapanmamış) bar `candles`'a yazılmaz.

    Binance `/klines` son bar olarak hâlâ oluşmakta olan barı döndürür; kısmi
    hacimle saklanırsa kapanınca `MAX(open_time)==last_closed` olduğu için
    catchup tetiklenmez ve son "kapalı" mum kısmi hacimle kalır (90 vs 400-870).
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
    forming = latest_closed + period  # şu an oluşmakta olan bar

    fake = _FormingRest(["BTCUSDT"])
    universe, service = await _make_klines(cfg, db, fake)
    try:
        rows = await service.get_candles("BTCUSDT", "1h", 10)
        # forming bar dönmez — son bar kapanmış olan olmalı
        assert all(r["open_time"] <= latest_closed for r in rows)
        assert rows[-1]["volume"] == 1000.0  # kısmi hacim (90) saklanmadı

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
    """1.6 yarış durumu: scheduler kapanış tetiklemesi forming bar'ı saklamamalı.

    Kapanış tetiklendiğinde Binance'ten limit'lik kline gelir; en son bar hâlâ
    oluşmakta olabilir. `_store` onu atmalı — aksi halde o bar kapanınca
    `_needs_catchup` yanlışlıkla "güncel" sanır ve tam hacim hiç çekilmez.
    """
    from tests.helpers import FakeRest

    period = 3600
    latest_closed = int(time.time() // period) * period - period

    fake = FakeRest(["BTCUSDT"])
    universe, service = await _make_klines(cfg, db, fake)
    try:
        # Scheduler kapalı mum yakalama: hedef open_time = latest_closed
        await service._enqueue_closed_bar_pass("1h", latest_closed)
        # worker'ların bitmesini bekle
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
    # `E` ms'dir; tek birim standardı gereği saniyeye iner
    assert e.event_time == 1700000000


def test_parse_force_order_combined_stream_envelope():
    """Binance combined stream sarmalı ({stream, data}) ayrıştırılmalı."""
    events = parse_force_order_arr(FORCE_ORDER_PAYLOAD_COMBINED)
    assert len(events) == 1
    assert events[0].symbol == "BTCUSDT"
    assert events[0].event_time == 1700000000


def test_parse_force_order_garbage_and_bad_fields():
    assert parse_force_order_arr("not json") == []
    assert parse_force_order_arr(json.dumps({"stream": "x"})) == []
    # eksik/bozuk alan → event atlanır (hata yükseltilmez)
    assert parse_force_order_arr(json.dumps({"o": {"s": "BTCUSDT"}})) == []
    assert parse_force_order_arr(json.dumps({"e": "forceOrder", "o": {"s": "BTCUSDT", "p": "x"}})) == []


async def test_liquidation_ws_writes_db_and_marks_stale(cfg, db):
    """Gerçek WS sunucusundan gelen event DB'ye yazılmalı; kopuk bağlantı `disconnected` yapmalı."""
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

        # Bağlantıyı kes → WS kopar → durum `disconnected`
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
    """Liquidation durumu REST poll'dan değil, WS client'tan türetilir."""
    from tests.helpers import FakeRest

    fake = FakeRest(["BTCUSDT"])
    pipeline = DataPipeline(cfg, db, rest=fake, futures_rest=fake)
    pipeline.universe = UniverseService(fake, cfg)

    status = pipeline.status()
    assert status["futures"]["liquidation"] == "disconnected"

    pipeline.liquidation_ws.mark_connected()
    assert pipeline.status()["futures"]["liquidation"] == "connected"

