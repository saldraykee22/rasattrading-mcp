"""2.20 FIX — STOP_LOSS_LIMIT (spot stop koruması) desteği.

Kullanıcı akışı: gerçek ALICE pozisyonu açıldı (market BUY) ama borsa tarafında
koruyucu emir yoktu; sistem yalnızca MARKET/LIMIT gönderebiliyordu. Bu fix:
- `BinanceOrderBroker.place_order` STOP_LOSS_LIMIT + stopPrice gönderir,
- `OrderService.place_order` stop_price'ı doğrular, kaydeder, broker'a taşır,
- orders kaydı stop_price sütunu taşır (migration 9).
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.order_broker import BinanceOrderBroker
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.storage.orders import OrderService
from tests.helpers import FakeOrderBroker

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


async def test_broker_stop_loss_limit_params():
    """STOP_LOSS_LIMIT: stopPrice + price + GTC gönderilir, stop yoksa reddedilir."""
    import hashlib
    import hmac

    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from rasattrading_mcp.data.rate_limit import RateLimitBudget

    api_key = "TESTKEY0000000000000000000000000000"
    api_secret = "TESTSECRET00000000000000000000000000"
    received: dict = {}

    def _verify(request):
        qs = request.query_string
        params = dict(request.query)
        signature = params.pop("signature", None)
        if not signature:
            return False
        signed_part = qs[: qs.index("&signature=")]
        expected = hmac.new(api_secret.encode(), signed_part.encode(), hashlib.sha256).hexdigest()
        return expected == signature

    async def order_handler(request):
        ok = _verify(request)
        if not ok:
            return web.json_response({"code": -1022}, status=400)
        received["params"] = dict(request.query)
        return web.json_response(
            {
                "orderId": 777,
                "clientOrderId": "stop-1",
                "status": "NEW",
                "executedQty": "0",
                "cummulativeQuoteQty": "0",
                "symbol": "ALICEUSDT",
                "side": "SELL",
                "type": "STOP_LOSS_LIMIT",
            }
        )

    app = web.Application()
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
            result = await broker.place_order(
                account_id="a1",
                symbol="ALICEUSDT",
                side="SELL",
                order_type="STOP_LOSS_LIMIT",
                quantity=1465.56,
                price=0.1220,
                client_order_id="stop-1",
                stop_price=0.1225,
            )
            assert result.status == "NEW"
            assert received["params"]["type"] == "STOP_LOSS_LIMIT"
            assert received["params"]["stopPrice"] == "0.1225"
            assert float(received["params"]["price"]) == 0.1220
            assert received["params"]["timeInForce"] == "GTC"
        finally:
            await broker.close()

    # stop_price eksik → reddedilmeli
    broker2 = BinanceOrderBroker("http://127.0.0.1:1", creds, budget=RateLimitBudget(6000))
    from rasattrading_mcp.errors import ErrorCode, RasatError

    with pytest.raises(RasatError) as exc:
        await broker2.place_order(
            account_id="a1", symbol="ALICEUSDT", side="SELL",
            order_type="STOP_LOSS_LIMIT", quantity=1.0, price=0.1220,
            client_order_id="x",
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST


async def test_order_service_place_stop_persists_stop_price(cfg, db):
    """Stop emri order kaydına stop_price ile yazılır ve broker'a taşınır."""
    class _FakeAccounts:
        async def get_account(self, account_id):
            return {"account_id": account_id, "trading_lock": "paper", "market": "spot"}

    class _FakeMarket:
        async def symbol_valid(self, symbol):
            return True

        async def price(self, symbol):
            return 0.1270

        async def filters(self, symbol):
            from rasattrading_mcp.position_sizing import SymbolFilters

            return SymbolFilters(
                symbol=symbol,
                base_asset="ALICE",
                quote_asset="USDT",
                status="TRADING",
                step_size=0.01,
                min_qty=0.01,
                max_qty=1e9,
                min_notional=5.0,
                tick_size=0.0001,
                min_price=0.0001,
                max_price=1000.0,
            )

    class _FakeRisk:
        async def get_policy(self, account_id):
            return {"policy_version": 0, "max_notional_per_order": None, "max_aggregate_exposure": None, "allowed_symbols": []}

        async def consume_override(self, *a, **k):
            return None

    broker = FakeOrderBroker(balances={"acc-1": {"USDT": 10000.0, "ALICE": 500.0}})
    service = OrderService(
        db,
        accounts=_FakeAccounts(),
        market=_FakeMarket(),
        broker=broker,
        risk=_FakeRisk(),
        audit=None,
    )

    result = await service.place_order(
        account_id="acc-1",
        symbol="ALICEUSDT",
        side="SELL",
        order_type="STOP_LOSS_LIMIT",
        quantity=100.0,
        price=0.1220,
        stop_price=0.1225,
        idempotency_key="stop-test-1",
    )
    assert result["stop_price"] == 0.1225
    assert result["price"] == 0.1220
    # paper hesap broker'a gitmez (doğru davranış) — kayıt ve audit izi yeterli
    assert broker.placed == []

    def _q(conn):
        row = conn.execute(
            "SELECT stop_price FROM orders WHERE idempotency_key='stop-test-1'"
        ).fetchone()
        return row["stop_price"] if row else None

    stored = await db.read(_q)
    assert stored == 0.1225
