"""2.21 FIX — Spot OCO emri: TP (LIMIT) + SL (STOP_LOSS_LIMIT) tek emir listesinde.

Canlı gözlem: aynı pozisyon için ayrı ayrı SL ve TP emri kurulamıyordu — ilk
emir bakiyeyi kilitliyor, ikincisi INSUFFICIENT_BALANCE alıyordu. Gerçek çözüm
Binance `/api/v3/orderList/oco`: biri dolunca diğeri borsada otomatik iptal.

Bu fix:
- `BinanceOrderBroker.place_oco` → orderList/oco (price=TP, stopPrice, stopLimitPrice),
- `OrderService.place_oco_order` → doğrulama + kayıt (order_type=OCO, üç fiyat) + broker,
- order kaydı stop_limit_price sütunu taşır (migration 10),
- `place_oco_order` tool'u.
"""

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.order_broker import BinanceOrderBroker
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.storage.orders import OrderService
from tests.helpers import FakeOrderBroker


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


async def test_broker_oco_sends_orderlist_params():
    """place_oco → /api/v3/orderList/oco: aboveType/belowType + fiyatlar + GTC."""
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

    async def oco_handler(request):
        if not _verify(request):
            return web.json_response({"code": -1022}, status=400)
        received["params"] = dict(request.query)
        received["path"] = request.path
        return web.json_response(
            {
                "orderListId": 555,
                "contingencyType": "OCO",
                "listStatusType": "EXEC_STARTED",
                "listOrderStatus": "EXECUTING",
                "orders": [
                    {"symbol": "ALICEUSDT", "orderId": 1, "clientOrderId": "a", "type": "LIMIT_MAKER"},
                    {"symbol": "ALICEUSDT", "orderId": 2, "clientOrderId": "b", "type": "STOP_LOSS_LIMIT"},
                ],
            }
        )

    app = web.Application()
    app.router.add_post("/api/v3/orderList/oco", oco_handler)
    async with TestServer(app) as server:
        async def creds(_aid):
            return (api_key, api_secret)

        broker = BinanceOrderBroker(
            server.make_url("/").human_repr(),
            credentials=creds,
            budget=RateLimitBudget(6000),
        )
        try:
            result = await broker.place_oco(
                account_id="a1",
                symbol="ALICEUSDT",
                side="SELL",
                quantity=1464.09,
                price=0.1378,
                stop_price=0.1225,
                stop_limit_price=0.1220,
                client_order_id="oco-1",
            )
            assert received["path"] == "/api/v3/orderList/oco"
            assert received["params"]["side"] == "SELL"
            assert float(received["params"]["quantity"]) == 1464.09
            # SELL: above = kâr hedefi (LIMIT_MAKER), below = stop (STOP_LOSS_LIMIT)
            assert received["params"]["aboveType"] == "LIMIT_MAKER"
            assert float(received["params"]["abovePrice"]) == 0.1378
            assert received["params"]["belowType"] == "STOP_LOSS_LIMIT"
            assert float(received["params"]["belowStopPrice"]) == 0.1225
            assert float(received["params"]["belowPrice"]) == 0.1220
            assert received["params"]["belowTimeInForce"] == "GTC"
            assert received["params"]["listClientOrderId"] == "oco-1"
            # eski biçim gönderilmemeli (Binance reddediyor)
            assert "price" not in received["params"]
            assert "stopPrice" not in received["params"]
            assert "stopLimitPrice" not in received["params"]
            assert result.exchange_order_id == "555"
        finally:
            await broker.close()


async def test_broker_oco_buy_swaps_above_below():
    """BUY OCO: above=STOP_LOSS_LIMIT, below=LIMIT_MAKER (kısa kapatma yönü)."""
    import hashlib
    import hmac

    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from rasattrading_mcp.data.rate_limit import RateLimitBudget

    api_key = "TESTKEY0000000000000000000000000000"
    api_secret = "TESTSECRET00000000000000000000000000"
    received: dict = {}

    async def oco_handler(request):
        received["params"] = dict(request.query)
        return web.json_response(
            {"orderListId": 666, "contingencyType": "OCO", "listStatusType": "EXEC_STARTED", "orders": []}
        )

    app = web.Application()
    app.router.add_post("/api/v3/orderList/oco", oco_handler)
    async with TestServer(app) as server:
        async def creds(_aid):
            return (api_key, api_secret)

        broker = BinanceOrderBroker(
            server.make_url("/").human_repr(),
            credentials=creds,
            budget=RateLimitBudget(6000),
        )
        try:
            await broker.place_oco(
                account_id="a1",
                symbol="ALICEUSDT",
                side="BUY",
                quantity=100.0,
                price=0.11,
                stop_price=0.13,
                stop_limit_price=0.135,
                client_order_id="oco-buy-1",
            )
            assert received["params"]["aboveType"] == "STOP_LOSS_LIMIT"
            assert float(received["params"]["aboveStopPrice"]) == 0.13
            assert float(received["params"]["abovePrice"]) == 0.135
            assert received["params"]["aboveTimeInForce"] == "GTC"
            assert received["params"]["belowType"] == "LIMIT_MAKER"
            assert float(received["params"]["belowPrice"]) == 0.11
        finally:
            await broker.close()


async def test_oco_validation_rejects_bad_stop_geometry(db, cfg):
    """stop_limit_price >= stop_price veya yön hatası → INVALID_REQUEST."""

    class _FakeAccounts:
        async def get_account(self, account_id):
            raise AssertionError("doğrulama servise ulaşmadan reddetmeli")

    service = OrderService(db, accounts=_FakeAccounts(), market=None, broker=None, risk=None, audit=None)
    with pytest.raises(RasatError) as exc:
        await service.place_oco_order(
            account_id="a", symbol="ALICEUSDT", side="SELL",
            quantity=10.0, price=0.1378, stop_price=0.1225, stop_limit_price=0.1300,
            idempotency_key="k",
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST


async def test_order_service_oco_persists_all_prices(cfg, db):
    """OCO kaydı: order_type=OCO, price=TP, stop_price, stop_limit_price saklanır."""

    class _FakeAccounts:
        async def get_account(self, account_id):
            return {"account_id": account_id, "trading_lock": "paper", "market": "spot"}

    class _FakeMarket:
        async def symbol_valid(self, symbol):
            return True

        async def price(self, symbol):
            return 0.1240

        async def filters(self, symbol):
            from rasattrading_mcp.position_sizing import SymbolFilters

            return SymbolFilters(
                symbol=symbol, base_asset="ALICE", quote_asset="USDT", status="TRADING",
                step_size=0.01, min_qty=0.01, max_qty=1e9, min_notional=5.0,
                tick_size=0.0001, min_price=0.0001, max_price=1000.0,
            )

    class _FakeRisk:
        async def get_policy(self, account_id):
            return {"policy_version": 0, "max_notional_per_order": None, "max_aggregate_exposure": None, "allowed_symbols": []}

        async def consume_override(self, *a, **k):
            return None

    broker = FakeOrderBroker(balances={"acc-1": {"USDT": 10000.0, "ALICE": 500.0}})
    service = OrderService(
        db, accounts=_FakeAccounts(), market=_FakeMarket(), broker=broker, risk=_FakeRisk(), audit=None,
    )

    result = await service.place_oco_order(
        account_id="acc-1",
        symbol="ALICEUSDT",
        side="SELL",
        quantity=100.0,
        price=0.1378,
        stop_price=0.1225,
        stop_limit_price=0.1220,
        idempotency_key="oco-test-1",
    )
    assert result["order_type"] == "OCO"
    assert result["price"] == 0.1378
    assert result["stop_price"] == 0.1225
    assert result["stop_limit_price"] == 0.1220
    # paper hesap broker'a gitmez (doğru davranış); broker passthrough'u test 1'de doğrulandı
    assert broker.placed == []

    def _q(conn):
        row = conn.execute(
            "SELECT order_type, price, stop_price, stop_limit_price FROM orders WHERE idempotency_key='oco-test-1'"
        ).fetchone()
        return dict(row) if row else None

    stored = await db.read(_q)
    assert stored["order_type"] == "OCO"
    assert stored["price"] == 0.1378
    assert stored["stop_price"] == 0.1225
    assert stored["stop_limit_price"] == 0.1220
