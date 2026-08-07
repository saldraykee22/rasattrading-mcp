"""2.21 FIX — Spot OCO emri: TP (LIMIT) + SL (STOP_LOSS_LIMIT) tek emir listesinde.

Canlı gözlem: aynı pozisyon için ayrı ayrı SL ve TP emri kurulamıyordu — ilk
emir bakiyeyi kilitliyor, ikincisi INSUFFICIENT_BALANCE alıyordu. Gerçek çözüm
Binance `/api/v3/orderList/oco`: biri dolunca diğeri borsada otomatik iptal.

Bu fix:
- `BinanceOrderBroker.place_oco` → orderList/oco (price=TP, stopPrice, stopLimitPrice),
- `OrderService.place_oco_order` → doğrulama + kayıt (order_type=OCO, üç fiyat) + broker,
- order kaydı stop_limit_price sütunu taşır (migration 10),
- `place_oco_order` tool'u.

T1 (OCO order broker):
- `_handle_existing` OCO kaydını `query_oco` ile reconcile eder (tekil `query_order`
  hep -2013 döner) — idem-retry OCO'yu UNKNOWN'a yanlış düşürmez.
- `BinanceOrderBroker.cancel_oco` → `DELETE /api/v3/orderList`; kill switch OCO
  satırlarını `cancel_oco` ile iptal eder.
- `reconcile_open_orders` UNKNOWN kayıtları da tarar (açılışta yeniden doğrulanır).
- `find_unprotected_positions` hesap erişim hatalarını `errors`/`complete` ile raporlar.
"""

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.order_broker import BinanceOrderBroker, OrderResult, to_client_order_id
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.position_sizing import SymbolFilters
from rasattrading_mcp.storage.accounts import AccountService
from rasattrading_mcp.storage.audit import AuditLog
from rasattrading_mcp.storage.credentials import SecretStore
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.storage.orders import OrderService
from rasattrading_mcp.storage.risk_policy import RiskPolicyService
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


# ---------- T1: OCO reconcile/cancel fix'leri ----------


_OCO_FILTERS = SymbolFilters(
    symbol="ALICEUSDT",
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


class _OcoMarket:
    """MarketFeed taklidi: ALICEUSDT fiyatı + filtreleri (real OCO akışı için)."""

    def __init__(self) -> None:
        self.price_map = {"ALICEUSDT": 0.1240}

    async def symbol_valid(self, symbol: str) -> bool:
        return symbol in self.price_map

    async def price(self, symbol: str) -> float | None:
        return self.price_map.get(symbol)

    async def filters(self, symbol: str) -> SymbolFilters | None:
        if symbol not in self.price_map:
            return None
        return SymbolFilters(**{**_OCO_FILTERS.__dict__, "symbol": symbol})


async def _real_oco_ctx(db: Database) -> dict:
    accounts = AccountService(db, secret_store=SecretStore(), audit=AuditLog(db))
    risk = RiskPolicyService(db, audit=AuditLog(db))
    broker = FakeOrderBroker()
    market = _OcoMarket()
    service = OrderService(
        db, accounts=accounts, risk=risk, broker=broker, market=market, audit=AuditLog(db),
    )
    created = await accounts.add_account(label="oco-real", api_key="AK_oco", api_secret="AS_oco")
    await accounts.enable_real_trading(created["account_id"], actor="test")
    broker.balances[created["account_id"]] = {"USDT": 10000.0, "ALICE": 500.0}
    return {"db": db, "accounts": accounts, "risk": risk, "broker": broker,
            "market": market, "service": service, "account_id": created["account_id"]}


async def test_oco_idem_retry_reconciles_via_query_oco(cfg, db):
    """T1-1: OCO idem-retry `query_oco` ile reconcile edilir (tekil `query_order` değil).

    Zaman aşımından sonra UNKNOWN'a düşen OCO, aynı key ile retry'de borsada
    FILLED görünüyorsa `query_oco` üzerinden doğru duruma çekilir. `query_order`
    OCO bacaklarını bulamadığı için (Binance kendi cid üretir) hiç çağrılmamalı.
    """
    ctx = await _real_oco_ctx(db)
    service = ctx["service"]
    broker = ctx["broker"]
    cid = to_client_order_id("oco-retry-1")
    # ilk deneme: ağ zaman aşımı → UNKNOWN (borsada doğrulanamadı)
    broker.place_errors[cid] = RasatError(ErrorCode.TIMEOUT, "OCO gönderimi zaman aşımı")
    broker.oco_query_results[cid] = None
    broker.query_results[cid] = None  # query_order kullanılırsa da bulunamaz

    first = await service.place_oco_order(
        account_id=ctx["account_id"], symbol="ALICEUSDT", side="SELL",
        quantity=100.0, price=0.1378, stop_price=0.1225, stop_limit_price=0.1220,
        idempotency_key="oco-retry-1",
    )
    assert first["status"] == "UNKNOWN"
    assert len(broker.place_errors) == 1
    assert broker.oco_queries  # timeout reconcile query_oco ile yapıldı

    # retry: aynı key → UNKNOWN kayıt yeniden sorgulanır; artık borsada FILLED
    broker.place_errors.pop(cid)
    broker.oco_query_results[cid] = OrderResult(
        status="FILLED", exchange_order_id="OL-99", executed_qty=100.0, avg_price=0.13,
    )

    second = await service.place_oco_order(
        account_id=ctx["account_id"], symbol="ALICEUSDT", side="SELL",
        quantity=100.0, price=0.1378, stop_price=0.1225, stop_limit_price=0.1220,
        idempotency_key="oco-retry-1",
    )
    assert second["status"] == "FILLED"
    assert second["exchange_order_id"] == "OL-99"
    assert len(broker.placed) == 1  # körlemesine tekrar gönderim yok
    # kritik: query_order HİÇ çağrılmadı (OCO bacaklarını bulamazdı)
    assert broker.queries == []
    # ve query_oco retry reconcile'ını yaptı
    assert any(q["list_client_order_id"] == cid for q in broker.oco_queries)

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE idempotency_key='oco-retry-1'").fetchone())

    row = await db.read(_q)
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "OL-99"


async def test_oco_unknown_retry_requeries_and_discovers_filled(cfg, db):
    """T1-1b: stored UNKNOWN OCO, retry'de `query_oco` ile FILLED'e çekilir."""
    ctx = await _real_oco_ctx(db)
    service = ctx["service"]
    broker = ctx["broker"]
    cid = to_client_order_id("oco-unknown-fill")

    def _insert(conn):
        order = service._insert_order(
            conn, account_id=ctx["account_id"], idempotency_key="oco-unknown-fill",
            symbol="ALICEUSDT", side="SELL", order_type="OCO", quantity=100.0,
            price=0.1378, notional=13.78, reference_price=0.1240, equity_snapshot=0.0,
            status="UNKNOWN", client_order_id=cid,
            stop_price=0.1225, stop_limit_price=0.1220,
        )
        return order["order_id"]

    order_id = await db.write(_insert)
    broker.query_results[cid] = None  # query_order kullanılırsa bulunamaz
    broker.oco_query_results[cid] = OrderResult(
        status="FILLED", exchange_order_id="OL-FILL", executed_qty=100.0, avg_price=0.13,
    )

    result = await service.place_oco_order(
        account_id=ctx["account_id"], symbol="ALICEUSDT", side="SELL",
        quantity=100.0, price=0.1378, stop_price=0.1225, stop_limit_price=0.1220,
        idempotency_key="oco-unknown-fill",
    )
    assert result["status"] == "FILLED"
    assert result["exchange_order_id"] == "OL-FILL"
    assert broker.queries == []  # query_order kullanılmadı
    assert len(broker.placed) == 0  # çift emir gönderilmedi

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())

    row = await db.read(_q)
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "OL-FILL"


async def test_kill_switch_cancels_oco_via_cancel_oco(cfg, db):
    """T1-2: kill switch OCO satırını `cancel_oco` ile iptal eder (tekil `cancel_order` değil)."""
    ctx = await _real_oco_ctx(db)
    service = ctx["service"]
    broker = ctx["broker"]
    cid = to_client_order_id("oco-kill-1")
    broker.open_orders = []  # borsada başka yetim emir yok (test izolasyonu)

    def _insert(conn):
        order = service._insert_order(
            conn, account_id=ctx["account_id"], idempotency_key="oco-kill-1",
            symbol="ALICEUSDT", side="SELL", order_type="OCO", quantity=100.0,
            price=0.1378, notional=13.78, reference_price=0.1240, equity_snapshot=0.0,
            status="NEW", client_order_id=cid,
            stop_price=0.1225, stop_limit_price=0.1220,
        )
        return order["order_id"]

    order_id = await db.write(_insert)

    result = await service.close_all_positions(account_id=ctx["account_id"], actor="test")
    detail = result["results"][0]
    assert detail["closed"] is True
    assert "ALICEUSDT" in detail["cancelled"]
    # OCO iptali cancel_oco ile yapıldı; tekil cancel_order kullanılmadı
    assert broker.cancelled_oco == [
        {"account_id": ctx["account_id"], "symbol": "ALICEUSDT", "list_client_order_id": cid}
    ]
    assert broker.cancelled == []

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())

    row = await db.read(_q)
    assert row["status"] == "CANCELED"


async def test_kill_switch_oco_cancel_unknown_when_not_found(cfg, db):
    """T1-2b: OCO iptali -2011 (borsada yok) ise `query_oco` ile doğrulanır."""
    ctx = await _real_oco_ctx(db)
    service = ctx["service"]
    broker = ctx["broker"]
    cid = to_client_order_id("oco-kill-2")
    broker.open_orders = []

    def _insert(conn):
        order = service._insert_order(
            conn, account_id=ctx["account_id"], idempotency_key="oco-kill-2",
            symbol="ALICEUSDT", side="SELL", order_type="OCO", quantity=100.0,
            price=0.1378, notional=13.78, reference_price=0.1240, equity_snapshot=0.0,
            status="NEW", client_order_id=cid,
            stop_price=0.1225, stop_limit_price=0.1220,
        )
        return order["order_id"]

    order_id = await db.write(_insert)
    # cancel_oco borsada bulamıyor (-2011 → None); sorgu query_oco ile yapılmalı
    broker.cancel_oco_results = {cid: None}
    broker.oco_query_results[cid] = OrderResult(status="NEW", exchange_order_id="OL-LIVE", executed_qty=0.0, avg_price=0.0)

    result = await service.close_all_positions(account_id=ctx["account_id"], actor="test")
    detail = result["results"][0]
    # iptal belirsiz → hesap kapandı denmez; local satır UNKNOWN
    assert detail["closed"] is False
    assert len(detail["cancel_errors"]) == 1

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())

    row = await db.read(_q)
    assert row["status"] == "UNKNOWN"
    # doğrulama query_oco ile yapıldı (tekil query_order değil)
    assert any(q["list_client_order_id"] == cid for q in broker.oco_queries)
    assert broker.queries == []


async def test_reconcile_open_orders_scans_unknown_records(cfg, db):
    """T1-3: `reconcile_open_orders` UNKNOWN kayıtları da tarar (OCO + tekil)."""
    ctx = await _real_oco_ctx(db)
    service = ctx["service"]
    broker = ctx["broker"]
    oco_cid = to_client_order_id("unk-oco")
    norm_cid = to_client_order_id("unk-norm")

    def _insert(conn):
        oco = service._insert_order(
            conn, account_id=ctx["account_id"], idempotency_key="unk-oco",
            symbol="ALICEUSDT", side="SELL", order_type="OCO", quantity=100.0,
            price=0.1378, notional=13.78, reference_price=0.1240, equity_snapshot=0.0,
            status="UNKNOWN", client_order_id=oco_cid,
            stop_price=0.1225, stop_limit_price=0.1220,
        )
        norm = service._insert_order(
            conn, account_id=ctx["account_id"], idempotency_key="unk-norm",
            symbol="ALICEUSDT", side="BUY", order_type="MARKET", quantity=1.0,
            price=0.1240, notional=0.124, reference_price=0.1240, equity_snapshot=0.0,
            status="UNKNOWN", client_order_id=norm_cid,
        )
        return oco["order_id"], norm["order_id"]

    oco_id, norm_id = await db.write(_insert)
    broker.oco_query_results[oco_cid] = OrderResult(
        status="FILLED", exchange_order_id="OL-UNK", executed_qty=100.0, avg_price=0.13,
    )
    broker.query_results[norm_cid] = OrderResult(
        status="FILLED", exchange_order_id="EX-UNK", executed_qty=1.0, avg_price=0.124,
    )

    result = await service.reconcile_open_orders()
    assert result["scanned"] >= 2  # UNKNOWN kayıtlar da tarandı
    assert result["reconciled"] >= 2

    def _q(conn):
        return {
            r["order_id"]: dict(r)
            for r in conn.execute("SELECT * FROM orders WHERE order_id IN (?, ?)", (oco_id, norm_id)).fetchall()
        }

    rows = await db.read(_q)
    assert rows[oco_id]["status"] == "FILLED"
    assert rows[oco_id]["exchange_order_id"] == "OL-UNK"
    assert rows[norm_id]["status"] == "FILLED"
    assert rows[norm_id]["exchange_order_id"] == "EX-UNK"


async def test_find_unprotected_positions_reports_account_errors(cfg, db):
    """T1-4: hesap erişim hatası sessizce yutulmaz — `errors` + `complete=false`."""
    accounts = AccountService(db, secret_store=SecretStore(), audit=AuditLog(db))
    risk = RiskPolicyService(db, audit=AuditLog(db))

    class _FlakyBroker(FakeOrderBroker):
        def __init__(self, balances=None):
            super().__init__(balances=balances)
            self.fail_detail: dict[str, Exception] = {}

        async def get_balance_detail(self, *, account_id):
            if account_id in self.fail_detail:
                raise self.fail_detail[account_id]
            return await super().get_balance_detail(account_id=account_id)

    broker = _FlakyBroker(balances={})
    market = _OcoMarket()
    market.price_map = {"BTCUSDT": 100.0, "ALICEUSDT": 0.1240}
    service = OrderService(db, accounts=accounts, risk=risk, broker=broker, market=market, audit=AuditLog(db))
    ok_id = (await accounts.add_account(label="ok", api_key="AK_ok", api_secret="AS_ok"))["account_id"]
    bad_id = (await accounts.add_account(label="bad", api_key="AK_bad", api_secret="AS_bad"))["account_id"]
    await accounts.enable_real_trading(ok_id, actor="test")
    await accounts.enable_real_trading(bad_id, actor="test")
    broker.balances[ok_id] = {"USDT": 10000.0, "BTC": 1.0}
    broker.balances[bad_id] = {"USDT": 10000.0, "BTC": 1.0}
    broker.fail_detail[bad_id] = RasatError(ErrorCode.TIMEOUT, "bakiye erişim hatası")

    result = await service.find_unprotected_positions()
    # iyi hesap hâlâ taranır; kötü hesap hata olarak taşınır
    assert any(u["account_id"] == ok_id for u in result["unprotected"])
    assert result["complete"] is False
    assert any(e["account_id"] == bad_id for e in result["errors"])
    assert any(e["error"]["code"] == ErrorCode.TIMEOUT for e in result["errors"])
    assert result["account_count"] == 2


# ---------- T1-koord: BinanceClock broker'a enjeksiyonu (signed timestamp) ----------


async def test_broker_signed_timestamp_uses_clock_server_now():
    """clock verilirse imzalı istek timestamp'i `clock.server_now()`'dan gelir (host saatine değil)."""
    import hashlib
    import hmac
    import time

    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from rasattrading_mcp.data.rate_limit import RateLimitBudget
    from tests.helpers import FakeClock

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
        if not _verify(request):
            return web.json_response({"code": -1022}, status=400)
        received["params"] = dict(request.query)
        return web.json_response(
            {"symbol": "ALICEUSDT", "orderId": 1, "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0"}
        )

    app = web.Application()
    app.router.add_get("/api/v3/order", order_handler)
    async with TestServer(app) as server:
        async def creds(_aid):
            return (api_key, api_secret)

        # offset = +300s → server_now() = local + 300
        clock = FakeClock(offset=300.0)
        broker = BinanceOrderBroker(
            server.make_url("/").human_repr(),
            credentials=creds,
            budget=RateLimitBudget(6000),
            clock=clock,
        )
        try:
            expected_ms = int((time.time() + 300.0) * 1000)
            await broker.query_order(account_id="a1", symbol="ALICEUSDT", client_order_id="oco-1")
            got = int(received["params"]["timestamp"])
            assert abs(got - expected_ms) < 5000  # clock.server_now() (offset uygulanmış)
            # host saatinden (offset'siz) net ayrılmalı — 300s fark test edilebilir
            assert got > int(time.time() * 1000) + 250_000
        finally:
            await broker.close()


async def test_broker_signed_timestamp_falls_back_to_local_when_clock_unavailable():
    """clock fail-closed (server_now → None) ise eski `time.time()` fallback'i korunur."""
    import hashlib
    import hmac
    import time

    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from rasattrading_mcp.data.rate_limit import RateLimitBudget
    from tests.helpers import FakeClock

    api_key = "TESTKEY0000000000000000000000000000"
    api_secret = "TESTSECRET00000000000000000000000000"
    received: dict = {}

    async def order_handler(request):
        received["params"] = dict(request.query)
        return web.json_response(
            {"symbol": "ALICEUSDT", "orderId": 1, "status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0"}
        )

    app = web.Application()
    app.router.add_get("/api/v3/order", order_handler)
    async with TestServer(app) as server:
        async def creds(_aid):
            return (api_key, api_secret)

        clock = FakeClock(offset=300.0)
        clock.set_available(False)  # server_now → None
        broker = BinanceOrderBroker(
            server.make_url("/").human_repr(),
            credentials=creds,
            budget=RateLimitBudget(6000),
            clock=clock,
        )
        try:
            before = int(time.time() * 1000)
            await broker.query_order(account_id="a1", symbol="ALICEUSDT", client_order_id="oco-1")
            got = int(received["params"]["timestamp"])
            after = int(time.time() * 1000)
            assert before <= got <= after  # yerel saat fallback'i (offset uygulanmadı)
        finally:
            await broker.close()
