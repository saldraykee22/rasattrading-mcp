import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.order_broker import OrderResult, to_client_order_id
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

FILTERS = SymbolFilters(
    symbol="BTCUSDT",
    base_asset="BTC",
    quote_asset="USDT",
    status="TRADING",
    step_size=0.00001,
    min_qty=0.00001,
    max_qty=100000,
    min_notional=5.0,
    tick_size=0.01,
    min_price=0.01,
    max_price=1000000.0,
)


class FakeMarket:
    def __init__(self) -> None:
        self.price_map = {"BTCUSDT": 100.0, "ETHUSDT": 50.0}
        self.stale: set[str] = set()

    async def symbol_valid(self, symbol: str) -> bool:
        return symbol in self.price_map

    async def price(self, symbol: str) -> float | None:
        if symbol in self.stale:
            return None
        return self.price_map.get(symbol)

    async def filters(self, symbol: str) -> SymbolFilters | None:
        if symbol not in self.price_map:
            return None
        return SymbolFilters(**{**FILTERS.__dict__, "symbol": symbol})


@pytest.fixture
async def ex_db(tmp_path):
    db = Database(Config(data_dir=tmp_path, pipeline_enabled=False).db_path)
    await db.start()
    await run_migrations(db)
    yield db
    await db.stop()


@pytest.fixture
async def ex_ctx(ex_db):
    accounts = AccountService(ex_db, secret_store=SecretStore(), audit=AuditLog(ex_db))
    risk = RiskPolicyService(ex_db, audit=AuditLog(ex_db))
    broker = FakeOrderBroker()
    market = FakeMarket()
    service = OrderService(
        ex_db,
        accounts=accounts,
        risk=risk,
        broker=broker,
        market=market,
        audit=AuditLog(ex_db),
    )
    return {"db": ex_db, "accounts": accounts, "risk": risk, "broker": broker,
            "market": market, "service": service}


async def _add_real_account(ex_ctx, label="main", balance_usdt=10000.0, base_holdings=None, tags=None):
    ctx = ex_ctx
    created = await ctx["accounts"].add_account(label=label, api_key=f"AK_{label}", api_secret=f"AS_{label}", tags=tags or [])
    await ctx["accounts"].enable_real_trading(created["account_id"], actor="test")
    balances = {"USDT": balance_usdt}
    balances.update(base_holdings or {})
    ctx["broker"].balances[created["account_id"]] = balances
    return created["account_id"]


# ---------- kill switch: close_all_positions ----------


async def _plant_open_order(ex_ctx, account_id, idempotency_key):
    """place_result=NEW ile açık (borsada duran) bir emir yerleştir."""
    ctx = ex_ctx
    ctx["broker"].place_result = {"status": "NEW"}
    service = ctx["service"]
    placed = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.5, price=80, idempotency_key=idempotency_key,
    )
    ctx["broker"].place_result = None  # sonraki sell'ler default FILLED dönsün
    return placed


async def test_close_all_positions_cancel_failure_not_marked_canceled(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    placed = await _plant_open_order(ctx, account_id, "open-fail")
    cid = to_client_order_id("open-fail")
    ctx["broker"].cancel_errors[cid] = RasatError(ErrorCode.TIMEOUT, "iptal ağ hatası")
    ctx["broker"].open_orders = []  # borsada başka yetim emir yok (T01 exchange scope)

    service = ctx["service"]
    result = await service.close_all_positions(account_id=account_id, actor="test")
    detail = result["results"][0]
    # iptal başarısız → hesap kapandı denmez; hata raporlanır
    assert detail["closed"] is False
    assert len(detail["cancel_errors"]) == 1
    assert detail["cancel_errors"][0]["error"]["code"] == ErrorCode.TIMEOUT
    assert "BTCUSDT" not in detail["cancelled"]

    # yerel durum CANCELED değil → UNKNOWN + hata kodu
    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (placed["order_id"],)).fetchone())

    row = await ctx["db"].read(_q)
    assert row["status"] != "CANCELED"
    assert row["status"] == "UNKNOWN"
    assert row["error_code"] == ErrorCode.TIMEOUT
    assert len(ctx["broker"].cancelled) == 0  # borsaya iptal kaydı düşmedi


async def test_close_all_positions_cancel_success_marks_canceled(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    placed = await _plant_open_order(ctx, account_id, "open-ok")
    ctx["broker"].open_orders = []  # borsada başka yetim emir yok (T01 exchange scope)

    service = ctx["service"]
    result = await service.close_all_positions(account_id=account_id, actor="test")
    detail = result["results"][0]
    assert detail["closed"] is True
    assert detail["cancel_errors"] == []
    assert "BTCUSDT" in detail["cancelled"]

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (placed["order_id"],)).fetchone())

    row = await ctx["db"].read(_q)
    assert row["status"] == "CANCELED"


async def test_close_all_positions_single_account_sells_holdings(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0, "ETH": 2.0})
    service = ctx["service"]

    result = await service.close_all_positions(account_id=account_id, actor="test")
    assert result["count"] == 1
    assert result["closed"] == 1
    detail = result["results"][0]
    assert detail["closed"] is True
    assert detail["mode"] == "real"
    sold = {s["symbol"]: s for s in detail["sold"]}
    assert sold["BTCUSDT"]["status"] == "FILLED"
    assert sold["ETHUSDT"]["status"] == "FILLED"
    assert len(ctx["broker"].placed) == 2  # iki sembol satıldı


async def test_close_all_positions_idempotent_no_double_sell(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    service = ctx["service"]

    first = await service.close_all_positions(account_id=account_id)
    # bakiye artık 0 BTC (fake broker satışı uygular) → ikinci çağrı satış üretmez
    second = await service.close_all_positions(account_id=account_id)
    assert second["closed"] == 1
    assert len(ctx["broker"].placed) == 1
    # aynı (account, symbol) için idempotency key çift satış üretmez
    third = await service.close_all_positions(account_id=account_id)
    assert len(ctx["broker"].placed) == 1


async def test_close_all_positions_sells_rebought_position(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    service = ctx["service"]

    first = await service.close_all_positions(account_id=account_id, actor="test")
    assert first["results"][0]["closed"] is True
    assert len(ctx["broker"].placed) == 1  # ilk close: 1 SELL

    # 3.11: close sonrası yeniden BTC alındı → ikinci close YENİ SELL üretmeli
    ctx["broker"].balances[account_id]["BTC"] = 2.0
    second = await service.close_all_positions(account_id=account_id, actor="test")
    assert second["results"][0]["closed"] is True
    assert len(ctx["broker"].placed) == 2  # 0 değil — yeni pozisyon satıldı
    sold = {s["symbol"]: s for s in second["results"][0]["sold"]}
    assert sold["BTCUSDT"]["quantity"] == pytest.approx(2.0)
    assert sold["BTCUSDT"]["status"] == "FILLED"


async def test_close_all_positions_same_qty_rebuy_sells_again(ex_ctx):
    # B1/3.14: birebir AYNI miktarın yeniden alımı bile yeni SELL üretmeli —
    # eski FILLED dedup'unun bıraktığı kenar durumu (3.11 kabul kriteri).
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    service = ctx["service"]

    first = await service.close_all_positions(account_id=account_id, actor="test")
    assert first["results"][0]["closed"] is True
    assert len(ctx["broker"].placed) == 1

    # tam aynı miktar (1.0) yeniden alındı
    ctx["broker"].balances[account_id]["BTC"] = 1.0
    second = await service.close_all_positions(account_id=account_id, actor="test")
    assert second["results"][0]["closed"] is True
    assert len(ctx["broker"].placed) == 2  # eski FILLED'a takılıp atlamadı
    sold = {s["symbol"]: s for s in second["results"][0]["sold"]}
    assert sold["BTCUSDT"]["quantity"] == pytest.approx(1.0)
    assert sold["BTCUSDT"]["status"] == "FILLED"


async def test_close_all_positions_rejected_retry_no_unique_error(ex_ctx):
    # H5/3.14: REJECTED ile biten close'un aynı miktar retry'i UNIQUE constraint
    # hatasına düşmeden yeni SELL üretmeli.
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    service = ctx["service"]

    ctx["broker"].place_result = {"status": "REJECTED", "exchange_order_id": None}
    first = await service.close_all_positions(account_id=account_id, actor="test")
    assert first["results"][0]["closed"] is False
    assert len(ctx["broker"].placed) == 1

    # retry: broker artık kabul ediyor
    ctx["broker"].place_result = None
    second = await service.close_all_positions(account_id=account_id, actor="test")
    assert second["results"][0]["closed"] is True  # UNIQUE hatası yok
    assert len(ctx["broker"].placed) == 2
    assert second["results"][0]["sold"][0]["status"] == "FILLED"


async def test_close_all_positions_all_partial_success(ex_ctx):
    ctx = ex_ctx
    ok_id = await _add_real_account(ctx, label="ok", base_holdings={"BTC": 1.0}, tags=["kill"])
    bad_id = await _add_real_account(ctx, label="bad", base_holdings={"BTC": 1.0}, tags=["kill"])
    # bad hesabın satışı ağ hatası versin ve Binance'te de bulunamasın → UNKNOWN.
    # (3.14 idem key run nonce'ı taşıdığından cid önceden bilinmez; hesap bazlı hata.)
    ctx["broker"].place_errors_by_account[bad_id] = RasatError(ErrorCode.TIMEOUT, "ağ hatası")
    ctx["broker"].query_results_by_account[bad_id] = None

    service = ctx["service"]
    result = await service.close_all_positions(account_id="all", actor="test")
    assert result["count"] == 2
    by_account = {r["account_id"]: r for r in result["results"]}
    # 3.19: satış UNKNOWN olduğu için bad hesap closed denmez — yalnızca FILLED kapalıdır
    assert by_account[ok_id]["closed"] is True
    assert by_account[bad_id]["closed"] is False
    # kısmi başarı raporu: ok_id BTC sattı (FILLED), bad_id satamadı (UNKNOWN)
    ok_sold = {s["symbol"]: s for s in by_account[ok_id]["sold"]}
    bad_sold = {s["symbol"]: s for s in by_account[bad_id]["sold"]}
    assert ok_sold["BTCUSDT"]["status"] == "FILLED"
    assert bad_sold["BTCUSDT"]["status"] == "UNKNOWN"


async def test_close_all_positions_unknown_account(ex_ctx):
    ctx = ex_ctx
    service = ctx["service"]
    with pytest.raises(RasatError) as exc_info:
        await service.close_all_positions(account_id="nope")
    assert exc_info.value.code == ErrorCode.ACCOUNT_NOT_FOUND


# ---------- disable_real_trading ----------


async def test_reconcile_open_orders_resolves_orphan_new(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    service = ctx["service"]

    # DB'de NEW'de bir emir bırak (crash senaryosu); Binance'te FILLED olduğunu simüle et
    cid = to_client_order_id("orphan-new")
    ctx["broker"].place_result = {"status": "NEW"}
    placed = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.5, price=80, idempotency_key="orphan-new",
    )
    assert placed["status"] == "NEW"
    ctx["broker"].query_results[cid] = OrderResult(
        status="FILLED", exchange_order_id="EX-ORPHAN", executed_qty=0.5, avg_price=80.0,
    )

    result = await service.reconcile_open_orders()
    assert result["scanned"] >= 1
    assert result["reconciled"] >= 1

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (placed["order_id"],)).fetchone())

    row = await ctx["db"].read(_q)
    assert row["status"] == "FILLED"
    assert row["exchange_order_id"] == "EX-ORPHAN"
    # exposure artık NEW emirle şişmez
    exposure = await service.get_total_exposure()
    assert exposure["by_symbol"].get("BTCUSDT", 0.0) == pytest.approx(1.0 * 100.0)  # sadece base holding


async def test_reconcile_open_orders_orphan_new_not_on_exchange(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]

    # Crash: DB'ye NEW yazıldı ama broker'a hiç gitmedi → Binance'te yok
    cid = to_client_order_id("orphan-crash")

    def _insert(conn):
        order = service._insert_order(
            conn, account_id=account_id, idempotency_key="orphan-crash",
            symbol="BTCUSDT", side="BUY", order_type="MARKET", quantity=0.5,
            price=100.0, notional=50.0, reference_price=100.0, equity_snapshot=0.0,
            status="NEW", client_order_id=cid,
        )
        return order["order_id"]

    order_id = await ctx["db"].write(_insert)

    result = await service.reconcile_open_orders()
    assert result["scanned"] >= 1
    assert result["reconciled"] >= 1

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())

    row = await ctx["db"].read(_q)
    assert row["status"] == "UNKNOWN"  # sonsuza dek NEW kalmaz
    assert row["error_code"] == ErrorCode.ORDER_UNKNOWN


async def test_reconcile_open_orders_skips_paper_accounts(ex_ctx):
    ctx = ex_ctx
    service = ctx["service"]

    paper = await ctx["accounts"].add_account(label="paper-only")
    ctx["broker"].balances[paper["account_id"]] = {"USDT": 10000.0}
    ctx["broker"].place_result = {"status": "NEW"}
    await service.place_order(
        account_id=paper["account_id"], symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.5, price=80, idempotency_key="paper-new",
    )
    # paper hesap reconcile edilmez (borsada gerçek emir yok)
    result = await service.reconcile_open_orders()
    assert result["scanned"] == 0


async def test_reconcile_open_orders_same_status_partial_fill_syncs(ex_ctx):
    # 3.18: durum aynı (PARTIALLY_FILLED) kalsa bile executed_qty/avg_price
    # ilerlemişse reconcile bunları güncellemeli — status eşit diye skip yok.
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]
    cid = to_client_order_id("partial-fill")

    def _insert(conn):
        order = service._insert_order(
            conn, account_id=account_id, idempotency_key="partial-fill",
            symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity=1.0,
            price=100.0, notional=100.0, reference_price=100.0, equity_snapshot=0.0,
            status="PARTIALLY_FILLED", client_order_id=cid,
        )
        return order["order_id"]

    order_id = await ctx["db"].write(_insert)

    # broker: aynı PARTIALLY_FILLED ama executed_qty 0.5→1.0, avg 100→101 ilerledi
    ctx["broker"].query_results[cid] = OrderResult(
        status="PARTIALLY_FILLED", exchange_order_id="EX-PF",
        executed_qty=1.0, avg_price=101.0,
    )

    result = await service.reconcile_open_orders()
    assert result["scanned"] >= 1
    assert result["unchanged"] == 0  # status eşit olsa da fill alanları değişti
    assert result["reconciled"] >= 1

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())

    row = await ctx["db"].read(_q)
    assert row["status"] == "PARTIALLY_FILLED"
    assert row["executed_qty"] == pytest.approx(1.0)
    assert row["avg_price"] == pytest.approx(101.0)
    assert row["exchange_order_id"] == "EX-PF"


async def test_reconcile_open_orders_same_fields_counts_unchanged(ex_ctx):
    # 3.18: tüm alanlar gerçekten aynıysa unchanged sayılır (gereksiz yazma yok).
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]
    cid = to_client_order_id("partial-unchanged")

    def _insert(conn):
        order = service._insert_order(
            conn, account_id=account_id, idempotency_key="partial-unchanged",
            symbol="BTCUSDT", side="BUY", order_type="LIMIT", quantity=1.0,
            price=100.0, notional=100.0, reference_price=100.0, equity_snapshot=0.0,
            status="PARTIALLY_FILLED", client_order_id=cid,
        )
        return order["order_id"]

    await ctx["db"].write(_insert)

    ctx["broker"].query_results[cid] = OrderResult(
        status="PARTIALLY_FILLED", exchange_order_id=None, executed_qty=0.0, avg_price=0.0,
    )

    result = await service.reconcile_open_orders()
    assert result["scanned"] >= 1
    assert result["unchanged"] >= 1


async def test_disable_real_trading(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    accounts = ctx["accounts"]

    disabled = await accounts.disable_real_trading(account_id, actor="test")
    assert disabled["trading_lock"] == "paper"
    assert disabled["already_paper"] is False
    assert (await accounts.get_account(account_id))["trading_lock"] == "paper"

    # idempotent
    again = await accounts.disable_real_trading(account_id)
    assert again["already_paper"] is True
    assert await AuditLog(ctx["db"]).verify() == []


async def test_disable_real_trading_blocks_new_orders(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    await ctx["accounts"].disable_real_trading(account_id)
    service = ctx["service"]
    result = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="blocked",
    )
    # paper moduna düştü → simüle edilir, broker'a gitmez
    assert result["status"] == "paper"
    assert len(ctx["broker"].placed) == 0


# ---------- get_total_exposure ----------


async def test_get_total_exposure_symbol_view(ex_ctx):
    ctx = ex_ctx
    a1 = await _add_real_account(ctx, label="a1", base_holdings={"BTC": 1.0, "ETH": 2.0})
    a2 = await _add_real_account(ctx, label="a2", base_holdings={"BTC": 2.0})
    service = ctx["service"]

    exposure = await service.get_total_exposure()
    # BTC: (1+2)*100 = 300; ETH: 2*50 = 100
    assert exposure["by_symbol"]["BTCUSDT"] == pytest.approx(300.0)
    assert exposure["by_symbol"]["ETHUSDT"] == pytest.approx(100.0)
    assert exposure["total"] == pytest.approx(400.0)
    assert exposure["account_count"] == 2
    assert exposure["per_account"][a1] == pytest.approx(200.0)
    assert exposure["per_account"][a2] == pytest.approx(200.0)


# ---------- get_audit_log ----------


async def test_get_audit_log_verified_and_tamper_detected(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    await ctx["accounts"].disable_real_trading(account_id, actor="test")
    service = ctx["service"]

    log = await service.get_audit_log(limit=10)
    assert log["verified"] is True
    assert log["broken"] == []
    assert log["count"] >= 2

    # zincir bozma
    def _tamper(conn):
        conn.execute("UPDATE audit_log SET details='{tampered}' WHERE seq=2")

    await ctx["db"].write(_tamper)
    log = await service.get_audit_log(limit=10)
    assert log["verified"] is False
    assert any(b["reason"] for b in log["broken"])


async def test_get_audit_log_limit_validation(ex_ctx):
    ctx = ex_ctx
    service = ctx["service"]
    with pytest.raises(RasatError) as exc_info:
        await service.get_audit_log(limit=0)
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST
