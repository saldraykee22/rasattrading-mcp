import asyncio

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
    """MarketFeed taklidi: sabit fiyat + filtre; stale sembolü taklit edebilir."""

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


async def _add_real_account(ex_ctx, label="main", balance_usdt=10000.0, tags=None):
    ctx = ex_ctx
    created = await ctx["accounts"].add_account(label=label, api_key=f"AK_{label}", api_secret=f"AS_{label}", tags=tags or [])
    await ctx["accounts"].enable_real_trading(created["account_id"], actor="test")
    ctx["broker"].balances[created["account_id"]] = {"USDT": balance_usdt}
    return created["account_id"]


async def _add_paper_account(ex_ctx, label="paper", tags=None):
    ctx = ex_ctx
    created = await ctx["accounts"].add_account(label=label, tags=tags or [])
    return created["account_id"]


def _order_rows(db):
    def _q(conn):
        return [dict(r) for r in conn.execute("SELECT * FROM orders").fetchall()]

    return db.read(_q)


# ---------- idempotency ----------


async def test_same_idempotency_key_no_double_order(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]

    first = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="idem-1",
    )
    assert first["status"] == "FILLED"

    second = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="idem-1",
    )
    assert second["order_id"] == first["order_id"]
    assert second["status"] == "FILLED"
    assert len(ctx["broker"].placed) == 1  # çift emir yok

    rows = await _order_rows(ctx["db"])
    assert len(rows) == 1


async def test_retry_after_timeout_reconciles_not_replaces(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]
    cid = to_client_order_id("idem-reconcile")
    ctx["broker"].place_errors[cid] = RasatError(ErrorCode.TIMEOUT, "ağ zaman aşımı")
    ctx["broker"].query_results[cid] = OrderResult(status="FILLED", exchange_order_id="EX777", executed_qty=1.0, avg_price=100.0)

    result = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="idem-reconcile",
    )
    assert result["status"] == "FILLED"
    assert result["exchange_order_id"] == "EX777"
    assert len(ctx["broker"].placed) == 1  # körlemesine tekrar gönderim yok
    assert len(ctx["broker"].queries) == 1  # reconcile sorgulandı


async def test_timeout_unknown_no_blind_retry(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]
    cid = to_client_order_id("idem-unknown")
    ctx["broker"].place_errors[cid] = RasatError(ErrorCode.TIMEOUT, "ağ zaman aşımı")
    ctx["broker"].query_results[cid] = None  # Binance'te bulunamadı

    result = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="idem-unknown",
    )
    assert result["status"] == "UNKNOWN"
    assert len(ctx["broker"].placed) == 1
    assert result["error"]["code"] == ErrorCode.ORDER_UNKNOWN


# ---------- serialization / concurrency ----------


async def test_concurrent_same_account_no_race(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]

    # Aynı hesapta farklı idempotency key'ler ile eşzamanlı — her ikisi de işlenmeli, race yok.
    results = await asyncio.gather(
        service.place_order(account_id=account_id, symbol="BTCUSDT", side="BUY",
                            order_type="MARKET", quantity=1.0, idempotency_key="concurrent-1"),
        service.place_order(account_id=account_id, symbol="ETHUSDT", side="BUY",
                            order_type="MARKET", quantity=2.0, idempotency_key="concurrent-2"),
    )
    assert all(r["status"] == "FILLED" for r in results)
    assert len(ctx["broker"].placed) == 2


async def test_concurrent_same_key_single_order(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]

    results = await asyncio.gather(
        service.place_order(account_id=account_id, symbol="BTCUSDT", side="BUY",
                            order_type="MARKET", quantity=1.0, idempotency_key="same-key"),
        service.place_order(account_id=account_id, symbol="BTCUSDT", side="BUY",
                            order_type="MARKET", quantity=1.0, idempotency_key="same-key"),
    )
    ids = {r["order_id"] for r in results}
    assert len(ids) == 1
    assert len(ctx["broker"].placed) == 1
    rows = await _order_rows(ctx["db"])
    assert len(rows) == 1


# ---------- partial success ----------


async def test_execute_on_accounts_partial_success(ex_ctx):
    ctx = ex_ctx
    ids = []
    for i in range(10):
        ids.append(await _add_real_account(ctx, label=f"acc{i}", tags=["batch"]))
    # 3 hesabın bakiyesi 0 → yetersiz bakiye
    ctx["broker"].balances[ids[0]] = {"USDT": 0.0}
    ctx["broker"].balances[ids[1]] = {"USDT": 0.0}
    ctx["broker"].balances[ids[2]] = {"USDT": 0.0}

    service = ctx["service"]
    result = await service.execute_on_accounts(
        tags=["batch"], symbol="BTCUSDT", side="BUY", entry=100, stop_loss=95,
        risk_pct=0.01, idempotency_key="batch-order",
    )
    assert result["count"] == 10
    assert result["succeeded"] == 7
    assert result["failed"] == 3
    # kalan 7 hesap etkilenmedi — emirleri işlendi
    filled = [r for r in result["results"] if r["status"] == "FILLED"]
    assert len(filled) == 7
    rejected = [r for r in result["results"] if r["status"] == "REJECTED"]
    assert len(rejected) == 3
    assert all(r["error"] for r in rejected)


async def test_execute_on_accounts_requires_target(ex_ctx):
    ctx = ex_ctx
    service = ctx["service"]
    with pytest.raises(RasatError) as exc_info:
        await service.execute_on_accounts(
            symbol="BTCUSDT", side="BUY", entry=100, stop_loss=95,
            risk_pct=0.01, idempotency_key="no-target",
        )
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST


# ---------- paper accounts ----------


async def test_paper_account_simulates_without_broker(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_paper_account(ctx)
    service = ctx["service"]
    result = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="paper-1",
    )
    assert result["status"] == "paper"
    assert len(ctx["broker"].placed) == 0  # broker'a gitmez


async def test_execute_on_accounts_paper_sizing(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_paper_account(ctx)
    ctx["broker"].balances[account_id] = {"USDT": 10000.0}
    service = ctx["service"]
    result = await service.execute_on_accounts(
        account_ids=[account_id], symbol="BTCUSDT", side="BUY", entry=100, stop_loss=95,
        risk_pct=0.01, idempotency_key="paper-sized",
    )
    assert result["results"][0]["status"] == "paper"
    assert result["results"][0]["quantity"] == 20  # 100 USDT risk / 5 risk-per-unit


# ---------- override + caps ----------


async def test_override_allows_cap_exceedance_once(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    await ctx["risk"].set_risk_policy(account_id, max_notional_per_order=150)
    service = ctx["service"]

    # cap 150; 1 BTC * 100 = 100 notional → cap altında, geçer
    ok = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="cap-ok",
    )
    assert ok["status"] == "FILLED"

    # 2 BTC * 100 = 200 notional > 150 cap → override yoksa reddedilir
    with pytest.raises(RasatError) as exc_info:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=2.0, idempotency_key="cap-exceed",
        )
    assert exc_info.value.code == ErrorCode.RISK_LIMIT_EXCEEDED

    # tek kullanımlık override ile bir kez geçer
    await ctx["risk"].create_override(account_id, reason="bilinçli aşım", idempotency_key="override-1")
    allowed = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=2.0, idempotency_key="cap-override",
    )
    assert allowed["status"] == "FILLED"

    # override tüketildi; yeni aşım tekrar reddedilir
    with pytest.raises(RasatError) as exc_info:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=2.0, idempotency_key="cap-exceed-2",
        )
    assert exc_info.value.code == ErrorCode.RISK_LIMIT_EXCEEDED


async def test_aggregate_exposure_cap_with_override(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    await ctx["risk"].set_risk_policy(account_id, max_aggregate_exposure=250)
    service = ctx["service"]

    # 1 BTC = 100 notional; toplam exposure 100 < 250 → geçer
    ok = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="agg-1",
    )
    assert ok["status"] == "FILLED"

    # 2 BTC = 200; exposure 100+200=300 > 250 → reddedilir
    with pytest.raises(RasatError) as exc_info:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=2.0, idempotency_key="agg-2",
        )
    assert exc_info.value.code == ErrorCode.RISK_LIMIT_EXCEEDED

    # override ile bir kez geçer
    await ctx["risk"].create_override(account_id, reason="aggr aşım", idempotency_key="agg-ovr")
    allowed = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=2.0, idempotency_key="agg-3",
    )
    assert allowed["status"] == "FILLED"


# ---------- accuracy checks her zaman aktif ----------


async def test_stale_price_rejects(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    ctx["market"].stale.add("BTCUSDT")
    service = ctx["service"]
    with pytest.raises(RasatError) as exc_info:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=1.0, idempotency_key="stale",
        )
    assert exc_info.value.code == ErrorCode.STALE_DATA
    assert len(ctx["broker"].placed) == 0


async def test_invalid_symbol_rejects(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]
    with pytest.raises(RasatError) as exc_info:
        await service.place_order(
            account_id=account_id, symbol="NOPEUSDT", side="BUY", order_type="MARKET",
            quantity=1.0, idempotency_key="bad-symbol",
        )
    assert exc_info.value.code == ErrorCode.INVALID_SYMBOL


async def test_trading_lock_required(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_paper_account(ctx)
    ctx["broker"].balances[account_id] = {"USDT": 10000.0}
    service = ctx["service"]
    result = await service.execute_on_accounts(
        account_ids=[account_id], symbol="BTCUSDT", side="BUY", entry=100, stop_loss=95,
        risk_pct=0.01, idempotency_key="paper-lock",
    )
    # paper hesap simüle eder, reddetmez
    assert result["results"][0]["status"] == "paper"
