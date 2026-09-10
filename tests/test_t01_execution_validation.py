"""T01 — Execution validation, timeout, and order lifecycle regression tests.

Kapsar:
- shared `validate_execution_order`: finite/enum/conditional fields/stop direction/OCO
  geometrisi/SymbolFilters (min/max/step/tick/min-notional);
- removal of LIMIT price fallback (explicit price required);
- broker `asyncio.TimeoutError` → canonical TIMEOUT/reconcile (no blind retry);
- close_all_positions exchange open-order scope + cancellation audit;
- get_total_exposure per-account errors / complete=false + risk gating fail-closed;
- risk.py / position_sizing.py NaN fail-closed;
- HTTP /rpc: schema validation, transport field merge, body limit, error redaction.
"""

import asyncio
import json
import time

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.order_broker import OrderResult, to_client_order_id
from rasattrading_mcp.daemon.handlers import approve_pending_order_handler
from rasattrading_mcp.daemon.readiness import Readiness
from rasattrading_mcp.daemon.server import ToolDispatcher, build_app
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.position_sizing import SymbolFilters, calculate_position_size
from rasattrading_mcp.risk import enforce_policy_caps, within_tolerance
from rasattrading_mcp.storage.accounts import AccountService
from rasattrading_mcp.storage.audit import AuditLog
from rasattrading_mcp.storage.credentials import SecretStore
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.storage.order_validation import validate_execution_order
from rasattrading_mcp.storage.orders import OrderService
from rasattrading_mcp.storage.risk_policy import RiskPolicyService
from rasattrading_mcp.tools import ToolRegistry, ToolSpec

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


async def _add_real_account(ex_ctx, label="main", balance_usdt=10000.0, base_holdings=None):
    ctx = ex_ctx
    created = await ctx["accounts"].add_account(label=label, api_key=f"AK_{label}", api_secret=f"AS_{label}")
    await ctx["accounts"].enable_real_trading(created["account_id"], actor="test")
    balances = {"USDT": balance_usdt}
    balances.update(base_holdings or {})
    ctx["broker"].balances[created["account_id"]] = balances
    return created["account_id"]


async def _order_rows(db):
    def _q(conn):
        return [dict(r) for r in conn.execute("SELECT * FROM orders").fetchall()]

    return await db.read(_q)


# =====================================================================
# Validator — unit level
# =====================================================================


def test_validator_rejects_nan_infinity():
    with pytest.raises(RasatError) as exc:
        validate_execution_order(side="BUY", order_type="MARKET", quantity=float("nan"))
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as exc:
        validate_execution_order(side="BUY", order_type="LIMIT", quantity=1.0, price=float("inf"))
    assert exc.value.code == ErrorCode.INVALID_REQUEST


def test_validator_rejects_bad_enum():
    with pytest.raises(RasatError) as exc:
        validate_execution_order(side="HOLD", order_type="MARKET", quantity=1.0)
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as exc:
        validate_execution_order(side="BUY", order_type="FANCY", quantity=1.0)
    assert exc.value.code == ErrorCode.INVALID_REQUEST


def test_validator_conditional_fields():
    with pytest.raises(RasatError) as exc:
        validate_execution_order(side="BUY", order_type="LIMIT", quantity=1.0)
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as exc:
        validate_execution_order(side="SELL", order_type="STOP_LOSS_LIMIT", quantity=1.0, price=90.0)
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as exc:
        validate_execution_order(side="SELL", order_type="OCO", quantity=1.0, price=110.0, stop_price=90.0)
    assert exc.value.code == ErrorCode.INVALID_REQUEST


def test_validator_stop_direction_vs_market():
    # SELL stop protection: stop must be BELOW the market.
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="SELL", order_type="STOP_LOSS_LIMIT", quantity=1.0,
            price=90.0, stop_price=105.0, market_price=100.0,
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    validate_execution_order(
        side="SELL", order_type="STOP_LOSS_LIMIT", quantity=1.0,
        price=90.0, stop_price=95.0, market_price=100.0,
    )


def test_validator_oco_geometry():
    # SELL: stop_limit < stop < price.
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="SELL", order_type="OCO", quantity=1.0,
            price=110.0, stop_price=95.0, stop_limit_price=97.0,
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    validate_execution_order(
        side="SELL", order_type="OCO", quantity=1.0,
        price=110.0, stop_price=95.0, stop_limit_price=94.0,
    )


def test_validator_filter_quantity_min_max():
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="BUY", order_type="MARKET", quantity=0.000001,  # minQty 0.00001
            filters=FILTERS, market_price=100.0,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION

    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="BUY", order_type="MARKET", quantity=1000000.0,  # maxQty 100000
            filters=FILTERS, market_price=100.0,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION


def test_validator_filter_step_and_tick():
    # Quantity is not a step multiple.
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="BUY", order_type="MARKET", quantity=0.1234567,
            filters=FILTERS, market_price=100.0,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION

    # Limit price is not a tick multiple.
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="BUY", order_type="LIMIT", quantity=1.0, price=100.005,  # tick 0.01
            filters=FILTERS, market_price=100.0,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION


def test_validator_filter_min_notional():
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="BUY", order_type="MARKET", quantity=0.00001,  # notional 0.001 < 5
            filters=FILTERS, market_price=100.0,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION


def test_validator_stop_price_price_filter():
    # stop_price is also subject to PRICE_FILTER min/max/tick (not only price).
    # Reject a non-tick-multiple stop even when SELL stop < market is valid.
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="SELL", order_type="STOP_LOSS_LIMIT", quantity=1.0,
            price=90.0, stop_price=95.005, market_price=100.0, filters=FILTERS,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION
    # stop_price below min_price → FILTER_VIOLATION
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="SELL", order_type="STOP_LOSS_LIMIT", quantity=1.0,
            price=90.0, stop_price=0.005, market_price=100.0, filters=FILTERS,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION
    # stop_price above max_price → FILTER_VIOLATION (direction remains valid)
    over = SymbolFilters(**{**FILTERS.__dict__, "max_price": 50.0})
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="SELL", order_type="STOP_LOSS_LIMIT", quantity=1.0,
            price=40.0, stop_price=60.0, market_price=100.0, filters=over,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION


def test_validator_stop_limit_price_price_filter():
    # OCO stop_limit_price da PRICE_FILTER tick'e tabidir.
    with pytest.raises(RasatError) as exc:
        validate_execution_order(
            side="SELL", order_type="OCO", quantity=1.0,
            price=110.0, stop_price=95.0, stop_limit_price=94.005,
            market_price=100.0, filters=FILTERS,
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION


# =====================================================================
# Service — broker is never called (fail-closed)
# =====================================================================


async def test_place_order_limit_missing_price_no_broker(ex_ctx):
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
            quantity=1.0, idempotency_key="no-price",
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    assert ex_ctx["broker"].placed == []


async def test_place_order_nan_quantity_no_broker(ex_ctx):
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=float("nan"), idempotency_key="nan-qty",
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    assert ex_ctx["broker"].placed == []


async def test_place_order_nan_price_no_broker(ex_ctx):
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
            quantity=1.0, price=float("inf"), idempotency_key="inf-price",
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    assert ex_ctx["broker"].placed == []


async def test_place_order_invalid_order_type_no_broker(ex_ctx):
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="TRAILING_STOP",
            quantity=1.0, idempotency_key="bad-type",
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    assert ex_ctx["broker"].placed == []


async def test_place_order_below_min_qty_filter_violation_no_broker(ex_ctx):
    # Acceptance criterion: quantity 1e-6 / minQty 1e-5 → FILTER_VIOLATION, broker is not called.
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=0.000001, idempotency_key="below-min",
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION
    assert ex_ctx["broker"].placed == []


async def test_place_order_step_violation_no_broker(ex_ctx):
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=0.1234567, idempotency_key="bad-step",
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION
    assert ex_ctx["broker"].placed == []


async def test_place_order_tick_violation_no_broker(ex_ctx):
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
            quantity=1.0, price=100.005, idempotency_key="bad-tick",
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION
    assert ex_ctx["broker"].placed == []


async def test_place_order_market_notional_below_min_no_broker(ex_ctx):
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=0.00001, idempotency_key="low-notional",  # 0.00001*100 = 0.001 < 5
        )
    assert exc.value.code == ErrorCode.FILTER_VIOLATION
    assert ex_ctx["broker"].placed == []


async def test_oco_bad_geometry_rejects_before_account(ex_db):
    # Account service must not be reached—geometry must be rejected in pre-validation.
    class _FakeAccounts:
        async def get_account(self, account_id):
            raise AssertionError("validation must reject before reaching the account")

    service = OrderService(ex_db, accounts=_FakeAccounts(), market=None, broker=None, risk=None, audit=None)
    with pytest.raises(RasatError) as exc:
        await service.place_oco_order(
            account_id="a", symbol="ALICEUSDT", side="SELL",
            quantity=10.0, price=0.1378, stop_price=0.1225, stop_limit_price=0.1300,
            idempotency_key="k",
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST


async def test_execute_on_accounts_limit_tick_violation(ex_ctx):
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    result = await service.execute_on_accounts(
        account_ids=[account_id], symbol="BTCUSDT", side="BUY", entry=100.005,
        stop_loss=95, risk_pct=0.01, order_type="LIMIT", idempotency_key="sized-tick",
    )
    assert result["results"][0]["status"] == "REJECTED"
    assert result["results"][0]["error"]["code"] == ErrorCode.FILTER_VIOLATION
    assert ex_ctx["broker"].placed == []


async def test_execute_on_accounts_stop_loss_limit_passes_price_and_stop(ex_ctx):
    # T01 regression: _execute_one_sized must not reject STOP_LOSS_LIMIT as "price required"
    # Must not reject—entry/stop_loss are passed to the validator as price/stop_price,
    # validation passes, and the flow reaches the balance gate (SELL: no base).
    account_id = await _add_real_account(ex_ctx)
    service = ex_ctx["service"]
    result = await service.execute_on_accounts(
        account_ids=[account_id], symbol="BTCUSDT", side="SELL", entry=95,
        stop_loss=98, risk_pct=0.01, order_type="STOP_LOSS_LIMIT",
        idempotency_key="sized-sll",
    )
    detail = result["results"][0]
    assert detail["status"] == "REJECTED"
    assert detail["error"]["code"] == ErrorCode.INSUFFICIENT_BALANCE
    assert ex_ctx["broker"].placed == []


async def test_execute_on_accounts_stop_loss_limit_reaches_broker_with_stop(ex_ctx):
    # T01 regression: after passing the balance gate, STOP_LOSS_LIMIT reaches the
    # broker through _place_and_record with stop_price (not "stop_price is required").
    account_id = await _add_real_account(ex_ctx, base_holdings={"BTC": 100.0})
    service = ex_ctx["service"]
    result = await service.execute_on_accounts(
        account_ids=[account_id], symbol="BTCUSDT", side="SELL", entry=95,
        stop_loss=98, risk_pct=0.01, order_type="STOP_LOSS_LIMIT",
        idempotency_key="sized-sll-real",
    )
    detail = result["results"][0]
    assert detail["status"] == "FILLED"
    placed = ex_ctx["broker"].placed[0]
    assert placed["order_type"] == "STOP_LOSS_LIMIT"
    assert placed["price"] == pytest.approx(95.0)
    assert placed["stop_price"] == pytest.approx(98.0)


# =====================================================================
# Timeout → canonical TIMEOUT/reconcile (no blind retry)
# =====================================================================


async def test_raw_timeout_error_maps_to_unknown_contract(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]
    cid = to_client_order_id("idem-raw-timeout")
    # Raw asyncio.TimeoutError (even if mapped by the broker, the service is safe too).
    ctx["broker"].place_errors[cid] = asyncio.TimeoutError("network timeout")
    ctx["broker"].query_results[cid] = None  # Not found on Binance.

    result = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="idem-raw-timeout",
    )
    # Raw TimeoutError does not escape → canonical UNKNOWN/ORDER_UNKNOWN contract.
    assert result["status"] == "UNKNOWN"
    assert result["error"]["code"] == ErrorCode.ORDER_UNKNOWN
    assert len(ctx["broker"].placed) == 1
    assert len(ctx["broker"].queries) == 1  # reconcile soruldu


async def test_raw_timeout_error_reconciles_to_filled(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]
    cid = to_client_order_id("idem-raw-timeout-fill")
    ctx["broker"].place_errors[cid] = asyncio.TimeoutError("network timeout")
    ctx["broker"].query_results[cid] = OrderResult(status="FILLED", exchange_order_id="EX-T", executed_qty=1.0, avg_price=100.0)

    result = await service.place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
        quantity=1.0, idempotency_key="idem-raw-timeout-fill",
    )
    assert result["status"] == "FILLED"
    assert result["exchange_order_id"] == "EX-T"
    assert len(ctx["broker"].placed) == 1  # No blind retry.


async def test_oco_raw_timeout_no_blind_resend(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    service = ctx["service"]
    cid = to_client_order_id("idem-oco-timeout")
    ctx["broker"].place_errors[cid] = asyncio.TimeoutError("network timeout")
    ctx["broker"].oco_query_results[cid] = None

    result = await service.place_oco_order(
        account_id=account_id, symbol="BTCUSDT", side="SELL",
        quantity=1.0, price=110.0, stop_price=95.0, stop_limit_price=94.0,
        idempotency_key="idem-oco-timeout",
    )
    assert result["status"] == "UNKNOWN"
    assert result["error"]["code"] == ErrorCode.ORDER_UNKNOWN
    assert len(ctx["broker"].placed) == 1


async def test_oco_raw_timeout_reconciles_executing_to_new(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    service = ctx["service"]
    cid = to_client_order_id("idem-oco-timeout-executing")
    ctx["broker"].place_errors[cid] = asyncio.TimeoutError("network timeout")
    ctx["broker"].oco_query_results[cid] = OrderResult(
        status="NEW", exchange_order_id="OL-EXECUTING",
    )

    result = await service.place_oco_order(
        account_id=account_id, symbol="BTCUSDT", side="SELL",
        quantity=1.0, price=110.0, stop_price=95.0, stop_limit_price=94.0,
        idempotency_key="idem-oco-timeout-executing",
    )

    assert result["status"] == "NEW"
    assert result["exchange_order_id"] == "OL-EXECUTING"
    assert len(ctx["broker"].oco_queries) == 1
    assert ctx["broker"].queries == []
    assert len(ctx["broker"].placed) == 1


async def test_reconcile_open_oco_uses_query_oco_for_executing(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    service = ctx["service"]
    cid = to_client_order_id("oco-startup-executing")

    def _insert(conn):
        order = service._insert_order(
            conn, account_id=account_id, idempotency_key="oco-startup-executing",
            symbol="BTCUSDT", side="SELL", order_type="OCO", quantity=1.0,
            price=110.0, stop_price=95.0, stop_limit_price=94.0, notional=100.0,
            reference_price=100.0, equity_snapshot=0.0, status="NEW",
            client_order_id=cid,
        )
        return order["order_id"]

    order_id = await ctx["db"].write(_insert)
    ctx["broker"].oco_query_results[cid] = OrderResult(
        status="NEW", exchange_order_id="OL-STARTUP",
    )

    result = await service.reconcile_open_orders()

    assert result["scanned"] == 1
    assert result["reconciled"] == 1
    assert result["unchanged"] == 0
    assert len(ctx["broker"].oco_queries) == 1
    assert ctx["broker"].oco_queries[0]["list_client_order_id"] == cid
    assert ctx["broker"].queries == []

    def _q(conn):
        return dict(conn.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone())

    row = await ctx["db"].read(_q)
    assert row["status"] == "NEW"
    assert row["exchange_order_id"] == "OL-STARTUP"


async def test_broker_request_maps_timeout_to_timestamp():
    # BinanceOrderBroker._request: asyncio.TimeoutError → canonical TIMEOUT.
    import hashlib
    import hmac

    from rasattrading_mcp.data.order_broker import BinanceOrderBroker

    api_key = "TESTKEY0000000000000000000000000000"
    api_secret = "TESTSECRET00000000000000000000000000"

    class _FakeTimeoutResp:
        async def __aenter__(self):
            raise asyncio.TimeoutError("fake timeout")

        async def __aexit__(self, *a):
            return False

    class _TimeoutSession:
        closed = False

        def request(self, *a, **k):
            return _FakeTimeoutResp()

    async def creds(_aid):
        return (api_key, api_secret)

    broker = BinanceOrderBroker("http://127.0.0.1:1", creds, session=_TimeoutSession())
    with pytest.raises(RasatError) as exc:
        await broker.place_order(
            account_id="a1", symbol="ALICEUSDT", side="BUY", order_type="MARKET",
            quantity=1.0, price=None, client_order_id="x",
        )
    assert exc.value.code == ErrorCode.TIMEOUT


# =====================================================================
# close_all_positions: exchange open orders + audit
# =====================================================================


async def test_close_all_cancels_exchange_open_order_not_in_local_db(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    # Open order on the exchange with no local DB record.
    ctx["broker"].open_orders = [
        {"symbol": "BTCUSDT", "order_id": "EX-ORPHAN", "client_order_id": "orphan-ex-1",
         "side": "BUY", "quantity": 0.5},
    ]
    service = ctx["service"]

    result = await service.close_all_positions(account_id=account_id, actor="test")
    detail = result["results"][0]
    assert detail["closed"] is True
    assert "BTCUSDT" in detail["cancelled"]
    # Orphan exchange order was canceled.
    assert any(c["client_order_id"] == "orphan-ex-1" for c in ctx["broker"].cancelled)
    # and the holding was sold
    sold = {s["symbol"]: s for s in detail["sold"]}
    assert sold["BTCUSDT"]["status"] == "FILLED"


async def test_close_all_exchange_cancel_failure_unknown_closed_false(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    ctx["broker"].open_orders = [
        {"symbol": "BTCUSDT", "order_id": "EX-1", "client_order_id": "orphan-fail",
         "side": "BUY", "quantity": 0.5},
    ]
    ctx["broker"].cancel_errors["orphan-fail"] = RasatError(ErrorCode.TIMEOUT, "cancel network error")
    service = ctx["service"]

    result = await service.close_all_positions(account_id=account_id, actor="test")
    detail = result["results"][0]
    assert detail["closed"] is False
    assert any(e.get("client_order_id") == "orphan-fail" for e in detail["cancel_errors"])


async def test_close_all_cancel_writes_audit(ex_ctx):
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    # Place a local open order.
    ctx["broker"].place_result = {"status": "NEW"}
    placed = await ctx["service"].place_order(
        account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="LIMIT",
        quantity=0.5, price=80, idempotency_key="open-audit",
    )
    ctx["broker"].place_result = None

    await ctx["service"].close_all_positions(account_id=account_id, actor="test")

    log = await ctx["service"].get_audit_log(limit=100)
    actions = [e["action"] for e in log["tail"]]
    assert "order_canceled" in actions
    assert log["verified"] is True


# =====================================================================
# get_total_exposure: per-account errors + risk gating fail-closed
# =====================================================================


async def test_get_total_exposure_reports_incomplete_and_errors(ex_ctx):
    ctx = ex_ctx
    a1 = await _add_real_account(ctx, label="ok", base_holdings={"BTC": 1.0})
    a2 = await _add_real_account(ctx, label="broken", base_holdings={"ETH": 2.0})
    ctx["broker"].get_balance_errors[a2] = RasatError(ErrorCode.TIMEOUT, "network error")
    service = ctx["service"]

    exposure = await service.get_total_exposure()
    assert exposure["complete"] is False
    assert exposure["errors"] == [
        {"account_id": a2, "error": {"code": ErrorCode.TIMEOUT, "message": "network error"}}
    ]
    # The healthy account is still reported; the failed account is not silently counted as zero.
    assert exposure["per_account"][a1] == pytest.approx(100.0)
    assert a2 not in exposure["per_account"]
    assert exposure["by_symbol"]["BTCUSDT"] == pytest.approx(100.0)


async def test_exposure_failure_fails_closed_order(ex_ctx):
    # Risk gating: reject the order when exposure calculation fails (do not proceed with partial data).
    ctx = ex_ctx
    account_id = await _add_real_account(ctx)
    ctx["broker"].get_balance_errors[account_id] = RasatError(ErrorCode.TIMEOUT, "network error")
    service = ctx["service"]
    with pytest.raises(RasatError) as exc:
        await service.place_order(
            account_id=account_id, symbol="BTCUSDT", side="BUY", order_type="MARKET",
            quantity=1.0, idempotency_key="exposure-fail",
        )
    assert exc.value.code == ErrorCode.TIMEOUT
    assert ctx["broker"].placed == []


# =====================================================================
# risk.py / position_sizing.py NaN fail-closed
# =====================================================================


def test_enforce_policy_caps_nan_fail_closed():
    with pytest.raises(RasatError) as exc:
        enforce_policy_caps(symbol="BTCUSDT", notional=float("nan"), policy={})
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    # NaN must not bypass the cap comparison.
    with pytest.raises(RasatError) as exc:
        enforce_policy_caps(
            symbol="BTCUSDT", notional=200.0,
            policy={"max_notional_per_order": float("nan")},
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST


def test_within_tolerance_nan_fail_closed():
    with pytest.raises(RasatError) as exc:
        within_tolerance(value=float("nan"), target=2.0)
    assert exc.value.code == ErrorCode.INVALID_REQUEST


def test_within_tolerance_non_numeric_fail_closed():
    # T01: non-numeric tolerance must not escape as TypeError—require_finite first.
    with pytest.raises(RasatError) as exc:
        within_tolerance(value=1.9, target=2.0, tolerance_pct="abc")
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    with pytest.raises(RasatError) as exc:
        within_tolerance(value="abc", target=2.0)
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    # negatif tolerans hâlâ INVALID_REQUEST
    with pytest.raises(RasatError) as exc:
        within_tolerance(value=1.9, target=2.0, tolerance_pct=-0.1)
    assert exc.value.code == ErrorCode.INVALID_REQUEST


def test_calculate_position_size_nan_fail_closed():
    with pytest.raises(RasatError) as exc:
        calculate_position_size(
            symbol="BTCUSDT", account_balance=float("nan"), risk_pct=0.01,
            entry=100, stop_loss=95, filters=FILTERS,
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST


def test_calculate_position_size_non_numeric_fail_closed():
    # T01: non-numeric entry is handled by require_finite before check_stop_direction.
    # Must fail with INVALID_REQUEST rather than TypeError.
    with pytest.raises(RasatError) as exc:
        calculate_position_size(
            symbol="BTCUSDT", account_balance=10000, risk_pct=0.01,
            entry="abc", stop_loss=95, filters=FILTERS,
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    with pytest.raises(RasatError) as exc:
        calculate_position_size(
            symbol="BTCUSDT", account_balance=10000, risk_pct=0.01,
            entry=100, stop_loss=None, filters=FILTERS,
        )
    assert exc.value.code == ErrorCode.INVALID_REQUEST


# =====================================================================
# tools.py schema synchronization (T02 contract)
# =====================================================================


def test_tools_schema_get_pending_orders_status_enum():
    from rasattrading_mcp.tools import REGISTRY

    spec = REGISTRY.get("get_pending_orders")
    enum = spec.input_schema["properties"]["status"]["enum"]
    for s in ("awaiting_approval", "approved", "executing", "rejected",
              "executed", "reconcile_required", "expired"):
        assert s in enum


def test_tools_schema_order_spec_requires_risk_pct():
    from rasattrading_mcp.tools import REGISTRY

    spec = REGISTRY.get("create_alert")
    order_spec = spec.input_schema["properties"]["order_spec"]
    assert "risk_pct" in order_spec["required"]
    assert "account_id" in order_spec["required"]
    assert order_spec["properties"]["entry"]["exclusiveMinimum"] == 0
    assert order_spec["properties"]["stop_loss"]["exclusiveMinimum"] == 0
    assert order_spec["properties"]["risk_pct"]["maximum"] == 1


# =====================================================================
# HTTP /rpc: schema validation, merge, body limit, redaction
# =====================================================================


def _ready():
    r = Readiness()
    for s in ["starting", "migrating", "warming_up", "ready"]:
        r.set_state(s)
    return r


async def _rpc_post(client, payload: dict):
    return await client.post(
        "/rpc",
        data=json.dumps(payload),
        headers={"Authorization": "Bearer t1", "Content-Type": "application/json"},
    )


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


async def test_rpc_schema_rejects_missing_required(cfg):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="t_place",
            description="",
            input_schema={
                "type": "object",
                "properties": {"symbol": {"type": "string"}, "idempotency_key": {"type": "string"}},
                "required": ["symbol", "idempotency_key"],
                "additionalProperties": False,
            },
        )
    )
    dispatcher = ToolDispatcher(registry)
    app = build_app(cfg, _ready(), "t1", dispatcher)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await _rpc_post(client, {"tool": "t_place", "params": {"symbol": "BTCUSDT"}})
            assert resp.status == 400
            body = await resp.json()
            assert body["error"]["code"] == ErrorCode.INVALID_REQUEST
            assert "missing required field" in body["error"]["message"]


async def test_rpc_schema_rejects_unknown_param(cfg):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="t_place",
            description="",
            input_schema={
                "type": "object",
                "properties": {"symbol": {"type": "string"}},
                "additionalProperties": False,
            },
        )
    )
    dispatcher = ToolDispatcher(registry)
    app = build_app(cfg, _ready(), "t1", dispatcher)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await _rpc_post(client, {"tool": "t_place", "params": {"symbol": "BTCUSDT", "bogus": 1}})
            assert resp.status == 400
            assert (await resp.json())["error"]["code"] == ErrorCode.INVALID_REQUEST


async def test_rpc_schema_rejects_nan_number(cfg):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="t_num",
            description="",
            input_schema={
                "type": "object",
                "properties": {"qty": {"type": "number", "exclusiveMinimum": 0}},
                "required": ["qty"],
            },
        )
    )
    dispatcher = ToolDispatcher(registry)
    app = build_app(cfg, _ready(), "t1", dispatcher)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await _rpc_post(client, {"tool": "t_num", "params": {"qty": float("nan")}})
            assert resp.status == 400
            assert (await resp.json())["error"]["code"] == ErrorCode.INVALID_REQUEST


async def test_rpc_transport_field_merge_and_conflict(cfg):
    seen: dict = {}

    async def _handler(params, ctx):
        seen.update(params)
        return {"echo": params.get("idempotency_key")}, None

    registry = ToolRegistry()
    registry.register(
        ToolSpec(
            name="t_place",
            description="",
            input_schema={
                "type": "object",
                "properties": {"symbol": {"type": "string"}, "idempotency_key": {"type": "string"}},
                "required": ["idempotency_key"],
                "additionalProperties": False,
            },
        )
    )
    dispatcher = ToolDispatcher(registry)
    dispatcher.register("t_place", _handler)
    app = build_app(cfg, _ready(), "t1", dispatcher)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            # Top-level idempotency_key merges into params → required validation passes.
            resp = await _rpc_post(client, {"tool": "t_place", "params": {"symbol": "BTCUSDT"}, "idempotency_key": "top-1"})
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert body["data"]["echo"] == "top-1"
            assert seen["idempotency_key"] == "top-1"

            # Conflict: top-level ≠ params → INVALID_REQUEST.
            resp2 = await _rpc_post(
                client,
                {"tool": "t_place", "params": {"symbol": "BTCUSDT", "idempotency_key": "p-1"}, "idempotency_key": "top-2"},
            )
            assert resp2.status == 400
            assert (await resp2.json())["error"]["code"] == ErrorCode.INVALID_REQUEST


async def test_rpc_body_size_limit(cfg):
    registry = ToolRegistry()
    registry.register(
        ToolSpec(name="t", description="", input_schema={"type": "object", "properties": {}})
    )
    dispatcher = ToolDispatcher(registry)
    app = build_app(cfg, _ready(), "t1", dispatcher)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            big = {"tool": "t", "params": {"blob": "x" * (2 * 1024 * 1024)}}
            resp = await _rpc_post(client, big)
            assert resp.status == 400
            assert (await resp.json())["error"]["code"] == ErrorCode.INVALID_REQUEST


async def test_rpc_generic_error_redacts_exception(cfg):
    async def _boom(params, ctx):
        raise RuntimeError("super secret internal path detail")

    registry = ToolRegistry()
    registry.register(ToolSpec(name="t_boom", description="", input_schema={"type": "object", "properties": {}}))
    dispatcher = ToolDispatcher(registry)
    dispatcher.register("t_boom", _boom)
    app = build_app(cfg, _ready(), "t1", dispatcher)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await _rpc_post(client, {"tool": "t_boom", "params": {}})
            assert resp.status == 500
            body = await resp.json()
            assert body["error"]["code"] == ErrorCode.INTERNAL_ERROR
            assert "super secret" not in body["error"]["message"]


# =====================================================================
# T3: fill entry for market orders + transient error mapping (approve handler)
# =====================================================================


def test_transient_error_codes_exclude_permanent_ones():
    """Transient error set excludes permanent errors—rejected decisions remain reliable."""
    from rasattrading_mcp.daemon import handlers

    transient = handlers._PENDING_TRANSIENT_ERRORS
    assert ErrorCode.TIMEOUT in transient
    assert ErrorCode.STALE_DATA in transient
    assert ErrorCode.RATE_LIMITED in transient
    assert ErrorCode.ORDER_UNKNOWN in transient
    for permanent in (
        ErrorCode.INSUFFICIENT_BALANCE,
        ErrorCode.INVALID_REQUEST,
        ErrorCode.ACCOUNT_NOT_FOUND,
        ErrorCode.FILTER_VIOLATION,
        ErrorCode.ORDER_REJECTED,
        ErrorCode.ORDER_EXPIRED,
    ):
        assert permanent not in transient


async def test_approve_handler_market_missing_entry_uses_market_feed(ex_db, ex_ctx):
    """Approve handler fills a market order's empty entry from the market feed."""
    import uuid

    from rasattrading_mcp.pa.alarms import AlarmService
    from rasattrading_mcp.pa.analysis import PAEngine
    from rasattrading_mcp.storage.state import PENDING_AWAITING_APPROVAL

    from tests.test_execution import FakeMarket

    db = ex_db
    ctx = ex_ctx
    account_id = await _add_real_account(ctx, balance_usdt=10000.0)
    oid = uuid.uuid4().hex
    now = int(time.time())

    def _ins(conn):
        conn.execute(
            "INSERT INTO pending_orders (order_id, alert_id, account_id, symbol, side, order_type, "
            "entry, stop_loss, risk_pct, status, created_at, note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (oid, "alert-x", account_id, "BTCUSDT", "BUY", "market", None, 95.0, 0.01,
             PENDING_AWAITING_APPROVAL, now, None),
        )

    await db.write(_ins)
    market = FakeMarket()
    engine = PAEngine(db)
    alarms = AlarmService(db, engine=engine)
    handler_ctx = {
        "db": db,
        "alarm_service": alarms,
        "order_service": ctx["service"],
        "market": market,
        "actor": "test",
    }

    data, _meta = await approve_pending_order_handler({"order_id": oid}, handler_ctx)
    assert data["status"] == "executed"

    def _q(conn):
        return dict(conn.execute("SELECT * FROM pending_orders WHERE order_id=?", (oid,)).fetchone())

    rec = await db.read(_q)
    assert rec["entry"] == pytest.approx(100.0)  # FakeMarket BTCUSDT price.
    assert rec["status"] == "executed"
