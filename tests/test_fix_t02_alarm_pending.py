"""T02 — Pending approval state machine (CAS, T00 contract) + approve preflight.

- `approve_pending_order_handler` does not treat a structured REJECTED/UNKNOWN result as `executed`
  it does not execute; after an exception, the record does not remain locked in `approved`.
- Two concurrent approvals for the same pending order produce only one execution claim.
- `executed_order_id` is filled only with a confirmed order identity.
- Missing/NaN risk_pct returns INVALID_REQUEST BEFORE the broker.
"""

import asyncio
import math
import time
import uuid

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import approve_pending_order_handler
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.alarms import (
    PENDING_AWAITING,
    PENDING_EXECUTING,
    PENDING_RECONCILE_REQUIRED,
    PENDING_REJECTED,
    AlarmService,
)
from rasattrading_mcp.storage.accounts import AccountService
from rasattrading_mcp.storage.audit import AuditLog
from rasattrading_mcp.storage.credentials import SecretStore
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.storage.orders import OrderService
from rasattrading_mcp.storage.risk_policy import RiskPolicyService

from tests.helpers import FakeOrderBroker
from tests.test_execution import FILTERS, FakeMarket


@pytest.fixture
async def t02_db(tmp_path):
    db = Database(Config(data_dir=tmp_path, pipeline_enabled=False).db_path)
    await db.start()
    await run_migrations(db)
    yield db
    await db.stop()


@pytest.fixture
async def t02_ctx(t02_db):
    accounts = AccountService(t02_db, secret_store=SecretStore(), audit=AuditLog(t02_db))
    risk = RiskPolicyService(t02_db, audit=AuditLog(t02_db))
    broker = FakeOrderBroker()
    market = FakeMarket()
    service = OrderService(
        t02_db,
        accounts=accounts,
        risk=risk,
        broker=broker,
        market=market,
        audit=AuditLog(t02_db),
    )
    engine = PAEngine(t02_db)
    alarms = AlarmService(t02_db, engine=engine)
    engine.alarm_service = alarms
    ctx = {
        "db": t02_db,
        "alarm_service": alarms,
        "order_service": service,
        "accounts": accounts,
        "risk": risk,
        "broker": broker,
        "market": market,
        "actor": "test",
    }
    yield ctx
    await t02_db.stop()


async def _add_paper_account(ctx, label="paper"):
    created = await ctx["accounts"].add_account(label=label, tags=[])
    return created["account_id"]


async def _add_real_account(ctx, label="real", balance_usdt=10000.0):
    created = await ctx["accounts"].add_account(label=label, api_key=f"AK_{label}", api_secret=f"AS_{label}", tags=[])
    await ctx["accounts"].enable_real_trading(created["account_id"], actor="test")
    ctx["broker"].balances[created["account_id"]] = {"USDT": balance_usdt}
    return created["account_id"]


async def _insert_pending(
    db,
    *,
    account_id,
    symbol="BTCUSDT",
    side="BUY",
    entry=100.0,
    stop_loss=95.0,
    risk_pct=0.01,
    order_type="market",
    status=PENDING_AWAITING,
):
    oid = uuid.uuid4().hex
    now = int(time.time())

    def _w(conn):
        conn.execute(
            "INSERT INTO pending_orders (order_id, alert_id, account_id, symbol, side, order_type, "
            "entry, stop_loss, risk_pct, status, created_at, note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (oid, "alert-x", account_id, symbol, side, order_type, entry, stop_loss, risk_pct, status, now, None),
        )

    await db.write(_w)
    return oid


async def _insert_executing_pending(db, *, account_id, execution_started_at=None, **overrides):
    """Simulate an approved order left in `executing` after a daemon crash."""
    oid = uuid.uuid4().hex
    now = int(time.time())
    started = execution_started_at if execution_started_at is not None else now

    def _w(conn):
        conn.execute(
            "INSERT INTO pending_orders (order_id, alert_id, account_id, symbol, side, order_type, "
            "entry, stop_loss, risk_pct, status, created_at, approved_at, execution_started_at, "
            "last_attempt_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                oid, "alert-x", account_id, overrides.get("symbol", "BTCUSDT"),
                overrides.get("side", "BUY"), overrides.get("order_type", "market"),
                overrides.get("entry", 100.0), overrides.get("stop_loss", 95.0),
                overrides.get("risk_pct", 0.01), PENDING_EXECUTING, now, now, started, now,
            ),
        )

    await db.write(_w)
    return oid


def _insert_order_for_pending(db, account_id, pending_order_id, *, status="FILLED", exchange_order_id="EX-REC"):
    """Create the orders-table match idempotency_key = 'pending:'||order_id."""

    def _w(conn):
        now = int(time.time())
        conn.execute(
            "INSERT INTO orders (order_id, account_id, idempotency_key, symbol, side, order_type, "
            "quantity, status, exchange_order_id, client_order_id, notional, reference_price, "
            "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                f"ord-{pending_order_id}", account_id, f"pending:{pending_order_id}", "BTCUSDT", "BUY",
                "MARKET", 1.0, status, exchange_order_id, f"cid-{pending_order_id}", 100.0, 100.0, now, now,
            ),
        )

    return db.write(_w)


def _pending_status(db, order_id):
    def _q(conn):
        return dict(conn.execute("SELECT * FROM pending_orders WHERE order_id=?", (order_id,)).fetchone())

    return db.read(_q)


def _order_rows(db):
    def _q(conn):
        return [dict(r) for r in conn.execute("SELECT * FROM orders").fetchall()]

    return db.read(_q)


async def test_approve_success_marks_executed_with_order_id(t02_ctx):
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    data, _meta = await approve_pending_order_handler({"order_id": oid}, ctx)

    assert data["status"] == "executed"
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == "executed"
    assert rec["executed_order_id"] == data["executed_order_id"]
    assert rec["executed_order_id"], "executed_order_id must be set on confirmed success"
    assert rec["execution_error_code"] is None
    assert len(await _order_rows(ctx["db"])) == 1


async def test_approve_structured_rejected_not_executed(t02_ctx):
    ctx = t02_ctx
    # Bakiye 0 → execute_on_accounts structured REJECTED (INSUFFICIENT_BALANCE).
    account_id = await _add_real_account(ctx, balance_usdt=0.0)
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.INSUFFICIENT_BALANCE

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_REJECTED
    assert rec["executed_order_id"] is None
    assert rec["execution_error_code"] == ErrorCode.INSUFFICIENT_BALANCE
    assert len(await _order_rows(ctx["db"])) == 0


async def test_approve_structured_unknown_reconcile(t02_ctx):
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    ctx["broker"].place_errors_by_account[account_id] = RuntimeError("network error")
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.ORDER_RECONCILE_REQUIRED

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None
    assert rec["execution_error_code"] == ErrorCode.INTERNAL_ERROR
    assert rec["last_attempt_at"] is not None


async def test_approve_exception_after_claim_not_stuck_in_approved(t02_ctx):
    ctx = t02_ctx
    # Missing account → execute_on_accounts exception (ACCOUNT_NOT_FOUND).
    oid = await _insert_pending(ctx["db"], account_id="missing-acc")

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.ACCOUNT_NOT_FOUND

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_REJECTED  # deterministic rejection → rejected
    assert rec["executed_order_id"] is None
    assert rec["execution_error_code"] == ErrorCode.ACCOUNT_NOT_FOUND
    assert rec["status"] not in ("approved", PENDING_EXECUTING)


async def test_approve_missing_risk_pct_preflight_before_broker(t02_ctx):
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id, risk_pct=None)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_AWAITING  # Fails before the preflight claim.
    assert len(ctx["broker"].placed) == 0
    assert len(await _order_rows(ctx["db"])) == 0


async def test_approve_nan_risk_pct_preflight_before_broker(t02_ctx):
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id, risk_pct=float("nan"))

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_AWAITING
    assert len(ctx["broker"].placed) == 0


async def test_approve_ambiguous_empty_results_reconcile(t02_ctx):
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    class _EmptyResults:
        async def execute_on_accounts(self, **kwargs):
            return {"results": [], "count": 0, "succeeded": 0, "failed": 0}

    ctx["order_service"] = _EmptyResults()
    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.ORDER_RECONCILE_REQUIRED

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None


async def test_concurrent_approve_single_execution_claim(t02_ctx):
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    outcomes = await asyncio.gather(
        approve_pending_order_handler({"order_id": oid}, ctx),
        approve_pending_order_handler({"order_id": oid}, ctx),
        return_exceptions=True,
    )
    ok = [o for o in outcomes if isinstance(o, tuple) and o[0]["status"] == "executed"]
    errors = [o for o in outcomes if isinstance(o, RasatError)]
    assert len(ok) == 1
    assert len(errors) == 1
    assert errors[0].code == ErrorCode.INVALID_REQUEST

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == "executed"
    assert len(await _order_rows(ctx["db"])) == 1  # One order record.
    assert len(ctx["broker"].placed) == 0  # paper → broker'a gitmez


async def test_approve_and_claim_cas_second_call_fails(t02_ctx):
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    claim = await ctx["alarm_service"].approve_and_claim_pending_execution(oid)
    assert claim["status"] == PENDING_EXECUTING

    with pytest.raises(RasatError) as exc_info:
        await ctx["alarm_service"].approve_and_claim_pending_execution(oid)
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST


async def test_order_spec_requires_risk_pct_and_finite_at_creation(t02_ctx):
    alarms = t02_ctx["alarm_service"]
    cond = [{"type": "above_below_vwap", "position": "above"}]

    with pytest.raises(RasatError) as e1:
        await alarms.create_alert(
            "BTCUSDT", "1h", cond,
            order_spec={"account_id": "a", "symbol": "BTCUSDT", "side": "BUY"},
        )
    assert e1.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as e2:
        await alarms.create_alert(
            "BTCUSDT", "1h", cond,
            order_spec={"account_id": "a", "symbol": "BTCUSDT", "side": "BUY", "risk_pct": float("nan")},
        )
    assert e2.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as e3:
        await alarms.create_alert(
            "BTCUSDT", "1h", cond,
            order_spec={"account_id": "a", "symbol": "BTCUSDT", "side": "BUY", "risk_pct": math.inf},
        )
    assert e3.value.code == ErrorCode.INVALID_REQUEST


async def test_approve_unknown_status_keeps_reconcile_not_executed(t02_ctx):
    """UNKNOWN structured result moves the record to reconcile_required, not executed."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    ctx["broker"].place_errors_by_account[account_id] = RuntimeError("network timeout")
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError):
        await approve_pending_order_handler({"order_id": oid}, ctx)

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None


# ---------- broker status mapping (only FILLED/paper is confirmed success) ----------


async def test_approve_real_filled_uses_exchange_order_id(t02_ctx):
    """On a real account, FILLED is confirmed success; executed_order_id gets the exchange id."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    data, _meta = await approve_pending_order_handler({"order_id": oid}, ctx)

    assert data["status"] == "executed"
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == "executed"
    assert rec["executed_order_id"].startswith("EX")  # Exchange order identity.


async def test_approve_broker_new_is_reconcile_not_executed(t02_ctx):
    """NEW (pending limit order) is NOT confirmed success → reconcile_required, empty id."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    ctx["broker"].place_result = {"status": "NEW", "exchange_order_id": "EX-NEW-1"}
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.ORDER_RECONCILE_REQUIRED

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None


async def test_approve_broker_partially_filled_is_reconcile(t02_ctx):
    """PARTIALLY_FILLED is non-terminal → reconcile_required, empty executed_order_id."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    ctx["broker"].place_result = {"status": "PARTIALLY_FILLED", "exchange_order_id": "EX-PF-1"}
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.ORDER_RECONCILE_REQUIRED

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None


async def test_approve_broker_canceled_is_rejected(t02_ctx):
    """CANCELED deterministic rejected → pending rejected, empty executed_order_id."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    ctx["broker"].place_result = {"status": "CANCELED", "exchange_order_id": "EX-C-1"}
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError):
        await approve_pending_order_handler({"order_id": oid}, ctx)

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_REJECTED
    assert rec["executed_order_id"] is None
    assert rec["execution_error_code"] == ErrorCode.ORDER_REJECTED


async def test_approve_broker_expired_is_rejected(t02_ctx):
    """EXPIRED deterministic rejected → pending rejected, empty executed_order_id."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    ctx["broker"].place_result = {"status": "EXPIRED", "exchange_order_id": "EX-E-1"}
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.ORDER_EXPIRED

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_REJECTED
    assert rec["executed_order_id"] is None
    assert rec["execution_error_code"] == ErrorCode.ORDER_EXPIRED


# =====================================================================
# T3: recover executing state (reconcile after daemon restart)
# =====================================================================


async def test_reconcile_pending_execution_filled_completes(t02_ctx):
    """FILLED orders record completes the executing record as executed (id is filled)."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    oid = await _insert_executing_pending(ctx["db"], account_id=account_id, execution_started_at=int(time.time()) - 600)
    await _insert_order_for_pending(ctx["db"], account_id, oid, status="FILLED", exchange_order_id="EX-REC")

    res = await ctx["alarm_service"].reconcile_pending_executions()

    assert res["scanned"] == 1
    assert res["completed"] == 1
    assert res["failed"] == 0
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == "executed"
    assert rec["executed_order_id"] == "EX-REC"


async def test_reconcile_pending_execution_paper_completes(t02_ctx):
    """PAPER orders record → confirmed success, executed."""
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_executing_pending(ctx["db"], account_id=account_id, execution_started_at=int(time.time()) - 600)
    await _insert_order_for_pending(ctx["db"], account_id, oid, status="paper", exchange_order_id="LOCAL-1")

    res = await ctx["alarm_service"].reconcile_pending_executions()
    assert res["completed"] == 1
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == "executed"
    assert rec["executed_order_id"] == "LOCAL-1"


async def test_reconcile_pending_execution_rejected_order_fails(t02_ctx):
    """REJECTED orders record → executing record becomes deterministic rejected."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    oid = await _insert_executing_pending(ctx["db"], account_id=account_id, execution_started_at=int(time.time()) - 600)
    await _insert_order_for_pending(ctx["db"], account_id, oid, status="REJECTED")

    res = await ctx["alarm_service"].reconcile_pending_executions()
    assert res["scanned"] == 1
    assert res["failed"] == 1
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_REJECTED
    assert rec["execution_error_code"] == ErrorCode.ORDER_REJECTED


async def test_reconcile_pending_execution_unknown_order_reconciles(t02_ctx):
    """UNKNOWN orders record (network uncertainty) → reconcile_required, empty executed_order_id."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    oid = await _insert_executing_pending(ctx["db"], account_id=account_id, execution_started_at=int(time.time()) - 600)
    await _insert_order_for_pending(ctx["db"], account_id, oid, status="UNKNOWN", exchange_order_id="EX-UNK")

    res = await ctx["alarm_service"].reconcile_pending_executions()
    assert res["failed"] == 1
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None


async def test_reconcile_pending_execution_no_order_stale_reconcile_required(t02_ctx):
    """No orders match + old execution_started_at → fail-closed reconcile_required."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    oid = await _insert_executing_pending(ctx["db"], account_id=account_id, execution_started_at=int(time.time()) - 600)

    res = await ctx["alarm_service"].reconcile_pending_executions()
    assert res["scanned"] == 1
    assert res["matched"] == 0
    assert res["failed"] == 1
    assert res["untouched"] == 0
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None
    assert rec["execution_error_code"] == ErrorCode.ORDER_UNKNOWN


async def test_reconcile_pending_execution_no_order_fresh_untouched(t02_ctx):
    """No orders match but execution_started_at is fresh → leave unchanged."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    oid = await _insert_executing_pending(ctx["db"], account_id=account_id, execution_started_at=int(time.time()))

    res = await ctx["alarm_service"].reconcile_pending_executions()
    assert res["scanned"] == 1
    assert res["untouched"] == 1
    assert res["failed"] == 0
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_EXECUTING  # hâlâ executing


async def test_reconcile_pending_execution_ignores_other_statuses(t02_ctx):
    """Do not touch records outside executing."""
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    awaiting = await _insert_pending(ctx["db"], account_id=account_id)
    res = await ctx["alarm_service"].reconcile_pending_executions()
    assert res["scanned"] == 0
    assert (await _pending_status(ctx["db"], awaiting))["status"] == PENDING_AWAITING


# =====================================================================
# T3: recover market-order entry/stop_loss before approval
# =====================================================================


async def test_approve_market_missing_entry_fills_market_price(t02_ctx):
    """If a market order has empty entry, fill it with current market price and approve successfully."""
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id, entry=None)

    data, _meta = await approve_pending_order_handler({"order_id": oid}, ctx)

    assert data["status"] == "executed"
    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == "executed"
    assert rec["entry"] == pytest.approx(100.0)  # FakeMarket BTCUSDT price.
    assert len(ctx["broker"].placed) == 0  # paper → broker'a gitmez


async def test_approve_market_missing_entry_and_no_price_fails_closed_awaiting(t02_ctx):
    """Market order with missing entry and unavailable price → STALE_DATA; record remains awaiting."""
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    ctx["market"].stale = {"BTCUSDT"}
    oid = await _insert_pending(ctx["db"], account_id=account_id, entry=None)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.STALE_DATA

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_AWAITING  # Not permanently rejected.
    assert len(await _order_rows(ctx["db"])) == 0


async def test_approve_market_missing_stop_loss_rejects_before_broker(t02_ctx):
    """Market order with missing stop_loss → INVALID_REQUEST; record remains awaiting_approval."""
    ctx = t02_ctx
    account_id = await _add_paper_account(ctx)
    oid = await _insert_pending(ctx["db"], account_id=account_id, stop_loss=None)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_AWAITING  # execute_on_accounts'a gidilmedi
    assert len(ctx["broker"].placed) == 0
    assert len(await _order_rows(ctx["db"])) == 0


# =====================================================================
# T3: transient errors become reconcile_required, not rejected
# =====================================================================


async def test_approve_stale_data_structured_result_maps_reconcile(t02_ctx):
    """Structured REJECTED + STALE_DATA code → not permanently rejected, reconcile_required."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    ctx["market"].stale = {"BTCUSDT"}  # execute_on_accounts → STALE_DATA structured REJECTED
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.STALE_DATA

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None
    assert rec["execution_error_code"] == ErrorCode.STALE_DATA


async def test_approve_insufficient_balance_stays_rejected(t02_ctx):
    """Permanent error (INSUFFICIENT_BALANCE) remains rejected—no retry."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx, balance_usdt=0.0)
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError) as exc_info:
        await approve_pending_order_handler({"order_id": oid}, ctx)
    assert exc_info.value.code == ErrorCode.INSUFFICIENT_BALANCE

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_REJECTED
    assert rec["execution_error_code"] == ErrorCode.INSUFFICIENT_BALANCE
