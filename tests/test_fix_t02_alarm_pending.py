"""T02 — Pending approval state machine (CAS, T00 sözleşmesi) + approve preflight.

- `approve_pending_order_handler` structured REJECTED/UNKNOWN sonucunu `executed`
  yapmaz; exception sonrası kayıt `approved` kilidinde kalmaz.
- Aynı pending order için iki eşzamanlı approve yalnızca tek execution claim üretir.
- `executed_order_id` yalnızca kesin emir kimliğiyle doldurulur.
- Missing/NaN risk_pct broker'dan ÖNCE INVALID_REQUEST döner.
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
    assert rec["executed_order_id"], "kesin başarıda executed_order_id dolu olmalı"
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
    ctx["broker"].place_errors_by_account[account_id] = RuntimeError("ağ hatası")
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
    # Var olmayan hesap → execute_on_accounts exception (ACCOUNT_NOT_FOUND).
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
    assert rec["status"] == PENDING_AWAITING  # preflight claim'den önce keser
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
    assert len(await _order_rows(ctx["db"])) == 1  # tek emir kaydı
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
    """UNKNOWN structured sonuç kaydı reconcile_required'a taşır, executed yapmaz."""
    ctx = t02_ctx
    account_id = await _add_real_account(ctx)
    ctx["broker"].place_errors_by_account[account_id] = RuntimeError("ağ zaman aşımı")
    oid = await _insert_pending(ctx["db"], account_id=account_id)

    with pytest.raises(RasatError):
        await approve_pending_order_handler({"order_id": oid}, ctx)

    rec = await _pending_status(ctx["db"], oid)
    assert rec["status"] == PENDING_RECONCILE_REQUIRED
    assert rec["executed_order_id"] is None
