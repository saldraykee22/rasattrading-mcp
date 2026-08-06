import asyncio

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.daemon.readiness import Readiness
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.risk import DEFAULT_TOLERANCE_PCT, enforce_policy_caps, within_tolerance
from rasattrading_mcp.storage.accounts import AccountService
from rasattrading_mcp.storage.audit import AuditLog
from rasattrading_mcp.storage.credentials import SecretStore
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.storage.risk_policy import RiskPolicyService


@pytest.fixture
async def risk_db(tmp_path):
    db = Database(Config(data_dir=tmp_path, pipeline_enabled=False).db_path)
    await db.start()
    yield db
    await db.stop()


@pytest.fixture
async def services(risk_db):
    await run_migrations(risk_db)
    accounts = AccountService(risk_db, secret_store=SecretStore(), audit=AuditLog(risk_db))
    risk = RiskPolicyService(risk_db, audit=AuditLog(risk_db))
    return risk_db, accounts, risk


async def _add_cred_account(accounts, label="main"):
    return await accounts.add_account(label=label, api_key="AK_TEST_123", api_secret="AS_TEST_456")


# ---------- enable_real_trading ----------


async def test_enable_real_trading_is_permanent_and_audited(services):
    risk_db, accounts, _ = services
    created = await _add_cred_account(accounts)

    result = await accounts.enable_real_trading(created["account_id"], actor="tester")
    assert result["trading_lock"] == "real"
    assert result["already_real"] is False

    # idempotent: tekrar çağrı hata değil
    again = await accounts.enable_real_trading(created["account_id"])
    assert again["already_real"] is True

    assert (await accounts.get_account(created["account_id"]))["trading_lock"] == "real"
    assert await AuditLog(risk_db).verify() == []


async def test_enable_real_trading_rejects_credentialless(services):
    _, accounts, _ = services
    public = await accounts.add_account(label="public")
    with pytest.raises(RasatError) as exc_info:
        await accounts.enable_real_trading(public["account_id"])
    assert exc_info.value.code == ErrorCode.ACCOUNT_NO_CREDENTIALS


async def test_enable_real_trading_unknown_account(services):
    _, accounts, _ = services
    with pytest.raises(RasatError) as exc_info:
        await accounts.enable_real_trading("nope")
    assert exc_info.value.code == ErrorCode.ACCOUNT_NOT_FOUND


# ---------- set_risk_policy ----------


async def test_risk_policy_default_is_empty_unlimited(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)
    policy = await risk.get_policy(created["account_id"])
    assert policy["configured"] is False
    assert policy["policy_version"] == 0
    assert policy["max_notional_per_order"] is None
    assert policy["max_aggregate_exposure"] is None
    assert policy["allowed_symbols"] == []


async def test_set_risk_policy_upserts_and_bumps_version(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)

    first = await risk.set_risk_policy(
        created["account_id"],
        max_notional_per_order=1000,
        allowed_symbols=["btcusdt", "BTCUSDT", "ethusdt"],
    )
    assert first["policy_version"] == 1
    assert first["changed"] is True
    assert first["allowed_symbols"] == ["BTCUSDT", "ETHUSDT"]  # upper + dedup

    # aynı değerle tekrar -> değişiklik yok, versiyon artmaz
    unchanged = await risk.set_risk_policy(
        created["account_id"], max_notional_per_order=1000, allowed_symbols=["BTCUSDT", "ETHUSDT"]
    )
    assert unchanged["policy_version"] == 1
    assert unchanged["changed"] is False

    # yeni değer -> versiyon artar
    changed = await risk.set_risk_policy(created["account_id"], max_aggregate_exposure=5000)
    assert changed["policy_version"] == 2
    assert changed["changed"] is True

    policy = await risk.get_policy(created["account_id"])
    assert policy["policy_version"] == 2
    assert policy["max_notional_per_order"] == 1000
    assert policy["max_aggregate_exposure"] == 5000


async def test_set_risk_policy_validation(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)

    with pytest.raises(RasatError) as exc_info:
        await risk.set_risk_policy(created["account_id"], max_notional_per_order=-5)
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as exc_info:
        await risk.set_risk_policy(created["account_id"], max_notional_per_order="lots")
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as exc_info:
        await risk.set_risk_policy(created["account_id"], allowed_symbols="BTCUSDT")
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST


async def test_set_risk_policy_unknown_account(services):
    risk_db, accounts, risk = services
    with pytest.raises(RasatError) as exc_info:
        await risk.set_risk_policy("nope", max_notional_per_order=100)
    assert exc_info.value.code == ErrorCode.ACCOUNT_NOT_FOUND


# ---------- override_risk_policy ----------


async def test_override_reserved_and_single_use(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)
    await risk.set_risk_policy(created["account_id"], max_notional_per_order=1000)

    override = await risk.create_override(created["account_id"], reason="büyük pozisyon")
    assert override["state"] == "reserved"
    assert override["scope"] == "next_order"
    assert override["policy_version"] == 1
    assert override["expires_at"] > override["created_at"]

    # ilk tüketim kazanır
    consumed = await risk.consume_override(
        created["account_id"], policy_version=1, consumed_by_idem="order-1"
    )
    assert consumed is not None
    assert consumed["state"] == "applied"
    assert consumed["consumed_by_idem"] == "order-1"

    # ikinci tüketim aynı override'ı alamaz
    second = await risk.consume_override(created["account_id"], policy_version=1, consumed_by_idem="order-2")
    assert second is None

    assert await AuditLog(risk_db).verify() == []


async def test_override_idempotent_retry_no_duplicate(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)

    first = await risk.create_override(created["account_id"], reason="tek", idempotency_key="ov-1")
    retry = await risk.create_override(created["account_id"], reason="tek", idempotency_key="ov-1")
    assert retry["override_id"] == first["override_id"]
    assert retry["state"] == "reserved"

    def _count(conn):
        return conn.execute(
            "SELECT COUNT(*) AS c FROM risk_override WHERE account_id=? AND idempotency_key=?",
            (created["account_id"], "ov-1"),
        ).fetchone()["c"]

    assert await risk_db.read(_count) == 1


async def test_override_requires_reason_and_valid_scope(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)

    with pytest.raises(RasatError) as exc_info:
        await risk.create_override(created["account_id"], reason="  ")
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as exc_info:
        await risk.create_override(created["account_id"], reason="x", scope="all_orders")
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as exc_info:
        await risk.create_override("nope", reason="x")
    assert exc_info.value.code == ErrorCode.ACCOUNT_NOT_FOUND


async def test_concurrent_consumers_single_winner(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)
    await risk.create_override(created["account_id"], reason="yarış")

    results = await asyncio.gather(
        risk.consume_override(created["account_id"], policy_version=0, consumed_by_idem="order-a"),
        risk.consume_override(created["account_id"], policy_version=0, consumed_by_idem="order-b"),
        risk.consume_override(created["account_id"], policy_version=0, consumed_by_idem="order-c"),
    )
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert all(r["state"] == "applied" for r in winners)


async def test_override_stale_when_policy_version_changes(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)
    await risk.set_risk_policy(created["account_id"], max_notional_per_order=1000)

    await risk.create_override(created["account_id"], reason="eski politikaya bağlı")
    await risk.set_risk_policy(created["account_id"], max_notional_per_order=5000)

    # policy_version değişti; eski override tüketilemez ve reconcile edilir
    consumed = await risk.consume_override(created["account_id"], policy_version=2, consumed_by_idem="order-x")
    assert consumed is None

    def _state(conn):
        return conn.execute(
            "SELECT state FROM risk_override WHERE account_id=?", (created["account_id"],)
        ).fetchone()["state"]

    assert await risk_db.read(_state) == "reconciled"


async def test_reconcile_expired_overrides(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)
    override = await risk.create_override(created["account_id"], reason="süresi dolar")
    reconciled = await risk.reconcile_overrides(now=override["expires_at"] + 1)
    assert reconciled == 1

    def _state(conn):
        return conn.execute("SELECT state FROM risk_override", ()).fetchone()["state"]

    assert await risk_db.read(_state) == "reconciled"
    # reconcile idempotent
    assert await risk.reconcile_overrides() == 0


# ---------- tolerans ilkesi + katı cap'ler ----------


def test_within_tolerance_soft_threshold():
    # RR hedef 2.0, tolerans %2 -> 1.97 (band içi) kabul, 1.95 (band dışı) red
    assert within_tolerance(1.97, 2.0, DEFAULT_TOLERANCE_PCT)
    assert within_tolerance(1.95, 2.0, DEFAULT_TOLERANCE_PCT) is False
    assert within_tolerance(2.03, 2.0, DEFAULT_TOLERANCE_PCT)
    # tolerans sıfır = katı
    assert within_tolerance(1.999, 2.0, 0) is False
    assert within_tolerance(2.0, 2.0, 0)


def test_caps_are_strict_no_tolerance():
    policy = {"max_notional_per_order": 1000.0, "allowed_symbols": ["BTCUSDT"]}

    enforce_policy_caps(symbol="BTCUSDT", notional=1000.0, policy=policy)
    enforce_policy_caps(symbol="BTCUSDT", notional=999.99, policy=policy)

    with pytest.raises(RasatError) as exc_info:
        enforce_policy_caps(symbol="BTCUSDT", notional=1000.01, policy=policy)
    assert exc_info.value.code == ErrorCode.RISK_LIMIT_EXCEEDED

    with pytest.raises(RasatError) as exc_info:
        enforce_policy_caps(symbol="ETHUSDT", notional=500, policy=policy)
    assert exc_info.value.code == ErrorCode.SYMBOL_NOT_ALLOWED


def test_aggregate_exposure_cap_is_strict():
    policy = {"max_aggregate_exposure": 5000.0}
    enforce_policy_caps(symbol="BTCUSDT", notional=1000, policy=policy, aggregate_exposure=5000.0)
    with pytest.raises(RasatError) as exc_info:
        enforce_policy_caps(symbol="BTCUSDT", notional=1000, policy=policy, aggregate_exposure=5000.01)
    assert exc_info.value.code == ErrorCode.RISK_LIMIT_EXCEEDED


def test_caps_default_unlimited():
    enforce_policy_caps(symbol="ANY", notional=10_000_000, policy={})


# ---------- tool dispatch ----------


async def test_risk_tools_registered_and_dispatch(services):
    risk_db, accounts, risk = services
    created = await _add_cred_account(accounts)
    readiness = Readiness()
    ctx = {
        "account_service": accounts,
        "risk_service": risk,
        "readiness": readiness,
        "started_at": 0,
        "pipeline": None,
    }
    dispatcher = build_dispatcher(ctx)
    expected = {"enable_real_trading", "set_risk_policy", "override_risk_policy", "get_risk_policy"}
    assert expected.issubset(set(dispatcher.names()))

    enabled, _ = await dispatcher.dispatch(
        "enable_real_trading", {"account_id": created["account_id"]}, ctx
    )
    assert enabled["trading_lock"] == "real"

    policy, _ = await dispatcher.dispatch(
        "set_risk_policy", {"account_id": created["account_id"], "max_notional_per_order": 1000}, ctx
    )
    assert policy["policy_version"] == 1

    override, _ = await dispatcher.dispatch(
        "override_risk_policy",
        {"account_id": created["account_id"], "reason": "tool üzerinden", "idempotency_key": "ov-tool"},
        ctx,
    )
    assert override["state"] == "reserved"

    fetched, _ = await dispatcher.dispatch("get_risk_policy", {"account_id": created["account_id"]}, ctx)
    assert fetched["max_notional_per_order"] == 1000

    # temel doğruluk kontrolleri override'dan bağımsız çalışır (3.3 kapsamı, burada dokunulmaz)
    policy = await risk.get_policy(created["account_id"])
    assert policy["policy_version"] == 1
