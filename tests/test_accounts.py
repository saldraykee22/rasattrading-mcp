import asyncio
import json
import time
import uuid

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.daemon.readiness import Readiness
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.storage.accounts import AccountService
from rasattrading_mcp.storage.audit import AuditLog
from rasattrading_mcp.storage.credentials import SecretStore
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.storage.risk_policy import RiskPolicyService


@pytest.fixture
async def account_db(tmp_path):
    db = Database(Config(data_dir=tmp_path, pipeline_enabled=False).db_path)
    await db.start()
    yield db
    await db.stop()


@pytest.fixture
async def account_service(account_db):
    await run_migrations(account_db)
    return AccountService(account_db, secret_store=SecretStore(), audit=AuditLog(account_db))


async def test_add_account_encrypts_and_list_never_returns_secrets(account_service, account_db):
    created = await account_service.add_account(
        label="main",
        api_key="AK_TEST_123",
        api_secret="AS_TEST_456",
        tags=["main", "aggressive", "main"],
    )

    assert created["account_id"]
    assert created["credentials_configured"] is True
    assert created["read_only"] is False
    assert created["trading_lock"] == "paper"
    assert "api_key" not in created
    assert "api_secret" not in created

    def _stored(conn):
        row = conn.execute(
            "SELECT encrypted_api_key, encrypted_secret FROM accounts WHERE account_id = ?",
            (created["account_id"],),
        ).fetchone()
        return bytes(row[0]), bytes(row[1])

    encrypted_key, encrypted_secret = await account_db.read(_stored)
    assert b"AK_TEST_123" not in encrypted_key
    assert b"AS_TEST_456" not in encrypted_secret
    assert encrypted_key.startswith(SecretStore.DPAPI_PREFIX) or encrypted_key.startswith(SecretStore.KEYRING_PREFIX)

    listed = await account_service.list_accounts()
    assert listed["mode"] == "authenticated"
    assert listed["read_only"] is False
    assert listed["count"] == 1
    account = listed["accounts"][0]
    assert account["account_id"] == created["account_id"]
    assert account["tags"] == ["main", "aggressive"]
    serialized = json.dumps(listed, ensure_ascii=False)
    assert "AK_TEST_123" not in serialized
    assert "AS_TEST_456" not in serialized
    assert "encrypted_api_key" not in account
    assert "encrypted_secret" not in account

    # Internal execution lookup round-trips only at the service boundary.
    assert await account_service.get_credentials(created["account_id"]) == ("AK_TEST_123", "AS_TEST_456")
    assert await AuditLog(account_db).verify() == []


async def test_account_without_keys_is_explicit_public_read_only(account_service):
    created = await account_service.add_account(label="public", tags=["watch-only"])
    assert created["credentials_configured"] is False
    assert created["read_only"] is True
    assert created["mode"] == "public"

    listed = await account_service.list_accounts()
    assert listed["mode"] == "public"
    assert listed["read_only"] is True
    assert listed["accounts"][0]["read_only"] is True

    with pytest.raises(RasatError) as exc_info:
        await account_service.get_credentials(created["account_id"])
    assert exc_info.value.code == ErrorCode.ACCOUNT_NO_CREDENTIALS


async def test_remove_account_deletes_row_and_is_audited(account_service, account_db):
    created = await account_service.add_account(label="temporary")
    removed = await account_service.remove_account(created["account_id"], actor="test-agent")
    assert removed == {
        "account_id": created["account_id"],
        "removed": True,
        "read_only": True,
    }
    assert (await account_service.list_accounts())["count"] == 0
    assert await AuditLog(account_db).verify() == []

    with pytest.raises(RasatError) as exc_info:
        await account_service.remove_account(created["account_id"])
    assert exc_info.value.code == ErrorCode.ACCOUNT_NOT_FOUND


async def test_account_validation_does_not_partially_store_credentials(account_service):
    with pytest.raises(RasatError) as exc_info:
        await account_service.add_account(label="broken", api_key="only-key")
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST
    assert (await account_service.list_accounts())["count"] == 0


async def test_account_tools_are_registered_and_dispatch(account_service):
    readiness = Readiness()
    ctx = {"account_service": account_service, "readiness": readiness, "started_at": 0, "pipeline": None}
    dispatcher = build_dispatcher(ctx)
    assert {"add_account", "list_accounts", "remove_account"}.issubset(set(dispatcher.names()))

    created, meta = await dispatcher.dispatch("add_account", {"label": "tool-account"}, ctx)
    assert created["account_id"]
    assert meta.source == "sqlite-accounts"
    listed, _ = await dispatcher.dispatch("list_accounts", {}, ctx)
    assert listed["count"] == 1
    removed, _ = await dispatcher.dispatch("remove_account", {"account_id": created["account_id"]}, ctx)
    assert removed["removed"] is True


# ---------- T04: account removal guard (fail-closed) ----------


async def _insert_order(db, account_id, *, status="NEW", order_id=None):
    def _ins(conn):
        oid = order_id or uuid.uuid4().hex
        now = int(time.time())
        conn.execute(
            "INSERT INTO orders "
            "(order_id, account_id, idempotency_key, symbol, side, order_type, quantity, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'BTCUSDT', 'BUY', 'LIMIT', 1.0, ?, ?, ?)",
            (oid, account_id, f"idem-{oid}", status, now, now),
        )

    await db.write(_ins)


async def _insert_pending(db, account_id, *, status="awaiting_approval", order_id=None):
    def _ins(conn):
        oid = order_id or uuid.uuid4().hex
        now = int(time.time())
        conn.execute(
            "INSERT INTO pending_orders "
            "(order_id, alert_id, account_id, symbol, side, order_type, status, created_at) "
            "VALUES (?, ?, ?, 'BTCUSDT', 'BUY', 'market', ?, ?)",
            (oid, f"alert-{oid}", account_id, status, now),
        )

    await db.write(_ins)


async def _order_count(db, account_id):
    def _count(conn):
        return int(conn.execute("SELECT COUNT(*) c FROM orders WHERE account_id = ?", (account_id,)).fetchone()["c"])

    return await db.read(_count)


async def test_remove_real_account_refused_fail_closed(account_service, account_db):
    created = await account_service.add_account(label="real", api_key="AK_REAL", api_secret="AS_REAL")
    await account_service.enable_real_trading(created["account_id"], actor="test")

    with pytest.raises(RasatError) as exc_info:
        await account_service.remove_account(created["account_id"], actor="test-agent")
    err = exc_info.value
    assert err.code == ErrorCode.ACCOUNT_IN_USE
    assert "trading_lock=real" in err.details["reasons"]
    assert err.details["account_id"] == created["account_id"]

    # DB and credentials are unchanged.
    assert (await account_service.list_accounts())["count"] == 1
    assert await account_service.get_credentials(created["account_id"]) == ("AK_REAL", "AS_REAL")

    # Red audit record was added; chain is intact and contains no secret.
    assert await AuditLog(account_db).verify() == []
    entries = await AuditLog(account_db).tail(10)
    refused = [e for e in entries if e["action"] == "remove_account_refused"]
    assert refused
    serialized = json.dumps(refused, ensure_ascii=False)
    assert "AK_REAL" not in serialized
    assert "AS_REAL" not in serialized


@pytest.mark.parametrize("status", ["NEW", "PARTIALLY_FILLED", "UNKNOWN", "RECONCILE_REQUIRED"])
async def test_remove_paper_account_with_open_order_refused(account_service, account_db, status):
    created = await account_service.add_account(label="paper")
    await _insert_order(account_db, created["account_id"], status=status)

    with pytest.raises(RasatError) as exc_info:
        await account_service.remove_account(created["account_id"])
    assert exc_info.value.code == ErrorCode.ACCOUNT_IN_USE
    assert any(r.startswith("open_orders=") for r in exc_info.value.details["reasons"])

    # Account and order remain; nothing was cascade-deleted.
    assert (await account_service.list_accounts())["count"] == 1
    assert await _order_count(account_db, created["account_id"]) == 1


@pytest.mark.parametrize("status", ["awaiting_approval", "approved", "executing", "reconcile_required"])
async def test_remove_paper_account_with_active_pending_refused(account_service, account_db, status):
    created = await account_service.add_account(label="paper")
    await _insert_pending(account_db, created["account_id"], status=status)

    with pytest.raises(RasatError) as exc_info:
        await account_service.remove_account(created["account_id"])
    assert exc_info.value.code == ErrorCode.ACCOUNT_IN_USE
    assert any(r.startswith("active_pending_orders=") for r in exc_info.value.details["reasons"])
    assert (await account_service.list_accounts())["count"] == 1


async def test_remove_paper_account_with_risk_policy_refused(account_service, account_db):
    created = await account_service.add_account(label="paper")
    risk = RiskPolicyService(account_db, audit=AuditLog(account_db))
    await risk.set_risk_policy(created["account_id"], max_notional_per_order=1000, actor="test")

    with pytest.raises(RasatError) as exc_info:
        await account_service.remove_account(created["account_id"])
    assert exc_info.value.code == ErrorCode.ACCOUNT_IN_USE
    assert any(r.startswith("risk_policy=") for r in exc_info.value.details["reasons"])
    assert (await account_service.list_accounts())["count"] == 1


async def test_remove_paper_account_with_risk_override_refused(account_service, account_db):
    created = await account_service.add_account(label="paper")
    risk = RiskPolicyService(account_db, audit=AuditLog(account_db))
    await risk.create_override(created["account_id"], reason="test", idempotency_key="ov-1", actor="test")

    with pytest.raises(RasatError) as exc_info:
        await account_service.remove_account(created["account_id"])
    assert exc_info.value.code == ErrorCode.ACCOUNT_IN_USE
    assert any(r.startswith("risk_overrides=") for r in exc_info.value.details["reasons"])
    assert (await account_service.list_accounts())["count"] == 1


@pytest.mark.parametrize("status", ["executed", "rejected", "expired"])
async def test_remove_paper_account_with_terminal_pending_allowed(account_service, account_db, status):
    created = await account_service.add_account(label="paper")
    await _insert_pending(account_db, created["account_id"], status=status)

    removed = await account_service.remove_account(created["account_id"])
    assert removed["removed"] is True
    assert (await account_service.list_accounts())["count"] == 0


async def test_remove_paper_account_keeps_historical_orders(account_service, account_db):
    created = await account_service.add_account(label="paper")
    await _insert_order(account_db, created["account_id"], status="FILLED", order_id="hist-1")

    removed = await account_service.remove_account(created["account_id"])
    assert removed["removed"] is True
    assert (await account_service.list_accounts())["count"] == 0
    # Historical records are not cascade-deleted.
    assert await _order_count(account_db, created["account_id"]) == 1


async def test_remove_credentialed_paper_account_cleans_credentials(account_service, account_db):
    created = await account_service.add_account(label="temporary", api_key="AK_TMP", api_secret="AS_TMP")
    removed = await account_service.remove_account(created["account_id"])
    assert removed["removed"] is True
    assert removed["read_only"] is False

    with pytest.raises(RasatError) as exc_info:
        await account_service.get_credentials(created["account_id"])
    assert exc_info.value.code == ErrorCode.ACCOUNT_NOT_FOUND


async def test_remove_races_with_dependency_insert_stays_consistent(account_service, account_db):
    created = await account_service.add_account(label="race")
    account_id = created["account_id"]

    def _insert_open_order_if_account_exists(conn):
        if conn.execute("SELECT 1 FROM accounts WHERE account_id = ?", (account_id,)).fetchone() is None:
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, "account was deleted")
        oid = uuid.uuid4().hex
        now = int(time.time())
        conn.execute(
            "INSERT INTO orders "
            "(order_id, account_id, idempotency_key, symbol, side, order_type, quantity, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'BTCUSDT', 'BUY', 'LIMIT', 1.0, 'NEW', ?, ?)",
            (oid, account_id, f"idem-{oid}", now, now),
        )
        return oid

    remove_task = asyncio.create_task(account_service.remove_account(account_id, actor="race-agent"))
    insert_task = asyncio.create_task(account_db.write(_insert_open_order_if_account_exists))
    results = await asyncio.gather(remove_task, insert_task, return_exceptions=True)

    def _snapshot(conn):
        acc = conn.execute("SELECT 1 FROM accounts WHERE account_id = ?", (account_id,)).fetchone()
        count = int(
            conn.execute("SELECT COUNT(*) c FROM orders WHERE account_id = ?", (account_id,)).fetchone()["c"]
        )
        return acc is not None, count

    acc_present, open_order_count = await account_db.read(_snapshot)

    # Under one writer, either insert wins (remove gets ACCOUNT_IN_USE, account remains)
    # or remove wins (account is gone, no orphan order remains). Never both.
    assert not (acc_present is False and open_order_count > 0)
    if acc_present:
        assert isinstance(results[0], RasatError)
        assert results[0].code == ErrorCode.ACCOUNT_IN_USE
        assert open_order_count == 1
    else:
        assert open_order_count == 0
        assert isinstance(results[1], RasatError)
