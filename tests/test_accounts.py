import json

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
