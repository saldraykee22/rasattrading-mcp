import asyncio

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.emergency_stop import EmergencyStopRunner
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.position_sizing import SymbolFilters
from rasattrading_mcp.storage.accounts import AccountService
from rasattrading_mcp.storage.audit import AuditLog
from rasattrading_mcp.storage.credentials import SecretStore
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.emergency_log import EmergencyLog, reconcile_emergency_log
from rasattrading_mcp.storage.migrations import run_migrations

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

    async def symbol_valid(self, symbol: str) -> bool:
        return symbol in self.price_map

    async def price(self, symbol: str) -> float | None:
        return self.price_map.get(symbol)

    async def filters(self, symbol: str) -> SymbolFilters | None:
        if symbol not in self.price_map:
            return None
        return SymbolFilters(**{**FILTERS.__dict__, "symbol": symbol})


@pytest.fixture
async def em_db(tmp_path):
    db = Database(Config(data_dir=tmp_path, pipeline_enabled=False).db_path)
    await db.start()
    await run_migrations(db)
    yield db
    await db.stop()


@pytest.fixture
async def em_ctx(em_db, tmp_path):
    accounts = AccountService(em_db, secret_store=SecretStore(), audit=AuditLog(em_db))
    broker = FakeOrderBroker()
    market = FakeMarket()
    log = EmergencyLog(tmp_path / "emergency_stop.log")
    runner = EmergencyStopRunner(Config(data_dir=tmp_path, pipeline_enabled=False), accounts, broker, log, market_price=market)
    return {"db": em_db, "accounts": accounts, "broker": broker, "market": market,
            "log": log, "runner": runner, "data_dir": tmp_path}


async def _add_real_account(em_ctx, label="main", base_holdings=None, tags=None):
    ctx = em_ctx
    created = await ctx["accounts"].add_account(label=label, api_key=f"AK_{label}", api_secret=f"AS_{label}", tags=tags or [])
    await ctx["accounts"].enable_real_trading(created["account_id"], actor="test")
    balances = {"USDT": 10000.0}
    balances.update(base_holdings or {})
    ctx["broker"].balances[created["account_id"]] = balances
    return created["account_id"]


# ---------- emergency log ----------


def test_emergency_log_append_verify(tmp_path):
    log = EmergencyLog(tmp_path / "emergency_stop.log")
    h1 = log.append(actor="emergency_stop", action="emergency_sell",
                    details={"idem_key": "acc:sym", "symbol": "BTCUSDT", "quantity": 1.0})
    h2 = log.append(actor="emergency_stop", action="emergency_sell",
                    details={"idem_key": "acc:sym2", "symbol": "ETHUSDT", "quantity": 2.0})
    assert h1 != h2
    assert log.verify() == []
    assert log.is_action_done("emergency_sell", "acc:sym") is True
    assert log.is_action_done("emergency_sell", "acc:other") is False


def test_emergency_log_detects_tamper(tmp_path):
    log = EmergencyLog(tmp_path / "emergency_stop.log")
    log.append(actor="emergency_stop", action="emergency_sell", details={"key": "k1", "quantity": 1.0})
    log.append(actor="emergency_stop", action="emergency_sell", details={"key": "k2", "quantity": 2.0})

    # ikinci satırı değiştir
    lines = log.path.read_text(encoding="utf-8").splitlines()
    modified = lines[1].replace('"quantity": 2.0', '"quantity": 999.0')
    log.path.write_text(lines[0] + "\n" + modified + "\n", encoding="utf-8")
    assert len(log.verify()) > 0


# ---------- emergency stop runner ----------


async def test_emergency_stop_sells_and_cancels(em_ctx, monkeypatch):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0, "ETH": 2.0})
    runner = ctx["runner"]

    # onayı otomatik ver (yes=True)
    result = await runner.run(account_ids=[account_id], yes=True)
    assert result["ok"] is True
    detail = result["results"][0]
    assert detail["ok"] is True
    assert len(detail["sold"]) == 2
    statuses = {s["symbol"]: s["status"] for s in detail["sold"]}
    assert statuses == {"BTCUSDT": "FILLED", "ETHUSDT": "FILLED"}
    # açık emirler iptal edildi
    assert len(ctx["broker"].cancelled_all) == 1  # BTCUSDT açık emri
    # log yazıldı
    assert ctx["log"].is_action_done("emergency_sell", f"{account_id}:BTCUSDT")
    assert ctx["log"].verify() == []


async def test_emergency_stop_no_double_sell(em_ctx):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    runner = ctx["runner"]

    first = await runner.run(account_ids=[account_id], yes=True)
    assert first["ok"] is True
    placed_after_first = len(ctx["broker"].placed)

    # ikinci çalıştırma: bakiye artık 0 (fake satışı uygular) + log idempotency
    second = await runner.run(account_ids=[account_id], yes=True)
    assert second["ok"] is True
    assert len(ctx["broker"].placed) == placed_after_first  # çift satış yok


async def test_emergency_stop_requires_confirmation(em_ctx, monkeypatch):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    runner = ctx["runner"]

    # onay yoksa abort
    monkeypatch.setattr("builtins.input", lambda *a, **k: "n")
    result = await runner.run(account_ids=[account_id], yes=False)
    assert result["ok"] is False
    assert result["results"][0]["error"]["code"] == "ABORTED"
    assert len(ctx["broker"].placed) == 0


async def test_emergency_stop_dry_run_no_network(em_ctx):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    runner = ctx["runner"]
    result = await runner.run(account_ids=[account_id], yes=True, dry_run=True)
    detail = result["results"][0]
    assert detail["sold"][0]["status"] == "DRY_RUN"
    assert len(ctx["broker"].placed) == 0
    assert ctx["log"].verify() == []


async def test_emergency_stop_skips_public_account(em_ctx):
    ctx = em_ctx
    public = await ctx["accounts"].add_account(label="public")
    runner = ctx["runner"]
    result = await runner.run(account_ids=[public["account_id"]], yes=True)
    assert result["results"][0]["ok"] is False
    assert result["results"][0]["error"]["code"] == ErrorCode.ACCOUNT_NO_CREDENTIALS


# ---------- daemon reconcile ----------


async def test_reconcile_emergency_log_into_audit(em_ctx):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    await ctx["runner"].run(account_ids=[account_id], yes=True)

    # daemon açılışı: log'u audit_log'a mutabakat et
    audit = AuditLog(ctx["db"])
    result = await reconcile_emergency_log(ctx["db"], audit, ctx["log"])
    assert result["reconciled"] >= 2  # cancel + sell(ler)
    assert result["log_broken"] == []
    assert await audit.verify() == []

    # idempotent: ikinci reconcile hiçbir şey yazmaz
    again = await reconcile_emergency_log(ctx["db"], audit, ctx["log"])
    assert again["reconciled"] == 0


async def test_reconcile_detects_broken_log(em_ctx):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    await ctx["runner"].run(account_ids=[account_id], yes=True)

    # log'u boz
    lines = ctx["log"].path.read_text(encoding="utf-8").splitlines()
    modified = lines[1].replace('"quantity"', '"quantityX"')
    ctx["log"].path.write_text(lines[0] + "\n" + modified + "\n", encoding="utf-8")

    audit = AuditLog(ctx["db"])
    result = await reconcile_emergency_log(ctx["db"], audit, ctx["log"])
    assert result["log_broken"]
    assert await audit.verify() == []
