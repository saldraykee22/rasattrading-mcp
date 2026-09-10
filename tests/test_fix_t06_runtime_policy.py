"""T06 runtime, risk-policy patch semantics and residual close hardening."""

import asyncio

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon import main as daemon_main
from rasattrading_mcp.daemon.main import DaemonRunner
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

    async def symbol_valid(self, symbol: str) -> bool:
        return symbol in self.price_map

    async def price(self, symbol: str) -> float | None:
        return self.price_map.get(symbol)

    async def filters(self, symbol: str) -> SymbolFilters | None:
        if symbol not in self.price_map:
            return None
        return SymbolFilters(**{**FILTERS.__dict__, "symbol": symbol})


@pytest.fixture
async def policy_services(tmp_path):
    db = Database(Config(data_dir=tmp_path, pipeline_enabled=False).db_path)
    await db.start()
    await run_migrations(db)
    accounts = AccountService(db, secret_store=SecretStore(), audit=AuditLog(db))
    risk = RiskPolicyService(db, audit=AuditLog(db))
    yield db, accounts, risk
    await db.stop()


@pytest.fixture
async def t06_ctx(tmp_path):
    db = Database(Config(data_dir=tmp_path, pipeline_enabled=False).db_path)
    await db.start()
    await run_migrations(db)
    accounts = AccountService(db, secret_store=SecretStore(), audit=AuditLog(db))
    risk = RiskPolicyService(db, audit=AuditLog(db))
    broker = FakeOrderBroker()
    service = OrderService(
        db,
        accounts=accounts,
        risk=risk,
        broker=broker,
        market=FakeMarket(),
        audit=AuditLog(db),
    )
    yield {"db": db, "accounts": accounts, "risk": risk, "broker": broker, "service": service}
    await db.stop()


async def _add_account(accounts: AccountService, *, label: str = "main", paper: bool = False) -> str:
    if paper:
        created = await accounts.add_account(label=label)
    else:
        created = await accounts.add_account(label=label, api_key=f"AK_{label}", api_secret=f"AS_{label}")
        await accounts.enable_real_trading(created["account_id"], actor="test")
    return created["account_id"]


async def test_risk_policy_clear_patch_preserves_noop_and_reconciles(policy_services):
    db, accounts, risk = policy_services
    account_id = await _add_account(accounts)

    first = await risk.set_risk_policy(
        account_id,
        max_notional_per_order=1000,
        max_aggregate_exposure=5000,
        allowed_symbols=["btcusdt", "ethusdt"],
    )
    assert first["policy_version"] == 1
    override = await risk.create_override(account_id, reason="before clear")
    assert override["policy_version"] == 1

    noop = await risk.set_risk_policy(account_id)
    assert noop["changed"] is False
    assert noop["policy_version"] == 1
    assert noop["max_notional_per_order"] == 1000
    assert noop["max_aggregate_exposure"] == 5000
    assert noop["allowed_symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert (await risk.get_active_override(account_id, policy_version=1)) is not None

    cleared = await risk.set_risk_policy(
        account_id,
        clear_max_notional=True,
        clear_max_exposure=True,
        clear_allowed_symbols=True,
    )
    assert cleared["changed"] is True
    assert cleared["policy_version"] == 2
    assert cleared["max_notional_per_order"] is None
    assert cleared["max_aggregate_exposure"] is None
    assert cleared["allowed_symbols"] == []
    assert cleared["updated_at"] == (await risk.get_policy(account_id))["updated_at"]
    assert await risk.get_active_override(account_id, policy_version=2) is None

    def _override_state(conn):
        return conn.execute(
            "SELECT state FROM risk_override WHERE override_id = ?", (override["override_id"],)
        ).fetchone()["state"]

    assert await db.read(_override_state) == "reconciled"
    assert await AuditLog(db).verify() == []


async def test_risk_policy_clear_validation_and_handler(policy_services):
    _, accounts, risk = policy_services
    account_id = await _add_account(accounts)

    with pytest.raises(RasatError) as exc:
        await risk.set_risk_policy(account_id, clear_max_notional="yes")
    assert exc.value.code == ErrorCode.INVALID_REQUEST
    with pytest.raises(RasatError) as exc:
        await risk.set_risk_policy(account_id, max_notional_per_order=float("nan"))
    assert exc.value.code == ErrorCode.INVALID_REQUEST

    from rasattrading_mcp.daemon.handlers import build_dispatcher
    from rasattrading_mcp.daemon.readiness import Readiness

    readiness = Readiness()
    ctx = {"account_service": accounts, "risk_service": risk, "readiness": readiness, "started_at": 0}
    dispatcher = build_dispatcher(ctx)
    data, _ = await dispatcher.dispatch(
        "set_risk_policy",
        {"account_id": account_id, "max_notional_per_order": 250, "clear_allowed_symbols": True},
        ctx,
    )
    assert data["max_notional_per_order"] == 250
    assert data["allowed_symbols"] == []


async def test_paper_close_is_explicitly_simulated_and_audited(t06_ctx):
    account_id = await _add_account(t06_ctx["accounts"], label="paper", paper=True)
    result = await t06_ctx["service"].close_all_positions(account_id=account_id, actor="test")
    detail = result["results"][0]
    assert detail["closed"] is False
    assert detail["mode"] == "paper"
    assert detail["simulated"] is True
    assert detail["position_close_supported"] is False
    assert detail["cancel_errors"] == []
    assert detail["sold"] == []


async def test_exposure_unknown_asset_price_is_incomplete_not_silently_zero(t06_ctx):
    account_id = await _add_account(t06_ctx["accounts"], label="unknown-asset")
    t06_ctx["broker"].balances[account_id] = {"USDT": 1000.0, "DOGE": 10.0}

    exposure = await t06_ctx["service"].get_total_exposure()
    assert exposure["complete"] is False
    assert exposure["per_account"] == {}
    assert exposure["errors"][0]["account_id"] == account_id
    assert exposure["errors"][0]["error"]["code"] == ErrorCode.STALE_DATA


class _Recorder:
    def __init__(self, events: list[str], name: str, *, error: Exception | None = None) -> None:
        self.events = events
        self.name = name
        self.error = error
        self.calls = 0

    async def stop(self) -> None:
        self.calls += 1
        self.events.append(self.name)
        if self.error is not None:
            raise self.error

    async def cleanup(self) -> None:
        await self.stop()

    async def close(self) -> None:
        await self.stop()


async def test_daemon_stop_closes_broker_before_pipeline_and_db(tmp_path):
    runner = DaemonRunner(Config(data_dir=tmp_path, pipeline_enabled=False))
    events: list[str] = []
    runner.http_site = _Recorder(events, "http-site")
    runner.http_runner = _Recorder(events, "http-runner")
    runner.order_broker = _Recorder(events, "broker")
    runner.pipeline = _Recorder(events, "pipeline")
    runner.db = _Recorder(events, "db")

    await runner.stop()
    await runner.stop()

    assert events == ["http-site", "http-runner", "broker", "pipeline", "db"]
    assert runner._stop.is_set()
    assert runner._shutdown_complete is True


async def test_daemon_run_stops_in_finally_on_cancellation(tmp_path):
    """stop() runs in finally even when run() is canceled (CancelledError/Windows Ctrl+C).

    T3 regression: when stop() was outside try/finally, CancelledError propagated
    before reaching this line, leaving the daemon hanging with DB writer executor and lock open.
    """
    runner = DaemonRunner(Config(data_dir=tmp_path, pipeline_enabled=False))
    events: list[str] = []
    runner.order_broker = _Recorder(events, "broker")
    runner.db = _Recorder(events, "db")

    task = asyncio.create_task(runner.run())
    await asyncio.sleep(0.05)  # run() now waits at _stop.wait().
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert "broker" in events
    assert "db" in events
    assert runner._shutdown_complete is True
    assert runner._stop.is_set()


async def test_daemon_run_returns_zero_on_normal_stop(tmp_path):
    """On normal finish (request_stop), run() returns 0 and stop() still runs."""
    runner = DaemonRunner(Config(data_dir=tmp_path, pipeline_enabled=False))
    events: list[str] = []
    runner.order_broker = _Recorder(events, "broker")
    runner.db = _Recorder(events, "db")

    run_task = asyncio.create_task(runner.run())
    await asyncio.sleep(0.05)
    runner.request_stop()
    result = await run_task

    assert result == 0
    assert "broker" in events
    assert "db" in events
    assert runner._shutdown_complete is True


async def test_daemon_stop_continues_after_partial_component_failure(tmp_path):
    runner = DaemonRunner(Config(data_dir=tmp_path, pipeline_enabled=False))
    events: list[str] = []
    runner.order_broker = _Recorder(events, "broker")
    runner.pipeline = _Recorder(events, "pipeline", error=RuntimeError("pipeline close detail"))
    runner.db = _Recorder(events, "db")

    await runner.stop()

    assert events == ["broker", "pipeline", "db"]
    assert runner._shutdown_complete is True


async def test_daemon_start_failure_releases_partial_resources(tmp_path, monkeypatch):
    monkeypatch.setattr(daemon_main, "setup_logging", lambda *args, **kwargs: None)
    runner = DaemonRunner(Config(data_dir=tmp_path, pipeline_enabled=False))
    db = _Recorder([], "db")

    async def broken_startup() -> None:
        runner.db = db
        raise RuntimeError("startup failed")

    runner._startup_sequence = broken_startup
    with pytest.raises(RuntimeError, match="startup failed"):
        await runner.start()

    assert db.calls == 1
    assert not runner.config.lock_path.exists()
    assert runner._shutdown_complete is True


async def test_ownership_watch_requests_shutdown_and_broker_cleanup(tmp_path, monkeypatch):
    runner = DaemonRunner(Config(data_dir=tmp_path, pipeline_enabled=False))
    runner.lock_info = daemon_main.LockInfo.create(runner.config.port)
    replacement = daemon_main.LockInfo.create(runner.config.port)
    events: list[str] = []
    runner.order_broker = _Recorder(events, "broker")

    async def fast_sleep(_: float) -> None:
        return None

    monkeypatch.setattr(daemon_main.asyncio, "sleep", fast_sleep)
    monkeypatch.setattr(daemon_main, "read_lock", lambda _: replacement)

    await runner._ownership_watch()
    assert runner._stop.is_set()
    await runner.stop()
    assert events == ["broker"]
