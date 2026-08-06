import asyncio

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.emergency_stop import EmergencyStopRunner, PublicPriceSource, run_emergency_stop
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


# ---------- PublicPriceSource (3.7: bağımsız fiyat kaynağı) ----------


class _FakeResp:
    def __init__(self, status=200, payload=None):
        self.status = status
        self._payload = payload
        self.headers = {"x-mbx-used-weight-1m": "1", "Content-Type": "application/json"}

    def raise_for_status(self):
        if self.status >= 400:
            import aiohttp
            from types import SimpleNamespace

            info = SimpleNamespace(real_url="http://fake", url="http://fake")
            raise aiohttp.ClientResponseError(info, None, status=self.status, message=f"fake {self.status}")

    async def json(self):
        return self._payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeSession:
    def __init__(self, handler):
        self._handler = handler
        self.calls = []
        self.closed = False

    def get(self, url, *, params=None, timeout=None):
        params = dict(params or {})
        self.calls.append((url, params))
        return self._handler(url, params)


async def test_public_price_source_makes_http_call():
    seen = []

    def handler(url, params):
        seen.append((url, dict(params)))
        return _FakeResp(200, {"symbol": "BTCUSDT", "price": "123.45"})

    src = PublicPriceSource("https://api.binance.com", session=_FakeSession(handler))
    try:
        price = await src.price("BTCUSDT")
    finally:
        await src.close()
    assert price == 123.45
    assert seen and seen[0][0].endswith("/api/v3/ticker/price")
    assert seen[0][1]["symbol"] == "BTCUSDT"


async def test_public_price_source_network_error_maps_to_rasat():
    def handler(url, params):
        import aiohttp

        raise aiohttp.ClientConnectionError("fiyat servisi çöktü")

    src = PublicPriceSource("https://api.binance.com", session=_FakeSession(handler))
    try:
        with pytest.raises(RasatError) as exc_info:
            await src.price("BTCUSDT")
        assert exc_info.value.code == ErrorCode.INTERNAL_ERROR
    finally:
        await src.close()


async def test_public_price_source_http_error_maps_to_rasat():
    def handler(url, params):
        return _FakeResp(500, {"code": -1121, "msg": "sunucu hatası"})

    src = PublicPriceSource("https://api.binance.com", session=_FakeSession(handler))
    try:
        with pytest.raises(RasatError) as exc_info:
            await src.price("BTCUSDT")
        assert exc_info.value.code == ErrorCode.INTERNAL_ERROR
    finally:
        await src.close()


async def test_public_price_source_unknown_symbol_maps_to_invalid_symbol():
    def handler(url, params):
        return _FakeResp(400, {"code": -1121, "msg": "Invalid symbol"})

    src = PublicPriceSource("https://api.binance.com", session=_FakeSession(handler))
    try:
        with pytest.raises(RasatError) as exc_info:
            await src.price("NOPEUSDT")
        assert exc_info.value.code == ErrorCode.INVALID_SYMBOL
    finally:
        await src.close()


class _FailingPriceSource:
    async def price(self, symbol):
        raise RasatError(ErrorCode.INTERNAL_ERROR, "fiyat servisi çöktü")

    async def filters(self, symbol):
        return None


async def test_emergency_stop_price_failure_fails_loud(em_ctx):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0, "ETH": 2.0})
    runner = EmergencyStopRunner(
        Config(data_dir=ctx["data_dir"], pipeline_enabled=False),
        ctx["accounts"], ctx["broker"], ctx["log"], market_price=_FailingPriceSource(),
    )
    result = await runner.run(account_ids=[account_id], yes=True)
    # fiyat servisi çöktü → ok:True dönülmez (fail-loud, 3.7)
    assert result["ok"] is False
    detail = result["results"][0]
    assert detail["ok"] is False
    assert len(detail["price_errors"]) == 2
    assert detail["sold"] == []
    assert len(ctx["broker"].placed) == 0  # hiçbir şey satılmadı


async def test_emergency_stop_price_failure_empty_plan_not_ok(em_ctx):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={"BTC": 1.0})
    runner = EmergencyStopRunner(
        Config(data_dir=ctx["data_dir"], pipeline_enabled=False),
        ctx["accounts"], ctx["broker"], ctx["log"], market_price=_FailingPriceSource(),
    )
    result = await runner.run(account_ids=[account_id], yes=True)
    # plan boş + price hatası → ok:True dönmez
    assert result["ok"] is False
    assert result["results"][0]["ok"] is False
    assert result["results"][0]["price_errors"]


async def test_emergency_stop_nothing_to_sell_still_ok(em_ctx):
    ctx = em_ctx
    account_id = await _add_real_account(ctx, base_holdings={})  # sadece USDT
    runner = ctx["runner"]
    result = await runner.run(account_ids=[account_id], yes=True)
    # satılacak gerçekten hiçbir şey yok → ok:True (fiyat hatası değil)
    assert result["ok"] is True
    detail = result["results"][0]
    assert detail["ok"] is True
    assert detail["sold"] == []
    assert detail["price_errors"] == []


async def test_run_emergency_stop_uses_public_price_source_by_default(tmp_path, monkeypatch):
    from rasattrading_mcp import emergency_stop as es
    from rasattrading_mcp.storage.accounts import AccountService
    from rasattrading_mcp.storage.db import Database
    from rasattrading_mcp.storage.migrations import run_migrations

    created_flag = {}

    class StubPriceSource:
        def __init__(self, *args, **kwargs):
            created_flag["created"] = True

        async def price(self, symbol):
            return 100.0

        async def filters(self, symbol):
            return None

    monkeypatch.setattr(es, "PublicPriceSource", StubPriceSource)

    cfg = Config(data_dir=tmp_path, pipeline_enabled=False)
    db = Database(cfg.db_path)
    await db.start()
    await run_migrations(db)
    accounts = AccountService(db, secret_store=SecretStore())
    created = await accounts.add_account(label="main", api_key="AK", api_secret="AS")
    await accounts.enable_real_trading(created["account_id"], actor="test")
    await db.stop()

    broker = FakeOrderBroker()
    broker.balances[created["account_id"]] = {"USDT": 1000.0, "BTC": 1.0}
    result = await run_emergency_stop(
        data_dir=tmp_path, account_ids=[created["account_id"]], yes=True, broker=broker,
        config=cfg,
    )
    assert created_flag.get("created") is True
    assert result["ok"] is True
    assert len(result["results"][0]["sold"]) == 1


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
