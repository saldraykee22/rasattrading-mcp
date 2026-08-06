import time

import pytest

from rasattrading_mcp.accuracy import (
    check_price_fresh,
    check_stop_direction,
    check_sufficient_balance,
    check_symbol_valid,
)
from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.daemon.readiness import Readiness
from rasattrading_mcp.envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.position_sizing import SymbolFilters, calculate_position_size, round_down_to_step
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

from tests.helpers import FakeRest

DEFAULT_FILTERS = SymbolFilters(
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


# ---------- temel doğruluk kontrolleri ----------


def test_check_symbol_valid():
    check_symbol_valid("BTCUSDT", {"BTCUSDT", "ETHUSDT"})
    with pytest.raises(RasatError) as exc_info:
        check_symbol_valid("NOPEUSDT", {"BTCUSDT"})
    assert exc_info.value.code == ErrorCode.INVALID_SYMBOL
    with pytest.raises(RasatError) as exc_info:
        check_symbol_valid("BTCUSDT", None)
    assert exc_info.value.code == ErrorCode.INVALID_SYMBOL


def test_check_price_fresh():
    check_price_fresh(FRESHNESS_FRESH, "BTCUSDT")
    with pytest.raises(RasatError) as exc_info:
        check_price_fresh(FRESHNESS_STALE, "BTCUSDT")
    assert exc_info.value.code == ErrorCode.STALE_DATA
    with pytest.raises(RasatError) as exc_info:
        check_price_fresh(None, "BTCUSDT")
    assert exc_info.value.code == ErrorCode.STALE_DATA


def test_check_stop_direction():
    check_stop_direction("BUY", entry=100, stop_loss=95)
    check_stop_direction("SELL", entry=100, stop_loss=105)
    with pytest.raises(RasatError) as exc_info:
        check_stop_direction("BUY", entry=100, stop_loss=100)  # stop == entry geçersiz
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST
    with pytest.raises(RasatError) as exc_info:
        check_stop_direction("BUY", entry=100, stop_loss=101)
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST
    with pytest.raises(RasatError) as exc_info:
        check_stop_direction("SELL", entry=100, stop_loss=99)
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST


def test_check_sufficient_balance():
    check_sufficient_balance(1000, 999.5, "BTCUSDT")
    check_sufficient_balance(1000, 1000, "BTCUSDT")
    with pytest.raises(RasatError) as exc_info:
        check_sufficient_balance(1000, 1000.01, "BTCUSDT")
    assert exc_info.value.code == ErrorCode.INSUFFICIENT_BALANCE


# ---------- position sizing ----------


def test_round_down_to_step():
    assert round_down_to_step(0.123456789, 0.00001) == 0.12345
    assert round_down_to_step(1.5, 0.5) == 1.5
    assert round_down_to_step(1.2, 0.5) == 1.0


def test_calculate_position_size_basic():
    result = calculate_position_size(
        symbol="BTCUSDT",
        account_balance=10000,
        risk_pct=0.01,  # 100 USDT risk
        entry=100,
        stop_loss=95,  # risk-per-unit 5 -> qty 20
        filters=DEFAULT_FILTERS,
    )
    assert result["risk_amount"] == 100
    assert result["quantity"] == 20
    assert result["notional"] == 2000
    assert result["estimated_fee"] == pytest.approx(2000 * 0.001)
    assert result["total_required"] == pytest.approx(2000 + 2)


def test_calculate_position_size_rounds_down_to_step():
    # risk 100, risk-per-unit 3 -> qty 33.3333 -> 33.33333 (step 0.00001 aşağı yuvarlama)
    result = calculate_position_size(
        symbol="BTCUSDT",
        account_balance=10000,
        risk_pct=0.01,
        entry=100,
        stop_loss=97,  # risk-per-unit 3
        filters=DEFAULT_FILTERS,
    )
    assert result["quantity"] == pytest.approx(33.33333, rel=1e-6)
    # yuvarlama sonrası nihai değer risk cap'ini aşmamalı
    assert result["risk_amount"] >= result["quantity"] * result["risk_per_unit"]


def test_calculate_position_size_respects_balance_cap():
    # bakiye 100; entry 100, stop 95 -> risk-per-unit 5, risk_pct 1.0 -> qty 20
    # ama bakiye ancak ~0.999 USDT karşılar -> balance cap uygulanır
    result = calculate_position_size(
        symbol="BTCUSDT",
        account_balance=100,
        risk_pct=1.0,
        entry=100,
        stop_loss=95,
        filters=DEFAULT_FILTERS,
    )
    assert result["total_required"] <= 100
    assert result["quantity"] < 20


def test_calculate_position_size_balance_cap_keeps_total_affordable():
    # risk-bazlı qty devasa olsa bile fee-aware balance cap total'i bakiye içinde tutar
    result = calculate_position_size(
        symbol="BTCUSDT",
        account_balance=1000,
        risk_pct=1.0,
        entry=100,
        stop_loss=99.9,  # risk-per-unit 0.1 -> risk qty 10000 -> notional 1M
        filters=DEFAULT_FILTERS,
    )
    assert result["total_required"] <= 1000
    assert result["quantity"] < 10


def test_calculate_position_size_min_notional_violation():
    # min_notional 5; düşük fiyatlı sembolde yeterli büyüklük geçer
    filters = SymbolFilters(**{**DEFAULT_FILTERS.__dict__, "min_price": 0.0, "tick_size": 0.000001})
    ok = calculate_position_size(
        symbol="BTCUSDT",
        account_balance=10000,
        risk_pct=0.01,  # 100 USDT risk
        entry=1.0,
        stop_loss=0.5,  # qty 200 -> notional 200
        filters=filters,
    )
    assert ok["notional"] >= 5

    # aynı sembolde küçük risk -> notional 2 < 5 -> FILTER_VIOLATION
    with pytest.raises(RasatError) as exc_info:
        calculate_position_size(
            symbol="BTCUSDT",
            account_balance=1000,
            risk_pct=0.001,  # 1 USDT risk
            entry=1.0,
            stop_loss=0.5,  # qty 2 -> notional 2 < 5
            filters=filters,
        )
    assert exc_info.value.code == ErrorCode.FILTER_VIOLATION


def test_calculate_position_size_stop_direction_rejected():
    with pytest.raises(RasatError) as exc_info:
        calculate_position_size(
            symbol="BTCUSDT",
            account_balance=10000,
            risk_pct=0.01,
            entry=100,
            stop_loss=105,  # long'da stop > entry
            filters=DEFAULT_FILTERS,
        )
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST


def test_calculate_position_size_price_filter_rejected():
    filters = SymbolFilters(**{**DEFAULT_FILTERS.__dict__, "max_price": 150})
    with pytest.raises(RasatError) as exc_info:
        calculate_position_size(
            symbol="BTCUSDT",
            account_balance=10000,
            risk_pct=0.01,
            entry=200,
            stop_loss=190,
            filters=filters,
        )
    assert exc_info.value.code == ErrorCode.FILTER_VIOLATION


# ---------- get_symbol_info + handler dispatch ----------


@pytest.fixture
async def pipeline_ctx(tmp_path):
    db = Database(Config(data_dir=tmp_path, pipeline_enabled=False).db_path)
    await db.start()
    await run_migrations(db)
    fake = FakeRest(["BTCUSDT", "ETHUSDT"])
    from rasattrading_mcp.data.pipeline import DataPipeline
    from rasattrading_mcp.data.universe import UniverseService

    pipeline = DataPipeline(Config(data_dir=tmp_path, pipeline_enabled=False), db, rest=fake, futures_rest=fake)
    pipeline.universe = UniverseService(fake, pipeline.config)
    await pipeline.universe.sync()
    from rasattrading_mcp.data.miniticker import TickerUpdate

    pipeline.ticker_cache.apply_updates(
        [TickerUpdate("BTCUSDT", 100.5, 99, 101, 98, 1000, 100000, 1.5, time.time())]
    )

    readiness = Readiness()
    ctx = {"pipeline": pipeline, "readiness": readiness, "started_at": 0, "db": db}
    dispatcher = build_dispatcher(ctx)
    try:
        yield dispatcher, ctx, pipeline
    finally:
        await db.stop()


async def test_get_symbol_info_dispatches(pipeline_ctx):
    dispatcher, ctx, _ = pipeline_ctx
    data, meta = await dispatcher.dispatch("get_symbol_info", {"symbol": "BTCUSDT"}, ctx)
    assert data["symbol"] == "BTCUSDT"
    assert data["status"] == "TRADING"
    assert data["filters"]["LOT_SIZE"]["step_size"] == 0.00001
    assert data["filters"]["MIN_NOTIONAL"]["min_notional"] == 5.0
    assert data["filters"]["PRICE_FILTER"]["tick_size"] == 0.01
    assert meta.source == "binance-rest-exchangeinfo"


async def test_get_symbol_info_unknown_symbol(pipeline_ctx):
    dispatcher, ctx, _ = pipeline_ctx
    with pytest.raises(RasatError) as exc_info:
        await dispatcher.dispatch("get_symbol_info", {"symbol": "NOPEUSDT"}, ctx)
    assert exc_info.value.code == ErrorCode.INVALID_SYMBOL


async def test_calculate_position_size_dispatches(pipeline_ctx):
    dispatcher, ctx, _ = pipeline_ctx
    data, _ = await dispatcher.dispatch(
        "calculate_position_size",
        {
            "symbol": "BTCUSDT",
            "account_balance": 10000,
            "risk_pct": 0.01,
            "entry": 100,
            "stop_loss": 95,
        },
        ctx,
    )
    assert data["quantity"] == 20
    assert data["notional"] == 2000


async def test_calculate_position_size_rejects_stale_price(pipeline_ctx):
    dispatcher, ctx, pipeline = pipeline_ctx
    # ticker cache boş / stale -> STALE_DATA
    pipeline.ticker_cache.mark_stale("test")
    with pytest.raises(RasatError) as exc_info:
        await dispatcher.dispatch(
            "calculate_position_size",
            {
                "symbol": "BTCUSDT",
                "account_balance": 10000,
                "risk_pct": 0.01,
                "entry": 100,
                "stop_loss": 95,
            },
            ctx,
        )
    assert exc_info.value.code == ErrorCode.STALE_DATA


async def test_calculate_position_size_rejects_unknown_symbol(pipeline_ctx):
    dispatcher, ctx, _ = pipeline_ctx
    with pytest.raises(RasatError) as exc_info:
        await dispatcher.dispatch(
            "calculate_position_size",
            {
                "symbol": "NOPEUSDT",
                "account_balance": 10000,
                "risk_pct": 0.01,
                "entry": 100,
                "stop_loss": 95,
            },
            ctx,
        )
    assert exc_info.value.code == ErrorCode.INVALID_SYMBOL


async def test_calculate_position_size_rejects_wrong_stop_direction(pipeline_ctx):
    dispatcher, ctx, _ = pipeline_ctx
    with pytest.raises(RasatError) as exc_info:
        await dispatcher.dispatch(
            "calculate_position_size",
            {
                "symbol": "BTCUSDT",
                "account_balance": 10000,
                "risk_pct": 0.01,
                "entry": 100,
                "stop_loss": 105,
            },
            ctx,
        )
    assert exc_info.value.code == ErrorCode.INVALID_REQUEST
