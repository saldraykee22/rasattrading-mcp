"""Daemon tarafı tool handler'ları (Modül 1 örnekleri; Modül 2/3 ekler)."""

from __future__ import annotations

import os
import time
from typing import Any

from .. import __version__
from ..daemon.readiness import Readiness
from ..envelope import FRESHNESS_FRESH, Meta, SOURCE_DAEMON, utc_iso
from ..errors import RasatError, ErrorCode
from .server import ToolDispatcher


async def ping_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    readiness: Readiness = ctx["readiness"]
    started_at: float = ctx["started_at"]
    data = {
        "pong": True,
        "state": readiness.state,
        "ready": readiness.is_ready(),
        "pid": os.getpid(),
        "version": __version__,
        "uptime_s": round(time.time() - started_at, 1),
    }
    return data, Meta(as_of=utc_iso(), source=SOURCE_DAEMON, freshness=FRESHNESS_FRESH)


async def readiness_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    readiness: Readiness = ctx["readiness"]
    pipeline = ctx.get("pipeline")
    data: dict[str, Any] = {
        "state": readiness.state,
        "history": readiness.history,
        "ready": readiness.is_ready(),
        "pid": os.getpid(),
        "version": __version__,
        "started_at": utc_iso(ctx["started_at"]),
    }
    if pipeline is not None:
        data["pipeline"] = pipeline.status()
    return data, Meta(as_of=utc_iso(), source=SOURCE_DAEMON, freshness=FRESHNESS_FRESH)


def _require_pipeline(ctx: dict):
    pipeline = ctx.get("pipeline")
    if pipeline is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "veri pipeline'ı bu daemon'da kapalı")
    return pipeline


def _require_account_service(ctx: dict):
    """Return the daemon-owned account service, creating a test-context one lazily."""

    service = ctx.get("account_service") or ctx.get("accounts") or ctx.get("account_store")
    if service is not None:
        return service
    db = ctx.get("db")
    if db is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "account servisi bu daemon'da başlatılmamış")
    from ..storage.accounts import AccountService

    service = AccountService(db, audit=ctx.get("audit"))
    ctx["account_service"] = service
    return service


async def candles_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    pipeline = _require_pipeline(ctx)
    symbol = params.get("symbol")
    timeframe = params.get("timeframe")
    limit = params.get("limit", 300)
    source = params.get("source", "spot")
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_REQUEST, "symbol zorunlu (string)")
    if not isinstance(timeframe, str) or not timeframe:
        raise RasatError(ErrorCode.INVALID_REQUEST, "timeframe zorunlu (string)")
    if not isinstance(limit, int):
        raise RasatError(ErrorCode.INVALID_REQUEST, "limit integer olmalı")

    rows = await pipeline.get_candles(symbol, timeframe, limit, source)
    freshness = pipeline.candle_freshness(symbol, timeframe, rows)
    return (
        {"symbol": symbol, "timeframe": timeframe, "source": source, "count": len(rows), "candles": rows},
        Meta(as_of=utc_iso(), source=f"binance-rest-{source}", freshness=freshness),
    )


async def ticker_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    pipeline = _require_pipeline(ctx)
    symbol = params.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_REQUEST, "symbol zorunlu (string)")
    if not await pipeline.ensure_symbol(symbol):
        raise RasatError(ErrorCode.INVALID_SYMBOL, f"evrende bilinmeyen sembol: {symbol}")
    ticker = pipeline.get_ticker(symbol)
    if ticker is None:
        raise RasatError(ErrorCode.STALE_DATA, f"ticker verisi yok: {symbol}")
    return ticker, Meta(as_of=utc_iso(), source="binance-ws-miniticker", freshness=ticker["freshness"])


async def add_account_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_account_service(ctx)
    data = await service.add_account(
        label=params.get("label"),
        api_key=params.get("api_key"),
        api_secret=params.get("api_secret"),
        tags=params.get("tags"),
        market=params.get("market", "spot"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-accounts", freshness=FRESHNESS_FRESH)


async def list_accounts_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_account_service(ctx)
    data = await service.list_accounts()
    return data, Meta(as_of=utc_iso(), source="sqlite-accounts", freshness=FRESHNESS_FRESH)


async def remove_account_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_account_service(ctx)
    data = await service.remove_account(
        params.get("account_id"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-accounts", freshness=FRESHNESS_FRESH)


def _require_risk_service(ctx: dict):
    """Return the daemon-owned risk policy service, creating a test-context one lazily."""

    service = ctx.get("risk_service") or ctx.get("risk_policy_service") or ctx.get("risk")
    if service is not None:
        return service
    db = ctx.get("db")
    if db is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "risk politikası servisi bu daemon'da başlatılmamış")
    from ..storage.risk_policy import RiskPolicyService

    service = RiskPolicyService(db, audit=ctx.get("audit"))
    ctx["risk_service"] = service
    return service


async def enable_real_trading_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_account_service(ctx)
    data = await service.enable_real_trading(
        params.get("account_id"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-accounts", freshness=FRESHNESS_FRESH)


async def set_risk_policy_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_risk_service(ctx)
    data = await service.set_risk_policy(
        params.get("account_id"),
        max_notional_per_order=params.get("max_notional_per_order"),
        max_aggregate_exposure=params.get("max_aggregate_exposure"),
        allowed_symbols=params.get("allowed_symbols"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-risk-policy", freshness=FRESHNESS_FRESH)


async def override_risk_policy_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_risk_service(ctx)
    data = await service.create_override(
        params.get("account_id"),
        reason=params.get("reason"),
        scope=params.get("scope", "next_order"),
        idempotency_key=params.get("idempotency_key"),
        actor=str(ctx.get("actor", "mcp-agent")),
        expires_at=params.get("expires_at"),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-risk-policy", freshness=FRESHNESS_FRESH)


async def get_risk_policy_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_risk_service(ctx)
    data = await service.get_policy(params.get("account_id"))
    return data, Meta(as_of=utc_iso(), source="sqlite-risk-policy", freshness=FRESHNESS_FRESH)


def _require_symbol_filters(ctx: dict, symbol: str) -> dict:
    """Return exchangeInfo filters for a symbol or raise INVALID_SYMBOL."""
    pipeline = _require_pipeline(ctx)
    info = pipeline.symbol_info(symbol)
    if info is None:
        raise RasatError(ErrorCode.INVALID_SYMBOL, f"evrende bilinmeyen sembol: {symbol}")
    return info


async def get_symbol_info_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    pipeline = _require_pipeline(ctx)
    symbol = params.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_REQUEST, "symbol zorunlu (string)")
    info = _require_symbol_filters(ctx, symbol)
    from ..position_sizing import SymbolFilters

    filters = SymbolFilters.from_exchange_info(info)
    data = filters.to_dict()
    data["status"] = filters.status
    freshness = FRESHNESS_FRESH if pipeline.universe.status == "ok" else FRESHNESS_STALE
    return data, Meta(as_of=utc_iso(), source="binance-rest-exchangeinfo", freshness=freshness)


async def calculate_position_size_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    pipeline = _require_pipeline(ctx)
    symbol = params.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_REQUEST, "symbol zorunlu (string)")
    account_balance = params.get("account_balance")
    risk_pct = params.get("risk_pct")
    entry = params.get("entry")
    stop_loss = params.get("stop_loss")
    side = params.get("side", "BUY")
    fee_rate = params.get("fee_rate", 0.001)
    for name, value in (("account_balance", account_balance), ("risk_pct", risk_pct), ("entry", entry), ("stop_loss", stop_loss)):
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{name} sayı olmalı")

    # Temel doğruluk kontrolleri (her zaman aktif, kapatılamaz):
    # 1) sembol geçerlilik / TRADING durumu
    from ..accuracy import check_price_fresh, check_symbol_valid

    check_symbol_valid(symbol, set(pipeline.universe_snapshot()))
    # 2) fiyat staleness — daemon'ın kendi taze ticker'ına güvenilir
    ticker = pipeline.get_ticker(symbol)
    freshness = ticker["freshness"] if ticker else FRESHNESS_STALE
    check_price_fresh(freshness, symbol)

    info = _require_symbol_filters(ctx, symbol)
    from ..position_sizing import SymbolFilters, calculate_position_size

    result = calculate_position_size(
        symbol=symbol,
        account_balance=float(account_balance),
        risk_pct=float(risk_pct),
        entry=float(entry),
        stop_loss=float(stop_loss),
        filters=SymbolFilters.from_exchange_info(info),
        side=side,
        fee_rate=float(fee_rate),
    )
    return result, Meta(as_of=utc_iso(), source="daemon-position-sizing", freshness=FRESHNESS_FRESH)


def _require_order_service(ctx: dict):
    """Return the daemon-owned order service, creating a test-context one lazily."""

    service = ctx.get("order_service") or ctx.get("orders")
    if service is not None:
        return service
    db = ctx.get("db")
    if db is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "order servisi bu daemon'da başlatılmamış")
    from ..storage.orders import OrderService

    accounts = _require_account_service(ctx)
    risk = _require_risk_service(ctx)
    pipeline = ctx.get("pipeline")
    broker = ctx.get("order_broker")
    if pipeline is None or broker is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "order servisi için pipeline ve broker gereklidir")
    from ..storage.orders import PipelineMarketFeed

    service = OrderService(
        db,
        accounts=accounts,
        risk=risk,
        broker=broker,
        market=PipelineMarketFeed(pipeline),
        audit=ctx.get("audit"),
    )
    ctx["order_service"] = service
    return service


async def execute_on_accounts_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_order_service(ctx)
    data = await service.execute_on_accounts(
        account_ids=params.get("account_ids"),
        tags=params.get("tags"),
        symbol=params.get("symbol"),
        side=params.get("side", "BUY"),
        entry=params.get("entry"),
        stop_loss=params.get("stop_loss"),
        risk_pct=params.get("risk_pct"),
        idempotency_key=params.get("idempotency_key"),
        order_type=params.get("order_type", "MARKET"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-orders", freshness=FRESHNESS_FRESH)


async def place_order_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_order_service(ctx)
    data = await service.place_order(
        account_id=params.get("account_id"),
        symbol=params.get("symbol"),
        side=params.get("side"),
        order_type=params.get("order_type", "MARKET"),
        quantity=params.get("quantity"),
        price=params.get("price"),
        idempotency_key=params.get("idempotency_key"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-orders", freshness=FRESHNESS_FRESH)


def build_dispatcher(ctx: dict) -> ToolDispatcher:
    from ..tools import REGISTRY

    dispatcher = ToolDispatcher(REGISTRY)
    dispatcher.register("ping", ping_handler)
    dispatcher.register("get_readiness", readiness_handler)
    if ctx.get("pipeline") is not None:
        dispatcher.register("get_candles", candles_handler)
        dispatcher.register("get_ticker", ticker_handler)
        dispatcher.register("get_symbol_info", get_symbol_info_handler)
        dispatcher.register("calculate_position_size", calculate_position_size_handler)
        dispatcher.register("execute_on_accounts", execute_on_accounts_handler)
        dispatcher.register("place_order", place_order_handler)
    dispatcher.register("add_account", add_account_handler)
    dispatcher.register("list_accounts", list_accounts_handler)
    dispatcher.register("remove_account", remove_account_handler)
    dispatcher.register("enable_real_trading", enable_real_trading_handler)
    dispatcher.register("set_risk_policy", set_risk_policy_handler)
    dispatcher.register("override_risk_policy", override_risk_policy_handler)
    dispatcher.register("get_risk_policy", get_risk_policy_handler)
    return dispatcher
