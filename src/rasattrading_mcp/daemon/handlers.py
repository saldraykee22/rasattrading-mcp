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


def build_dispatcher(ctx: dict) -> ToolDispatcher:
    from ..tools import REGISTRY

    dispatcher = ToolDispatcher(REGISTRY)
    dispatcher.register("ping", ping_handler)
    dispatcher.register("get_readiness", readiness_handler)
    if ctx.get("pipeline") is not None:
        dispatcher.register("get_candles", candles_handler)
        dispatcher.register("get_ticker", ticker_handler)
    dispatcher.register("add_account", add_account_handler)
    dispatcher.register("list_accounts", list_accounts_handler)
    dispatcher.register("remove_account", remove_account_handler)
    return dispatcher
