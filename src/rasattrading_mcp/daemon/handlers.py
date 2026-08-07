"""Daemon tarafı tool handler'ları (Modül 1 örnekleri; Modül 2/3 ekler)."""

from __future__ import annotations

import os
import time
from typing import Any

from .. import __version__
from ..daemon.readiness import Readiness
from ..envelope import FRESHNESS_FRESH, FRESHNESS_STALE, Meta, SOURCE_DAEMON, utc_iso
from ..errors import RasatError, ErrorCode
from ..storage.orders import STATUS_PAPER
from ..storage.state import (
    PENDING_AWAITING_APPROVAL,
    PENDING_RECONCILE_REQUIRED,
    PENDING_REJECTED,
)
from .server import ToolDispatcher


# T02: execution sonucu status'u → pending terminal eşlemesi.
# Yalnızca kesin borsa dolumu (FILLED) veya yerel paper simülasyonu kesin başarıdır;
# NEW/PARTIALLY_FILLED gibi non-terminal ve bilinmeyen durumlar reconcile_required
# taşır; REJECTED/CANCELED/EXPIRED deterministik rejected'dir. executed_order_id
# yalnızca kesin başarıda doldurulur.
_PENDING_DEFINITE_SUCCESS = frozenset({"FILLED", STATUS_PAPER})
_PENDING_DEFINITE_REJECTED = frozenset({"REJECTED", "CANCELED", "EXPIRED"})

# T3: geçici/retry-edilebilir hatalar → reconcile_required (kullanıcı ya da daemon
# tekrar deneyebilir). Kalıcı deterministik hatalar (INSUFFICIENT_BALANCE,
# INVALID_REQUEST, vb.) rejected'da kalır. STALE_DATA/RATE_LIMITED/TIMEOUT
# eskiden kalıcı rejected üretiyordu — retry imkânı kayboluyordu.
_PENDING_TRANSIENT_ERRORS = frozenset(
    {
        ErrorCode.TIMEOUT,
        ErrorCode.STALE_DATA,
        ErrorCode.RATE_LIMITED,
        ErrorCode.ORDER_UNKNOWN,
        ErrorCode.ORDER_RECONCILE_REQUIRED,
    }
)


async def _current_market_price(ctx: dict, symbol: str) -> float | None:
    """Approve onayında market emir entry'sini dolduracak güncel piyasa fiyatı.

    Daemon bağlamında `pipeline`'ın taze ticker'ı, test bağlamında `market`
    feed'i kullanılır. Fiyat yoksa veya stale ise None döner (fail-closed).
    """
    pipeline = ctx.get("pipeline")
    if pipeline is not None:
        ticker = pipeline.get_ticker(symbol)
        if ticker and ticker.get("freshness") == FRESHNESS_FRESH:
            try:
                return float(ticker["last"])
            except (TypeError, ValueError):
                return None
        return None
    market = ctx.get("market")
    if market is not None:
        price = await market.price(symbol)
        return float(price) if price is not None else None
    return None


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
        raise RasatError(
            ErrorCode.PIPELINE_UNAVAILABLE,
            "veri pipeline'ı bu daemon'da kapalı — bu tool canlı veri gerektiriyor",
        )
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


# ---------- Modül 2: PA tool handler'ları ----------


def _require_pa_engine(ctx: dict):
    engine = ctx.get("pa_engine")
    if engine is not None:
        return engine
    from ..pa.analysis import PAEngine

    db = ctx.get("db")
    if db is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "PA motoru bu daemon'da başlatılmamış")
    engine = PAEngine(db, pipeline=ctx.get("pipeline"))
    ctx["pa_engine"] = engine
    return engine


def _pa_meta(timeframe: str, as_of: int | None, algo_version: str | None) -> Meta:
    from ..pa.analysis import PAEngine

    return Meta(
        as_of=utc_iso(as_of) if as_of else utc_iso(),
        source="pa-engine",
        freshness=PAEngine.freshness_for(timeframe, as_of),
        algo_version=algo_version,
        extra={
            "freshness_note": (
                "fresh = analiz, timeframe'in son kapanmış mumunu içerir (as_of, son "
                "kapanmış mumun open_time'ıdır); ticker gibi gerçek zamanlı değil"
            )
        },
    )


async def get_market_structure_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    engine = _require_pa_engine(ctx)
    symbol = params["symbol"]
    timeframe = params["timeframe"]
    lookback = params.get("lookback", 200)
    data = await engine.get_market_structure(symbol, timeframe, lookback)
    return data, _pa_meta(timeframe, data["as_of"], data["algo_version"])


async def get_liquidity_zones_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    engine = _require_pa_engine(ctx)
    data = await engine.get_liquidity_zones(
        params["symbol"], params["timeframe"], params.get("include_mitigated", False), params.get("lookback", 200)
    )
    return data, _pa_meta(params["timeframe"], data["as_of"], data["algo_version"])


async def get_order_blocks_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    engine = _require_pa_engine(ctx)
    data = await engine.get_order_blocks(
        params["symbol"], params["timeframe"], params.get("include_mitigated", False), params.get("lookback", 200)
    )
    return data, _pa_meta(params["timeframe"], data["as_of"], data["algo_version"])


async def get_full_analysis_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    engine = _require_pa_engine(ctx)
    data = await engine.get_full_analysis(
        params["symbol"], params["timeframe"], params.get("include_mitigated", False), params.get("lookback", 200)
    )
    return data, _pa_meta(params["timeframe"], data["as_of"], data["algo_version"])


def _require_annotations(ctx: dict):
    svc = ctx.get("annotations")
    if svc is not None:
        return svc
    from ..pa.annotations import AnnotationService

    db = ctx.get("db")
    if db is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "annotation servisi bu daemon'da başlatılmamış")
    svc = AnnotationService(db)
    ctx["annotations"] = svc
    return svc


async def annotate_chart_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    svc = _require_annotations(ctx)
    ids = await svc.annotate(
        params["symbol"],
        params["timeframe"],
        params["annotations"],
        created_by=str(params.get("created_by", "agent")),
    )
    return {"symbol": params["symbol"], "timeframe": params["timeframe"], "annotation_ids": ids}, Meta(
        as_of=utc_iso(), source="sqlite-annotations", freshness=FRESHNESS_FRESH
    )


async def get_chart_annotations_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    svc = _require_annotations(ctx)
    items = await svc.get(params["symbol"], params["timeframe"])
    return {"symbol": params["symbol"], "timeframe": params["timeframe"], "annotations": items}, Meta(
        as_of=utc_iso(), source="sqlite-annotations", freshness=FRESHNESS_FRESH
    )


async def clear_annotations_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    svc = _require_annotations(ctx)
    removed = await svc.clear(params["symbol"], params["timeframe"])
    return {"symbol": params["symbol"], "timeframe": params["timeframe"], "removed": removed}, Meta(
        as_of=utc_iso(), source="sqlite-annotations", freshness=FRESHNESS_FRESH
    )


async def scan_market_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    from ..pa.screener import Screener

    screener = ctx.get("screener")
    if screener is None:
        db = ctx.get("db")
        if db is None:
            raise RasatError(ErrorCode.NOT_IMPLEMENTED, "screener bu daemon'da başlatılmamış")
        screener = Screener(db, engine=ctx.get("pa_engine"), pipeline=ctx.get("pipeline"))
        ctx["screener"] = screener
    data = await screener.scan(
        params.get("filters"),
        combine=params.get("combine", "AND"),
        sort_by=params.get("sort_by", "symbol"),
        limit=params.get("limit", 50),
        cursor=params.get("cursor"),
        timeframe=params.get("timeframe", "1h"),
    )
    return data, Meta(as_of=utc_iso(), source="pa-screener", freshness=data["freshness"])


# ---------- Modül 2 / 2.6: alarm handler'ları ----------


def _require_alarm_service(ctx: dict):
    alarm = ctx.get("alarm_service")
    if alarm is not None:
        return alarm
    from ..pa.alarms import AlarmService

    engine = _require_pa_engine(ctx)
    alarm = AlarmService(engine.db, engine=engine)
    engine.alarm_service = alarm  # PAEngine.analyze → on_analysis_updated hook'u
    ctx["alarm_service"] = alarm
    return alarm


async def create_alert_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    alarm = _require_alarm_service(ctx)
    data = await alarm.create_alert(
        params["symbol"],
        params["timeframe"],
        params["condition"],
        cooldown_seconds=params.get("cooldown_seconds", 300),
        note=params.get("note"),
        order_spec=params.get("order_spec"),
    )
    return data, Meta(as_of=utc_iso(), source="alarm-engine", freshness=FRESHNESS_FRESH)


async def get_pending_orders_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    alarm = _require_alarm_service(ctx)
    data = await alarm.get_pending_orders(
        status=params.get("status"),
        limit=params.get("limit", 50),
    )
    return data, Meta(as_of=utc_iso(), source="alarm-engine", freshness=FRESHNESS_FRESH)


async def approve_pending_order_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    """Onaylı bekleyen emri GERÇEK emir olarak açıp T00 state machine'iyle kapatır.

    Emir boyutlandırma daemon tarafında yapılır: `execute_on_accounts`
    risk_pct + hesap equity'si + sembol filtreleriyle; spec'teki miktar
    kullanılmaz (agent'a güvenilmez — plan 5.2). Idempotency key:
    `pending:<order_id>` — retry çift emir üretmez.

    T02 state akışı (CAS, T00 sözleşmesi):
    - `awaiting_approval → approved → executing` tek transaction'da claim edilir
      (iki eşzamanlı approve yalnızca bir execution claim üretir).
    - `execute_on_accounts` structured REJECTED/UNKNOWN sonucu başarı sayılmaz;
      exception sonrası kayıt `approved` kilidinde kalmaz.
    - Kesin başarı yalnızca `status == FILLED` (borsa dolumu) veya paper
      simülasyonudur → `executed` (`executed_order_id` kesin emir kimliğiyle
      doldurulur). REJECTED/CANCELED/EXPIRED → deterministic `rejected`;
      NEW/PARTIALLY_FILLED/UNKNOWN ve bilinmeyen status → `reconcile_required`
      (non-terminal/ambiguous; executed_order_id boş kalır).
    """
    alarm = _require_alarm_service(ctx)
    order_id = params["order_id"]
    rec = await alarm._pending_order(order_id)
    if rec["status"] != PENDING_AWAITING_APPROVAL:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"onay bekleyen durumda değil: {rec['status']}")

    # T3: market emirde entry eksikse daemon'ın kendi taze piyasa fiyatıyla doldur
    # (storage tarafında entry zorunlu — eksik değer kalıcı rejected üretiyordu).
    # Fiyat alınamıyorsa fail-closed: kayıt awaiting_approval'da kalır, retry edilebilir.
    order_type = rec.get("order_type") or "market"
    entry = rec.get("entry")
    stop_loss = rec.get("stop_loss")
    if str(order_type).lower() == "market" and entry is None:
        market_price = await _current_market_price(ctx, rec["symbol"])
        if market_price is None:
            raise RasatError(
                ErrorCode.STALE_DATA,
                f"market emir için güncel piyasa fiyatı alınamadı: {rec['symbol']}",
            )
        entry = market_price
        await alarm._fill_pending_entry(order_id, entry)

    # T02 preflight: risk_pct/required execution alanları broker'dan ÖNCE yeniden doğrulanır.
    alarm._validate_execution_inputs(
        risk_pct=rec.get("risk_pct"),
        entry=entry,
        stop_loss=stop_loss,
        order_type=rec.get("order_type"),
    )
    # T3: stop_loss tüm order tiplerinde zorunludur (storage tarafında zorunlu tutulur).
    # Eksikse execute_on_accounts'a gitmeden INVALID_REQUEST döner — kayıt kalıcı
    # rejected'a düşmez, kullanıcı düzeltip yeniden onaylayabilir.
    if stop_loss is None:
        raise RasatError(
            ErrorCode.INVALID_REQUEST,
            "stop_loss zorunlu (onaylı emir için koruma seviyesi eksik)",
        )
    order_service = ctx.get("order_service")
    if order_service is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "order service bu daemon'da başlatılmamış")

    await alarm.approve_and_claim_pending_execution(order_id)

    try:
        executed = await order_service.execute_on_accounts(
            account_ids=[rec["account_id"]],
            tags=None,
            symbol=rec["symbol"],
            side=rec["side"],
            entry=entry,
            stop_loss=stop_loss,
            risk_pct=rec["risk_pct"],
            idempotency_key=f"pending:{order_id}",
            order_type=rec["order_type"] or "MARKET",
            actor=str(ctx.get("actor", "mcp-agent")),
        )
    except RasatError as exc:
        # Deterministik rejection → rejected; geçici/retry-edilebilir → reconcile_required.
        terminal = (
            PENDING_RECONCILE_REQUIRED
            if exc.code in _PENDING_TRANSIENT_ERRORS
            else PENDING_REJECTED
        )
        await alarm.fail_pending_execution(
            order_id, status=terminal, error_code=exc.code, error_message=exc.message
        )
        raise
    except Exception as exc:  # noqa: BLE001
        await alarm.fail_pending_execution(
            order_id,
            status=PENDING_RECONCILE_REQUIRED,
            error_code=ErrorCode.INTERNAL_ERROR,
            error_message="beklenmeyen hata; emir durumu doğrulanamadı",
        )
        raise

    results = executed.get("results") if isinstance(executed, dict) else None
    result = results[0] if isinstance(results, list) and results else None
    if not isinstance(result, dict):
        # Boş/ambiguous sonuç — executed_order_id'yi doldurmadan reconcile bırak.
        await alarm.fail_pending_execution(
            order_id,
            status=PENDING_RECONCILE_REQUIRED,
            error_code=ErrorCode.ORDER_UNKNOWN,
            error_message="emir sonucu alınamadı; reconcile gerekli",
        )
        raise RasatError(ErrorCode.ORDER_RECONCILE_REQUIRED, "emir sonucu belirsiz; reconcile gerekli")

    status = result.get("status")
    error = result.get("error") or {}
    if status in _PENDING_DEFINITE_SUCCESS:
        # Kesin başarı: yalnızca FILLED (borsa dolumu) veya paper simülasyonu.
        executed_order_id = result.get("exchange_order_id") or result.get("order_id")
        if not executed_order_id:
            await alarm.fail_pending_execution(
                order_id,
                status=PENDING_RECONCILE_REQUIRED,
                error_code=ErrorCode.ORDER_UNKNOWN,
                error_message="kesin emir kimliği alınamadı; reconcile gerekli",
            )
            raise RasatError(ErrorCode.ORDER_RECONCILE_REQUIRED, "kesin emir kimliği alınamadı")
        await alarm.complete_pending_execution(order_id, executed_order_id)
        return {
            "order_id": order_id,
            "status": "executed",
            "executed_order_id": executed_order_id,
            "executed": executed,
        }, Meta(as_of=utc_iso(), source="order-service", freshness=FRESHNESS_FRESH)

    if status in _PENDING_DEFINITE_REJECTED:
        # Deterministik red: borsa emri kesin reddedildi/iptal/süresi doldu.
        # T3: structured REJECTED içindeki geçici hata kodları (STALE_DATA,
        # RATE_LIMITED vb.) kalıcı rejected DEĞİLDİR — retry imkânı kalsın diye
        # reconcile_required'a çekilir. Yalnızca gerçekten kalıcı kodlar (örn.
        # borsa ORDER_REJECTED / INSUFFICIENT_BALANCE) rejected'da kalır.
        code = error.get("code") or {
            "REJECTED": ErrorCode.ORDER_REJECTED,
            "EXPIRED": ErrorCode.ORDER_EXPIRED,
            "CANCELED": ErrorCode.ORDER_REJECTED,
        }.get(status, ErrorCode.ORDER_REJECTED)
        message = error.get("message") or f"emir kesin sonlandı (borsa durumu: {status})"
        terminal = PENDING_RECONCILE_REQUIRED if code in _PENDING_TRANSIENT_ERRORS else PENDING_REJECTED
        await alarm.fail_pending_execution(
            order_id, status=terminal, error_code=code, error_message=message
        )
        raise RasatError(code, message)

    # Diğer tüm durumlar (UNKNOWN, NEW, PARTIALLY_FILLED ve bilinmeyen status):
    # non-terminal/ambiguous → reconcile_required; executed_order_id doldurulmaz.
    code = error.get("code") or ErrorCode.ORDER_UNKNOWN
    message = error.get("message") or f"emir durumu kesin değil (borsa durumu: {status}); reconcile gerekli"
    await alarm.fail_pending_execution(
        order_id, status=PENDING_RECONCILE_REQUIRED, error_code=code, error_message=message
    )
    raise RasatError(ErrorCode.ORDER_RECONCILE_REQUIRED, "emir kesin dolmadı; reconcile gerekli")


async def reject_pending_order_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    alarm = _require_alarm_service(ctx)
    data = await alarm.reject_pending_order(params["order_id"], reason=params.get("reason"))
    return data, Meta(as_of=utc_iso(), source="alarm-engine", freshness=FRESHNESS_FRESH)


async def create_composite_alert_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    alarm = _require_alarm_service(ctx)
    data = await alarm.create_composite_alert(
        params["clauses"],
        combine=params.get("combine", "AND"),
        cooldown_seconds=params.get("cooldown_seconds", 300),
        note=params.get("note"),
    )
    return data, Meta(as_of=utc_iso(), source="alarm-engine", freshness=FRESHNESS_FRESH)


async def list_alerts_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    alarm = _require_alarm_service(ctx)
    data = await alarm.list_alerts()
    return {"alerts": data}, Meta(as_of=utc_iso(), source="alarm-engine", freshness=FRESHNESS_FRESH)


async def delete_alert_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    alarm = _require_alarm_service(ctx)
    removed = await alarm.delete_alert(params["alert_id"])
    return {"alert_id": params["alert_id"], "removed": removed}, Meta(
        as_of=utc_iso(), source="alarm-engine", freshness=FRESHNESS_FRESH
    )


async def get_triggered_alerts_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    alarm = _require_alarm_service(ctx)
    data = await alarm.get_triggered_alerts(
        alert_id=params.get("alert_id"),
        limit=params.get("limit", 50),
        cursor=params.get("cursor"),
    )
    return data, Meta(as_of=utc_iso(), source="alarm-engine", freshness=FRESHNESS_FRESH)


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
        clear_max_notional=params.get("clear_max_notional", False),
        clear_max_exposure=params.get("clear_max_exposure", False),
        clear_allowed_symbols=params.get("clear_allowed_symbols", False),
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
        raise RasatError(
            ErrorCode.PIPELINE_UNAVAILABLE,
            "order servisi için pipeline ve broker gereklidir — bu daemon'da kapalı",
        )
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
        stop_price=params.get("stop_price"),
        idempotency_key=params.get("idempotency_key"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-orders", freshness=FRESHNESS_FRESH)


async def place_oco_order_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    """OCO emri: LIMIT (kâr hedefi) + STOP_LOSS_LIMIT tek emir listesinde.

    Biri dolunca diğeri borsada otomatik iptal olur — aynı pozisyon için
    ayrı ayrı SL+TP emri bakiyeyi birbirinden çaldığı için tek çağrı şarttır.
    """
    service = _require_order_service(ctx)
    data = await service.place_oco_order(
        account_id=params.get("account_id"),
        symbol=params.get("symbol"),
        side=params.get("side"),
        quantity=params.get("quantity"),
        price=params.get("price"),
        stop_price=params.get("stop_price"),
        stop_limit_price=params.get("stop_limit_price"),
        idempotency_key=params.get("idempotency_key"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-orders", freshness=FRESHNESS_FRESH)


async def close_all_positions_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_order_service(ctx)
    data = await service.close_all_positions(
        account_id=params.get("account_id"),
        actor=str(ctx.get("actor", "mcp-agent")),
    )
    return data, Meta(as_of=utc_iso(), source="sqlite-orders", freshness=FRESHNESS_FRESH)


async def disable_real_trading_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    account_id = params.get("account_id")
    service = _require_account_service(ctx)
    if account_id == "all":
        accounts = await service.list_accounts()
        results = []
        for account in accounts["accounts"]:
            results.append(await service.disable_real_trading(account["account_id"], actor=str(ctx.get("actor", "mcp-agent"))))
        data = {"results": results, "count": len(results)}
    else:
        data = await service.disable_real_trading(account_id, actor=str(ctx.get("actor", "mcp-agent")))
    return data, Meta(as_of=utc_iso(), source="sqlite-accounts", freshness=FRESHNESS_FRESH)


async def get_total_exposure_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_order_service(ctx)
    data = await service.get_total_exposure()
    return data, Meta(as_of=utc_iso(), source="sqlite-orders", freshness=FRESHNESS_FRESH)


async def get_account_balance_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_order_service(ctx)
    data = await service.get_account_balance(account_id=params.get("account_id"))
    return data, Meta(as_of=utc_iso(), source="binance", freshness=FRESHNESS_FRESH)


async def get_open_orders_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_order_service(ctx)
    data = await service.get_open_orders(account_id=params.get("account_id"))
    return data, Meta(as_of=utc_iso(), source="binance", freshness=FRESHNESS_FRESH)


async def get_unprotected_positions_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    service = _require_order_service(ctx)
    data = await service.find_unprotected_positions()
    return data, Meta(as_of=utc_iso(), source="binance", freshness=FRESHNESS_FRESH)


async def get_audit_log_handler(params: dict, ctx: dict) -> tuple[dict, Meta]:
    """Audit log sorgusu — tamamen DB-yerel, pipeline gerektirmez."""
    audit = ctx.get("audit")
    if audit is None:
        raise RasatError(ErrorCode.NOT_IMPLEMENTED, "audit log bu daemon'da başlatılmamış")
    limit = params.get("limit", 50)
    if not isinstance(limit, int) or limit < 1 or limit > 500:
        raise RasatError(ErrorCode.INVALID_REQUEST, "limit 1-500 arası olmalı")
    broken = await audit.verify()
    tail = await audit.tail(limit)
    data = {
        "verified": len(broken) == 0,
        "broken": broken,
        "tail": tail,
        "count": len(tail),
    }
    return data, Meta(as_of=utc_iso(), source="sqlite-audit", freshness=FRESHNESS_FRESH)


def build_dispatcher(ctx: dict) -> ToolDispatcher:
    from ..tools import REGISTRY

    dispatcher = ToolDispatcher(REGISTRY)
    dispatcher.register("ping", ping_handler)
    dispatcher.register("get_readiness", readiness_handler)
    # TÜM tool'lar her zaman kayıtlıdır (adapter 32'sini de tanıtır).
    # Pipeline'a gerçekten bağımlı olanlar çağrıldığında PIPELINE_UNAVAILABLE
    # döner — TOOL_NOT_FOUND değil (tanıtım/tanım tutarlılığı).
    dispatcher.register("get_candles", candles_handler)
    dispatcher.register("get_ticker", ticker_handler)
    dispatcher.register("get_symbol_info", get_symbol_info_handler)
    dispatcher.register("calculate_position_size", calculate_position_size_handler)
    dispatcher.register("execute_on_accounts", execute_on_accounts_handler)
    dispatcher.register("place_order", place_order_handler)
    dispatcher.register("place_oco_order", place_oco_order_handler)
    dispatcher.register("close_all_positions", close_all_positions_handler)
    dispatcher.register("get_total_exposure", get_total_exposure_handler)
    dispatcher.register("get_account_balance", get_account_balance_handler)
    dispatcher.register("get_open_orders", get_open_orders_handler)
    dispatcher.register("get_unprotected_positions", get_unprotected_positions_handler)
    dispatcher.register("get_audit_log", get_audit_log_handler)
    dispatcher.register("add_account", add_account_handler)
    dispatcher.register("list_accounts", list_accounts_handler)
    dispatcher.register("remove_account", remove_account_handler)
    dispatcher.register("get_market_structure", get_market_structure_handler)
    dispatcher.register("get_liquidity_zones", get_liquidity_zones_handler)
    dispatcher.register("get_order_blocks", get_order_blocks_handler)
    dispatcher.register("get_full_analysis", get_full_analysis_handler)
    dispatcher.register("annotate_chart", annotate_chart_handler)
    dispatcher.register("get_chart_annotations", get_chart_annotations_handler)
    dispatcher.register("clear_annotations", clear_annotations_handler)
    dispatcher.register("scan_market", scan_market_handler)
    dispatcher.register("create_alert", create_alert_handler)
    dispatcher.register("create_composite_alert", create_composite_alert_handler)
    dispatcher.register("list_alerts", list_alerts_handler)
    dispatcher.register("delete_alert", delete_alert_handler)
    dispatcher.register("get_triggered_alerts", get_triggered_alerts_handler)
    dispatcher.register("get_pending_orders", get_pending_orders_handler)
    dispatcher.register("approve_pending_order", approve_pending_order_handler)
    dispatcher.register("reject_pending_order", reject_pending_order_handler)


    dispatcher.register("enable_real_trading", enable_real_trading_handler)
    dispatcher.register("disable_real_trading", disable_real_trading_handler)
    dispatcher.register("set_risk_policy", set_risk_policy_handler)
    dispatcher.register("override_risk_policy", override_risk_policy_handler)
    dispatcher.register("get_risk_policy", get_risk_policy_handler)
    return dispatcher
