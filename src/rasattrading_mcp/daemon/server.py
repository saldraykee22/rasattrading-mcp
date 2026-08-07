"""Daemon tarafı HTTP IPC sunucusu (localhost-only, bearer token).

- `GET  /health` — daemon state/pid/sürüm (readiness + sahiplik probu)
- `POST /rpc`    — tool çağrısı: `{tool, params, request_id?, idempotency_key?}`
                    → ortak envelope `{ok, data?, error?, meta}`

Tüm istekler `Authorization: Bearer <token>` ister. Daemon `ready` değilken sadece
`allowed_before_ready` tool'lar çağrılabilir; diğerleri NOT_READY ile fail-closed döner.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

from aiohttp import web

from .. import __version__
from ..config import Config
from ..daemon.readiness import Readiness
from ..envelope import Meta, error_response, error_response_from_exc, ok_response, utc_iso
from ..errors import ErrorCode, RasatError, http_status_for
from ..tools import REGISTRY, ToolRegistry

logger = logging.getLogger("rasattrading.daemon.http")

HandlerFn = Callable[..., Awaitable[tuple[Any, Meta]]]


class ToolDispatcher:
    """Tool adı → handler. Handler `(params, ctx) -> (data, meta)` imzalıdır."""

    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry
        self._handlers: dict[str, HandlerFn] = {}

    def register(self, name: str, handler: HandlerFn) -> None:
        if name not in self._registry:
            raise ValueError(f"registry'de olmayan tool handler'ı: {name}")
        self._handlers[name] = handler

    def names(self) -> list[str]:
        return list(self._handlers)

    async def dispatch(self, name: str, params: dict, ctx: Any) -> tuple[Any, Meta]:
        handler = self._handlers.get(name)
        if handler is None:
            raise RasatError(ErrorCode.TOOL_NOT_FOUND, f"bilinmeyen tool: {name}")
        return await handler(params, ctx)


def _verify_token(expected_token: str, auth_header: str | None) -> bool:
    if not auth_header:
        return False
    try:
        scheme, _, token = auth_header.partition(" ")
    except Exception:  # noqa: BLE001
        return False
    if scheme.lower() != "bearer" or not token:
        return False
    return token == expected_token


def build_app(
    config: Config,
    readiness: Readiness,
    token: str,
    dispatcher: ToolDispatcher,
    extra: dict[str, Any] | None = None,
) -> web.Application:
    extra = extra or {}

    @web.middleware
    async def auth_middleware(request: web.Request, handler):
        if not _verify_token(token, request.headers.get("Authorization")):
            return web.json_response(
                error_response(ErrorCode.UNAUTHORIZED, "geçersiz/eksik bearer token"),
                status=401,
            )
        return await handler(request)

    @web.middleware
    async def error_middleware(request: web.Request, handler):
        try:
            return await handler(request)
        except RasatError as exc:
            return web.json_response(
                error_response_from_exc(exc),
                status=exc.http_status or http_status_for(exc.code),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("HTTP istek hatası: %s %s", request.method, request.path)
            return web.json_response(
                error_response(ErrorCode.INTERNAL_ERROR, str(exc)),
                status=500,
            )

    app = web.Application(middlewares=[auth_middleware, error_middleware])

    async def health_handler(request: web.Request) -> web.Response:
        pid = extra.get("pid", None)
        started_at = extra.get("started_at", None)
        data = {
            "state": readiness.state,
            "ready": readiness.is_ready(),
            "pid": pid,
            "version": __version__,
            "port": config.port,
            "started_at": utc_iso(started_at) if started_at else None,
            "uptime_s": round(time.time() - started_at, 1) if started_at else None,
        }
        return web.json_response(ok_response(data=data, meta=Meta(as_of=utc_iso())))

    async def rpc_handler(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except Exception:  # noqa: BLE001
            raise RasatError(ErrorCode.INVALID_REQUEST, "istek gövdesi geçerli JSON değil")

        if not isinstance(body, dict):
            raise RasatError(ErrorCode.INVALID_REQUEST, "istek gövdesi nesne olmalı")

        tool = body.get("tool")
        if not isinstance(tool, str) or not tool:
            raise RasatError(ErrorCode.INVALID_REQUEST, "tool alanı zorunlu (string)")

        params = body.get("params") or {}
        if not isinstance(params, dict):
            raise RasatError(ErrorCode.INVALID_REQUEST, "params alanı nesne olmalı")

        request_id = body.get("request_id")
        idempotency_key = body.get("idempotency_key")

        spec = REGISTRY.get(tool)
        if spec is None:
            raise RasatError(ErrorCode.TOOL_NOT_FOUND, f"bilinmeyen tool: {tool}")

        if not spec.allowed_before_ready and not readiness.is_ready():
            raise RasatError(
                ErrorCode.NOT_READY,
                f"daemon ready değil (state={readiness.state}) — tool çağrısı reddedildi",
            )

        try:
            data, meta = await dispatcher.dispatch(tool, params, ctx=extra)
        except asyncio.CancelledError:
            raise
        except RasatError:
            raise
        except KeyError as exc:
            # Handler eksik/zorunlu parametreye `params["x"]` ile erişiyordu →
            # KeyError generic except'e düşüp 500 üretiyordu. İstemci hatasıdır.
            logger.warning("tool %s eksik parametre: %s", tool, exc)
            raise RasatError(
                ErrorCode.INVALID_REQUEST, f"{tool} eksik zorunlu parametre: {exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            logger.exception("tool hatası: %s", tool)
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"{tool} başarısız: {exc}")

        return web.json_response(ok_response(data=data, meta=meta, request_id=request_id))

    app.router.add_get("/health", health_handler)
    app.router.add_post("/rpc", rpc_handler)
    return app


async def build_site(
    config: Config,
    readiness: Readiness,
    token: str,
    dispatcher: ToolDispatcher,
    extra: dict[str, Any] | None = None,
) -> tuple[web.TCPSite, web.AppRunner]:
    app = build_app(config, readiness, token, dispatcher, extra)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.host, config.port)
    await site.start()
    logger.info("HTTP IPC dinleniyor: %s:%s", config.host, config.port)
    return site, runner
