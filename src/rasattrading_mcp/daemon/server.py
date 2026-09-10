"""Daemon-side HTTP IPC server (localhost-only, bearer token).

- `GET  /health` — daemon state/pid/version (readiness and ownership probe)
- `POST /rpc`    — tool call: `{tool, params, request_id?, idempotency_key?}`
                    → common envelope `{ok, data?, error?, meta}`

All requests require `Authorization: Bearer <token>`. Before the daemon is `ready`,
only tools with `allowed_before_ready` may be called; all others fail closed with NOT_READY.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import time
from typing import Any, Awaitable, Callable

from aiohttp import web

from .. import __version__
from ..config import Config
from ..daemon.readiness import Readiness
from ..envelope import Meta, error_response, error_response_from_exc, ok_response, utc_iso
from ..errors import ErrorCode, RasatError, http_status_for
from ..schema_lite import validate_params
from ..tools import ToolRegistry

logger = logging.getLogger("rasattrading.daemon.http")

HandlerFn = Callable[..., Awaitable[tuple[Any, Meta]]]

#: /rpc request-body limit (schemas are small; oversized bodies are rejected).
MAX_BODY_BYTES = 1_000_000


class ToolDispatcher:
    """Tool name → handler. Handlers have the `(params, ctx) -> (data, meta)` signature."""

    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry
        self._handlers: dict[str, HandlerFn] = {}

    def register(self, name: str, handler: HandlerFn) -> None:
        if name not in self._registry:
            raise ValueError(f"tool handler is not in the registry: {name}")
        self._handlers[name] = handler

    def spec_for(self, name: str):
        """Tool spec (input_schema/allowed_before_ready) from the dispatcher's registry."""
        return self._registry.get(name)

    def names(self) -> list[str]:
        return list(self._handlers)

    async def dispatch(self, name: str, params: dict, ctx: Any) -> tuple[Any, Meta]:
        handler = self._handlers.get(name)
        if handler is None:
            raise RasatError(ErrorCode.TOOL_NOT_FOUND, f"unknown tool: {name}")
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
    # T01: constant-time comparison (no timing oracle).
    return hmac.compare_digest(token, expected_token)


def _merge_transport_fields(body: dict, params: dict) -> dict:
    """Merge top-level request_id/idempotency_key into dispatch params.

    If the same key is supplied at both top level and inside params with DIFFERENT
    values, raise a conflict error (INVALID_REQUEST); if equal, the top-level value wins.
    """
    merged = dict(params)
    for key in ("request_id", "idempotency_key"):
        top = body.get(key)
        param_val = params.get(key)
        if top is not None and param_val is not None and top != param_val:
            raise RasatError(
                ErrorCode.INVALID_REQUEST,
                f"{key} values conflict between top level and params",
            )
        if top is not None:
            merged[key] = top
    return merged


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
                error_response(ErrorCode.UNAUTHORIZED, "invalid or missing bearer token"),
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
            # T01: do not leak raw exception/path/secret; log only on the server side.
            logger.exception("HTTP request failed: %s %s", request.method, request.path)
            return web.json_response(
                error_response(ErrorCode.INTERNAL_ERROR, "internal error"),
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
            raw = await request.read()
        except Exception:  # noqa: BLE001
            raise RasatError(ErrorCode.INVALID_REQUEST, "could not read request body")
        if len(raw) > MAX_BODY_BYTES:
            raise RasatError(ErrorCode.INVALID_REQUEST, "request body is too large")
        try:
            body = json.loads(raw)
        except Exception:  # noqa: BLE001
            raise RasatError(ErrorCode.INVALID_REQUEST, "request body is not valid JSON")

        if not isinstance(body, dict):
            raise RasatError(ErrorCode.INVALID_REQUEST, "request body must be an object")

        tool = body.get("tool")
        if not isinstance(tool, str) or not tool:
            raise RasatError(ErrorCode.INVALID_REQUEST, "tool field is required (string)")

        params = body.get("params") or {}
        if not isinstance(params, dict):
            raise RasatError(ErrorCode.INVALID_REQUEST, "params field must be an object")

        spec = dispatcher.spec_for(tool)
        if spec is None:
            raise RasatError(ErrorCode.TOOL_NOT_FOUND, f"unknown tool: {tool}")

        if not spec.allowed_before_ready and not readiness.is_ready():
            raise RasatError(
                ErrorCode.NOT_READY,
                f"daemon is not ready (state={readiness.state}) — tool call rejected",
            )

        # T01: merge top-level request_id/idempotency_key into dispatch params.
        params = _merge_transport_fields(body, params)

        # T01: defense-in-depth input schema validation — the schema is not the
        # sole authority, but this cuts obvious client errors before the service layer.
        validate_params(params, spec.input_schema)

        try:
            data, meta = await dispatcher.dispatch(tool, params, ctx=extra)
        except asyncio.CancelledError:
            raise
        except RasatError:
            raise
        except KeyError as exc:
            # A handler accessed a missing/required parameter with `params["x"]`,
            # causing KeyError to fall into the generic except and produce 500.
            # This is a client error.
            logger.warning("tool %s missing parameter: %s", tool, exc)
            raise RasatError(
                ErrorCode.INVALID_REQUEST, f"{tool} missing required parameter: {exc}"
            ) from exc
        except Exception as exc:  # noqa: BLE001
            # T01: do not leak raw exception text into the response (keep it in the server log).
            logger.exception("tool failed: %s", tool)
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"{tool} failed")

        return web.json_response(ok_response(data=data, meta=meta, request_id=params.get("request_id")))

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
    logger.info("HTTP IPC listening on %s:%s", config.host, config.port)
    return site, runner
