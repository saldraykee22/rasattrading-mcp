"""Adapter tarafı daemon'a HTTP istemcisi (bearer token)."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from ..config import Config
from ..errors import ErrorCode, RasatError

logger = logging.getLogger("rasattrading.adapter.transport")


class DaemonUnavailableError(RasatError):
    def __init__(self, message: str, code: str = ErrorCode.INTERNAL_ERROR) -> None:
        super().__init__(code, message)


class DaemonClient:
    """localhost HTTP üzerinden daemon ile konuşur; ortak envelope'ı döner."""

    def __init__(self, config: Config, token: str) -> None:
        self._config = config
        self._token = token
        self._base = f"http://{config.host}:{config.port}"
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._config.http_timeout_seconds)
            )
        return self._session

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    async def health(self) -> dict[str, Any]:
        """GET /health — daemon canlı ve sahibi mi?"""
        try:
            session = await self._ensure_session()
            async with session.get(f"{self._base}/health", headers=self._headers()) as resp:
                if resp.status == 401:
                    raise DaemonUnavailableError("token geçersiz (farklı daemon sahibi)", code=ErrorCode.UNAUTHORIZED)
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientError as exc:
            raise DaemonUnavailableError(f"daemon'a ulaşılamadı: {exc}") from exc

    async def call_tool(self, tool: str, params: dict[str, Any]) -> dict[str, Any]:
        """POST /rpc — ortak envelope döner. request_id/idempotency_key üst seviyeye taşınır."""
        payload: dict[str, Any] = {"tool": tool, "params": params}
        for key in ("request_id", "idempotency_key"):
            if key in params:
                payload[key] = params[key]
        try:
            session = await self._ensure_session()
            async with session.post(f"{self._base}/rpc", json=payload, headers=self._headers()) as resp:
                body = await resp.json()
                if not isinstance(body, dict):
                    raise DaemonUnavailableError("daemon geçersiz envelope döndü")
                if resp.status == 401:
                    body = {
                        "ok": False,
                        "error": {"code": ErrorCode.UNAUTHORIZED, "message": "token geçersiz"},
                    }
                return body
        except aiohttp.ClientError as exc:
            raise DaemonUnavailableError(f"daemon'a ulaşılamadı: {exc}") from exc

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
