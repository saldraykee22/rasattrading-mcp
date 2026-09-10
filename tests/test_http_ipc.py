import asyncio
import json

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.daemon.readiness import Readiness
from rasattrading_mcp.daemon.server import build_app
from rasattrading_mcp.tools import REGISTRY, ToolSpec

TOKEN = "test-token-abc"


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


def _make_app(cfg, state: str = "ready"):
    readiness = Readiness()
    states = ["starting", "migrating", "warming_up", "ready"]
    for s in states[: states.index(state) + 1]:
        readiness.set_state(s)
    ctx = {
        "config": cfg,
        "readiness": readiness,
        "started_at": 0,
        "pid": 12345,
        "pipeline": None,
    }
    dispatcher = build_dispatcher(ctx)
    return build_app(cfg, readiness, TOKEN, dispatcher, extra=ctx)


@pytest.fixture
async def client(cfg):
    app = _make_app(cfg, "ready")
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            yield client


def _json_body(data):
    return json.dumps(data)


async def test_no_token_rejected(client):
    resp = await client.get("/health")
    assert resp.status == 401
    body = await resp.json()
    assert body["ok"] is False
    assert body["error"]["code"] == "UNAUTHORIZED"


async def test_wrong_token_rejected(client):
    resp = await client.get("/health", headers={"Authorization": "Bearer wrong"})
    assert resp.status == 401
    body = await resp.json()
    assert body["ok"] is False


async def test_health_with_token(cfg, client):
    resp = await client.get("/health", headers={"Authorization": f"Bearer {TOKEN}"})
    assert resp.status == 200
    body = await resp.json()
    assert body["ok"] is True
    assert body["data"]["state"] == "ready"
    assert body["data"]["ready"] is True
    assert "as_of" in body["meta"]
    assert body["meta"]["freshness"] == "fresh"


async def test_rpc_ping_returns_envelope(cfg, client):
    resp = await client.post(
        "/rpc",
        data=_json_body({"tool": "ping", "params": {}, "request_id": "req-1"}),
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    assert resp.status == 200
    body = await resp.json()
    assert body["ok"] is True
    assert body["request_id"] == "req-1"
    assert body["data"]["pong"] is True
    assert body["data"]["state"] == "ready"
    assert "meta" in body and "as_of" in body["meta"]


async def test_rpc_unknown_tool(cfg, client):
    resp = await client.post(
        "/rpc",
        data=_json_body({"tool": "no_such_tool", "params": {}}),
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    assert resp.status == 404
    body = await resp.json()
    assert body["ok"] is False
    assert body["error"]["code"] == "TOOL_NOT_FOUND"


async def test_rpc_invalid_body(cfg, client):
    resp = await client.post(
        "/rpc",
        data="not-json",
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    assert resp.status == 400
    body = await resp.json()
    assert body["error"]["code"] == "INVALID_REQUEST"


async def test_rpc_missing_required_param_still_400_with_service(cfg):
    """With alert service available, missing parameter returns 400 (live daemon scenario).

    Handlers read with `params["x"]`; a missing key raised KeyError and fell into
    generic except → 500. This is a client error; correct status is 400.
    """
    class _StubAlarmService:
        async def create_alert(self, symbol, timeframe, condition, **kwargs):
            raise AssertionError("handler must not pass the missing parameter to the service")

    readiness = Readiness()
    for s in ["starting", "migrating", "warming_up", "ready"]:
        readiness.set_state(s)
    ctx = {
        "config": cfg,
        "readiness": readiness,
        "started_at": 0,
        "pid": 12345,
        "pipeline": None,
        "alarm_service": _StubAlarmService(),
    }
    dispatcher = build_dispatcher(ctx)
    app = build_app(cfg, readiness, TOKEN, dispatcher, extra=ctx)
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.post(
                "/rpc",
                data=_json_body({"tool": "create_alert", "params": {"symbol": "BTCUSDT"}}),
                headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
            )
            assert resp.status == 400
            body = await resp.json()
            assert body["ok"] is False
            assert body["error"]["code"] == "INVALID_REQUEST"
            assert "missing required parameter" in body["error"]["message"]


async def test_not_ready_fails_closed_for_regular_tool(cfg):
    # A regular tool must be rejected before the server is ready (NOT_READY fail-closed).
    REGISTRY.register(
        ToolSpec(name="test_requires_ready", description="", input_schema={"type": "object", "properties": {}})
    )
    try:
        app = _make_app(cfg, "warming_up")
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                resp = await client.post(
                    "/rpc",
                    data=_json_body({"tool": "test_requires_ready", "params": {}}),
                    headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
                )
                assert resp.status == 503
                body = await resp.json()
                assert body["ok"] is False
                assert body["error"]["code"] == "NOT_READY"
    finally:
        REGISTRY._tools.pop("test_requires_ready", None)


async def test_not_ready_allows_ping(cfg):
    app = _make_app(cfg, "migrating")
    async with TestServer(app) as server:
        async with TestClient(server) as client:
            resp = await client.post(
                "/rpc",
                data=_json_body({"tool": "ping", "params": {}}),
                headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
            )
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert body["data"]["state"] == "migrating"
