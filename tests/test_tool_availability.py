"""Ticket 1-5: tool availability with pipeline disabled (review finding H6).

Beklentiler:
- get_audit_log must work with pipeline disabled (DB-local, no pipeline required).
- Tools that truly require pipeline must return PIPELINE_UNAVAILABLE (not TOOL_NOT_FOUND).
- Dispatcher must keep all 32 tools registered regardless of pipeline state
  (consistent with the adapter's advertised list).
"""

import json

import pytest
from aiohttp.test_utils import TestClient, TestServer

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.daemon.readiness import Readiness
from rasattrading_mcp.daemon.server import build_app
from rasattrading_mcp.storage.audit import AuditLog
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.tools import REGISTRY

TOKEN = "test-token-1-5"


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


def _ready():
    r = Readiness()
    r.set_state("migrating")
    r.set_state("warming_up")
    r.set_state("ready")
    return r


async def _make_no_pipeline_app(cfg, with_pipeline_stub=False):
    """Simulate a daemon setup without pipeline: db + audit, no pipeline."""
    db = Database(cfg.db_path)
    await db.start()
    await run_migrations(db)
    audit = AuditLog(db)
    ctx = {
        "config": cfg,
        "readiness": _ready(),
        "started_at": 0,
        "pid": 1,
        "db": db,
        "audit": audit,
    }
    if with_pipeline_stub:
        class StubPipeline:
            def status(self):
                return {}

        ctx["pipeline"] = StubPipeline()
    dispatcher = build_dispatcher(ctx)
    app = build_app(cfg, ctx["readiness"], TOKEN, dispatcher, extra=ctx)
    return db, audit, app, dispatcher


async def _post(client, tool, params):
    return await client.post(
        "/rpc",
        data=json.dumps({"tool": tool, "params": params}),
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )


async def test_dispatcher_registers_all_tools_without_pipeline(cfg):
    db, _, _, dispatcher = await _make_no_pipeline_app(cfg)
    try:
        assert set(dispatcher.names()) == set(REGISTRY.names())
        assert len(dispatcher.names()) == 39
    finally:
        await db.stop()


async def test_get_audit_log_works_without_pipeline(cfg):
    db, audit, app, _ = await _make_no_pipeline_app(cfg)
    try:
        await audit.append("agent-a", "place_order", {"symbol": "BTCUSDT"})
        await audit.append("agent-b", "cancel_order", {})
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                resp = await _post(client, "get_audit_log", {"limit": 50})
                assert resp.status == 200
                body = await resp.json()
                assert body["ok"] is True
                assert body["data"]["count"] == 2
                assert body["data"]["verified"] is True
                assert body["data"]["broken"] == []
    finally:
        await db.stop()


async def test_pipeline_tools_return_pipeline_unavailable(cfg):
    db, _, app, _ = await _make_no_pipeline_app(cfg)
    try:
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                # execute_on_accounts: depends on pipeline → meaningful error, not TOOL_NOT_FOUND.
                resp = await _post(
                    client,
                    "execute_on_accounts",
                    {"account_ids": ["x"], "symbol": "BTCUSDT", "side": "BUY",
                     "entry": 90000, "stop_loss": 88000, "risk_pct": 0.02,
                     "idempotency_key": "k1"},
                )
                assert resp.status == 503
                body = await resp.json()
                assert body["ok"] is False
                assert body["error"]["code"] == "PIPELINE_UNAVAILABLE"

                # get_candles also depends on pipeline.
                resp2 = await _post(client, "get_candles", {"symbol": "BTCUSDT", "timeframe": "15m"})
                body2 = await resp2.json()
                assert body2["error"]["code"] == "PIPELINE_UNAVAILABLE"

                # A genuinely unknown tool is still TOOL_NOT_FOUND.
                resp3 = await _post(client, "no_such_tool", {})
                body3 = await resp3.json()
                assert body3["error"]["code"] == "TOOL_NOT_FOUND"
    finally:
        await db.stop()


async def test_get_audit_log_works_with_pipeline(cfg):
    db, audit, app, _ = await _make_no_pipeline_app(cfg, with_pipeline_stub=True)
    try:
        await audit.append("agent-a", "startup", {})
        async with TestServer(app) as server:
            async with TestClient(server) as client:
                resp = await _post(client, "get_audit_log", {"limit": 10})
                body = await resp.json()
                assert body["ok"] is True
                assert body["data"]["count"] == 1
    finally:
        await db.stop()
