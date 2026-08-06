import json

import pytest
from aiohttp.test_utils import TestServer
from mcp.shared.memory import create_connected_server_and_client_session

from rasattrading_mcp.adapter.main import build_adapter_server, build_mcp_tools
from rasattrading_mcp.adapter.transport import DaemonClient
from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.daemon.readiness import Readiness
from rasattrading_mcp.daemon.server import build_app
from rasattrading_mcp.tools import REGISTRY

TOKEN = "test-token-abc"


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


async def _start_daemon_app(cfg):
    """Gerçek daemon app'ini geçici porta bağlar; (runner, port) döner."""
    from aiohttp import web

    readiness = Readiness()
    readiness.set_state("migrating")
    readiness.set_state("warming_up")
    readiness.set_state("ready")
    ctx = {"config": cfg, "readiness": readiness, "started_at": 0, "pid": 1, "pipeline": None}
    dispatcher = build_dispatcher(ctx)
    app = build_app(cfg, readiness, TOKEN, dispatcher, extra=ctx)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, port


def _extract_envelope(result) -> dict:
    content = result.content[0]
    assert content.type == "text"
    return json.loads(content.text)


async def test_mcp_handshake_list_tools_and_call(cfg):
    runner, port = await _start_daemon_app(cfg)
    try:
        client = DaemonClient(Config(data_dir=cfg.data_dir, port=port), TOKEN)
        server = build_adapter_server(client)

        # tool listesi registry'den yansıyor mu?
        tools = build_mcp_tools()
        names = {t.name for t in tools}
        assert names == set(REGISTRY.names())

        async with create_connected_server_and_client_session(server) as session:
            listed = await session.list_tools()
            listed_names = {t.name for t in listed.tools}
            assert listed_names == names
            assert all(t.description for t in listed.tools)
            assert all(t.inputSchema for t in listed.tools)

            result = await session.call_tool("ping", {"request_id": "mcp-1"})
            assert result.isError is False
            envelope = _extract_envelope(result)
            assert envelope["ok"] is True
            assert envelope["request_id"] == "mcp-1"
            assert envelope["data"]["pong"] is True
            assert envelope["data"]["state"] == "ready"
            assert "as_of" in envelope["meta"]
        await client.close()
    finally:
        await runner.cleanup()


async def test_initialization_options_buildable_for_run(cfg):
    """mcp SDK >=1.29 Server.run stdio yolu icin gerekli options uretilebiliyor.

    `run_adapter` stdio_server + Server.run yolunu kullanir; SDK 1.29'da
    InitializationOptions (capabilities dahil) zorunlu hale geldi ve bu yol
    hicbir unit testte sarmalanmadigi icin canli stdio testinde patlamisti.
    Bu test, options kurulumunun yapilabilir oldugunu sabitler.
    """
    from rasattrading_mcp.adapter.main import build_initialization_options

    runner, port = await _start_daemon_app(cfg)
    try:
        client = DaemonClient(Config(data_dir=cfg.data_dir, port=port), TOKEN)
        try:
            server = build_adapter_server(client)
            options = build_initialization_options(server)
            assert options.server_name == "rasattrading-mcp"
            assert options.server_version
            assert options.capabilities is not None
            assert options.capabilities.tools is not None
        finally:
            await client.close()
    finally:
        await runner.cleanup()


async def test_mcp_unknown_tool_is_error(cfg):
    runner, port = await _start_daemon_app(cfg)
    try:
        client = DaemonClient(Config(data_dir=cfg.data_dir, port=port), TOKEN)
        server = build_adapter_server(client)
        async with create_connected_server_and_client_session(server) as session:
            result = await session.call_tool("no_such_tool", {})
            assert result.isError is True
            envelope = _extract_envelope(result)
            assert envelope["ok"] is False
            assert envelope["error"]["code"] == "TOOL_NOT_FOUND"
        await client.close()
    finally:
        await runner.cleanup()


async def test_mcp_wrong_token_from_client(cfg):
    runner, port = await _start_daemon_app(cfg)
    try:
        client = DaemonClient(Config(data_dir=cfg.data_dir, port=port), "wrong-token")
        server = build_adapter_server(client)
        async with create_connected_server_and_client_session(server) as session:
            result = await session.call_tool("ping", {})
            assert result.isError is True
            envelope = _extract_envelope(result)
            assert envelope["error"]["code"] == "UNAUTHORIZED"
        await client.close()
    finally:
        await runner.cleanup()
