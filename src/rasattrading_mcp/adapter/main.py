"""MCP adapter: forwards tool calls received over stdio to the daemon over HTTP.

Thin client with no business logic. Starts/connects to the daemon with
`ensure_daemon`, mirrors the tool list from the shared registry, and returns
responses in the common envelope.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any

from mcp.server.lowlevel import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import CallToolResult, TextContent, Tool

from .. import __version__
from ..config import Config
from ..envelope import dumps
from ..errors import ErrorCode
from ..logging_util import setup_logging
from ..tools import REGISTRY
from .launcher import DaemonUnavailableError, ensure_daemon
from .transport import DaemonClient

logger = logging.getLogger("rasattrading.adapter")


def build_mcp_tools() -> list[Tool]:
    return [
        Tool(name=t.name, description=t.description, inputSchema=t.input_schema)
        for t in REGISTRY.list()
    ]


def build_initialization_options(server: Server) -> InitializationOptions:
    """InitializationOptions required by `Server.run` in mcp SDK >=1.29.

Starting with SDK 1.29, `initialization_options` is required, and the
capabilities field is also required by pydantic. Keeping this separate
improves testability (the stdio path is not wrapped in unit tests).
    """
    return InitializationOptions(
        server_name="rasattrading-mcp",
        server_version=__version__,
        capabilities=server.get_capabilities(
            notification_options=NotificationOptions(),
            experimental_capabilities={},
        ),
    )


def build_adapter_server(client: DaemonClient) -> Server:
    """Build the MCP LowLevelServer: tools come from the registry, calls go to the daemon."""
    server = Server("rasattrading-mcp")

    @server.list_tools()
    async def _list_tools() -> list[Tool]:
        return build_mcp_tools()

    @server.call_tool()
    async def _call_tool(name: str, arguments: dict[str, Any] | None) -> CallToolResult:
        try:
            envelope = await client.call_tool(name, arguments or {})
        except DaemonUnavailableError as exc:
            logger.error("could not forward daemon request: %s", exc.message)
            return _error_result(exc.code or ErrorCode.INTERNAL_ERROR, exc.message)
        except Exception as exc:  # noqa: BLE001
            logger.exception("tool call failed: %s", name)
            return _error_result(ErrorCode.INTERNAL_ERROR, str(exc))
        return _envelope_to_result(envelope)

    return server


def _envelope_to_result(envelope: dict) -> CallToolResult:
    text = dumps(envelope)
    if envelope.get("ok") is True:
        return CallToolResult(content=[TextContent(type="text", text=text)], isError=False)
    return CallToolResult(content=[TextContent(type="text", text=text)], isError=True)


def _error_result(code: str, message: str) -> CallToolResult:
    return _envelope_to_result({"ok": False, "error": {"code": code, "message": message}})


async def run_adapter(config: Config) -> int:
    setup_logging("rasattrading", stderr=True)

    try:
        token = await ensure_daemon(config)
    except DaemonUnavailableError as exc:
        logger.error("could not make daemon ready: %s", exc.message)
        return 1

    client = DaemonClient(config, token)
    server = build_adapter_server(client)

    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                build_initialization_options(server),
            )
    finally:
        await client.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rasattrading-mcp", description="Rasattrading MCP adapter (thin client)")
    p.add_argument("--data-dir", help="data directory (default: ~/.rasattrading)")
    p.add_argument("--port", type=int, help="daemon HTTP portu")
    p.add_argument("--no-pipeline", action="store_true", help="start the daemon without the pipeline")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    overrides = {}
    if args.data_dir:
        overrides["data_dir"] = args.data_dir
    if args.port:
        overrides["port"] = args.port
    if args.no_pipeline:
        overrides["pipeline_enabled"] = False
    config = Config.from_env(overrides)
    try:
        return asyncio.run(run_adapter(config))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
