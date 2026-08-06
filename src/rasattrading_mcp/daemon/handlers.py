"""Daemon tarafı tool handler'ları (Modül 1 örnekleri; Modül 2/3 ekler)."""

from __future__ import annotations

import os
import time
from typing import Any

from .. import __version__
from ..config import Config
from ..daemon.lock import LockInfo
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


def build_dispatcher(ctx: dict) -> ToolDispatcher:
    from ..tools import REGISTRY

    dispatcher = ToolDispatcher(REGISTRY)
    dispatcher.register("ping", ping_handler)
    dispatcher.register("get_readiness", readiness_handler)
    return dispatcher
