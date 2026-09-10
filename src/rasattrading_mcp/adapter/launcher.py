"""Daemon auto-start and stale-lock recovery on the adapter side.

Rules:
- If there is no lock, start the daemon as a detached process.
- If the lock owner has exited (the PID is not alive), treat the lock as stale,
  remove it, and restart.
- If the lock owner is alive, verify it with an HTTP /health probe (1.3). Retry
  the probe several times because the daemon may not have bound its server yet;
  if it still does not respond, treat the lock as stale.
- Wait until `ready`; fail closed by raising an error on timeout.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from ..config import Config
from ..daemon.lock import LockInfo, owner_alive, read_lock
from ..errors import ErrorCode, RasatError
from .transport import DaemonClient

logger = logging.getLogger("rasattrading.adapter.launcher")


class DaemonUnavailableError(RasatError):
    def __init__(self, message: str, code: str = ErrorCode.INTERNAL_ERROR) -> None:
        super().__init__(code, message)


def _spawn_daemon(config: Config) -> None:
    """Start the daemon detached in its own process group."""
    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.log_dir.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    env["RASATTRADING_DATA_DIR"] = str(config.data_dir)
    env["RASATTRADING_PORT"] = str(config.port)
    env["RASATTRADING_PIPELINE_ENABLED"] = "1" if config.pipeline_enabled else "0"

    cmd = [sys.executable, "-m", "rasattrading_mcp.daemon"]
    with open(config.daemon_log_path, "a", encoding="utf-8") as logf:
        creationflags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        subprocess.Popen(
            cmd,
            cwd=str(Path(__file__).resolve().parents[3]),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=logf,
            stderr=logf,
            creationflags=creationflags,
            close_fds=True,
        )
    logger.info("daemon started: %s", " ".join(cmd))


def _cleanup_stale_lock(config: Config) -> None:
    lock = read_lock(config.lock_path)
    if lock is not None and not owner_alive(lock):
        config.lock_path.unlink(missing_ok=True)
        logger.info("removed stale lock (pid=%s)", lock.pid)


async def _wait_for_lock(config: Config, timeout: float) -> LockInfo:
    """Wait for the daemon to create its lock file."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lock = read_lock(config.lock_path)
        if lock is not None and owner_alive(lock):
            return lock
        await asyncio.sleep(0.2)
    raise DaemonUnavailableError(
        f"daemon did not create a lock file within {timeout}s", code=ErrorCode.TIMEOUT
    )


async def _http_probe_ok(config: Config, token: str) -> bool:
    """Probe HTTP /health to check whether the daemon is alive and owns the token."""
    try:
        client = DaemonClient(config, token)
        try:
            health = await client.health()
            return health.get("ok") is True
        finally:
            await client.close()
    except Exception:  # noqa: BLE001
        return False


async def _http_state(config: Config, token: str) -> str | None:
    try:
        client = DaemonClient(config, token)
        try:
            health = await client.health()
            return health.get("data", {}).get("state")
        finally:
            await client.close()
    except Exception:  # noqa: BLE001
        return None


async def _wait_until_ready(config: Config, lock: LockInfo, timeout: float) -> None:
    """Wait for `ready` through HTTP /health, the authoritative channel."""
    deadline = time.monotonic() + timeout
    last_state: str | None = None
    while time.monotonic() < deadline:
        state = await _http_state(config, lock.token)
        if state is not None:
            last_state = state
            if state == "ready":
                return
        await asyncio.sleep(0.25)
    raise DaemonUnavailableError(
        f"daemon was not ready within {timeout}s (last state={last_state})",
        code=ErrorCode.NOT_READY,
    )


async def ensure_daemon(config: Config) -> str:
    """Ensure that the daemon is running and ready, then return its bearer token."""
    _cleanup_stale_lock(config)

    lock = read_lock(config.lock_path)
    if lock is None:
        _spawn_daemon(config)
        lock = await _wait_for_lock(config, timeout=config.ready_timeout_seconds)
    elif not owner_alive(lock):
        # Check again for a race.
        if read_lock(config.lock_path) is not None:
            config.lock_path.unlink(missing_ok=True)
        _spawn_daemon(config)
        lock = await _wait_for_lock(config, timeout=config.ready_timeout_seconds)
    elif await _http_probe_ok(config, lock.token):
        # Verified as alive over HTTP; continue.
        pass
    else:
        # The PID is alive but the HTTP probe failed; treat it as stale after retries.
        retries = max(config.lock_probe_retries, 1)
        ok = False
        for _ in range(retries):
            if await _http_probe_ok(config, lock.token):
                ok = True
                break
            await asyncio.sleep(config.lock_probe_delay)
        if not ok:
            logger.warning("lock owner PID is alive but HTTP is not responding — treating lock as stale (pid=%s)", lock.pid)
            config.lock_path.unlink(missing_ok=True)
            _spawn_daemon(config)
            lock = await _wait_for_lock(config, timeout=config.ready_timeout_seconds)

    await _wait_until_ready(config, lock, timeout=config.ready_timeout_seconds)
    return lock.token
