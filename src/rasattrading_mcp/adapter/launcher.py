"""Daemon oto-başlatma + stale-lock recovery (adapter tarafı).

Kurallar:
- Kilit yoksa: daemon'ı detached süreç olarak başlat.
- Kilit sahibi ölmüşse (PID canlı değil): stale kabul et, temizle, yeniden başlat.
- Kilit sahibi canlıysa: HTTP /health probuyla doğrula (1.3). Probu birkaç kez dener
  (daemon daha server'ı bağlamamış olabilir); hâlâ yanıt yoksa stale kabul eder.
- `ready` olana kadar bekle; timeoute girerse fail-closed (hata fırlatır).
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
    """Daemon'ı detached (kendi süreç grubunda) başlatır."""
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
    logger.info("daemon başlatıldı: %s", " ".join(cmd))


def _cleanup_stale_lock(config: Config) -> None:
    lock = read_lock(config.lock_path)
    if lock is not None and not owner_alive(lock):
        config.lock_path.unlink(missing_ok=True)
        logger.info("stale kilit temizlendi (pid=%s)", lock.pid)


async def _wait_for_lock(config: Config, timeout: float) -> LockInfo:
    """Daemon'un kilit dosyasını oluşturmasını bekle."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lock = read_lock(config.lock_path)
        if lock is not None and owner_alive(lock):
            return lock
        await asyncio.sleep(0.2)
    raise DaemonUnavailableError(
        f"daemon kilit dosyası oluşturamadı ({timeout}s içinde)", code=ErrorCode.TIMEOUT
    )


async def _http_probe_ok(config: Config, token: str) -> bool:
    """HTTP /health probu: daemon canlı ve token sahibi mi?"""
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
    """HTTP /health üzerinden `ready` beklenir (yetkili kanal)."""
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
        f"daemon {timeout}s içinde ready olmadı (son state={last_state})",
        code=ErrorCode.NOT_READY,
    )


async def ensure_daemon(config: Config) -> str:
    """Daemon'un ayakta ve ready olduğunu garanti eder. Bearer token döner."""
    _cleanup_stale_lock(config)

    lock = read_lock(config.lock_path)
    if lock is None:
        _spawn_daemon(config)
        lock = await _wait_for_lock(config, timeout=config.ready_timeout_seconds)
    elif not owner_alive(lock):
        # tekrar kontrol et (race)
        if read_lock(config.lock_path) is not None:
            config.lock_path.unlink(missing_ok=True)
        _spawn_daemon(config)
        lock = await _wait_for_lock(config, timeout=config.ready_timeout_seconds)
    elif await _http_probe_ok(config, lock.token):
        # canlı ve HTTP'de doğrulandı — devam
        pass
    else:
        # PID canlı ama HTTP probu başarısız: retry sonrası stale kabul et
        retries = max(config.lock_probe_retries, 1)
        ok = False
        for _ in range(retries):
            if await _http_probe_ok(config, lock.token):
                ok = True
                break
            await asyncio.sleep(config.lock_probe_delay)
        if not ok:
            logger.warning("kilit sahibi PID canlı ama HTTP yanıt vermiyor — stale kabul ediliyor (pid=%s)", lock.pid)
            config.lock_path.unlink(missing_ok=True)
            _spawn_daemon(config)
            lock = await _wait_for_lock(config, timeout=config.ready_timeout_seconds)

    await _wait_until_ready(config, lock, timeout=config.ready_timeout_seconds)
    return lock.token
