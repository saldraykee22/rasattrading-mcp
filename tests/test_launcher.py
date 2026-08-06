"""Launcher entegrasyon testleri: gerçek daemon subprocess'i ile.

1-1 Doğrulama Beklentisi:
- İki adapter aynı anda çalıştırılırsa tek daemon başlamalı (race yok)
- Daemon zorla öldürülüp adapter tekrar çağrılırsa stale kilit temizlenip yeni daemon başlamalı
"""

import asyncio
import socket

import psutil
import pytest

from rasattrading_mcp.adapter.launcher import ensure_daemon
from rasattrading_mcp.adapter.transport import DaemonClient
from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.lock import owner_alive, read_lock


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, port=_free_port(), pipeline_enabled=False, ready_timeout_seconds=30)


async def _daemon_pid(cfg) -> int | None:
    lock = read_lock(cfg.lock_path)
    return lock.pid if lock else None


async def _kill_daemon(cfg) -> None:
    pid = await _daemon_pid(cfg)
    if pid is None:
        return
    proc = psutil.Process(pid)
    proc.kill()
    proc.wait(timeout=15)


async def test_ensure_daemon_spawns_and_reuses(cfg):
    token1 = await ensure_daemon(cfg)
    pid1 = await _daemon_pid(cfg)
    assert pid1 is not None
    assert token1

    # İkinci çağrı aynı daemon'ı kullanır (yeni spawn yok)
    token2 = await ensure_daemon(cfg)
    pid2 = await _daemon_pid(cfg)
    assert token1 == token2
    assert pid1 == pid2
    assert owner_alive(read_lock(cfg.lock_path))

    # daemon gerçekten ready ve token ile yanıt veriyor
    client = DaemonClient(cfg, token1)
    try:
        health = await client.health()
        assert health["ok"] is True
        assert health["data"]["state"] == "ready"
    finally:
        await client.close()

    await _kill_daemon(cfg)
    assert not owner_alive(read_lock(cfg.lock_path))


async def test_two_concurrent_ensure_daemon_single_instance(cfg):
    results = await asyncio.gather(ensure_daemon(cfg), ensure_daemon(cfg))
    assert results[0] == results[1]
    pid = await _daemon_pid(cfg)
    assert pid is not None
    # Sadece bir daemon ayakta olmalı (aynı pid)
    assert pid == read_lock(cfg.lock_path).pid
    await _kill_daemon(cfg)


async def test_stale_lock_after_force_kill(cfg):
    token1 = await ensure_daemon(cfg)
    pid1 = await _daemon_pid(cfg)

    # Daemon'ı zorla öldür (sessizce kilit temizlenemez)
    await _kill_daemon(cfg)
    await asyncio.sleep(0.5)
    assert not owner_alive(read_lock(cfg.lock_path))

    # Adapter tekrar çağrılırsa stale kilit temizlenip yeni daemon başlar
    token2 = await ensure_daemon(cfg)
    pid2 = await _daemon_pid(cfg)
    assert token2 != token1  # yeni token
    assert pid2 != pid1  # yeni daemon süreci
    assert owner_alive(read_lock(cfg.lock_path))

    await _kill_daemon(cfg)
