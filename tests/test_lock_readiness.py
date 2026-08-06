import asyncio
import os
import stat
import sys

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.lock import (
    LockHeldError,
    LockInfo,
    LockManager,
    owner_alive,
    pid_alive,
    read_lock,
)
from rasattrading_mcp.daemon.readiness import Readiness, ReadinessError


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


def test_acquire_creates_lock(cfg):
    mgr = LockManager(cfg.lock_path, cfg.port)
    info = mgr.acquire()
    assert info.pid == os.getpid()
    assert info.token and info.nonce
    assert info.state == "starting"
    assert cfg.lock_path.exists()
    if sys.platform != "win32":
        mode = stat.S_IMODE(os.stat(cfg.lock_path).st_mode)
        assert not (mode & stat.S_IRWXO)


def test_second_acquire_while_alive_raises(cfg):
    mgr1 = LockManager(cfg.lock_path, cfg.port)
    mgr1.acquire()
    mgr2 = LockManager(cfg.lock_path, cfg.port)
    with pytest.raises(LockHeldError) as exc:
        mgr2.acquire()
    assert exc.value.existing.pid == os.getpid()


def test_stale_lock_recovered(cfg):
    info = LockInfo.create(port=cfg.port)
    info.pid = 999_999_999  # muhtemelen ölü PID
    info.state = "ready"
    cfg.lock_path.write_text(_json(info))
    assert not pid_alive(info.pid)

    mgr = LockManager(cfg.lock_path, cfg.port)
    new_info = mgr.acquire()
    assert new_info.pid == os.getpid()
    assert read_lock(cfg.lock_path).pid == os.getpid()


def test_corrupt_lock_treated_as_stale(cfg):
    cfg.lock_path.write_text("{bozuk-json")
    mgr = LockManager(cfg.lock_path, cfg.port)
    info = mgr.acquire()
    assert info.pid == os.getpid()


def test_update_state_preserves_identity(cfg):
    mgr = LockManager(cfg.lock_path, cfg.port)
    info = mgr.acquire()
    mgr.update_state("migrating")
    current = read_lock(cfg.lock_path)
    assert current.state == "migrating"
    assert current.token == info.token
    assert current.nonce == info.nonce
    assert current.pid == info.pid


def test_release_only_removes_own_lock(cfg):
    mgr = LockManager(cfg.lock_path, cfg.port)
    mgr.acquire()
    mgr.release()
    assert not cfg.lock_path.exists()


def test_release_does_not_remove_others(cfg):
    mgr = LockManager(cfg.lock_path, cfg.port)
    info = mgr.acquire()
    other = LockInfo.create(port=cfg.port)
    cfg.lock_path.write_text(_json(other))  # halef el koydu
    mgr.release()
    current = read_lock(cfg.lock_path)
    assert current is not None
    assert current.nonce == other.nonce


def test_owner_alive_pid_reuse_detected(cfg):
    """Aynı PID ama farklı start_time -> sahibi değil (PID reuse koruması)."""
    info = LockInfo.create(port=cfg.port)
    assert owner_alive(info) is True  # bizim kendi sürecimiz

    info.start_time = info.start_time - 10_000  # eski bir process başlangıcı
    assert owner_alive(info) is False


def _json(info: LockInfo) -> str:
    import json

    return json.dumps(info.to_dict())


# ---------- readiness ----------

def test_readiness_forward_transitions():
    r = Readiness()
    assert r.state == "starting"
    r.set_state("migrating")
    r.set_state("warming_up")
    r.set_state("ready")
    assert r.is_ready()
    assert r.history == ["starting", "migrating", "warming_up", "ready"]


def test_readiness_backward_raises():
    r = Readiness()
    r.set_state("migrating")
    with pytest.raises(ReadinessError):
        r.set_state("starting")
    r.set_state("warming_up")
    r.set_state("ready")
    with pytest.raises(ReadinessError):
        r.set_state("warming_up")


def test_readiness_invalid_state_raises():
    r = Readiness()
    with pytest.raises(ReadinessError):
        r.set_state("bogus")


async def test_wait_ready_timeout():
    r = Readiness()
    assert await r.wait_ready(timeout=0.05) is False
    r.set_state("migrating")
    r.set_state("warming_up")
    assert await r.wait_ready(timeout=0.05) is False
    r.set_state("ready")
    assert await r.wait_ready(timeout=0.5) is True
