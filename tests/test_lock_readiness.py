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
    info.pid = 999_999_999  # Probably dead PID.
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
    cfg.lock_path.write_text(_json(other))  # successor took ownership
    mgr.release()
    current = read_lock(cfg.lock_path)
    assert current is not None
    assert current.nonce == other.nonce


def test_owner_alive_pid_reuse_detected(cfg):
    """Same PID but different start_time → not owner (PID reuse protection)."""
    info = LockInfo.create(port=cfg.port)
    assert owner_alive(info) is True  # Our own process.

    info.start_time = info.start_time - 10_000  # Old process start.
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


# ---------- daemon signal fallback (Windows) ----------


async def test_run_daemon_signal_fallback_on_windows(tmp_path, monkeypatch):
    """add_signal_handler NotImplementedError (Windows) → install signal.signal fallback."""
    from rasattrading_mcp.daemon import main as dm

    class _FakeLoop:
        def add_signal_handler(self, sig, cb):
            raise NotImplementedError("Windows signal handler is not supported")

        def call_soon_threadsafe(self, cb):
            cb()

    monkeypatch.setattr(dm.asyncio, "get_running_loop", lambda: _FakeLoop())

    installed = []
    monkeypatch.setattr(dm.signal, "signal", lambda sig, cb: installed.append((sig, cb)))

    async def _noop_start(self):
        return None

    async def _noop_run(self):
        return 0

    monkeypatch.setattr(dm.DaemonRunner, "start", _noop_start)
    monkeypatch.setattr(dm.DaemonRunner, "run", _noop_run)

    result = await dm.run_daemon(Config(data_dir=tmp_path, pipeline_enabled=False))
    assert result == 0
    # Install signal.signal fallback for SIGINT + SIGTERM (instead of break).
    assert len(installed) == 2
    sigs = {s for s, _ in installed}
    assert dm.signal.SIGINT in sigs
    assert dm.signal.SIGTERM in sigs


async def test_run_daemon_signal_fallback_handler_requests_stop(tmp_path, monkeypatch):
    """When the fallback handler fires, request_stop is sent to the loop thread."""
    from rasattrading_mcp.daemon import main as dm

    class _FakeLoop:
        def add_signal_handler(self, sig, cb):
            raise NotImplementedError("win")

        def call_soon_threadsafe(self, cb):
            cb()

    monkeypatch.setattr(dm.asyncio, "get_running_loop", lambda: _FakeLoop())

    captured = []
    monkeypatch.setattr(dm.signal, "signal", lambda sig, cb: captured.append((sig, cb)))

    async def _noop_start(self):
        return None

    async def _noop_run(self):
        return 0

    stop_calls = []
    monkeypatch.setattr(dm.DaemonRunner, "start", _noop_start)
    monkeypatch.setattr(dm.DaemonRunner, "run", _noop_run)
    monkeypatch.setattr(dm.DaemonRunner, "request_stop", lambda self: stop_calls.append(self))

    assert await dm.run_daemon(Config(data_dir=tmp_path, pipeline_enabled=False)) == 0
    # Trigger fallback handler manually → deliver to request_stop loop thread.
    assert captured
    handler = captured[0][1]
    handler(dm.signal.SIGINT, None)
    assert len(stop_calls) == 1
