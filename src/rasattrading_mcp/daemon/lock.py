"""Lock-file management.

The lock file guarantees that only one daemon is running and carries IPC credentials:
PID + process start time + random nonce (against PID reuse) + random bearer token.

- Initial creation is atomic (O_CREAT|O_EXCL): two daemons cannot win concurrently.
- Stale-lock recovery: remove the lock when its owner has exited (the PID is not alive).
- State updates use atomic replace (temp + os.replace), preserving token/pid/nonce.
"""

from __future__ import annotations

import json
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path

import psutil

LOCK_VERSION = 1


class LockHeldError(Exception):
    """The lock is held by another live daemon."""

    def __init__(self, existing: "LockInfo") -> None:
        super().__init__(f"daemon is already running (pid={existing.pid})")
        self.existing = existing


@dataclass
class LockInfo:
    version: int
    pid: int
    start_time: float
    nonce: str
    token: str
    state: str
    port: int
    created_at: float

    @classmethod
    def create(cls, port: int, state: str = "starting") -> "LockInfo":
        now = time.time()
        try:
            start_time = psutil.Process().create_time()
        except (psutil.Error, OSError):
            start_time = now
        return cls(
            version=LOCK_VERSION,
            pid=os.getpid(),
            start_time=start_time,
            nonce=secrets.token_hex(16),
            token=secrets.token_hex(32),
            state=state,
            port=port,
            created_at=now,
        )

    @classmethod
    def from_dict(cls, d: dict) -> "LockInfo":
        return cls(
            version=int(d["version"]),
            pid=int(d["pid"]),
            start_time=float(d["start_time"]),
            nonce=str(d["nonce"]),
            token=str(d["token"]),
            state=str(d.get("state", "starting")),
            port=int(d.get("port", 0)),
            created_at=float(d.get("created_at", 0.0)),
        )

    def to_dict(self) -> dict:
        return {
            "version": self.version,
            "pid": self.pid,
            "start_time": self.start_time,
            "nonce": self.nonce,
            "token": self.token,
            "state": self.state,
            "port": self.port,
            "created_at": self.created_at,
        }


def pid_alive(pid: int) -> bool:
    """Is the PID alive? Uses psutil to avoid os.kill's false Windows KeyboardInterrupt
    quirk and allows start_time matching to prevent PID reuse."""
    if pid <= 0:
        return False
    try:
        return psutil.pid_exists(pid)
    except (psutil.Error, OSError):
        return False


def owner_alive(info: LockInfo, start_time_tolerance: float = 2.0) -> bool:
    """Is the lock owner still the same process? PID and process start time must match."""
    if info.pid <= 0:
        return False
    if not pid_alive(info.pid):
        return False
    try:
        actual = psutil.Process(info.pid).create_time()
    except (psutil.Error, OSError):
        return False
    return abs(actual - info.start_time) < start_time_tolerance


def read_lock(path: Path) -> LockInfo | None:
    """Read the lock file. Return None if it is missing or corrupted."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return LockInfo.from_dict(data)
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _write_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_name(f"{path.name}.tmp.{secrets.token_hex(4)}")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.chmod(tmp, 0o600)
        except OSError:
            pass  # Best effort on Windows.
        # On Windows, the target may be locked briefly by a reader/AV; retry.
        last_err: Exception | None = None
        for _ in range(5):
            try:
                os.replace(tmp, path)
                return
            except PermissionError as exc:
                last_err = exc
                time.sleep(0.05)
        raise last_err  # type: ignore[misc]
    finally:
        tmp.unlink(missing_ok=True)


class LockManager:
    """Daemon-side lock acquisition, state updates, and release."""

    def __init__(self, path: Path, port: int) -> None:
        self._path = path
        self._port = port
        self._info: LockInfo | None = None

    @property
    def info(self) -> LockInfo | None:
        return self._info

    def acquire(self, max_retries: int = 5) -> LockInfo:
        """Atomically create the lock file; raise LockHeldError if another daemon is alive."""
        for _ in range(max_retries):
            existing = read_lock(self._path)

            if existing is None and self._path.exists():
                # It may be corrupted or still being written (the gap between O_EXCL
                # and writing JSON). Do not delete it while a rival may still be writing;
                # read it again several times.
                for _ in range(3):
                    time.sleep(0.02)
                    existing = read_lock(self._path)
                    if existing is not None:
                        break
                if existing is None:
                    # Still unreadable → genuinely corrupted/stale; remove it.
                    self._path.unlink(missing_ok=True)

            if existing is not None and not owner_alive(existing):
                self._path.unlink(missing_ok=True)
                continue

            if existing is not None:
                raise LockHeldError(existing)

            info = LockInfo.create(port=self._port)
            try:
                fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                continue  # A rival won; evaluate again.

            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(info.to_dict(), f)
                f.flush()
                os.fsync(f.fileno())
            self._info = info
            return info

        raise LockHeldError(read_lock(self._path)) if read_lock(self._path) else LockHeldError(
            LockInfo.create(port=self._port)
        )

    def update_state(self, state: str) -> None:
        """Update the lock file with atomic replace (preserving token/pid/nonce)."""
        if self._info is None:
            raise RuntimeError("cannot update state before acquiring the lock")
        self._info.state = state
        _write_atomic(self._path, self._info.to_dict())

    def release(self) -> None:
        """Remove the lock only if it is ours (do not touch a successor's lock file)."""
        if self._info is None:
            return
        current = read_lock(self._path)
        if current is not None and current.pid == self._info.pid and current.nonce == self._info.nonce:
            self._path.unlink(missing_ok=True)
        self._info = None
