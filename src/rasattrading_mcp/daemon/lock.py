"""Kilit dosyası yönetimi.

Kilit dosyası tek bir daemon'un ayakta olduğunu garanti eder ve IPC kimlik bilgilerini taşır:
PID + process başlangıç zamanı + rastgele nonce (PID reuse'a karşı) + rastgele bearer token.

- İlk oluşturma atomiktir (O_CREAT|O_EXCL): aynı anda iki daemon kazanamaz (race yok).
- Stale-lock recovery: sahibi ölmüş (PID canlı değil) ise temizlenir.
- State güncellemeleri atomic replace (temp + os.replace) ile yapılır, token/pid/nonce korunur.
"""

from __future__ import annotations

import json
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path

LOCK_VERSION = 1


class LockHeldError(Exception):
    """Kilit başka bir canlı daemon tarafından tutuluyor."""

    def __init__(self, existing: "LockInfo") -> None:
        super().__init__(f"daemon zaten çalışıyor (pid={existing.pid})")
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
        return cls(
            version=LOCK_VERSION,
            pid=os.getpid(),
            start_time=now,
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
    """PID canlı mı? (liveness probe, process'i öldürmez)"""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def read_lock(path: Path) -> LockInfo | None:
    """Kilit dosyasını okur. Yoksa veya bozuksa None döner."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return LockInfo.from_dict(data)
    except (OSError, ValueError, KeyError, TypeError):
        return None


def _write_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_name(f"{path.name}.tmp.{secrets.token_hex(4)}")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
        f.flush()
        os.fsync(f.fileno())
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass  # Windows'ta best-effort
    os.replace(tmp, path)


class LockManager:
    """Daemon tarafı: kilidi alır, state günceller, bırakır."""

    def __init__(self, path: Path, port: int) -> None:
        self._path = path
        self._port = port
        self._info: LockInfo | None = None

    @property
    def info(self) -> LockInfo | None:
        return self._info

    def acquire(self, max_retries: int = 3) -> LockInfo:
        """Kilit dosyasını atomik oluşturur. Başka canlı daemon varsa LockHeldError fırlatır."""
        for _ in range(max_retries):
            existing = read_lock(self._path)
            if existing is not None and not pid_alive(existing.pid):
                self._path.unlink(missing_ok=True)
            elif existing is not None:
                raise LockHeldError(existing)

            info = LockInfo.create(port=self._port)
            try:
                fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                continue  # bir rakip kazandı; tekrar değerlendir

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
        """Kilit dosyasını atomic replace ile günceller (token/pid/nonce korunur)."""
        if self._info is None:
            raise RuntimeError("lock alınmadan state güncellenemez")
        self._info.state = state
        _write_atomic(self._path, self._info.to_dict())

    def release(self) -> None:
        """Yalnızca bizim kilidimizse siler (halefin kilit dosyasına dokunmaz)."""
        if self._info is None:
            return
        current = read_lock(self._path)
        if current is not None and current.pid == self._info.pid and current.nonce == self._info.nonce:
            self._path.unlink(missing_ok=True)
        self._info = None
