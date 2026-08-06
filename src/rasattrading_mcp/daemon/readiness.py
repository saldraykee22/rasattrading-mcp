"""Readiness state machine: starting → migrating → warming_up → ready."""

from __future__ import annotations

import asyncio
from typing import Optional

STATES = ("starting", "migrating", "warming_up", "ready", "stopping")


class ReadinessError(Exception):
    pass


class Readiness:
    """İleri yönlü (forward-only) state machine. State sorgulanabilir, `ready` beklene bilir."""

    def __init__(self) -> None:
        self._state = "starting"
        self._event = asyncio.Event()
        self._history: list[str] = ["starting"]

    @property
    def state(self) -> str:
        return self._state

    @property
    def history(self) -> list[str]:
        return list(self._history)

    def is_ready(self) -> bool:
        return self._state == "ready"

    def set_state(self, state: str) -> None:
        if state not in STATES:
            raise ReadinessError(f"geçersiz state: {state}")
        # İleri yön kontrolü (starting dışına dönüş yasak, ready geri alınamaz)
        if state in ("starting",):
            if self._state != "starting":
                raise ReadinessError(f"{state} durumuna geri dönülemez (mevcut: {self._state})")
        elif self._state not in STATES[: STATES.index(state)]:
            raise ReadinessError(f"durum sırası ihlali: {self._state} -> {state}")
        if state == "ready":
            self._event.set()
        self._state = state
        self._history.append(state)

    async def wait_ready(self, timeout: float | None = None) -> bool:
        """`ready` olana kadar bekler. Timeout'ta False döner."""
        if self.is_ready():
            return True
        try:
            if timeout is not None:
                await asyncio.wait_for(self._event.wait(), timeout=timeout)
            else:
                await self._event.wait()
        except asyncio.TimeoutError:
            return False
        return self.is_ready()
