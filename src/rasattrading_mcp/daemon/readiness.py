"""Readiness state machine: starting → migrating → warming_up → ready."""

from __future__ import annotations

import asyncio

STATES = ("starting", "migrating", "warming_up", "ready", "stopping")


class ReadinessError(Exception):
    pass


class Readiness:
    """Forward-only state machine. Its state can be queried and `ready` can be awaited."""

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
            raise ReadinessError(f"invalid state: {state}")
        # Enforce forward-only transitions (no return to starting; ready cannot be undone).
        if state in ("starting",):
            if self._state != "starting":
                raise ReadinessError(f"cannot return to state {state} (current: {self._state})")
        elif self._state not in STATES[: STATES.index(state)]:
            raise ReadinessError(f"state order violation: {self._state} -> {state}")
        if state == "ready":
            self._event.set()
        self._state = state
        self._history.append(state)

    async def wait_ready(self, timeout: float | None = None) -> bool:
        """Wait until `ready`; return False on timeout."""
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
