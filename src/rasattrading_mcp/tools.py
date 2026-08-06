"""Paylaşılan tool registry.

Hem daemon (handler'ları çalıştırır) hem adapter (MCP tool listesini yansıtır)
bu registry'yi kullanır — tek doğruluk kaynağı. Modül 3 gerçek tool'ları buraya ekler.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ToolSpec:
    name: str
    description: str
    input_schema: dict  # JSON Schema object
    allowed_before_ready: bool = False


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}

    def register(self, spec: ToolSpec) -> ToolSpec:
        if spec.name in self._tools:
            raise ValueError(f"tool zaten kayıtlı: {spec.name}")
        self._tools[spec.name] = spec
        return spec

    def get(self, name: str) -> ToolSpec | None:
        return self._tools.get(name)

    def list(self) -> list[ToolSpec]:
        return list(self._tools.values())

    def names(self) -> list[str]:
        return list(self._tools)

    def __contains__(self, name: str) -> bool:
        return name in self._tools


REGISTRY = ToolRegistry()


def register_tool(spec: ToolSpec) -> ToolSpec:
    REGISTRY.register(spec)
    return spec


# ---------- örnek/Modül 1 tool'ları ----------

register_tool(
    ToolSpec(
        name="ping",
        description="Daemon ile bağlantı ve canlılık kontrolü. Tool çağrısı için `ready` gerekmez.",
        input_schema={
            "type": "object",
            "properties": {
                "request_id": {"type": "string", "description": "İsteğe özel kimlik (trace için)"},
                "idempotency_key": {"type": "string"},
            },
            "additionalProperties": False,
        },
        allowed_before_ready=True,
    )
)

register_tool(
    ToolSpec(
        name="get_readiness",
        description="Daemon hazır olma durumu: starting|migrating|warming_up|ready ve pipeline sağlığı.",
        input_schema={
            "type": "object",
            "properties": {
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "additionalProperties": False,
        },
        allowed_before_ready=True,
    )
)


def describe_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": t.name,
            "description": t.description,
            "inputSchema": t.input_schema,
        }
        for t in REGISTRY.list()
    ]
