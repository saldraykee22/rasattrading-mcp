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

register_tool(
    ToolSpec(
        name="get_candles",
        description=(
            "Ham OHLCV mum verisi. Sembol sabit izlenen timeframe setinde ise (15m/1h/4h/1d) "
            "öncelikli warm-up yapılır; değilse istek anında canlı çekilir. "
            "meta.freshness son barın güncelliğini gösterir."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Spot USDT çifti, örn. BTCUSDT"},
                "timeframe": {"type": "string", "description": "1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 300},
                "source": {"type": "string", "enum": ["spot", "futures"], "default": "spot"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["symbol", "timeframe"],
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="get_ticker",
        description=(
            "Sembolün anlık 24h ticker'ı (miniTicker WS'ten). meta.freshness verinin güncel olup "
            "olmadığını söyler; WS kopuksa stale döner."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Spot USDT çifti, örn. BTCUSDT"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["symbol"],
            "additionalProperties": False,
        },
    )
)

# ---------- Modül 3 / 3.1: account + credential CRUD ----------

register_tool(
    ToolSpec(
        name="add_account",
        description=(
            "Spot hesap ekler. api_key/api_secret verilmezse hesap public/read-only modda oluşturulur; "
            "secret alanlar hiçbir cevapta döndürülmez."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "label": {"type": "string", "minLength": 1, "maxLength": 200},
                "api_key": {"type": "string", "minLength": 1},
                "api_secret": {"type": "string", "minLength": 1},
                "tags": {"type": "array", "items": {"type": "string"}, "default": []},
                "market": {"type": "string", "enum": ["spot"], "default": "spot"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["label"],
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="list_accounts",
        description=(
            "Hesapları secret içermeyen özetlerle listeler. credentials_configured/read_only alanları "
            "hesapta API anahtarı bulunup bulunmadığını gösterir."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="remove_account",
        description="Hesabı ve bağlı credential kayıtlarını kaldırır; işlem audit log'a yazılır.",
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["account_id"],
            "additionalProperties": False,
        },
    )
)


# ---------- Modül 2 / 2.4: PA tool'ları + annotation ----------


def _pa_schema(extra: dict) -> dict:
    base = {
        "symbol": {"type": "string", "description": "Spot USDT çifti, örn. BTCUSDT"},
        "timeframe": {"type": "string", "description": "1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M"},
        "lookback": {"type": "integer", "minimum": 20, "maximum": 1000, "default": 200},
        "request_id": {"type": "string"},
        "idempotency_key": {"type": "string"},
    }
    base.update(extra)
    return {"type": "object", "properties": base, "required": ["symbol", "timeframe"], "additionalProperties": False}


register_tool(
    ToolSpec(
        name="get_market_structure",
        description=(
            "Swing High/Low + BOS/CHoCH yapısı: trend, swing'ler (HH/LH/HL/LL) ve yapı kırılım "
            "olayları. meta.algo_version algoritma sürümünü taşır."
        ),
        input_schema=_pa_schema({}),
    )
)

register_tool(
    ToolSpec(
        name="get_liquidity_zones",
        description=(
            "Equal highs/lows likidite bölgeleri + sweep/mitigasyon durumu + futures tabanlı "
            "likidite skoru. Varsayılan yalnızca aktif (mitigasyonsuz) bölgeleri döner; "
            "include_mitigated=true ile depolanan tarihçenin tamamı döner."
        ),
        input_schema=_pa_schema({"include_mitigated": {"type": "boolean", "default": False}}),
    )
)

register_tool(
    ToolSpec(
        name="get_order_blocks",
        description=(
            "BOS/CHoCH sonrası order block'lar (order_block|breaker|mitigation_block) + FVG'ler. "
            "Varsayılan yalnızca aktif bölgeler; include_mitigated=true ile tam tarihçe."
        ),
        input_schema=_pa_schema({"include_mitigated": {"type": "boolean", "default": False}}),
    )
)

register_tool(
    ToolSpec(
        name="get_full_analysis",
        description=(
            "Tek çağrıda tüm PA özeti: yapı + likidite + order block/FVG + VWAP + session "
            "seviyeleri. Context şişmesin diye vwap noktaları sınırlıdır."
        ),
        input_schema=_pa_schema({"include_mitigated": {"type": "boolean", "default": False}}),
    )
)

register_tool(
    ToolSpec(
        name="annotate_chart",
        description=(
            "Sembol+timeframe'e agent işaretlemesi ekler (örn. {level, label, kind}). "
            "Hesaplamaya etkisi yoktur, kalıcı kaydedilir."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "timeframe": {"type": "string"},
                "annotations": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "İşaretleme listesi (tek nesne de kabul edilir)",
                },
                "created_by": {"type": "string", "default": "agent"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["symbol", "timeframe", "annotations"],
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="get_chart_annotations",
        description="Sembol+timeframe'in kayıtlı işaretlemelerini döner.",
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "timeframe": {"type": "string"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["symbol", "timeframe"],
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="clear_annotations",
        description="Sembol+timeframe'in tüm işaretlemelerini siler; silinen sayıyı döner.",
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "timeframe": {"type": "string"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["symbol", "timeframe"],
            "additionalProperties": False,
        },
    )
)


register_tool(
    ToolSpec(
        name="scan_market",
        description=(
            "Sembol evrenini allowlisted filtre AST'si ile tarar (serbest SQL değil). "
            "Filtre türleri: volume_change, price_change, structure_event, "
            "liquidity_sweep_occurred, near_order_block, funding_rate, oi_change, "
            "above_below_vwap; and/or düğümleriyle iç içe kullanılabilir. "
            "Sonuç veri güncelliğini (freshness) ve stale sembolleri açıkça taşır."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "filters": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Filtre AST'si, örn. [{\"type\": \"price_change\", \"min\": 3}]",
                },
                "combine": {"type": "string", "enum": ["AND", "OR"], "default": "AND"},
                "sort_by": {"type": "string", "enum": ["symbol", "price_change", "volume_change"], "default": "symbol"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 250, "default": 50},
                "cursor": {"type": "integer", "description": "Sayfalama imleci (next_cursor ile döner)"},
                "timeframe": {"type": "string", "default": "1h"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["filters"],
            "additionalProperties": False,
        },
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
