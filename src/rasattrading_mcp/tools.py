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

# ---------- Modül 3 / 3.2: trading kilidi + risk politikası ----------

register_tool(
    ToolSpec(
        name="enable_real_trading",
        description=(
            "Hesabın trading kilidini kalıcı olarak `real`'e çevirir (tek yönlü; zaten real ise idempotent). "
            "Credential'sız hesapta reddedilir; değişiklik audit_log'a yazılır."
        ),
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

register_tool(
    ToolSpec(
        name="set_risk_policy",
        description=(
            "Hesap için isteğe bağlı risk politikası tanımlar: max_notional_per_order, "
            "max_aggregate_exposure, allowed_symbols. Varsayılan tamamen boş/limitsiz. "
            "Cap'ler KATI üst sınırdır — tolerans uygulanmaz, yuvarlama sonrası nihai değer `<= cap` olmalıdır."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "max_notional_per_order": {"type": "number", "exclusiveMinimum": 0},
                "max_aggregate_exposure": {"type": "number", "exclusiveMinimum": 0},
                "allowed_symbols": {"type": "array", "items": {"type": "string", "minLength": 1}},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["account_id"],
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="override_risk_policy",
        description=(
            "Tek kullanımlık, atomik risk politikası istisnası (scope='next_order'). "
            "Yalnızca kullanıcı-tanımlı cap'leri bir emir için aşmaya izin verir; temel doğruluk "
            "kontrollerini asla atlamaz. reason zorunludur, audit_log'a yazılır. "
            "Aynı idempotency_key ile retry aynı override'a bağlanır, ikincil üretmez."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "scope": {"type": "string", "enum": ["next_order"], "default": "next_order"},
                "reason": {"type": "string", "minLength": 1},
                "idempotency_key": {"type": "string", "minLength": 1},
                "expires_at": {"type": "integer", "description": "unix zaman damgası (sn)"},
                "request_id": {"type": "string"},
            },
            "required": ["account_id", "reason"],
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="get_risk_policy",
        description="Hesabın mevcut risk politikasını döner (configüre edilmemişse boş/limitsiz varsayılan).",
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

# ---------- Modül 3 / 3.3: doğruluk kontrolleri + position sizing ----------

register_tool(
    ToolSpec(
        name="get_symbol_info",
        description=(
            "Binance exchangeInfo filtrelerini döner: LOT_SIZE (step_size/min_qty/max_qty), "
            "MIN_NOTIONAL, PRICE_FILTER (tick_size/min_price/max_price) + sembol durumu. "
            "meta.freshness exchangeInfo'nun güncelliğini gösterir."
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

register_tool(
    ToolSpec(
        name="calculate_position_size",
        description=(
            "Risk-bazlı pozisyon boyutu hesaplar (base asset): account_balance * risk_pct risk tutarı, "
            "|entry - stop| risk-per-unit'e bölünür; fee düşülür; LOT_SIZE/MIN_NOTIONAL/PRICE_FILTER'e göre "
            "aşağı yuvarlanır. Borsa filtreleri karşılanamıyorsa FILTER_VIOLATION döner (fail-closed). "
            "Temel doğruluk kontrolleri her zaman aktiftir: stale fiyat / yanlış stop yönü / bilinmeyen "
            "sembol / yetersiz bakiye reddedilir."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Spot USDT çifti, örn. BTCUSDT"},
                "account_balance": {"type": "number", "exclusiveMinimum": 0, "description": "Kotasyon (USDT) bakiyesi"},
                "risk_pct": {"type": "number", "exclusiveMinimum": 0, "maximum": 1, "description": "Hesap equity yüzdesi (0.02 = %2)"},
                "entry": {"type": "number", "exclusiveMinimum": 0},
                "stop_loss": {"type": "number", "exclusiveMinimum": 0},
                "side": {"type": "string", "enum": ["BUY", "SELL"], "default": "BUY"},
                "fee_rate": {"type": "number", "minimum": 0, "default": 0.001, "description": "Komisyon oranı"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["symbol", "account_balance", "risk_pct", "entry", "stop_loss"],
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
