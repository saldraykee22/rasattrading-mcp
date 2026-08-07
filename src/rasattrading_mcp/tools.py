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
            "Cap'ler KATI üst sınırdır — tolerans uygulanmaz, yuvarlama sonrası nihai değer `<= cap` olmalıdır. "
            "Patch'te gönderilmeyen değerler korunur; temizleme yalnızca açık `clear_max_notional`, "
            "`clear_max_exposure` veya `clear_allowed_symbols` boolean'larıyla yapılır."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "max_notional_per_order": {"type": "number", "exclusiveMinimum": 0},
                "max_aggregate_exposure": {"type": "number", "exclusiveMinimum": 0},
                "allowed_symbols": {"type": "array", "items": {"type": "string", "minLength": 1}},
                "clear_max_notional": {"type": "boolean", "description": "max_notional_per_order değerini temizle"},
                "clear_max_exposure": {"type": "boolean", "description": "max_aggregate_exposure değerini temizle"},
                "clear_allowed_symbols": {"type": "boolean", "description": "allowed_symbols listesini temizle"},
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

# ---------- Modül 3 / 3.4: emir yürütme ----------

register_tool(
    ToolSpec(
        name="execute_on_accounts",
        description=(
            "Birden çok hesapta pozisyon açar. account_ids + tags birlikte verilirse UNION'dur; "
            "ikisi de boşsa reddedilir. Emir boyutu daemon'ın kendi taze bakiye/equity/fiyat "
            "snapshot'ından hesaplanır (agent rakamlarına güvenilmez). Idempotency: aynı "
            "idempotency_key retry'i çift emir üretmez. Kısmi başarı: hesap başına ayrı sonuç döner. "
            "Temel doğruluk kontrolleri (bakiye/stale/stop yönü/sembol) her zaman aktiftir."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_ids": {"type": "array", "items": {"type": "string", "minLength": 1}, "description": "Hedef account_id'ler (tags ile UNION)"},
                "tags": {"type": "array", "items": {"type": "string", "minLength": 1}, "description": "Hedef tag'ler (account_ids ile UNION)"},
                "symbol": {"type": "string", "description": "Spot USDT çifti, örn. BTCUSDT"},
                "side": {"type": "string", "enum": ["BUY", "SELL"], "default": "BUY"},
                "entry": {"type": "number", "exclusiveMinimum": 0},
                "stop_loss": {"type": "number", "exclusiveMinimum": 0},
                "risk_pct": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
                "order_type": {"type": "string", "enum": ["MARKET", "LIMIT"], "default": "MARKET"},
                "idempotency_key": {"type": "string", "minLength": 1},
                "request_id": {"type": "string"},
            },
            "required": ["symbol", "side", "entry", "stop_loss", "risk_pct", "idempotency_key"],
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="place_order",
        description=(
            "Tek hesapta doğrudan emir gönderir (order_type: MARKET|LIMIT|STOP_LOSS_LIMIT, miktar base asset). "
            "STOP_LOSS_LIMIT spot stop korumasıdır: stop_price'a ulaşınca price seviyesinde LIMIT satış tetiklenir "
            "(pozisyonu borsada korur, daemon kapalı olsa bile). Aynı idempotency_key ile retry çift emir üretmez; "
            "ağ zaman aşımında Binance'ten gerçek durum reconcile edilir. Temel doğruluk kontrolleri her zaman aktiftir."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "symbol": {"type": "string", "description": "Spot USDT çifti, örn. ALICEUSDT"},
                "side": {"type": "string", "enum": ["BUY", "SELL"]},
                "order_type": {"type": "string", "enum": ["MARKET", "LIMIT", "STOP_LOSS_LIMIT"], "default": "MARKET"},
                "quantity": {"type": "number", "exclusiveMinimum": 0, "description": "Base asset miktarı"},
                "price": {"type": "number", "exclusiveMinimum": 0, "description": "LIMIT/STOP_LOSS_LIMIT için zorunlu (stop tetiklenince satılacak fiyat)"},
                "stop_price": {"type": "number", "exclusiveMinimum": 0, "description": "STOP_LOSS_LIMIT için zorunlu (stop tetikleme seviyesi)"},
                "idempotency_key": {"type": "string", "minLength": 1},
                "request_id": {"type": "string"},
            },
            "required": ["account_id", "symbol", "side", "quantity", "idempotency_key"],
            "additionalProperties": False,
        },
    )
)

# ---------- Modül 3 / 3.5: kill switch + exposure + audit ----------

register_tool(
    ToolSpec(
        name="place_oco_order",
        description=(
            "Spot OCO emri: kâr hedefi (LIMIT) + stop (STOP_LOSS_LIMIT) TEK emir listesinde. "
            "Biri dolunca diğeri borsada otomatik iptal olur (true OCO). Aynı pozisyon için "
            "ayrı ayrı SL+TP emri bakiyeyi birbirinden çaldığı için imkânsızdır; bu tool ikisini "
            "tek `orderList/oco` çağrısında taşır. price=TP, stop_price=stop tetikleme, "
            "stop_limit_price=stop tetiklenince satılacak limit (stop_price'dan düşük olmalı)."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "symbol": {"type": "string", "description": "Spot USDT çifti, örn. ALICEUSDT"},
                "side": {"type": "string", "enum": ["BUY", "SELL"]},
                "quantity": {"type": "number", "exclusiveMinimum": 0, "description": "Base asset miktarı"},
                "price": {"type": "number", "exclusiveMinimum": 0, "description": "Kâr hedefi (limit) fiyatı"},
                "stop_price": {"type": "number", "exclusiveMinimum": 0, "description": "Stop tetikleme seviyesi"},
                "stop_limit_price": {"type": "number", "exclusiveMinimum": 0, "description": "Stop tetiklenince satılacak limit fiyatı (< stop_price)"},
                "idempotency_key": {"type": "string", "minLength": 1},
                "request_id": {"type": "string"},
            },
            "required": ["account_id", "symbol", "side", "quantity", "price", "stop_price", "stop_limit_price", "idempotency_key"],
            "additionalProperties": False,
        },
    )
)

register_tool(
    ToolSpec(
        name="close_all_positions",
        description=(
            "Hesabın (veya account_id='all' ise tüm hesapların) açık emirlerini iptal edip "
            "base asset bakiyelerini market fiyatından satar. Kısmi başarıda hangi hesabın "
            "kapandığı/kapanamadığı açıkça raporlanır; idempotenttir (tekrar çalıştırma çift "
            "satış yapmaz). Paper hesapta gerçek bakiye satışı yapılmaz; yanıt `closed=false`, "
            "`simulated=true` ve `position_close_supported=false` ile yalnızca yerel emir iptalini belirtir. "
            "İptal geçişleri audit_log'a yazılır."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Hedef account_id veya 'all'"},
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
        name="disable_real_trading",
        description=(
            "Kill switch: hesabın (veya 'all') trading kilidini `real`'den `paper`'a çevirir; "
            "yeni emirler gönderilmez. Audit log'a yazılır; zaten paper ise idempotent."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Hedef account_id veya 'all'"},
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
        name="get_total_exposure",
        description=(
            "Tüm hesapların toplam exposure'ını döner: sembol bazlı (açık emir notional + base "
            "bakiye değeri, daemon'ın taze fiyatıyla) ve hesap bazlı özet."
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
        name="get_account_balance",
        description=(
            "Hesabın tam bakiye görünümü: free (serbest), locked (açık emirlerde kilitli), "
            "holdings_value (elde tutulan base asset'lerin güncel piyasa değeri) ve "
            "total/equity (free + locked + holdings_value). Sadece serbest bakiyeyi değil, "
            "hesabın gerçek toplam değerini döner — daemon'ın kendi taze bakiye/fiyat snapshot'ıyla."
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
        name="get_audit_log",
        description=(
            "Hash-chain doğrulamalı audit log sorgusu. verified=true ise zincir sağlam; "
            "değilse broken kırık satırları içerir."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
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
            "include_mitigated=true ile depolanan tarihçenin tamamı döner. Skorun equal_levels "
            "bileşeni aktif bölge sayısına göre puanlanır (mitigasyonlular puan getirmez); "
            "funding bileşeni `bias: long_crowded|short_crowded` taşır. "
            "NOT: include_mitigated=true tarihçe, eski (2.15 öncesi) semantiğe göre "
            "mitigated=false kalmış breaker kayıtlarını da içerebilir — bu beklenen "
            "immutable-tarihçe davranışıdır, listedeki her bölge 'aktif' değildir; "
            "aktif görünüm varsayılan çağrıdır."
        ),
        input_schema=_pa_schema({"include_mitigated": {"type": "boolean", "default": False}}),
    )
)

register_tool(
    ToolSpec(
        name="get_order_blocks",
        description=(
            "BOS/CHoCH sonrası order block'lar (order_block|breaker|mitigation_block) + FVG'ler. "
            "Varsayılan yalnızca aktif bölgeler; include_mitigated=true ile tam tarihçe. "
            "Breaker'lar kapanışla kırılmış OB olduğu için mitigated=true taşır; aynı/çok yakın "
            "fiyat aralığındaki OB'ler tek mantıksal bölgede birleştirilir (2.15). "
            "NOT: include_mitigated=true tarihçe, eski semantiğe göre mitigated=false kalmış "
            "breaker kayıtlarını da içerebilir (immutable geçmişin üzerine yazılmaz) — "
            "listedeki her bölge 'aktif' değildir; aktif görünüm varsayılan çağrıdır."
        ),
        input_schema=_pa_schema({"include_mitigated": {"type": "boolean", "default": False}}),
    )
)

register_tool(
    ToolSpec(
        name="get_full_analysis",
        description=(
            "Tek çağrıda tüm PA özeti: yapı + likidite + order block/FVG + VWAP + session "
            "seviyeleri. Context şişmesin diye vwap noktaları sınırlıdır. meta.algo_version ve "
            "data.algo_version tüm bileşen sürümlerini taşır; data.versions her bileşeni ayrı "
            "verir. Likidite skorunun equal_levels açıklamasındaki zones toplamıdır; aktif "
            "(mitigasyonsuz) sayı varsayılan zones listesiyle birebir örtüşür."
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
            "Her satır `data_stale` (PA tazeliği) YANINDA `symbol_valid` (evrende "
            "işlem yapılabilir mi — delist olmayan), `matched_filters` (hangi "
            "filtre(ler) eşleşti) ve `signal_summary` (eşleşmeyi tetikleyen ham "
            "değerler) taşır (2.16). `data_stale=false` tek başına sembolün "
            "işlem yapılabilir olduğu anlamına gelmez; `symbol_valid` kontrol edin."
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


# ---------- Modül 2 / 2.6: Alarm motoru ----------


def _alarm_schema(extra: dict) -> dict:
    base = {
        "request_id": {"type": "string"},
        "idempotency_key": {"type": "string"},
    }
    base.update(extra)
    return {"type": "object", "properties": base, "additionalProperties": False}


register_tool(
    ToolSpec(
        name="create_alert",
        description=(
            "Tek sembol+timeframe için koşullu alarm tanımlar. Koşul, scan_market ile aynı "
            "allowlisted filtre AST'sidir. State machine: armed→triggered→cooldown→armed; "
            "aynı veri penceresi tekrar tetiklenmez (dedup)."
        ),
        input_schema=_alarm_schema(
            {
                "symbol": {"type": "string"},
                "timeframe": {"type": "string"},
                "condition": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Filtre AST'si (scan_market ile aynı türler)",
                },
                "cooldown_seconds": {"type": "integer", "minimum": 0, "default": 300},
                "note": {"type": "string"},
                "order_spec": {
                    "type": "object",
                    "description": (
                        "Opsiyonel: alarm tetiklenince onay bekleyen emir kaydı oluşturur "
                        "(awaiting_approval). Emir OTOMATİK açılmaz — approve_pending_order gerekir. "
                        "Alanlar: account_id (zorunlu), symbol (zorunlu), side (zorunlu, BUY|SELL), "
                        "order_type (market|limit), risk_pct (ZORUNLU, (0,1] — boyutlandırma için), "
                        "entry (order_type=limit ise zorunlu; sonlu sayı), stop_loss (sonlu sayı). "
                        "Sayılar sonlu olmalıdır (NaN/Infinity kabul edilmez)."
                    ),
                    "properties": {
                        "account_id": {"type": "string", "minLength": 1},
                        "symbol": {"type": "string", "minLength": 1},
                        "side": {"type": "string", "enum": ["BUY", "SELL"]},
                        "order_type": {"type": "string", "enum": ["market", "limit"], "default": "market"},
                        "entry": {"type": "number", "exclusiveMinimum": 0},
                        "stop_loss": {"type": "number", "exclusiveMinimum": 0},
                        "risk_pct": {"type": "number", "exclusiveMinimum": 0, "maximum": 1},
                    },
                    "required": ["account_id", "symbol", "side", "risk_pct"],
                },
            }
        ),
    )
)

register_tool(
    ToolSpec(
        name="create_composite_alert",
        description=(
            "Birden çok clause'ı AND/OR ile birleştiren alarm. Her clause bir "
            "(symbol,timeframe) çifti + koşul taşır; tüm clause'ların verisi taze "
            "olmadan değerlendirilmez (stale → tetiklenmez)."
        ),
        input_schema=_alarm_schema(
            {
                "clauses": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "symbol": {"type": "string"},
                            "timeframe": {"type": "string"},
                            "filters": {"type": "array", "items": {"type": "object"}},
                        },
                        "required": ["symbol", "timeframe", "filters"],
                    },
                },
                "combine": {"type": "string", "enum": ["AND", "OR"], "default": "AND"},
                "cooldown_seconds": {"type": "integer", "minimum": 0, "default": 300},
                "note": {"type": "string"},
            }
        ),
    )
)

register_tool(
    ToolSpec(
        name="list_alerts",
        description="Tüm alarm tanımlarını durumlarıyla (armed/triggered) listeler.",
        input_schema=_alarm_schema({}),
    )
)

register_tool(
    ToolSpec(
        name="delete_alert",
        description="Alarm tanımını siler.",
        input_schema=_alarm_schema({"alert_id": {"type": "string", "minLength": 1}}),
    )
)

register_tool(
    ToolSpec(
        name="get_triggered_alerts",
        description=(
            "Kalıcı tetiklenme kayıtlarını döner (agent kapalıyken tetiklenenler kaybolmaz). "
            "İsteğe bağlı alert_id filtresi + sayfalama."
        ),
        input_schema=_alarm_schema(
            {
                "alert_id": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50},
                "cursor": {"type": "integer"},
            }
        ),
    )
)

register_tool(
    ToolSpec(
        name="get_pending_orders",
        description=(
            "Onay bekleyen emir kayıtlarını listeler (alarm order_spec'i tetiklenince "
            "awaiting_approval kaydı düşer). status filtresi: awaiting_approval|approved|"
            "executing|rejected|executed|reconcile_required|expired. Emirler otomatik "
            "açılmaz — approve_pending_order gerekir."
        ),
        input_schema=_alarm_schema(
            {
                "status": {
                    "type": "string",
                    "enum": [
                        "awaiting_approval",
                        "approved",
                        "executing",
                        "rejected",
                        "executed",
                        "reconcile_required",
                        "expired",
                    ],
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 50},
            }
        ),
    )
)

register_tool(
    ToolSpec(
        name="approve_pending_order",
        description=(
            "Onay bekleyen emri onaylar ve GERÇEK emir olarak açar. Boyutlandırma daemon "
            "tarafında yapılır (risk_pct x hesap equity'si + sembol filtreleri). "
            "Idempotency: pending:<order_id> key'iyle retry çift emir üretmez. "
            "Bu işlem gerçek para kullanır — yalnızca kullanıcının açık onayıyla çağrılmalı."
        ),
        input_schema=_alarm_schema({"order_id": {"type": "string", "minLength": 1}}),
    )
)

register_tool(
    ToolSpec(
        name="reject_pending_order",
        description="Onay bekleyen emri reddeder (emir açılmaz).",
        input_schema=_alarm_schema(
            {"order_id": {"type": "string", "minLength": 1}, "reason": {"type": "string"}}
        ),
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
