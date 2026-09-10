"""Shared tool registry.

Both the daemon (which runs handlers) and adapter (which mirrors the MCP tool list)
use this registry — the single source of truth.
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
            raise ValueError(f"tool is already registered: {spec.name}")
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


# ---------- core tools ----------

register_tool(
    ToolSpec(
        name="ping",
        description="Checks daemon connectivity and liveness. The daemon does not need to be `ready` for this tool call.",
        input_schema={
            "type": "object",
            "properties": {
                "request_id": {"type": "string", "description": "Request-specific identifier (for tracing)"},
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
        description="Daemon readiness state: starting|migrating|warming_up|ready, plus pipeline health.",
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
            "Raw OHLCV candle data. If the symbol is in the continuously monitored timeframe set (15m/1h/4h/1d), it receives priority warm-up; otherwise it is fetched live when requested. `meta.freshness` indicates the freshness of the latest candle."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Spot USDT pair, e.g. BTCUSDT"},
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
            "The symbol's current 24h ticker (from the miniTicker WebSocket). `meta.freshness` indicates whether the data is current; a disconnected WebSocket returns stale data."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Spot USDT pair, e.g. BTCUSDT"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["symbol"],
            "additionalProperties": False,
        },
    )
)

# ---------- account and credential management ----------

register_tool(
    ToolSpec(
        name="add_account",
        description=(
            "Adds a Spot account. If api_key/api_secret are omitted, the account is created in public/read-only mode; secret fields are never returned in any response."
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
            "Lists accounts using summaries that contain no secrets. The credentials_configured/read_only fields indicate whether API credentials are present for the account."
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
        description="Removes the account and its associated credential records; the operation is written to the audit log.",
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

# ---------- trading lock and risk policy ----------

register_tool(
    ToolSpec(
        name="enable_real_trading",
        description=(
            "Permanently changes the account's trading lock to `real` (one-way; idempotent if it is already real). Rejected for accounts without credentials; the change is written to the audit log."
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
            "Defines an optional risk policy for the account: max_notional_per_order, max_aggregate_exposure, and allowed_symbols. The default is completely empty/unlimited. Caps are HARD upper bounds—no tolerance is applied, and the final value after rounding must be `<= cap`. Values omitted from a patch are preserved; clearing is possible only with the explicit `clear_max_notional`, `clear_max_exposure`, or `clear_allowed_symbols` booleans."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "max_notional_per_order": {"type": "number", "exclusiveMinimum": 0},
                "max_aggregate_exposure": {"type": "number", "exclusiveMinimum": 0},
                "allowed_symbols": {"type": "array", "items": {"type": "string", "minLength": 1}},
                "clear_max_notional": {"type": "boolean", "description": "Clear the max_notional_per_order value"},
                "clear_max_exposure": {"type": "boolean", "description": "Clear the max_aggregate_exposure value"},
                "clear_allowed_symbols": {"type": "boolean", "description": "Clear the allowed_symbols list"},
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
            "One-time, atomic risk-policy exception (`scope='next_order'`). It only permits exceeding user-defined caps for one order and never bypasses the core correctness checks. `reason` is required and is written to the audit log. A retry with the same idempotency_key attaches to the same override and does not create another one."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "scope": {"type": "string", "enum": ["next_order"], "default": "next_order"},
                "reason": {"type": "string", "minLength": 1},
                "idempotency_key": {"type": "string", "minLength": 1},
                "expires_at": {"type": "integer", "description": "Unix timestamp (seconds)"},
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
        description="Returns the account's current risk policy (an empty/unlimited default if none is configured).",
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

# ---------- accuracy checks and position sizing ----------

register_tool(
    ToolSpec(
        name="get_symbol_info",
        description=(
            "Returns Binance exchangeInfo filters: LOT_SIZE (step_size/min_qty/max_qty), MIN_NOTIONAL, PRICE_FILTER (tick_size/min_price/max_price), plus symbol status. `meta.freshness` indicates how current the exchangeInfo data is."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Spot USDT pair, e.g. BTCUSDT"},
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
            "Calculates a risk-based position size (base asset): the account_balance * risk_pct risk amount is divided by the |entry - stop| risk-per-unit; fees are deducted; the result is rounded down to LOT_SIZE/MIN_NOTIONAL/PRICE_FILTER constraints. Returns FILTER_VIOLATION (fail-closed) when exchange filters cannot be satisfied. Core correctness checks are always active: stale prices, an invalid stop direction, an unknown symbol, or insufficient balance are rejected."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Spot USDT pair, e.g. BTCUSDT"},
                "account_balance": {"type": "number", "exclusiveMinimum": 0, "description": "Quote (USDT) balance"},
                "risk_pct": {"type": "number", "exclusiveMinimum": 0, "maximum": 1, "description": "Percentage of account equity (0.02 = 2%)"},
                "entry": {"type": "number", "exclusiveMinimum": 0},
                "stop_loss": {"type": "number", "exclusiveMinimum": 0},
                "side": {"type": "string", "enum": ["BUY", "SELL"], "default": "BUY"},
                "fee_rate": {"type": "number", "minimum": 0, "default": 0.001, "description": "Fee rate"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["symbol", "account_balance", "risk_pct", "entry", "stop_loss"],
            "additionalProperties": False,
        },
    )
)

# ---------- order execution ----------

register_tool(
    ToolSpec(
        name="execute_on_accounts",
        description=(
            "Opens a position across multiple accounts. If account_ids and tags are both provided, they are combined as a UNION; the request is rejected if both are empty. Order size is calculated from the daemon's own fresh balance/equity/price snapshot (agent-supplied numbers are not trusted). Idempotency: retrying with the same idempotency_key does not create duplicate orders. Partial success returns a separate result for each account. Core correctness checks (balance/staleness/stop direction/symbol) are always active. This tool sends orders directly and bypasses the pending-approval queue; it does not call approve_pending_order. If the target account is unlocked for real trading, it can use real money and requires the user's explicit approval."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_ids": {"type": "array", "items": {"type": "string", "minLength": 1}, "description": "Target account IDs (UNION with tags)"},
                "tags": {"type": "array", "items": {"type": "string", "minLength": 1}, "description": "Target tags (UNION with account_ids)"},
                "symbol": {"type": "string", "description": "Spot USDT pair, e.g. BTCUSDT"},
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
            "Sends an order directly for one account (order_type: MARKET|LIMIT|STOP_LOSS_LIMIT, quantity in base asset). STOP_LOSS_LIMIT provides Spot stop protection: when stop_price is reached, a LIMIT sell is triggered at price (protecting the position on the exchange even if the daemon is offline). Retrying with the same idempotency_key does not create a duplicate order; after a network timeout, the actual Binance state is reconciled. Core correctness checks are always active. This tool sends the order directly and bypasses the pending-approval queue; it does not call approve_pending_order. If the account is unlocked for real trading, it can use real money and requires the user's explicit approval."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "symbol": {"type": "string", "description": "Spot USDT pair, e.g. ALICEUSDT"},
                "side": {"type": "string", "enum": ["BUY", "SELL"]},
                "order_type": {"type": "string", "enum": ["MARKET", "LIMIT", "STOP_LOSS_LIMIT"], "default": "MARKET"},
                "quantity": {"type": "number", "exclusiveMinimum": 0, "description": "Base-asset quantity"},
                "price": {"type": "number", "exclusiveMinimum": 0, "description": "Required for LIMIT/STOP_LOSS_LIMIT (the sell price after the stop triggers)"},
                "stop_price": {"type": "number", "exclusiveMinimum": 0, "description": "Required for STOP_LOSS_LIMIT (stop trigger level)"},
                "idempotency_key": {"type": "string", "minLength": 1},
                "request_id": {"type": "string"},
            },
            "required": ["account_id", "symbol", "side", "quantity", "idempotency_key"],
            "additionalProperties": False,
        },
    )
)

# ---------- kill switch, exposure, and audit ----------

register_tool(
    ToolSpec(
        name="place_oco_order",
        description=(
            "Spot OCO order: profit target (LIMIT) + stop (STOP_LOSS_LIMIT) in ONE order list. When one leg fills, the other is automatically canceled on the exchange (true OCO). Separate SL and TP orders for the same position are not supported because they compete for the same balance; this tool sends both in a single `orderList/oco` call. `price` is the TP, `stop_price` is the stop trigger, and `stop_limit_price` is the limit price to sell after the stop triggers (it must be below stop_price)."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "minLength": 1},
                "symbol": {"type": "string", "description": "Spot USDT pair, e.g. ALICEUSDT"},
                "side": {"type": "string", "enum": ["BUY", "SELL"]},
                "quantity": {"type": "number", "exclusiveMinimum": 0, "description": "Base-asset quantity"},
                "price": {"type": "number", "exclusiveMinimum": 0, "description": "Profit-target (limit) price"},
                "stop_price": {"type": "number", "exclusiveMinimum": 0, "description": "Stop trigger level"},
                "stop_limit_price": {"type": "number", "exclusiveMinimum": 0, "description": "Limit price to sell after the stop triggers (< stop_price)"},
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
            "Cancels open orders for the account (or for all accounts when account_id='all') and sells base-asset balances at market price. Partial success clearly reports which accounts were or were not closed; the operation is idempotent (repeating it does not create duplicate sells). Real balances are not sold for paper accounts; the response uses `closed=false`, `simulated=true`, and `position_close_supported=false` to indicate local order cancellation only. Cancellation transitions are written to the audit log."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Target account ID or 'all'"},
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
            "Kill switch: changes the account's (or 'all' accounts') trading lock from `real` to `paper`; no new orders are sent. The change is written to the audit log; it is idempotent if the account is already in paper mode."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "account_id": {"type": "string", "description": "Target account ID or 'all'"},
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
            "Returns total exposure across all accounts: a symbol-level view (open-order notional plus base-balance value at the daemon's fresh price) and an account-level summary."
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
            "Returns the complete account balance view: free (available), locked (reserved by open orders), holdings_value (current market value of held base assets), and total/equity (free + locked + holdings_value). It reports the account's actual total value, not only free balance, using the daemon's own fresh balance/price snapshot."
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
        name="get_open_orders",
        description=(
            "Returns the REAL open orders currently on Binance. Unlike `get_pending_orders`, this is not the MCP's internal approval queue; these are orders actually waiting on the exchange (including OCO/stop-loss/limit orders and the source of the account balance's `locked` amount). Read-only; it does not modify or cancel any order."
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
        name="get_unprotected_positions",
        description=(
            "Finds dust-above base-asset balances across all real accounts that have no open SELL order (including stop-loss/take-profit/OCO protection)—a one-call answer to which positions are unprotected. Read-only; it does not modify orders. An empty result means every position in every scanned real account matches at least one open SELL order."
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
        name="get_audit_log",
        description=(
            "Queries the hash-chain-verified audit log. `verified=true` means the chain is intact; otherwise `broken` contains the broken rows."
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


# ---------- price-action tools and annotations ----------


def _pa_schema(extra: dict) -> dict:
    base = {
        "symbol": {"type": "string", "description": "Spot USDT pair, e.g. BTCUSDT"},
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
            "Swing High/Low + BOS/CHoCH structure: trend, swings (HH/LH/HL/LL), and structure-break events. `meta.algo_version` carries the algorithm version."
        ),
        input_schema=_pa_schema({}),
    )
)

register_tool(
    ToolSpec(
        name="get_liquidity_zones",
        description=(
            "Equal-high/equal-low liquidity zones, sweep/mitigation state, and a futures-based liquidity score. By default, returns only active (unmitigated) zones; `include_mitigated=true` returns the full stored history. The score's equal_levels component is based on the number of active zones (mitigated zones contribute no points); the funding component carries `bias: long_crowded|short_crowded`. NOTE: history returned with `include_mitigated=true` may also contain breaker records left with mitigated=false under the pre-2.15 semantics—this is expected immutable-history behavior; not every listed zone is active. The default call provides the active view."
        ),
        input_schema=_pa_schema({"include_mitigated": {"type": "boolean", "default": False}}),
    )
)

register_tool(
    ToolSpec(
        name="get_order_blocks",
        description=(
            "Order blocks after BOS/CHoCH (`order_block|breaker|mitigation_block`) plus FVGs. By default, returns only active zones; `include_mitigated=true` returns the full history. Breakers are OBs broken by a close and therefore carry mitigated=true; OBs in the same or a very close price range are merged into one logical zone (2.15). NOTE: history returned with `include_mitigated=true` may also contain breaker records left with mitigated=false under the old semantics (immutable history is not overwritten)—not every listed zone is active. The default call provides the active view."
        ),
        input_schema=_pa_schema({"include_mitigated": {"type": "boolean", "default": False}}),
    )
)

register_tool(
    ToolSpec(
        name="get_full_analysis",
        description=(
            "A complete PA summary in one call: structure + liquidity + order blocks/FVGs + VWAP + session levels. VWAP points are limited to avoid inflating context. `meta.algo_version` and `data.algo_version` carry the versions of all components; `data.versions` reports each component separately. The liquidity score's equal_levels value is the total number of zones described there; the active (unmitigated) count matches the default zones list exactly."
        ),
        input_schema=_pa_schema({"include_mitigated": {"type": "boolean", "default": False}}),
    )
)

register_tool(
    ToolSpec(
        name="annotate_chart",
        description=(
            "Adds an agent annotation to a symbol/timeframe (e.g. {level, label, kind}). It does not affect calculations and is stored persistently."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "symbol": {"type": "string"},
                "timeframe": {"type": "string"},
                "annotations": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Annotation list (a single object is also accepted)",
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
        description="Returns the stored annotations for a symbol/timeframe.",
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
        description="Deletes all annotations for a symbol/timeframe and returns the number deleted.",
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
            "Scans the symbol universe with an allowlisted filter AST (not free-form SQL). Filter types: volume_change, price_change, structure_event, liquidity_sweep_occurred, near_order_block, funding_rate, oi_change, and above_below_vwap; nested with AND/OR nodes. Each row carries `data_stale` (PA freshness) as well as `symbol_valid` (whether the symbol is tradable in the universe—not delisted), `matched_filters` (which filters matched), and `signal_summary` (the raw values that triggered the match) (2.16). `data_stale=false` alone does not mean that the symbol is tradable; check `symbol_valid`."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "filters": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Filter AST, e.g. [{\"type\": \"price_change\", \"min\": 3}]",
                },
                "combine": {"type": "string", "enum": ["AND", "OR"], "default": "AND"},
                "sort_by": {"type": "string", "enum": ["symbol", "price_change", "volume_change"], "default": "symbol"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 250, "default": 50},
                "cursor": {"type": "integer", "description": "Pagination cursor (returned as next_cursor)"},
                "timeframe": {"type": "string", "default": "1h"},
                "request_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
            },
            "required": ["filters"],
            "additionalProperties": False,
        },
    )
)


# ---------- alarm engine ----------


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
            "Defines a conditional alert for one symbol/timeframe. The condition uses the same allowlisted filter AST as scan_market. State machine: armed→triggered→cooldown→armed; the same data window is not triggered twice (deduplication)."
        ),
        input_schema=_alarm_schema(
            {
                "symbol": {"type": "string"},
                "timeframe": {"type": "string"},
                "condition": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Filter AST (same types as scan_market)",
                },
                "cooldown_seconds": {"type": "integer", "minimum": 0, "default": 300},
                "note": {"type": "string"},
                "order_spec": {
                    "type": "object",
                    "description": (
                        "Optional: when the alert triggers, creates an order record awaiting approval (`awaiting_approval`). The order is NOT opened automatically—`approve_pending_order` is required. Fields: account_id (required), symbol (required), side (required, BUY|SELL), order_type (market|limit), risk_pct (REQUIRED, (0,1]—for sizing), entry (required when order_type=limit; finite number), stop_loss (finite number). Numbers must be finite (NaN/Infinity are rejected)."
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
            "An alert that combines multiple clauses with AND/OR. Each clause carries a (symbol,timeframe) pair plus a condition; evaluation is skipped until all clause data is fresh (stale data does not trigger the alert)."
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
        description="Lists all alert definitions with their states (armed/triggered).",
        input_schema=_alarm_schema({}),
    )
)

register_tool(
    ToolSpec(
        name="delete_alert",
        description="Deletes an alert definition.",
        input_schema=_alarm_schema({"alert_id": {"type": "string", "minLength": 1}}),
    )
)

register_tool(
    ToolSpec(
        name="get_triggered_alerts",
        description=(
            "Returns persistent trigger records (triggers that occur while the agent is offline are not lost). Optional alert_id filter and pagination."
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
            "Lists orders awaiting approval (an awaiting_approval record is created when an alert order_spec triggers). Status filter: awaiting_approval|approved|executing|rejected|executed|reconcile_required|expired. Orders are not opened automatically—`approve_pending_order` is required."
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
            "Approves a pending order and opens it as a REAL order. Sizing is performed by the daemon (risk_pct x account equity + symbol filters). Idempotency: retrying with the pending:<order_id> key does not create a duplicate order. This operation uses real money—call it only with the user's explicit approval."
        ),
        input_schema=_alarm_schema({"order_id": {"type": "string", "minLength": 1}}),
    )
)

register_tool(
    ToolSpec(
        name="reject_pending_order",
        description="Rejects a pending order (no order is opened).",
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
