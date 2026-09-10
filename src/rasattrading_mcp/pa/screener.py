"""2.5 — Screener (scan_market): allowlisted filter AST plus safe evaluation.

Filters are NOT free-form SQL. `validate_filters` builds an AST limited to known
types and key sets; reject every input with an unknown type/key/type (no injection
risk — evaluation happens in Python with bound parameters). Evaluate through a
per-symbol context; when data is `stale`, the symbol result explicitly carries
`data_stale: true`.

Filter types:
- volume_change: {recent_bars?, baseline_bars?, min?, max?} — recent window volume
  vs. preceding equal window (%)
- price_change: {window_bars?, min?, max?} — close change (%)
- structure_event: {event, since_bars?}
- liquidity_sweep_occurred: {since_bars?}
- near_order_block: {max_distance_pct}
- funding_rate: {min?, max?}   (only fresh data matches)
- oi_change: {min?, max?, window?} (percentage change between OI samples)
- above_below_vwap: {position: above|below}
"""

from __future__ import annotations

import json
import logging
from typing import Any

from ..envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from ..errors import ErrorCode, RasatError
from ..storage.db import Database
from .analysis import PAEngine, PA_LOOKBACK, _read_current
from .liquidity import load_futures_series
from .vwap_sessions import compute_vwap

logger = logging.getLogger("rasattrading.pa.screener")

ANALYSIS_FILTERS = {
    "structure_event",
    "liquidity_sweep_occurred",
    "near_order_block",
    "funding_rate",
    "oi_change",
    "above_below_vwap",
}

FILTER_KEYS: dict[str, set[str]] = {
    "volume_change": {"recent_bars", "baseline_bars", "min", "max"},
    "price_change": {"window_bars", "min", "max"},
    "structure_event": {"event", "since_bars"},
    "liquidity_sweep_occurred": {"since_bars"},
    "near_order_block": {"max_distance_pct"},
    "funding_rate": {"min", "max"},
    "oi_change": {"min", "max", "window"},
    "above_below_vwap": {"position"},
}

EVENT_TYPES = {"bos_bullish", "bos_bearish", "choch_bullish", "choch_bearish"}

# Keys that require a positive integer when present.
INT_KEYS_BY_FILTER: dict[str, tuple[str, ...]] = {
    "volume_change": ("recent_bars", "baseline_bars"),
    "price_change": ("window_bars",),
    "structure_event": ("since_bars",),
    "liquidity_sweep_occurred": ("since_bars",),
    "oi_change": ("window",),
}


def _defaults(f: dict) -> dict:
    ftype = f["type"]
    d = dict(f)
    if ftype == "volume_change":
        d.setdefault("recent_bars", 24)
        d.setdefault("baseline_bars", 24)
    elif ftype == "price_change":
        d.setdefault("window_bars", 24)
    elif ftype in ("structure_event", "liquidity_sweep_occurred"):
        d.setdefault("since_bars", 50)
    elif ftype == "oi_change":
        d.setdefault("window", 1)
    return d


def validate_filters(filters: list | dict | None, combine: str = "AND") -> dict:
    """Validate the filter list and return the normalized root AST node."""
    combine = (combine or "AND").upper()
    if combine not in ("AND", "OR"):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"combine can only be AND|OR: {combine}")
    items = filters if isinstance(filters, list) else ([filters] if filters else [])
    if not items:
        raise RasatError(ErrorCode.INVALID_REQUEST, "at least one filter is required")
    validated = [_validate_node(it) for it in items]
    return {"type": combine.lower(), "filters": validated}


def _validate_node(node: Any) -> dict:
    if not isinstance(node, dict) or "type" not in node:
        raise RasatError(ErrorCode.INVALID_REQUEST, "filter must be an object with a 'type' field")
    ftype = node["type"]
    if ftype in ("and", "or"):
        for key in node:
            if key not in ("type", "filters"):
                raise RasatError(ErrorCode.INVALID_REQUEST, f"unknown key in {ftype} node: {key}")
        subs = node.get("filters")
        if not isinstance(subs, list) or not subs:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype} node requires at least one child filter")
        return {"type": ftype, "filters": [_validate_node(s) for s in subs]}
    if ftype not in FILTER_KEYS:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"unknown filter type: {ftype}")
    allowed = FILTER_KEYS[ftype]
    for key in node:
        if key == "type":
            continue
        if key not in allowed:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"unknown key in filter '{ftype}': {key}")
    for key in INT_KEYS_BY_FILTER.get(ftype, ()):
        if key in node and (isinstance(node[key], bool) or not isinstance(node[key], int) or node[key] < 1):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype}.{key} must be a positive integer (given: {node[key]!r})")
    for key in ("min", "max"):
        if key in node:
            v = node[key]
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype}.{key} must be a number")
    if "min" in node and "max" in node and node["min"] > node["max"]:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype}: min cannot exceed max")
    if ftype == "structure_event" and node.get("event") not in EVENT_TYPES:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"unknown structure event: {node.get('event')}")
    if ftype == "above_below_vwap" and node.get("position") not in ("above", "below"):
        raise RasatError(ErrorCode.INVALID_REQUEST, "above_below_vwap.position must be above|below")
    if ftype == "near_order_block":
        v = node.get("max_distance_pct")
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
            raise RasatError(ErrorCode.INVALID_REQUEST, "near_order_block.max_distance_pct must be a number >= 0")
    return _defaults(node)


# ---------------------------------------------------------------------------
# Filter evaluation
# ---------------------------------------------------------------------------


def _between(value: float | None, lo: float | None, hi: float | None) -> bool:
    if value is None:
        return False
    if lo is not None and value < lo:
        return False
    if hi is not None and value > hi:
        return False
    return True


def _price_change_pct(ctx: dict, window_bars: int) -> float | None:
    closes = [c["close"] for c in ctx["candles"]]
    w = window_bars
    if len(closes) < w + 1:
        return None
    base = closes[-w - 1]
    if not base:
        return None
    return (closes[-1] / base - 1.0) * 100.0


def _volume_change_pct(ctx: dict, recent_bars: int, baseline_bars: int) -> float | None:
    vols = [c["volume"] or 0.0 for c in ctx["candles"]]
    recent = recent_bars
    baseline = baseline_bars
    if len(vols) < recent + baseline:
        return None
    recent_sum = sum(vols[-recent:])
    base_sum = sum(vols[-recent - baseline : -recent])
    if base_sum <= 0:
        return None
    return (recent_sum / base_sum - 1.0) * 100.0


def _oi_change_pct(ctx: dict, window: int) -> float | None:
    series = ctx.get("oi_series") or []
    fresh = [s for s in series if s.get("freshness") == "fresh"]
    w = window
    if len(fresh) < w + 1:
        return None
    latest = fresh[-1]["value"]
    base = fresh[-1 - w]["value"]
    if not latest or not base:
        return None
    return (latest / base - 1.0) * 100.0


def _eval_volume_change(f: dict, ctx: dict) -> bool:
    pct = _volume_change_pct(ctx, f["recent_bars"], f["baseline_bars"])
    return _between(pct, f.get("min"), f.get("max"))


def _eval_price_change(f: dict, ctx: dict) -> bool:
    pct = _price_change_pct(ctx, f["window_bars"])
    return _between(pct, f.get("min"), f.get("max"))


def _eval_structure_event(f: dict, ctx: dict) -> bool:
    structure = ctx.get("structure")
    if not structure:
        return False
    candles = ctx["candles"]
    since = f["since_bars"]
    cutoff = candles[max(0, len(candles) - since)]["open_time"]
    for ev in structure.get("events", []):
        if ev["type"] != f["event"]:
            continue
        t = ev.get("time")
        if t is not None:
            if t >= cutoff:
                return True
        elif ev["index"] >= len(candles) - since:
            return True
    return False


def _eval_liquidity_sweep(f: dict, ctx: dict) -> bool:
    zones = ctx.get("liquidity_zones")
    if not zones:
        return False
    candles = ctx["candles"]
    since = f["since_bars"]
    cutoff = candles[max(0, len(candles) - since)]["open_time"]
    for z in zones:
        if not z.get("mitigated"):
            continue
        st = z.get("swept_at_time")
        if st is not None:
            if st >= cutoff:
                return True
        elif z.get("swept_at") is not None and z["swept_at"] >= len(candles) - since:
            return True
    return False


def _eval_near_order_block(f: dict, ctx: dict) -> bool:
    obs = ctx.get("order_blocks")
    if not obs:
        return False
    close = ctx["close"]
    max_dist = f["max_distance_pct"]
    for ob in obs:
        if ob.get("mitigated"):
            continue
        lo, hi = ob["range"]["low"], ob["range"]["high"]
        dist = min(abs(close - lo), abs(close - hi))
        if dist / close * 100.0 <= max_dist:
            return True
    return False


def _eval_funding_rate(f: dict, ctx: dict) -> bool:
    fr = ctx.get("funding_rate")
    if fr is None or fr.get("freshness") != "fresh":
        return False
    return _between(fr.get("value"), f.get("min"), f.get("max"))


def _eval_oi_change(f: dict, ctx: dict) -> bool:
    pct = _oi_change_pct(ctx, f["window"])
    return _between(pct, f.get("min"), f.get("max"))


def _eval_above_below_vwap(f: dict, ctx: dict) -> bool:
    vwap = ctx.get("vwap")
    if vwap is None:
        return False
    close = ctx["close"]
    if f["position"] == "above":
        return close > vwap
    return close < vwap


FILTER_FNS = {
    "volume_change": _eval_volume_change,
    "price_change": _eval_price_change,
    "structure_event": _eval_structure_event,
    "liquidity_sweep_occurred": _eval_liquidity_sweep,
    "near_order_block": _eval_near_order_block,
    "funding_rate": _eval_funding_rate,
    "oi_change": _eval_oi_change,
    "above_below_vwap": _eval_above_below_vwap,
}


def _eval_node(node: dict, ctx: dict) -> bool:
    ftype = node["type"]
    if ftype == "and":
        return all(_eval_node(s, ctx) for s in node["filters"])
    if ftype == "or":
        return any(_eval_node(s, ctx) for s in node["filters"])
    return FILTER_FNS[ftype](node, ctx)


def _eval_with_matches(node: dict, ctx: dict) -> tuple[bool, list[dict]]:
    """`_eval_node` plus matching leaf filters (for auditability, 2.16).

    Return `(bool, [matching filter nodes])`. For an AND node, merge matches when
    all child filters match; for an OR node, collect only matching child filters.
    """
    ftype = node["type"]
    if ftype == "and":
        matched: list[dict] = []
        for s in node["filters"]:
            ok, m = _eval_with_matches(s, ctx)
            if not ok:
                return False, []
            matched.extend(m)
        return True, matched
    if ftype == "or":
        matched = []
        for s in node["filters"]:
            ok, m = _eval_with_matches(s, ctx)
            if ok:
                matched.extend(m)
        return bool(matched), matched
    if FILTER_FNS[ftype](node, ctx):
        return True, [node]
    return False, []


def _signal_summary(f: dict, ctx: dict) -> str:
    """Short, human-readable signal summary for a matching filter (2.16)."""
    ftype = f["type"]
    if ftype == "structure_event":
        return f"{f['event']} since={f['since_bars']}"
    if ftype == "liquidity_sweep_occurred":
        return f"sweep since={f['since_bars']}"
    if ftype == "near_order_block":
        close = ctx["close"]
        best: float | None = None
        for ob in ctx.get("order_blocks") or []:
            if ob.get("mitigated"):
                continue
            lo, hi = ob["range"]["low"], ob["range"]["high"]
            dist = min(abs(close - lo), abs(close - hi)) / close * 100.0
            if best is None or dist < best:
                best = dist
        d = f"{best:.2f}%" if best is not None else "?"
        return f"near_ob {d} (max {f['max_distance_pct']}%)"
    if ftype == "funding_rate":
        fr = ctx.get("funding_rate")
        v = fr.get("value") if fr else None
        return f"funding={v!r}"
    if ftype == "above_below_vwap":
        vwap = ctx.get("vwap")
        diff = (ctx["close"] / vwap - 1.0) * 100.0 if vwap else None
        dd = f"{diff:+.2f}%" if diff is not None else "?"
        return f"vwap {f['position']} ({dd})"
    if ftype == "price_change":
        pct = _price_change_pct(ctx, f["window_bars"])
        d = f"{pct:+.2f}%" if pct is not None else "?"
        return f"price {f['window_bars']}bar {d}"
    if ftype == "volume_change":
        pct = _volume_change_pct(ctx, f["recent_bars"], f["baseline_bars"])
        d = f"{pct:+.2f}%" if pct is not None else "?"
        return f"volume {f['recent_bars']}/{f['baseline_bars']}bar {d}"
    if ftype == "oi_change":
        pct = _oi_change_pct(ctx, f["window"])
        d = f"{pct:+.2f}%" if pct is not None else "?"
        return f"oi {f['window']}bar {d}"
    return ftype


# ---------------------------------------------------------------------------
# Screener service
# ---------------------------------------------------------------------------


class Screener:
    def __init__(self, db: Database, engine: PAEngine | None = None, pipeline=None, compute_budget: int | None = None) -> None:
        self.db = db
        self.engine = engine or PAEngine(db, pipeline=pipeline)
        self.pipeline = pipeline
        # T2 (K3 pattern — see pa/alarms.py): bound on-demand `analyze` (and thus
        # warm-up/backfill load) in a cold universe when stored analysis is absent;
        # allow limited on-demand computation per scan pass and defer excess symbols
        # (`deferred_analysis`).
        if compute_budget is None:
            cfg = self.engine.config if self.engine is not None else None
            compute_budget = getattr(cfg, "alarm_compute_budget", 8) if cfg is not None else 8
        self._compute_budget = max(0, int(compute_budget))
        self._compute_used = 0
        self._compute_deferred = 0

    def begin_evaluation_pass(self) -> None:
        """Start a new scan pass: reset the on-demand PA computation budget (T2)."""
        self._compute_used = 0
        self._compute_deferred = 0

    async def _candidate_symbols(self) -> list[str]:
        if self.pipeline is not None and hasattr(self.pipeline, "universe"):
            snap = self.pipeline.universe.snapshot()
            if snap:
                return sorted(snap)

        def _q(conn):
            rows = conn.execute("SELECT DISTINCT symbol FROM candles WHERE source='spot'").fetchall()
            return sorted(r["symbol"] for r in rows)

        return await self.db.read(_q)

    async def _build_context(self, symbol: str, timeframe: str, needs_analysis: bool, filter_types: list[str]) -> dict | None:
        # SAME window as the PA engine's structure/liquidity payload (2.16 fix):
        # event indexes are generated relative to this window; a different candle
        # count misaligned event/sweep indexes and missed recent events.
        candles = await self.engine._read_candles(symbol, timeframe, PA_LOOKBACK)
        if not candles:
            return None
        from .swings import filter_closed_candles

        candles = filter_closed_candles(candles, timeframe, now=self.engine._now())
        if not candles:
            return None
        ctx: dict[str, Any] = {
            "symbol": symbol,
            "candles": candles,
            "close": candles[-1]["close"],
            "as_of": candles[-1]["open_time"],
        }
        if needs_analysis:
            ms = await _read_current(self.db, "market_structure", symbol, timeframe)
            lz = await _read_current(self.db, "liquidity_zones", symbol, timeframe)
            ob = await _read_current(self.db, "order_blocks", symbol, timeframe)
            if ms is None or lz is None or ob is None:
                # T2: an unbudgeted analyze call for every symbol in a cold universe
                # could keep a scan running for minutes; bound it with the K3 pattern.
                if self._compute_used >= self._compute_budget:
                    self._compute_deferred += 1
                    return None
                self._compute_used += 1
                await self.engine.analyze(symbol, timeframe)
                ms = await _read_current(self.db, "market_structure", symbol, timeframe)
                lz = await _read_current(self.db, "liquidity_zones", symbol, timeframe)
                ob = await _read_current(self.db, "order_blocks", symbol, timeframe)
            if ms is None:
                return None
            ctx["as_of"] = ms["effective_from"]
            ctx["structure"] = json.loads(ms["payload"])
            ctx["liquidity_zones"] = json.loads(lz["payload"]).get("zones", []) if lz else []
            ctx["order_blocks"] = json.loads(ob["payload"]).get("order_blocks", []) if ob else []
            ctx["funding_rate"] = await self._latest_futures(symbol, "funding_rate")
            if "oi_change" in filter_types:
                ctx["oi_series"] = await load_futures_series(self.db, symbol, "open_interest")
        vwap = compute_vwap(candles)
        ctx["vwap"] = vwap["current"]
        return ctx

    async def _latest_futures(self, symbol: str, ftype: str) -> dict | None:
        series = await load_futures_series(self.db, symbol, ftype, limit=1)
        return series[-1] if series else None

    def _symbol_validity(self, symbol: str) -> bool | None:
        """Symbol validity when the universe can be verified (2.16): True/False, else None.

        `data_stale` measures only PA freshness; it does not say whether the symbol
        is still tradable (in the universe). When the universe is loaded (non-empty
        `snapshot`), validate with `universe.contains`; when it is not loaded (empty
        snapshot), filtering is impossible, so return `None` and keep the symbol as a candidate.
        """
        if self.pipeline is None or not hasattr(self.pipeline, "universe"):
            return None
        uni = self.pipeline.universe
        if not uni.snapshot():
            return None
        return uni.contains(symbol)

    async def scan(
        self,
        filters: list | dict,
        combine: str = "AND",
        sort_by: str = "symbol",
        limit: int = 50,
        cursor: int | None = None,
        timeframe: str = "1h",
        require_fresh: bool = True,
    ) -> dict:
        root = validate_filters(filters, combine)
        if not 1 <= limit <= 250:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"limit must be between 1 and 250 (given: {limit})")
        if sort_by not in ("symbol", "price_change", "volume_change"):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"unknown sort_by: {sort_by}")
        self.begin_evaluation_pass()

        def _collect(node, acc):
            if node["type"] in ("and", "or"):
                for s in node["filters"]:
                    _collect(s, acc)
            else:
                acc.append(node["type"])

        filter_types: list[str] = []
        _collect(root, filter_types)
        needs_analysis = any(t in ANALYSIS_FILTERS for t in filter_types)

        symbols = await self._candidate_symbols()
        matched: list[dict] = []
        stale_symbols: list[dict] = []
        any_stale = False
        for symbol in symbols:
            valid = self._symbol_validity(symbol)
            if valid is False:
                # Delisted / non-universe symbol — do not produce a misleading match (2.16).
                continue
            ctx = await self._build_context(symbol, timeframe, needs_analysis, filter_types)
            if ctx is None:
                continue
            ok, matched_nodes = _eval_with_matches(root, ctx)
            if not ok:
                continue
            stale = self.engine.freshness(timeframe, ctx["as_of"]) != FRESHNESS_FRESH
            any_stale = any_stale or stale
            entry = {
                "symbol": symbol,
                "data_stale": stale,
                "symbol_valid": valid,
                "price": ctx["close"],
                "as_of": ctx["as_of"],
                "matched_filters": [n["type"] for n in matched_nodes],
                "signal_summary": "; ".join(_signal_summary(n, ctx) for n in matched_nodes),
                "sort_value": self._sort_value(sort_by, ctx),
            }
            if stale and require_fresh:
                # T2: do not mix stale symbols into filter results (same fail-closed
                # behavior as alarms); keep them visible in the separate report.
                stale_symbols.append({"symbol": symbol, "as_of": ctx["as_of"]})
                continue
            matched.append(entry)

        matched.sort(key=lambda r: (r["sort_value"], r["symbol"]))
        total = len(matched)
        start = cursor or 0
        if start > total:
            start = total
        page = matched[start : start + limit]
        next_cursor = (start + limit) if (start + limit) < total else None

        return {
            "symbols": [
                {
                    "symbol": r["symbol"],
                    "data_stale": r["data_stale"],
                    "symbol_valid": r["symbol_valid"],
                    "price": r["price"],
                    "as_of": r["as_of"],
                    "matched_filters": r["matched_filters"],
                    "signal_summary": r["signal_summary"],
                }
                for r in page
            ],
            "total_matched": total,
            "next_cursor": next_cursor,
            "combine": combine.upper(),
            "freshness": FRESHNESS_STALE if (any_stale and not require_fresh) else FRESHNESS_FRESH,
            "stale_symbols": stale_symbols,
            "stale_total": len(stale_symbols),
            "deferred_analysis": self._compute_deferred,
        }

    @staticmethod
    def _sort_value(sort_by: str, ctx: dict) -> float:
        if sort_by == "price_change":
            closes = [c["close"] for c in ctx["candles"]]
            if len(closes) >= 25:
                return (closes[-1] / closes[-25] - 1.0) * 100.0
            return 0.0
        if sort_by == "volume_change":
            vols = [c["volume"] or 0.0 for c in ctx["candles"]]
            if len(vols) >= 48:
                return (sum(vols[-24:]) / sum(vols[-48:-24]) - 1.0) * 100.0
            return 0.0
        return 0.0
