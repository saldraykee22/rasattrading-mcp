"""2.5 — Screener (scan_market): allowlisted filtre AST + güvenli değerlendirme.

Filtreler serbest SQL DEĞİLDİR. `validate_filters` bilinen türler ve anahtar
kümeleriyle sınırlı bir AST üretir; bilinmeyen tür/anahtar/tipe sahip her girdi
reddedilir (enjeksiyon riski yok — değerlendirme Python tarafında, bağlı
parametrelerle yapılır). Değerlendirme sembol başına sembol context'i üzerinden
çalışır; veri `stale` ise sembol sonucu açıkça `data_stale: true` taşır.

Filtre türleri:
- volume_change: {recent_bars?, baseline_bars?, min?, max?} — son pencere hacmi
  vs önceki eşit pencere (%)
- price_change: {window_bars?, min?, max?} — close değişimi (%)
- structure_event: {event, since_bars?}
- liquidity_sweep_occurred: {since_bars?}
- near_order_block: {max_distance_pct}
- funding_rate: {min?, max?}   (yalnız fresh veri eşleşir)
- oi_change: {min?, max?, window?} (OI örnekleri arası % değişim)
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

# pozitif integer bekleyen anahtarlar (varsa)
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
    """Filtre listesini doğrular; normalize edilmiş kök AST düğümünü döner."""
    combine = (combine or "AND").upper()
    if combine not in ("AND", "OR"):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"combine yalnızca AND|OR olabilir: {combine}")
    items = filters if isinstance(filters, list) else ([filters] if filters else [])
    if not items:
        raise RasatError(ErrorCode.INVALID_REQUEST, "en az bir filtre gerekli")
    validated = [_validate_node(it) for it in items]
    return {"type": combine.lower(), "filters": validated}


def _validate_node(node: Any) -> dict:
    if not isinstance(node, dict) or "type" not in node:
        raise RasatError(ErrorCode.INVALID_REQUEST, "filtre bir nesne ve 'type' alanı taşımalı")
    ftype = node["type"]
    if ftype in ("and", "or"):
        for key in node:
            if key not in ("type", "filters"):
                raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype} düğümü bilinmeyen anahtar: {key}")
        subs = node.get("filters")
        if not isinstance(subs, list) or not subs:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype} düğümü en az bir alt filtre ister")
        return {"type": ftype, "filters": [_validate_node(s) for s in subs]}
    if ftype not in FILTER_KEYS:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"bilinmeyen filtre türü: {ftype}")
    allowed = FILTER_KEYS[ftype]
    for key in node:
        if key == "type":
            continue
        if key not in allowed:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"filtre '{ftype}' bilinmeyen anahtar: {key}")
    for key in INT_KEYS_BY_FILTER.get(ftype, ()):
        if key in node and (isinstance(node[key], bool) or not isinstance(node[key], int) or node[key] < 1):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype}.{key} pozitif integer olmalı (verildi: {node[key]!r})")
    for key in ("min", "max"):
        if key in node:
            v = node[key]
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype}.{key} sayı olmalı")
    if "min" in node and "max" in node and node["min"] > node["max"]:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{ftype}: min, max'tan büyük olamaz")
    if ftype == "structure_event" and node.get("event") not in EVENT_TYPES:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"bilinmeyen structure event: {node.get('event')}")
    if ftype == "above_below_vwap" and node.get("position") not in ("above", "below"):
        raise RasatError(ErrorCode.INVALID_REQUEST, "above_below_vwap.position above|below olmalı")
    if ftype == "near_order_block":
        v = node.get("max_distance_pct")
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
            raise RasatError(ErrorCode.INVALID_REQUEST, "near_order_block.max_distance_pct >= 0 sayı olmalı")
    return _defaults(node)


# ---------------------------------------------------------------------------
# Filtre değerlendirme
# ---------------------------------------------------------------------------


def _between(value: float | None, lo: float | None, hi: float | None) -> bool:
    if value is None:
        return False
    if lo is not None and value < lo:
        return False
    if hi is not None and value > hi:
        return False
    return True


def _eval_volume_change(f: dict, ctx: dict) -> bool:
    vols = [c["volume"] or 0.0 for c in ctx["candles"]]
    recent = f["recent_bars"]
    baseline = f["baseline_bars"]
    if len(vols) < recent + baseline:
        return False
    recent_sum = sum(vols[-recent:])
    base_sum = sum(vols[-recent - baseline : -recent])
    if base_sum <= 0:
        return False
    pct = (recent_sum / base_sum - 1.0) * 100.0
    return _between(pct, f.get("min"), f.get("max"))


def _eval_price_change(f: dict, ctx: dict) -> bool:
    closes = [c["close"] for c in ctx["candles"]]
    w = f["window_bars"]
    if len(closes) < w + 1:
        return False
    pct = (closes[-1] / closes[-w - 1] - 1.0) * 100.0
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
    series = ctx.get("oi_series") or []
    fresh = [s for s in series if s.get("freshness") == "fresh"]
    w = f["window"]
    if len(fresh) < w + 1:
        return False
    latest = fresh[-1]["value"]
    base = fresh[-1 - w]["value"]
    if not latest or not base:
        return False
    pct = (latest / base - 1.0) * 100.0
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


# ---------------------------------------------------------------------------
# Screener servisi
# ---------------------------------------------------------------------------


class Screener:
    def __init__(self, db: Database, engine: PAEngine | None = None, pipeline=None) -> None:
        self.db = db
        self.engine = engine or PAEngine(db, pipeline=pipeline)
        self.pipeline = pipeline

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
        # PA engine'in yapı/likidite payload'ıyla AYNI pencere (2.16 fix): event
        # indeksleri bu pencereye göre üretilir; farklı mum sayısı event/sweep
        # indekslerini hizasız bırakıp gerçek son olayları kaçırıyordu.
        candles = await self.engine._read_candles(symbol, timeframe, PA_LOOKBACK)
        if not candles:
            return None
        from .swings import filter_closed_candles

        candles = filter_closed_candles(candles, timeframe)
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

    async def scan(
        self,
        filters: list | dict,
        combine: str = "AND",
        sort_by: str = "symbol",
        limit: int = 50,
        cursor: int | None = None,
        timeframe: str = "1h",
    ) -> dict:
        root = validate_filters(filters, combine)
        if not 1 <= limit <= 250:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"limit 1-250 arası olmalı (verildi: {limit})")
        if sort_by not in ("symbol", "price_change", "volume_change"):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"bilinmeyen sort_by: {sort_by}")

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
        any_stale = False
        for symbol in symbols:
            ctx = await self._build_context(symbol, timeframe, needs_analysis, filter_types)
            if ctx is None:
                continue
            ok = _eval_node(root, ctx)
            if not ok:
                continue
            stale = PAEngine.freshness_for(timeframe, ctx["as_of"]) != FRESHNESS_FRESH
            any_stale = any_stale or stale
            matched.append(
                {
                    "symbol": symbol,
                    "data_stale": stale,
                    "price": ctx["close"],
                    "sort_value": self._sort_value(sort_by, ctx),
                }
            )

        matched.sort(key=lambda r: (r["sort_value"], r["symbol"]))
        total = len(matched)
        start = cursor or 0
        if start > total:
            start = total
        page = matched[start : start + limit]
        next_cursor = (start + limit) if (start + limit) < total else None

        return {
            "symbols": [{"symbol": r["symbol"], "data_stale": r["data_stale"], "price": r["price"]} for r in page],
            "total_matched": total,
            "next_cursor": next_cursor,
            "combine": combine.upper(),
            "freshness": FRESHNESS_STALE if any_stale else FRESHNESS_FRESH,
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
