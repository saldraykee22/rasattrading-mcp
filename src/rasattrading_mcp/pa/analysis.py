"""2.4 — PA analysis orchestration and immutable-record management.

`PAEngine` reads candles, computes the full PA chain (swing→liquidity→OB/FVG +
VWAP + session), and writes results to the `market_structure`/`liquidity_zones`/
`order_blocks` tables with an immutable pattern: never overwrite; a new result
closes the old open record with `effective_to` and preserves history.
`include_mitigated=true` builds history from these stored records, not invented data.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from ..config import TIMEFRAME_SECONDS
from ..envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from ..errors import ErrorCode, RasatError
from ..storage.db import Database
from .liquidity import compute_liquidity_zones, load_futures_context
from .obfvg import compute_order_blocks
from .swings import SWING_LOOKBACK, detect_structure, filter_closed_candles
from .vwap_sessions import compute_session_levels, compute_vwap

logger = logging.getLogger("rasattrading.pa.analysis")

MAX_VWAP_POINTS = 20

# Default closed-candle window used by PA analysis. The screener (`_build_context`)
# uses the same window; otherwise structure/sweep event indexes (relative to this
# window) would be misaligned with the screener context candle count and recent
# events could be missed by `since_bars` filters (2.16 fix).
PA_LOOKBACK = 200

IMMUTABLE_TABLES = ("market_structure", "liquidity_zones", "order_blocks")


async def _store_payload(
    db: Database,
    table: str,
    symbol: str,
    timeframe: str,
    algo_version: str,
    effective_from: int,
    payload: dict,
) -> None:
    """Immutable record: never overwrite; preserve history (2.9 fix).

    - Same (symbol, timeframe, effective_from) + same version + same payload →
      idempotent no-op; leave the existing record untouched.
    - Same effective_from + different version/payload → close the existing revision
      (effective_to = effective_from, point interval) and add a new open revision.
      Both records remain queryable in history.
    - Forward (new bar): close the open record at `effective_from - 1` and add a new open record.
    - Backward (backfill / historical as-of): do not close the open record; add a
      historical closed record, avoiding an inverted interval (e.g. 300..199).
    """
    payload_json = json.dumps(payload, ensure_ascii=False)

    def _w(conn) -> None:
        now = int(time.time())
        same_bar = conn.execute(
            f"SELECT id, effective_to, algo_version, payload FROM {table} "
            f"WHERE symbol=? AND timeframe=? AND effective_from=? ORDER BY id DESC LIMIT 1",
            (symbol, timeframe, effective_from),
        ).fetchone()
        if same_bar is not None:
            if same_bar["algo_version"] == algo_version and same_bar["payload"] == payload_json:
                return  # Idempotent — never overwrite silently.
            if same_bar["effective_to"] is None:
                conn.execute(f"UPDATE {table} SET effective_to=? WHERE id=?", (effective_from, same_bar["id"]))
                conn.execute(
                    f"INSERT INTO {table} (symbol, timeframe, algo_version, effective_from, effective_to, payload, created_at) "
                    f"VALUES (?,?,?,?,NULL,?,?)",
                    (symbol, timeframe, algo_version, effective_from, payload_json, now),
                )
            else:
                conn.execute(
                    f"INSERT INTO {table} (symbol, timeframe, algo_version, effective_from, effective_to, payload, created_at) "
                    f"VALUES (?,?,?,?,?,?,?)",
                    (symbol, timeframe, algo_version, effective_from, same_bar["effective_to"], payload_json, now),
                )
            return

        open_row = conn.execute(
            f"SELECT id, effective_from FROM {table} "
            f"WHERE symbol=? AND timeframe=? AND effective_to IS NULL "
            f"ORDER BY effective_from DESC, id DESC LIMIT 1",
            (symbol, timeframe),
        ).fetchone()
        if open_row is None:
            conn.execute(
                f"INSERT INTO {table} (symbol, timeframe, algo_version, effective_from, effective_to, payload, created_at) "
                f"VALUES (?,?,?,?,NULL,?,?)",
                (symbol, timeframe, algo_version, effective_from, payload_json, now),
            )
            return
        if effective_from > open_row["effective_from"]:
            conn.execute(f"UPDATE {table} SET effective_to=? WHERE id=?", (effective_from - 1, open_row["id"]))
            conn.execute(
                f"INSERT INTO {table} (symbol, timeframe, algo_version, effective_from, effective_to, payload, created_at) "
                f"VALUES (?,?,?,?,NULL,?,?)",
                (symbol, timeframe, algo_version, effective_from, payload_json, now),
            )
            return
        # Backward (backfill): add a historical closed record without closing the open record.
        next_gt = conn.execute(
            f"SELECT MIN(effective_from) AS m FROM {table} WHERE symbol=? AND timeframe=? AND effective_from > ?",
            (symbol, timeframe, effective_from),
        ).fetchone()
        eff_to = (next_gt["m"] - 1) if next_gt["m"] is not None else open_row["effective_from"] - 1
        conn.execute(
            f"INSERT INTO {table} (symbol, timeframe, algo_version, effective_from, effective_to, payload, created_at) "
            f"VALUES (?,?,?,?,?,?,?)",
            (symbol, timeframe, algo_version, effective_from, eff_to, payload_json, now),
        )

    await db.write(_w)


async def _read_current(db: Database, table: str, symbol: str, timeframe: str) -> dict | None:
    def _q(conn):
        row = conn.execute(
            f"SELECT effective_from, algo_version, payload FROM {table} "
            f"WHERE symbol=? AND timeframe=? AND effective_to IS NULL "
            f"ORDER BY effective_from DESC LIMIT 1",
            (symbol, timeframe),
        ).fetchone()
        return dict(row) if row else None

    return await db.read(_q)


async def _read_history(db: Database, table: str, symbol: str, timeframe: str) -> list[dict]:
    def _q(conn):
        rows = conn.execute(
            f"SELECT effective_from, effective_to, algo_version, payload FROM {table} "
            f"WHERE symbol=? AND timeframe=? ORDER BY effective_from ASC",
            (symbol, timeframe),
        ).fetchall()
        return [dict(r) for r in rows]

    return await db.read(_q)


class PAEngine:
    def __init__(self, db: Database, pipeline=None, config=None) -> None:
        self.db = db
        self.pipeline = pipeline
        self.config = config
        self.alarm_service = None  # Connected in 2.6 (event-driven evaluation).

    def _now(self) -> float:
        """Shared `now` source for the PA layer (T2).

        The data layer (`data/klines.py`) makes close/freshness decisions with the
        Binance server clock (`pipeline.clock.server_now()`). Use the same server
        clock here so PA decisions do not drift with the host clock; fall back to
        local time when pipeline/clock is unavailable.
        """
        clock = getattr(self.pipeline, "clock", None) if self.pipeline is not None else None
        if clock is not None:
            server_now = clock.server_now()
            if server_now is not None:
                return server_now
        return time.time()

    # ---------- candle reading ----------

    async def _read_candles(self, symbol: str, timeframe: str, lookback: int, source: str = "spot") -> list[dict]:
        def _q(conn):
            rows = conn.execute(
                "SELECT open_time, open, high, low, close, volume FROM candles "
                "WHERE symbol=? AND timeframe=? AND source=? ORDER BY open_time DESC LIMIT ?",
                (symbol, timeframe, source, lookback),
            ).fetchall()
            return [dict(r) for r in reversed(rows)]

        return await self.db.read(_q)

    async def _load_candles(self, symbol: str, timeframe: str, lookback: int) -> list[dict]:
        rows = await self._read_candles(symbol, timeframe, lookback)
        if not rows and self.pipeline is not None:
            # Cold symbol → pipeline performs warm-up (and universe validation here).
            await self.pipeline.get_candles(symbol, timeframe, limit=lookback)
            rows = await self._read_candles(symbol, timeframe, lookback)
        return rows

    # ---------- analiz ----------

    async def analyze(self, symbol: str, timeframe: str, lookback: int = PA_LOOKBACK) -> dict[str, Any]:
        if timeframe not in TIMEFRAME_SECONDS:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"invalid timeframe: {timeframe}")
        candles = await self._load_candles(symbol, timeframe, lookback)
        if not candles:
            raise RasatError(ErrorCode.STALE_DATA, f"no candle data for {symbol} {timeframe}")
        candles = filter_closed_candles(candles, timeframe, now=self._now())
        if len(candles) < 2 * SWING_LOOKBACK + 1:
            raise RasatError(ErrorCode.STALE_DATA, f"not enough closed candles for {symbol} {timeframe}")

        structure = detect_structure(candles)
        futures = await load_futures_context(self.db, symbol)
        liquidity = compute_liquidity_zones(candles, structure, futures)
        obfvg = compute_order_blocks(candles, structure)
        vwap = compute_vwap(candles)
        sessions = compute_session_levels(candles)

        effective_from = int(candles[-1]["open_time"])
        await _store_payload(self.db, "market_structure", symbol, timeframe, structure["algo_version"], effective_from, structure)
        await _store_payload(self.db, "liquidity_zones", symbol, timeframe, liquidity["algo_version"], effective_from, liquidity)
        await _store_payload(self.db, "order_blocks", symbol, timeframe, obfvg["algo_version"], effective_from, obfvg)

        result = {
            "symbol": symbol,
            "timeframe": timeframe,
            "as_of": effective_from,
            "structure": structure,
            "liquidity": liquidity,
            "order_blocks": obfvg,
            "vwap": vwap,
            "sessions": sessions,
        }
        if self.alarm_service is not None:
            await self.alarm_service.on_analysis_updated(symbol, timeframe, result)
        return result

    # ---------- queries ----------

    async def get_market_structure(self, symbol: str, timeframe: str, lookback: int = PA_LOOKBACK) -> dict[str, Any]:
        result = await self.analyze(symbol, timeframe, lookback)
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "as_of": result["as_of"],
            "algo_version": result["structure"]["algo_version"],
            "structure": result["structure"],
        }

    async def get_liquidity_zones(
        self, symbol: str, timeframe: str, include_mitigated: bool = False, lookback: int = PA_LOOKBACK
    ) -> dict[str, Any]:
        result = await self.analyze(symbol, timeframe, lookback)
        if include_mitigated:
            zones = await self._merged_zones("liquidity_zones", symbol, timeframe, "zone_id")
        else:
            zones = [z for z in result["liquidity"]["zones"] if not z["mitigated"]]
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "as_of": result["as_of"],
            "algo_version": result["liquidity"]["algo_version"],
            "zones": zones,
            "score": result["liquidity"]["score"],
        }

    async def get_order_blocks(
        self, symbol: str, timeframe: str, include_mitigated: bool = False, lookback: int = PA_LOOKBACK
    ) -> dict[str, Any]:
        result = await self.analyze(symbol, timeframe, lookback)
        if include_mitigated:
            obs = await self._merged_zones("order_blocks", symbol, timeframe, "zone_id", list_key="order_blocks")
            fvgs = await self._merged_zones("order_blocks", symbol, timeframe, "zone_id", list_key="fvgs")
        else:
            obs = [z for z in result["order_blocks"]["order_blocks"] if not z["mitigated"]]
            fvgs = [z for z in result["order_blocks"]["fvgs"] if not z["mitigated"]]
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "as_of": result["as_of"],
            "algo_version": result["order_blocks"]["algo_version"],
            "order_blocks": obs,
            "fvgs": fvgs,
        }

    async def get_full_analysis(
        self, symbol: str, timeframe: str, include_mitigated: bool = False, lookback: int = PA_LOOKBACK
    ) -> dict[str, Any]:
        result = await self.analyze(symbol, timeframe, lookback)
        if include_mitigated:
            zones = await self._merged_zones("liquidity_zones", symbol, timeframe, "zone_id")
            obs = await self._merged_zones("order_blocks", symbol, timeframe, "zone_id", list_key="order_blocks")
            fvgs = await self._merged_zones("order_blocks", symbol, timeframe, "zone_id", list_key="fvgs")
        else:
            zones = [z for z in result["liquidity"]["zones"] if not z["mitigated"]]
            obs = [z for z in result["order_blocks"]["order_blocks"] if not z["mitigated"]]
            fvgs = [z for z in result["order_blocks"]["fvgs"] if not z["mitigated"]]

        vwap_points = result["vwap"].get("points", [])[-MAX_VWAP_POINTS:]
        versions = {
            "structure": result["structure"]["algo_version"],
            "liquidity": result["liquidity"]["algo_version"],
            "order_blocks": result["order_blocks"]["algo_version"],
            "vwap": result["vwap"]["algo_version"],
            "sessions": result["sessions"]["algo_version"],
        }
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "as_of": result["as_of"],
            "algo_version": ",".join(versions.values()),
            "versions": versions,
            "structure": result["structure"],
            "liquidity": {
                "algo_version": result["liquidity"]["algo_version"],
                "zones": zones,
                "score": result["liquidity"]["score"],
            },
            "order_blocks": {
                "algo_version": result["order_blocks"]["algo_version"],
                "order_blocks": obs,
                "fvgs": fvgs,
            },
            "vwap": {
                "algo_version": result["vwap"]["algo_version"],
                "current": result["vwap"]["current"],
                "anchored_at": result["vwap"]["anchored_at"],
                "points": vwap_points,
            },
            "sessions": result["sessions"],
        }

    # ---------- history merging ----------

    async def _merged_zones(
        self, table: str, symbol: str, timeframe: str, zone_key: str, list_key: str | None = None
    ) -> list[dict]:
        """Merge zones from all stored records by zone_key (the newest state wins)."""
        history = await _read_history(self.db, table, symbol, timeframe)
        order: list[str] = []
        seen: dict[str, dict] = {}
        for row in history:
            payload = json.loads(row["payload"])
            zones = payload.get(list_key) if list_key else payload.get("zones", [])
            for z in zones or []:
                key = z.get(zone_key)
                if key is None:
                    continue
                if key not in seen:
                    order.append(key)
                seen[key] = z
        return [seen[k] for k in order]

    # ---------- freshness ----------

    @staticmethod
    def freshness_for(timeframe: str, as_of: int | None, now: float | None = None) -> str:
        """Freshness label for a PA snapshot (2.15 fix — semantics clarified).

        `fresh` means the analysis includes the timeframe's **last closed candle**:
        `as_of` (the `open_time` of the analysis's last closed candle) is not less
        than the last closed candle's `open_time` (`latest_closed`) on the wall clock.
        This is not an instantaneous real-time snapshot like a ticker; bar-based
        analysis is naturally at most one period behind. Analysis based on an older
        period (with the last closed candle missing) is `stale`.

        If `now` is omitted, use local time; for consistency with the data layer,
        prefer `PAEngine.freshness()` through the instance (server clock, T2).
        """
        if as_of is None:
            return FRESHNESS_STALE
        period = TIMEFRAME_SECONDS[timeframe]
        if now is None:
            now = time.time()
        latest_closed = int(now // period) * period - period
        return FRESHNESS_FRESH if as_of >= latest_closed else FRESHNESS_STALE

    def freshness(self, timeframe: str, as_of: int | None) -> str:
        """Instance-clock freshness: pipeline server clock, or local time (T2)."""
        return self.freshness_for(timeframe, as_of, now=self._now())
