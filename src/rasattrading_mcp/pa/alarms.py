"""2.6 — Alarm engine (passive but persistent registry).

State machine: `armed → triggered → cooldown → armed`.
- Triggering is event-driven when PA analysis is updated (`PAEngine.analyze` →
  `on_analysis_updated`).
- Dedup: `triggered_alerts(alert_id, trigger_key)` UNIQUE — the same alert does
  not trigger again for the same candle/data window (`trigger_key`).
- Persistence: triggered records remain in `triggered_alerts`; records triggered
  while the agent is offline can later be read with `get_triggered_alerts`.
- Stale rule: do not trigger on stale data (defer evaluation for missing/stale
  symbols; never silently trigger on old data).

Conditions use the screener's allowlisted filter AST (the same safe evaluation).
2.19: triggering can run an external notification command (`notify_command`, for
example `agent notify`); if an alert definition carries `order_spec`, triggering
creates an approval-pending order record in `pending_orders` — the order is NOT
opened automatically and is opened only after approval.
"""

from __future__ import annotations

import json
import logging
import math
import re
import shlex
import subprocess
import time
import uuid
from typing import Any

from ..envelope import FRESHNESS_FRESH
from ..errors import ErrorCode, RasatError
from ..storage.db import Database
from ..storage.orders import STATUS_PAPER
from ..storage.state import (
    PENDING_APPROVED,
    PENDING_EXECUTING,
    PENDING_EXECUTED,
    PENDING_EXPIRED,
    PENDING_RECONCILE_REQUIRED,
    PENDING_REJECTED,
)
from .analysis import PAEngine, _read_current
from .liquidity import load_futures_series
from .screener import _eval_node, validate_filters
from .swings import filter_closed_candles

logger = logging.getLogger("rasattrading.pa.alarms")

STATE_ARMED = "armed"
STATE_TRIGGERED = "triggered"
STATE_COOLDOWN = "cooldown"

# Canonical pending-state names (T00 contract — storage.state is the single source).
PENDING_AWAITING = "awaiting_approval"

# Allowed keys in order_spec (allowlist — no arbitrary parameters).
ORDER_SPEC_KEYS = {"account_id", "symbol", "side", "entry", "stop_loss", "risk_pct", "order_type"}

# {placeholder} marker in the notify template — substitute in one pass so
# placeholder-like text inside values is not substituted a second time.
_PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")


class AlarmService:
    def __init__(
        self,
        db: Database,
        engine: PAEngine | None = None,
        compute_budget: int | None = None,
        notify_command: str | None = None,
    ) -> None:
        self.db = db
        self.engine = engine
        self._notify_command = notify_command
        # K3: bound on-demand `analyze` (and therefore warm-up/backfill load) when
        # no stored analysis exists; permit only a limited number per evaluation pass
        # and defer the rest.
        if compute_budget is None:
            cfg = engine.config if engine is not None else None
            compute_budget = getattr(cfg, "alarm_compute_budget", 8) if cfg is not None else 8
        self._compute_budget = max(0, int(compute_budget))
        self._compute_used = 0

    def begin_evaluation_pass(self) -> None:
        """Start a new evaluation pass: reset the on-demand PA computation budget (K3)."""
        self._compute_used = 0

    # ---------- CRUD ----------

    async def create_alert(
        self,
        symbol: str,
        timeframe: str,
        condition: list | dict,
        cooldown_seconds: int = 300,
        note: str | None = None,
        order_spec: dict | None = None,
    ) -> dict:
        if not isinstance(symbol, str) or not symbol:
            raise RasatError(ErrorCode.INVALID_REQUEST, "symbol is required (string)")
        if not isinstance(timeframe, str) or not timeframe:
            raise RasatError(ErrorCode.INVALID_REQUEST, "timeframe is required (string)")
        if not isinstance(cooldown_seconds, int) or cooldown_seconds < 0:
            raise RasatError(ErrorCode.INVALID_REQUEST, "cooldown_seconds must be a non-negative integer")
        root = validate_filters(condition)
        definition = {
            "type": "simple",
            "symbol": symbol,
            "timeframe": timeframe,
            "condition": root,
            "cooldown_seconds": cooldown_seconds,
            "note": note,
        }
        if order_spec is not None:
            definition["order_spec"] = self._validate_order_spec(order_spec)
        return await self._insert_alert(definition)

    @staticmethod
    def _validate_order_spec(spec: dict) -> dict:
        """Validate the order_spec allowlist (2.19 + T02): known keys only.

        T02: risk_pct is required for an approval-pending order (used for sizing
        during approval) and must be finite; entry for a limit order must be a
        positive finite number — INVALID_REQUEST before reaching the broker.
        """
        if not isinstance(spec, dict):
            raise RasatError(ErrorCode.INVALID_REQUEST, "order_spec must be an object")
        for key in spec:
            if key not in ORDER_SPEC_KEYS:
                raise RasatError(ErrorCode.INVALID_REQUEST, f"unknown order_spec key: {key}")
        for required in ("account_id", "symbol", "side"):
            if not spec.get(required):
                raise RasatError(ErrorCode.INVALID_REQUEST, f"order_spec.{required} is required")
        if spec["side"] not in ("BUY", "SELL"):
            raise RasatError(ErrorCode.INVALID_REQUEST, "order_spec.side must be BUY|SELL")
        if spec.get("order_type", "market") not in ("market", "limit"):
            raise RasatError(ErrorCode.INVALID_REQUEST, "order_spec.order_type must be market|limit")
        AlarmService._validate_execution_inputs(
            risk_pct=spec.get("risk_pct"),
            entry=spec.get("entry"),
            stop_loss=spec.get("stop_loss"),
            order_type=spec.get("order_type"),
        )
        return dict(spec)

    @staticmethod
    def _validate_execution_inputs(risk_pct: Any, entry: Any, stop_loss: Any = None, order_type: Any = None) -> None:
        """Execution preflight before approval (T02): risk_pct required + finite.

        Alert creation (`_validate_order_spec`) and the approval handler share this
        validation. NaN/Infinity cannot pass fail-open; missing risk_pct returns
        INVALID_REQUEST before reaching the broker.
        """
        if risk_pct is None or isinstance(risk_pct, bool):
            raise RasatError(ErrorCode.INVALID_REQUEST, "order_spec.risk_pct is required (for post-approval sizing)")
        if not isinstance(risk_pct, (int, float)):
            raise RasatError(ErrorCode.INVALID_REQUEST, "order_spec.risk_pct must be a number")
        risk = float(risk_pct)
        if not math.isfinite(risk) or not (0 < risk <= 1):
            raise RasatError(ErrorCode.INVALID_REQUEST, "order_spec.risk_pct must be finite and in the (0,1] range")
        for name, value in (("entry", entry), ("stop_loss", stop_loss)):
            if value is None:
                continue
            if isinstance(value, bool):
                raise RasatError(ErrorCode.INVALID_REQUEST, f"order_spec.{name} must be a number")
            if not isinstance(value, (int, float)):
                raise RasatError(ErrorCode.INVALID_REQUEST, f"order_spec.{name} must be a number")
            value_f = float(value)
            if not math.isfinite(value_f) or value_f <= 0:
                raise RasatError(ErrorCode.INVALID_REQUEST, f"order_spec.{name} must be positive and finite")
        if (order_type or "market").lower() == "limit":
            if entry is None:
                raise RasatError(ErrorCode.INVALID_REQUEST, "order_spec.entry is required for a limit order")

    async def create_composite_alert(
        self,
        clauses: list[dict],
        combine: str = "AND",
        cooldown_seconds: int = 300,
        note: str | None = None,
    ) -> dict:
        if not isinstance(clauses, list) or not clauses:
            raise RasatError(ErrorCode.INVALID_REQUEST, "at least one clause is required")
        if not isinstance(cooldown_seconds, int) or cooldown_seconds < 0:
            raise RasatError(ErrorCode.INVALID_REQUEST, "cooldown_seconds must be a non-negative integer")
        combine = (combine or "AND").upper()
        if combine not in ("AND", "OR"):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"combine must be AND|OR: {combine}")
        norm_clauses = []
        for cl in clauses:
            if not isinstance(cl, dict) or not cl.get("symbol") or not cl.get("timeframe"):
                raise RasatError(ErrorCode.INVALID_REQUEST, "each clause must contain symbol+timeframe+filters")
            norm_clauses.append(
                {
                    "symbol": cl["symbol"],
                    "timeframe": cl["timeframe"],
                    "condition": validate_filters(cl.get("filters")),
                }
            )
        definition = {
            "type": "composite",
            "combine": combine,
            "clauses": norm_clauses,
            "cooldown_seconds": cooldown_seconds,
            "note": note,
        }
        return await self._insert_alert(definition)

    async def _insert_alert(self, definition: dict) -> dict:
        alert_id = uuid.uuid4().hex
        now = int(time.time())

        def _w(conn):
            conn.execute(
                "INSERT INTO alerts (alert_id, definition, state, created_at, updated_at, cooldown_until) "
                "VALUES (?,?,?,?,?,NULL)",
                (alert_id, json.dumps(definition, ensure_ascii=False), STATE_ARMED, now, now),
            )
            return alert_id

        await self.db.write(_w)
        return self._summary(alert_id, definition, STATE_ARMED, now)

    async def delete_alert(self, alert_id: str) -> int:
        if not isinstance(alert_id, str) or not alert_id:
            raise RasatError(ErrorCode.INVALID_REQUEST, "alert_id is required (string)")

        def _w(conn) -> int:
            cur = conn.execute("DELETE FROM alerts WHERE alert_id=?", (alert_id,))
            return cur.rowcount

        removed = await self.db.write(_w)
        if removed == 0:
            raise RasatError(ErrorCode.NOT_FOUND, f"alert not found: {alert_id}")
        return removed

    async def list_alerts(self) -> list[dict]:
        def _q(conn):
            rows = conn.execute("SELECT * FROM alerts ORDER BY created_at DESC").fetchall()
            return [dict(r) for r in rows]

        rows = await self.db.read(_q)
        now = int(time.time())
        out = []
        for r in rows:
            definition = json.loads(r["definition"])
            state = self._lazy_state(r["state"], r.get("cooldown_until"), now)
            out.append(self._summary(r["alert_id"], definition, state, r["created_at"], cooldown_until=r.get("cooldown_until")))
        return out

    # ---------- state machine ----------

    @staticmethod
    def _lazy_state(state: str, cooldown_until: int | None, now: int) -> str:
        """Derive visible state from persistent `state` + `cooldown_until`.

        - `cooldown_until` in the future → `cooldown` (persistent, survives restart).
        - `cooldown_until` in the past → `armed` (cooldown ended).
        - `triggered` + cooldown_until NULL (cooldown=0) → immediately reusable → `armed`.
        """
        if cooldown_until is not None:
            if now < cooldown_until:
                return STATE_COOLDOWN
            return STATE_ARMED
        if state == STATE_TRIGGERED:
            return STATE_ARMED
        return state

    async def _maybe_trigger(self, alert_id: str, trigger_key: str, payload: dict, cooldown_seconds: int) -> dict | None:
        """Persist a matching condition after dedup and state checks.

        2.19: on trigger, (a) run the external `notify_command` (wake/notify the
        agent), and (b) if the alert definition has `order_spec`, add an
        `awaiting_approval` record to `pending_orders`; the order is NOT opened
        automatically and waits for `approve_pending_order`.
        """
        now = int(time.time())
        definition: dict = {}

        def _w(conn):
            nonlocal definition
            row = conn.execute("SELECT state, cooldown_until, definition FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
            if row is None:
                raise RasatError(ErrorCode.NOT_FOUND, f"alert not found: {alert_id}")
            definition = json.loads(row["definition"])
            state = self._lazy_state(row["state"], row["cooldown_until"], now)
            if state != STATE_ARMED:
                return None  # In cooldown → do not trigger.
            exists = conn.execute(
                "SELECT 1 FROM triggered_alerts WHERE alert_id=? AND trigger_key=?", (alert_id, trigger_key)
            ).fetchone()
            if exists is not None:
                return None  # Same data window → dedup.
            conn.execute(
                "INSERT INTO triggered_alerts (alert_id, trigger_key, payload, triggered_at) VALUES (?,?,?,?)",
                (alert_id, trigger_key, json.dumps(payload, ensure_ascii=False), now),
            )
            order_spec = definition.get("order_spec")
            if order_spec is not None:
                conn.execute(
                    "INSERT INTO pending_orders (order_id, alert_id, account_id, symbol, side, order_type, "
                    "entry, stop_loss, risk_pct, status, created_at, note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        uuid.uuid4().hex, alert_id, order_spec["account_id"], order_spec["symbol"],
                        order_spec["side"], order_spec.get("order_type", "market"),
                        order_spec.get("entry"), order_spec.get("stop_loss"), order_spec.get("risk_pct"),
                        PENDING_AWAITING, now, definition.get("note"),
                    ),
                )
            if cooldown_seconds > 0:
                conn.execute(
                    "UPDATE alerts SET state=?, cooldown_until=?, updated_at=? WHERE alert_id=?",
                    (STATE_COOLDOWN, now + cooldown_seconds, now, alert_id),
                )
            else:
                conn.execute(
                    "UPDATE alerts SET state=?, cooldown_until=NULL, updated_at=? WHERE alert_id=?",
                    (STATE_TRIGGERED, now, alert_id),
                )
            return {"alert_id": alert_id, "trigger_key": trigger_key, "triggered_at": now}

        res = await self.db.write(_w)
        if res is not None:
            self._notify(definition, payload, res.get("alert_id"))
        return res

    def _notify(self, definition: dict, payload: dict, alert_id: str | None = None) -> None:
        """External notification command (2.19; T02 safe argv): fire-and-forget.

        Parse the command template into a safe argv list with `shlex.split` and
        run with `shell=False`; pipes, redirections, and shell expansion are not
        supported (they remain literal argv elements). Substitute the {alert_id},
        {symbol}, {timeframe}, {note}, and {price} placeholders per token in one
        pass; values are ordinary argv elements, not shell code (`; whoami` inside
        `note` does not start a process).

        On parse failure, notification stays fail closed: start no process, do not
        change alert state, and only log. Do not log the full command (no note/sensitive
        content leakage).
        """
        command = self._notify_command
        if not command:
            return
        fmt = {
            "alert_id": str(alert_id or payload.get("alert_id", "")),
            "symbol": str(payload.get("symbol", "")),
            "timeframe": str(payload.get("timeframe", "")),
            "note": str(definition.get("note", "") or ""),
            "price": str(payload.get("close", "") or ""),
        }
        try:
            template = shlex.split(command)
        except ValueError as exc:
            logger.warning("could not parse alert notification template (notification skipped): %s", exc)
            return
        argv = [_PLACEHOLDER_RE.sub(lambda m: fmt.get(m.group(1), m.group(0)), arg) for arg in template]
        try:
            if hasattr(subprocess, "CREATE_NO_WINDOW"):
                flags = subprocess.CREATE_NO_WINDOW
            else:
                flags = 0
            subprocess.Popen(
                argv,
                shell=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=flags,
            )
            logger.info("alert notification sent: %s", payload.get("alert_id"))
        except Exception as exc:  # noqa: BLE001
            logger.warning("alert notification failed: %s", exc)

    # ---------- evaluation ----------

    async def _clause_context(self, symbol: str, timeframe: str) -> dict | None:
        """Build current analysis context for a symbol (stored or computed on demand)."""
        candles = await self.engine._read_candles(symbol, timeframe, 300)
        if not candles:
            return None
        candles = filter_closed_candles(candles, timeframe)
        if not candles:
            return None
        ms = await _read_current(self.db, "market_structure", symbol, timeframe)
        lz = await _read_current(self.db, "liquidity_zones", symbol, timeframe)
        ob = await _read_current(self.db, "order_blocks", symbol, timeframe)
        if ms is None or lz is None or ob is None:
            if self._compute_used >= self._compute_budget:
                return None  # K3: on-demand PA budget exhausted → defer this pass.
            self._compute_used += 1
            await self.engine.analyze(symbol, timeframe)
            ms = await _read_current(self.db, "market_structure", symbol, timeframe)
            lz = await _read_current(self.db, "liquidity_zones", symbol, timeframe)
            ob = await _read_current(self.db, "order_blocks", symbol, timeframe)
        if ms is None:
            return None
        as_of = ms["effective_from"]
        if self.engine.freshness(timeframe, as_of) != FRESHNESS_FRESH:
            return None  # Stale → defer evaluation (do not trigger).
        from .vwap_sessions import compute_vwap

        ctx: dict[str, Any] = {
            "candles": candles,
            "close": candles[-1]["close"],
            "as_of": as_of,
            "structure": json.loads(ms["payload"]),
            "liquidity_zones": json.loads(lz["payload"]).get("zones", []) if lz else [],
            "order_blocks": json.loads(ob["payload"]).get("order_blocks", []) if ob else [],
            "vwap": compute_vwap(candles)["current"],
        }
        fr = await self._latest_futures(symbol, "funding_rate")
        if fr is not None:
            ctx["funding_rate"] = fr
        oi = await load_futures_series(self.db, symbol, "open_interest")
        if oi:
            ctx["oi_series"] = oi
        return ctx

    async def _latest_futures(self, symbol: str, ftype: str) -> dict | None:
        series = await load_futures_series(self.db, symbol, ftype, limit=1)
        return series[-1] if series else None

    async def on_analysis_updated(self, symbol: str, timeframe: str, analysis: dict) -> list[dict]:
        """Evaluate related simple and composite alerts after PA analysis updates.

        Freshness gate (2.8): fail closed and do not trigger when the analysis
        snapshot is stale. Build context from closed candles; exclude the forming bar.
        """
        as_of = analysis.get("as_of")
        if self.engine.freshness(timeframe, as_of) != FRESHNESS_FRESH:
            return []  # Stale analysis → no trigger.
        triggers: list[dict] = []
        candles = await self.engine._read_candles(symbol, timeframe, 300)
        candles = filter_closed_candles(candles, timeframe)
        ctx = self._ctx_from_analysis(analysis, candles)
        fr = await self._latest_futures(symbol, "funding_rate")
        if fr is not None:
            ctx["funding_rate"] = fr
        oi = await load_futures_series(self.db, symbol, "open_interest")
        if oi:
            ctx["oi_series"] = oi
        alerts = await self._alerts_by_symbol(symbol, timeframe)
        for alert in alerts:
            definition = json.loads(alert["definition"])
            if definition.get("type") != "simple":
                continue  # Composite alerts are evaluated below.
            state = self._lazy_state(alert["state"], alert.get("cooldown_until"), int(time.time()))
            if state != STATE_ARMED:
                continue
            matched = _eval_node(definition["condition"], ctx)
            if not matched:
                continue
            trigger_key = f"{symbol}:{timeframe}:{analysis['as_of']}"
            res = await self._maybe_trigger(
                alert["alert_id"], trigger_key, {"symbol": symbol, "timeframe": timeframe, "as_of": analysis["as_of"]},
                definition["cooldown_seconds"],
            )
            if res:
                triggers.append(res)
        triggers.extend(await self._evaluate_composites_for(symbol, timeframe))
        return triggers

    async def evaluate_symbol(self, symbol: str, timeframe: str) -> list[dict]:
        """On-demand evaluation from stored analysis (for the daemon background loop).

        If no PA record exists, `_clause_context` computes the analysis so the
        alarm loop relies on current records without an agent tool call (not empty).
        """
        alerts = await self._alerts_by_symbol(symbol, timeframe)
        triggers: list[dict] = []
        for alert in alerts:
            definition = json.loads(alert["definition"])
            if definition.get("type") != "simple":
                continue
            state = self._lazy_state(alert["state"], alert.get("cooldown_until"), int(time.time()))
            if state != STATE_ARMED:
                continue
            ctx = await self._clause_context(symbol, timeframe)
            if ctx is None:
                continue
            if not _eval_node(definition["condition"], ctx):
                continue
            res = await self._maybe_trigger(
                alert["alert_id"], f"{symbol}:{timeframe}:{ctx['as_of']}",
                {"symbol": symbol, "timeframe": timeframe, "as_of": ctx["as_of"]},
                definition["cooldown_seconds"],
            )
            if res:
                triggers.append(res)
        triggers.extend(await self._evaluate_composites_for(symbol, timeframe))
        return triggers

    async def _evaluate_composites_for(self, symbol: str, timeframe: str) -> list[dict]:
        triggers: list[dict] = []
        for alert in await self._composite_alerts():
            definition = json.loads(alert["definition"])
            if not any(c["symbol"] == symbol and c["timeframe"] == timeframe for c in definition["clauses"]):
                continue
            res = await self._evaluate_composite(alert)
            if res:
                triggers.append(res)
        return triggers

    async def _evaluate_composite(self, alert: dict) -> dict | None:
        definition = json.loads(alert["definition"])
        state = self._lazy_state(alert["state"], alert.get("cooldown_until"), int(time.time()))
        if state != STATE_ARMED:
            return None
        contexts: list[dict] = []
        for clause in definition["clauses"]:
            ctx = await self._clause_context(clause["symbol"], clause["timeframe"])
            if ctx is None:
                return None  # defer when any clause has missing or stale data
            contexts.append((clause, ctx))
        if definition["combine"] == "AND":
            matched = all(_eval_node(c["condition"], ctx) for c, ctx in contexts)
        else:
            matched = any(_eval_node(c["condition"], ctx) for c, ctx in contexts)
        if not matched:
            return None
        as_of = max(ctx["as_of"] for _, ctx in contexts)
        return await self._maybe_trigger(
            alert["alert_id"],
            f"composite:{alert['alert_id']}:{as_of}",
            {"type": "composite", "as_of": as_of},
            definition["cooldown_seconds"],
        )

    # ---------- query helpers ----------

    @staticmethod
    def _ctx_from_analysis(analysis: dict, candles: list[dict] | None = None) -> dict:
        candles = candles or []
        return {
            "candles": candles,
            "close": candles[-1]["close"] if candles else None,
            "as_of": analysis["as_of"],
            "structure": analysis["structure"],
            "liquidity_zones": analysis["liquidity"]["zones"],
            "order_blocks": analysis["order_blocks"]["order_blocks"],
            "vwap": analysis["vwap"]["current"],
        }

    async def _alerts_by_symbol(self, symbol: str, timeframe: str) -> list[dict]:
        def _q(conn):
            rows = conn.execute(
                "SELECT * FROM alerts WHERE state IN ('armed','triggered') OR cooldown_until IS NULL OR cooldown_until <= ?",
                (int(time.time()),),
            ).fetchall()
            out = []
            for r in rows:
                definition = json.loads(r["definition"])
                if definition.get("type") == "simple":
                    if definition.get("symbol") == symbol and definition.get("timeframe") == timeframe:
                        out.append(dict(r))
                elif any(c["symbol"] == symbol and c["timeframe"] == timeframe for c in definition.get("clauses", [])):
                    out.append(dict(r))
            return out

        return await self.db.read(_q)

    async def _composite_alerts(self) -> list[dict]:
        def _q(conn):
            rows = conn.execute("SELECT * FROM alerts").fetchall()
            return [
                dict(r) for r in rows if json.loads(r["definition"]).get("type") == "composite"
            ]

        return await self.db.read(_q)

    async def alert_symbols(self) -> list[tuple[str, str]]:
        """Return the (symbol, timeframe) pairs needed by all alerts (simple + composite clauses)."""
        def _q(conn):
            rows = conn.execute("SELECT definition FROM alerts").fetchall()
            pairs: set[tuple[str, str]] = set()
            for r in rows:
                d = json.loads(r["definition"])
                if d.get("type") == "simple":
                    pairs.add((d["symbol"], d["timeframe"]))
                else:
                    for c in d.get("clauses", []):
                        pairs.add((c["symbol"], c["timeframe"]))
            return sorted(pairs)

        return await self.db.read(_q)

    async def get_triggered_alerts(self, alert_id: str | None = None, limit: int = 50, cursor: int | None = None) -> dict:
        if not 1 <= limit <= 500:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"limit must be between 1 and 500 (given: {limit})")

        def _q(conn):
            sql = "SELECT alert_id, trigger_key, payload, triggered_at FROM triggered_alerts"
            params: tuple = ()
            if alert_id:
                sql += " WHERE alert_id=?"
                params = (alert_id,)
            sql += " ORDER BY triggered_at DESC, id DESC LIMIT ? OFFSET ?"
            params = params + (limit + 1, cursor or 0)
            rows = conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

        rows = await self.db.read(_q)
        has_more = len(rows) > limit
        rows = rows[:limit]
        return {
            "triggered": [
                {
                    "alert_id": r["alert_id"],
                    "trigger_key": r["trigger_key"],
                    "triggered_at": r["triggered_at"],
                    "payload": json.loads(r["payload"]),
                }
                for r in rows
            ],
            "next_cursor": (cursor or 0) + len(rows) if has_more else None,
        }

    # ---------- approval-pending orders (2.19) ----------

    async def get_pending_orders(self, status: str | None = None, limit: int = 50) -> dict:
        """List approval-pending and historical pending-order records."""
        if not 1 <= limit <= 500:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"limit must be between 1 and 500 (given: {limit})")
        valid_statuses = (
            PENDING_AWAITING,
            PENDING_APPROVED,
            PENDING_EXECUTING,
            PENDING_EXECUTED,
            PENDING_REJECTED,
            PENDING_RECONCILE_REQUIRED,
            PENDING_EXPIRED,
        )
        if status is not None and status not in valid_statuses:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"unknown status: {status}")

        def _q(conn):
            sql = "SELECT * FROM pending_orders"
            params: tuple = ()
            if status:
                sql += " WHERE status=?"
                params = (status,)
            sql += " ORDER BY created_at DESC LIMIT ?"
            rows = conn.execute(sql, params + (limit,)).fetchall()
            return [dict(r) for r in rows]

        rows = await self.db.read(_q)
        return {
            "pending": [
                {
                    "order_id": r["order_id"],
                    "alert_id": r["alert_id"],
                    "account_id": r["account_id"],
                    "symbol": r["symbol"],
                    "side": r["side"],
                    "order_type": r["order_type"],
                    "entry": r["entry"],
                    "stop_loss": r["stop_loss"],
                    "risk_pct": r["risk_pct"],
                    "status": r["status"],
                    "note": r["note"],
                    "created_at": r["created_at"],
                    "executed_order_id": r["executed_order_id"],
                    "execution_error_code": r["execution_error_code"],
                    "execution_error_message": r["execution_error_message"],
                    "last_attempt_at": r["last_attempt_at"],
                }
                for r in rows
            ],
            "count": len(rows),
        }

    async def _pending_order(self, order_id: str) -> dict:
        def _q(conn):
            row = conn.execute("SELECT * FROM pending_orders WHERE order_id=?", (order_id,)).fetchone()
            return dict(row) if row else None

        rec = await self.db.read(_q)
        if rec is None:
            raise RasatError(ErrorCode.NOT_FOUND, f"pending order not found: {order_id}")
        return rec

    async def _fill_pending_entry(self, order_id: str, entry: float) -> None:
        """Fill a missing market-order entry with the current market price on approval (T3).

        Storage requires entry; a missing value in an approved record used to create
        a permanent rejection. Persist the price so re-approval/audit remains consistent.
        """

        def _w(conn):
            conn.execute("UPDATE pending_orders SET entry=? WHERE order_id=?", (entry, order_id))

        await self.db.write(_w)

    async def approve_pending_order(self, order_id: str) -> dict:
        """Approve: `awaiting_approval → approved`. The handler opens the order."""
        now = int(time.time())

        def _w(conn):
            cur = conn.execute(
                "UPDATE pending_orders SET status=?, approved_at=? WHERE order_id=? AND status=?",
                (PENDING_APPROVED, now, order_id, PENDING_AWAITING),
            )
            if cur.rowcount == 0:
                row = conn.execute("SELECT status FROM pending_orders WHERE order_id=?", (order_id,)).fetchone()
                if row is None:
                    raise RasatError(ErrorCode.NOT_FOUND, f"pending order not found: {order_id}")
                raise RasatError(ErrorCode.INVALID_REQUEST, f"not approvable in current state: {row['status']}")
            return {"order_id": order_id, "status": PENDING_APPROVED}

        return await self.db.write(_w)

    async def reject_pending_order(self, order_id: str, reason: str | None = None) -> dict:
        """Reject: `awaiting_approval → rejected` (the order is not opened)."""
        now = int(time.time())

        def _w(conn):
            cur = conn.execute(
                "UPDATE pending_orders SET status=?, rejected_at=?, reject_reason=? WHERE order_id=? AND status=?",
                (PENDING_REJECTED, now, reason, order_id, PENDING_AWAITING),
            )
            if cur.rowcount == 0:
                row = conn.execute("SELECT status FROM pending_orders WHERE order_id=?", (order_id,)).fetchone()
                if row is None:
                    raise RasatError(ErrorCode.NOT_FOUND, f"pending order not found: {order_id}")
                raise RasatError(ErrorCode.INVALID_REQUEST, f"not rejectable in current state: {row['status']}")
            return {"order_id": order_id, "status": PENDING_REJECTED}

        return await self.db.write(_w)

    async def approve_and_claim_pending_execution(self, order_id: str) -> dict:
        """Approval + execution claim (T00 CAS): `awaiting_approval → approved → executing`.

        Perform both CAS transitions in one transaction; only one of two concurrent
        `approve` calls can claim execution (the other gets INVALID_REQUEST). Commit
        the transaction atomically; there is no post-approval window stuck in `approved`.
        """
        now = int(time.time())

        def _w(conn):
            cur = conn.execute(
                "UPDATE pending_orders SET status=?, approved_at=? WHERE order_id=? AND status=?",
                (PENDING_APPROVED, now, order_id, PENDING_AWAITING),
            )
            if cur.rowcount == 0:
                row = conn.execute("SELECT status FROM pending_orders WHERE order_id=?", (order_id,)).fetchone()
                if row is None:
                    raise RasatError(ErrorCode.NOT_FOUND, f"pending order not found: {order_id}")
                raise RasatError(ErrorCode.INVALID_REQUEST, f"not approvable in current state: {row['status']}")
            cur2 = conn.execute(
                "UPDATE pending_orders SET status=?, execution_started_at=?, last_attempt_at=?, "
                "execution_error_code=NULL, execution_error_message=NULL "
                "WHERE order_id=? AND status=?",
                (PENDING_EXECUTING, now, now, order_id, PENDING_APPROVED),
            )
            if cur2.rowcount == 0:
                raise RasatError(ErrorCode.INTERNAL_ERROR, "could not claim execution")
            return {"order_id": order_id, "status": PENDING_EXECUTING}

        return await self.db.write(_w)

    async def complete_pending_execution(self, order_id: str, executed_order_id: str | None) -> dict:
        """Terminal success (CAS): `executing → executed`.

        Populate `executed_order_id` only with a definite real order ID
        (real → exchange_order_id, paper → local order record).
        """
        now = int(time.time())

        def _w(conn):
            cur = conn.execute(
                "UPDATE pending_orders SET status=?, executed_order_id=?, execution_finished_at=?, "
                "last_attempt_at=? WHERE order_id=? AND status=?",
                (PENDING_EXECUTED, executed_order_id, now, now, order_id, PENDING_EXECUTING),
            )
            if cur.rowcount == 0:
                row = conn.execute("SELECT status FROM pending_orders WHERE order_id=?", (order_id,)).fetchone()
                if row is None:
                    raise RasatError(ErrorCode.NOT_FOUND, f"pending order not found: {order_id}")
                raise RasatError(ErrorCode.INVALID_REQUEST, f"cannot transition to executed from current state: {row['status']}")
            return {"order_id": order_id, "status": PENDING_EXECUTED, "executed_order_id": executed_order_id}

        return await self.db.write(_w)

    async def fail_pending_execution(
        self, order_id: str, *, status: str, error_code: str, error_message: str
    ) -> dict:
        """Terminal/error (CAS): `executing → rejected | reconcile_required`.

        After an exception/timeout, do not leave the record locked in `approved`;
        write the correct terminal or reconcile state plus execution error code/message.
        """
        if status not in (PENDING_REJECTED, PENDING_RECONCILE_REQUIRED):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"invalid failure status: {status}")
        now = int(time.time())

        def _w(conn):
            cur = conn.execute(
                "UPDATE pending_orders SET status=?, execution_error_code=?, execution_error_message=?, "
                "execution_finished_at=?, last_attempt_at=? WHERE order_id=? AND status=?",
                (status, error_code, error_message, now, now, order_id, PENDING_EXECUTING),
            )
            if cur.rowcount == 0:
                row = conn.execute("SELECT status FROM pending_orders WHERE order_id=?", (order_id,)).fetchone()
                if row is None:
                    raise RasatError(ErrorCode.NOT_FOUND, f"pending order not found: {order_id}")
                raise RasatError(ErrorCode.INVALID_REQUEST, f"cannot transition to {status} (current state: {row['status']})")
            return {"order_id": order_id, "status": status, "execution_error_code": error_code}

        return await self.db.write(_w)

    async def reconcile_pending_executions(self, stale_after_seconds: int = 300) -> dict:
        """Recover approved orders left in `executing` after a daemon restart (T3).

        A crash may occur after claiming `executing` but before processing the order
        result. For each record, look for `idempotency_key = 'pending:' || order_id`
        in the `orders` table:

        - If a matching order record exists, transition by status:
          FILLED/paper → `complete_pending_execution`, REJECTED/CANCELED/EXPIRED →
          `fail_pending_execution(rejected)`, others (NEW/PARTIALLY_FILLED/UNKNOWN)
          → `fail_pending_execution(reconcile_required)`.
        - If no match exists and `execution_started_at` is older than the threshold,
          transition fail closed to `reconcile_required` (the order may exist on the
          exchange; do not blindly resubmit). Leave fresh records untouched; the
          daemon may still be processing them (re-evaluate at the next startup).
        """
        now = int(time.time())

        def _executing(conn):
            rows = conn.execute(
                "SELECT * FROM pending_orders WHERE status=?",
                (PENDING_EXECUTING,),
            ).fetchall()
            return [dict(r) for r in rows]

        records = await self.db.read(_executing)
        matched = 0
        completed = 0
        failed = 0
        untouched = 0
        details: list[dict] = []
        for rec in records:
            order_id = rec["order_id"]

            def _match(conn, oid: str = order_id, acc: str = rec["account_id"]):
                row = conn.execute(
                    "SELECT status, exchange_order_id FROM orders "
                    "WHERE account_id=? AND idempotency_key=?",
                    (acc, f"pending:{oid}"),
                ).fetchone()
                return dict(row) if row is not None else None

            order = await self.db.read(_match)
            if order is not None:
                matched += 1
                exec_status = order["status"]
                if exec_status in ("FILLED", STATUS_PAPER):
                    await self.complete_pending_execution(order_id, order["exchange_order_id"])
                    completed += 1
                    details.append({"order_id": order_id, "action": "completed", "status": exec_status})
                elif exec_status in ("REJECTED", "CANCELED", "EXPIRED"):
                    await self.fail_pending_execution(
                        order_id,
                        status=PENDING_REJECTED,
                        error_code=ErrorCode.ORDER_REJECTED,
                        error_message=f"restart reconciliation: order definitively ended ({exec_status})",
                    )
                    failed += 1
                    details.append({"order_id": order_id, "action": "rejected", "status": exec_status})
                else:
                    # NEW/PARTIALLY_FILLED/UNKNOWN → non-terminal/ambiguous.
                    await self.fail_pending_execution(
                        order_id,
                        status=PENDING_RECONCILE_REQUIRED,
                        error_code=ErrorCode.ORDER_UNKNOWN,
                        error_message="restart reconciliation: order status is not definitive",
                    )
                    failed += 1
                    details.append({"order_id": order_id, "action": "reconcile_required", "status": exec_status})
                continue

            started = rec.get("execution_started_at")
            if started is not None and now - int(started) > stale_after_seconds:
                await self.fail_pending_execution(
                    order_id,
                    status=PENDING_RECONCILE_REQUIRED,
                    error_code=ErrorCode.ORDER_UNKNOWN,
                    error_message="restart reconciliation: order record not found; fail-closed reconciliation",
                )
                failed += 1
                details.append({"order_id": order_id, "action": "reconcile_required_no_order"})
            else:
                untouched += 1
                details.append({"order_id": order_id, "action": "untouched"})

        return {
            "scanned": len(records),
            "matched": matched,
            "completed": completed,
            "failed": failed,
            "untouched": untouched,
            "details": details,
        }

    @staticmethod
    def _summary(alert_id: str, definition: dict, state: str, created_at: int, cooldown_until: int | None = None) -> dict:
        out: dict[str, Any] = {
            "alert_id": alert_id,
            "type": definition["type"],
            "state": state,
            "cooldown_seconds": definition["cooldown_seconds"],
            "note": definition.get("note"),
            "created_at": created_at,
            "cooldown_until": cooldown_until,
        }
        if definition["type"] == "simple":
            out["symbol"] = definition["symbol"]
            out["timeframe"] = definition["timeframe"]
            out["condition"] = definition["condition"]
        else:
            out["combine"] = definition["combine"]
            out["clauses"] = [
                {"symbol": c["symbol"], "timeframe": c["timeframe"], "condition": c["condition"]}
                for c in definition["clauses"]
            ]
        return out
