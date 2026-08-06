"""2.6 — Alarm Motoru (pasif ama kalıcı kayıt defteri).

State machine: `armed → triggered → cooldown → armed`.
- Tetikleme, PA analizi güncellendiğinde (`PAEngine.analyze` → `on_analysis_updated`)
  event-driven çalışır.
- Dedup: `triggered_alerts(alert_id, trigger_key)` UNIQUE — aynı bar/veri
  penceresi (trigger_key) aynı alarmı tekrar tetiklemez.
- Kalıcılık: tetiklenen kayıtlar `triggered_alerts` tablosunda kalıcıdır;
  agent kapalıyken tetiklenenler `get_triggered_alerts` ile sonradan okunur.
- Stale kuralı: stale veriye dayalı değerlendirmede tetiklenmez (eksik/stale
  sembol için değerlendirme ertelenir, sessizce eski veriyle tetiklenmez).

Koşullar, screener'daki allowlisted filtre AST'sini kullanır (aynı güvenli
değerlendirme). v1'de dış bildirim (Telegram/webhook) YOK — bu bir pasif
kalıcı kayıt defteridir.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from ..envelope import FRESHNESS_FRESH
from ..errors import ErrorCode, RasatError
from ..storage.db import Database
from .analysis import PAEngine, _read_current
from .liquidity import load_futures_series
from .screener import _eval_node, validate_filters

logger = logging.getLogger("rasattrading.pa.alarms")

STATE_ARMED = "armed"
STATE_TRIGGERED = "triggered"


class AlarmService:
    def __init__(self, db: Database, engine: PAEngine | None = None) -> None:
        self.db = db
        self.engine = engine

    # ---------- CRUD ----------

    async def create_alert(
        self,
        symbol: str,
        timeframe: str,
        condition: list | dict,
        cooldown_seconds: int = 300,
        note: str | None = None,
    ) -> dict:
        if not isinstance(symbol, str) or not symbol:
            raise RasatError(ErrorCode.INVALID_REQUEST, "symbol zorunlu (string)")
        if not isinstance(timeframe, str) or not timeframe:
            raise RasatError(ErrorCode.INVALID_REQUEST, "timeframe zorunlu (string)")
        if not isinstance(cooldown_seconds, int) or cooldown_seconds < 0:
            raise RasatError(ErrorCode.INVALID_REQUEST, "cooldown_seconds >= 0 integer olmalı")
        root = validate_filters(condition)
        definition = {
            "type": "simple",
            "symbol": symbol,
            "timeframe": timeframe,
            "condition": root,
            "cooldown_seconds": cooldown_seconds,
            "note": note,
        }
        return await self._insert_alert(definition)

    async def create_composite_alert(
        self,
        clauses: list[dict],
        combine: str = "AND",
        cooldown_seconds: int = 300,
        note: str | None = None,
    ) -> dict:
        if not isinstance(clauses, list) or not clauses:
            raise RasatError(ErrorCode.INVALID_REQUEST, "en az bir clause gerekli")
        combine = (combine or "AND").upper()
        if combine not in ("AND", "OR"):
            raise RasatError(ErrorCode.INVALID_REQUEST, f"combine yalnızca AND|OR: {combine}")
        norm_clauses = []
        for cl in clauses:
            if not isinstance(cl, dict) or not cl.get("symbol") or not cl.get("timeframe"):
                raise RasatError(ErrorCode.INVALID_REQUEST, "her clause symbol+timeframe+filters taşımalı")
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
            raise RasatError(ErrorCode.INVALID_REQUEST, "alert_id zorunlu (string)")

        def _w(conn) -> int:
            cur = conn.execute("DELETE FROM alerts WHERE alert_id=?", (alert_id,))
            return cur.rowcount

        removed = await self.db.write(_w)
        if removed == 0:
            raise RasatError(ErrorCode.NOT_FOUND, f"alarm bulunamadı: {alert_id}")
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
        if state == STATE_TRIGGERED and cooldown_until is not None and now >= cooldown_until:
            return STATE_ARMED
        return state

    async def _maybe_trigger(self, alert_id: str, trigger_key: str, payload: dict, cooldown_seconds: int) -> dict | None:
        """Koşul eşleşti; dedup + state kontrolü yapıp kalıcı kayda geçer."""
        now = int(time.time())

        def _w(conn):
            row = conn.execute("SELECT state, cooldown_until FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
            if row is None:
                raise RasatError(ErrorCode.NOT_FOUND, f"alarm bulunamadı: {alert_id}")
            state = self._lazy_state(row["state"], row["cooldown_until"], now)
            if state == STATE_TRIGGERED:
                return None  # cooldown'da → tetiklenmez
            exists = conn.execute(
                "SELECT 1 FROM triggered_alerts WHERE alert_id=? AND trigger_key=?", (alert_id, trigger_key)
            ).fetchone()
            if exists is not None:
                return None  # aynı veri penceresi → dedup
            conn.execute(
                "INSERT INTO triggered_alerts (alert_id, trigger_key, payload, triggered_at) VALUES (?,?,?,?)",
                (alert_id, trigger_key, json.dumps(payload, ensure_ascii=False), now),
            )
            conn.execute(
                "UPDATE alerts SET state=?, cooldown_until=?, updated_at=? WHERE alert_id=?",
                (STATE_TRIGGERED, now + cooldown_seconds, now, alert_id),
            )
            return {"alert_id": alert_id, "trigger_key": trigger_key, "triggered_at": now}

        return await self.db.write(_w)

    # ---------- değerlendirme ----------

    async def _clause_context(self, symbol: str, timeframe: str) -> dict | None:
        """Bir sembolün güncel analizini (depolanmış/istenirse hesaplanmış) context yapar."""
        candles = await self.engine._read_candles(symbol, timeframe, 300)
        if not candles:
            return None
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
        as_of = ms["effective_from"]
        if PAEngine.freshness_for(timeframe, as_of) != FRESHNESS_FRESH:
            return None  # stale → değerlendirme ertelenir (tetiklenmez)
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
        oi = await self._latest_futures(symbol, "open_interest")
        if oi is not None:
            ctx["oi_series"] = [oi]
        return ctx

    async def _latest_futures(self, symbol: str, ftype: str) -> dict | None:
        series = await load_futures_series(self.db, symbol, ftype, limit=1)
        return series[-1] if series else None

    async def on_analysis_updated(self, symbol: str, timeframe: str, analysis: dict) -> list[dict]:
        """PA analizi güncellendi → ilgili basit + kompozit alarmları değerlendir."""
        triggers: list[dict] = []
        candles = await self.engine._read_candles(symbol, timeframe, 300)
        ctx = self._ctx_from_analysis(analysis, candles)
        fr = await self._latest_futures(symbol, "funding_rate")
        if fr is not None:
            ctx["funding_rate"] = fr
        oi = await self._latest_futures(symbol, "open_interest")
        if oi is not None:
            ctx["oi_series"] = [oi]
        alerts = await self._alerts_by_symbol(symbol, timeframe)
        for alert in alerts:
            definition = json.loads(alert["definition"])
            if definition.get("type") != "simple":
                continue  # kompozitler aşağıda ayrıca değerlendirilir
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
        """Talep üzerine değerlendirme: depolanmış analizden (daemon arka plan döngüsü için)."""
        analysis = None
        ms = await _read_current(self.db, "market_structure", symbol, timeframe)
        if ms is not None:
            analysis = {"as_of": ms["effective_from"]}
        if analysis is None:
            return []
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
                return None  # herhangi bir clause'ta veri eksik/stale → ertelenir
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

    # ---------- sorgu yardımcıları ----------

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
                "SELECT * FROM alerts WHERE state != 'triggered' OR cooldown_until IS NULL OR cooldown_until <= ?",
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
        """Tüm alarmların (simple + kompozit clause) ihtiyaç duyduğu (symbol, timeframe) çiftleri."""
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
            raise RasatError(ErrorCode.INVALID_REQUEST, f"limit 1-500 arası olmalı (verildi: {limit})")

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
