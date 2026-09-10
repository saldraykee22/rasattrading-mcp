"""2.4 — Chart annotation service (keyed by symbol+timeframe, stateless).

Agents' own PA annotations are stored in the `annotations` table.
Add/read/clear by symbol+timeframe; there is no computation logic.
"""

from __future__ import annotations

import json
import time
from typing import Any

from ..errors import ErrorCode, RasatError
from ..storage.db import Database


class AnnotationService:
    def __init__(self, db: Database) -> None:
        self.db = db

    async def annotate(
        self, symbol: str, timeframe: str, annotations: list[dict] | dict, created_by: str = "agent"
    ) -> list[int]:
        """Add one or more annotations and return their saved IDs."""
        if not isinstance(symbol, str) or not symbol:
            raise RasatError(ErrorCode.INVALID_REQUEST, "symbol is required (string)")
        if not isinstance(timeframe, str) or not timeframe:
            raise RasatError(ErrorCode.INVALID_REQUEST, "timeframe is required (string)")
        items = annotations if isinstance(annotations, list) else [annotations]
        if not items:
            return []
        for it in items:
            if not isinstance(it, dict):
                raise RasatError(ErrorCode.INVALID_REQUEST, "annotation must be an object")

        now = int(time.time())

        def _w(conn) -> list[int]:
            ids = []
            for it in items:
                cur = conn.execute(
                    "INSERT INTO annotations (symbol, timeframe, created_by, data, created_at) VALUES (?,?,?,?,?)",
                    (symbol, timeframe, created_by, json.dumps(it, ensure_ascii=False), now),
                )
                ids.append(cur.lastrowid)
            return ids

        return await self.db.write(_w)

    async def get(self, symbol: str, timeframe: str) -> list[dict]:
        def _q(conn):
            rows = conn.execute(
                "SELECT id, created_by, data, created_at FROM annotations "
                "WHERE symbol=? AND timeframe=? ORDER BY created_at ASC, id ASC",
                (symbol, timeframe),
            ).fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "id": r["id"],
                        "created_by": r["created_by"],
                        "created_at": r["created_at"],
                        "data": json.loads(r["data"]),
                    }
                )
            return out

        return await self.db.read(_q)

    async def clear(self, symbol: str, timeframe: str) -> int:
        def _w(conn) -> int:
            cur = conn.execute("DELETE FROM annotations WHERE symbol=? AND timeframe=?", (symbol, timeframe))
            return cur.rowcount

        return await self.db.write(_w)
