"""Append-only, hash-chain'li audit log.

Her satır bir önceki satırın hash'ini taşır. Bir satır silinir/değiştirilirse zincir
kırılır ve `verify` bunu tespit eder. Sırlar (key/secret/token) asla loglanmaz —
`_redact` ile details'taki hassas anahtarlar temizlenir.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from typing import Any

from .db import Database

logger = logging.getLogger("rasattrading.storage.audit")

GENESIS_HASH = "GENESIS"

SENSITIVE_KEYS = {
    "key",
    "secret",
    "api_key",
    "api_secret",
    "apikey",
    "apisecret",
    "token",
    "password",
    "passphrase",
    "encrypted_api_key",
    "encrypted_secret",
    "signature",
}


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: ("***REDACTED***" if k.lower() in SENSITIVE_KEYS else _redact(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact(v) for v in value]
    return value


def hash_entry(prev_hash: str, seq: int, actor: str, action: str, details_json: str, created_at: int) -> str:
    payload = f"{prev_hash}|{seq}|{actor}|{action}|{details_json}|{created_at}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class AuditLog:
    """Yazma kuyruğu üzerinden sıralı append; okuma ile doğrulama."""

    def __init__(self, db: Database) -> None:
        self._db = db

    async def append(self, actor: str, action: str, details: dict | None = None) -> int:
        """Bir satır ekler; satırın seq'ini döner."""
        details = _redact(details or {})

        def _append(conn: sqlite3.Connection) -> int:
            row = conn.execute("SELECT seq, hash FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
            prev_seq = int(row["seq"]) if row else 0
            prev_hash = str(row["hash"]) if row else GENESIS_HASH
            seq = prev_seq + 1
            details_json = json.dumps(details, ensure_ascii=False, sort_keys=True, default=str)
            created_at = int(time.time())
            h = hash_entry(prev_hash, seq, actor, action, details_json, created_at)
            conn.execute(
                "INSERT INTO audit_log (seq, actor, action, details, prev_hash, hash, created_at) VALUES (?,?,?,?,?,?,?)",
                (seq, actor, action, details_json, prev_hash, h, created_at),
            )
            return seq

        return await self._db.write(_append)

    async def tail(self, limit: int = 50) -> list[dict]:
        def _tail(conn: sqlite3.Connection) -> list[dict]:
            rows = conn.execute(
                "SELECT id, seq, actor, action, details, prev_hash, hash, created_at "
                "FROM audit_log ORDER BY seq DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

        return await self._db.read(_tail)

    async def verify(self) -> list[dict]:
        """Zincir bütünlüğünü doğrular; kırık satırları döner (boş = sağlam)."""

        def _verify(conn: sqlite3.Connection) -> list[dict]:
            rows = conn.execute(
                "SELECT seq, actor, action, details, prev_hash, hash, created_at FROM audit_log ORDER BY seq"
            ).fetchall()
            broken: list[dict] = []
            prev_hash = GENESIS_HASH
            prev_seq = 0
            for row in rows:
                seq = int(row["seq"])
                expected_prev = prev_hash
                if seq != prev_seq + 1:
                    broken.append({"seq": seq, "reason": f"seq atlama/silinme (beklenen {prev_seq + 1})"})
                if str(row["prev_hash"]) != expected_prev:
                    broken.append({"seq": seq, "reason": "zincir kopması (prev_hash uyuşmuyor)"})
                details_json = str(row["details"])
                created_at = int(row["created_at"])
                expected = hash_entry(expected_prev, seq, str(row["actor"]), str(row["action"]), details_json, created_at)
                if str(row["hash"]) != expected:
                    broken.append({"seq": seq, "reason": "hash uyuşmazlığı (satır değiştirilmiş)"})
                prev_hash = str(row["hash"])
                prev_seq = seq
            return broken

        return await self._db.read(_verify)
