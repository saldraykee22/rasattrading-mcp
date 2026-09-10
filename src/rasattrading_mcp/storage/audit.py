"""Append-only audit log with a hash chain.

Each row carries the previous row's hash. If a row is deleted or changed, the chain
breaks and `verify` detects it. Never log secrets (key/secret/token); `_redact`
cleans sensitive keys from details.
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
    """Serialized append through the write queue; verification through reads."""

    def __init__(self, db: Database) -> None:
        self._db = db

    async def append(self, actor: str, action: str, details: dict | None = None) -> int:
        """Append a row and return its sequence number."""
        return await self._db.write(
            lambda conn: self.append_in_connection(conn, actor=actor, action=action, details=details)
        )

    def append_in_connection(
        self,
        conn: sqlite3.Connection,
        *,
        actor: str,
        action: str,
        details: dict | None = None,
    ) -> int:
        """Append within an already-open transaction.

        Account mutations use this helper so the CRUD row and its audit entry
        commit or roll back together through the single SQLite writer queue.
        Callers must not close/commit the connection outside ``Database.write``.
        """

        safe_details = _redact(details or {})
        row = conn.execute("SELECT seq, hash FROM audit_log ORDER BY seq DESC LIMIT 1").fetchone()
        prev_seq = int(row["seq"]) if row else 0
        prev_hash = str(row["hash"]) if row else GENESIS_HASH
        seq = prev_seq + 1
        details_json = json.dumps(safe_details, ensure_ascii=False, sort_keys=True, default=str)
        created_at = int(time.time())
        h = hash_entry(prev_hash, seq, actor, action, details_json, created_at)
        conn.execute(
            "INSERT INTO audit_log (seq, actor, action, details, prev_hash, hash, created_at) VALUES (?,?,?,?,?,?,?)",
            (seq, actor, action, details_json, prev_hash, h, created_at),
        )
        return seq

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
        """Verify chain integrity and return broken rows (empty = intact)."""

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
                    broken.append({"seq": seq, "reason": f"sequence gap/deletion (expected {prev_seq + 1})"})
                if str(row["prev_hash"]) != expected_prev:
                    broken.append({"seq": seq, "reason": "chain break (prev_hash mismatch)"})
                details_json = str(row["details"])
                created_at = int(row["created_at"])
                expected = hash_entry(expected_prev, seq, str(row["actor"]), str(row["action"]), details_json, created_at)
                if str(row["hash"]) != expected:
                    broken.append({"seq": seq, "reason": "hash mismatch (row changed)"})
                prev_hash = str(row["hash"])
                prev_seq = seq
            return broken

        return await self._db.read(_verify)
