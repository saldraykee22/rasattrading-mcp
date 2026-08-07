"""Risk politikası + tek kullanımlık override (ticket 3.2).

State machine (override): ``reserved -> applied | reconciled``.

- ``reserved``  : oluşturuldu, sıradaki emir tarafından tüketilebilir.
- ``applied``   : bir emir tarafından tüketildi (``consumed_by_idem`` kaydedilir).
- ``reconciled``: artık geçerli değil (süresi doldu / policy versiyonu değişti).

Sözleşmeler (mimari plan Bölüm 5.2):
- ``create_override`` (account_id, idempotency_key) üzerinde idempotenttir: aynı
  anahtarla retry aynı override'ı döner, ikinci bir override üretmez.
- ``consume_override`` tek yazma kuyruğunda (transaction seviyesinde) koşullu
  ``UPDATE ... WHERE state='reserved'`` yapar; iki eşzamanlı emir aynı override'ı
  tüketemez — ilki kazanır, ikincisi boş döner.
- Override yalnızca kullanıcı-tanımlı risk politikası cap'lerini bir emir için
  atlar. Temel doğruluk kontrolleri (3.3) bu modülün dışında kalır ve asla atlanmaz.
- ``max_notional_per_order`` / ``max_aggregate_exposure`` cap'lerine tolerans
  uygulanmaz; enforcement 3.4'te ``risk.enforce_policy_caps`` ile yapılır.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from typing import Any

from ..errors import ErrorCode, RasatError
from ..numeric import require_finite
from .audit import AuditLog
from .db import Database

logger = logging.getLogger("rasattrading.storage.risk")

STATE_RESERVED = "reserved"
STATE_APPLIED = "applied"
STATE_RECONCILED = "reconciled"

SCOPE_NEXT_ORDER = "next_order"

#: Override'ların varsayılan ömrü (sn). Süresi dolunca `reconcile_overrides` onları kapatır.
DEFAULT_OVERRIDE_TTL_SECONDS = 24 * 3600

_RISK_POLICY_FIELDS = (
    "account_id",
    "max_notional_per_order",
    "max_aggregate_exposure",
    "allowed_symbols",
    "policy_version",
    "created_at",
    "updated_at",
)

_OVERRIDE_FIELDS = (
    "override_id",
    "account_id",
    "policy_version",
    "idempotency_key",
    "actor",
    "reason",
    "scope",
    "state",
    "created_at",
    "expires_at",
    "applied_at",
    "consumed_by_idem",
    "reconciled_at",
    "reconcile_reason",
)


def _parse_symbols(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return []
    if not isinstance(raw, list):
        return []
    seen: list[str] = []
    for entry in raw:
        if isinstance(entry, str):
            value = entry.strip().upper()
            if value and value not in seen:
                seen.append(value)
    return seen


def _validate_optional_amount(value: Any, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{field} pozitif sayı olmalı")
    amount = require_finite(value, field)
    if amount <= 0:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{field} sıfırdan büyük olmalı")
    return amount


def _validate_clear_flag(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise RasatError(ErrorCode.INVALID_REQUEST, f"{field} boolean olmalı")
    return value


def _validate_symbols(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise RasatError(ErrorCode.INVALID_REQUEST, "allowed_symbols string listesi olmalı")
    for entry in value:
        if not isinstance(entry, str) or not entry.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "allowed_symbols yalnızca boş olmayan string içermeli")
    return _parse_symbols(value)


class RiskPolicyService:
    """Risk politikası + override yönetimi (tek yazma kuyruğu üzerinden)."""

    def __init__(self, db: Database, audit: AuditLog | None = None) -> None:
        self.db = db
        self.audit = audit

    # ---------- policy ----------

    def _policy_default(self, account_id: str) -> dict[str, Any]:
        return {
            "account_id": account_id,
            "max_notional_per_order": None,
            "max_aggregate_exposure": None,
            "allowed_symbols": [],
            "policy_version": 0,
            "configured": False,
        }

    def _policy_row(self, row: sqlite3.Row | None, account_id: str) -> dict[str, Any]:
        if row is None:
            return self._policy_default(account_id)
        return {
            "account_id": account_id,
            "max_notional_per_order": row["max_notional_per_order"],
            "max_aggregate_exposure": row["max_aggregate_exposure"],
            "allowed_symbols": _parse_symbols(row["allowed_symbols"]),
            "policy_version": int(row["policy_version"]),
            "configured": True,
            "created_at": int(row["created_at"]),
            "updated_at": int(row["updated_at"]),
        }

    async def get_policy(self, account_id: Any) -> dict[str, Any]:
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
        account_id = account_id.strip()

        def _get(conn: sqlite3.Connection) -> dict[str, Any]:
            if conn.execute("SELECT 1 FROM accounts WHERE account_id = ?", (account_id,)).fetchone() is None:
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {account_id}")
            row = conn.execute(
                "SELECT " + ", ".join(_RISK_POLICY_FIELDS) + " FROM risk_policy WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            return self._policy_row(row, account_id)

        return await self.db.read(_get)

    async def set_risk_policy(
        self,
        account_id: Any,
        *,
        max_notional_per_order: Any = None,
        max_aggregate_exposure: Any = None,
        allowed_symbols: Any = None,
        clear_max_notional: Any = False,
        clear_max_exposure: Any = False,
        clear_allowed_symbols: Any = False,
        actor: str = "mcp-agent",
    ) -> dict[str, Any]:
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
        account_id = account_id.strip()
        max_notional = _validate_optional_amount(max_notional_per_order, "max_notional_per_order")
        max_aggregate = _validate_optional_amount(max_aggregate_exposure, "max_aggregate_exposure")
        symbols = _validate_symbols(allowed_symbols)
        clear_notional = _validate_clear_flag(clear_max_notional, "clear_max_notional")
        clear_aggregate = _validate_clear_flag(clear_max_exposure, "clear_max_exposure")
        clear_symbols = _validate_clear_flag(clear_allowed_symbols, "clear_allowed_symbols")

        def _upsert(conn: sqlite3.Connection) -> dict[str, Any]:
            if conn.execute("SELECT 1 FROM accounts WHERE account_id = ?", (account_id,)).fetchone() is None:
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {account_id}")
            row = conn.execute(
                "SELECT " + ", ".join(_RISK_POLICY_FIELDS) + " FROM risk_policy WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                version = 1
                changed = True
                created_at = updated_at = int(time.time())
                merged_notional = None if clear_notional else max_notional
                merged_aggregate = None if clear_aggregate else max_aggregate
                merged_symbols = [] if clear_symbols else symbols
                conn.execute(
                    "INSERT INTO risk_policy "
                    "(account_id, max_notional_per_order, max_aggregate_exposure, allowed_symbols, policy_version, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        account_id,
                        merged_notional,
                        merged_aggregate,
                        json.dumps(merged_symbols, ensure_ascii=False, separators=(",", ":")),
                        version,
                        created_at,
                        updated_at,
                    ),
                )
            else:
                prev_symbols = _parse_symbols(row["allowed_symbols"])
                prev_notional = row["max_notional_per_order"]
                prev_aggregate = row["max_aggregate_exposure"]
                # `None` means no-op. Empty allowed_symbols is also a no-op;
                # clearing is explicit so callers cannot accidentally remove a
                # symbol allowlist while constructing a partial patch.
                merged_notional = None if clear_notional else (
                    max_notional if max_notional is not None else prev_notional
                )
                merged_aggregate = None if clear_aggregate else (
                    max_aggregate if max_aggregate is not None else prev_aggregate
                )
                merged_symbols = [] if clear_symbols else (
                    symbols if allowed_symbols is not None and symbols else prev_symbols
                )
                changed = (
                    merged_notional != prev_notional
                    or merged_aggregate != prev_aggregate
                    or merged_symbols != prev_symbols
                )
                version = int(row["policy_version"])
                if changed:
                    version += 1
                updated_at = int(time.time()) if changed else int(row["updated_at"])
                conn.execute(
                    "UPDATE risk_policy SET max_notional_per_order = ?, max_aggregate_exposure = ?, "
                    "allowed_symbols = ?, policy_version = ?, updated_at = ? WHERE account_id = ?",
                    (
                        merged_notional,
                        merged_aggregate,
                        json.dumps(merged_symbols, ensure_ascii=False, separators=(",", ":")),
                        version,
                        updated_at,
                        account_id,
                    ),
                )
                created_at = int(row["created_at"])

            if changed:
                self._reconcile_stale_overrides(
                    conn, account_id, keep_version=version, actor=actor, reason="policy_version_changed"
                )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor or "mcp-agent",
                    action="set_risk_policy",
                    details={
                        "account_id": account_id,
                        "policy_version": version,
                        "max_notional_per_order": merged_notional,
                        "max_aggregate_exposure": merged_aggregate,
                        "allowed_symbols": merged_symbols,
                        "clear_max_notional": clear_notional,
                        "clear_max_exposure": clear_aggregate,
                        "clear_allowed_symbols": clear_symbols,
                        "changed": changed,
                    },
                )
            return {
                "account_id": account_id,
                "max_notional_per_order": merged_notional,
                "max_aggregate_exposure": merged_aggregate,
                "allowed_symbols": merged_symbols,
                "policy_version": version,
                "configured": True,
                "changed": changed,
                "created_at": created_at,
                "updated_at": updated_at,
            }

        return await self.db.write(_upsert)

    # ---------- override ----------

    def _override_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "override_id": row["override_id"],
            "account_id": row["account_id"],
            "policy_version": int(row["policy_version"]),
            "idempotency_key": row["idempotency_key"],
            "actor": row["actor"],
            "reason": row["reason"],
            "scope": row["scope"],
            "state": row["state"],
            "created_at": int(row["created_at"]),
            "expires_at": int(row["expires_at"]),
            "applied_at": int(row["applied_at"]) if row["applied_at"] is not None else None,
            "consumed_by_idem": row["consumed_by_idem"],
            "reconciled_at": int(row["reconciled_at"]) if row["reconciled_at"] is not None else None,
            "reconcile_reason": row["reconcile_reason"],
        }

    async def create_override(
        self,
        account_id: Any,
        *,
        reason: Any,
        scope: Any = SCOPE_NEXT_ORDER,
        idempotency_key: Any = None,
        actor: str = "mcp-agent",
        expires_at: Any = None,
        ttl_seconds: float = DEFAULT_OVERRIDE_TTL_SECONDS,
    ) -> dict[str, Any]:
        """Tek kullanımlık override rezerve eder (idempotent).

        - Aynı (account_id, idempotency_key) ile tekrar çağrı AYNI override'ı döner;
          ikinci bir override üretilmez.
        - `idempotency_key` verilmezse üretilir (yine de çağrı tarafından
          korunabilmesi için dönüşte raporlanır).
        - `expires_at` verilmezse now + ttl_seconds.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
        account_id = account_id.strip()
        if not isinstance(reason, str) or not reason.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "reason zorunlu (string)")
        reason = reason.strip()
        if scope != SCOPE_NEXT_ORDER:
            raise RasatError(ErrorCode.INVALID_REQUEST, "v1 yalnızca scope='next_order' destekler")
        if idempotency_key is None:
            idempotency_key = uuid.uuid4().hex
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "idempotency_key geçerli bir string olmalı")
        idempotency_key = idempotency_key.strip()
        now = int(time.time())
        if expires_at is None:
            expires_at = now + int(ttl_seconds)
        if not isinstance(expires_at, (int, float)) or int(expires_at) <= now:
            raise RasatError(ErrorCode.INVALID_REQUEST, "expires_at gelecekte bir unix zamanı olmalı")
        expires_at = int(expires_at)

        def _create(conn: sqlite3.Connection) -> dict[str, Any]:
            if conn.execute("SELECT 1 FROM accounts WHERE account_id = ?", (account_id,)).fetchone() is None:
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {account_id}")
            # Idempotent retry: aynı anahtar varsa mevcut override'ı döndür.
            existing = conn.execute(
                "SELECT " + ", ".join(_OVERRIDE_FIELDS)
                + " FROM risk_override WHERE account_id = ? AND idempotency_key = ?",
                (account_id, idempotency_key),
            ).fetchone()
            if existing is not None:
                return self._override_row(existing)

            policy_row = conn.execute(
                "SELECT policy_version FROM risk_policy WHERE account_id = ?", (account_id,)
            ).fetchone()
            policy_version = int(policy_row["policy_version"]) if policy_row else 0
            override_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO risk_override "
                "(override_id, account_id, policy_version, idempotency_key, actor, reason, scope, state, created_at, expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    override_id,
                    account_id,
                    policy_version,
                    idempotency_key,
                    actor or "mcp-agent",
                    reason,
                    scope,
                    STATE_RESERVED,
                    now,
                    expires_at,
                ),
            )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor or "mcp-agent",
                    action="override_risk_policy",
                    details={
                        "override_id": override_id,
                        "account_id": account_id,
                        "policy_version": policy_version,
                        "scope": scope,
                        "reason": reason,
                        "expires_at": expires_at,
                    },
                )
            return {
                "override_id": override_id,
                "account_id": account_id,
                "policy_version": policy_version,
                "idempotency_key": idempotency_key,
                "actor": actor or "mcp-agent",
                "reason": reason,
                "scope": scope,
                "state": STATE_RESERVED,
                "created_at": now,
                "expires_at": expires_at,
                "applied_at": None,
                "consumed_by_idem": None,
                "reconciled_at": None,
                "reconcile_reason": None,
            }

        try:
            return await self.db.write(_create)
        except sqlite3.IntegrityError:
            raise RasatError(
                ErrorCode.INVALID_REQUEST,
                "aynı (account_id, idempotency_key) kombinasyonu zaten var",
            ) from None

    async def consume_override(
        self,
        account_id: Any,
        *,
        policy_version: int,
        consumed_by_idem: str,
        now: int | None = None,
    ) -> dict[str, Any] | None:
        """Tek kullanımlık override'ı transaction seviyesinde tüketir.

        Uygun (reserved + süresi dolmamış + policy_version eşleşen) bir override
        varsa onu ``applied`` yapıp döner; yoksa ``None`` döner (cap enforcement
        devam eder). Tek yazma kuyruğu sayesinde eşzamanlı iki emir aynı
        override'ı tüketemez: koşullu UPDATE yalnızca `state='reserved'` satırını
        eşleştirir, ikinci çağrı hiç satır güncellemeden `None` alır.

        3.4 sıralaması: temel doğruluk kontrollerini ÖNCE çalıştır, sonra bu
        metodu çağır, sonra emri gönder — override yalnızca cap'leri atlar,
        doğruluk kontrollerini asla atlamaz.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
        if not isinstance(consumed_by_idem, str) or not consumed_by_idem.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "consumed_by_idem zorunlu (string)")
        account_id = account_id.strip()
        now = int(time.time()) if now is None else int(now)

        def _consume(conn: sqlite3.Connection) -> dict[str, Any] | None:
            candidate = conn.execute(
                "SELECT " + ", ".join(_OVERRIDE_FIELDS)
                + " FROM risk_override WHERE account_id = ? AND state = ? AND policy_version = ? "
                "AND expires_at > ? ORDER BY created_at ASC, id ASC LIMIT 1",
                (account_id, STATE_RESERVED, int(policy_version), now),
            ).fetchone()
            if candidate is None:
                return None
            cur = conn.execute(
                "UPDATE risk_override SET state = ?, applied_at = ?, consumed_by_idem = ? "
                "WHERE override_id = ? AND state = ?",
                (STATE_APPLIED, now, consumed_by_idem, candidate["override_id"], STATE_RESERVED),
            )
            if cur.rowcount != 1:
                return None
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor="system",
                    action="risk_override_applied",
                    details={
                        "override_id": candidate["override_id"],
                        "account_id": account_id,
                        "consumed_by_idem": consumed_by_idem,
                        "policy_version": int(candidate["policy_version"]),
                    },
                )
            return {
                **self._override_row(candidate),
                "state": STATE_APPLIED,
                "applied_at": now,
                "consumed_by_idem": consumed_by_idem,
            }

        return await self.db.write(_consume)

    async def get_active_override(self, account_id: Any, *, policy_version: int, now: int | None = None) -> dict | None:
        """Salt-okunur kontrol: tüketilebilir bir override var mı? (tüketmez)."""

        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
        account_id = account_id.strip()
        now = int(time.time()) if now is None else int(now)

        def _get(conn: sqlite3.Connection) -> dict | None:
            row = conn.execute(
                "SELECT " + ", ".join(_OVERRIDE_FIELDS)
                + " FROM risk_override WHERE account_id = ? AND state = ? AND policy_version = ? "
                "AND expires_at > ? ORDER BY created_at ASC, id ASC LIMIT 1",
                (account_id, STATE_RESERVED, int(policy_version), now),
            ).fetchone()
            return self._override_row(row) if row else None

        return await self.db.read(_get)

    # ---------- reconcile ----------

    def _reconcile_stale_overrides(
        self,
        conn: sqlite3.Connection,
        account_id: str,
        *,
        keep_version: int,
        actor: str,
        reason: str,
    ) -> None:
        """Reserved override'ların policy_version eşleşmeyenlerini kapatır."""
        rows = conn.execute(
            "SELECT override_id FROM risk_override WHERE account_id = ? AND state = ? AND policy_version != ?",
            (account_id, STATE_RESERVED, keep_version),
        ).fetchall()
        now = int(time.time())
        for row in rows:
            conn.execute(
                "UPDATE risk_override SET state = ?, reconciled_at = ?, reconcile_reason = ? WHERE override_id = ?",
                (STATE_RECONCILED, now, reason, row["override_id"]),
            )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor or "mcp-agent",
                    action="risk_override_reconciled",
                    details={"override_id": row["override_id"], "account_id": account_id, "reason": reason},
                )

    async def reconcile_overrides(self, *, now: int | None = None) -> int:
        """Süresi dolan reserved override'ları `reconciled`'a kapatır (daemon açılışı)."""

        now = int(time.time()) if now is None else int(now)

        def _reconcile(conn: sqlite3.Connection) -> int:
            rows = conn.execute(
                "SELECT override_id, account_id FROM risk_override WHERE state = ? AND expires_at <= ?",
                (STATE_RESERVED, now),
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE risk_override SET state = ?, reconciled_at = ?, reconcile_reason = ? WHERE override_id = ?",
                    (STATE_RECONCILED, now, "expired", row["override_id"]),
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor="system",
                        action="risk_override_reconciled",
                        details={"override_id": row["override_id"], "account_id": row["account_id"], "reason": "expired"},
                    )
            return len(rows)

        return await self.db.write(_reconcile)
