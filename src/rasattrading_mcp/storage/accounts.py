"""Account CRUD and credential handling."""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from typing import Any

from ..errors import ErrorCode, RasatError
from .audit import AuditLog
from .credentials import SecretDecryptError, SecretStore, SecretStoreError
from .db import Database
from .state import ORDER_RECONCILE_REQUIRED, PENDING_ACTIVE_STATUSES

logger = logging.getLogger("rasattrading.storage.accounts")

_MAX_LABEL_LENGTH = 200
_ALLOWED_MARKET = "spot"

#: Order states considered open/active — an account cannot be deleted while present (T04).
#: RECONCILE_REQUIRED is the canonical T00 state for an uncertain/unresolved order
#: (like UNKNOWN) and blocks deletion fail closed.
_ACTIVE_ORDER_STATUSES = ("NEW", "PARTIALLY_FILLED", "UNKNOWN", ORDER_RECONCILE_REQUIRED)
_ACCOUNT_FIELDS = (
    "account_id",
    "label",
    "tags",
    "market",
    "trading_lock",
    "encrypted_api_key",
    "encrypted_secret",
    "created_at",
    "updated_at",
)


def _public_account(row: Any) -> dict[str, Any]:
    """Convert an account row without ever including encrypted columns."""

    if row is None:
        raise ValueError("account row is missing")
    raw_tags = row["tags"] if isinstance(row, sqlite3.Row) else row.get("tags", "[]")
    try:
        tags = json.loads(raw_tags) if isinstance(raw_tags, str) else raw_tags
    except (TypeError, ValueError):
        tags = []
    if not isinstance(tags, list):
        tags = []
    configured = bool(row["encrypted_api_key"] and row["encrypted_secret"])
    return {
        "account_id": str(row["account_id"]),
        "label": row["label"],
        "tags": tags,
        "market": str(row["market"]),
        "trading_lock": str(row["trading_lock"]),
        "credentials_configured": configured,
        "read_only": not configured,
        "mode": "authenticated" if configured else "public",
        "created_at": int(row["created_at"]),
        "updated_at": int(row["updated_at"]),
    }


def _validate_label(label: Any) -> str:
    if not isinstance(label, str):
        raise RasatError(ErrorCode.INVALID_REQUEST, "label is required (string)")
    normalized = label.strip()
    if not normalized:
        raise RasatError(ErrorCode.INVALID_REQUEST, "label cannot be empty")
    if len(normalized) > _MAX_LABEL_LENGTH:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"label can be at most {_MAX_LABEL_LENGTH} characters")
    return normalized


def _validate_tags(tags: Any) -> list[str]:
    if tags is None:
        return []
    if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
        raise RasatError(ErrorCode.INVALID_REQUEST, "tags must be a list of strings")
    normalized: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        value = tag.strip()
        if not value:
            raise RasatError(ErrorCode.INVALID_REQUEST, "tags cannot contain empty values")
        if value not in seen:
            seen.add(value)
            normalized.append(value)
    return normalized


def _validate_credentials(api_key: Any, api_secret: Any) -> tuple[str | None, str | None]:
    if api_key is None and api_secret is None:
        return None, None
    if not isinstance(api_key, str) or not isinstance(api_secret, str):
        raise RasatError(ErrorCode.INVALID_REQUEST, "api_key and api_secret must both be strings")
    if not api_key or not api_secret:
        raise RasatError(ErrorCode.INVALID_REQUEST, "api_key and api_secret cannot be empty")
    return api_key, api_secret


def _removal_reasons(conn: sqlite3.Connection, account_id: str, row: sqlite3.Row) -> list[str]:
    """Return dependencies that prevent account deletion (empty = deletion allowed).

    - An account with the real-trading lock is NEVER deletable (fail closed).
    - Reject deletion when open orders, active pending orders, or risk policy/override
      records exist; otherwise those rows would become orphans (no foreign-key cascade).
    - Historical (terminal) order/pending records do NOT block deletion; they are
      not cascade-deleted and remain after the account is removed.
    """
    reasons: list[str] = []
    if str(row["trading_lock"]) == "real":
        reasons.append("trading_lock=real")

    order_marks = ", ".join("?" * len(_ACTIVE_ORDER_STATUSES))
    open_orders = conn.execute(
        f"SELECT COUNT(*) AS c FROM orders WHERE account_id = ? AND status IN ({order_marks})",
        (account_id, *_ACTIVE_ORDER_STATUSES),
    ).fetchone()
    if open_orders and int(open_orders["c"]) > 0:
        reasons.append(f"open_orders={int(open_orders['c'])}")

    pending_marks = ", ".join("?" * len(PENDING_ACTIVE_STATUSES))
    active_pending = conn.execute(
        f"SELECT COUNT(*) AS c FROM pending_orders WHERE account_id = ? AND status IN ({pending_marks})",
        (account_id, *PENDING_ACTIVE_STATUSES),
    ).fetchone()
    if active_pending and int(active_pending["c"]) > 0:
        reasons.append(f"active_pending_orders={int(active_pending['c'])}")

    policy_row = conn.execute(
        "SELECT COUNT(*) AS c FROM risk_policy WHERE account_id = ?",
        (account_id,),
    ).fetchone()
    if policy_row and int(policy_row["c"]) > 0:
        reasons.append(f"risk_policy={int(policy_row['c'])}")

    override_row = conn.execute(
        "SELECT COUNT(*) AS c FROM risk_override WHERE account_id = ?",
        (account_id,),
    ).fetchone()
    if override_row and int(override_row["c"]) > 0:
        reasons.append(f"risk_overrides={int(override_row['c'])}")

    return reasons


class AccountService:
    """Serialized account mutations backed by the daemon SQLite writer queue."""

    def __init__(
        self,
        db: Database,
        secret_store: SecretStore | None = None,
        audit: AuditLog | None = None,
    ) -> None:
        self.db = db
        self.secret_store = secret_store or SecretStore()
        self.audit = audit

    async def add_account(
        self,
        *,
        label: Any,
        api_key: Any = None,
        api_secret: Any = None,
        tags: Any = None,
        market: Any = _ALLOWED_MARKET,
        actor: str = "mcp-agent",
    ) -> dict[str, Any]:
        normalized_label = _validate_label(label)
        normalized_tags = _validate_tags(tags)
        api_key, api_secret = _validate_credentials(api_key, api_secret)
        if market != _ALLOWED_MARKET:
            raise RasatError(ErrorCode.INVALID_REQUEST, "v1 supports only market='spot'")

        account_id = uuid.uuid4().hex
        encrypted_api_key: bytes | None = None
        encrypted_secret: bytes | None = None
        try:
            if api_key is not None and api_secret is not None:
                encrypted_api_key = self.secret_store.encrypt(account_id, "api_key", api_key)
                encrypted_secret = self.secret_store.encrypt(account_id, "api_secret", api_secret)
        except SecretStoreError as exc:
            # If a keyring backend created only one reference, remove it without
            # ever putting the plaintext into the exception or audit trail.
            self.secret_store.delete(account_id, "api_key", encrypted_api_key)
            self.secret_store.delete(account_id, "api_secret", encrypted_secret)
            raise RasatError(ErrorCode.CREDENTIAL_STORE_UNAVAILABLE, "OS credential backend is unavailable") from exc

        now = int(time.time())
        tags_json = json.dumps(normalized_tags, ensure_ascii=False, separators=(",", ":"))

        def _insert(conn: sqlite3.Connection) -> dict[str, Any]:
            conn.execute(
                "INSERT INTO accounts "
                "(account_id, label, tags, market, trading_lock, encrypted_api_key, encrypted_secret, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, 'paper', ?, ?, ?, ?)",
                (
                    account_id,
                    normalized_label,
                    tags_json,
                    _ALLOWED_MARKET,
                    encrypted_api_key,
                    encrypted_secret,
                    now,
                    now,
                ),
            )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor or "mcp-agent",
                    action="add_account",
                    details={
                        "account_id": account_id,
                        "label": normalized_label,
                        "market": _ALLOWED_MARKET,
                        "tags": normalized_tags,
                        "credentials_configured": encrypted_api_key is not None and encrypted_secret is not None,
                    },
                )
            return {
                "account_id": account_id,
                "label": normalized_label,
                "tags": normalized_tags,
                "market": _ALLOWED_MARKET,
                "trading_lock": "paper",
                "credentials_configured": encrypted_api_key is not None and encrypted_secret is not None,
                "read_only": encrypted_api_key is None or encrypted_secret is None,
                "mode": "authenticated" if encrypted_api_key is not None and encrypted_secret is not None else "public",
                "created_at": now,
                "updated_at": now,
            }

        try:
            return await self.db.write(_insert)
        except sqlite3.IntegrityError as exc:
            self.secret_store.delete(account_id, "api_key", encrypted_api_key)
            self.secret_store.delete(account_id, "api_secret", encrypted_secret)
            raise RasatError(ErrorCode.ACCOUNT_EXISTS, "could not create account; unique constraint violation") from exc
        except Exception:
            self.secret_store.delete(account_id, "api_key", encrypted_api_key)
            self.secret_store.delete(account_id, "api_secret", encrypted_secret)
            raise

    async def list_accounts(self) -> dict[str, Any]:
        def _list(conn: sqlite3.Connection) -> list[dict[str, Any]]:
            rows = conn.execute(
                "SELECT " + ", ".join(_ACCOUNT_FIELDS) + " FROM accounts ORDER BY created_at DESC, account_id"
            ).fetchall()
            return [_public_account(row) for row in rows]

        accounts = await self.db.read(_list)
        any_configured = any(bool(account["credentials_configured"]) for account in accounts)
        return {
            "accounts": accounts,
            "count": len(accounts),
            "mode": "authenticated" if any_configured else "public",
            "read_only": not any_configured,
        }

    async def remove_account(self, account_id: Any, *, actor: str = "mcp-agent") -> dict[str, Any]:
        """Delete an account only when it is not in use (fail closed, T04).

        - An account with ``trading_lock=real`` can NEVER be deleted.
        - With open/UNKNOWN orders, active pending orders, or risk policy/override
          records, reject with canonical ``ACCOUNT_IN_USE``; do not change the DB
          (account/order/pending/risk rows) or credentials.
        - Run the check and delete in the same single-writer DB transaction; no
          interleaved write can create a check-then-delete race.
        - Do not cascade-delete historical order/audit records.
        - Clean credentials only after an authorized successful deletion.
        - On refusal, append a secret-free ``remove_account_refused`` audit record
          (committed with the transaction).
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id is required (string)")
        account_id = account_id.strip()

        def _remove(
            conn: sqlite3.Connection,
        ) -> dict[str, Any] | tuple[dict[str, Any], bytes | None, bytes | None] | None:
            row = conn.execute(
                "SELECT " + ", ".join(_ACCOUNT_FIELDS) + " FROM accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                return None
            public = _public_account(row)

            reasons = _removal_reasons(conn, account_id, row)
            if reasons:
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor or "mcp-agent",
                        action="remove_account_refused",
                        details={"account_id": account_id, "label": public["label"], "reasons": reasons},
                    )
                return {"refused": True, "reasons": reasons}

            conn.execute("DELETE FROM accounts WHERE account_id = ?", (account_id,))
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor or "mcp-agent",
                    action="remove_account",
                    details={"account_id": account_id, "label": public["label"]},
                )
            return (
                {
                    "account_id": account_id,
                    "removed": True,
                    "read_only": public["read_only"],
                },
                row["encrypted_api_key"],
                row["encrypted_secret"],
            )

        removed = await self.db.write(_remove)
        if removed is None:
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account not found: {account_id}")
        if isinstance(removed, dict) and removed.get("refused"):
            raise RasatError(
                ErrorCode.ACCOUNT_IN_USE,
                f"could not delete account; account is in use: {', '.join(removed['reasons'])}",
                details={"account_id": account_id, "reasons": removed["reasons"]},
            )
        result, encrypted_api_key, encrypted_secret = removed

        # DPAPI blobs need no cleanup.  A keyring fallback does, and cleanup is
        # intentionally after the transactional DB delete to keep CRUD atomic.
        self.secret_store.delete(account_id, "api_key", encrypted_api_key)
        self.secret_store.delete(account_id, "api_secret", encrypted_secret)
        return result

    async def enable_real_trading(self, account_id: Any, *, actor: str = "mcp-agent") -> dict[str, Any]:
        """Permanently switch the trading lock to `real` (one-way, ticket 3.2).

        - New accounts default to `paper`; once this call runs, switch permanently
          to `real` and do not allow a return (except the 3.5 kill switch).
        - If already `real`, behave idempotently (not an error).
        - Reject credential-less (public/read-only) accounts because real trading is
          meaningless there — fail closed.
        - Write the change to `audit_log`.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id is required (string)")
        account_id = account_id.strip()

        def _flip(conn: sqlite3.Connection) -> dict[str, Any]:
            row = conn.execute(
                "SELECT " + ", ".join(_ACCOUNT_FIELDS) + " FROM accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account not found: {account_id}")
            if not row["encrypted_api_key"] or not row["encrypted_secret"]:
                raise RasatError(
                    ErrorCode.ACCOUNT_NO_CREDENTIALS,
                    "cannot enable real trading for a credential-less (public/read-only) account",
                )
            already_real = str(row["trading_lock"]) == "real"
            if not already_real:
                now = int(time.time())
                conn.execute(
                    "UPDATE accounts SET trading_lock = 'real', updated_at = ? WHERE account_id = ?",
                    (now, account_id),
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor or "mcp-agent",
                        action="enable_real_trading",
                        details={"account_id": account_id, "trading_lock": "real"},
                    )
            return {
                "account_id": account_id,
                "trading_lock": "real",
                "already_real": already_real,
                "mode": "authenticated",
            }

        return await self.db.write(_flip)

    async def disable_real_trading(self, account_id: Any, *, actor: str = "mcp-agent") -> dict[str, Any]:
        """Kill switch: switch a real account to `paper` (3.5).

        The inverse of `enable_real_trading`; write to audit_log. If already paper,
        behave idempotently.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id is required (string)")
        account_id = account_id.strip()

        def _flip(conn: sqlite3.Connection) -> dict[str, Any]:
            row = conn.execute(
                "SELECT " + ", ".join(_ACCOUNT_FIELDS) + " FROM accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account not found: {account_id}")
            already_paper = str(row["trading_lock"]) != "real"
            if not already_paper:
                now = int(time.time())
                conn.execute(
                    "UPDATE accounts SET trading_lock = 'paper', updated_at = ? WHERE account_id = ?",
                    (now, account_id),
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor or "mcp-agent",
                        action="disable_real_trading",
                        details={"account_id": account_id, "trading_lock": "paper"},
                    )
            return {
                "account_id": account_id,
                "trading_lock": "paper",
                "already_paper": already_paper,
                "mode": "authenticated",
            }

        return await self.db.write(_flip)

    async def get_account(self, account_id: str) -> dict[str, Any]:
        """Internal account lookup used by execution tickets; no secrets returned."""

        def _get(conn: sqlite3.Connection):
            return conn.execute(
                "SELECT " + ", ".join(_ACCOUNT_FIELDS) + " FROM accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()

        row = await self.db.read(_get)
        if row is None:
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account not found: {account_id}")
        return _public_account(row)

    async def get_credentials(self, account_id: str) -> tuple[str, str]:
        """Internal execution lookup; plaintext exists only in the return value."""

        def _get(conn: sqlite3.Connection):
            return conn.execute(
                "SELECT account_id, encrypted_api_key, encrypted_secret FROM accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()

        row = await self.db.read(_get)
        if row is None:
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account not found: {account_id}")
        if not row["encrypted_api_key"] or not row["encrypted_secret"]:
            raise RasatError(ErrorCode.ACCOUNT_NO_CREDENTIALS, "account is public/read-only; no API credentials")
        try:
            return (
                self.secret_store.decrypt(account_id, "api_key", row["encrypted_api_key"]),
                self.secret_store.decrypt(account_id, "api_secret", row["encrypted_secret"]),
            )
        except SecretDecryptError as exc:
            raise RasatError(ErrorCode.CREDENTIAL_DECRYPT_FAILED, "could not decrypt account credential") from exc
