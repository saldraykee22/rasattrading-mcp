"""Account CRUD and credential handling for Module 3 ticket 3.1."""

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

#: Açık/aktif sayılan emir durumları — varlığında hesap silinemez (T04).
#: RECONCILE_REQUIRED, T00 state sözleşmesindeki canonical durumdur (UNKNOWN gibi
#: belirsiz/çözülmemiş emir) ve fail-closed olarak silme engelidir.
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
        raise ValueError("account satırı yok")
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
        raise RasatError(ErrorCode.INVALID_REQUEST, "label zorunlu (string)")
    normalized = label.strip()
    if not normalized:
        raise RasatError(ErrorCode.INVALID_REQUEST, "label boş olamaz")
    if len(normalized) > _MAX_LABEL_LENGTH:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"label en fazla {_MAX_LABEL_LENGTH} karakter olabilir")
    return normalized


def _validate_tags(tags: Any) -> list[str]:
    if tags is None:
        return []
    if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
        raise RasatError(ErrorCode.INVALID_REQUEST, "tags string listesi olmalı")
    normalized: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        value = tag.strip()
        if not value:
            raise RasatError(ErrorCode.INVALID_REQUEST, "tags boş değer içeremez")
        if value not in seen:
            seen.add(value)
            normalized.append(value)
    return normalized


def _validate_credentials(api_key: Any, api_secret: Any) -> tuple[str | None, str | None]:
    if api_key is None and api_secret is None:
        return None, None
    if not isinstance(api_key, str) or not isinstance(api_secret, str):
        raise RasatError(ErrorCode.INVALID_REQUEST, "api_key ve api_secret birlikte string olmalı")
    if not api_key or not api_secret:
        raise RasatError(ErrorCode.INVALID_REQUEST, "api_key ve api_secret boş olamaz")
    return api_key, api_secret


def _removal_reasons(conn: sqlite3.Connection, account_id: str, row: sqlite3.Row) -> list[str]:
    """Hesabın silinmesini engelleyen bağımlılıkları döner (boş = silme serbest).

    - Real trading kilidi açıksa hesap ASLA silinemez (fail-closed).
    - Açık emir, aktif pending veya risk policy/override kaydı varsa silme
      reddedilir — aksi halde bu satırlar orphan kalır (foreign key/cascade yok).
    - Tarihsel (terminal) order/pending kayıtları engel DEĞİLDİR; onlar cascade
      ile silinmez, hesap silinse bile yerinde kalır.
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
            raise RasatError(ErrorCode.INVALID_REQUEST, "v1 yalnızca market='spot' destekler")

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
            raise RasatError(ErrorCode.CREDENTIAL_STORE_UNAVAILABLE, "OS credential backend kullanılamıyor") from exc

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
            raise RasatError(ErrorCode.ACCOUNT_EXISTS, "account oluşturulamadı; benzersiz kısıt ihlali") from exc
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
        """Hesabı yalnızca kullanımda değilken siler (fail-closed, T04).

        - ``trading_lock=real`` hesap ASLA silinemez.
        - Açık/UNKNOWN emir, aktif pending veya risk policy/override kaydı varken
          silme canonical ``ACCOUNT_IN_USE`` ile reddedilir; DB (hesap/emir/
          pending/risk satırları) ve credentials değiştirilmez.
        - Kontrol + delete aynı tek-yazıcı DB transaction'ında çalışır; araya
          yazma giremez, check-then-delete yarışı oluşmaz.
        - Tarihsel order/audit kayıtları cascade ile silinmez.
        - Credentials yalnızca başarılı ve izin verilen silme sonrası temizlenir.
        - Red durumunda audit'e secret içermeyen ``remove_account_refused`` kaydı
          düşer (transaction ile birlikte commit edilir).
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
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
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {account_id}")
        if isinstance(removed, dict) and removed.get("refused"):
            raise RasatError(
                ErrorCode.ACCOUNT_IN_USE,
                f"account silinemedi; hesap kullanımda: {', '.join(removed['reasons'])}",
                details={"account_id": account_id, "reasons": removed["reasons"]},
            )
        result, encrypted_api_key, encrypted_secret = removed

        # DPAPI blobs need no cleanup.  A keyring fallback does, and cleanup is
        # intentionally after the transactional DB delete to keep CRUD atomic.
        self.secret_store.delete(account_id, "api_key", encrypted_api_key)
        self.secret_store.delete(account_id, "api_secret", encrypted_secret)
        return result

    async def enable_real_trading(self, account_id: Any, *, actor: str = "mcp-agent") -> dict[str, Any]:
        """Trading kilidini kalıcı olarak `real`'e çevirir (tek yönlü, ticket 3.2).

        - Yeni hesap varsayılan `paper`'dır; bu çağrı bir kere yapılınca kalıcı
          `real`'e geçer ve geri dönüş yoktur (3.5 kill switch hariç).
        - Zaten `real` ise idempotent davranır (hata değil).
        - Credential'sız (public/read-only) hesapta real trading anlamsız olduğu
          için reddedilir — fail-closed.
        - Değişiklik `audit_log`'a yazılır.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
        account_id = account_id.strip()

        def _flip(conn: sqlite3.Connection) -> dict[str, Any]:
            row = conn.execute(
                "SELECT " + ", ".join(_ACCOUNT_FIELDS) + " FROM accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {account_id}")
            if not row["encrypted_api_key"] or not row["encrypted_secret"]:
                raise RasatError(
                    ErrorCode.ACCOUNT_NO_CREDENTIALS,
                    "credential'sız (public/read-only) hesapta real trading açılamaz",
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
        """Kill switch: real hesabı `paper`'a çevirir (3.5).

        `enable_real_trading`'in tersi; audit_log'a yazılır. Zaten paper ise
        idempotent davranır.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
        account_id = account_id.strip()

        def _flip(conn: sqlite3.Connection) -> dict[str, Any]:
            row = conn.execute(
                "SELECT " + ", ".join(_ACCOUNT_FIELDS) + " FROM accounts WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {account_id}")
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
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {account_id}")
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
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {account_id}")
        if not row["encrypted_api_key"] or not row["encrypted_secret"]:
            raise RasatError(ErrorCode.ACCOUNT_NO_CREDENTIALS, "account public/read-only modda; API credential yok")
        try:
            return (
                self.secret_store.decrypt(account_id, "api_key", row["encrypted_api_key"]),
                self.secret_store.decrypt(account_id, "api_secret", row["encrypted_secret"]),
            )
        except SecretDecryptError as exc:
            raise RasatError(ErrorCode.CREDENTIAL_DECRYPT_FAILED, "account credential çözülemedi") from exc
