"""OS-protected credential storage for Binance account secrets.

The normal Windows backend is DPAPI (via :mod:`win32crypt`).  DPAPI binds the
ciphertext to the current Windows user, so copying the SQLite database to a
different Windows account does not make the API credentials readable.  A
``keyring`` backend is kept as a portability fallback for environments where
the pywin32 binding is unavailable; keyring itself is expected to use an OS
credential backend and never writes a plaintext secret to the accounts table.

This module deliberately exposes only opaque bytes to the account service.  It
does not log, stringify, or return decrypted values except through the
internal ``decrypt`` method used by execution code later in the project.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("rasattrading.storage.credentials")

try:  # pywin32 is installed on the supported Windows runtime.
    import win32crypt  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - exercised on non-Windows CI
    win32crypt = None  # type: ignore[assignment]

try:  # Optional fallback; Windows DPAPI remains the preferred backend.
    import keyring  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dependency
    keyring = None  # type: ignore[assignment]


class SecretStoreError(RuntimeError):
    """Base error for credential storage without exposing secret material."""


class SecretStoreUnavailable(SecretStoreError):
    """No OS-protected credential backend is available."""


class SecretDecryptError(SecretStoreError):
    """An opaque credential could not be decrypted by its backend."""


class SecretStore:
    """Encrypt/decrypt account credentials using a user-bound OS backend.

    ``win32crypt`` is injectable for tests and is intentionally resolved at
    construction time.  The keyring module is injectable for the same reason.
    The keyring descriptor stored in SQLite contains only a service/user
    reference; the secret remains in the OS keyring.
    """

    DPAPI_PREFIX = b"dpapi:v1:"
    KEYRING_PREFIX = b"keyring:v1:"
    KEYRING_SERVICE = "rasattrading-mcp"

    def __init__(self, *, win32crypt_module: Any = None, keyring_module: Any = None) -> None:
        self._win32crypt = win32crypt if win32crypt_module is None else win32crypt_module
        self._keyring = keyring if keyring_module is None else keyring_module

    @property
    def backend(self) -> str | None:
        if self._win32crypt is not None and os.name == "nt":
            return "dpapi"
        if self._keyring is not None:
            return "keyring"
        return None

    @property
    def available(self) -> bool:
        return self.backend is not None

    @staticmethod
    def _validate_secret(value: str) -> None:
        if not isinstance(value, str) or not value:
            raise SecretStoreError("credential değeri geçerli bir string olmalı")

    @staticmethod
    def _keyring_username(account_id: str, field: str) -> str:
        # account_id and field are generated/allow-listed by AccountService.
        return f"{account_id}:{field}"

    def encrypt(self, account_id: str, field: str, value: str) -> bytes:
        """Return opaque ciphertext/reference bytes; never return ``value``."""

        self._validate_secret(value)
        if self._win32crypt is not None and os.name == "nt":
            try:
                protected = self._win32crypt.CryptProtectData(
                    value.encode("utf-8"),
                    "rasattrading-mcp account credential",
                    None,
                    None,
                    None,
                    0,
                )
                # pywin32 returns raw bytes; a few compatible bindings return
                # the ``(description, bytes)`` tuple used by UnprotectData.
                ciphertext = protected[1] if isinstance(protected, tuple) else protected
                if not isinstance(ciphertext, (bytes, bytearray)):
                    raise TypeError("DPAPI ciphertext tipi geçersiz")
                return self.DPAPI_PREFIX + bytes(ciphertext)
            except Exception as exc:  # noqa: BLE001 - do not leak provider details
                # Do not silently downgrade a Windows installation that has
                # DPAPI available: an arbitrary keyring backend could be a
                # plaintext file backend.  Fail closed instead.
                logger.warning("DPAPI credential encryption başarısız")
                if os.name == "nt" and self._win32crypt is not None:
                    raise SecretStoreUnavailable("Windows DPAPI kullanılamıyor") from exc

        if self._keyring is not None:
            username = self._keyring_username(account_id, field)
            try:
                self._keyring.set_password(self.KEYRING_SERVICE, username, value)
                return self.KEYRING_PREFIX + username.encode("utf-8")
            except Exception as exc:  # noqa: BLE001 - provider may include secret text
                raise SecretStoreUnavailable("OS credential backend kullanılamıyor") from exc

        raise SecretStoreUnavailable("Windows DPAPI/keyring backend kullanılamıyor")

    def decrypt(self, account_id: str, field: str, opaque: bytes) -> str:
        """Decrypt an opaque value for internal execution use only."""

        if not isinstance(opaque, (bytes, bytearray)):
            raise SecretDecryptError("credential ciphertext tipi geçersiz")
        blob = bytes(opaque)
        if blob.startswith(self.DPAPI_PREFIX):
            if self._win32crypt is None:
                raise SecretDecryptError("DPAPI backend kullanılamıyor")
            try:
                _description, plaintext = self._win32crypt.CryptUnprotectData(blob[len(self.DPAPI_PREFIX) :], None)
                if isinstance(plaintext, bytes):
                    return plaintext.decode("utf-8")
                if isinstance(plaintext, str):
                    return plaintext
                raise TypeError("DPAPI plaintext tipi geçersiz")
            except Exception as exc:  # noqa: BLE001 - never expose ciphertext/secret
                raise SecretDecryptError("credential çözülemedi") from exc

        if blob.startswith(self.KEYRING_PREFIX):
            if self._keyring is None:
                raise SecretDecryptError("keyring backend kullanılamıyor")
            try:
                username = blob[len(self.KEYRING_PREFIX) :].decode("utf-8", errors="strict")
                # Do not trust a copied descriptor to read another field/account.
                expected = self._keyring_username(account_id, field)
                if username != expected:
                    raise SecretDecryptError("credential referansı geçersiz")
                value = self._keyring.get_password(self.KEYRING_SERVICE, username)
            except Exception as exc:  # noqa: BLE001
                if isinstance(exc, SecretDecryptError):
                    raise
                raise SecretDecryptError("credential çözülemedi") from exc
            if not isinstance(value, str) or not value:
                raise SecretDecryptError("credential bulunamadı")
            return value

        raise SecretDecryptError("bilinmeyen credential formatı")

    def delete(self, account_id: str, field: str, opaque: bytes | None) -> None:
        """Best-effort cleanup for keyring references; DPAPI needs no cleanup."""

        if not opaque or self._keyring is None:
            return
        blob = bytes(opaque)
        if not blob.startswith(self.KEYRING_PREFIX):
            return
        try:
            username = blob[len(self.KEYRING_PREFIX) :].decode("utf-8", errors="strict")
            if username != self._keyring_username(account_id, field):
                return
            self._keyring.delete_password(self.KEYRING_SERVICE, username)
        except Exception:  # noqa: BLE001 - deletion must not log credential material
            logger.warning("keyring credential cleanup başarısız (account_id=%s, field=%s)", account_id, field)


# Explicit alias for callers/tests that want to document the primary backend.
DPAPISecretStore = SecretStore
