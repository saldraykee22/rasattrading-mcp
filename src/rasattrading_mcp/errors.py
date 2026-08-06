"""Canonical hata kodu sözlüğü (Modül 2/3 genişletecektir)."""

from __future__ import annotations

from typing import Any


class ErrorCode:
    INTERNAL_ERROR = "INTERNAL_ERROR"
    INVALID_REQUEST = "INVALID_REQUEST"
    UNAUTHORIZED = "UNAUTHORIZED"
    NOT_READY = "NOT_READY"
    TOOL_NOT_FOUND = "TOOL_NOT_FOUND"
    INVALID_SYMBOL = "INVALID_SYMBOL"
    STALE_DATA = "STALE_DATA"
    RATE_LIMITED = "RATE_LIMITED"
    TIMEOUT = "TIMEOUT"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    ACCOUNT_NOT_FOUND = "ACCOUNT_NOT_FOUND"
    ACCOUNT_EXISTS = "ACCOUNT_EXISTS"
    ACCOUNT_NO_CREDENTIALS = "ACCOUNT_NO_CREDENTIALS"
    CREDENTIAL_STORE_UNAVAILABLE = "CREDENTIAL_STORE_UNAVAILABLE"
    CREDENTIAL_DECRYPT_FAILED = "CREDENTIAL_DECRYPT_FAILED"
    RISK_LIMIT_EXCEEDED = "RISK_LIMIT_EXCEEDED"
    SYMBOL_NOT_ALLOWED = "SYMBOL_NOT_ALLOWED"
    OVERRIDE_NOT_AVAILABLE = "OVERRIDE_NOT_AVAILABLE"
    TRADING_NOT_ENABLED = "TRADING_NOT_ENABLED"
    FILTER_VIOLATION = "FILTER_VIOLATION"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"


class RasatError(Exception):
    """Tüm modüllerde fırlatılan canonical hata."""

    def __init__(
        self,
        code: str,
        message: str,
        details: Any = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details
        self.http_status = http_status

    def to_dict(self) -> dict:
        d: dict = {"code": self.code, "message": self.message}
        if self.details is not None:
            d["details"] = self.details
        return d


_HTTP_STATUS: dict[str, int] = {
    ErrorCode.UNAUTHORIZED: 401,
    ErrorCode.INVALID_REQUEST: 400,
    ErrorCode.TOOL_NOT_FOUND: 404,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.NOT_READY: 503,
    ErrorCode.TIMEOUT: 504,
    ErrorCode.STALE_DATA: 409,
    ErrorCode.INVALID_SYMBOL: 422,
    ErrorCode.INSUFFICIENT_BALANCE: 409,
    ErrorCode.ACCOUNT_NOT_FOUND: 404,
    ErrorCode.ACCOUNT_EXISTS: 409,
    ErrorCode.ACCOUNT_NO_CREDENTIALS: 409,
    ErrorCode.CREDENTIAL_STORE_UNAVAILABLE: 503,
    ErrorCode.CREDENTIAL_DECRYPT_FAILED: 500,
    ErrorCode.RISK_LIMIT_EXCEEDED: 422,
    ErrorCode.SYMBOL_NOT_ALLOWED: 422,
    ErrorCode.OVERRIDE_NOT_AVAILABLE: 409,
    ErrorCode.TRADING_NOT_ENABLED: 409,
    ErrorCode.FILTER_VIOLATION: 422,
}


def http_status_for(code: str) -> int:
    return _HTTP_STATUS.get(code, 500)
