"""Temel doğruluk kontrolleri (ticket 3.3).

Bu kontroller risk politikasından ve override'dan BAĞIMSIZ, her emirde zorunlu ve
kapatılamaz olan "çöp emir göndermeme" garantileridir:
- yetersiz bakiye
- fiyat staleness
- stop yönü mantığı (long'da stop < entry)
- sembol geçerlilik / TRADING durumu

Pure fonksiyonlardır; veriyi dışarıdan alırlar (pipeline/universe/handler çağırır).
3.4 emir göndermeden önce bu kontrolleri çalıştırır; override bu kontrolleri asla atlamaz.
"""

from __future__ import annotations

from .envelope import FRESHNESS_FRESH
from .errors import ErrorCode, RasatError


def check_symbol_valid(symbol: str, universe: set[str] | list[str] | None) -> None:
    """Sembol evrende mi (status=TRADING + USDT evreni)? Değilse reddet."""
    if not isinstance(symbol, str) or not symbol:
        raise RasatError(ErrorCode.INVALID_SYMBOL, "symbol zorunlu (string)")
    if universe is None:
        raise RasatError(ErrorCode.INVALID_SYMBOL, f"sembol evreni mevcut değil — {symbol} doğrulanamadı")
    if symbol not in universe:
        raise RasatError(ErrorCode.INVALID_SYMBOL, f"evrende bilinmeyen / TRADING olmayan sembol: {symbol}")


def check_price_fresh(freshness: str | None, symbol: str) -> None:
    """Fiyat verisi `fresh` değilse emir reddedilir (stale veriye emir verilmez)."""
    if freshness != FRESHNESS_FRESH:
        raise RasatError(ErrorCode.STALE_DATA, f"fiyat verisi stale — emir reddedildi: {symbol}")


def check_stop_direction(side: str, entry: float, stop_loss: float) -> None:
    """Stop yönü mantığı: long'da stop < entry, short'ta stop > entry."""
    normalized = (side or "BUY").upper()
    if normalized == "BUY":
        if stop_loss >= entry:
            raise RasatError(ErrorCode.INVALID_REQUEST, "long'da stop_loss entry'nin altında olmalı")
    elif normalized == "SELL":
        if stop_loss <= entry:
            raise RasatError(ErrorCode.INVALID_REQUEST, "short'ta stop_loss entry'nin üstünde olmalı")
    else:
        raise RasatError(ErrorCode.INVALID_REQUEST, f"geçersiz side: {side}")


def check_sufficient_balance(available_quote: float, required_quote: float, symbol: str) -> None:
    """Spot'ta emir tutarı (notional + tahmini fee) bakiyeyi aşarsa reddet."""
    if required_quote > available_quote:
        raise RasatError(
            ErrorCode.INSUFFICIENT_BALANCE,
            f"yetersiz bakiye: gereken {required_quote:.8f} > mevcut {available_quote:.8f} ({symbol})",
        )
