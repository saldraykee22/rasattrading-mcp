"""Zaman birimi yardımcıları.

Tek zaman standardı: UNIX epoch **saniye**. Binance REST ham timestamp'leri
milisaniye döndürür (`open_time`, `event_time`, `time`); `to_epoch_seconds`
bunları girişte bir kez saniyeye normalize eder. Kod içinde karışık birim
kalmamalı — PA/freshness/retention/session/envelope'nin tamamı saniye bekler.
"""

from __future__ import annotations

# Saniye olarak yıl ~5138'a denk gelir; gerçek ms değerleri ~1.7e12 üzerindedir.
_MS_THRESHOLD = 100_000_000_000


def to_epoch_seconds(ts: int | float) -> int:
    """Binance ms değerini saniyeye çevirir; zaten saniye olanı aynen bırakır.

    Test fixture'ları saniye üretebildiği için (FakeRest gibi) keskin `/1000`
    yerine eşik bazlı dönüşüm kullanılır: ~10^11 üzerindeki değerler ms kabul
    edilip 1000'e bölünür, altı saniye sayılır.
    """
    value = int(ts)
    if value >= _MS_THRESHOLD:
        return value // 1000
    return value
