"""PA algoritma sürümleri ve sabit eşikler.

Bir sürüm içinde TÜM eşikler/lookback/toleranslar sabittir. Bir eşik değişirse
sürüm artmalı ve eski kayıtlar `effective_to` ile kapatılmalıdır (immutable
desen — geçmiş bozulmaz). Bu dosya tek doğruluk kaynağıdır.
"""

from __future__ import annotations

# ---------------- sürümler ----------------

SWING_ALGO_VERSION = "swing-v1"
LIQUIDITY_ALGO_VERSION = "liquidity-v1"
OBFVG_ALGO_VERSION = "obfvg-v1"
VWAP_ALGO_VERSION = "vwap-v1"
SESSION_ALGO_VERSION = "session-v1"

# ---------------- 2.1 swing / BOS / CHoCH ----------------

# Fractal yarım pencere: pivot, sağında ve solunda `lookback` bar içinde
# en yüksek (high) / en düşük (low) olan bar işaretlenir (2 → 2-2 fractal).
SWING_LOOKBACK = 2

# ---------------- 2.2 likidite ----------------

# Eşit high/low kümesi toleransı: seviyeler fiyatın bu yüzdesi kadar
# yakınsa aynı likidite bölgesi sayılır. (0.05 → %0.05)
EQUAL_LEVEL_TOLERANCE_PCT = 0.05

# Bir bölgenin "sweep" (likidite alımı) sayılması için seviyeyi geçmesi
# gereken fiyat yüzdesi. 0.0 → seviyeyi (wick ile) geçmesi yeterli.
SWEEP_EXCEED_PCT = 0.0

# Likidite skoru bileşen ağırlıkları (0-100 ölçek).
LIQUIDITY_WEIGHTS = {
    "equal_levels": 40.0,
    "open_interest": 35.0,
    "funding_rate": 15.0,
    "liquidation": 10.0,
}

# ---------------- 2.3 OB/FVG ----------------

# FVG en küçük boşluk (fiyat yüzdesi): boşluk bu eşiğin altındaysa
# gürültü sayılır ve bölge üretilmez.
FVG_MIN_GAP_PCT = 0.0

# ---------------- 2.3 session ----------------

# Killzone tanımları (UTC saat cinsinden, sabit). Fiyat seviyeleri günün
# (UTC) bu aralıklarındaki en yüksek/düşük fiyatından üretilir.
SESSION_TIMEZONE = "UTC"
KILLZONES = {
    "asian": (0, 8),
    "london": (7, 16),
    "newyork": (12, 21),
}
