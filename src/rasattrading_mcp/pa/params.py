"""PA algorithm versions and fixed thresholds.

ALL thresholds/lookbacks/tolerances are fixed within a version. When a threshold
changes, increment the version and close old records with `effective_to` (immutable
pattern — history is not corrupted). This file is the single source of truth.
"""

from __future__ import annotations

# ---------------- versions ----------------

SWING_ALGO_VERSION = "swing-v1"
LIQUIDITY_ALGO_VERSION = "liquidity-v1"
OBFVG_ALGO_VERSION = "obfvg-v1"
VWAP_ALGO_VERSION = "vwap-v1"
SESSION_ALGO_VERSION = "session-v1"

# ---------------- 2.1 swing / BOS / CHoCH ----------------

# Fractal half-window: mark a pivot when its bar is the highest (high) / lowest
# (low) within `lookback` bars on both sides (2 → 2-2 fractal).
SWING_LOOKBACK = 2

# ---------------- 2.2 likidite ----------------

# Equal high/low cluster tolerance: levels within this percentage of price count
# as the same liquidity zone. (0.05 → 0.05%)
EQUAL_LEVEL_TOLERANCE_PCT = 0.05

# Price percentage required to exceed a level for a zone to count as a "sweep"
# (liquidity taken). 0.0 → crossing the level with a wick is enough.
SWEEP_EXCEED_PCT = 0.0

# Liquidity-score component weights (0-100 scale).
LIQUIDITY_WEIGHTS = {
    "equal_levels": 40.0,
    "open_interest": 35.0,
    "funding_rate": 15.0,
    "liquidation": 10.0,
}

# ---------------- 2.3 OB/FVG ----------------

# Minimum FVG gap (price percentage): gaps below this threshold are noise and
# do not produce a zone.
FVG_MIN_GAP_PCT = 0.0

# Order-block dedup tolerance: OBs covering the same or a very close price range
# merge into one logical zone (2.15 fix — multiple BOS/CHoCH events selected the
# same candle as an OB candidate and produced separate records).
OB_DEDUP_TOLERANCE_PCT = 0.05

# ---------------- 2.3 session ----------------

# Killzone definitions (fixed UTC hours). Price levels use the highest/lowest
# price in these ranges of the UTC day.
SESSION_TIMEZONE = "UTC"
KILLZONES = {
    "asian": (0, 8),
    "london": (7, 16),
    "newyork": (12, 21),
}
