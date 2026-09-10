"""Time-unit helpers.

Single time standard: UNIX epoch **seconds**. Binance REST raw timestamps are in
milliseconds (`open_time`, `event_time`, `time`); `to_epoch_seconds` normalizes them
to seconds once at input. No mixed units should remain in code; PA/freshness/
retention/session/envelope all expect seconds.
"""

from __future__ import annotations

# As seconds this reaches about year 5138; real ms values are above ~1.7e12.
_MS_THRESHOLD = 100_000_000_000


def to_epoch_seconds(ts: int | float) -> int:
    """Convert a Binance ms value to seconds; leave a value already in seconds unchanged.

    Because test fixtures such as FakeRest may produce seconds, use threshold-based
    conversion instead of blindly dividing by `/1000`: values above ~10^11 are
    treated as ms and divided by 1000; lower values are treated as seconds.
    """
    value = int(ts)
    if value >= _MS_THRESHOLD:
        return value // 1000
    return value
