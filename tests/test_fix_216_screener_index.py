"""2.16 FIX — Screener structure/sweep index-alignment regression tests.

Found during live validation (artifacts/rasattrading-live-buy-scan-validation):
`screener._build_context` built its context from 300 candles, but the PA engine's
the structure/liquidity payload used the default 200-candle window. Therefore
`_eval_structure_event`/`_eval_liquidity_sweep` compared real event/sweep indices
in the payload (for example, BOS index ~192-195) with `len(ctx) - since_bars`
(a 300-candle basis) and MISSED them for reasonable `since_bars` (24/36/100);
they appeared only for nonsensically large values such as `since_bars=200`.

2.16 fix:
- Screener context uses the SAME window as the PA engine (`PA_LOOKBACK`),
- Structure events and liquidity sweeps are compared by absolute `open_time`
  (independent of index reference frame); liquidity zones also carry
  `swept_at_time`.
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.analysis import PAEngine, PA_LOOKBACK
from rasattrading_mcp.pa.screener import Screener
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1h"
PERIOD = 3600

# Live-evidence scenario: the last bullish BOS event for BMT/HUMA/AUSDT is at
# ~192-195 in the 200-candle payload window. With a 300-candle context (old code),
# the `len - since` threshold for 24/36/100 would be 276/264/200 and 192-195
# would never match.
TARGETS = {"BMTUSDT": 193, "HUMAUSDT": 195, "AUSDT": 194}


def breakout_series(target: int, window: int = 200) -> list[tuple[float, float, float, float]]:
    """A `window`-candle series whose last bullish BOS event is at `target`."""
    rows: list[tuple[float, float, float, float]] = []
    rows += [
        (100, 100.5, 99.5, 100),
        (100, 100.5, 99.5, 100),
        (99, 100, 98, 99.5),          # swing low
        (99.5, 100.5, 99, 100),
        (100, 102, 99.5, 101),        # swing high → trend up
    ]
    rows += [(101, 103, 100.5, 102.5)]  # bos_bullish@5, new swing high 103
    while len(rows) < target:
        rows.append((102, 102.5, 101.5, 102))
    rows.append((103, 105, 102.5, 104.5))  # bos_bullish@target
    while len(rows) < window:
        rows.append((104, 104.5, 103.5, 104))
    return rows


def old_breakout_series(window: int = 200) -> list[tuple[float, float, float, float]]:
    """Last BOS is at ~index 50—it must not match reasonable since_bars (negative control)."""
    return breakout_series(50, window)


def sweep_series(target: int = 195, window: int = 200) -> list[tuple[float, float, float, float]]:
    """Series with an equal-high zone swept at the `target` index."""
    rows: list[tuple[float, float, float, float]] = [
        (100, 100.5, 99.5, 100),
        (100, 100.5, 99.5, 100),
        (99, 100, 98, 99.5),
        (99.5, 100.5, 99, 100),
        (100, 101, 99.5, 100.5),      # swing high 101.00
        (100.5, 100.5, 100, 100.5),
        (100.5, 100.5, 100, 100.5),
        (100, 101.04, 99.5, 100.5),   # swing high 101.04 → equal_highs
        (100.5, 100.5, 100, 100.5),
        (100.5, 100.5, 100, 100.5),
    ]
    while len(rows) < target:
        rows.append((100, 100.5, 99.5, 100.4))
    rows.append((100.5, 102, 100.5, 101.5))  # sweep: 102 > 101.04
    while len(rows) < window:
        rows.append((101, 101.5, 100.5, 101))
    return rows


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


@pytest.fixture
async def db(cfg):
    d = Database(cfg.db_path)
    await d.start()
    await run_migrations(d)
    yield d
    await d.stop()


async def seed(db, symbol, rows, n_extra_flat=100):
    """Add more than `n_extra_flat` candles — the old code built the context from 300 candles,
    while building, the payload still used the last 200 candles (source of misalignment)."""
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    flat = [(100, 100.5, 99.5, 100)] * n_extra_flat
    full = flat + rows
    n = len(full)

    def _w(conn):
        for i, (o, h, l, c) in enumerate(full):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, latest_closed - (n - 1 - i) * PERIOD, o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


async def _seed_with_volumes(db, symbol, ohlc, volumes):
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    n = len(ohlc)

    def _w(conn):
        for i, (o, h, l, c) in enumerate(ohlc):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, latest_closed - (n - 1 - i) * PERIOD, o, h, l, c, volumes[i], "spot", int(time.time())),
            )

    await db.write(_w)


async def _seed_futures(db, symbol, ftype, times_values):
    now = int(time.time())

    def _w(conn):
        conn.executemany(
            "INSERT INTO futures_context (symbol, type, event_time, value, fetched_at, freshness) VALUES (?,?,?,?,?,?)",
            [(symbol, ftype, t, v, now, "fresh") for t, v in times_values],
        )

    await db.write(_w)


# ---------------------------------------------------------------------------
# S1 — structure_event: real last BOS now matches reasonable since_bars
# ---------------------------------------------------------------------------


async def test_2_16_recent_bos_matches_reasonable_since_bars(db):
    """BMT/HUMA/AUSDT last BOS index ~192-195 → since_bars=24/36/100 now match.

    The old context had 300 candles; because `ev['index'] >= 300 - since` was
    276/264/200 for 24/36/100, index ~193 never matched (only since_bars=200
    matched—the exact live-evidence observation).
    """
    for sym, target in TARGETS.items():
        await seed(db, sym, breakout_series(target))
    screener = Screener(db)
    for since in (24, 36, 100):
        res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": since}])
        syms = {s["symbol"] for s in res["symbols"]}
        assert {"BMTUSDT", "HUMAUSDT", "AUSDT"} <= syms, f"since_bars={since} did not match: {syms}"


async def test_2_16_recent_bos_payload_indices_are_real(db):
    """Confirm that last BOS indices in the payload are really in ~192-195 (scenario check)."""
    await seed(db, "BMTUSDT", breakout_series(193))
    engine = PAEngine(db)
    ms = await engine.get_market_structure("BMTUSDT", TF)
    bos = [e["index"] for e in ms["structure"]["events"] if e["type"] == "bos_bullish"]
    assert bos[-1] == 193  # Last BOS is ~193 in the 200-candle payload window.


async def test_2_16_old_bos_does_not_match_small_since_bars(db):
    """Last BOS ~index 50 → since_bars=24/50/100 must not match; since_bars=150 must match."""
    await seed(db, "OLDBOS", old_breakout_series())
    screener = Screener(db)
    for since in (24, 50, 100):
        res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": since}])
        assert "OLDBOS" not in {s["symbol"] for s in res["symbols"]}, f"since_bars={since} should not match"
    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": 150}])
    assert "OLDBOS" in {s["symbol"] for s in res["symbols"]}


# ---------------------------------------------------------------------------
# S2 — liquidity_sweep_occurred: sweep now matches reasonable since_bars
# ---------------------------------------------------------------------------


async def test_2_16_recent_sweep_matches_reasonable_since_bars(db):
    """Equal-high zone swept at ~index 195 → since_bars=24/50 matches."""
    await seed(db, "SWEEPUSDT", sweep_series(195))
    screener = Screener(db)
    for since in (24, 50):
        res = await screener.scan([{"type": "liquidity_sweep_occurred", "since_bars": since}])
        assert "SWEEPUSDT" in {s["symbol"] for s in res["symbols"]}, f"since_bars={since} did not match"


async def test_2_16_zone_carries_swept_at_time(db):
    """Liquidity zones carry sweep time as absolute open_time (for alignment)."""
    await seed(db, "SWEEPUSDT", sweep_series(195))
    engine = PAEngine(db)
    data = await engine.get_liquidity_zones("SWEEPUSDT", TF, include_mitigated=True)
    swept = [z for z in data["zones"] if z.get("swept_at")]
    assert swept
    z = swept[0]
    assert z["swept_at_time"] is not None
    assert z["swept_at_time"] > 0


# ---------------------------------------------------------------------------
# S3 — root cause: screener context uses the same window as the PA engine
# ---------------------------------------------------------------------------


async def test_2_16_screener_context_uses_pa_lookback(db):
    """`_build_context` reads PA_LOOKBACK candles (not 300)—index frame is aligned."""
    await seed(db, "BMTUSDT", breakout_series(193))
    screener = Screener(db)
    ctx = await screener._build_context("BMTUSDT", TF, needs_analysis=False, filter_types=[])
    assert ctx is not None
    assert len(ctx["candles"]) <= PA_LOOKBACK
    assert len(ctx["candles"]) == PA_LOOKBACK  # 200 closed candles available.


# ---------------------------------------------------------------------------
# S4 — Issue 2: symbol_valid alongside data_stale (delist filtering)
# ---------------------------------------------------------------------------


class _FakeUniverse:
    def __init__(self, symbols: list[str]) -> None:
        self._symbols = list(symbols)

    def snapshot(self) -> list[str]:
        return list(self._symbols)

    def contains(self, symbol: str) -> bool:
        return symbol in self._symbols


class _FakePipeline:
    def __init__(self, symbols: list[str]) -> None:
        self.universe = _FakeUniverse(symbols)


class _StaleCandidateScreener(Screener):
    """Screener that returns candidates from DB (including delisted symbols).

    In live use, delisted symbols were already filtered when `_candidate_symbols`
    used the universe snapshot; the real risk was candidates bypassing universe
    validation in the DB fallback path. This subclass pulls candidates from DB
    (including COHRUSDT) to verify `_symbol_validity` actually runs.
    """

    async def _candidate_symbols(self) -> list[str]:
        def _q(conn):
            rows = conn.execute("SELECT DISTINCT symbol FROM candles WHERE source='spot'").fetchall()
            return sorted(r["symbol"] for r in rows)

        return await self.db.read(_q)


async def test_2_16_invalid_symbol_filtered_by_universe(db):
    """A delisted symbol no longer matches misleadingly (even when data_stale=false)."""
    # Live examples COHRUSDT/USARUSDT/FLNCUSDT: candle data exists but symbol is absent from universe.
    for sym, target in TARGETS.items():
        await seed(db, sym, breakout_series(target))
    await seed(db, "COHRUSDT", breakout_series(193))  # NOT in universe.

    screener = _StaleCandidateScreener(db, pipeline=_FakePipeline(list(TARGETS)))  # COHRUSDT is not in universe.
    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}])
    syms = [s["symbol"] for s in res["symbols"]]
    assert "COHRUSDT" not in syms  # delisted — absent from scan results
    assert {"BMTUSDT", "HUMAUSDT", "AUSDT"} <= set(syms)
    for s in res["symbols"]:
        assert s["symbol_valid"] is True


async def test_2_16_symbol_valid_unknown_without_pipeline(db):
    """Without a pipeline, symbol_valid is None (universe cannot be validated)—preserve behavior."""
    await seed(db, "BMTUSDT", breakout_series(193))
    screener = Screener(db)  # No pipeline.
    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}])
    bmt = next(s for s in res["symbols"] if s["symbol"] == "BMTUSDT")
    assert bmt["symbol_valid"] is None


# ---------------------------------------------------------------------------
# S5 — Issue 3: scan rows are auditable (matched_filters + signal_summary)
# ---------------------------------------------------------------------------


async def test_2_16_scan_rows_are_auditable(db):
    """Return the filters triggering a match and raw signal summary per row."""
    rows = breakout_series(193)
    # `above_below_vwap: above` requires close > vwap; the flat 104 series ended
    # with day-anchored VWAP exactly 104.0 and did not match → move final bars above VWAP.
    rows[-2:] = [(104, 105.5, 104, 105.2), (104.5, 106.0, 104.5, 105.7)]
    await seed(db, "BMTUSDT", rows)
    screener = Screener(db)
    res = await screener.scan(
        [
            {"type": "structure_event", "event": "bos_bullish", "since_bars": 24},
            {"type": "above_below_vwap", "position": "above"},
        ],
        combine="AND",
    )
    bmt = next(s for s in res["symbols"] if s["symbol"] == "BMTUSDT")
    assert "structure_event" in bmt["matched_filters"]
    assert "above_below_vwap" in bmt["matched_filters"]
    assert "bos_bullish" in bmt["signal_summary"]
    assert "vwap above" in bmt["signal_summary"]
    assert bmt["as_of"] is not None
    assert bmt["price"] > 0


async def test_2_16_matched_filters_or_semantics(db):
    """In an OR combination, only matching filters enter matched_filters."""
    await seed(db, "BMTUSDT", breakout_series(193))
    screener = Screener(db)
    res = await screener.scan(
        [
            {"type": "structure_event", "event": "bos_bullish", "since_bars": 24},
            {"type": "price_change", "window_bars": 10, "min": 500},
        ],
        combine="OR",
    )
    bmt = next(s for s in res["symbols"] if s["symbol"] == "BMTUSDT")
    assert "structure_event" in bmt["matched_filters"]
    assert "price_change" not in bmt["matched_filters"]  # No 500% change → no match.


async def test_2_16_signal_summary_price_change_has_pct(db):
    """price_change match carries the real % value in signal_summary (auditability)."""
    await seed(db, "BMTUSDT", breakout_series(193))
    screener = Screener(db)
    res = await screener.scan([{"type": "price_change", "window_bars": 10, "min": 1.0}])
    bmt = next(s for s in res["symbols"] if s["symbol"] == "BMTUSDT")
    assert "price 10bar +1.96%" in bmt["signal_summary"]


async def test_2_16_signal_summary_volume_change_has_pct(db):
    """volume_change match carries the real % value in signal_summary (auditability)."""
    rows = [(100, 100.5, 99.5, 100)] * 48
    vols = [10.0] * 24 + [15.0] * 24  # Last 24 bars are +50% versus previous 24.
    await _seed_with_volumes(db, "BMTUSDT", rows, vols)
    screener = Screener(db)
    res = await screener.scan([{"type": "volume_change", "recent_bars": 24, "baseline_bars": 24}])
    bmt = next(s for s in res["symbols"] if s["symbol"] == "BMTUSDT")
    assert "volume 24/24bar +50.00%" in bmt["signal_summary"]


async def test_2_16_signal_summary_oi_change_has_pct(db):
    """oi_change match carries the real % value in signal_summary (auditability)."""
    await _seed_with_volumes(db, "BTCUSDT", [(100, 100.5, 99.5, 100)] * 48, [10.0] * 48)
    await _seed_futures(db, "BTCUSDT", "open_interest", [(100, 10.0), (200, 30.0)])  # +%200
    screener = Screener(db)
    res = await screener.scan([{"type": "oi_change", "window": 1}])
    btc = next(s for s in res["symbols"] if s["symbol"] == "BTCUSDT")
    assert "oi 1bar +200.00%" in btc["signal_summary"]
