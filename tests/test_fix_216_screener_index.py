"""2.16 FIX — Screener yapı/sweep index hizalaması regresyon testleri.

Canlı doğrulamada (artifacts/rasattrading-live-buy-scan-validation) bulundu:
`screener._build_context` context'i 300 mumla kuruyordu ama PA engine'in
yapı/likidite payload'ı varsayılan 200 mumluk pencerede hesaplanıyordu. Bu
yüzden `_eval_structure_event`/`_eval_liquidity_sweep`, payload'daki gerçek
son olay/sweep indekslerini (örn. BOS index ~192-195) `len(ctx) - since_bars`
(yani 300 tabanlı) ile karşılaştırıp makul `since_bars` (24/36/100) ile KAÇIRIYORDU;
yalnızca `since_bars=200` gibi anlamsız büyük değerlerde görünüyorlardı.

2.16 fix:
- Screener context'i PA engine ile AYNI pencereyi kullanır (`PA_LOOKBACK`),
- Yapı event ve likidite sweep karşılaştırması mutlak `open_time` üzerinden
  yapılır (index referans çerçevesinden bağımsız); liquidity zone'ları ek
  `swept_at_time` taşır.
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

# Canlı kanıttaki senaryo: BMT/HUMA/AUSDT'nin son bullish BOS event'i 200 mumluk
# payload penceresinde ~192-195 indeksinde. Context 300 mum olsaydı (eski kod)
# `len - since` eşiği 24/36/100 için 276/264/200 olur ve 192-195 asla eşleşmezdi.
TARGETS = {"BMTUSDT": 193, "HUMAUSDT": 195, "AUSDT": 194}


def breakout_series(target: int, window: int = 200) -> list[tuple[float, float, float, float]]:
    """Son bullish BOS event'i `target` indeksinde olan `window` mumluk seri."""
    rows: list[tuple[float, float, float, float]] = []
    rows += [
        (100, 100.5, 99.5, 100),
        (100, 100.5, 99.5, 100),
        (99, 100, 98, 99.5),          # swing low
        (99.5, 100.5, 99, 100),
        (100, 102, 99.5, 101),        # swing high → trend up
    ]
    rows += [(101, 103, 100.5, 102.5)]  # bos_bullish@5, yeni swing high 103
    while len(rows) < target:
        rows.append((102, 102.5, 101.5, 102))
    rows.append((103, 105, 102.5, 104.5))  # bos_bullish@target
    while len(rows) < window:
        rows.append((104, 104.5, 103.5, 104))
    return rows


def old_breakout_series(window: int = 200) -> list[tuple[float, float, float, float]]:
    """Son BOS ~index 50'de — makul since_bars ile eşleşmemeli (negatif kontrol)."""
    return breakout_series(50, window)


def sweep_series(target: int = 195, window: int = 200) -> list[tuple[float, float, float, float]]:
    """Equal-high bölgesi `target` indeksinde sweep edilen seri."""
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
    """`n_extra_flat` mumdan fazla veri ekler — eski kod context'i 300 mumla
    kurarken payload hâlâ son 200 mumda hesaplanıyordu (hizasızlığın kaynağı)."""
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


# ---------------------------------------------------------------------------
# S1 — structure_event: gerçek son BOS artık makul since_bars ile eşleşiyor
# ---------------------------------------------------------------------------


async def test_2_16_recent_bos_matches_reasonable_since_bars(db):
    """BMT/HUMA/AUSDT son BOS index ~192-195 → since_bars=24/36/100 artık eşleşir.

    Eski kodda context 300 mumdu; `ev['index'] >= 300 - since` eşiği 24/36/100
    için 276/264/200 olduğundan index ~193 hiçbir zaman eşleşmiyordu (yalnızca
    since_bars=200 eşleştiriyordu — canlı kanıtın birebir gözlemi).
    """
    for sym, target in TARGETS.items():
        await seed(db, sym, breakout_series(target))
    screener = Screener(db)
    for since in (24, 36, 100):
        res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": since}])
        syms = {s["symbol"] for s in res["symbols"]}
        assert {"BMTUSDT", "HUMAUSDT", "AUSDT"} <= syms, f"since_bars={since} eşleşmedi: {syms}"


async def test_2_16_recent_bos_payload_indices_are_real(db):
    """Payload'daki son BOS indeksleri gerçekten ~192-195 aralığında (senaryo teyidi)."""
    await seed(db, "BMTUSDT", breakout_series(193))
    engine = PAEngine(db)
    ms = await engine.get_market_structure("BMTUSDT", TF)
    bos = [e["index"] for e in ms["structure"]["events"] if e["type"] == "bos_bullish"]
    assert bos[-1] == 193  # 200 mumluk payload penceresinde son BOS ~193


async def test_2_16_old_bos_does_not_match_small_since_bars(db):
    """Son BOS ~index 50'de → since_bars=24/50/100 eşleşmemeli, since_bars=150 eşleşmeli."""
    await seed(db, "OLDBOS", old_breakout_series())
    screener = Screener(db)
    for since in (24, 50, 100):
        res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": since}])
        assert "OLDBOS" not in {s["symbol"] for s in res["symbols"]}, f"since_bars={since} eşleşmemeliydi"
    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": 150}])
    assert "OLDBOS" in {s["symbol"] for s in res["symbols"]}


# ---------------------------------------------------------------------------
# S2 — liquidity_sweep_occurred: sweep artık makul since_bars ile eşleşiyor
# ---------------------------------------------------------------------------


async def test_2_16_recent_sweep_matches_reasonable_since_bars(db):
    """~index 195'te sweep edilen equal-high bölgesi → since_bars=24/50 eşleşir."""
    await seed(db, "SWEEPUSDT", sweep_series(195))
    screener = Screener(db)
    for since in (24, 50):
        res = await screener.scan([{"type": "liquidity_sweep_occurred", "since_bars": since}])
        assert "SWEEPUSDT" in {s["symbol"] for s in res["symbols"]}, f"since_bars={since} eşleşmedi"


async def test_2_16_zone_carries_swept_at_time(db):
    """Liquidity zone'ları sweep zamanını mutlak open_time olarak taşır (hizalama için)."""
    await seed(db, "SWEEPUSDT", sweep_series(195))
    engine = PAEngine(db)
    data = await engine.get_liquidity_zones("SWEEPUSDT", TF, include_mitigated=True)
    swept = [z for z in data["zones"] if z.get("swept_at")]
    assert swept
    z = swept[0]
    assert z["swept_at_time"] is not None
    assert z["swept_at_time"] > 0


# ---------------------------------------------------------------------------
# S3 — kök neden: screener context'i PA engine ile aynı pencereyi kullanıyor
# ---------------------------------------------------------------------------


async def test_2_16_screener_context_uses_pa_lookback(db):
    """`_build_context` PA_LOOKBACK mum okur (300 değil) — index çerçevesi hizalanır."""
    await seed(db, "BMTUSDT", breakout_series(193))
    screener = Screener(db)
    ctx = await screener._build_context("BMTUSDT", TF, needs_analysis=False, filter_types=[])
    assert ctx is not None
    assert len(ctx["candles"]) <= PA_LOOKBACK
    assert len(ctx["candles"]) == PA_LOOKBACK  # 200 kapanmış mum mevcut


# ---------------------------------------------------------------------------
# S4 — Sorun 2: data_stale yanında symbol_valid (delist filtreleme)
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
    """Aday listesini DB'den (delist edilmiş semboller dahil) dönen screener.

    Canlıda `_candidate_symbols` evren snapshot'ını kullandığında delist semboller
    zaten elenirdi; asıl risk DB fallback yolunda adayın evren kontrolünden
    geçmemesidir. Bu subclass adayları DB'den (COHRUSDT dahil) çekerek
    `_symbol_validity` filtresinin gerçekten devreye girdiğini doğrular.
    """

    async def _candidate_symbols(self) -> list[str]:
        def _q(conn):
            rows = conn.execute("SELECT DISTINCT symbol FROM candles WHERE source='spot'").fetchall()
            return sorted(r["symbol"] for r in rows)

        return await self.db.read(_q)


async def test_2_16_invalid_symbol_filtered_by_universe(db):
    """Delist edilmiş sembol artık yanıltıcı şekilde eşleşmez (data_stale=false iken bile)."""
    # COHRUSDT/USARUSDT/FLNCUSDT canlı örneği: mum verisi var ama evrende yok.
    for sym, target in TARGETS.items():
        await seed(db, sym, breakout_series(target))
    await seed(db, "COHRUSDT", breakout_series(193))  # evrende YOK

    screener = _StaleCandidateScreener(db, pipeline=_FakePipeline(list(TARGETS)))  # COHRUSDT evrende değil
    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}])
    syms = [s["symbol"] for s in res["symbols"]]
    assert "COHRUSDT" not in syms  # delist — tarama sonucunda yer almaz
    assert {"BMTUSDT", "HUMAUSDT", "AUSDT"} <= set(syms)
    for s in res["symbols"]:
        assert s["symbol_valid"] is True


async def test_2_16_symbol_valid_unknown_without_pipeline(db):
    """Pipeline yoksa symbol_valid None olur (evren doğrulanamaz) — davranış korunur."""
    await seed(db, "BMTUSDT", breakout_series(193))
    screener = Screener(db)  # pipeline yok
    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}])
    bmt = next(s for s in res["symbols"] if s["symbol"] == "BMTUSDT")
    assert bmt["symbol_valid"] is None


# ---------------------------------------------------------------------------
# S5 — Sorun 3: tarama satırları denetlenebilir (matched_filters + signal_summary)
# ---------------------------------------------------------------------------


async def test_2_16_scan_rows_are_auditable(db):
    """Eşleşmeyi tetikleyen filtreler ve ham sinyal özeti satır bazında döner."""
    await seed(db, "BMTUSDT", breakout_series(193))
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
    """OR kombinasyonda yalnız eşleşen filtreler matched_filters'a girer."""
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
    assert "price_change" not in bmt["matched_filters"]  # %500 değişim yok → eşleşmedi
