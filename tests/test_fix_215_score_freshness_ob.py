"""2.15 FIX — Skor/Tazelik/OB tasarım zayıflıkları regresyon testleri.

Bağımsız eleştiri ajanının (INJ/SEI/ARB canlı analizi) bulduğu 5 zayıflık test'e
çevrilir:

- S1 (tazelik): `freshness_for` son kapanmış mumdan bir period eski snapshot'ı
  artık `stale` sayar (önceden `- period` toleransıyla `fresh` diyordu); PA meta
  `freshness_note` taşır.
- S2 (mitigasyon ağırlığı): `equal_levels` puanı aktif bölge sayısına göre —
  10 bölgenin 7'si mitigasyonluysa tam puan verilmez.
- S3 (funding yön): funding bileşeni `bias: long_crowded|short_crowded` taşır.
- S4 (breaker): kapanışla kırılan OB `zone_type=breaker` + `mitigated=true`.
- S5 (OB dedup): aynı/çok yakın fiyat aralığını kapsayan OB'ler tek mantıksal
  bölgede birleştirilir (birden çok BOS/CHoCH aynı mumu seçiyordu).
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.liquidity import liquidity_score
from rasattrading_mcp.pa.obfvg import compute_order_blocks
from rasattrading_mcp.pa.swings import detect_structure
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1h"
PERIOD = 3600

# İki BOS event'i (7 ve 12) aynı mumu (index 6) OB adayı seçer → aynı aralık.
DUP_EVENTS = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]

# OB sonrası fiyat bölgeyi kapanışla aşar → breaker.
BREAKER = DUP_EVENTS[:8] + [
    (102, 102.5, 101.5, 102),
    (101.5, 102, 100.4, 100.3),  # close 100.3 < OB.low 100.5 → breaker
    (101, 101.5, 100.5, 101),
]


def mk(rows):
    return [
        {"open_time": i * 100, "open": o, "high": h, "low": l, "close": c, "volume": 10.0}
        for i, (o, h, l, c) in enumerate(rows)
    ]


# ---------------------------------------------------------------------------
# S1 — tazelik
# ---------------------------------------------------------------------------


def test_s1_freshness_strict_no_period_tolerance():
    """Son kapanmış mumdan bir period eski snapshot artık fresh DEĞİL (2.15)."""
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    # En güncel: analiz son kapanmış mumu içerir → fresh.
    assert PAEngine.freshness_for(TF, latest_closed) == FRESHNESS_FRESH
    # Bir period geri: son kapanmış mum eksik → artık stale (önceden toleransla fresh).
    assert PAEngine.freshness_for(TF, latest_closed - PERIOD) == FRESHNESS_STALE
    # İki period geri: hâlâ stale.
    assert PAEngine.freshness_for(TF, latest_closed - 2 * PERIOD) == FRESHNESS_STALE
    assert PAEngine.freshness_for(TF, None) == FRESHNESS_STALE


async def test_s1_pa_meta_carries_freshness_note(db, cfg):
    """PA tool meta'sı freshness anlamını açıklayan not taşır."""
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    times = [latest_closed - (len(DUP_EVENTS) - 1 - i) * PERIOD for i in range(len(DUP_EVENTS))]

    def _w(conn):
        for i, (o, h, l, c) in enumerate(DUP_EVENTS):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("BTCUSDT", TF, times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)
    ctx = {"db": db, "config": cfg, "readiness": None, "pipeline": None, "started_at": time.time()}
    dispatcher = build_dispatcher(ctx)
    data, meta = await dispatcher.dispatch("get_full_analysis", {"symbol": "BTCUSDT", "timeframe": TF}, ctx)
    assert meta.freshness == FRESHNESS_FRESH  # taze mum verisi
    assert "freshness_note" in meta.to_dict()
    assert "kapanmış mum" in meta.to_dict()["freshness_note"]


async def test_s1_klineservice_freshness_strict(db, cfg):
    """KlineService.freshness_for de fazladan bir period toleransı içermez."""
    from tests.helpers import FakeRest
    from rasattrading_mcp.data.klines import KlineService
    from rasattrading_mcp.data.universe import UniverseService

    fake = FakeRest(["BTCUSDT"])
    uni = UniverseService(fake, cfg)
    svc = KlineService(fake, db, uni, cfg)
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    fresh_rows = [{"open_time": latest_closed - 5 * PERIOD}, {"open_time": latest_closed}]
    stale_rows = [{"open_time": latest_closed - 5 * PERIOD}, {"open_time": latest_closed - PERIOD}]
    assert svc.freshness_for("BTCUSDT", TF, fresh_rows) == FRESHNESS_FRESH
    assert svc.freshness_for("BTCUSDT", TF, stale_rows) == FRESHNESS_STALE


# ---------------------------------------------------------------------------
# S2 — mitigasyon ağırlığı
# ---------------------------------------------------------------------------


def test_s2_equal_levels_weighted_by_active_zones():
    """10 bölgenin 7'si mitigasyonlu → tam puan (40) verilmez, aktif sayıya göre."""
    zones = [{"kind": "equal_highs", "mitigated": i < 7} for i in range(10)]
    sc = liquidity_score(zones, None)
    eq = sc["components"]["equal_levels"]
    assert eq["zones"] == 10
    assert eq["active_zones"] == 3
    assert eq["mitigated_zones"] == 7
    assert eq["points"] == 12.0  # 3/10 * 40
    assert "puan aktif bölge sayısına göre" in eq["note"]


def test_s2_all_mitigated_scores_zero():
    sc = liquidity_score([{"kind": "equal_highs", "mitigated": True}], None)
    eq = sc["components"]["equal_levels"]
    assert eq["active_zones"] == 0
    assert eq["points"] == 0.0


# ---------------------------------------------------------------------------
# S3 — funding yön
# ---------------------------------------------------------------------------


def test_s3_funding_bias_long_crowded():
    futures = {"funding_rate": {"freshness": "fresh", "value": 0.0005}}
    sc = liquidity_score([], futures)
    comp = sc["components"]["funding_rate"]
    assert comp["bias"] == "long_crowded"
    assert sc["funding_bias"] == "long_crowded"


def test_s3_funding_bias_short_crowded():
    futures = {"funding_rate": {"freshness": "fresh", "value": -0.0005}}
    sc = liquidity_score([], futures)
    assert sc["components"]["funding_rate"]["bias"] == "short_crowded"
    assert sc["funding_bias"] == "short_crowded"


def test_s3_funding_stale_no_bias():
    futures = {"funding_rate": {"freshness": "stale", "value": 0.0005}}
    sc = liquidity_score([], futures)
    assert "bias" not in sc["components"]["funding_rate"]
    assert "funding_bias" not in sc


# ---------------------------------------------------------------------------
# S4 — breaker → mitigated
# ---------------------------------------------------------------------------


def test_s4_breaker_is_mitigated():
    st = detect_structure(mk(BREAKER))
    res = compute_order_blocks(mk(BREAKER), st)
    ob = next(o for o in res["order_blocks"] if o["event_index"] == 7)
    assert ob["zone_type"] == "breaker"
    assert ob["mitigated"] is True  # kapanışla kırılmış → geçerli aktif bölge değil
    # Varsayılan (aktif) görünümde breaker görünmemeli — include_mitigated ile görünür.
    active = [o for o in res["order_blocks"] if not o["mitigated"]]
    assert all(o["zone_type"] != "breaker" for o in active)


# ---------------------------------------------------------------------------
# S5 — OB dedup
# ---------------------------------------------------------------------------


def test_s5_duplicate_events_same_candle_collapse():
    """İki BOS event'i aynı mumu seçse de tek mantıksal OB üretilir."""
    st = detect_structure(mk(DUP_EVENTS))
    assert [e["type"] for e in st["events"]] == ["bos_bullish", "bos_bullish"]
    res = compute_order_blocks(mk(DUP_EVENTS), st)
    obs = res["order_blocks"]
    assert len(obs) == 1  # önceden 2 ayrı kayıt üretiliyordu
    assert obs[0]["range"] == {"low": 100.5, "high": 101.5}
    assert obs[0]["event_index"] == 7  # ilk (en erken) kayıt korunur


def test_s5_distinct_ranges_not_merged():
    """Farklı fiyat aralığındaki OB'ler birleştirilmez."""
    rows = DUP_EVENTS[:8] + [
        (102, 102.5, 101.6, 102),     # fiyat ilk OB'ye geri dönmez → aktif kalır
        (102, 102.5, 101.6, 102),
        (102.5, 103.5, 102, 103),
        (103.5, 103.0, 101.8, 102.6),  # kırmızı mum → yeni OB adayı (farklı aralık)
        (103, 105, 102.4, 104.5),      # bos_bullish@12 → bu mumu OB seçer
        (104, 104.5, 103.5, 104),
    ]
    st = detect_structure(mk(rows))
    res = compute_order_blocks(mk(rows), st)
    obs = res["order_blocks"]
    # İki farklı fiyat aralığı → dedup birleştirmez.
    assert len(obs) == 2
    ranges = {o["range"]["low"] for o in obs}
    assert 100.5 in ranges and 101.8 in ranges


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
