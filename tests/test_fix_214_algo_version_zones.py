"""2.14 FIX — get_full_analysis bileşik algo_version + zones/skor tutarlılığı.

Gerçek piyasa araştırmasında tespit edilen iki tutarsızlık test'e çevrilir:
- S1: get_full_analysis tek bileşenli `meta.algo_version` (sadece swing-v1) yerine
      tüm bileşen sürümlerini taşır (bileşik string + `versions` haritası + her
      alt-bölümde kendi algo_version'ı).
- S2: likidite skorunun equal_levels açıklamasındaki `zones` toplamı, varsayılan
      (include_mitigated=false) zones listesiyle aynı varsayımı kullanır: aktif
      (mitigasyonsuz) sayı liste uzunluğuyla birebir örtüşür, not açıkça toplamı
      ve kırılımı belgeler.
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.daemon.handlers import build_dispatcher
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.liquidity import liquidity_score
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1h"
PERIOD = 3600

UPTREND = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]

# Tek equal_high bölgesi + sweep → mitigasyonlu
EQ_SWEEP = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 101, 99.5, 100.5), (100.5, 100.5, 100, 100.5),
    (100.5, 100.5, 100, 100.5), (100, 101.04, 99.5, 100.5), (100.5, 100.5, 100, 100.5),
    (100.5, 100.5, 100, 100.5), (100.5, 102, 100.5, 101.5),
]

FULL_VERSIONS = {
    "structure": "swing-v1",
    "liquidity": "liquidity-v1",
    "order_blocks": "obfvg-v1",
    "vwap": "vwap-v1",
    "sessions": "session-v1",
}
COMPOSITE = ",".join(FULL_VERSIONS.values())


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


async def seed(db, symbol, rows):
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    times = [latest_closed - (len(rows) - 1 - i) * PERIOD for i in range(len(rows))]

    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


# ---------------------------------------------------------------------------
# S1 — get_full_analysis bileşik algo_version
# ---------------------------------------------------------------------------


async def test_2_14_full_analysis_versions_consistent(db):
    await seed(db, "BTCUSDT", UPTREND)
    engine = PAEngine(db)
    data = await engine.get_full_analysis("BTCUSDT", TF)

    assert data["versions"] == FULL_VERSIONS
    assert data["algo_version"] == COMPOSITE
    # Her alt-bölüm kendi algo_version'ını taşır
    assert data["structure"]["algo_version"] == "swing-v1"
    assert data["liquidity"]["algo_version"] == "liquidity-v1"
    assert data["order_blocks"]["algo_version"] == "obfvg-v1"
    assert data["vwap"]["algo_version"] == "vwap-v1"
    assert data["sessions"]["algo_version"] == "session-v1"


async def test_2_14_full_analysis_meta_composite(db, cfg):
    await seed(db, "BTCUSDT", UPTREND)
    ctx = {"db": db, "config": cfg, "readiness": None, "pipeline": None, "started_at": time.time()}
    dispatcher = build_dispatcher(ctx)

    data, meta = await dispatcher.dispatch("get_full_analysis", {"symbol": "BTCUSDT", "timeframe": TF}, ctx)
    assert meta.algo_version == COMPOSITE
    assert data["algo_version"] == meta.algo_version
    assert set(data["versions"]) == set(FULL_VERSIONS)


# ---------------------------------------------------------------------------
# S2 — zones listesi ile skor açıklaması tutarlılığı
# ---------------------------------------------------------------------------


async def test_2_14_zones_list_matches_score_active(db):
    """Varsayılan (include_mitigated=false): liste uzunluğu equal_levels.active_zones ile aynı."""
    await seed(db, "BTCUSDT", EQ_SWEEP)
    engine = PAEngine(db)
    data = await engine.get_liquidity_zones("BTCUSDT", TF)

    eq = data["score"]["components"]["equal_levels"]
    assert eq["zones"] == 1  # toplam
    assert eq["mitigated_zones"] == 1
    assert eq["active_zones"] == 0
    assert len(data["zones"]) == eq["active_zones"]  # liste varsayılanla birebir
    assert "1 eşit-seviye bölge" in eq["note"]
    assert "0 aktif" in eq["note"]


async def test_2_14_include_mitigated_list_has_documented_relationship(db):
    """include_mitigated=true: liste tarihçeden gelir, skor notu farkı açıklar."""
    await seed(db, "BTCUSDT", EQ_SWEEP)
    engine = PAEngine(db)
    full = await engine.get_liquidity_zones("BTCUSDT", TF, include_mitigated=True)
    eq = full["score"]["components"]["equal_levels"]

    assert len(full["zones"]) == 1
    assert full["zones"][0]["mitigated"] is True
    # Skor toplamı analizdeki bölgeleri sayar; liste tarihçe merge eder.
    assert eq["zones"] == 1
    assert eq["active_zones"] == 0
    assert "varsayılan listede yalnızca aktifler görünür" in eq["note"]


def test_2_14_score_equal_levels_mitigation_breakdown():
    """Karma bölge setinde aktif/mitigasyonlu kırılımı doğru hesaplanır.

    2.15 fix: puan aktif bölge sayısına göre hesaplanır — mitigasyonlu bölgeler
    "kullanılmış likidite" olarak puan getirmez (önceden tüm bölgeler sayılırdı).
    """
    zones = [
        {"kind": "equal_highs", "mitigated": False},
        {"kind": "equal_highs", "mitigated": True},
        {"kind": "equal_lows", "mitigated": True},
        {"kind": "equal_highs", "mitigated": False},
        {"kind": "order_block"},  # eşit-seviye sayılmaz
    ]
    sc = liquidity_score(zones, None)
    eq = sc["components"]["equal_levels"]
    assert eq["zones"] == 4
    assert eq["active_zones"] == 2
    assert eq["mitigated_zones"] == 2
    assert eq["points"] == 8.0  # 2 aktif / 10 * 40
    assert "2 aktif" in eq["note"]
    assert "puan aktif bölge sayısına göre" in eq["note"]


async def test_2_14_full_analysis_score_and_zones_together(db):
    """get_full_analysis: skor kırılımı ve zones listesi aynı analizden gelir."""
    await seed(db, "BTCUSDT", EQ_SWEEP)
    engine = PAEngine(db)
    data = await engine.get_full_analysis("BTCUSDT", TF)

    eq = data["liquidity"]["score"]["components"]["equal_levels"]
    assert len(data["liquidity"]["zones"]) == eq["active_zones"]
    assert data["liquidity"]["score"]["algo_version"] == "liquidity-v1"
