"""2.7 FIX — timestamp birimi (ms → s tek standardı).

Review kanıtları test'e çevrilir:
- Gerçek Binance ms kline'ları saniyeye inmeli ve `filter_closed_candles`
  tarafından yanlışlıkla elenmemeli.
- Bilerek eski/stale mum → `freshness` `stale` dönmeli (önceden ms ile fresh).
- Soğuk (saniye) mum verisi warm sanılmamalı → yeniden çekim tetiklenmeli.
- Retention saniye sözleşmesiyle doğru budamalı.
- Session/utc_iso saniye değerle Windows'ta OSError fırlatmamalı.
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.data.klines import KlineService, parse_klines
from rasattrading_mcp.envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.swings import filter_closed_candles
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from rasattrading_mcp.storage.retention import prune_candles

TF = "1h"
PERIOD = 3600


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


def _ms_kline(open_ms, close="100.0"):
    return [open_ms, "99", "101", "98", close, "10", open_ms + 3600_000, "1000", 5, "0", "0", "0"]


async def test_real_ms_klines_become_seconds_and_survive_closed_filter():
    """Gerçek Binance ms kline'ları saniyeye iner; kapalı mum filtresi onları elenez."""
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    raw = [_ms_kline((latest_closed - 2 * PERIOD) * 1000), _ms_kline((latest_closed - PERIOD) * 1000)]
    rows = parse_klines(raw)
    assert rows[0]["open_time"] == latest_closed - 2 * PERIOD
    kept = filter_closed_candles(rows, TF)
    assert len(kept) == 2  # geçmişteki gerçek kapalı mumlar yanlışlıkla elenmez


async def test_filter_closed_candles_rejects_forming_bar():
    """Şu an oluşmakta olan (kapanmamış) bar elenir."""
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    raw = [_ms_kline((latest_closed + PERIOD) * 1000)]
    rows = parse_klines(raw)
    assert filter_closed_candles(rows, TF) == []


async def test_pa_freshness_stale_for_old_data(db):
    """Bilerek eski bir as_of → stale (önceden ms ile yanlışlıkla fresh)."""
    old = int(time.time()) - 10 * PERIOD
    assert PAEngine.freshness_for(TF, old) == FRESHNESS_STALE


async def test_pa_freshness_fresh_for_latest_closed(db):
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    assert PAEngine.freshness_for(TF, latest_closed) == FRESHNESS_FRESH


async def test_klineservice_freshness_uses_seconds(db, cfg):
    from tests.helpers import FakeClock, FakeRest
    from rasattrading_mcp.data.universe import UniverseService

    fake = FakeRest(["BTCUSDT"])
    uni = UniverseService(fake, cfg)
    svc = KlineService(fake, fake, db, uni, cfg, clock=FakeClock())

    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    fresh_rows = [{"open_time": latest_closed - 5 * PERIOD}, {"open_time": latest_closed}]
    stale_rows = [{"open_time": latest_closed - 5 * PERIOD}]
    assert svc.freshness_for("BTCUSDT", TF, fresh_rows) == FRESHNESS_FRESH
    assert svc.freshness_for("BTCUSDT", TF, stale_rows) == FRESHNESS_STALE


async def test_get_candles_cold_seconds_trigger_refetch(cfg, db):
    """Saniye cinsinden eski mum 'warm' sanılmamalı → öncelikli yeniden çekim tetiklenmeli."""
    from tests.helpers import FakeClock, FakeRest
    from rasattrading_mcp.data.universe import UniverseService

    fake = FakeRest(["BTCUSDT"])
    uni = UniverseService(fake, cfg)
    await uni.sync()
    svc = KlineService(fake, fake, db, uni, cfg, clock=FakeClock())
    await svc.start()
    try:

        def _w(conn):
            old = int(time.time()) - 30 * 86400
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("BTCUSDT", "15m", old, 1, 1, 1, 1, 1, "spot", old),
            )

        await db.write(_w)
        rows = await svc.get_candles("BTCUSDT", "15m", 100)
        assert len(rows) == 100  # cold → FakeRest'ten taze veri çekildi
        latest = rows[-1]["open_time"]
        assert latest >= int(time.time()) - 3600
    finally:
        await svc.stop()


async def test_retention_prunes_old_seconds_data(db):
    """Retention saniye sözleşmesiyle doğru budar (ms ile asla budamıyordu)."""
    now = int(time.time())

    def _w(conn):
        conn.executemany(
            "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                ("BTCUSDT", "1d", now - 300 * 86400, 1, 1, 1, 1, 1, "spot", now),
                ("BTCUSDT", "15m", now - 100 * 86400, 1, 1, 1, 1, 1, "spot", now),
            ],
        )

    await db.write(_w)
    removed = await prune_candles(db, {"15m": 90, "1d": 730})
    assert removed.get("15m") == 1
    assert "1d" not in removed


async def test_session_levels_seconds_no_oserror():
    from rasattrading_mcp.pa.vwap_sessions import compute_session_levels

    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    candles = [
        {"open_time": latest_closed - i * PERIOD, "high": 102, "low": 99}
        for i in range(6)
    ]
    res = compute_session_levels(candles)
    assert res["timezone"] == "UTC"


async def test_utc_iso_seconds_no_oserror():
    from rasattrading_mcp.envelope import utc_iso

    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    s = utc_iso(latest_closed)
    assert s.startswith("20")
