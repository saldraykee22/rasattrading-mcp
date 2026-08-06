"""2.6 — Alarm motoru: state machine, dedup, kalıcılık, stale kuralı.

2.8 notu: `on_analysis_updated` artık freshness kapısı taşır — stale analizle
tetiklenmez. Bu yüzden tetikleme testleri taze (kapalı bara hizalı) mumlarla
seed edilir; "eski mum → tetiklenmez" senaryosu ayrı testtir (review kanıtı).
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.analysis import PAEngine, _read_history
from rasattrading_mcp.pa.alarms import AlarmService
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1h"
PERIOD = 3600
OLD_BASE = 1_700_000_000

UPTREND = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]

FALLING = [(105 - i * 0.5, 105.5 - i * 0.5, 104.5 - i * 0.5, 105 - i * 0.5) for i in range(15)]


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


def _open_times(n, fresh=False, offset=0):
    if fresh:
        latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
        return [latest_closed - (n - 1 - i) * PERIOD for i in range(n)]
    return [OLD_BASE + (offset + i) * PERIOD for i in range(n)]


async def seed(db, symbol, rows, fresh=False, offset=0):
    times = _open_times(len(rows), fresh=fresh, offset=offset)
    await seed_at(db, symbol, rows, times)


async def seed_at(db, symbol, rows, open_times):
    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, open_times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


def engine_with_alarms(db):
    engine = PAEngine(db)
    alarms = AlarmService(db, engine=engine)
    engine.alarm_service = alarms
    return engine, alarms


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


async def test_alert_crud(db):
    alarms = AlarmService(db, engine=PAEngine(db))
    a = await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=300)
    assert a["alert_id"]
    assert a["state"] == "armed"

    comp = await alarms.create_composite_alert(
        [{"symbol": "BTCUSDT", "timeframe": TF, "filters": [{"type": "above_below_vwap", "position": "above"}]}],
        combine="AND",
    )
    assert comp["type"] == "composite"

    listed = await alarms.list_alerts()
    assert len(listed) == 2

    removed = await alarms.delete_alert(a["alert_id"])
    assert removed == 1
    assert len(await alarms.list_alerts()) == 1

    with pytest.raises(Exception):
        await alarms.delete_alert("nonexistent")


async def test_create_alert_validation(db):
    alarms = AlarmService(db, engine=PAEngine(db))
    with pytest.raises(Exception):
        await alarms.create_alert("", TF, [{"type": "price_change", "min": 1}])
    with pytest.raises(Exception):
        await alarms.create_alert("BTCUSDT", TF, [{"type": "DROP TABLE x"}])
    with pytest.raises(Exception):
        await alarms.create_alert("BTCUSDT", TF, [{"type": "price_change"}], cooldown_seconds=-1)
    with pytest.raises(Exception):
        await alarms.create_composite_alert([])


# ---------------------------------------------------------------------------
# Tetikleme + dedup + cooldown
# ---------------------------------------------------------------------------


async def test_trigger_on_analysis_update(db):
    await seed(db, "BTCUSDT", UPTREND, fresh=True)
    engine, alarms = engine_with_alarms(db)
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=300)

    await engine.analyze("BTCUSDT", TF)
    rec = await alarms.get_triggered_alerts()
    assert rec["next_cursor"] is None
    assert len(rec["triggered"]) == 1
    assert rec["triggered"][0]["alert_id"] == (await alarms.list_alerts())[0]["alert_id"]
    state = (await alarms.list_alerts())[0]["state"]
    assert state == "triggered"


async def test_stale_data_does_not_trigger(db):
    """Review kanıtı: stale analiz snapshot'ı event-driven yoldan alarm tetiklememeli."""
    await seed(db, "BTCUSDT", UPTREND)  # OLD_BASE → stale
    engine, alarms = engine_with_alarms(db)
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)
    await engine.analyze("BTCUSDT", TF)
    assert (await alarms.get_triggered_alerts())["triggered"] == []


async def test_dedup_same_bar(db):
    await seed(db, "BTCUSDT", UPTREND, fresh=True)
    engine, alarms = engine_with_alarms(db)
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)

    await engine.analyze("BTCUSDT", TF)
    await engine.analyze("BTCUSDT", TF)  # aynı son bar → aynı trigger_key → dedup
    rec = await alarms.get_triggered_alerts()
    assert len(rec["triggered"]) == 1


def _advance_clock(monkeypatch, now: int):
    """`time.time`'ı global taklit eder — tüm modüller (analysis/swings/alarms) aynı stdlib
    modülüne referans verdiği için tek patch yeterli."""
    import time as _time

    monkeypatch.setattr(_time, "time", lambda: now)


async def test_cooldown_blocks_new_bar(db, monkeypatch):
    """Cooldown süresi dolmadıkça yeni kapalı bar alarmı yeniden tetiklemez."""
    now0 = int(time.time())
    await seed_at(db, "BTCUSDT", UPTREND, _open_times(len(UPTREND), fresh=True))
    engine, alarms = engine_with_alarms(db)
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=2 * PERIOD)
    await engine.analyze("BTCUSDT", TF)
    assert len((await alarms.get_triggered_alerts())["triggered"]) == 1

    # Zamanı bir period ileri al + yeni kapalı bar ekle → koşul hâlâ doğru, cooldown sürüyor
    now1 = now0 + PERIOD
    _advance_clock(monkeypatch, now1)
    await seed_at(db, "BTCUSDT", [(104.5, 106, 104, 105.5)], [int(now1 // PERIOD) * PERIOD - PERIOD])
    await engine.analyze("BTCUSDT", TF)
    assert len((await alarms.get_triggered_alerts())["triggered"]) == 1


async def test_new_bar_triggers_after_cooldown_zero(db, monkeypatch):
    """Cooldown yoksa (0) yeni kapalı bar → yeni trigger_key → yeniden tetiklenir."""
    now0 = int(time.time())
    await seed_at(db, "BTCUSDT", UPTREND, _open_times(len(UPTREND), fresh=True))
    engine, alarms = engine_with_alarms(db)
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)
    await engine.analyze("BTCUSDT", TF)
    assert len((await alarms.get_triggered_alerts())["triggered"]) == 1

    now1 = now0 + PERIOD
    _advance_clock(monkeypatch, now1)
    await seed_at(db, "BTCUSDT", [(104.5, 106, 104, 105.5)], [int(now1 // PERIOD) * PERIOD - PERIOD])
    await engine.analyze("BTCUSDT", TF)
    assert len((await alarms.get_triggered_alerts())["triggered"]) == 2  # yeni bar = yeni trigger_key


async def test_persistence_across_service_restart(db):
    """Agent bağlı değilken tetiklenen kayıt, yeni servis (yeni session) ile görülür."""
    await seed(db, "BTCUSDT", UPTREND, fresh=True)
    engine, alarms = engine_with_alarms(db)
    alert = await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)
    await engine.analyze("BTCUSDT", TF)

    fresh_alarms = AlarmService(db, engine=PAEngine(db))  # yeni oturum
    rec = await fresh_alarms.get_triggered_alerts(alert_id=alert["alert_id"])
    assert len(rec["triggered"]) == 1


# ---------------------------------------------------------------------------
# Kompozit + stale kuralı
# ---------------------------------------------------------------------------


async def test_composite_and_triggers(db):
    await seed(db, "BTCUSDT", UPTREND, fresh=True)
    await seed(db, "SOLUSDT", FALLING, fresh=True)
    engine, alarms = engine_with_alarms(db)
    await alarms.create_composite_alert(
        [
            {"symbol": "BTCUSDT", "timeframe": TF, "filters": [{"type": "above_below_vwap", "position": "above"}]},
            {"symbol": "SOLUSDT", "timeframe": TF, "filters": [{"type": "above_below_vwap", "position": "below"}]},
        ],
        combine="AND",
        cooldown_seconds=300,
    )
    await engine.analyze("BTCUSDT", TF)
    await engine.analyze("SOLUSDT", TF)
    rec = await alarms.get_triggered_alerts()
    comp = [t for t in rec["triggered"] if t["payload"].get("type") == "composite"]
    assert len(comp) == 1


async def test_composite_stale_clause_defers(db):
    """Clause'lardan biri stale → kompozit tetiklenmez (stale kuralı)."""
    await seed(db, "BTCUSDT", UPTREND, fresh=True)
    await seed(db, "SOLUSDT", FALLING, fresh=False)  # stale veri
    engine, alarms = engine_with_alarms(db)
    await alarms.create_composite_alert(
        [
            {"symbol": "BTCUSDT", "timeframe": TF, "filters": [{"type": "above_below_vwap", "position": "above"}]},
            {"symbol": "SOLUSDT", "timeframe": TF, "filters": [{"type": "above_below_vwap", "position": "below"}]},
        ],
        combine="AND",
    )
    await engine.analyze("BTCUSDT", TF)
    rec = await alarms.get_triggered_alerts()
    assert rec["triggered"] == []


async def test_evaluate_symbol_fresh_only(db):
    await seed(db, "BTCUSDT", UPTREND, fresh=True)
    engine, alarms = engine_with_alarms(db)
    alert = await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)
    await engine.analyze("BTCUSDT", TF)
    await alarms.delete_alert(alert["alert_id"])  # değerlendirme için yeniden kur
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)
    triggered = await alarms.evaluate_symbol("BTCUSDT", TF)
    assert len(triggered) == 1


async def test_evaluate_symbol_computes_missing_analysis(db):
    """PA kaydı yokken alarm döngüsü boş dönmez — evaluate_symbol analizi hesaplatır."""
    await seed(db, "BTCUSDT", UPTREND, fresh=True)
    engine, alarms = engine_with_alarms(db)
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)
    # analyze hiç çağrılmadı → market_structure kaydı yok
    assert await _read_history(db, "market_structure", "BTCUSDT", TF) == []
    await alarms.evaluate_symbol("BTCUSDT", TF)
    # Analiz hesaplandı ve alarm tetiklendi (on_analysis_updated event-driven yolu da
    # aynı bar için tek trigger üretir — dedup). Kayıt kalıcıdır.
    assert len(await _read_history(db, "market_structure", "BTCUSDT", TF)) == 1
    rec = await alarms.get_triggered_alerts()
    assert len(rec["triggered"]) == 1


async def test_get_triggered_alerts_pagination(db):
    await seed(db, "BTCUSDT", UPTREND, fresh=True)
    engine, alarms = engine_with_alarms(db)
    await alarms.create_alert("BTCUSDT", TF, [{"type": "above_below_vwap", "position": "above"}], cooldown_seconds=0)
    await engine.analyze("BTCUSDT", TF)
    rec = await alarms.get_triggered_alerts(limit=1)
    assert len(rec["triggered"]) == 1
    assert rec["next_cursor"] is None
