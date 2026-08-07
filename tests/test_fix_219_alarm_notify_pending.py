"""2.19 FIX — Alarm → ajan bildirimi (notify_command) + onay bekleyen emir (order_spec).

Kullanıcı akışı: alarm tetiklenince (a) daemon harici bir komut çalıştırır
(örn. `traycer agent send` ile ajanı uyandırır), (b) alarm `order_spec`
taşıyorsa `pending_orders`'a `awaiting_approval` kaydı düşer. Emir OTOMATİK
AÇILMAZ: `approve_pending_order` → handler emri açar → `executed`.
"""

import asyncio
import json
import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.alarms import (
    PENDING_AWAITING,
    PENDING_REJECTED,
    AlarmService,
)
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


def _fresh_times(n):
    latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    return [latest_closed - (n - 1 - i) * PERIOD for i in range(n)]


async def seed(db, symbol, rows):
    times = _fresh_times(len(rows))

    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


def _make_service(db, notify_command=None):
    engine = PAEngine(db)
    alarms = AlarmService(db, engine=engine, notify_command=notify_command)
    engine.alarm_service = alarms
    return engine, alarms


async def test_alert_with_order_spec_creates_pending_on_trigger(db):
    """order_spec'li alarm tetiklenince awaiting_approval kaydı düşer (emir açılmaz)."""
    await seed(db, "BTCUSDT", UPTREND)
    engine, alarms = _make_service(db)

    order_spec = {
        "account_id": "acc-1",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "order_type": "market",
        "risk_pct": 0.02,
    }
    created = await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0, note="test", order_spec=order_spec,
    )
    assert created["state"] == "armed"

    await engine.analyze("BTCUSDT", TF)
    pending = await alarms.get_pending_orders()
    assert pending["count"] == 1
    rec = pending["pending"][0]
    assert rec["alert_id"] == created["alert_id"]
    assert rec["account_id"] == "acc-1"
    assert rec["symbol"] == "BTCUSDT"
    assert rec["side"] == "BUY"
    assert rec["risk_pct"] == 0.02
    assert rec["status"] == PENDING_AWAITING


async def test_alert_without_order_spec_creates_no_pending(db):
    """order_spec yoksa tetiklenme pending kaydı üretmez (eski davranış korunur)."""
    await seed(db, "BTCUSDT", UPTREND)
    engine, alarms = _make_service(db)

    await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0,
    )
    await engine.analyze("BTCUSDT", TF)
    pending = await alarms.get_pending_orders()
    assert pending["count"] == 0


async def test_order_spec_validation(db):
    """order_spec allowlist: bilinmeyen anahtar / eksik zorunlu reddedilir."""
    engine, alarms = _make_service(db)
    from rasattrading_mcp.errors import ErrorCode, RasatError

    with pytest.raises(RasatError) as e1:
        await alarms.create_alert(
            "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish"}],
            order_spec={"account_id": "a", "symbol": "BTCUSDT", "side": "BUY", "sneaky": 1},
        )
    assert e1.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as e2:
        await alarms.create_alert(
            "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish"}],
            order_spec={"symbol": "BTCUSDT", "side": "BUY"},  # account_id yok
        )
    assert e2.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as e3:
        await alarms.create_alert(
            "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish"}],
            order_spec={"account_id": "a", "symbol": "BTCUSDT", "side": "BUY", "risk_pct": 2.0},
        )
    assert e3.value.code == ErrorCode.INVALID_REQUEST


async def test_notify_command_executed_on_trigger(db, tmp_path):
    """notify_command tetiklenmede çalıştırılır (fire-and-forget)."""
    await seed(db, "BTCUSDT", UPTREND)
    marker = tmp_path / "notify.txt"
    # Windows'ta cmd echo ile dosyaya yaz; yer tutucular dolu gelmeli.
    notify = f'cmd /c echo {{symbol}} {{alert_id}} > "{marker}"'
    engine, alarms = _make_service(db, notify_command=notify)

    created = await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0, note="n",
    )
    await engine.analyze("BTCUSDT", TF)

    for _ in range(200):
        if marker.exists():
            break
        await asyncio.sleep(0.05)
    assert marker.exists(), "notify komutu çalışmadı"
    content = marker.read_text(encoding="utf-8", errors="replace").strip()
    assert content.startswith("BTCUSDT")
    assert created["alert_id"] in content


async def test_approve_reject_pending_lifecycle(db):
    """onay/red state machine'i: awaiting → approved|rejected; retry güvenli."""
    await seed(db, "BTCUSDT", UPTREND)
    engine, alarms = _make_service(db)

    await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0,
        order_spec={"account_id": "acc-1", "symbol": "BTCUSDT", "side": "BUY", "risk_pct": 0.01},
    )
    await engine.analyze("BTCUSDT", TF)
    rec = (await alarms.get_pending_orders())["pending"][0]
    oid = rec["order_id"]

    # onayla
    await alarms.approve_pending_order(oid)
    assert (await alarms.get_pending_orders(status="approved"))["count"] == 1

    # aynı emri tekrar onaylamak reddedilmeli (state zaten approved)
    from rasattrading_mcp.errors import ErrorCode, RasatError

    with pytest.raises(RasatError) as e:
        await alarms.approve_pending_order(oid)
    assert e.value.code == ErrorCode.INVALID_REQUEST

    # ikinci bir emir üret (cooldown 0 → yeni bar tetikleyebilir)
    await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0,
        order_spec={"account_id": "acc-2", "symbol": "BTCUSDT", "side": "BUY", "risk_pct": 0.01},
    )
    await engine.analyze("BTCUSDT", TF)
    rec2 = (await alarms.get_pending_orders(status=PENDING_AWAITING))["pending"][-1]
    await alarms.reject_pending_order(rec2["order_id"], reason="kullanıcı vazgeçti")
    assert rec2["order_id"] not in [p["order_id"] for p in (await alarms.get_pending_orders(status=PENDING_AWAITING))["pending"]]
    assert (await alarms.get_pending_orders(status=PENDING_REJECTED))["count"] == 1
