"""2.19 FIX — Alarm → ajan bildirimi (notify_command) + onay bekleyen emir (order_spec).

Kullanıcı akışı: alarm tetiklenince (a) daemon harici bir komut çalıştırır
(örn. `traycer agent send` ile ajanı uyandırır), (b) alarm `order_spec`
taşıyorsa `pending_orders`'a `awaiting_approval` kaydı düşer. Emir OTOMATİK
AÇILMAZ: `approve_pending_order` → handler emri açar → `executed`.
"""

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


async def test_notify_uses_safe_argv_shell_false(db, monkeypatch):
    """T02: notify güvenli argv + shell=False; placeholder'lar tek argv elemanı."""
    await seed(db, "BTCUSDT", UPTREND)
    calls = []

    def _fake_popen(argv, **kwargs):
        calls.append({"argv": list(argv), "kwargs": kwargs})
        return None

    monkeypatch.setattr("rasattrading_mcp.pa.alarms.subprocess.Popen", _fake_popen)
    engine, alarms = _make_service(
        db,
        notify_command="traycer agent send --message {note} --symbol {symbol} --tf {timeframe} --id {alert_id}",
    )
    created = await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0, note="; whoami",
    )
    await engine.analyze("BTCUSDT", TF)

    assert len(calls) == 1
    argv = calls[0]["argv"]
    kwargs = calls[0]["kwargs"]
    assert kwargs["shell"] is False
    assert argv[0:3] == ["traycer", "agent", "send"]
    assert argv[argv.index("--message") + 1] == "; whoami"
    assert argv[argv.index("--symbol") + 1] == "BTCUSDT"
    assert argv[argv.index("--tf") + 1] == TF
    assert argv[argv.index("--id") + 1] == created["alert_id"]


async def test_notify_note_shell_metachars_stays_single_arg(db, monkeypatch):
    """T02: note içindeki shell metacharacter'ları process komutu olarak yorumlanmaz."""
    await seed(db, "BTCUSDT", UPTREND)
    calls = []

    def _fake_popen(argv, **kwargs):
        calls.append(list(argv))
        return None

    monkeypatch.setattr("rasattrading_mcp.pa.alarms.subprocess.Popen", _fake_popen)
    malicious = "$(calc.exe); rm -rf /; echo pwned > /tmp/x && ls | grep x `id`"
    engine, alarms = _make_service(db, notify_command="notifier --message {note}")
    await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0, note=malicious,
    )
    await engine.analyze("BTCUSDT", TF)

    assert len(calls) == 1
    assert calls[0] == ["notifier", "--message", malicious]


async def test_notify_parse_error_fails_closed(db, monkeypatch):
    """T02: şablon parse hatasında notify fail-closed — process başlamaz, alarm state bozulmaz."""
    await seed(db, "BTCUSDT", UPTREND)
    called = []

    def _fake_popen(*args, **kwargs):
        called.append(args)
        return None

    monkeypatch.setattr("rasattrading_mcp.pa.alarms.subprocess.Popen", _fake_popen)
    engine, alarms = _make_service(db, notify_command="notifier --message '{note}")  # dengesiz tırnak
    await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0,
    )
    await engine.analyze("BTCUSDT", TF)

    assert called == []
    triggered = await alarms.get_triggered_alerts()
    assert len(triggered["triggered"]) == 1  # tetiklenme kaydı bozulmadı


async def test_notify_does_not_log_command_or_note(db, monkeypatch, caplog):
    """T02: komutun tamamı loglanmaz (note/hassas içerik sızmaz)."""
    import logging

    await seed(db, "BTCUSDT", UPTREND)

    def _fake_popen(*args, **kwargs):
        return None

    monkeypatch.setattr("rasattrading_mcp.pa.alarms.subprocess.Popen", _fake_popen)
    secret_note = "gizli-icerik-S3CRET"
    engine, alarms = _make_service(db, notify_command="notifier --message {note}")
    with caplog.at_level(logging.INFO, logger="rasattrading.pa.alarms"):
        await alarms.create_alert(
            "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
            cooldown_seconds=0, note=secret_note,
        )
        await engine.analyze("BTCUSDT", TF)
    assert secret_note not in caplog.text


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
