"""Alert → agent notification (notify_command) + order awaiting approval (order_spec).

User flow: when an alert fires, (a) the daemon runs an external command
(for example, an agent notification via `agent notify`), and (b) when the alert
carries `order_spec`, it creates an `awaiting_approval` record in
`pending_orders`. The order is NOT opened automatically:
`approve_pending_order` → handler opens the order → `executed`.
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
    """When an alert with order_spec fires, create awaiting_approval (order is not opened)."""
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
    """Without order_spec, firing does not create a pending record (preserve old behavior)."""
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
    """order_spec allowlist: reject unknown keys / missing required fields."""
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
        order_spec={"symbol": "BTCUSDT", "side": "BUY"},  # account_id missing.
        )
    assert e2.value.code == ErrorCode.INVALID_REQUEST

    with pytest.raises(RasatError) as e3:
        await alarms.create_alert(
            "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish"}],
            order_spec={"account_id": "a", "symbol": "BTCUSDT", "side": "BUY", "risk_pct": 2.0},
        )
    assert e3.value.code == ErrorCode.INVALID_REQUEST


async def test_notify_uses_safe_argv_shell_false(db, monkeypatch):
    """T02: notify uses safe argv + shell=False; placeholders remain one argv element."""
    await seed(db, "BTCUSDT", UPTREND)
    calls = []

    def _fake_popen(argv, **kwargs):
        calls.append({"argv": list(argv), "kwargs": kwargs})
        return None

    monkeypatch.setattr("rasattrading_mcp.pa.alarms.subprocess.Popen", _fake_popen)
    engine, alarms = _make_service(
        db,
        notify_command="agent notify --message {note} --symbol {symbol} --tf {timeframe} --id {alert_id}",
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
    assert argv[0:2] == ["agent", "notify"]
    assert argv[argv.index("--message") + 1] == "; whoami"
    assert argv[argv.index("--symbol") + 1] == "BTCUSDT"
    assert argv[argv.index("--tf") + 1] == TF
    assert argv[argv.index("--id") + 1] == created["alert_id"]


async def test_notify_note_shell_metachars_stays_single_arg(db, monkeypatch):
    """T02: shell metacharacters in note are not interpreted as process commands."""
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
    """T02: on template parse error, notify fails closed—the process does not start and alert state is unchanged."""
    await seed(db, "BTCUSDT", UPTREND)
    called = []

    def _fake_popen(*args, **kwargs):
        called.append(args)
        return None

    monkeypatch.setattr("rasattrading_mcp.pa.alarms.subprocess.Popen", _fake_popen)
    engine, alarms = _make_service(db, notify_command="notifier --message '{note}")  # Unbalanced quote.
    await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0,
    )
    await engine.analyze("BTCUSDT", TF)

    assert called == []
    triggered = await alarms.get_triggered_alerts()
    assert len(triggered["triggered"]) == 1  # Trigger record remains intact.


async def test_notify_does_not_log_command_or_note(db, monkeypatch, caplog):
    """T02: the full command is not logged (note/sensitive content does not leak)."""
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
    """Approval/rejection state machine: awaiting → approved|rejected; retry-safe."""
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

    # Approve the order.
    await alarms.approve_pending_order(oid)
    assert (await alarms.get_pending_orders(status="approved"))["count"] == 1

    # Re-approving the same order must be rejected (state is already approved).
    from rasattrading_mcp.errors import ErrorCode, RasatError

    with pytest.raises(RasatError) as e:
        await alarms.approve_pending_order(oid)
    assert e.value.code == ErrorCode.INVALID_REQUEST

    # Create a second order (cooldown 0 → a new bar can trigger it).
    await alarms.create_alert(
        "BTCUSDT", TF, [{"type": "structure_event", "event": "bos_bullish", "since_bars": 24}],
        cooldown_seconds=0,
        order_spec={"account_id": "acc-2", "symbol": "BTCUSDT", "side": "BUY", "risk_pct": 0.01},
    )
    await engine.analyze("BTCUSDT", TF)
    rec2 = (await alarms.get_pending_orders(status=PENDING_AWAITING))["pending"][-1]
    await alarms.reject_pending_order(rec2["order_id"], reason="user canceled")
    assert rec2["order_id"] not in [p["order_id"] for p in (await alarms.get_pending_orders(status=PENDING_AWAITING))["pending"]]
    assert (await alarms.get_pending_orders(status=PENDING_REJECTED))["count"] == 1
