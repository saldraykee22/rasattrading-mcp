import asyncio

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.storage.audit import AuditLog
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import current_version, run_migrations
from rasattrading_mcp.storage.retention import prune_candles


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False)


@pytest.fixture
async def db(cfg):
    d = Database(cfg.db_path)
    await d.start()
    yield d
    await d.stop()


def _tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {r["name"] for r in rows}


async def test_migrations_on_empty_db(cfg, db):
    applied = await run_migrations(db)
    assert applied == [1, 2, 3]
    assert await current_version(db) == 3
    tables = await db.read(_tables)
    expected = {
        "candles", "futures_context", "market_structure", "liquidity_zones",
        "order_blocks", "annotations", "alerts", "triggered_alerts",
        "accounts", "audit_log", "schema_migrations",
    }
    assert expected.issubset(tables)

    def _cols(conn):
        return {r["name"] for r in conn.execute("PRAGMA table_info(alerts)").fetchall()}

    cols = await db.read(_cols)
    assert "cooldown_until" in cols


async def test_migrations_idempotent_on_filled_db(cfg, db):
    await run_migrations(db)

    def _insert(conn):
        conn.execute(
            "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
            "VALUES ('BTCUSDT','15m',1000,1,2,0.5,1.5,10,'spot',123)"
        )

    await db.write(_insert)
    applied_again = await run_migrations(db)
    assert applied_again == []  # ikinci çalıştırma no-op

    def _count(conn):
        return conn.execute("SELECT COUNT(*) AS c FROM candles").fetchone()["c"]

    assert await db.read(_count) == 1  # veri korundu


async def test_write_queue_returns_value(cfg, db):
    await run_migrations(db)

    def _fn(conn):
        conn.execute("INSERT INTO annotations (symbol, timeframe, data, created_at) VALUES ('X','1h','{}',1)")
        return "done"

    assert await db.write(_fn) == "done"


async def test_concurrent_writers_no_lock_errors(cfg, db):
    """WS+REST+PA+alarm+execution simülasyonu: eşzamanlı yazıcılar hata üretmemeli."""
    await run_migrations(db)

    def _mk_writer(n, i):
        def _w(conn):
            conn.execute(
                "INSERT INTO annotations (symbol, timeframe, data, created_at) VALUES (?,?,'{}',?)",
                (f"S{n}", "1h", i),
            )

        return _w

    async def writer(n):
        for i in range(20):
            await db.write(_mk_writer(n, i))

    results = await asyncio.gather(*[writer(n) for n in range(10)], return_exceptions=True)
    assert all(r is None for r in results), results

    def _count(conn):
        return conn.execute("SELECT COUNT(*) AS c FROM annotations").fetchone()["c"]

    assert await db.read(_count) == 200


async def test_reader_parallel_with_writer(cfg, db):
    await run_migrations(db)
    stop = asyncio.Event()
    counter = [0]

    def _w(conn):
        i = counter[0]
        counter[0] += 1
        conn.execute("INSERT INTO annotations (symbol, timeframe, data, created_at) VALUES ('W','1h','{}',?)", (i,))

    def _r(conn):
        return conn.execute("SELECT COUNT(*) AS c FROM annotations").fetchone()["c"]

    async def spam_writer():
        while not stop.is_set():
            await db.write(_w)

    async def reader_loop():
        for _ in range(20):
            await db.read(_r)
        return True

    w = asyncio.create_task(spam_writer())
    await asyncio.sleep(0.05)
    assert await reader_loop() is True
    stop.set()
    await w


async def test_audit_append_and_verify(cfg, db):
    await run_migrations(db)
    audit = AuditLog(db)
    await audit.append("agent-a", "place_order", {"symbol": "BTCUSDT", "api_key": "SECRET123"})
    await audit.append("agent-b", "cancel_order", {"reason": "manual"})
    await audit.append("system", "startup", {})

    broken = await audit.verify()
    assert broken == []

    tail = await audit.tail()
    assert len(tail) == 3
    # Sır redact edildi (tail seq DESC olduğu için aktöre göre bul)
    place_order = next(t for t in tail if t["actor"] == "agent-a")
    assert "SECRET123" not in place_order["details"]
    assert "REDACTED" in place_order["details"]


async def test_audit_detects_tamper(cfg, db):
    await run_migrations(db)
    audit = AuditLog(db)
    await audit.append("a", "act1", {"x": 1})
    await audit.append("b", "act2", {"x": 2})

    def _tamper(conn):
        conn.execute("UPDATE audit_log SET details='{tampered}' WHERE seq=2")

    await db.write(_tamper)
    broken = await audit.verify()
    assert any(b["seq"] == 2 and "hash" in b["reason"] for b in broken)


async def test_audit_detects_deletion(cfg, db):
    await run_migrations(db)
    audit = AuditLog(db)
    await audit.append("a", "act1", {})
    await audit.append("b", "act2", {})
    await audit.append("c", "act3", {})

    def _delete(conn):
        conn.execute("DELETE FROM audit_log WHERE seq=2")

    await db.write(_delete)
    broken = await audit.verify()
    assert any(b["reason"].startswith("seq") for b in broken)


async def test_immutable_record_pattern(cfg, db):
    """market_structure gibi immutable kayıtlar üzerine yazılmaz, pencerelerle kapatılır."""
    await run_migrations(db)

    def _insert(conn):
        conn.execute(
            "INSERT INTO market_structure (symbol, timeframe, algo_version, effective_from, effective_to, payload, created_at) "
            "VALUES ('BTCUSDT','1h','v1',100,150,'{}',1)"
        )
        conn.execute(
            "INSERT INTO market_structure (symbol, timeframe, algo_version, effective_from, effective_to, payload, created_at) "
            "VALUES ('BTCUSDT','1h','v2',151,NULL,'{}',2)"
        )

    await db.write(_insert)

    def _read(conn):
        rows = conn.execute(
            "SELECT algo_version, effective_from, effective_to FROM market_structure "
            "WHERE symbol='BTCUSDT' AND timeframe='1h' ORDER BY effective_from"
        ).fetchall()
        return [dict(r) for r in rows]

    rows = await db.read(_read)
    assert rows == [
        {"algo_version": "v1", "effective_from": 100, "effective_to": 150},
        {"algo_version": "v2", "effective_from": 151, "effective_to": None},
    ]


async def test_retention_prunes_old_candles(cfg, db):
    await run_migrations(db)

    def _insert(conn):
        now = 1_700_000_000
        # A (15m) çok eski -> budanır; B (1d) yeni -> kalır
        conn.execute(
            "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
            "VALUES ('A','15m',?,1,2,0,3,1,'spot',?)",
            (now - 1_000_000, now),
        )
        conn.execute(
            "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
            "VALUES ('B','1d',?,1,2,0,3,1,'spot',?)",
            (now - 40_000, now),
        )

    await db.write(_insert)
    removed = await prune_candles(db, {"15m": 7, "1d": 1}, now=1_700_000_000)
    assert removed == {"15m": 1}
