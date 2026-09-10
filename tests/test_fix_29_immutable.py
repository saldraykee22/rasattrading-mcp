"""2.9 FIX — immutable PA storage: no overwrite + no reversed intervals.

Review evidence is converted into tests:
- A second write with the same effective_from must not delete/overwrite the first.
- Backward as-of (calculating 200 while 300 is open) must not create a reversed interval.
"""

import json

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.analysis import _read_history, _store_payload
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TABLE = "market_structure"
SYM = "BTCUSDT"
TF = "1h"
V = "swing-v1"


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


async def test_same_bar_same_version_no_overwrite(db):
    """Repeating the same bar/version/payload → no-op; preserve existing record (no overwrite)."""
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 1})
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 1})
    rows = await _read_history(db, TABLE, SYM, TF)
    assert len(rows) == 1
    assert json.loads(rows[0]["payload"]) == {"a": 1}
    assert rows[0]["effective_to"] is None


async def test_same_bar_different_payload_preserves_history(db):
    """Recalculation for the same bar with late futures data does not destroy the previous payload."""
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 1})
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 2})
    rows = await _read_history(db, TABLE, SYM, TF)
    assert len(rows) == 2  # Revision preserved; both are queryable.
    values = sorted(json.loads(r["payload"])["a"] for r in rows)
    assert values == [1, 2]
    open_rows = [r for r in rows if r["effective_to"] is None]
    assert len(open_rows) == 1
    assert json.loads(open_rows[0]["payload"])["a"] == 2


async def test_forward_interval_sequential(db):
    await _store_payload(db, TABLE, SYM, TF, V, 100, {"a": 1})
    await _store_payload(db, TABLE, SYM, TF, V, 200, {"a": 2})
    rows = await _read_history(db, TABLE, SYM, TF)
    assert len(rows) == 2
    assert rows[0]["effective_from"] == 100
    assert rows[0]["effective_to"] == 199
    assert rows[1]["effective_from"] == 200
    assert rows[1]["effective_to"] is None


async def test_backward_insert_no_inverted_interval(db):
    """Review evidence: calculating 200 while 300 is open must not create reversed interval (300..199)."""
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 3})
    await _store_payload(db, TABLE, SYM, TF, V, 200, {"a": 2})  # Backward (backfill / time correction).
    rows = await _read_history(db, TABLE, SYM, TF)
    assert len(rows) == 2
    first, second = rows
    assert first["effective_from"] == 200
    assert first["effective_to"] == 299  # Logical interval, not reversed.
    assert second["effective_from"] == 300
    assert second["effective_to"] is None  # Open record was not closed.


async def test_backward_then_forward_chain(db):
    """A forward insert after a backward insert also produces consistent, non-reversed intervals."""
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 3})
    await _store_payload(db, TABLE, SYM, TF, V, 200, {"a": 2})
    await _store_payload(db, TABLE, SYM, TF, V, 400, {"a": 4})
    rows = await _read_history(db, TABLE, SYM, TF)
    intervals = [(r["effective_from"], r["effective_to"]) for r in rows]
    for ef, et in intervals:
        assert et is None or et >= ef  # No interval can be reversed.
    assert intervals[-1] == (400, None)
