"""2.9 FIX — immutable PA storage: üzerine yazma yok + ters aralık yok.

Review kanıtları test'e çevrilir:
- Aynı effective_from ile ikinci yazma ilkini silmemeli/ezmemeli.
- Backward as-of (açık 300 varken 200 hesaplanması) ters aralık üretmemeli.
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
    """Aynı bar/sürüm/payload tekrarı → no-op; mevcut kayıt korunur (üzerine yazılmaz)."""
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 1})
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 1})
    rows = await _read_history(db, TABLE, SYM, TF)
    assert len(rows) == 1
    assert json.loads(rows[0]["payload"]) == {"a": 1}
    assert rows[0]["effective_to"] is None


async def test_same_bar_different_payload_preserves_history(db):
    """Aynı bar için geç futures verisiyle yeniden hesaplama önceki payload'ı yok etmez."""
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 1})
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 2})
    rows = await _read_history(db, TABLE, SYM, TF)
    assert len(rows) == 2  # revision korunur, ikisi de sorgulanabilir
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
    """Review kanıtı: açık 300 varken 200 hesaplanınca ters aralık (300..199) üretilmemeli."""
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 3})
    await _store_payload(db, TABLE, SYM, TF, V, 200, {"a": 2})  # geriye (backfill / zaman düzeltmesi)
    rows = await _read_history(db, TABLE, SYM, TF)
    assert len(rows) == 2
    first, second = rows
    assert first["effective_from"] == 200
    assert first["effective_to"] == 299  # mantıklı aralık, ters değil
    assert second["effective_from"] == 300
    assert second["effective_to"] is None  # açık kayıt kapatılmadı


async def test_backward_then_forward_chain(db):
    """Geriye ekleme sonrası ileri ekleme de tutarlı, tersi olmayan aralıklar üretir."""
    await _store_payload(db, TABLE, SYM, TF, V, 300, {"a": 3})
    await _store_payload(db, TABLE, SYM, TF, V, 200, {"a": 2})
    await _store_payload(db, TABLE, SYM, TF, V, 400, {"a": 4})
    rows = await _read_history(db, TABLE, SYM, TF)
    intervals = [(r["effective_from"], r["effective_to"]) for r in rows]
    for ef, et in intervals:
        assert et is None or et >= ef  # hiçbir aralık ters olamaz
    assert intervals[-1] == (400, None)
