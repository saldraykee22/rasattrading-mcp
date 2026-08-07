"""2.18 FIX — PA worker kalıcı yetersiz veriyi tur blokajından ayırır.

Canlı gözlem: tokenized hisse senedi çiftleri (SMCIBUSDT, ALABBUSDT, ...)
1d'de yalnızca ~2 kapanmış mum taşıyor; `analyze` `STALE_DATA` fırlatınca
worker her döngüde 489 sembolün tamamını yeniden işliyordu (1d turları
~25 sn'de bir tekrarlıyordu) çünkü `_last_processed` hiç ilerleyemiyordu.

Fix: `STALE_DATA` (yetersiz kapanmış mum) `"insufficient"` olarak sayılır —
turu bloklamaz, yeni kapalı bar geldiğinde yeniden denenir. Geçici durumlar
(stale veri, hata, boş) 2.12 davranışını korur: tur bloklanır, retry edilir.
"""

import time

import pytest

from rasattrading_mcp.config import Config
from rasattrading_mcp.pa.analysis import PAEngine, _read_history
from rasattrading_mcp.pa.worker import PAWorker
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations

TF = "1d"
PERIOD = 86400
OLD_BASE = 1_700_000_000

UPTREND = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]


class FakeUniverse:
    def __init__(self, symbols):
        self._symbols = symbols

    def snapshot(self):
        return list(self._symbols)


@pytest.fixture
def cfg(tmp_path):
    return Config(data_dir=tmp_path, pipeline_enabled=False, kline_intervals=(TF,), pa_worker_concurrency=2)


@pytest.fixture
async def db(cfg):
    d = Database(cfg.db_path)
    await d.start()
    await run_migrations(d)
    yield d
    await d.stop()


def _fresh_times(n, now=None):
    now = now if now is not None else time.time()
    latest_closed = int(now // PERIOD) * PERIOD - PERIOD
    return [latest_closed - (n - 1 - i) * PERIOD for i in range(n)]


async def seed_at(db, symbol, rows, open_times):
    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(symbol, timeframe, open_time, source) DO UPDATE SET "
                "open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, "
                "volume=excluded.volume, updated_at=excluded.updated_at",
                (symbol, TF, open_times[i], o, h, l, c, 10.0, "spot", int(time.time())),
            )

    await db.write(_w)


async def test_insufficient_symbol_does_not_block_round(db, cfg):
    """Sağlıklı + yetersiz veri karışımında tur tamamlanır; yetersiz sembol atlanır."""
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
    await seed_at(db, "SMCIBUSDT", UPTREND[:2], _fresh_times(2))  # 2 bar < 2*SWING_LOOKBACK+1
    worker = PAWorker(PAEngine(db), FakeUniverse(["BTCUSDT", "SMCIBUSDT"]), cfg)

    assert await worker.check_and_process() == 1  # yalnızca BTCUSDT başarılı sayılır
    assert worker._last_processed.get(TF) is not None  # tur YİNE DE tamamlandı
    assert len(await _read_history(db, "market_structure", "BTCUSDT", TF)) == 1
    assert await _read_history(db, "market_structure", "SMCIBUSDT", TF) == []


async def test_all_symbols_insufficient_still_advances(db, cfg):
    """Yalnızca yetersiz veri olan evren turu bloklamaz (tekrarlanan 489-sembol turları biter)."""
    await seed_at(db, "SMCIBUSDT", UPTREND[:2], _fresh_times(2))
    worker = PAWorker(PAEngine(db), FakeUniverse(["SMCIBUSDT"]), cfg)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is not None

    # Aynı kapalı bar için tur tekrarlanmaz (marker ilerledi)
    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is not None


async def test_insufficient_symbol_retried_when_new_bar_arrives(db, cfg):
    """Yetersiz sembol yeni kapalı bar geldiğinde yeniden denenir (kalıcı atlama değil)."""
    await seed_at(db, "SMCIBUSDT", UPTREND[:2], _fresh_times(2))
    worker = PAWorker(PAEngine(db), FakeUniverse(["SMCIBUSDT"]), cfg)
    assert await worker.check_and_process() == 0
    first_marker = worker._last_processed.get(TF)
    assert first_marker is not None

    # 3. kapanmış bar eklendi (hâlâ < 5, yine insufficient) → yeni bar olduğu için tur başlar
    times = _fresh_times(3)
    await seed_at(db, "SMCIBUSDT", UPTREND[:3], times)
    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) == times[2]  # yeni marker


async def test_stale_insufficient_still_blocks_round(db, cfg):
    """2.12 korunur: yetersiz veri STALE ise (hedef bara yetişmemişse) tur bloklanır."""
    await seed_at(db, "SMCIBUSDT", UPTREND[:2], [OLD_BASE + i * PERIOD for i in range(2)])
    worker = PAWorker(PAEngine(db), FakeUniverse(["SMCIBUSDT"]), cfg)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is None  # stale → retry beklenir
