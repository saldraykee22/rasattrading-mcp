"""T2 FIX — PA katmanı server clock tutarlılığı + screener fail-closed + OI durumu.

Review bulguları test'e çevrilir (fix-t2-data-pa-clock ticket'ı):
- `pa/swings.filter_closed_candles`, `pa/analysis.freshness_for`, `pa/worker.latest_closed`
  yerel saat kullanıyordu; data katmanı (klines) server clock kullanıyor. Host saati
  kayarsa PA kararları kayıyordu → `PAEngine._now()` (pipeline clock, yoksa yerel saat)
  ortak kaynak yapıldı, kapanmış-mum/freshness kararları buna geçirildi.
- `klines._needs_catchup` timeframe genelinde tek MAX kontrolü yapıyordu → sembol bazlı.
- `futures.poll_open_interest` hata sonrası durumu "ok" ile eziyordu → gerçek durum korunur.
- Screener stale sembolleri sonuca karışık tazelikte sokuyordu (fail-open) → `require_fresh`.
- Screener soğuk evrende bütçesiz `engine.analyze` çağırıyordu → K3 bütçe deseni.

Kapsam: pa/swings, pa/analysis, pa/worker, pa/screener, data/klines, data/futures.
"""

import time

import pytest

from rasattrading_mcp.config import Config, TIMEFRAME_SECONDS
from rasattrading_mcp.data.futures import FuturesContextPoller
from rasattrading_mcp.data.klines import KlineService
from rasattrading_mcp.data.universe import UniverseService
from rasattrading_mcp.envelope import FRESHNESS_FRESH, FRESHNESS_STALE
from rasattrading_mcp.errors import ErrorCode, RasatError
from rasattrading_mcp.pa.analysis import PAEngine
from rasattrading_mcp.pa.screener import Screener
from rasattrading_mcp.pa.swings import filter_closed_candles
from rasattrading_mcp.pa.worker import PAWorker
from rasattrading_mcp.storage.db import Database
from rasattrading_mcp.storage.migrations import run_migrations
from tests.helpers import FakeClock, FakeRest

TF = "1h"
PERIOD = 3600

UPTREND = [
    (100, 100.5, 99.5, 100), (100, 100.5, 99.5, 100), (99, 100, 98, 99.5),
    (99.5, 100.5, 99, 100), (100, 102, 99.5, 101), (101, 101.5, 100.5, 101),
    (101, 101.5, 100.5, 100.5), (100.5, 103, 101, 102.5), (102, 102.5, 101.5, 102),
    (102, 102.5, 101.5, 102), (102.5, 103.5, 102, 103), (103, 103.5, 102.5, 103),
    (103, 105, 102.5, 104.5), (104, 104.5, 103.5, 104), (104, 104.5, 103.5, 103.5),
]


class FakePipeline:
    def __init__(self, clock):
        self.clock = clock


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


# ---------------------------------------------------------------------------
# PA katmanı server clock (kapanmış-mum + freshness)
# ---------------------------------------------------------------------------


def test_pa_filter_closed_candles_uses_server_clock():
    """Host saati server'dan 2 period ilerideyken kapanış kararı server clock'a göre verilir.

    Host'un "kapanmış" saydığı son bar, server'a göre hâlâ oluşuyordur — filter onu atar.
    """
    clock = FakeClock(offset=-2 * PERIOD)
    now = clock.server_now()
    server_latest_closed = int(now // PERIOD) * PERIOD - PERIOD
    host_latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    assert host_latest_closed > server_latest_closed  # senaryo teyidi

    candles = [
        {"open_time": server_latest_closed - PERIOD, "open": 1, "high": 2, "low": 0.5, "close": 1.5},
        {"open_time": server_latest_closed, "open": 1.5, "high": 2.5, "low": 1, "close": 2},
        {"open_time": host_latest_closed, "open": 2, "high": 3, "low": 1.5, "close": 2.5},
    ]
    kept = filter_closed_candles(candles, TF, now=now)
    assert [c["open_time"] for c in kept] == [server_latest_closed - PERIOD, server_latest_closed]
    # `now` verilmezse yerel saat — eski davranış korunur
    kept_local = filter_closed_candles(candles, TF)
    assert host_latest_closed in [c["open_time"] for c in kept_local]


def test_pa_freshness_uses_server_clock():
    """Freshness `now` parametresiyle server clock'a göre hesaplanır (T2).

    Host saatinden `PERIOD` geride olan server clock'ta, server'ın son kapanmış
    mumu (`server_latest_closed`) fresh'tir — ama yerel saatle (host) hesaplanırsa
    aynı bar `stale` görünürdü (yanlış stale riski). Fix, kararı server clock'a bağlar.
    """
    clock = FakeClock(offset=-PERIOD)
    server_now = clock.server_now()
    server_latest_closed = int(server_now // PERIOD) * PERIOD - PERIOD

    # Server clock ile server'ın son kapanmış mumu fresh
    assert PAEngine.freshness_for(TF, server_latest_closed, now=server_now) == FRESHNESS_FRESH
    # Yerel saatle (now verilmezse) aynı bar stale görünür → veri katmanıyla tutarsızlık
    assert PAEngine.freshness_for(TF, server_latest_closed) == FRESHNESS_STALE


async def test_pa_engine_now_prefers_pipeline_clock(db):
    """`PAEngine._now()` pipeline clock varken server saatini döner; yoksa yerel saati."""
    clock = FakeClock(offset=7 * 60)
    engine = PAEngine(db, pipeline=FakePipeline(clock))
    assert engine._now() == pytest.approx(clock.server_now(), abs=1)

    engine_local = PAEngine(db)  # pipeline yok → yerel saat
    assert engine_local._now() == pytest.approx(time.time(), abs=1)


async def test_pa_freshness_instance_uses_server_clock(db):
    """`PAEngine.freshness()` örnek metodu server clock'a bağlıdır (screener/worker kullanımı)."""
    clock = FakeClock(offset=-PERIOD)
    engine = PAEngine(db, pipeline=FakePipeline(clock))
    server_latest_closed = int(clock.server_now() // PERIOD) * PERIOD - PERIOD

    assert engine.freshness(TF, server_latest_closed) == FRESHNESS_FRESH
    # Yerel saatle aynı bar stale görünür — örnek metot server clock kullandığı için fresh
    assert PAEngine.freshness_for(TF, server_latest_closed) == FRESHNESS_STALE


def test_pa_worker_latest_closed_uses_server_clock():
    """`PAWorker.latest_closed` `now` ile server'ın son kapalı barını döndürür."""
    clock = FakeClock(offset=-PERIOD)
    server_latest_closed = int(clock.server_now() // PERIOD) * PERIOD - PERIOD
    host_latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD

    assert PAWorker.latest_closed(TF, now=clock.server_now()) == server_latest_closed
    assert PAWorker.latest_closed(TF) == host_latest_closed  # now yoksa yerel saat


async def test_worker_processes_when_server_clock_says_closed(db, cfg):
    """Host saati kaymış olsa da server clock'a göre kapalı olan bar worker tarafından işlenir.

    Eski davranış (yerel saat) host saatine göre "kapalı olmayan" barı stale sayıp
    turu atlıyordu; server clock ile data katmanıyla tutarlı işlenir.
    """
    clock = FakeClock(offset=-PERIOD)
    engine = PAEngine(db, pipeline=FakePipeline(clock))
    server_latest_closed = int(clock.server_now() // PERIOD) * PERIOD - PERIOD
    host_latest_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
    assert host_latest_closed == server_latest_closed + PERIOD  # senaryo teyidi

    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND), now=clock.server_now()))
    worker = PAWorker(engine, FakeUniverse(["BTCUSDT"]), cfg)

    assert await worker.check_and_process() == 1
    assert worker._last_processed.get(TF) == server_latest_closed


async def test_worker_skips_when_data_behind_server(db, cfg):
    """Server clock'a göre veri son kapanmış bara yetişmemişse (bir period geride)
    worker turu işlemez — fail-closed (2.12 davranışı server clock ile korunur)."""
    clock = FakeClock(offset=-PERIOD)
    engine = PAEngine(db, pipeline=FakePipeline(clock))
    server_latest_closed = int(clock.server_now() // PERIOD) * PERIOD - PERIOD

    # Son mum server'ın son kapanmış barından bir period geride
    await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND), now=clock.server_now() - PERIOD))
    worker = PAWorker(engine, FakeUniverse(["BTCUSDT"]), cfg)

    assert await worker.check_and_process() == 0
    assert worker._last_processed.get(TF) is None  # stale → retry beklenir


# ---------------------------------------------------------------------------
# Screener: require_fresh (fail-closed) + analiz bütçesi
# ---------------------------------------------------------------------------


async def _seed_old(db, symbol, rows):
    base = 1_700_000_000
    def _w(conn):
        for i, (o, h, l, c) in enumerate(rows):
            conn.execute(
                "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, source, updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (symbol, TF, base + i * PERIOD, o, h, l, c, 10.0, "spot", int(time.time())),
            )
    await db.write(_w)


async def test_screener_require_fresh_excludes_stale(db):
    """`require_fresh=True` (varsayılan): stale eşleşenler sonuç listesinden çıkar,
    `stale_symbols` alanında raporlanır (alarmlarla aynı fail-closed davranış)."""
    await _seed_old(db, "BTCUSDT", UPTREND)  # eski damgalı → stale
    await _seed_old(db, "SOLUSDT", [(105 - i * 0.5, 105.5 - i * 0.5, 104.5 - i * 0.5, 105 - i * 0.5) for i in range(15)])
    screener = Screener(db)

    res = await screener.scan([{"type": "price_change", "window_bars": 10, "min": 2}])
    assert res["symbols"] == []  # tüm eşleşenler stale → sonuç boş (fail-closed)
    assert res["freshness"] == FRESHNESS_FRESH  # sunulan sonuç yok → stale veri yok
    stale_syms = {s["symbol"] for s in res["stale_symbols"]}
    assert "BTCUSDT" in stale_syms  # eşleşip stale kaldığı için ayrı raporda
    assert "SOLUSDT" not in stale_syms  # filtreyle eşleşmedi → stale listesinde de yok


async def test_screener_require_fresh_false_keeps_legacy_behavior(db):
    """`require_fresh=False`: stale eşleşenler işaretlenerek sonuca girer (eski davranış)."""
    await _seed_old(db, "BTCUSDT", UPTREND)
    screener = Screener(db)

    res = await screener.scan([{"type": "price_change", "window_bars": 10, "min": 2}], require_fresh=False)
    btc = next(s for s in res["symbols"] if s["symbol"] == "BTCUSDT")
    assert btc["data_stale"] is True
    assert res["freshness"] == FRESHNESS_STALE
    assert res["stale_symbols"] == []  # require_fresh=False → stale semboller sonuçta işaretli


async def test_screener_analysis_budget_defers_cold_symbols(db):
    """K3 deseni: depolanmış analiz yokken on-demand `analyze` bütçeyle sınırlanır;
    bütçeyi aşan semboller ertelenir (`deferred_analysis`), scan dakikalarca sürmez."""
    for sym in ("BTCUSDT", "ETHUSDT"):
        await seed_at(db, sym, UPTREND, _fresh_times(len(UPTREND)))
    screener = Screener(db, compute_budget=1)

    res = await screener.scan([{"type": "structure_event", "event": "bos_bullish"}])
    assert res["deferred_analysis"] == 1  # 2 sembolden 1'i analiz edildi, 1'i ertelendi
    assert res["total_matched"] == 1  # yalnızca analiz edilen sembol değerlendirilebildi


async def test_screener_budget_resets_each_scan(db):
    """Her scan turu bütçeyi sıfırlar — ertelenen sembol bir sonraki turda işlenebilir."""
    for sym in ("BTCUSDT", "ETHUSDT"):
        await seed_at(db, sym, UPTREND, _fresh_times(len(UPTREND)))
    screener = Screener(db, compute_budget=1)

    first = await screener.scan([{"type": "structure_event", "event": "bos_bullish"}])
    assert first["deferred_analysis"] == 1  # BTCUSDT analiz edildi, ETHUSDT ertelendi
    assert first["total_matched"] == 1

    # İkinci turda bütçe sıfırlandı; BTCUSDT artık depolanmış → ETHUSDT de analiz edilir
    second = await screener.scan([{"type": "structure_event", "event": "bos_bullish"}])
    assert second["deferred_analysis"] == 0
    assert second["total_matched"] == 2


# ---------------------------------------------------------------------------
# klines: sembol bazlı catchup
# ---------------------------------------------------------------------------


async def _klines_svc(cfg, db, fake):
    universe = UniverseService(fake, cfg)
    await universe.sync()
    service = KlineService(fake, fake, db, universe, cfg, clock=FakeClock())
    await service.start()
    return universe, service


async def _max_open_for(db, symbol):
    def _q(conn):
        row = conn.execute(
            "SELECT MAX(open_time) AS m FROM candles WHERE symbol=? AND timeframe=?",
            (symbol, TF),
        ).fetchone()
        return int(row["m"]) if row["m"] is not None else None

    return await db.read(_q)


async def test_needs_catchup_symbol_level(cfg, db):
    """Bir sembol güncelken diğer sembol gerideyse catchup atlanmaz (T2)."""
    fake = FakeRest(["BTCUSDT", "ETHUSDT"])
    _, svc = await _klines_svc(cfg, db, fake)
    try:
        last_closed = int(time.time() // PERIOD) * PERIOD - PERIOD

        # BTCUSDT güncel (son kapalı bar), ETHUSDT bir period geride
        await seed_at(db, "BTCUSDT", UPTREND, _fresh_times(len(UPTREND)))
        await seed_at(db, "ETHUSDT", UPTREND, _fresh_times(len(UPTREND), now=time.time() - PERIOD))

        assert await svc._needs_catchup(TF, last_closed, "spot") is True

        # Her ikisi de güncel → catchup gerekmez
        await seed_at(db, "ETHUSDT", UPTREND, _fresh_times(len(UPTREND)))
        assert await svc._needs_catchup(TF, last_closed, "spot") is False
    finally:
        await svc.stop()


async def test_needs_catchup_no_symbols_false(cfg, db):
    """Evrende sembol yoksa catchup gerekmez (tek MAX kontrolündeki None davranışı korunur)."""
    fake = FakeRest(["BTCUSDT"])
    _, svc = await _klines_svc(cfg, db, fake)
    try:
        # BTCUSDT evrende ama hiç veri yok → catchup gerekir
        last_closed = int(time.time() // PERIOD) * PERIOD - PERIOD
        assert await svc._needs_catchup(TF, last_closed, "spot") is True
    finally:
        await svc.stop()


# ---------------------------------------------------------------------------
# futures: OI poll durumu yalnızca temiz turda "ok" olur
# ---------------------------------------------------------------------------


async def _poller(cfg, db, fake):
    universe = UniverseService(fake, cfg)
    await universe.sync()
    return FuturesContextPoller(fake, db, universe, cfg)


async def test_oi_poll_ok_only_on_clean_round(cfg, db):
    """Temiz tur → durum `ok`."""
    fake = FakeRest(["BTCUSDT"])
    poller = await _poller(cfg, db, fake)
    await poller.poll_open_interest()
    assert poller.status()["open_interest"] == "ok"


async def test_oi_poll_rate_limited_status_preserved(cfg, db):
    """RATE_LIMITED'te durum `rate_limited` kalır; döngü sonunda `ok` ile ezilmez."""
    class _OiRateRest(FakeRest):
        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/openInterest":
                raise RasatError(ErrorCode.RATE_LIMITED, "bütçe dolu")
            return await super().get(path, params, weight)

    fake = _OiRateRest(["BTCUSDT"])
    poller = await _poller(cfg, db, fake)
    n = await poller.poll_open_interest()
    assert n == 0
    assert poller.status()["open_interest"] == "rate_limited"


async def test_oi_poll_error_status_preserved(cfg, db):
    """Genel hata (timeout vb.) durumu `error` yapar; `ok` yazılmaz."""
    class _OiFailRest(FakeRest):
        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/openInterest":
                raise RasatError(ErrorCode.INTERNAL_ERROR, "ağ hatası")
            return await super().get(path, params, weight)

    fake = _OiFailRest(["BTCUSDT"])
    poller = await _poller(cfg, db, fake)
    await poller.poll_open_interest()
    assert poller.status()["open_interest"] == "error"


async def test_oi_poll_invalid_symbol_400_still_ok(cfg, db):
    """400 (sembol fapi'de yok) bir sembolü kümeden düşürür ama döngü temiz tamamlanır → `ok`."""
    class _Oi400Rest(FakeRest):
        def __init__(self, symbols):
            super().__init__(symbols)
            self.fail_oi_for: set[str] = set()

        async def get(self, path, params=None, weight=1):
            if path == "/fapi/v1/openInterest" and (params or {}).get("symbol") in self.fail_oi_for:
                raise RasatError(ErrorCode.INVALID_REQUEST, "Binance 400")
            return await super().get(path, params, weight)

    fake = _Oi400Rest(["BTCUSDT", "ETHUSDT"])
    fake.fail_oi_for = {"ETHUSDT"}
    poller = await _poller(cfg, db, fake)
    n = await poller.poll_open_interest()
    assert n == 1
    assert poller.status()["open_interest"] == "ok"
