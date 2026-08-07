"""2.8 — Arka plan PA hesaplama worker'ı.

Plan Bölüm 2.3: sabit timeframe seti (15m/1h/4h/1d) sürekli güncel tutulur.
Bu worker, bir timeframe'in kapalı barı yenilendiğinde evrendeki semboller için
PA zincirini otomatik yeniden hesaplar — agent'ın `get_market_structure`
çağrısını beklemeden. Sembolün mum verisi hedef kapalı bara yetişmemişse
(stale / kline scheduler tamamlamadıysa) yeniden hesaplama atlanır; veri
tazelenince bir sonraki turda işlenir. Concurrency semaphore ile sınırlanır.

2.18: **Kalıcı yetersiz veri** (tokenized hisse senedi gibi yeni listelenmiş
sembollerde `PA_LOOKBACK` kadarlık kapanmış mum bulunamaması) turu bloklamaz:
o semboller `insufficient` sayılır, `_last_processed` ilerler ve yeni kapalı
bar geldiğinde yeniden denenir. Aksi halde tek bloklayıcı sembol (örn. 1d'de
2 mumluk SMCIBUSDT) her döngüde 489 sembolün tamamını yeniden işletiyordu.
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config, TIMEFRAME_SECONDS
from ..envelope import FRESHNESS_FRESH
from ..errors import ErrorCode, RasatError
from .analysis import PAEngine, PA_LOOKBACK

logger = logging.getLogger("rasattrading.pa.worker")


class PAWorker:
    def __init__(self, engine: PAEngine, universe, config: Config) -> None:
        self.engine = engine
        self.universe = universe
        self.config = config
        self._last_processed: dict[str, int] = {}
        self._sem = asyncio.Semaphore(config.pa_worker_concurrency)

    @staticmethod
    def latest_closed(tf: str, now: float | None = None) -> int:
        """Timeframe'in son kapanmış mumunun `open_time`'ı (server clock ile, T2).

        `now` verilmezse yerel saate düşülür; `PAWorker` turları server clock ile
        tutarlı kalması için `engine._now()` geçirir.
        """
        period = TIMEFRAME_SECONDS[tf]
        if now is None:
            now = time.time()
        return int(now // period) * period - period

    async def run(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.config.pa_check_seconds)
                await self.check_and_process()
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("PA worker döngü hatası")

    async def check_and_process(self) -> int:
        """Bir geçiş: yeni kapalı bar olan timeframe'ler için PA'yı yeniden hesaplar.

        İşlenen (başarılı) sembol sayısını döndürür (gözlem/diagnostik). İlk turda her
        timeframe işlenir (soğuk evren warm-up'ı) — `_last_processed` boştur.
        Marker yalnızca timeframe turu TAMAMEN başarılı olduğunda ilerler:
        stale/hatalı/boş kalan bir sembol varsa aynı kapalı bar bir sonraki
        turda tekrar denenir (2.12 fix). **Kalıcı yetersiz veri sembolleri
        (`insufficient`, 2.18) turu bloklamaz** — onlar yeni kapalı bar
        geldiğinde yeniden denenir.
        """
        processed = 0
        now = self.engine._now()
        for tf in self.config.kline_intervals:
            latest_closed = self.latest_closed(tf, now=now)
            if self._last_processed.get(tf) == latest_closed:
                continue
            symbols = sorted(self.universe.snapshot())
            if not symbols:
                continue
            logger.info("PA yeniden hesaplama turu: %s (%d sembol)", tf, len(symbols))
            results = await asyncio.gather(
                *(self._process(symbol, tf) for symbol in symbols), return_exceptions=True
            )
            successes = sum(1 for r in results if r is True)
            insufficient = sum(1 for r in results if r == "insufficient")
            processed += successes
            if successes + insufficient == len(symbols):
                self._last_processed[tf] = latest_closed
        return processed

    async def _process(self, symbol: str, tf: str) -> bool | str:
        """Sembolü işler. `True` başarı, `"insufficient"` kalıcı yetersiz veri,
        `False` geçici başarısızlık (sonraki turda yeniden denenir)."""
        async with self._sem:
            try:
                candles = await self.engine._load_candles(symbol, tf, PA_LOOKBACK)
            except Exception as exc:  # noqa: BLE001
                logger.debug("PA mum yüklenemedi (atlandı): %s %s — %r", symbol, tf, exc)
                return False
            if not candles:
                return False
            if self.engine.freshness(tf, candles[-1]["open_time"]) != FRESHNESS_FRESH:
                # Mum verisi hedef kapalı bara yetişmedi → kline scheduler tamamlayınca işlenir.
                return False
            try:
                await self.engine.analyze(symbol, tf)
                return True
            except RasatError as exc:
                if exc.code == ErrorCode.STALE_DATA:
                    # Kalıcı yetersiz veri (örn. tokenized stock'ta 1d için az mum):
                    # turu bloklamaz, yeni kapalı bar geldiğinde yeniden denenir.
                    logger.info("PA veri yetersiz (atlandı): %s %s — %s", symbol, tf, exc.message)
                    return "insufficient"
                logger.warning("PA hesaplama başarısız: %s %s — %s", symbol, tf, exc.message)
                return False
            except Exception as exc:  # noqa: BLE001
                logger.warning("PA hesaplama başarısız: %s %s — %r", symbol, tf, exc)
                return False
