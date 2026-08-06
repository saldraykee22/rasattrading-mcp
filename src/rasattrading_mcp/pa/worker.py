"""2.8 — Arka plan PA hesaplama worker'ı.

Plan Bölüm 2.3: sabit timeframe seti (15m/1h/4h/1d) sürekli güncel tutulur.
Bu worker, bir timeframe'in kapalı barı yenilendiğinde evrendeki semboller için
PA zincirini otomatik yeniden hesaplar — agent'ın `get_market_structure`
çağrısını beklemeden. Sembolün mum verisi hedef kapalı bara yetişmemişse
(stale / kline scheduler tamamlamadıysa) yeniden hesaplama atlanır; veri
tazelenince bir sonraki turda işlenir. Concurrency semaphore ile sınırlanır.
"""

from __future__ import annotations

import asyncio
import logging
import time

from ..config import Config, TIMEFRAME_SECONDS
from ..envelope import FRESHNESS_FRESH
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
    def latest_closed(tf: str) -> int:
        period = TIMEFRAME_SECONDS[tf]
        return int(time.time() // period) * period - period

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
        """Yeni kapalı bar olan timeframe'ler için PA'yı yeniden hesaplar.

        İşlenen sembol sayısını döndürür (gözlem/diagnostik). İlk turda her
        timeframe işlenir (soğuk evren warm-up'ı) — `_last_processed` boştur.
        Marker yalnızca timeframe turu TAMAMEN başarılı olduğunda ilerler:
        stale/hatalı/boş kalan bir sembol varsa aynı kapalı bar bir sonraki
        turda tekrar denenir (2.12 fix).
        """
        processed = 0
        for tf in self.config.kline_intervals:
            latest_closed = self.latest_closed(tf)
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
            processed += successes
            if successes == len(symbols):
                self._last_processed[tf] = latest_closed
        return processed

    async def _process(self, symbol: str, tf: str) -> bool:
        async with self._sem:
            try:
                candles = await self.engine._load_candles(symbol, tf, PA_LOOKBACK)
            except Exception:  # noqa: BLE001
                return False
            if not candles:
                return False
            if PAEngine.freshness_for(tf, candles[-1]["open_time"]) != FRESHNESS_FRESH:
                # Mum verisi hedef kapalı bara yetişmedi → kline scheduler tamamlayınca işlenir.
                return False
            try:
                await self.engine.analyze(symbol, tf)
                return True
            except Exception:  # noqa: BLE001
                logger.warning("PA hesaplama başarısız: %s %s", symbol, tf)
                return False
