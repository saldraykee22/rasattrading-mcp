"""Test yardımcıları: FakeRest (Binance REST taklidi)."""

import time


class FakeRest:
    """Binance REST'i taklit eder: klines/exchangeInfo/premiumIndex/OI üretir, çağrıları kaydeder."""

    def __init__(self, symbols: list[str] | None = None, extra_exchange_entries: list[dict] | None = None) -> None:
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT"]
        self.extra_exchange_entries = extra_exchange_entries or []
        self.calls: list[tuple[str, dict, int]] = []
        self.fail_kline_for: set[str] = set()

    @staticmethod
    def _gen_klines(symbol: str, interval: str, limit: int) -> list:
        from rasattrading_mcp.config import TIMEFRAME_SECONDS

        period = TIMEFRAME_SECONDS[interval]
        now = time.time()
        end = int(now // period) * period - period
        start = end - (limit - 1) * period
        rows = []
        for i in range(limit):
            t = start + i * period
            rows.append(
                [t, "100.0", "101.0", "99.0", "100.5", "1000.0", t + period - 1000, "100000", 10, "0", "0", "0"]
            )
        return rows

    async def get(self, path: str, params: dict | None = None, weight: int = 1) -> object:
        params = dict(params or {})
        self.calls.append((path, params, weight))

        if path == "/api/v3/exchangeInfo":
            symbols = [
                {"symbol": s, "status": "TRADING", "quoteAsset": "USDT"} for s in self.symbols
            ]
            symbols.extend(self.extra_exchange_entries)
            return {"timezone": "UTC", "symbols": symbols}
        if path == "/api/v3/klines":
            symbol = params.get("symbol")
            if symbol in self.fail_kline_for:
                raise RuntimeError(f"fake network error for {symbol}")
            return self._gen_klines(symbol, params.get("interval", "1h"), int(params.get("limit", 100)))
        if path == "/fapi/v1/premiumIndex":
            return [
                {"symbol": s, "lastFundingRate": "0.0001", "time": int(time.time() * 1000), "nextFundingTime": 0}
                for s in self.symbols
            ]
        if path == "/fapi/v1/openInterest":
            return {"openInterest": "1234.5", "time": int(time.time() * 1000)}
        if path == "/fapi/v1/allForceOrders":
            return [
                {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "price": "90000.0",
                    "origQty": "0.5",
                    "time": int(time.time() * 1000),
                }
            ]
        raise AssertionError(f"FakeRest bilinmeyen path: {path}")
