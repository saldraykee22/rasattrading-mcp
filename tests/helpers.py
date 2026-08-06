"""Test yardımcıları: FakeRest (Binance REST taklidi) + FakeOrderBroker."""

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

    @staticmethod
    def _symbol_entry(symbol: str, *, status: str = "TRADING", quote_asset: str = "USDT") -> dict:
        return {
            "symbol": symbol,
            "status": status,
            "baseAsset": symbol.replace(quote_asset, ""),
            "quoteAsset": quote_asset,
            "filters": [
                {"filterType": "LOT_SIZE", "minQty": "0.00001", "maxQty": "100000", "stepSize": "0.00001"},
                {"filterType": "MIN_NOTIONAL", "minNotional": "5.0", "applyToMarket": True, "avgPriceMins": 5},
                {"filterType": "PRICE_FILTER", "minPrice": "0.01000000", "maxPrice": "1000000.00000000", "tickSize": "0.01000000"},
            ],
        }

    async def get(self, path: str, params: dict | None = None, weight: int = 1) -> object:
        params = dict(params or {})
        self.calls.append((path, params, weight))

        if path == "/api/v3/exchangeInfo":
            symbols = [self._symbol_entry(s) for s in self.symbols]
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


class FakeOrderBroker:
    """OrderBroker taklidi: emirleri kaydeder, durum machine'ini simüle eder.

    - `place_order` çağrıları `placed` listesine eklenir; `place_result` dict'i
      verilirse o döner (status/exchange_order_id/executed_qty/avg_price).
    - `place_errors` dict'i {client_order_id: RasatError|Exception} ağ hatalarını
      tetikler (timeout/reject senaryoları).
    - `query_order` `queries` listesine eklenir; `query_results` dict'i
      {client_order_id: OrderResult|None} verilirse onu döner, yoksa son
      `placed` kaydını döner (bulunamadı → None).
    - `balances` dict'i {account_id: {asset: free}} bakiye simülasyonu.
    """

    def __init__(self, balances: dict[str, dict] | None = None) -> None:
        self.placed: list[dict] = []
        self.queries: list[dict] = []
        self.cancelled: list[dict] = []
        self.balances = balances or {}
        self.place_result: dict | None = None
        self.place_errors: dict[str, Exception] = {}
        self.query_results: dict[str, object | None] = {}
        self.cancel_errors: dict[str, Exception] = {}
        self._seq = 1000

    async def place_order(self, *, account_id, symbol, side, order_type, quantity, price, client_order_id):
        self.placed.append(
            {
                "account_id": account_id,
                "symbol": symbol,
                "side": side,
                "order_type": order_type,
                "quantity": quantity,
                "price": price,
                "client_order_id": client_order_id,
            }
        )
        if client_order_id in self.place_errors:
            raise self.place_errors[client_order_id]
        from rasattrading_mcp.data.order_broker import OrderResult

        if self.place_result is not None:
            result = OrderResult(
                status=self.place_result.get("status", "FILLED"),
                exchange_order_id=self.place_result.get("exchange_order_id", f"EX{self._seq}"),
                executed_qty=self.place_result.get("executed_qty", quantity),
                avg_price=self.place_result.get("avg_price", price or 100.0),
            )
        else:
            self._seq += 1
            result = OrderResult(
                status="FILLED",
                exchange_order_id=f"EX{self._seq}",
                executed_qty=quantity,
                avg_price=price or 100.0,
            )
        self._apply_fill(account_id, symbol, side, result, quantity, price)
        return result

    def _apply_fill(self, account_id, symbol, side, result, quantity, price):
        """FILLED olursa bakiye simülasyonunu güncelle: USDT düş, base ekle."""
        if result.status != "FILLED" or not symbol.endswith("USDT"):
            return
        base = symbol[: -len("USDT")]
        balances = self.balances.setdefault(account_id, {"USDT": 10000.0})
        avg = result.avg_price or price or 0.0
        if side == "BUY":
            balances["USDT"] = balances.get("USDT", 0.0) - result.executed_qty * avg
            balances[base] = balances.get(base, 0.0) + result.executed_qty
        else:
            balances["USDT"] = balances.get("USDT", 0.0) + result.executed_qty * avg
            balances[base] = balances.get(base, 0.0) - result.executed_qty

    async def query_order(self, *, account_id, symbol, client_order_id):
        self.queries.append(
            {"account_id": account_id, "symbol": symbol, "client_order_id": client_order_id}
        )
        from rasattrading_mcp.data.order_broker import OrderResult

        if client_order_id in self.query_results:
            return self.query_results[client_order_id]
        match = [p for p in self.placed if p.get("client_order_id") == client_order_id]
        if not match:
            return None
        return OrderResult(status="FILLED", exchange_order_id="EX1", executed_qty=match[0]["quantity"])

    async def cancel_order(self, *, account_id, symbol, client_order_id):
        if client_order_id in self.cancel_errors:
            raise self.cancel_errors[client_order_id]
        self.cancelled.append(
            {"account_id": account_id, "symbol": symbol, "client_order_id": client_order_id}
        )
        from rasattrading_mcp.data.order_broker import OrderResult

        return OrderResult(status="CANCELED", exchange_order_id=f"CX{len(self.cancelled)}")

    async def get_all_open_orders(self, *, account_id):
        return [
            {"symbol": "BTCUSDT", "order_id": "O1", "client_order_id": "open-1", "side": "BUY", "quantity": 0.5}
        ]

    async def cancel_all_open_orders(self, *, account_id, symbol):
        self.cancelled_all = getattr(self, "cancelled_all", [])
        self.cancelled_all.append({"account_id": account_id, "symbol": symbol})
        return 1

    async def get_balance(self, *, account_id):
        return dict(self.balances.get(account_id, {"USDT": 10000.0}))
