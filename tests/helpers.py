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
        if path == "/fapi/v1/exchangeInfo":
            # Varsayılan: spot evreninin tamamı futures'ta da var (test kolaylığı).
            # Yalnızca belirli sembollerin futures'ta olduğunu simüle etmek için
            # `fapi_symbols` set edilebilir.
            fapi_symbols = getattr(self, "fapi_symbols", self.symbols)
            return {
                "symbols": [
                    {"symbol": s, "status": "TRADING", "quoteAsset": "USDT"}
                    for s in fapi_symbols
                ]
            }
        if path in ("/api/v3/klines", "/fapi/v1/klines"):
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
        if path == "/fapi/v1/forceOrders":
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


class FakeClock:
    """BinanceClock taklidi: offset ve availability testte kontrol edilebilir.

    - `server_now()` `available` ise `time.time() + offset_seconds` döner;
      değilse `None` (fail-closed senaryoları).
    - `set_offset` / `set_available` ile host-sunucu saat kayması ve
      clock-unavailable durumları simüle edilir.
    """

    def __init__(self, offset: float = 0.0, available: bool = True) -> None:
        self.offset_seconds = float(offset)
        self._available = available

    def server_now(self) -> float | None:
        if not self._available:
            return None
        import time

        return time.time() + self.offset_seconds

    @property
    def available(self) -> bool:
        return self._available

    def set_offset(self, offset: float) -> None:
        self.offset_seconds = float(offset)

    def set_available(self, available: bool) -> None:
        self._available = available


class FakeOrderBroker:
    """OrderBroker taklidi: emirleri kaydeder, durum machine'ini simüle eder.

    - `place_order` çağrıları `placed` listesine eklenir; `place_result` dict'i
      verilirse o döner (status/exchange_order_id/executed_qty/avg_price).
    - `place_errors` dict'i {client_order_id: RasatError|Exception} ağ hatalarını
      tetikler (timeout/reject senaryoları).
    - `query_order` `queries` listesine eklenir; `query_results` dict'i
      {client_order_id: OrderResult|None} verilirse onu döner, yoksa son
      `placed` kaydını döner (bulunamadı → None).
    - `query_oco` `oco_queries` listesine eklenir; `oco_query_results` ve
      `oco_query_results_by_account` dict'leri OCO liste sorgularını simüle eder.
    - `balances` dict'i {account_id: {asset: free}} bakiye simülasyonu.
    - `locked_balances` dict'i {account_id: {asset: locked}} açık emirlerde kilitli
      miktarları simüle eder (3.21); `get_balance_detail` bunu free ile birleştirir.
    """

    def __init__(self, balances: dict[str, dict] | None = None) -> None:
        self.placed: list[dict] = []
        self.queries: list[dict] = []
        self.oco_queries: list[dict] = []
        self.cancelled: list[dict] = []
        self.balances = balances or {}
        self.locked_balances: dict[str, dict] = {}
        self.place_result: dict | None = None
        self.place_errors: dict[str, Exception] = {}
        self.place_errors_by_account: dict[str, Exception] = {}
        self.query_results: dict[str, object | None] = {}
        self.query_results_by_account: dict[str, object | None] = {}
        self.oco_query_results: dict[str, object | None] = {}
        self.oco_query_results_by_account: dict[str, object | None] = {}
        self.cancel_errors: dict[str, Exception] = {}
        self.cancel_all_errors: dict[str, Exception] = {}
        #: Borsada duran (local DB'de kaydı olmayabilir) açık emirler.
        #: Varsayılan, legacy `get_all_open_orders` davranışını korur (bir BTCUSDT
        #: yetim emri); isteyen test `open_orders = []` ile temizler.
        self.open_orders: list[dict] = [
            {"symbol": "BTCUSDT", "order_id": "O1", "client_order_id": "open-1", "side": "BUY", "quantity": 0.5}
        ]
        #: get_balance çağrısında hesap bazlı hata (exposure fail-closed testleri).
        self.get_balance_errors: dict[str, Exception] = {}
        self._seq = 1000

    async def place_order(self, *, account_id, symbol, side, order_type, quantity, price, client_order_id, stop_price=None):
        self.placed.append(
            {
                "account_id": account_id,
                "symbol": symbol,
                "side": side,
                "order_type": order_type,
                "quantity": quantity,
                "price": price,
                "stop_price": stop_price,
                "client_order_id": client_order_id,
            }
        )
        if account_id in self.place_errors_by_account:
            raise self.place_errors_by_account[account_id]
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

    async def place_oco(self, *, account_id, symbol, side, quantity, price, stop_price, stop_limit_price, client_order_id):
        """OCO emri: tek kayıt olarak placed listesine düşer (newOrderList simülasyonu)."""
        self.placed.append(
            {
                "account_id": account_id,
                "symbol": symbol,
                "side": side,
                "order_type": "OCO",
                "quantity": quantity,
                "price": price,
                "stop_price": stop_price,
                "stop_limit_price": stop_limit_price,
                "client_order_id": client_order_id,
            }
        )
        if account_id in self.place_errors_by_account:
            raise self.place_errors_by_account[account_id]
        if client_order_id in self.place_errors:
            raise self.place_errors[client_order_id]
        from rasattrading_mcp.data.order_broker import OrderResult

        self._seq += 1
        return OrderResult(
            status="NEW",
            exchange_order_id=f"OL{self._seq}",
            executed_qty=0.0,
            avg_price=0.0,
        )

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

        if account_id in self.query_results_by_account:
            return self.query_results_by_account[account_id]
        if client_order_id in self.query_results:
            return self.query_results[client_order_id]
        match = [p for p in self.placed if p.get("client_order_id") == client_order_id]
        if not match:
            return None
        return OrderResult(status="FILLED", exchange_order_id="EX1", executed_qty=match[0]["quantity"])

    async def query_oco(self, *, account_id, list_client_order_id):
        self.oco_queries.append(
            {"account_id": account_id, "list_client_order_id": list_client_order_id}
        )
        from rasattrading_mcp.data.order_broker import OrderResult

        if account_id in self.oco_query_results_by_account:
            return self.oco_query_results_by_account[account_id]
        if list_client_order_id in self.oco_query_results:
            return self.oco_query_results[list_client_order_id]
        match = [p for p in self.placed if p.get("client_order_id") == list_client_order_id and p.get("order_type") == "OCO"]
        if not match:
            return None
        return OrderResult(status="NEW", exchange_order_id="OL1")

    async def cancel_order(self, *, account_id, symbol, client_order_id):
        if client_order_id in self.cancel_errors:
            raise self.cancel_errors[client_order_id]
        self.cancelled.append(
            {"account_id": account_id, "symbol": symbol, "client_order_id": client_order_id}
        )
        from rasattrading_mcp.data.order_broker import OrderResult

        return OrderResult(status="CANCELED", exchange_order_id=f"CX{len(self.cancelled)}")

    async def get_all_open_orders(self, *, account_id):
        return list(self.open_orders)

    async def cancel_all_open_orders(self, *, account_id, symbol):
        if symbol in getattr(self, "cancel_all_errors", {}):
            raise self.cancel_all_errors[symbol]
        self.cancelled_all = getattr(self, "cancelled_all", [])
        self.cancelled_all.append({"account_id": account_id, "symbol": symbol})
        return 1

    async def get_balance(self, *, account_id):
        if account_id in self.get_balance_errors:
            raise self.get_balance_errors[account_id]
        return dict(self.balances.get(account_id, {"USDT": 10000.0}))

    async def get_balance_detail(self, *, account_id):
        free = dict(self.balances.get(account_id, {"USDT": 10000.0}))
        locked = dict(self.locked_balances.get(account_id, {}))
        assets = set(free) | set(locked)
        return {
            asset: {
                "free": float(free.get(asset, 0) or 0),
                "locked": float(locked.get(asset, 0) or 0),
            }
            for asset in assets
        }
