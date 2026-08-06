"""Emir yürütme servisi (ticket 3.4).

`execute_on_accounts` / `place_order`:

- **Idempotency:** `orders` tablosu (account_id, idempotency_key) UNIQUE. Aynı
  anahtarla retry çift emir üretmez — mevcut emir döner / durumu reconcile edilir.
- **Reconcile-before-retry:** ağ zaman aşımında Binance'ten gerçek durum
  `client_order_id` ile sorgulanır; bulunursa o durum kullanılır, bulunamazsa
  UNKNOWN döner. Körlemesine tekrar gönderim YOK.
- **Per-account serialization:** her hesap için `asyncio.Lock` — eşzamanlı iki
  `execute_on_accounts` aynı hesapta race yapmaz.
- **Aggregate exposure:** hesabın açık emir notional'ı + kısmi dolumlar + base
  bakiye (daemon'ın kendi taze bakiye/fiyat snapshot'ı) toplamı policy cap'ine
  karşı kontrol edilir; aşılacaksa tek kullanımlık override tüketilir.
- **Kısmi başarı:** toplu çağrılar hesap başına ayrı sonuç döner.
- Temel doğruluk kontrolleri (3.3) her zaman önce çalışır; override bunları asla atlamaz.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
import uuid
from typing import Any, Protocol

from ..accuracy import check_price_fresh, check_stop_direction, check_symbol_valid
from ..errors import ErrorCode, RasatError
from ..envelope import FRESHNESS_FRESH
from ..position_sizing import SymbolFilters, calculate_position_size
from .accounts import AccountService
from .audit import AuditLog
from .db import Database
from .risk_policy import RiskPolicyService

logger = logging.getLogger("rasattrading.storage.orders")

#: Binance state machine + yerel PAPER (paper hesap simülasyonu).
STATUS_PAPER = "paper"

_ORDER_COLUMNS = (
    "order_id",
    "account_id",
    "idempotency_key",
    "symbol",
    "side",
    "order_type",
    "quantity",
    "price",
    "status",
    "exchange_order_id",
    "client_order_id",
    "executed_qty",
    "avg_price",
    "fee",
    "notional",
    "reference_price",
    "error_code",
    "error_message",
    "created_at",
    "updated_at",
)


class MarketFeed(Protocol):
    """Daemon'ın kendi taze piyasa snapshot'ı (agent rakamlarına güvenilmez)."""

    async def symbol_valid(self, symbol: str) -> bool: ...
    async def price(self, symbol: str) -> float | None: ...
    async def filters(self, symbol: str) -> SymbolFilters | None: ...


class PipelineMarketFeed:
    """DataPipeline'ı MarketFeed arayüzüne adapte eder."""

    def __init__(self, pipeline: Any) -> None:
        self._pipeline = pipeline

    async def symbol_valid(self, symbol: str) -> bool:
        return await self._pipeline.ensure_symbol(symbol)

    async def price(self, symbol: str) -> float | None:
        ticker = self._pipeline.get_ticker(symbol)
        if ticker is None or ticker.get("freshness") != FRESHNESS_FRESH:
            return None
        return float(ticker["last"])

    async def filters(self, symbol: str) -> SymbolFilters | None:
        info = self._pipeline.symbol_info(symbol)
        if info is None:
            return None
        return SymbolFilters.from_exchange_info(info)


class OrderService:
    def __init__(
        self,
        db: Database,
        accounts: AccountService,
        risk: RiskPolicyService,
        broker: Any,
        market: MarketFeed,
        audit: AuditLog | None = None,
        default_fee_rate: float = 0.001,
    ) -> None:
        self.db = db
        self.accounts = accounts
        self.risk = risk
        self.broker = broker
        self.market = market
        self.audit = audit
        self.default_fee_rate = default_fee_rate
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock(self, account_id: str) -> asyncio.Lock:
        lock = self._locks.get(account_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[account_id] = lock
        return lock

    # ---------- hedef çözümleme ----------

    async def _resolve_targets(self, account_ids: Any, tags: Any) -> list[dict]:
        if not account_ids and not tags:
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_ids veya tags zorunlu (ikisi de boş olamaz)")
        id_set = {str(a).strip() for a in (account_ids or []) if a}
        tag_list = [str(t).strip() for t in (tags or []) if t]
        listed = await self.accounts.list_accounts()
        accounts = listed["accounts"]
        missing = [a for a in id_set if not any(acc["account_id"] == a for acc in accounts)]
        if missing:
            raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {missing[0]}")
        selected = [
            acc
            for acc in accounts
            if acc["account_id"] in id_set or (tag_list and any(t in acc["tags"] for t in tag_list))
        ]
        return selected

    # ---------- aggregate exposure ----------

    @staticmethod
    def _open_order_notional(conn: sqlite3.Connection, account_id: str) -> float:
        row = conn.execute(
            "SELECT COALESCE(SUM(notional), 0) AS total FROM orders "
            "WHERE account_id = ? AND status IN ('NEW', 'PARTIALLY_FILLED')",
            (account_id,),
        ).fetchone()
        return float(row["total"]) if row else 0.0

    async def _current_exposure(self, account_id: str, quote_asset: str) -> float:
        """Açık emirler + kısmi dolumlar + (base) bakiye değeri — daemon'ın taze verisi."""

        def _open(conn: sqlite3.Connection) -> float:
            return self._open_order_notional(conn, account_id)

        open_notional = await self.db.read(_open)
        balances = await self.broker.get_balance(account_id=account_id)
        held_value = 0.0
        for asset, free in balances.items():
            if asset == quote_asset:
                continue
            symbol = f"{asset}{quote_asset}"
            price = await self.market.price(symbol)
            if price is None:
                continue  # fiyatı bilinmeyen varlık exposure'a katılmaz (sessizce 0 sayılmaz)
            held_value += free * price
        return open_notional + held_value

    # ---------- emir kaydı ----------

    def _insert_order(
        self,
        conn: sqlite3.Connection,
        *,
        account_id: str,
        idempotency_key: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        notional: float,
        reference_price: float,
        equity_snapshot: float,
        status: str,
        client_order_id: str,
    ) -> dict:
        now = int(time.time())
        order_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO orders "
            "(order_id, account_id, idempotency_key, symbol, side, order_type, quantity, price, status, "
            " client_order_id, executed_qty, avg_price, fee, notional, reference_price, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,0,0,0,?,?,?,?)",
            (
                order_id,
                account_id,
                idempotency_key,
                symbol,
                side,
                order_type,
                quantity,
                price,
                status,
                client_order_id,
                notional,
                reference_price,
                now,
                now,
            ),
        )
        return {
            "order_id": order_id,
            "account_id": account_id,
            "idempotency_key": idempotency_key,
            "symbol": symbol,
            "side": side,
            "order_type": order_type,
            "quantity": quantity,
            "price": price,
            "status": status,
            "exchange_order_id": None,
            "client_order_id": client_order_id,
            "executed_qty": 0.0,
            "avg_price": 0.0,
            "fee": 0.0,
            "notional": notional,
            "reference_price": reference_price,
            "equity_snapshot": equity_snapshot,
            "error_code": None,
            "error_message": None,
            "created_at": now,
            "updated_at": now,
        }

    def _update_order(self, conn: sqlite3.Connection, order_id: str, **fields: Any) -> None:
        now = int(time.time())
        sets = ", ".join(f"{k} = ?" for k in fields)
        conn.execute(f"UPDATE orders SET {sets}, updated_at = ? WHERE order_id = ?", (*fields.values(), now, order_id))

    def _load_order(self, conn: sqlite3.Connection, account_id: str, idempotency_key: str) -> dict | None:
        row = conn.execute(
            "SELECT " + ", ".join(_ORDER_COLUMNS) + " FROM orders WHERE account_id = ? AND idempotency_key = ?",
            (account_id, idempotency_key),
        ).fetchone()
        if row is None:
            return None
        return dict(row)

    @staticmethod
    def _order_to_result(order: dict, *, position_size: float | None = None, error: dict | None = None) -> dict:
        result = {
            "account_id": order["account_id"],
            "status": order["status"],
            "symbol": order["symbol"],
            "side": order["side"],
            "order_type": order["order_type"],
            "quantity": order["quantity"],
            "notional": order["notional"],
            "order_id": order.get("order_id"),
            "exchange_order_id": order.get("exchange_order_id"),
            "executed_qty": order.get("executed_qty", 0.0),
            "avg_price": order.get("avg_price", 0.0),
        }
        if position_size is not None:
            result["position_size"] = position_size
        if error:
            result["error"] = error
        return result

    # ---------- ana akış ----------

    async def execute_on_accounts(
        self,
        *,
        account_ids: Any = None,
        tags: Any = None,
        symbol: str,
        side: str,
        entry: float,
        stop_loss: float,
        risk_pct: float,
        idempotency_key: str,
        order_type: str = "MARKET",
        actor: str = "mcp-agent",
    ) -> dict[str, Any]:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "idempotency_key zorunlu (string)")
        targets = await self._resolve_targets(account_ids, tags)
        results = []
        for account in targets:
            lock = self._lock(account["account_id"])
            async with lock:
                try:
                    result = await self._execute_one_sized(
                        account,
                        symbol=symbol,
                        side=side,
                        entry=float(entry),
                        stop_loss=float(stop_loss),
                        risk_pct=float(risk_pct),
                        idempotency_key=idempotency_key.strip(),
                        order_type=order_type,
                        actor=actor or "mcp-agent",
                    )
                    results.append(result)
                except RasatError as exc:
                    results.append(
                        {
                            "account_id": account["account_id"],
                            "status": "REJECTED",
                            "symbol": symbol,
                            "error": {"code": exc.code, "message": exc.message},
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("hesap emri başarısız: %s", account["account_id"])
                    results.append(
                        {
                            "account_id": account["account_id"],
                            "status": "UNKNOWN",
                            "symbol": symbol,
                            "error": {"code": ErrorCode.INTERNAL_ERROR, "message": str(exc)},
                        }
                    )
        succeeded = sum(1 for r in results if r.get("status") not in ("REJECTED", "UNKNOWN"))
        return {
            "results": results,
            "count": len(results),
            "succeeded": succeeded,
            "failed": len(results) - succeeded,
        }

    async def place_order(
        self,
        *,
        account_id: str,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None = None,
        idempotency_key: str,
        actor: str = "mcp-agent",
    ) -> dict[str, Any]:
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string)")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "idempotency_key zorunlu (string)")
        lock = self._lock(account_id.strip())
        async with lock:
            return await self._execute_one_direct(
                account_id.strip(),
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=float(quantity),
                price=None if price is None else float(price),
                idempotency_key=idempotency_key.strip(),
                actor=actor or "mcp-agent",
            )

    # ---------- tekil emirler ----------

    async def _is_real(self, account: dict) -> bool:
        """Hesap real modda mı? Real ise credential'ları doğrula (yoksa fail-closed)."""
        if str(account.get("trading_lock", "paper")) != "real":
            return False
        await self.accounts.get_credentials(account["account_id"])
        return True

    async def _accuracy_preflight(self, symbol: str, side: str, entry: float, stop_loss: float) -> None:
        if not await self.market.symbol_valid(symbol):
            raise RasatError(ErrorCode.INVALID_SYMBOL, f"evrende bilinmeyen sembol: {symbol}")
        price = await self.market.price(symbol)
        check_price_fresh(FRESHNESS_FRESH if price is not None else None, symbol)
        check_stop_direction(side, entry, stop_loss)

    @staticmethod
    def _check_available_balance(
        *,
        balances: dict,
        side: str,
        symbol: str,
        base_asset: str,
        quote_asset: str,
        quantity: float,
        notional: float,
        fee: float,
    ) -> None:
        """Spot'ta emrin gerektirdiği SERBEST bakiyeyi kontrol eder (3.8).

        - BUY  → serbest quote-asset (USDT) >= notional + fee
        - SELL → serbest base-asset >= quantity

        Equity değil, broker'ın `free` bakiyesi kullanılır: base asset tutan bir
        hesapta equity mevcut USDT'den büyük olduğu için equity-bazlı kontrol
        "yetersiz bakiye → gönderilmez" garantisini sağlamaz (review H1).
        Yetersizlikte her zaman `INSUFFICIENT_BALANCE` fırlatılır.
        """
        side_norm = (side or "BUY").upper()
        if side_norm == "BUY":
            available = float(balances.get(quote_asset, 0) or 0)
            required = notional + fee
            if required > available:
                raise RasatError(
                    ErrorCode.INSUFFICIENT_BALANCE,
                    f"yetersiz bakiye: BUY için gereken {required:.8f} {quote_asset} "
                    f"> serbest {available:.8f} ({symbol})",
                )
        elif side_norm == "SELL":
            available = float(balances.get(base_asset, 0) or 0)
            if quantity > available:
                raise RasatError(
                    ErrorCode.INSUFFICIENT_BALANCE,
                    f"yetersiz bakiye: SELL için gereken {quantity:.8f} {base_asset} "
                    f"> serbest {available:.8f} ({symbol})",
                )
        else:
            raise RasatError(ErrorCode.INVALID_REQUEST, f"geçersiz side: {side}")

    async def _execute_one_sized(
        self,
        account: dict,
        *,
        symbol: str,
        side: str,
        entry: float,
        stop_loss: float,
        risk_pct: float,
        idempotency_key: str,
        order_type: str,
        actor: str,
    ) -> dict:
        is_real = await self._is_real(account)
        await self._accuracy_preflight(symbol, side, entry, stop_loss)
        quote_asset = "USDT"
        price = await self.market.price(symbol)

        # Idempotency: bu anahtarla emir zaten var mı?
        existing = await self.db.read(lambda conn: self._load_order(conn, account["account_id"], idempotency_key))
        if existing is not None:
            return await self._handle_existing(account, existing)

        # 1) Daemon'ın kendi taze bakiye snapshot'ı + equity + exchange filtreleri
        balances = await self.broker.get_balance(account_id=account["account_id"])
        equity = 0.0
        for asset, free in balances.items():
            if asset == quote_asset:
                equity += free
                continue
            asset_price = await self.market.price(f"{asset}{quote_asset}")
            if asset_price is not None:
                equity += free * asset_price

        filters = await self.market.filters(symbol)
        if filters is None:
            raise RasatError(ErrorCode.FILTER_VIOLATION, f"exchangeInfo filtreleri yok: {symbol}")

        # 3.8: boyutlandırma girişi yan-aware — BUY'da serbest USDT (equity değil),
        # SELL'de equity (base gate'i boyutlandırma sonrası uygulanır). Sıfır
        # bakiye INSUFFICIENT_BALANCE döner, INVALID_REQUEST değil.
        side_norm = (side or "BUY").upper()
        if side_norm == "BUY":
            available_quote = float(balances.get(quote_asset, 0) or 0)
            if available_quote <= 0:
                raise RasatError(
                    ErrorCode.INSUFFICIENT_BALANCE,
                    f"yetersiz bakiye: serbest {quote_asset} 0 ({symbol})",
                )
            sizing_balance = available_quote
        else:
            sizing_balance = equity
            if sizing_balance <= 0:
                raise RasatError(
                    ErrorCode.INSUFFICIENT_BALANCE,
                    f"yetersiz bakiye: hesap bakiyesi 0 ({symbol})",
                )

        # 2) Position sizing (exchange filtrelerine uyumlu, fee-aware)
        sized = calculate_position_size(
            symbol=symbol,
            account_balance=sizing_balance,
            risk_pct=risk_pct,
            entry=entry,
            stop_loss=stop_loss,
            filters=filters,
            side=side,
            fee_rate=self.default_fee_rate,
        )
        quantity = sized["quantity"]
        notional = quantity * price

        # 3) Serbest bakiye gate'i (3.8): BUY → serbest USDT, SELL → serbest base.
        #    Market fiyatı entry'den yüksekse sizing'in entry-bazlı cap'i yetmez;
        #    bu gate nihai notional/fee üzerinden borsaya gitmeden reddeder.
        self._check_available_balance(
            balances=balances, side=side_norm, symbol=symbol,
            base_asset=filters.base_asset, quote_asset=quote_asset,
            quantity=quantity, notional=notional, fee=notional * self.default_fee_rate,
        )

        # 4) Risk politikası cap'leri (override yoksa katı)
        policy = await self.risk.get_policy(account["account_id"])
        exposure_after = await self._current_exposure(account["account_id"], quote_asset) + notional
        await self._enforce_caps_or_override(
            account, policy, symbol, notional, exposure_after, idempotency_key
        )

        # 5) Emri gönder
        return await self._place_and_record(
            account, is_real, symbol, side, order_type, quantity, entry, notional, price,
            idempotency_key, equity, actor,
        )

    async def _execute_one_direct(
        self,
        account_id: str,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        idempotency_key: str,
        actor: str,
    ) -> dict:
        account = await self.accounts.get_account(account_id)
        is_real = await self._is_real(account)
        if not await self.market.symbol_valid(symbol):
            raise RasatError(ErrorCode.INVALID_SYMBOL, f"evrende bilinmeyen sembol: {symbol}")
        market_price = await self.market.price(symbol)
        check_price_fresh(FRESHNESS_FRESH if market_price is not None else None, symbol)
        if price is None:
            price = market_price
        notional = quantity * price

        existing = await self.db.read(lambda conn: self._load_order(conn, account_id, idempotency_key))
        if existing is not None:
            return await self._handle_existing(account, existing)

        # 3.8: serbest bakiye gate'i (BUY → USDT, SELL → base) — equity değil.
        filters = await self.market.filters(symbol)
        base_asset = filters.base_asset if filters else (
            symbol[: -len("USDT")] if symbol.endswith("USDT") else symbol
        )
        balances = await self.broker.get_balance(account_id=account_id)
        self._check_available_balance(
            balances=balances, side=(side or "BUY").upper(), symbol=symbol,
            base_asset=base_asset, quote_asset="USDT",
            quantity=quantity, notional=notional, fee=notional * self.default_fee_rate,
        )

        policy = await self.risk.get_policy(account_id)
        exposure_after = await self._current_exposure(account_id, "USDT") + notional
        await self._enforce_caps_or_override(account, policy, symbol, notional, exposure_after, idempotency_key)

        equity = sum(float(v) for v in balances.values())
        return await self._place_and_record(
            account, is_real, symbol, side, order_type, quantity, price, notional, market_price,
            idempotency_key, equity, actor,
        )

    async def _enforce_caps_or_override(
        self,
        account: dict,
        policy: dict,
        symbol: str,
        notional: float,
        exposure_after: float,
        idempotency_key: str,
    ) -> None:
        """Cap'leri uygula; aşılacaksa tek kullanımlık override'ı dene.

        Override yalnızca kullanıcı-tanımlı policy cap'lerini atlar — temel doğruluk
        kontrolleri (3.3) bu fonksiyonun dışında, her zaman önce çalışır.
        """
        from ..risk import enforce_policy_caps

        try:
            enforce_policy_caps(
                symbol=symbol,
                notional=notional,
                policy=policy,
                aggregate_exposure=exposure_after,
            )
            return
        except RasatError as exc:
            if exc.code not in (ErrorCode.RISK_LIMIT_EXCEEDED, ErrorCode.SYMBOL_NOT_ALLOWED):
                raise
            override = await self.risk.consume_override(
                account["account_id"],
                policy_version=policy["policy_version"],
                consumed_by_idem=idempotency_key,
            )
            if override is None:
                raise exc
            logger.info(
                "override tüketildi (account=%s, policy_version=%s): %s",
                account["account_id"],
                policy["policy_version"],
                idempotency_key,
            )

    async def _place_and_record(
        self,
        account: dict,
        is_real: bool,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        notional: float,
        reference_price: float,
        idempotency_key: str,
        equity_snapshot: float,
        actor: str,
    ) -> dict:
        account_id = account["account_id"]
        from ..data.order_broker import to_client_order_id

        client_order_id = to_client_order_id(idempotency_key)

        if not is_real:
            # PAPER hesap: simüle edilmiş emir, broker'a gitmez
            def _insert(conn: sqlite3.Connection) -> dict:
                order = self._insert_order(
                    conn,
                    account_id=account_id,
                    idempotency_key=idempotency_key,
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    notional=notional,
                    reference_price=reference_price,
                    equity_snapshot=equity_snapshot,
                    status=STATUS_PAPER,
                    client_order_id=client_order_id,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor,
                        action="place_order_paper",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": symbol,
                            "side": side,
                            "quantity": quantity,
                            "notional": notional,
                        },
                    )
                return order

            order = await self.db.write(_insert)
            return self._order_to_result(order, position_size=quantity)

        # REAL hesap
        # Önce kaydı aç (crash sonrası iz), sonra broker'a git.
        def _insert(conn: sqlite3.Connection) -> dict:
            return self._insert_order(
                conn,
                account_id=account_id,
                idempotency_key=idempotency_key,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                notional=notional,
                reference_price=reference_price,
                equity_snapshot=equity_snapshot,
                status="NEW",
                client_order_id=client_order_id,
            )

        order = await self.db.write(_insert)
        try:
            result = await self.broker.place_order(
                account_id=account_id,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity,
                price=price,
                client_order_id=client_order_id,
            )
        except RasatError as exc:
            if exc.code == ErrorCode.TIMEOUT:
                # Reconcile-before-retry: Binance'ten gerçek durumu sor.
                return await self._reconcile_after_timeout(
                    account, order, symbol, client_order_id, actor
                )
            status = "REJECTED" if exc.code == ErrorCode.ORDER_REJECTED else "UNKNOWN"

            def _fail(conn: sqlite3.Connection) -> None:
                self._update_order(
                    conn,
                    order["order_id"],
                    status=status,
                    error_code=exc.code,
                    error_message=exc.message,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor,
                        action="place_order_failed",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": symbol,
                            "error_code": exc.code,
                        },
                    )

            await self.db.write(_fail)
            order = {**order, "status": status, "error_code": exc.code, "error_message": exc.message}
            return self._order_to_result(order, position_size=quantity)

        def _fill(conn: sqlite3.Connection) -> dict:
            self._update_order(
                conn,
                order["order_id"],
                status=result.status,
                exchange_order_id=result.exchange_order_id,
                executed_qty=result.executed_qty,
                avg_price=result.avg_price,
            )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor,
                    action="place_order",
                    details={
                        "account_id": account_id,
                        "order_id": order["order_id"],
                        "symbol": symbol,
                        "side": side,
                        "quantity": quantity,
                        "status": result.status,
                        "exchange_order_id": result.exchange_order_id,
                    },
                )
            return {**order, "status": result.status, "exchange_order_id": result.exchange_order_id,
                    "executed_qty": result.executed_qty, "avg_price": result.avg_price}

        order = await self.db.write(_fill)
        return self._order_to_result(order, position_size=quantity)

    async def _reconcile_after_timeout(
        self,
        account: dict,
        order: dict,
        symbol: str,
        client_order_id: str,
        actor: str,
    ) -> dict:
        account_id = account["account_id"]
        try:
            found = await self.broker.query_order(
                account_id=account_id, symbol=symbol, client_order_id=client_order_id
            )
        except RasatError:
            found = None
        if found is not None:
            def _apply(conn: sqlite3.Connection) -> None:
                self._update_order(
                    conn,
                    order["order_id"],
                    status=found.status,
                    exchange_order_id=found.exchange_order_id,
                    executed_qty=found.executed_qty,
                    avg_price=found.avg_price,
                )
                if self.audit is not None:
                    self.audit.append_in_connection(
                        conn,
                        actor=actor,
                        action="order_reconciled",
                        details={
                            "account_id": account_id,
                            "order_id": order["order_id"],
                            "symbol": symbol,
                            "status": found.status,
                            "exchange_order_id": found.exchange_order_id,
                        },
                    )

            await self.db.write(_apply)
            return self._order_to_result({**order, "status": found.status,
                                          "exchange_order_id": found.exchange_order_id,
                                          "executed_qty": found.executed_qty,
                                          "avg_price": found.avg_price})
        # Bulunamadı → durum belirsiz; UNKNOWN, körlemesine retry yok.
        def _unknown(conn: sqlite3.Connection) -> None:
            self._update_order(conn, order["order_id"], status="UNKNOWN", error_code=ErrorCode.ORDER_UNKNOWN,
                               error_message="ağ zaman aşımı; emir durumu doğrulanamadı")
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor=actor,
                    action="order_unknown",
                    details={"account_id": account_id, "order_id": order["order_id"], "symbol": symbol},
                )

        await self.db.write(_unknown)
        return self._order_to_result(
            {**order, "status": "UNKNOWN", "error_code": ErrorCode.ORDER_UNKNOWN,
             "error_message": "ağ zaman aşımı; emir durumu doğrulanamadı"},
            error={"code": ErrorCode.ORDER_UNKNOWN, "message": "ağ zaman aşımı; emir durumu doğrulanamadı"},
        )

    async def _handle_existing(self, account: dict, existing: dict) -> dict:
        """Aynı idempotency_key ile gelen retry: çift emir üretmez.

        - Terminal durumdaysa stored sonuç döner.
        - Açık/NEW durumdaysa reconcile edilir (gerçek durum sorulur) ve döner.
        """
        if existing["status"] not in ("NEW", "PARTIALLY_FILLED"):
            return self._order_to_result(existing)

        if str(account.get("trading_lock", "paper")) != "real":
            # paper hesap: simülasyon tekrarlanmaz, stored sonuç döner
            return self._order_to_result(existing)

        try:
            found = await self.broker.query_order(
                account_id=account["account_id"],
                symbol=existing["symbol"],
                client_order_id=existing["client_order_id"],
            )
        except RasatError:
            found = None
        if found is None:
            return self._order_to_result(existing)

        def _apply(conn: sqlite3.Connection) -> None:
            self._update_order(
                conn,
                existing["order_id"],
                status=found.status,
                exchange_order_id=found.exchange_order_id,
                executed_qty=found.executed_qty,
                avg_price=found.avg_price,
            )
            if self.audit is not None:
                self.audit.append_in_connection(
                    conn,
                    actor="system",
                    action="order_reconciled",
                    details={
                        "account_id": account["account_id"],
                        "order_id": existing["order_id"],
                        "status": found.status,
                    },
                )

        await self.db.write(_apply)
        return self._order_to_result(
            {**existing, "status": found.status, "exchange_order_id": found.exchange_order_id,
             "executed_qty": found.executed_qty, "avg_price": found.avg_price}
        )

    # ---------- kill switch: close_all_positions (3.5) ----------

    async def close_all_positions(self, *, account_id: str, actor: str = "mcp-agent") -> dict[str, Any]:
        """Açık emirleri iptal edip base asset bakiyelerini market fiyatından satar.

        - `account_id == "all"` ise tüm hesaplar; değilse o hesap.
        - Spot long-only: "pozisyon kapat" = elde tutulan base asset bakiyesini satmak.
        - Kısmi başarı: her hesap ayrı sonuç; hangi hesabın kapandığı/kapanamadığı açıkça raporlanır.
        - Idempotent: aynı (account, symbol) satışı tekrar çalıştırmada çift satış yapmaz.
        """
        if not isinstance(account_id, str) or not account_id.strip():
            raise RasatError(ErrorCode.INVALID_REQUEST, "account_id zorunlu (string|'all')")
        account_id = account_id.strip()

        if account_id == "all":
            accounts = (await self.accounts.list_accounts())["accounts"]
        else:
            accounts = [await self.accounts.get_account(account_id)]

        results = []
        for account in accounts:
            lock = self._lock(account["account_id"])
            async with lock:
                try:
                    result = await self._close_one(account, actor=actor or "mcp-agent")
                    results.append(result)
                except RasatError as exc:
                    results.append(
                        {
                            "account_id": account["account_id"],
                            "closed": False,
                            "error": {"code": exc.code, "message": exc.message},
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("pozisyon kapatma başarısız: %s", account["account_id"])
                    results.append(
                        {
                            "account_id": account["account_id"],
                            "closed": False,
                            "error": {"code": ErrorCode.INTERNAL_ERROR, "message": str(exc)},
                        }
                    )

        closed = sum(1 for r in results if r.get("closed"))
        return {"results": results, "count": len(results), "closed": closed, "failed": len(results) - closed}

    async def _close_one(self, account: dict, actor: str) -> dict:
        from ..data.order_broker import to_client_order_id

        account_id = account["account_id"]
        is_real = await self._is_real(account)
        cancelled: list[str] = []
        cancel_errors: list[dict] = []
        sold: list[dict] = []

        # 1) Açık emirleri iptal et
        def _open_orders(conn: sqlite3.Connection) -> list[dict]:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT " + ", ".join(_ORDER_COLUMNS)
                    + " FROM orders WHERE account_id = ? AND status IN ('NEW', 'PARTIALLY_FILLED')",
                    (account_id,),
                ).fetchall()
            ]

        open_orders = await self.db.read(_open_orders)
        for order in open_orders:
            if is_real:
                try:
                    await self.broker.cancel_order(
                        account_id=account_id,
                        symbol=order["symbol"],
                        client_order_id=order["client_order_id"],
                    )
                except RasatError as exc:
                    # 3.9: iptal başarısız → sessizce CANCELED yapma. Gerçek
                    # durum bilinmiyor; UNKNOWN'a çek ve sonuçta açıkça raporla
                    # (kısmi başarı sözleşmesi, kill switch dahil çağıran görür).
                    def _unknown(conn: sqlite3.Connection, oid: str = order["order_id"], cerr: RasatError = exc) -> None:
                        self._update_order(
                            conn, oid, status="UNKNOWN",
                            error_code=cerr.code, error_message=cerr.message,
                        )

                    await self.db.write(_unknown)
                    cancel_errors.append(
                        {
                            "symbol": order["symbol"],
                            "order_id": order["order_id"],
                            "client_order_id": order["client_order_id"],
                            "error": {"code": exc.code, "message": exc.message},
                        }
                    )
                    continue
            cancelled.append(order["symbol"])

            def _cancel(conn: sqlite3.Connection, oid: str = order["order_id"]) -> None:
                self._update_order(conn, oid, status="CANCELED")

            await self.db.write(_cancel)

        # 2) Base asset bakiyelerini sat
        if not is_real:
            return {"account_id": account_id, "closed": False, "mode": "paper",
                    "cancelled": cancelled, "cancel_errors": cancel_errors, "sold": sold}

        balances = await self.broker.get_balance(account_id=account_id)
        quote_asset = "USDT"
        for asset, free in balances.items():
            if asset == quote_asset or free <= 0:
                continue
            symbol = f"{asset}{quote_asset}"
            if not await self.market.symbol_valid(symbol):
                continue
            idem = f"close-{account_id}-{symbol}"
            existing = await self.db.read(lambda conn: self._load_order(conn, account_id, idem))
            if existing is not None:
                sold.append({"symbol": symbol, "quantity": existing["quantity"],
                             "status": existing["status"], "order_id": existing["order_id"]})
                continue
            price = await self.market.price(symbol)
            if price is None:
                sold.append({"symbol": symbol, "skipped": "stale price"})
                continue
            filters = await self.market.filters(symbol)
            if filters is None:
                sold.append({"symbol": symbol, "skipped": "no filters"})
                continue
            from ..position_sizing import round_down_to_step

            qty = round_down_to_step(float(free), filters.step_size)
            if qty < filters.min_qty:
                sold.append({"symbol": symbol, "skipped": "below min_qty"})
                continue
            result = await self._place_and_record(
                account, True, symbol, "SELL", "MARKET", qty, None, qty * price, price,
                idem, 0.0, actor,
            )
            sold.append({"symbol": symbol, "quantity": result["quantity"],
                         "status": result["status"], "order_id": result["order_id"]})

        return {"account_id": account_id, "closed": not cancel_errors, "mode": "real",
                "cancelled": cancelled, "cancel_errors": cancel_errors, "sold": sold}

    # ---------- exposure + audit (3.5) ----------

    async def _exposure_by_symbol(self, account_id: str) -> dict[str, float]:
        """Hesabın sembol bazlı exposure'ı: açık emir notional + base bakiye değeri."""

        def _open(conn: sqlite3.Connection) -> dict[str, float]:
            rows = conn.execute(
                "SELECT symbol, SUM(notional) AS n FROM orders "
                "WHERE account_id = ? AND status IN ('NEW', 'PARTIALLY_FILLED') GROUP BY symbol",
                (account_id,),
            ).fetchall()
            return {str(r["symbol"]): float(r["n"]) for r in rows}

        by_symbol = await self.db.read(_open)
        balances = await self.broker.get_balance(account_id=account_id)
        for asset, free in balances.items():
            if asset == "USDT":
                continue
            symbol = f"{asset}USDT"
            price = await self.market.price(symbol)
            if price is None:
                continue
            by_symbol[symbol] = by_symbol.get(symbol, 0.0) + free * price
        return by_symbol

    async def get_total_exposure(self) -> dict[str, Any]:
        """Tüm hesapların toplam exposure'ı: sembol bazlı risk görünümü."""
        accounts = (await self.accounts.list_accounts())["accounts"]
        by_symbol: dict[str, float] = {}
        per_account: dict[str, float] = {}
        for account in accounts:
            try:
                per = await self._exposure_by_symbol(account["account_id"])
            except Exception:  # noqa: BLE001
                continue
            per_account[account["account_id"]] = sum(per.values())
            for symbol, value in per.items():
                by_symbol[symbol] = by_symbol.get(symbol, 0.0) + value
        ordered = dict(sorted(by_symbol.items(), key=lambda kv: -kv[1]))
        return {
            "total": sum(by_symbol.values()),
            "by_symbol": ordered,
            "per_account": per_account,
            "account_count": len(accounts),
        }

    async def get_audit_log(self, *, limit: int = 50) -> dict[str, Any]:
        """Hash-chain doğrulamalı audit log sorgusu (3.5)."""
        if self.audit is None:
            raise RasatError(ErrorCode.NOT_IMPLEMENTED, "audit log bu bağlamda yok")
        if not isinstance(limit, int) or limit < 1 or limit > 500:
            raise RasatError(ErrorCode.INVALID_REQUEST, "limit 1-500 arası olmalı")
        broken = await self.audit.verify()
        tail = await self.audit.tail(limit)
        return {
            "verified": len(broken) == 0,
            "broken": broken,
            "tail": tail,
            "count": len(tail),
        }
