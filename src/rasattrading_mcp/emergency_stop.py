"""`emergency_stop` — daemon'dan bağımsız acil durdurma (ticket 3.6).

Daemon'a ihtiyaç duymadan:
1. Şifreli credential'ları doğrudan SQLite + SecretStore ile çözer.
2. Binance REST'e direkt bağlanır: tüm açık emirleri iptal eder,
   spot'ta elde tutulan base asset bakiyelerini market fiyatından satar.
3. Kendi append-only hash-chain log'una yazar (`EmergencyLog`).
4. İdempotenttir: "tüm bakiyeyi sat" kapsamı çalıştırılmadan önce hangi
   sembol/miktarın satılacağı gösterilir ve onay istenir; headless senaryoda
   önceden verilmiş `yes` bayrağıyla çalışır.

Fiyat kaynağı (ticket 3.7): script `daemon`/`data` pipeline'ına bağımlı olmadan
imzasız Binance public `/api/v3/ticker/price` uç noktasını kendi kullanır
(`PublicPriceSource`). Fiyat alınamazsa o asset `price_errors` içinde açıkça
raporlanır — sessizce atlanmaz — ve hiçbir şey satılamadıysa `ok: True` dönülmez.
"""

from __future__ import annotations

import asyncio
import logging
import math
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import aiohttp

from .config import Config
from .data.order_broker import OrderBroker
from .errors import ErrorCode, RasatError
from .position_sizing import SymbolFilters
from .storage.accounts import AccountService
from .storage.emergency_log import EmergencyLog
from .storage.credentials import SecretStore

logger = logging.getLogger("rasattrading.emergency_stop")

SELL_LOG_ACTION = "emergency_sell"
CANCEL_LOG_ACTION = "emergency_cancel"

_FILTERS_TTL_SECONDS = 3600.0

# T03: bu durumlarla biten bir emergency satış non-terminaldir — pozisyon kesin
# kapanmamıştır, broker'dan reconcile gerektirir ve `ok` asla True olamaz.
_NON_TERMINAL_SELL_STATUSES = ("NEW", "PARTIALLY_FILLED", "UNKNOWN")


def _mark_sell(entry: dict, status: str, **extra: Any) -> dict:
    """Satış sonucuna T03 pending/reconcile etiketini ekler.

    NEW/PARTIALLY_FILLED/UNKNOWN non-terminaldir: `pending=True` ve
    `reconcile_required=True`. DRY_RUN/FILLED terminal-ok'tur; diğer tüm
    durumlar (FAILED/CANCELED/REJECTED/EXPIRED) pending değildir ama `ok`
    mantığında "kesin FILLED" sayılmaz.
    """
    pending = status in _NON_TERMINAL_SELL_STATUSES
    return {**entry, "status": status, "pending": pending,
            "reconcile_required": pending, **extra}


class PublicPriceSource:
    """Binance public `/api/v3/ticker/price` + `/api/v3/exchangeInfo` — imzasız,
    daemon/pipeline'dan bağımsız fiyat/filtre kaynağı (ticket 3.7, 3.16).

    `EmergencyStopRunner.market_price` arayüzünü karşılar (`price` + `filters`).
    Fiyat alınamıyorsa `None` dönmez, `RasatError` fırlatır — böylece çağıran
    (EmergencyStopRunner) asset'i `price_errors` içinde fail-loud raporlar.
    3.16: `filters` imzasız public exchangeInfo'dan çekilir ve cache'lenir;
    filtre alınamazsa runner fail-closed davranır (ham miktar asla gönderilmez).
    Fiyat finite/pozitif değilse reddedilir.
    """

    def __init__(
        self,
        base_url: str,
        *,
        session: aiohttp.ClientSession | None = None,
        timeout_seconds: float = 20.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._session = session
        self._own_session = session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._filters_cache: dict[str, tuple[float, SymbolFilters]] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def price(self, symbol: str) -> float:
        session = await self._get_session()
        try:
            async with session.get(
                f"{self.base_url}/api/v3/ticker/price",
                params={"symbol": symbol},
                timeout=self._timeout,
            ) as resp:
                if resp.status == 400:
                    raise RasatError(ErrorCode.INVALID_SYMBOL, f"fiyat kaynağı sembolü tanımıyor: {symbol}")
                resp.raise_for_status()
                data = await resp.json()
        except RasatError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"fiyat kaynağına ulaşılamadı ({symbol}): {exc}") from exc
        if not isinstance(data, dict) or "price" not in data:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"fiyat kaynağı geçersiz yanıt ({symbol})")
        try:
            price = float(data["price"])
        except (TypeError, ValueError):
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"fiyat kaynağı geçersiz fiyat ({symbol})") from None
        # 3.16/M2: finite ve pozitif olmayan fiyat asla kabul edilmez.
        if not math.isfinite(price) or price <= 0:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"fiyat kaynağı finite/pozitif olmayan fiyat ({symbol})")
        return price

    async def _exchange_info(self) -> dict[str, dict]:
        """Public exchangeInfo'yu çeker; sembol → ham entry dict döner (imzasız)."""
        session = await self._get_session()
        try:
            async with session.get(f"{self.base_url}/api/v3/exchangeInfo", timeout=self._timeout) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"exchangeInfo alınamadı: {exc}") from exc
        if not isinstance(data, dict) or not isinstance(data.get("symbols"), list):
            raise RasatError(ErrorCode.INTERNAL_ERROR, "exchangeInfo geçersiz yanıt")
        return {str(e.get("symbol", "")): e for e in data["symbols"] if e.get("symbol")}

    async def filters(self, symbol: str) -> SymbolFilters | None:
        now = time.time()
        cached = self._filters_cache.get(symbol)
        if cached is not None and now - cached[0] < _FILTERS_TTL_SECONDS:
            return cached[1]
        try:
            info = await self._exchange_info()
        except RasatError:
            info = {}
        entry = info.get(symbol)
        if entry is None:
            return None
        filters = SymbolFilters.from_exchange_info(entry)
        self._filters_cache[symbol] = (now, filters)
        return filters

    async def close(self) -> None:
        if self._own_session and self._session is not None and not self._session.closed:
            await self._session.close()


class EmergencyStopRunner:
    """Daemon'dan bağımsız, doğrudan Binance'e bağlanan acil durdurma."""

    def __init__(
        self,
        config: Config,
        accounts: AccountService,
        broker: OrderBroker,
        log: EmergencyLog,
        *,
        market_price: Any | None = None,
        quote_asset: str = "USDT",
    ) -> None:
        self.config = config
        self.accounts = accounts
        self.broker = broker
        self.log = log
        self.market_price = market_price  # async (symbol) -> float | None; yoksa balance snapshot kullanılır
        self.quote_asset = quote_asset

    async def run(
        self,
        *,
        account_ids: list[str] | None = None,
        yes: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Tüm hedeflerde açık emirleri iptal eder ve bakiyeleri satar."""
        accounts = await self._resolve_accounts(account_ids)
        results = []
        for account in accounts:
            account_id = account["account_id"]
            try:
                results.append(await self._stop_one(account, yes=yes, dry_run=dry_run))
            except RasatError as exc:
                results.append(
                    {"account_id": account_id, "ok": False, "error": {"code": exc.code, "message": exc.message}}
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("emergency stop başarısız: %s", account_id)
                results.append(
                    {"account_id": account_id, "ok": False,
                     "error": {"code": ErrorCode.INTERNAL_ERROR, "message": str(exc)}}
                )

        all_ok = all(r.get("ok") for r in results)
        return {"ok": all_ok, "results": results, "count": len(results)}

    async def _resolve_accounts(self, account_ids: list[str] | None) -> list[dict]:
        listed = await self.accounts.list_accounts()
        accounts = listed["accounts"]
        if account_ids:
            wanted = set(account_ids)
            missing = [a for a in wanted if not any(acc["account_id"] == a for acc in accounts)]
            if missing:
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account bulunamadı: {missing[0]}")
            accounts = [acc for acc in accounts if acc["account_id"] in wanted]
        return accounts

    async def _stop_one(self, account: dict, *, yes: bool, dry_run: bool) -> dict:
        account_id = account["account_id"]
        # Credential'ları doğrula (public hesabı atla)
        try:
            await self.accounts.get_credentials(account_id)
        except RasatError as exc:
            if exc.code == ErrorCode.ACCOUNT_NO_CREDENTIALS:
                return {"account_id": account_id, "ok": False,
                        "error": {"code": ErrorCode.ACCOUNT_NO_CREDENTIALS, "message": "credential yok — atlandı"}}
            raise

        # 1) Açık emirleri iptal et — 3.15: done anahtarı hesap+sembol değil,
        # HESAP+SEMBOL+EMİR KİMLİĞİ. Yeni bir açık emir (yeni client_order_id)
        # eski cancel log'una takılıp atlanmaz, gerçekten iptal edilir.
        open_orders = await self.broker.get_all_open_orders(account_id=account_id)
        by_symbol: dict[str, list[dict]] = {}
        for o in open_orders:
            by_symbol.setdefault(o.get("symbol") or "", []).append(o)
        cancelled = 0
        cancel_errors: list[dict] = []
        for symbol, orders in sorted(by_symbol.items()):
            if dry_run:
                continue
            keys = [f"{account_id}:{symbol}:{o.get('client_order_id') or o.get('order_id')}" for o in orders]
            if all(self.log.is_action_done(CANCEL_LOG_ACTION, k) for k in keys):
                continue
            try:
                n = await self.broker.cancel_all_open_orders(account_id=account_id, symbol=symbol)
            except RasatError as exc:
                # T03: iptal hatası `ok`'u bozar; raporlanır, kalan sembollerde
                # satışa devam edilir. İptal log'lanmadığı için sonraki koşu yeniden dener.
                cancel_errors.append({"symbol": symbol, "error": {"code": exc.code, "message": exc.message}})
                continue
            cancelled += n
            for k in keys:
                self.log.append(
                    actor="emergency_stop",
                    action=CANCEL_LOG_ACTION,
                    details={"idem_key": k, "account_id": account_id, "symbol": symbol, "cancelled": n},
                )

        # 2) Base asset bakiyelerini topla → satış planı (3.16: filtre fail-closed)
        balances = await self.broker.get_balance(account_id=account_id)
        plan: list[dict] = []
        price_errors: list[dict] = []
        filter_errors: list[dict] = []
        for asset, free in balances.items():
            if asset == self.quote_asset or free <= 0:
                continue
            symbol = f"{asset}{self.quote_asset}"
            filters = await self._symbol_filters(symbol)
            if filters is None:
                # 3.16: filtre bilgisi yoksa ham miktar ASLA gönderilmez (fail-closed)
                filter_errors.append(
                    {"symbol": symbol, "asset": asset,
                     "error": {"code": ErrorCode.FILTER_VIOLATION,
                               "message": "filtre bilgisi alınamadı — satış yapılmadı"}}
                )
                continue
            qty = self._round_down(free, filters.step_size)
            if qty < filters.min_qty:
                filter_errors.append(
                    {"symbol": symbol, "asset": asset,
                     "error": {"code": ErrorCode.FILTER_VIOLATION,
                               "message": f"miktar LOT_SIZE minQty altında: {qty} < {filters.min_qty}"}}
                )
                continue
            try:
                price = await self._price(symbol, filters)
            except RasatError as exc:
                # Fail-loud (3.7): fiyat alınamayan asset sessizce atlanmaz,
                # sonuç listesinde açıkça error olarak raporlanır.
                price_errors.append(
                    {"symbol": symbol, "asset": asset,
                     "error": {"code": exc.code, "message": exc.message}}
                )
                continue
            if qty * price < filters.min_notional:
                filter_errors.append(
                    {"symbol": symbol, "asset": asset,
                     "error": {"code": ErrorCode.FILTER_VIOLATION,
                               "message": f"tutar MIN_NOTIONAL altında: {qty * price:.6g} < {filters.min_notional}"}}
                )
                continue
            plan.append({"symbol": symbol, "asset": asset, "quantity": qty, "price": price,
                         "notional": qty * price})

        # Onay: hangi sembol/miktar satılacağı gösterilir (headless: yes bayrağı)
        if plan and not yes and not dry_run:
            # T4: stdin yoksa/kapalıysa (headless/CI) input() EOFError/OSError
            # fırlatır ve genel except'e iç hata olarak sızar. Onay akışına
            # girmeden önce fail-closed, sabit bir hata döndürülür (--yes gerekir).
            if getattr(sys.stdin, "closed", False):
                return {"account_id": account_id, "ok": False,
                        "error": {"code": "CONFIRMATION_REQUIRED",
                                  "message": "confirmation required, use --yes for non-interactive"},
                        "plan": plan}
            lines = [f"  {p['symbol']}: {p['quantity']} @ ~{p['price']:.6g} ≈ {p['notional']:.4g} {self.quote_asset}"
                     for p in plan]
            print(f"EMERGENCY STOP — {account_id} şunları satacak:")
            print("\n".join(lines))
            try:
                answer = input("Onaylıyor musun? [y/N]: ").strip().lower()
            except (EOFError, OSError):
                # T4: kapalı/okunamaz stdin → iç exception sızdırılmaz (fail-closed).
                return {"account_id": account_id, "ok": False,
                        "error": {"code": "CONFIRMATION_REQUIRED",
                                  "message": "confirmation required, use --yes for non-interactive"},
                        "plan": plan}
            if answer not in ("y", "yes"):
                return {"account_id": account_id, "ok": False,
                        "error": {"code": "ABORTED", "message": "kullanıcı onaylamadı"},
                        "plan": plan}

        # 3) Sat — 3.15: done anahtarı MİKTAR + run nonce'ı içerir; FILLED olmayan
        # (NEW/PARTIALLY_FILLED/UNKNOWN) satış "done" sayılmaz, broker'dan gerçek
        # durum sorgulanır (reconcile-before-resend). Aynı miktarın rebuy'ı dahi
        # yeni run nonce'ı ile yeni SELL üretir.
        run_nonce = uuid.uuid4().hex[:8]
        sold = []
        for p in plan:
            if dry_run:
                sold.append(_mark_sell(p, "DRY_RUN"))
                continue
            sell_key = f"{account_id}:{p['symbol']}:{p['quantity']}:{run_nonce}"
            # T4: cid, sembolü de içerir — aynı run'da birden fazla base asset
            # satılırken (örn. BTC+ETH) her SELL ayrı clientOrderId alır; Binance
            # clientOrderId çakışması ikinci emrin reddine yol açabilir. Uzunluk
            # 36 char sınırının altındadır (e- + 6 + - + 8 + - + 8 = 26 max).
            cid = f"e-{account_id[:6]}-{run_nonce}-{p['symbol'][:8]}"
            # Bu sembolde işlemde kalmış bir emergency sell var mı?
            prior = self._latest_sell_details(account_id, p["symbol"])
            if prior is not None and prior.get("status") in _NON_TERMINAL_SELL_STATUSES:
                prior_cid = prior.get("cid") or f"e-{account_id[:6]}-{prior.get('run_nonce') or ''}-{p['symbol'][:8]}"
                try:
                    found = await self.broker.query_order(
                        account_id=account_id, symbol=p["symbol"], client_order_id=prior_cid,
                    )
                except RasatError:
                    found = None
                if found is not None and found.status in _NON_TERMINAL_SELL_STATUSES:
                    sold.append(_mark_sell(p, found.status, exchange_order_id=found.exchange_order_id, cid=prior_cid))
                    continue
                # FILLED döndüyse önceki satış dolu; mevcut bakiye yeniden alınmış
                # olabilir → yeni SELL yerleştir. Not found → önceki hiç gitmemiş → yeni SELL.
            try:
                result = await self.broker.place_order(
                    account_id=account_id,
                    symbol=p["symbol"],
                    side="SELL",
                    order_type="MARKET",
                    quantity=p["quantity"],
                    price=None,
                    client_order_id=cid,
                )
                self.log.append(
                    actor="emergency_stop",
                    action=SELL_LOG_ACTION,
                    details={
                        "idem_key": sell_key,
                        "cid": cid,
                        "run_nonce": run_nonce,
                        "account_id": account_id,
                        "symbol": p["symbol"],
                        "quantity": p["quantity"],
                        "status": result.status,
                        "exchange_order_id": result.exchange_order_id,
                    },
                )
                sold.append(_mark_sell(p, result.status, exchange_order_id=result.exchange_order_id, cid=cid))
            except RasatError as exc:
                sold.append({**p, "status": "FAILED", "pending": False, "reconcile_required": False,
                             "error": {"code": exc.code, "message": exc.message}})

        # T03: `ok` yalnızca hiçbir hata/iptal hatası yoksa VE plan içindeki tüm
        # satışlar kesin FILLED ise true. NEW/PARTIALLY_FILLED/UNKNOWN dahil her
        # dolmayan durum (FAILED/CANCELED/REJECTED/EXPIRED) ok'u bozar. Plan boşsa
        # (satılacak yok) hata yokluğunda mevcut no-position ok=True semantiği korunur.
        sell_not_filled = any(s.get("status") not in ("FILLED", "DRY_RUN") for s in sold)
        pending_sells = [s for s in sold if s.get("pending")]
        return {
            "account_id": account_id,
            "ok": not price_errors and not filter_errors and not cancel_errors and not sell_not_filled,
            "cancelled_orders": cancelled,
            "cancel_errors": cancel_errors,
            "sold": sold,
            "pending_count": len(pending_sells),
            "pending": pending_sells,
            "reconcile_required": bool(pending_sells),
            "price_errors": price_errors,
            "filter_errors": filter_errors,
            "plan": plan if dry_run else None,
        }

    def _latest_sell_details(self, account_id: str, symbol: str) -> dict | None:
        """Bu sembol için en son loglanmış emergency sell kaydı (yoksa None)."""
        prefix = f"{account_id}:{symbol}:"
        latest: dict | None = None
        for row in self.log.entries():
            if row.get("_corrupt"):
                continue
            details = row.get("details") or {}
            if row.get("action") != SELL_LOG_ACTION:
                continue
            if not str(details.get("idem_key", "")).startswith(prefix):
                continue
            latest = details
        return latest

    async def _symbol_filters(self, symbol: str) -> SymbolFilters | None:
        if self.market_price is not None and hasattr(self.market_price, "filters"):
            return await self.market_price.filters(symbol)
        return None

    async def _price(self, symbol: str, filters: SymbolFilters | None) -> float:
        if self.market_price is None:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"fiyat kaynağı yok — satış planlanamıyor: {symbol}")
        if not hasattr(self.market_price, "price"):
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"fiyat kaynağı geçersiz arayüz: {symbol}")
        price = await self.market_price.price(symbol)
        if price is None:
            raise RasatError(ErrorCode.STALE_DATA, f"fiyat alınamadı: {symbol}")
        return float(price)

    @staticmethod
    def _round_down(value: float, step: float) -> float:
        if step <= 0:
            return value
        import math

        return math.floor(value / step + 1e-9) * step


async def run_emergency_stop(
    *,
    data_dir: Path,
    account_ids: list[str] | None = None,
    yes: bool = False,
    dry_run: bool = False,
    broker: OrderBroker | None = None,
    market_price: Any | None = None,
    config: Config | None = None,
) -> dict[str, Any]:
    """Kendi DB/secret/log bağlamını kurar; daemon'a ihtiyaç duymaz.

    `market_price` verilmezse (üretim yolu) bağımsız `PublicPriceSource` ile
    Binance public `/api/v3/ticker/price` kullanılır (ticket 3.7).
    """
    config = config or Config(data_dir=data_dir)
    from .data.binance_client import BinanceREST
    from .data.order_broker import BinanceOrderBroker
    from .data.rate_limit import RateLimitBudget
    from .storage.db import Database
    from .storage.migrations import run_migrations

    db = Database(config.db_path)
    await db.start()
    try:
        await run_migrations(db)
        accounts = AccountService(db, secret_store=SecretStore())
        budget = RateLimitBudget(max_weight=config.rate_limit_max_weight, window_seconds=60)
        owned_broker = broker is None
        if broker is None:
            broker = BinanceOrderBroker(
                config.rest_spot_base,
                credentials=lambda account_id: accounts.get_credentials(account_id),
                budget=budget,
            )
        owned_price = market_price is None
        if market_price is None:
            market_price = PublicPriceSource(config.rest_spot_base)
        log = EmergencyLog(config.data_dir / "emergency_stop.log")
        runner = EmergencyStopRunner(
            config, accounts, broker, log, market_price=market_price
        )
        try:
            return await runner.run(account_ids=account_ids, yes=yes, dry_run=dry_run)
        finally:
            # 3.16/M2: runner'ın kendi açtığı HTTP session'ları kapat (sızıntı yok).
            if owned_price and market_price is not None and hasattr(market_price, "close"):
                await market_price.close()
            if owned_broker and broker is not None and hasattr(broker, "close"):
                await broker.close()
    finally:
        await db.stop()


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="rasattrading-emergency-stop",
                                     description="Daemon'dan bağımsız acil durdurma")
    parser.add_argument("--data-dir", help="veri dizini (varsayılan: ~/.rasattrading)")
    parser.add_argument("--account-id", action="append", help="hedef hesap (tekrar edilebilir); yoksa tümü")
    parser.add_argument("--yes", action="store_true", help="satış onayını otomatik ver (headless)")
    parser.add_argument("--dry-run", action="store_true", help="sadece planı göster, hiçbir şey gönderme")
    args = parser.parse_args(argv)

    overrides = {"data_dir": Path(args.data_dir)} if args.data_dir else {}
    cfg = Config.from_env(overrides)
    result = asyncio.run(
        run_emergency_stop(data_dir=cfg.data_dir, account_ids=args.account_id,
                           yes=args.yes, dry_run=args.dry_run)
    )
    print(result)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
