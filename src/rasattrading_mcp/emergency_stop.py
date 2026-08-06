"""`emergency_stop` — daemon'dan bağımsız acil durdurma (ticket 3.6).

Daemon'a ihtiyaç duymadan:
1. Şifreli credential'ları doğrudan SQLite + SecretStore ile çözer.
2. Binance REST'e direkt bağlanır: tüm açık emirleri iptal eder,
   spot'ta elde tutulan base asset bakiyelerini market fiyatından satar.
3. Kendi append-only hash-chain log'una yazar (`EmergencyLog`).
4. İdempotenttir: "tüm bakiyeyi sat" kapsamı çalıştırılmadan önce hangi
   sembol/miktarın satılacağı gösterilir ve onay istenir; headless senaryoda
   önceden verilmiş `yes` bayrağıyla çalışır.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

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

        # 1) Açık emirleri iptal et
        open_orders = await self.broker.get_all_open_orders(account_id=account_id)
        symbols_with_orders = {o["symbol"] for o in open_orders if o.get("symbol")}
        cancelled = 0
        for symbol in sorted(symbols_with_orders):
            if dry_run:
                continue
            if self.log.is_action_done(CANCEL_LOG_ACTION, f"{account_id}:{symbol}"):
                continue
            n = await self.broker.cancel_all_open_orders(account_id=account_id, symbol=symbol)
            cancelled += n
            self.log.append(
                actor="emergency_stop",
                action=CANCEL_LOG_ACTION,
                details={"idem_key": f"{account_id}:{symbol}", "account_id": account_id, "symbol": symbol, "cancelled": n},
            )

        # 2) Base asset bakiyelerini topla → satış planı
        balances = await self.broker.get_balance(account_id=account_id)
        plan: list[dict] = []
        for asset, free in balances.items():
            if asset == self.quote_asset or free <= 0:
                continue
            symbol = f"{asset}{self.quote_asset}"
            filters = await self._symbol_filters(symbol)
            qty = self._round_down(free, filters.step_size if filters else 0)
            if filters is not None and qty < filters.min_qty:
                continue
            price = await self._price(symbol, filters)
            if price is None:
                continue
            plan.append({"symbol": symbol, "asset": asset, "quantity": qty, "price": price,
                         "notional": qty * price})

        # Onay: hangi sembol/miktar satılacağı gösterilir (headless: yes bayrağı)
        if plan and not yes and not dry_run:
            lines = [f"  {p['symbol']}: {p['quantity']} @ ~{p['price']:.6g} ≈ {p['notional']:.4g} {self.quote_asset}"
                     for p in plan]
            print(f"EMERGENCY STOP — {account_id} şunları satacak:")
            print("\n".join(lines))
            answer = input("Onaylıyor musun? [y/N]: ").strip().lower()
            if answer not in ("y", "yes"):
                return {"account_id": account_id, "ok": False,
                        "error": {"code": "ABORTED", "message": "kullanıcı onaylamadı"},
                        "plan": plan}

        sold = []
        for p in plan:
            if dry_run:
                sold.append({**p, "status": "DRY_RUN"})
                continue
            if self.log.is_action_done(SELL_LOG_ACTION, f"{account_id}:{p['symbol']}"):
                sold.append({**p, "status": "SKIPPED_DONE"})
                continue
            try:
                result = await self.broker.place_order(
                    account_id=account_id,
                    symbol=p["symbol"],
                    side="SELL",
                    order_type="MARKET",
                    quantity=p["quantity"],
                    price=None,
                    client_order_id=f"emergency-{account_id[:8]}-{p['symbol']}",
                )
                self.log.append(
                    actor="emergency_stop",
                    action=SELL_LOG_ACTION,
                    details={
                        "idem_key": f"{account_id}:{p['symbol']}",
                        "account_id": account_id,
                        "symbol": p["symbol"],
                        "quantity": p["quantity"],
                        "status": result.status,
                        "exchange_order_id": result.exchange_order_id,
                    },
                )
                sold.append({**p, "status": result.status, "exchange_order_id": result.exchange_order_id})
            except RasatError as exc:
                sold.append({**p, "status": "FAILED", "error": {"code": exc.code, "message": exc.message}})

        return {
            "account_id": account_id,
            "ok": True,
            "cancelled_orders": cancelled,
            "sold": sold,
            "plan": plan if dry_run else None,
        }

    async def _symbol_filters(self, symbol: str) -> SymbolFilters | None:
        if self.market_price is not None and hasattr(self.market_price, "filters"):
            return await self.market_price.filters(symbol)
        return None

    async def _price(self, symbol: str, filters: SymbolFilters | None) -> float | None:
        if self.market_price is not None and hasattr(self.market_price, "price"):
            return await self.market_price.price(symbol)
        return None

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
    """Kendi DB/secret/log bağlamını kurar; daemon'a ihtiyaç duymaz."""
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
        rest = BinanceREST(config.rest_spot_base, budget)
        if broker is None:
            broker = BinanceOrderBroker(
                config.rest_spot_base,
                credentials=lambda account_id: accounts.get_credentials(account_id),
                budget=budget,
            )
        log = EmergencyLog(config.data_dir / "emergency_stop.log")
        runner = EmergencyStopRunner(
            config, accounts, broker, log, market_price=market_price
        )
        return await runner.run(account_ids=account_ids, yes=yes, dry_run=dry_run)
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
