"""`emergency_stop` — daemon-independent emergency stop (ticket 3.6).

Without requiring the daemon:
1. Decrypt encrypted credentials directly through SQLite + SecretStore.
2. Connect directly to Binance REST: cancel all open orders and sell held
   base-asset spot balances at market price.
3. Write to its own append-only hash-chain log (`EmergencyLog`).
4. It is idempotent: before the "sell all balances" scope runs, show which
   symbols/quantities will be sold and request confirmation; in headless scenarios,
   run with the pre-supplied `yes` flag.

Price source (ticket 3.7): without depending on the `daemon`/`data` pipeline, the
script uses Binance's unsigned public `/api/v3/ticker/price` endpoint itself
(`PublicPriceSource`). If a price cannot be obtained, report that asset explicitly
in `price_errors` rather than silently skipping it, and do not return `ok: True`
when nothing could be sold.
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

# T03: an emergency sale ending in one of these states is non-terminal — the
# position is not confirmed closed, requires broker reconciliation, and `ok` can never be True.
_NON_TERMINAL_SELL_STATUSES = ("NEW", "PARTIALLY_FILLED", "UNKNOWN")


def _mark_sell(entry: dict, status: str, **extra: Any) -> dict:
    """Add the T03 pending/reconciliation labels to a sale result.

    NEW/PARTIALLY_FILLED/UNKNOWN are non-terminal: `pending=True` and
    `reconcile_required=True`. DRY_RUN/FILLED are terminal-ok; all other states
    (FAILED/CANCELED/REJECTED/EXPIRED) are not pending but do not count as
    "definitely FILLED" for `ok`.
    """
    pending = status in _NON_TERMINAL_SELL_STATUSES
    return {**entry, "status": status, "pending": pending,
            "reconcile_required": pending, **extra}


class PublicPriceSource:
    """Binance public `/api/v3/ticker/price` + `/api/v3/exchangeInfo` — unsigned,
    daemon/pipeline-independent price/filter source (tickets 3.7, 3.16).

    Implements the `EmergencyStopRunner.market_price` interface (`price` + `filters`).
    If a price is unavailable, raise `RasatError` instead of returning `None`, so
    the caller (EmergencyStopRunner) reports the asset loudly in `price_errors`.
    3.16: fetch and cache `filters` from unsigned public exchangeInfo; if filters
    cannot be obtained, the runner fails closed (never sends a raw quantity).
    Reject non-finite or non-positive prices.
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
                    raise RasatError(ErrorCode.INVALID_SYMBOL, f"price source does not recognize symbol: {symbol}")
                resp.raise_for_status()
                data = await resp.json()
        except RasatError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"could not reach price source ({symbol}): {exc}") from exc
        if not isinstance(data, dict) or "price" not in data:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"invalid price-source response ({symbol})")
        try:
            price = float(data["price"])
        except (TypeError, ValueError):
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"invalid price from price source ({symbol})") from None
        # 3.16/M2: never accept a non-finite or non-positive price.
        if not math.isfinite(price) or price <= 0:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"price source returned a non-finite/non-positive price ({symbol})")
        return price

    async def _exchange_info(self) -> dict[str, dict]:
        """Fetch public exchangeInfo and return symbol → raw entry dict (unsigned)."""
        session = await self._get_session()
        try:
            async with session.get(f"{self.base_url}/api/v3/exchangeInfo", timeout=self._timeout) as resp:
                resp.raise_for_status()
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"could not fetch exchangeInfo: {exc}") from exc
        if not isinstance(data, dict) or not isinstance(data.get("symbols"), list):
            raise RasatError(ErrorCode.INTERNAL_ERROR, "invalid exchangeInfo response")
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
    """Daemon-independent emergency stop that connects directly to Binance."""

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
        self.market_price = market_price  # async (symbol) -> float | None; otherwise use the balance snapshot
        self.quote_asset = quote_asset

    async def run(
        self,
        *,
        account_ids: list[str] | None = None,
        yes: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Cancel open orders and sell balances for all targets."""
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
                logger.exception("emergency stop failed: %s", account_id)
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
                raise RasatError(ErrorCode.ACCOUNT_NOT_FOUND, f"account not found: {missing[0]}")
            accounts = [acc for acc in accounts if acc["account_id"] in wanted]
        return accounts

    async def _stop_one(self, account: dict, *, yes: bool, dry_run: bool) -> dict:
        account_id = account["account_id"]
        # Validate credentials (skip public accounts).
        try:
            await self.accounts.get_credentials(account_id)
        except RasatError as exc:
            if exc.code == ErrorCode.ACCOUNT_NO_CREDENTIALS:
                return {"account_id": account_id, "ok": False,
                        "error": {"code": ErrorCode.ACCOUNT_NO_CREDENTIALS, "message": "no credentials — skipped"}}
            raise

        # 1) Cancel open orders — 3.15: the done key is ACCOUNT+SYMBOL+ORDER ID,
        # not account+symbol. A new open order (new client_order_id) is not skipped
        # because of an old cancel log; it is actually canceled.
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
                # T03: a cancel error makes `ok` false and is reported; continue selling
                # other symbols. Because it is not logged as canceled, the next run retries.
                cancel_errors.append({"symbol": symbol, "error": {"code": exc.code, "message": exc.message}})
                continue
            cancelled += n
            for k in keys:
                self.log.append(
                    actor="emergency_stop",
                    action=CANCEL_LOG_ACTION,
                    details={"idem_key": k, "account_id": account_id, "symbol": symbol, "cancelled": n},
                )

        # 2) Collect base-asset balances → sale plan (3.16: fail closed on filters).
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
                # 3.16: never send a raw quantity when filter information is missing (fail closed).
                filter_errors.append(
                    {"symbol": symbol, "asset": asset,
                     "error": {"code": ErrorCode.FILTER_VIOLATION,
                               "message": "could not obtain filter information — sale not placed"}}
                )
                continue
            qty = self._round_down(free, filters.step_size)
            if qty < filters.min_qty:
                filter_errors.append(
                    {"symbol": symbol, "asset": asset,
                     "error": {"code": ErrorCode.FILTER_VIOLATION,
                               "message": f"quantity is below LOT_SIZE minQty: {qty} < {filters.min_qty}"}}
                )
                continue
            try:
                price = await self._price(symbol, filters)
            except RasatError as exc:
                # Fail loud (3.7): do not silently skip an asset whose price is
                # unavailable; report it explicitly as an error in the result list.
                price_errors.append(
                    {"symbol": symbol, "asset": asset,
                     "error": {"code": exc.code, "message": exc.message}}
                )
                continue
            if qty * price < filters.min_notional:
                filter_errors.append(
                    {"symbol": symbol, "asset": asset,
                     "error": {"code": ErrorCode.FILTER_VIOLATION,
                               "message": f"notional is below MIN_NOTIONAL: {qty * price:.6g} < {filters.min_notional}"}}
                )
                continue
            plan.append({"symbol": symbol, "asset": asset, "quantity": qty, "price": price,
                         "notional": qty * price})

        # Confirmation: show which symbols/quantities will be sold (headless: yes flag).
        if plan and not yes and not dry_run:
            # T4: when stdin is absent/closed (headless/CI), input() raises
            # EOFError/OSError and would leak as an internal error through the
            # generic except. Fail closed with a fixed error before confirmation (--yes required).
            if getattr(sys.stdin, "closed", False):
                return {"account_id": account_id, "ok": False,
                        "error": {"code": "CONFIRMATION_REQUIRED",
                                  "message": "confirmation required, use --yes for non-interactive"},
                        "plan": plan}
            lines = [f"  {p['symbol']}: {p['quantity']} @ ~{p['price']:.6g} ≈ {p['notional']:.4g} {self.quote_asset}"
                     for p in plan]
            print(f"EMERGENCY STOP — {account_id} will sell:")
            print("\n".join(lines))
            try:
                answer = input("Confirm? [y/N]: ").strip().lower()
            except (EOFError, OSError):
                # T4: closed/unreadable stdin → do not leak an internal exception (fail closed).
                return {"account_id": account_id, "ok": False,
                        "error": {"code": "CONFIRMATION_REQUIRED",
                                  "message": "confirmation required, use --yes for non-interactive"},
                        "plan": plan}
            if answer not in ("y", "yes"):
                return {"account_id": account_id, "ok": False,
                        "error": {"code": "ABORTED", "message": "user did not confirm"},
                        "plan": plan}

        # 3) Sell — 3.15: the done key includes QUANTITY + run nonce; a non-FILLED
        # (NEW/PARTIALLY_FILLED/UNKNOWN) sale is not done, and the broker's real state
        # is queried (reconcile-before-resend). Even a rebuy of the same quantity
        # produces a new SELL with a new run nonce.
        run_nonce = uuid.uuid4().hex[:8]
        sold = []
        for p in plan:
            if dry_run:
                sold.append(_mark_sell(p, "DRY_RUN"))
                continue
            sell_key = f"{account_id}:{p['symbol']}:{p['quantity']}:{run_nonce}"
            # T4: cid also includes the symbol. When selling multiple base assets
            # in one run (e.g. BTC+ETH), each SELL gets a distinct clientOrderId;
            # a Binance clientOrderId collision could reject the second order.
            # Length remains under the 36-character limit (e- + 6 + - + 8 + - + 8 = 26 max).
            cid = f"e-{account_id[:6]}-{run_nonce}-{p['symbol'][:8]}"
            # Is there an emergency sell still in progress for this symbol?
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
                # If FILLED, the previous sale completed and the current balance may
                # have been rebought → place a new SELL. Not found → the previous
                # order never arrived → place a new SELL.
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

        # T03: `ok` is true only when there are no errors/cancel errors AND all
        # sales in the plan are definitely FILLED. Every non-filled state, including
        # NEW/PARTIALLY_FILLED/UNKNOWN and FAILED/CANCELED/REJECTED/EXPIRED, makes
        # `ok` false. If the plan is empty (nothing to sell), preserve the existing
        # no-position ok=True semantics when there are no errors.
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
        """Return the latest logged emergency sell record for this symbol, or None."""
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
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"price source unavailable — cannot plan sale: {symbol}")
        if not hasattr(self.market_price, "price"):
            raise RasatError(ErrorCode.INTERNAL_ERROR, f"invalid price-source interface: {symbol}")
        price = await self.market_price.price(symbol)
        if price is None:
            raise RasatError(ErrorCode.STALE_DATA, f"could not obtain price: {symbol}")
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
    """Build its own DB/secret/log context without requiring the daemon.

    If `market_price` is not provided (production path), use Binance public
    `/api/v3/ticker/price` through the independent `PublicPriceSource` (ticket 3.7).
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
            # 3.16/M2: close HTTP sessions opened by the runner (no leaks).
            if owned_price and market_price is not None and hasattr(market_price, "close"):
                await market_price.close()
            if owned_broker and broker is not None and hasattr(broker, "close"):
                await broker.close()
    finally:
        await db.stop()


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="rasattrading-emergency-stop",
                                     description="Daemon-independent emergency stop")
    parser.add_argument("--data-dir", help="data directory (default: ~/.rasattrading)")
    parser.add_argument("--account-id", action="append", help="target account (repeatable); all if omitted")
    parser.add_argument("--yes", action="store_true", help="automatically confirm sales (headless)")
    parser.add_argument("--dry-run", action="store_true", help="show the plan only; send nothing")
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
