"""Daemon main process: acquire the lock, become ready, and serve requests.

The daemon acquires its lock, initializes readiness and migrations, serves HTTP IPC,
and starts the market-data pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from typing import Optional

from .. import __version__
from ..config import Config
from ..logging_util import setup_logging
from .lock import LockHeldError, LockInfo, LockManager, read_lock
from .readiness import Readiness

logger = logging.getLogger("rasattrading.daemon")


class DaemonRunner:
    """Orchestrate daemon startup and shutdown."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.lock_mgr = LockManager(config.lock_path, config.port)
        self.lock_info: Optional[LockInfo] = None
        self.readiness = Readiness()
        self.started_at = time.time()
        self._stop = asyncio.Event()
        self.db = None  # populated in 1.2
        self.audit = None
        self.account_service = None
        self.risk_service = None
        self.order_service = None
        self.order_broker = None
        self.pipeline = None  # populated in 1.4
        self.http_site = None
        self.http_runner = None
        self.dispatcher = None
        self.pa_engine = None
        self.alarm_service = None
        self.pa_worker = None
        self._pa_worker_task = None
        self._shutdown_lock = asyncio.Lock()
        self._shutdown_complete = False

    # ---------- startup ----------

    async def start(self) -> None:
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.config.log_dir.mkdir(parents=True, exist_ok=True)
        setup_logging("rasattrading", file_path=self.config.daemon_log_path, stderr=True)

        logger.info(
            "rasattrading-daemon %s starting (data_dir=%s, port=%s, pipeline=%s)",
            __version__,
            self.config.data_dir,
            self.config.port,
            self.config.pipeline_enabled,
        )

        try:
            self.lock_info = self.lock_mgr.acquire()
        except LockHeldError as e:
            logger.warning("lock is held by another daemon: pid=%s — exiting", e.existing.pid)
            raise

        logger.info("lock acquired pid=%s nonce=%s", self.lock_info.pid, self.lock_info.nonce[:8])

        try:
            await self._startup_sequence()
            await self._start_http()  # Real server in 1.3; no-op in 1.1.
        except asyncio.CancelledError:
            await asyncio.shield(self.stop())
            raise
        except Exception:
            # If any startup stage is interrupted, do not leave the lock, DB,
            # pipeline, or broker session behind. stop() safely cleans components
            # even in a partially initialized state.
            await self.stop()
            raise

        self.readiness.set_state("ready")
        self.lock_mgr.update_state(self.readiness.state)
        logger.info("daemon ready (state=%s)", self.readiness.state)

    async def _startup_sequence(self) -> None:
        """Sequential startup: migration (1.2), followed by the pipeline (1.4)."""
        await self._run_migrations()
        if self.config.pipeline_enabled:
            await self._start_pipeline()

    async def _run_migrations(self) -> None:
        """Open SQLite and apply migrations while in the `migrating` state."""
        from ..storage.db import Database
        from ..storage.accounts import AccountService
        from ..storage.audit import AuditLog
        from ..storage.credentials import SecretStore
        from ..storage.migrations import run_migrations

        self.db = Database(self.config.db_path)
        await self.db.start()

        self.readiness.set_state("migrating")
        self.lock_mgr.update_state(self.readiness.state)
        applied = await run_migrations(self.db)
        if applied:
            logger.info("migration applied: %s", applied)
        self.audit = AuditLog(self.db)
        self.account_service = AccountService(self.db, secret_store=SecretStore(), audit=self.audit)
        from ..storage.risk_policy import RiskPolicyService

        self.risk_service = RiskPolicyService(self.db, audit=self.audit)
        await self.risk_service.reconcile_overrides()
        # Reconcile the emergency_stop log written while the daemon was down into audit_log (3.6).
        from ..storage.emergency_log import EmergencyLog, reconcile_emergency_log

        try:
            await reconcile_emergency_log(self.db, self.audit, EmergencyLog(self.config.data_dir / "emergency_stop.log"))
        except Exception:  # noqa: BLE001
            logger.exception("could not reconcile emergency_stop log")
        self.readiness.set_state("warming_up")
        self.lock_mgr.update_state(self.readiness.state)

    async def _start_pipeline(self) -> None:
        """Data-collection pipeline: universe, miniTicker WS, kline scheduler, futures, and retention."""
        from ..data.pipeline import DataPipeline

        self.pipeline = DataPipeline(self.config, self.db)
        await self.pipeline.start()
        logger.info("data pipeline started")

    async def _start_http(self) -> None:
        """Start the HTTP IPC server (localhost-only, bearer token)."""
        from .handlers import build_dispatcher
        from .server import build_site
        from ..pa.analysis import PAEngine
        from ..pa.alarms import AlarmService

        pa_engine = PAEngine(self.db, pipeline=self.pipeline, config=self.config)
        alarm_service = AlarmService(
            self.db, engine=pa_engine, notify_command=self.config.alarm_notify_command
        )
        pa_engine.alarm_service = alarm_service
        self.pa_engine = pa_engine
        self.alarm_service = alarm_service

        # 2.8: background PA worker — automatically recomputes PA when bars close.
        self._pa_worker_task = None
        if self.pipeline is not None:
            from ..pa.worker import PAWorker

            self.pa_worker = PAWorker(pa_engine, self.pipeline.universe, self.config)
            self._pa_worker_task = asyncio.create_task(self.pa_worker.run())
            logger.info("background PA worker started")

        # Order broker: uses the pipeline budget and account credentials.
        from ..data.order_broker import BinanceOrderBroker
        from ..storage.orders import OrderService, PipelineMarketFeed

        order_service = None
        if self.pipeline is not None and self.account_service is not None:
            self.order_broker = BinanceOrderBroker(
                self.config.rest_spot_base,
                credentials=lambda account_id: self.account_service.get_credentials(account_id),
                budget=self.pipeline.budget,
                clock=self.pipeline.clock,
            )
            order_service = OrderService(
                self.db,
                accounts=self.account_service,
                risk=self.risk_service,
                broker=self.order_broker,
                market=PipelineMarketFeed(self.pipeline),
                audit=self.audit,
            )
            self.order_service = order_service

            # 3.13: verify orders left in NEW after a crash with Binance at startup
            # to prevent inflated exposure.
            try:
                res = await order_service.reconcile_open_orders()
                if res["scanned"]:
                    logger.info("open-order reconciliation (3.13): %s", res)
            except Exception:  # noqa: BLE001
                logger.exception("could not reconcile open orders (3.13)")

            # T3: recover approved orders left in `executing` after a crash at startup;
            # the daemon may have stopped before processing the order result.
            try:
                res = await alarm_service.reconcile_pending_executions()
                if res["scanned"]:
                    logger.info("pending execution reconcile (T3): %s", res)
            except Exception:  # noqa: BLE001
                logger.exception("pending execution reconciliation failed (T3)")

        ctx = {
            "config": self.config,
            "readiness": self.readiness,
            "started_at": self.started_at,
            "pid": os.getpid(),
            "db": self.db,
            "audit": self.audit,
            "account_service": self.account_service,
            "accounts": self.account_service,
            "risk_service": self.risk_service,
            "risk_policy_service": self.risk_service,
            "order_service": order_service,
            "order_broker": self.order_broker,
            "pipeline": self.pipeline,
            "pa_engine": pa_engine,
            "alarm_service": alarm_service,
        }
        self.dispatcher = build_dispatcher(ctx)
        self.http_site, self.http_runner = await build_site(
            self.config, self.readiness, self.lock_info.token, self.dispatcher, extra=ctx
        )

    # ---------- main loop ----------

    async def run(self) -> int:
        watch = asyncio.create_task(self._ownership_watch())
        alarm_watch = asyncio.create_task(self._alarm_eval_loop()) if self.alarm_service is not None else None
        try:
            await self._stop.wait()
        finally:
            watch.cancel()
            if alarm_watch is not None:
                alarm_watch.cancel()
            tasks = [watch] + ([alarm_watch] if alarm_watch is not None else [])
            await asyncio.gather(*tasks, return_exceptions=True)
            # stop() runs on every exit path: normal completion, CancelledError
            # (Windows Ctrl+C), and any exception. Otherwise CancelledError would
            # propagate before the DB writer executor/lock closed, leaving the daemon hung.
            await self.stop()
        return 0

    async def _alarm_eval_loop(self) -> None:
        """Periodic alarm evaluation (the background fallback for event-driven evaluation).

        Evaluate immediately (no initial 30s delay); the primary trigger is
        event-driven through `PAEngine.analyze → on_analysis_updated`. Reset the
        on-demand PA computation budget at the start of every pass (K3).
        If there are no alarms, a pass is only one inexpensive DB query.
        """
        while True:
            try:
                self.alarm_service.begin_evaluation_pass()
                pairs = await self.alarm_service.alert_symbols()
                for symbol, timeframe in pairs:
                    try:
                        await self.alarm_service.evaluate_symbol(symbol, timeframe)
                    except Exception:  # noqa: BLE001
                        logger.exception("alarm evaluation failed: %s %s", symbol, timeframe)
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                logger.exception("alarm loop failed")
            try:
                await asyncio.sleep(self.config.alarm_eval_seconds)
            except asyncio.CancelledError:
                return

    async def _ownership_watch(self) -> None:
        """Shut down if we no longer own the lock file (a successor started or someone deleted it)."""
        while True:
            await asyncio.sleep(5)
            current = read_lock(self.config.lock_path)
            if self.lock_info is not None and (current is None or current.nonce != self.lock_info.nonce):
                logger.warning("lock file ownership was lost — shutting down")
                self._stop.set()
                return

    def request_stop(self) -> None:
        self._stop.set()

    # ---------- shutdown ----------

    async def stop(self) -> None:
        async with self._shutdown_lock:
            if self._shutdown_complete:
                return
            logger.info("daemon shutting down")
            self._stop.set()

            try:
                pa_task = self._pa_worker_task
                if pa_task is not None:
                    pa_task.cancel()
                    await asyncio.gather(pa_task, return_exceptions=True)

                if self.http_site is not None:
                    try:
                        await self.http_site.stop()
                    except Exception:  # noqa: BLE001
                        logger.exception("could not stop HTTP site")
                if self.http_runner is not None:
                    try:
                        await self.http_runner.cleanup()
                    except Exception:  # noqa: BLE001
                        logger.exception("could not clean up HTTP runner")

                # Close the broker HTTP session after handlers stop and before the
                # pipeline/DB close, so no new order/REST call can start while
                # resources are being shut down.
                broker = self.order_broker
                close_broker = getattr(broker, "close", None) if broker is not None else None
                if close_broker is not None:
                    try:
                        await close_broker()
                    except Exception:  # noqa: BLE001
                        logger.exception("could not close order broker session")

                if self.pipeline is not None:
                    try:
                        await self.pipeline.stop()
                    except Exception:  # noqa: BLE001
                        logger.exception("could not stop pipeline")
                if self.db is not None:
                    try:
                        await self.db.stop()
                    except Exception:  # noqa: BLE001
                        logger.exception("could not stop database")
            finally:
                try:
                    self.lock_mgr.release()
                except Exception:  # noqa: BLE001
                    logger.exception("could not release daemon lock")
                self._shutdown_complete = True


async def run_daemon(config: Config) -> int:
    runner = DaemonRunner(config)
    try:
        await runner.start()
    except LockHeldError:
        return 0  # Already running; the caller (adapter) continues using the lock.

    loop = asyncio.get_running_loop()

    def _signal_fallback(signum: int, frame) -> None:
        # On Windows, loop.add_signal_handler raises NotImplementedError; safely
        # forward the signal to the loop thread and start graceful shutdown.
        try:
            loop.call_soon_threadsafe(runner.request_stop)
        except RuntimeError:
            logger.warning("loop is closed while handling signal — ignoring (sig=%s)", signum)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, runner.request_stop)
        except (NotImplementedError, RuntimeError):
            # Windows (or a platform without signal-handler support): the signal.signal
            # fallback runs on the main thread and forwards request_stop to the loop.
            try:
                signal.signal(sig, _signal_fallback)
                logger.info("signal-handler fallback installed (sig=%s, signal.signal)", sig)
            except (ValueError, OSError, RuntimeError):
                logger.warning("could not install signal handler (sig=%s)", sig, exc_info=True)

    return await runner.run()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rasattrading-daemon", description="Rasattrading MCP daemon")
    p.add_argument("--data-dir", help="data directory (default: ~/.rasattrading)")
    p.add_argument("--port", type=int, help="HTTP IPC portu")
    p.add_argument("--no-pipeline", action="store_true", help="disable the data-collection pipeline")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    overrides = {}
    if args.data_dir:
        overrides["data_dir"] = args.data_dir
    if args.port:
        overrides["port"] = args.port
    if args.no_pipeline:
        overrides["pipeline_enabled"] = False
    config = Config.from_env(overrides)
    try:
        return asyncio.run(run_daemon(config))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
