"""Daemon ana süreci: kilit al, hazır ol, istekleri dinle.

Modül 1 ticket'larıyla kademeli genişletilir:
- 1.1: kilit + readiness + idle/ownership watch
- 1.2: migration (migrating state)
- 1.3: HTTP IPC sunucusu
- 1.4: veri toplama pipeline
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
    """Daemon'un startup/shutdown orkestrasyonu."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.lock_mgr = LockManager(config.lock_path, config.port)
        self.lock_info: Optional[LockInfo] = None
        self.readiness = Readiness()
        self.started_at = time.time()
        self._stop = asyncio.Event()
        self.db = None  # 1.2'de doldurulur
        self.audit = None
        self.account_service = None
        self.risk_service = None
        self.pipeline = None  # 1.4'te doldurulur
        self.http_site = None
        self.http_runner = None
        self.dispatcher = None

    # ---------- startup ----------

    async def start(self) -> None:
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.config.log_dir.mkdir(parents=True, exist_ok=True)
        setup_logging("rasattrading", file_path=self.config.daemon_log_path, stderr=True)

        logger.info(
            "rasattrading-daemon %s başlıyor (data_dir=%s, port=%s, pipeline=%s)",
            __version__,
            self.config.data_dir,
            self.config.port,
            self.config.pipeline_enabled,
        )

        try:
            self.lock_info = self.lock_mgr.acquire()
        except LockHeldError as e:
            logger.warning("kilit başka daemon'da: pid=%s — çıkıyorum", e.existing.pid)
            raise

        logger.info("kilit alındı pid=%s nonce=%s", self.lock_info.pid, self.lock_info.nonce[:8])

        await self._startup_sequence()
        await self._start_http()  # 1.3'te gerçek sunucu; 1.1'de no-op

        self.readiness.set_state("ready")
        self.lock_mgr.update_state(self.readiness.state)
        logger.info("daemon ready (state=%s)", self.readiness.state)

    async def _startup_sequence(self) -> None:
        """Sıralı başlatma: migration (1.2), ardından pipeline (1.4)."""
        await self._run_migrations()
        if self.config.pipeline_enabled:
            await self._start_pipeline()

    async def _run_migrations(self) -> None:
        """SQLite açılır, migration'lar `migrating` durumunda uygulanır."""
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
            logger.info("migration uygulandı: %s", applied)
        self.audit = AuditLog(self.db)
        self.account_service = AccountService(self.db, secret_store=SecretStore(), audit=self.audit)
        from ..storage.risk_policy import RiskPolicyService

        self.risk_service = RiskPolicyService(self.db, audit=self.audit)
        await self.risk_service.reconcile_overrides()
        self.readiness.set_state("warming_up")
        self.lock_mgr.update_state(self.readiness.state)

    async def _start_pipeline(self) -> None:
        """Veri toplama pipeline'ı: universe, miniTicker WS, kline scheduler, futures, retention."""
        from ..data.pipeline import DataPipeline

        self.pipeline = DataPipeline(self.config, self.db)
        await self.pipeline.start()
        logger.info("veri pipeline başlatıldı")

    async def _start_http(self) -> None:
        """HTTP IPC sunucusunu başlatır (localhost-only, bearer token)."""
        from .handlers import build_dispatcher
        from .server import build_site

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
            "pipeline": self.pipeline,
        }
        self.dispatcher = build_dispatcher(ctx)
        self.http_site, self.http_runner = await build_site(
            self.config, self.readiness, self.lock_info.token, self.dispatcher, extra=ctx
        )

    # ---------- main loop ----------

    async def run(self) -> int:
        watch = asyncio.create_task(self._ownership_watch())
        try:
            await self._stop.wait()
        finally:
            watch.cancel()
            await asyncio.gather(watch, return_exceptions=True)
        await self.stop()
        return 0

    async def _ownership_watch(self) -> None:
        """Kilit dosyası elimizde değilse (halef başladı / biri sildi) kapan."""
        while True:
            await asyncio.sleep(5)
            current = read_lock(self.config.lock_path)
            if self.lock_info is not None and (current is None or current.nonce != self.lock_info.nonce):
                logger.warning("kilit dosyası elimizden alındı — kapanıyor")
                self._stop.set()
                return

    def request_stop(self) -> None:
        self._stop.set()

    # ---------- shutdown ----------

    async def stop(self) -> None:
        logger.info("daemon kapanıyor")
        if self.http_site is not None:
            try:
                await self.http_site.stop()
            except Exception:  # noqa: BLE001
                logger.exception("HTTP site durdurulamadı")
        if self.http_runner is not None:
            try:
                await self.http_runner.cleanup()
            except Exception:  # noqa: BLE001
                logger.exception("HTTP runner temizlenemedi")
        if self.pipeline is not None:
            try:
                await self.pipeline.stop()
            except Exception:  # noqa: BLE001
                logger.exception("pipeline durdurulamadı")
        if self.db is not None:
            try:
                await self.db.stop()
            except Exception:  # noqa: BLE001
                logger.exception("db durdurulamadı")
        self.lock_mgr.release()


async def run_daemon(config: Config) -> int:
    runner = DaemonRunner(config)
    try:
        await runner.start()
    except LockHeldError:
        return 0  # zaten çalışıyor; arayan (adapter) kilidi kullanmaya devam edecek

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, runner.request_stop)
        except (NotImplementedError, RuntimeError):
            break  # Windows'ta bazı sinyaller desteklenmez

    return await runner.run()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rasattrading-daemon", description="Rasattrading MCP daemon")
    p.add_argument("--data-dir", help="veri dizini (varsayılan: ~/.rasattrading)")
    p.add_argument("--port", type=int, help="HTTP IPC portu")
    p.add_argument("--no-pipeline", action="store_true", help="veri toplama pipeline'ını devre dışı bırak")
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
