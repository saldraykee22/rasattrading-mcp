"""Numaralı migration sistemi.

Daemon açılışta `migrating` durumunda `run_migrations`'ı çağırır; uygulanmamış
migration'lar sırayla, her biri kendi transaction'ında çalışır. Uygulanan sürümler
`schema_migrations` tablosuna kaydedilir. Boş DB'de ve dolu DB'de idempotenttir.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Callable

from .db import Database

logger = logging.getLogger("rasattrading.storage.migrations")

MigrationFn = Callable[[sqlite3.Connection], None]

SCHEMA_BOOTSTRAP = """
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at INTEGER NOT NULL
);
"""


def _m1_initial_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        -- Ham mum verisi (spot/futures kaynaklı)
        CREATE TABLE candles (
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          open_time INTEGER NOT NULL,
          open REAL, high REAL, low REAL, close REAL,
          volume REAL, quote_volume REAL, trades INTEGER,
          source TEXT NOT NULL DEFAULT 'spot',
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (symbol, timeframe, open_time, source)
        );

        -- Futures bağlam sinyali (salt-okunur): funding_rate | open_interest | liquidation
        CREATE TABLE futures_context (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          type TEXT NOT NULL,
          event_time INTEGER NOT NULL,
          value REAL,
          extra TEXT,
          fetched_at INTEGER NOT NULL,
          freshness TEXT NOT NULL DEFAULT 'fresh',
          UNIQUE (symbol, type, event_time)
        );

        -- Immutable PA hesap kayıtları: üzerine yazılmaz, algo_version + effective penceresi korunur
        CREATE TABLE market_structure (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          algo_version TEXT NOT NULL,
          effective_from INTEGER NOT NULL,
          effective_to INTEGER,
          payload TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );

        CREATE TABLE liquidity_zones (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          algo_version TEXT NOT NULL,
          effective_from INTEGER NOT NULL,
          effective_to INTEGER,
          payload TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );

        CREATE TABLE order_blocks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          algo_version TEXT NOT NULL,
          effective_from INTEGER NOT NULL,
          effective_to INTEGER,
          payload TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );

        -- Agent işaretlemeleri
        CREATE TABLE annotations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          created_by TEXT,
          data TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );

        -- Alarm tanımları (state machine: armed/triggered/cooldown)
        CREATE TABLE alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          alert_id TEXT NOT NULL UNIQUE,
          definition TEXT NOT NULL,
          state TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );

        -- Tetiklenen alarm kayıtları (dedup key: alert_id + trigger_key)
        CREATE TABLE triggered_alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          alert_id TEXT NOT NULL,
          trigger_key TEXT NOT NULL,
          payload TEXT NOT NULL,
          triggered_at INTEGER NOT NULL,
          UNIQUE (alert_id, trigger_key)
        );

        -- Çoklu hesap (key'ler DPAPI/Modül 3 ile şifrelenir)
        CREATE TABLE accounts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          account_id TEXT NOT NULL UNIQUE,
          label TEXT,
          tags TEXT NOT NULL DEFAULT '[]',
          market TEXT NOT NULL DEFAULT 'spot',
          trading_lock TEXT NOT NULL DEFAULT 'paper',
          encrypted_api_key BLOB,
          encrypted_secret BLOB,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );

        -- Append-only, hash-chain'li audit log (sır asla yazılmaz)
        CREATE TABLE audit_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          seq INTEGER NOT NULL UNIQUE,
          actor TEXT NOT NULL,
          action TEXT NOT NULL,
          details TEXT NOT NULL,
          prev_hash TEXT NOT NULL,
          hash TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );
        """
    )


def _m2_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_candles_lookup ON candles (symbol, timeframe, open_time DESC);
        CREATE INDEX IF NOT EXISTS idx_candles_tf_time ON candles (timeframe, open_time);
        CREATE INDEX IF NOT EXISTS idx_ms_lookup ON market_structure (symbol, timeframe, effective_from DESC);
        CREATE INDEX IF NOT EXISTS idx_lz_lookup ON liquidity_zones (symbol, timeframe, effective_from DESC);
        CREATE INDEX IF NOT EXISTS idx_ob_lookup ON order_blocks (symbol, timeframe, effective_from DESC);
        CREATE INDEX IF NOT EXISTS idx_fc_lookup ON futures_context (symbol, type, event_time DESC);
        CREATE INDEX IF NOT EXISTS idx_ann_lookup ON annotations (symbol, timeframe, created_at DESC);
        """
    )


MIGRATIONS: list[tuple[int, str, MigrationFn]] = [
    (1, "initial_schema", _m1_initial_schema),
    (2, "indexes", _m2_indexes),
]


def applied_versions(conn: sqlite3.Connection) -> set[int]:
    conn.execute(SCHEMA_BOOTSTRAP)
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(r["version"]) for r in rows}


async def run_migrations(db: Database) -> list[int]:
    """Uygulanmamış migration'ları sırayla uygular; uygulanan sürümleri döner."""

    def _run(conn: sqlite3.Connection) -> list[int]:
        conn.execute(SCHEMA_BOOTSTRAP)
        existing = applied_versions(conn)
        applied: list[int] = []
        for version, name, fn in MIGRATIONS:
            if version in existing:
                continue
            with conn:  # her migration tek transaction
                fn(conn)
                conn.execute(
                    "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
                    (version, name, int(time.time())),
                )
            applied.append(version)
            logger.info("migration %d (%s) uygulandı", version, name)
        return applied

    return await db.write(_run)


async def current_version(db: Database) -> int:
    def _get(conn: sqlite3.Connection) -> int:
        conn.execute(SCHEMA_BOOTSTRAP)
        row = conn.execute("SELECT MAX(version) AS v FROM schema_migrations").fetchone()
        return int(row["v"]) if row and row["v"] is not None else 0

    return await db.read(_get)
