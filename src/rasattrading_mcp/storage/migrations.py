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


def _m3_alert_cooldown(conn: sqlite3.Connection) -> None:
    """Alarm state machine'i için soğuma sütunu (armed→triggered→cooldown→armed)."""
    conn.execute("ALTER TABLE alerts ADD COLUMN cooldown_until INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_state ON alerts (state, updated_at)")


def _m4_risk_policy(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        -- Hesap bazlı opsiyonel risk politikası (v1: spot long-only)
        -- max_notional_per_order / max_aggregate_exposure cap'leri:
        --   REAL NULL = sınırsız; varsa KATI üst sınırdır (tolerans uygulanmaz).
        -- allowed_symbols: JSON string listesi; boş [] = tüm semboller serbest.
        CREATE TABLE risk_policy (
          account_id TEXT PRIMARY KEY,
          max_notional_per_order REAL,
          max_aggregate_exposure REAL,
          allowed_symbols TEXT NOT NULL DEFAULT '[]',
          policy_version INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );

        -- Tek kullanımlık override state machine: reserved -> applied|reconciled
        -- (account_id, policy_version, idempotency_key, actor, expires_at) taşır.
        -- consumed_by_idem = override'ı tüketen emir isteğinin idempotency key'i.
        CREATE TABLE risk_override (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          override_id TEXT NOT NULL UNIQUE,
          account_id TEXT NOT NULL,
          policy_version INTEGER NOT NULL,
          idempotency_key TEXT NOT NULL,
          actor TEXT NOT NULL,
          reason TEXT NOT NULL,
          scope TEXT NOT NULL DEFAULT 'next_order',
          state TEXT NOT NULL DEFAULT 'reserved',
          created_at INTEGER NOT NULL,
          expires_at INTEGER NOT NULL,
          applied_at INTEGER,
          consumed_by_idem TEXT,
          reconciled_at INTEGER,
          reconcile_reason TEXT,
          UNIQUE (account_id, idempotency_key)
        );

        CREATE INDEX idx_risk_override_account ON risk_override (account_id, state, expires_at);
        """
    )


def _m5_orders(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        -- Emir kayıtları: idempotency + gerçek Binance state machine + aggregate exposure.
        -- status: NEW | PARTIALLY_FILLED | FILLED | CANCELED | REJECTED | EXPIRED | UNKNOWN | PAPER
        -- (account_id, idempotency_key) UNIQUE -> aynı anahtarla retry çift emir üretmez,
        --   stored emir döner / durum reconcile edilir.
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          order_id TEXT NOT NULL UNIQUE,
          account_id TEXT NOT NULL,
          idempotency_key TEXT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          order_type TEXT NOT NULL,
          quantity REAL NOT NULL,
          price REAL,
          status TEXT NOT NULL,
          exchange_order_id TEXT,
          client_order_id TEXT,
          executed_qty REAL,
          avg_price REAL,
          fee REAL,
          notional REAL,
          reference_price REAL,
          error_code TEXT,
          error_message TEXT,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          UNIQUE (account_id, idempotency_key)
        );

        CREATE INDEX idx_orders_account ON orders (account_id, status);
        CREATE INDEX idx_orders_open ON orders (account_id, status, created_at);
        """
    )


def _m6_emergency_reconciled(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        -- emergency_stop log dosyasından audit_log'a reconcile edilen entry'ler.
        -- entry_hash UNIQUE -> daemon açılışında aynı emergency entry ikinci kez
        -- audit_log'a yazılmaz (idempotent reconcile).
        CREATE TABLE emergency_reconciled (
          entry_hash TEXT PRIMARY KEY,
          seq INTEGER NOT NULL,
          reconciled_at INTEGER NOT NULL
        );
        """
    )


def _m7_orders_equity_snapshot(conn: sqlite3.Connection) -> None:
    """Emir kaydına hesap equity snapshot'ı (3.20 M1).

    `execute_on_accounts` equity'yi hesaplayıp `_insert_order`'a geçiyordu ama
    tabloda sütun yoktu; risk_pct boyutlandırmasının yapıldığı anın equity'si
    kalıcı olarak saklanmazdı. Eski emir kayıtları NULL kalır (geriye dönük dolgu
    yok — geçmişin equity'si yeniden hesaplanamaz).
    """
    conn.execute("ALTER TABLE orders ADD COLUMN equity_snapshot REAL")


def _m8_pending_orders(conn: sqlite3.Connection) -> None:
    """Alarm → onay bekleyen emir kayıtları (2.19).

    Alarm tetiklenip `order_spec` taşıdığında `awaiting_approval` kaydı düşer.
    Emir OTOMATİK açılmaz: `approve_pending_order` onayında gerçek emir
    açılır (`executed_order_id` doldurulur), `reject_pending_order` iptal eder.
    `approved_at`/`executed_order_id` onay akışının audit izidir.
    """
    conn.executescript(
        """
        CREATE TABLE pending_orders (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          order_id TEXT NOT NULL UNIQUE,
          alert_id TEXT NOT NULL,
          account_id TEXT NOT NULL,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          order_type TEXT NOT NULL DEFAULT 'market',
          entry REAL,
          stop_loss REAL,
          risk_pct REAL,
          status TEXT NOT NULL DEFAULT 'awaiting_approval',
          note TEXT,
          created_at INTEGER NOT NULL,
          approved_at INTEGER,
          rejected_at INTEGER,
          executed_order_id TEXT,
          reject_reason TEXT
        );
        CREATE INDEX idx_pending_orders_status ON pending_orders (status, created_at);
        CREATE INDEX idx_pending_orders_alert ON pending_orders (alert_id);
        """
    )


MIGRATIONS: list[tuple[int, str, MigrationFn]] = [
    (1, "initial_schema", _m1_initial_schema),
    (2, "indexes", _m2_indexes),
    (3, "alert_cooldown", _m3_alert_cooldown),
    (4, "risk_policy_override", _m4_risk_policy),
    (5, "orders", _m5_orders),
    (6, "emergency_reconciled", _m6_emergency_reconciled),
    (7, "orders_equity_snapshot", _m7_orders_equity_snapshot),
    (8, "pending_orders", _m8_pending_orders),
]


def applied_versions(conn: sqlite3.Connection) -> set[int]:
    conn.execute(SCHEMA_BOOTSTRAP)
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(r["version"]) for r in rows}


def applied_names(conn: sqlite3.Connection) -> set[str]:
    conn.execute(SCHEMA_BOOTSTRAP)
    rows = conn.execute("SELECT name FROM schema_migrations").fetchall()
    return {str(r["name"]) for r in rows}


async def run_migrations(db: Database) -> list[int]:
    """Uygulanmamış migration'ları sırayla uygular; uygulanan sürümleri döner.

    Atlama ölçütü: sürüm numarası VEYA isim daha önce uygulanmışsa atlanır.
    İsim kontrolü, numaralandırmanın değiştiği durumlarda (örn. branch merge'i
    sonrası yeniden numaralandırma) aynı migration'ın çift uygulanmasını engeller.
    """

    def _run(conn: sqlite3.Connection) -> list[int]:
        conn.execute(SCHEMA_BOOTSTRAP)
        existing_versions = applied_versions(conn)
        existing_names = applied_names(conn)
        applied: list[int] = []
        for version, name, fn in MIGRATIONS:
            if version in existing_versions or name in existing_names:
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
