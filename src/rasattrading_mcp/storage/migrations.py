"""Numbered migration system.

The daemon calls `run_migrations` in the `migrating` state at startup; pending
migrations run in order, each in its own transaction. Applied versions are
recorded in the `schema_migrations` table. The process is idempotent on both
empty and populated databases.
"""

from __future__ import annotations

from contextlib import contextmanager
import logging
import os
import sqlite3
import time
from pathlib import Path
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
        -- Raw candle data (from spot/futures sources)
        CREATE TABLE IF NOT EXISTS candles (
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          open_time INTEGER NOT NULL,
          open REAL, high REAL, low REAL, close REAL,
          volume REAL, quote_volume REAL, trades INTEGER,
          source TEXT NOT NULL DEFAULT 'spot',
          updated_at INTEGER NOT NULL,
          PRIMARY KEY (symbol, timeframe, open_time, source)
        );

        -- Futures context signals (read-only): funding_rate | open_interest | liquidation
        CREATE TABLE IF NOT EXISTS futures_context (
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

        -- Immutable PA calculation records: never overwritten; algo_version + effective window preserved
        CREATE TABLE IF NOT EXISTS market_structure (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          algo_version TEXT NOT NULL,
          effective_from INTEGER NOT NULL,
          effective_to INTEGER,
          payload TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS liquidity_zones (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          algo_version TEXT NOT NULL,
          effective_from INTEGER NOT NULL,
          effective_to INTEGER,
          payload TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS order_blocks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          algo_version TEXT NOT NULL,
          effective_from INTEGER NOT NULL,
          effective_to INTEGER,
          payload TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );

        -- Agent annotations
        CREATE TABLE IF NOT EXISTS annotations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          created_by TEXT,
          data TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );

        -- Alert definitions (state machine: armed/triggered/cooldown)
        CREATE TABLE IF NOT EXISTS alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          alert_id TEXT NOT NULL UNIQUE,
          definition TEXT NOT NULL,
          state TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );

        -- Triggered alert records (dedup key: alert_id + trigger_key)
        CREATE TABLE IF NOT EXISTS triggered_alerts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          alert_id TEXT NOT NULL,
          trigger_key TEXT NOT NULL,
          payload TEXT NOT NULL,
          triggered_at INTEGER NOT NULL,
          UNIQUE (alert_id, trigger_key)
        );

        -- Multiple accounts; credentials are encrypted by the credential store.
        CREATE TABLE IF NOT EXISTS accounts (
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

        -- Append-only, hash-chained audit log (secrets are never written)
        CREATE TABLE IF NOT EXISTS audit_log (
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
    """Add the cooldown column for the alert state machine (armed→triggered→cooldown→armed)."""
    conn.execute("ALTER TABLE alerts ADD COLUMN cooldown_until INTEGER")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_state ON alerts (state, updated_at)")


def _m4_risk_policy(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        -- Optional account-level risk policy (v1: spot long-only)
        -- max_notional_per_order / max_aggregate_exposure caps:
        --   REAL NULL = unlimited; when set, this is a HARD upper bound (no tolerance).
        -- allowed_symbols: JSON string list; empty [] = all symbols allowed.
        CREATE TABLE IF NOT EXISTS risk_policy (
          account_id TEXT PRIMARY KEY,
          max_notional_per_order REAL,
          max_aggregate_exposure REAL,
          allowed_symbols TEXT NOT NULL DEFAULT '[]',
          policy_version INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );

        -- One-time override state machine: reserved -> applied|reconciled
        -- Carries (account_id, policy_version, idempotency_key, actor, expires_at).
        -- consumed_by_idem = the idempotency key of the order request consuming the override.
        CREATE TABLE IF NOT EXISTS risk_override (
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

        CREATE INDEX IF NOT EXISTS idx_risk_override_account ON risk_override (account_id, state, expires_at);
        """
    )


def _m5_orders(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        -- Order records: idempotency + real Binance state machine + aggregate exposure.
        -- status: NEW | PARTIALLY_FILLED | FILLED | CANCELED | REJECTED | EXPIRED | UNKNOWN | PAPER
        -- (account_id, idempotency_key) UNIQUE -> retries with the same key do not create
        --   duplicate orders; the stored order is returned / its state is reconciled.
        CREATE TABLE IF NOT EXISTS orders (
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

        CREATE INDEX IF NOT EXISTS idx_orders_account ON orders (account_id, status);
        CREATE INDEX IF NOT EXISTS idx_orders_open ON orders (account_id, status, created_at);
        """
    )


def _m6_emergency_reconciled(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        -- Entries reconciled from the emergency_stop log file into audit_log.
        -- entry_hash UNIQUE -> the same emergency entry is not written to audit_log
        -- a second time during daemon startup (idempotent reconciliation).
        CREATE TABLE IF NOT EXISTS emergency_reconciled (
          entry_hash TEXT PRIMARY KEY,
          seq INTEGER NOT NULL,
          reconciled_at INTEGER NOT NULL
        );
        """
    )


def _m7_orders_equity_snapshot(conn: sqlite3.Connection) -> None:
    """Add an account equity snapshot to order records (3.20 M1).

    `execute_on_accounts` calculated equity and passed it to `_insert_order`, but
    the table had no column; the equity at the time of risk_pct sizing was not
    persisted. Existing order records remain NULL (no backfill—the historical
    equity cannot be recalculated).
    """
    conn.execute("ALTER TABLE orders ADD COLUMN equity_snapshot REAL")


def _m8_pending_orders(conn: sqlite3.Connection) -> None:
    """Add order records awaiting approval after an alert (2.19).

    When an alert fires with an `order_spec`, an `awaiting_approval` record is
    created. The order is NOT opened automatically: approval via
    `approve_pending_order` opens the real order (`executed_order_id` is filled),
    while `reject_pending_order` cancels it. `approved_at`/`executed_order_id`
    are the audit trail for the approval flow.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS pending_orders (
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


def _m9_stop_price(conn: sqlite3.Connection) -> None:
    """Add the stop_price column to order records (2.20).

    STOP_LOSS_LIMIT orders carry stopPrice; it must also be stored in the record
    (audit + position-protection visibility). Existing orders remain NULL.
    """
    conn.execute("ALTER TABLE orders ADD COLUMN stop_price REAL")


def _m10_stop_limit_price(conn: sqlite3.Connection) -> None:
    """Add the stop_limit_price column to OCO order records (2.21).

    When stopPrice triggers in an OCO, a LIMIT sell is placed at the
    stopLimitPrice level; all three prices (price=TP, stop_price,
    stop_limit_price) must remain in the record.
    """
    conn.execute("ALTER TABLE orders ADD COLUMN stop_limit_price REAL")


def _m11_pending_execution_state(conn: sqlite3.Connection) -> None:
    """Pending execution attempt/reconcile state (T00 contract)."""

    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(pending_orders)").fetchall()
    }
    additions = (
        ("execution_started_at", "INTEGER"),
        ("execution_finished_at", "INTEGER"),
        ("execution_error_code", "TEXT"),
        ("execution_error_message", "TEXT"),
        ("last_attempt_at", "INTEGER"),
    )
    for name, definition in additions:
        if name not in columns:
            conn.execute(
                f"ALTER TABLE pending_orders ADD COLUMN {name} {definition}"
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
    (9, "orders_stop_price", _m9_stop_price),
    (10, "orders_stop_limit_price", _m10_stop_limit_price),
    (11, "pending_execution_state", _m11_pending_execution_state),
]


@contextmanager
def _migration_lock(db_path: Path):
    """Cross-process migration lock for daemon and standalone kill-switch."""

    lock_path = db_path.with_name(db_path.name + ".migrations.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_BINARY", 0)
    fd = os.open(str(lock_path), flags)
    acquired = False
    deadline = time.monotonic() + 30.0
    try:
        if os.fstat(fd).st_size == 0:
            os.write(fd, b"0")
        if os.name == "nt":
            import msvcrt

            while True:
                try:
                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                    acquired = True
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("could not acquire migration lock")
                    time.sleep(0.05)
        else:
            import fcntl

            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("could not acquire migration lock")
                    time.sleep(0.05)
        yield
    finally:
        if acquired:
            try:
                if os.name == "nt":
                    import msvcrt

                    os.lseek(fd, 0, os.SEEK_SET)
                    msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
        else:
            os.close(fd)


def applied_versions(conn: sqlite3.Connection) -> set[int]:
    conn.execute(SCHEMA_BOOTSTRAP)
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(r["version"]) for r in rows}


def applied_names(conn: sqlite3.Connection) -> set[str]:
    conn.execute(SCHEMA_BOOTSTRAP)
    rows = conn.execute("SELECT name FROM schema_migrations").fetchall()
    return {str(r["name"]) for r in rows}


async def run_migrations(db: Database) -> list[int]:
    """Apply pending migrations in order and return the applied versions.

    Skip criterion: skip a migration if its version number OR name was already applied.
    The name check prevents a migration from being applied twice when numbering
    changes, for example after a branch merge.
    """

    def _run(conn: sqlite3.Connection) -> list[int]:
        with _migration_lock(db.path):
            conn.execute(SCHEMA_BOOTSTRAP)
            existing_versions = applied_versions(conn)
            existing_names = applied_names(conn)
            applied: list[int] = []
            for version, name, fn in MIGRATIONS:
                if version in existing_versions or name in existing_names:
                    continue
                with conn:  # one transaction per migration
                    fn(conn)
                    conn.execute(
                        "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
                        (version, name, int(time.time())),
                    )
                applied.append(version)
                logger.info("migration %d (%s) applied", version, name)
            return applied

    return await db.write(_run)


async def current_version(db: Database) -> int:
    def _get(conn: sqlite3.Connection) -> int:
        conn.execute(SCHEMA_BOOTSTRAP)
        row = conn.execute("SELECT MAX(version) AS v FROM schema_migrations").fetchone()
        return int(row["v"]) if row and row["v"] is not None else 0

    return await db.read(_get)
