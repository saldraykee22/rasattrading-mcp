"""Application configuration built from environment variables and overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_DATA_DIR = Path.home() / ".rasattrading"
DEFAULT_PORT = 8751

# timeframes -> seconds (fixed monitored set plus all Binance intervals supported on demand)
TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "8h": 28800,
    "12h": 43200,
    "1d": 86400,
    "3d": 259200,
    "1w": 604800,
    "1M": 2592000,
}

DEFAULT_CANDLES_RETENTION_DAYS: dict[str, int] = {
    "15m": 90,
    "1h": 180,
    "4h": 365,
    "1d": 730,
}


@dataclass(frozen=True)
class Config:
    """Configuration shared by the daemon and adapter."""

    data_dir: Path
    host: str = "127.0.0.1"
    port: int = DEFAULT_PORT
    pipeline_enabled: bool = True
    rate_limit_max_weight: int = 6000
    rest_spot_base: str = "https://api.binance.com"
    rest_futures_base: str = "https://fapi.binance.com"
    ws_spot_base: str = "wss://stream.binance.com:9443"
    ws_futures_base: str = "wss://fstream.binance.com"
    kline_intervals: tuple[str, ...] = ("15m", "1h", "4h", "1d")
    universe_refresh_seconds: float = 1800.0
    futures_poll_seconds: float = 300.0
    liquidation_poll_seconds: float = 60.0
    futures_universe_ttl_seconds: float = 3600.0
    kline_backfill_bars: int = 300
    kline_catchup_bars: int = 5
    kline_workers: int = 8
    candles_retention_days: dict[str, int] = field(default_factory=lambda: dict(DEFAULT_CANDLES_RETENTION_DAYS))
    ready_timeout_seconds: float = 120.0
    lock_probe_retries: int = 5
    lock_probe_delay: float = 0.4
    rate_limit_window_seconds: float = 60.0
    http_timeout_seconds: float = 20.0
    alarm_eval_seconds: float = 30.0
    alarm_compute_budget: int = 8
    alarm_notify_command: str | None = None
    pa_check_seconds: float = 20.0
    pa_worker_concurrency: int = 4
    futures_stale_after_seconds: float = 1800.0

    @property
    def db_path(self) -> Path:
        return self.data_dir / "rasattrading.db"

    @property
    def lock_path(self) -> Path:
        return self.data_dir / "daemon.lock"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def daemon_log_path(self) -> Path:
        return self.log_dir / "daemon.log"

    @property
    def ws_all_miniticker_url(self) -> str:
        return f"{self.ws_spot_base}/stream?streams=!miniTicker@arr"

    @property
    def ws_force_order_url(self) -> str:
        """Market-wide liquidation orders for all symbols (public; no signature required)."""
        return f"{self.ws_futures_base}/ws/!forceOrder@arr"

    @classmethod
    def from_env(cls, overrides: dict | None = None) -> "Config":
        env = os.environ
        kwargs: dict = {
            "data_dir": Path(env.get("RASATTRADING_DATA_DIR", str(DEFAULT_DATA_DIR))),
            "port": int(env.get("RASATTRADING_PORT", str(DEFAULT_PORT))),
            "pipeline_enabled": env.get("RASATTRADING_PIPELINE_ENABLED", "1").lower() not in ("0", "false", "no"),
            "rate_limit_max_weight": int(env.get("RASATTRADING_RATE_LIMIT_WEIGHT", "6000")),
            "alarm_notify_command": env.get("RASATTRADING_ALARM_NOTIFY_COMMAND"),
        }
        if overrides:
            kwargs.update(overrides)
        return cls(**kwargs)
