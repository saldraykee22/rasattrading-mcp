"""Logging helpers.

Never write to stdout, so the adapter's stdio protocol is preserved; write only to stderr and/or a file.
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(name: str, file_path: Path | None = None, stderr: bool = True, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    if file_path is not None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(file_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    if stderr:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        logger.addHandler(sh)

    return logger
