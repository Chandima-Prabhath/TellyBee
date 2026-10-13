"""Logging setup: pretty text output or JSON for log collectors."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

_CONFIGURED = False


class JsonFormatter(logging.Formatter):
    """Minimal structured formatter for docker/ELK style environments."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO", fmt: str = "text") -> None:
    """Configure root logging once. Safe to call multiple times."""
    global _CONFIGURED
    root = logging.getLogger()
    if _CONFIGURED:
        root.setLevel(level.upper())
        return

    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s │ %(levelname)-7s │ %(name)-24s │ %(message)s",
                datefmt="%H:%M:%S",
            )
        )
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())

    # Tame noisy third-party loggers, keep our own at the configured level.
    for noisy in ("aiogram.event", "aiohttp.access", "aiosqlite"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    _CONFIGURED = True
