"""Logging setup: pretty text output or JSON for log collectors.

Every line that reaches a handler is passed through a secret scrubber so the
bot token — a component of Bot API method/file URLs that otherwise shows up
in exception messages like ``ClientResponseError: 404 … url='…/bot<TOKEN>/…'``
— can never leak into docker logs or chat receipts.
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Sequence
from datetime import UTC, datetime

_CONFIGURED = False
_SECRETS: tuple[str, ...] = ()

_MASK = "***"


class SecretScrubbingFormatter(logging.Formatter):
    """Base formatter that masks every configured secret in the final text."""

    def scrub(self, text: str) -> str:
        for secret in _SECRETS:
            if secret and secret in text:
                text = text.replace(secret, _MASK)
        return text

    def format(self, record: logging.LogRecord) -> str:
        return self.scrub(super().format(record))


class JsonFormatter(SecretScrubbingFormatter):
    """Minimal structured formatter for docker/ELK style environments."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": self.scrub(record.getMessage()),
        }
        if record.exc_info:
            payload["exc"] = self.scrub(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False)


def register_secrets(secrets: Sequence[str]) -> None:
    """Secrets to mask in all future log output (e.g. the bot token)."""
    global _SECRETS
    _SECRETS = tuple(s for s in secrets if s)


def setup_logging(
    level: str = "INFO",
    fmt: str = "text",
    secrets: Sequence[str] = (),
) -> None:
    """Configure root logging once. Safe to call multiple times."""
    global _CONFIGURED
    if secrets:
        register_secrets(secrets)
    root = logging.getLogger()
    if _CONFIGURED:
        root.setLevel(level.upper())
        return

    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            SecretScrubbingFormatter(
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
