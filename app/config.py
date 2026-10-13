"""TellyBee configuration.

Everything is configured through environment variables (optionally a ``.env``
file). Values are validated and normalized at startup by pydantic so the bot
fails fast with a clear message instead of misbehaving at runtime.

Human friendly formats are accepted everywhere, e.g.::

    MAX_FILE_SIZE=20MB        DOWNLOAD_CONCURRENCY=4
    ADMIN_IDS=11111111,22222222
    ALLOWED_TYPES=photo,video,document
    ALLOWED_TYPES=["photo","video"]    (a JSON list works too)

List-typed settings accept an empty value ("keep the default"), a
comma-separated string, or a JSON array — so a bare ``ALLOWED_TYPES=``
line in ``.env`` is valid, never a parse error.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from app.utils.sizes import parse_size

logger = logging.getLogger(__name__)

#: Media kinds the bot knows how to receive and download.
SUPPORTED_MEDIA_TYPES: tuple[str, ...] = (
    "photo",
    "video",
    "document",
    "audio",
    "voice",
    "video_note",
    "sticker",
    "animation",
)

#: Tokens usable in the FILE_TEMPLATE setting.
TEMPLATE_TOKENS: tuple[str, ...] = (
    "user",
    "username",
    "user_id",
    "type",
    "date",
    "year",
    "month",
    "day",
    "chat_id",
    "chat_type",
)

_TOKEN_RE = re.compile(r"\{([a-z_]+)\}")
_BOT_TOKEN_RE = re.compile(r"^\d{6,}:[A-Za-z0-9_-]{30,}$")


def _parse_list(raw: object) -> list[str]:
    """Normalize an env / kwarg value into a clean list of strings.

    Accepted inputs:

    * ``None`` / ``""`` / whitespace  ->  ``[]`` (keep the default)
    * ``"a, b ,,c"``                 ->  ``["a", "b", "c"]``
    * ``'["a","b"]'`` (JSON array)  ->  ``["a", "b"]``
    * real lists / sets / tuples     ->  unchanged (stripped)

    Env sources hand us raw strings (JSON decoding is disabled via
    ``NoDecode``), so all tolerance has to live here.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                loaded = json.loads(text)
            except json.JSONDecodeError as err:
                raise ValueError(f"invalid JSON list: {raw!r}") from err
            if not isinstance(loaded, list):
                raise ValueError(f"expected a JSON list, got: {raw!r}")
            items: object = loaded
        else:
            items = text.split(",")
        return [str(item).strip() for item in items if str(item).strip()]  # type: ignore[union-attr]
    if isinstance(raw, (list, set, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [str(raw)]


class Settings(BaseSettings):
    """All runtime settings, sourced from env vars / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_prefix="",
    )

    # ── Bot core ────────────────────────────────────────────────────────────
    bot_token: str = ""
    # Custom Bot API server base, e.g. http://localhost:8081 for a local
    # telegram-bot-api instance (raises the 20MB download limit to 2GB).
    api_base: str = ""

    # ── Storage ─────────────────────────────────────────────────────────────
    download_dir: Path = Path("downloads")
    db_path: Path = Path("data/tellybee.db")
    # Relative folder template for saved files, e.g. {user}/{type}/{date}
    file_template: str = "{user}/{type}/{date}"

    # ── Access control (two tiers: admin / user) ───────────────────────────
    # ``NoDecode`` stops pydantic-settings from JSON-decoding these fields
    # before our validators see them; empty / CSV / JSON-list all work.
    admin_ids: Annotated[set[int], NoDecode] = Field(default_factory=set)
    allowed_user_ids: Annotated[set[int], NoDecode] = Field(default_factory=set)

    # ── Media type filters (names from SUPPORTED_MEDIA_TYPES) ──────────────
    # Empty ALLOWED_TYPES means "all supported types".
    allowed_types: Annotated[set[str], NoDecode] = Field(default_factory=set)
    blocked_types: Annotated[set[str], NoDecode] = Field(default_factory=set)

    # ── Download engine ─────────────────────────────────────────────────────
    download_concurrency: int = 3
    max_queue_size: int = 100
    download_retries: int = 3
    # Per-file hard timeout in seconds.
    download_timeout: float = 900.0
    chunk_size: int = 262144
    max_file_size: int = 20 * 1024 * 1024
    # Telegram file links are reusable ~1h; cache getFile lookups this long.
    file_cache_ttl: float = 900.0

    # ── Progress UX ─────────────────────────────────────────────────────────
    # Only files (or albums) at least this big get a live progress message.
    progress_min_bytes: int = 2 * 1024 * 1024
    # Minimum seconds between edits of a progress message.
    progress_edit_interval: float = 2.0

    # ── Serving mode ────────────────────────────────────────────────────────
    polling_mode: bool = True
    # Setting WEBHOOK_URL automatically switches the bot to webhook mode.
    webhook_url: str = ""
    webhook_path: str = "/tellybee/webhook"
    webhook_secret: str = ""
    listen_host: str = "0.0.0.0"
    listen_port: int = 8080

    # ── Misc ────────────────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_format: str = "text"  # "text" | "json"
    throttle_rate: int = 20  # updates per user per window
    throttle_window: float = 60.0

    # ── Validators ──────────────────────────────────────────────────────────

    @field_validator("admin_ids", "allowed_user_ids", mode="before")
    @classmethod
    def _parse_id_set(cls, raw: object) -> set[int]:
        ids: set[int] = set()
        for item in _parse_list(raw):
            if not item.lstrip("-").isdigit():
                raise ValueError(f"user id must be numeric, got {item!r}")
            ids.add(int(item))
        return ids

    @field_validator("allowed_types", "blocked_types", mode="before")
    @classmethod
    def _parse_type_set(cls, raw: object) -> set[str]:
        types = {item.lower() for item in _parse_list(raw)}
        unknown = types - set(SUPPORTED_MEDIA_TYPES)
        if unknown:
            raise ValueError(
                f"unknown media types {sorted(unknown)}; "
                f"supported: {', '.join(SUPPORTED_MEDIA_TYPES)}"
            )
        return types

    @field_validator(
        "max_file_size", "chunk_size", "progress_min_bytes", mode="before"
    )
    @classmethod
    def _parse_bytes(cls, raw: object) -> int:
        return parse_size(raw)

    @field_validator("bot_token", mode="before")
    @classmethod
    def _clean_token(cls, raw: object) -> str:
        return str(raw or "").strip()

    @field_validator("file_template")
    @classmethod
    def _validate_template(cls, raw: str) -> str:
        template = (raw or "").strip().strip("/")
        if not template:
            return "{user}/{type}/{date}"
        if ".." in template:
            raise ValueError("file template must not contain '..'")
        unknown = set(_TOKEN_RE.findall(template)) - set(TEMPLATE_TOKENS)
        if unknown:
            raise ValueError(
                f"unknown template tokens {sorted(unknown)}; "
                f"available: {', '.join(TEMPLATE_TOKENS)}"
            )
        return template

    @field_validator("log_level")
    @classmethod
    def _validate_level(cls, raw: str) -> str:
        level = (raw or "INFO").upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"invalid log level: {raw!r}")
        return level

    @field_validator("log_format")
    @classmethod
    def _validate_log_format(cls, raw: str) -> str:
        fmt = (raw or "text").lower()
        if fmt not in {"text", "json"}:
            raise ValueError("LOG_FORMAT must be 'text' or 'json'")
        return fmt

    @model_validator(mode="after")
    def _final_checks(self) -> Settings:
        # Token (empty allowed here so `--check-config` style tools can run,
        # main() refuses to start without it).
        if self.bot_token and not _BOT_TOKEN_RE.match(self.bot_token):
            raise ValueError(
                "BOT_TOKEN does not look like a Telegram bot token "
                "(expected format: '1234567890:AAE...'). Get one from @BotFather."
            )

        conflict = self.allowed_types & self.blocked_types
        if conflict:
            raise ValueError(
                f"types listed in both ALLOWED_TYPES and BLOCKED_TYPES: {sorted(conflict)}"
            )

        if not 1 <= self.download_concurrency <= 16:
            raise ValueError("DOWNLOAD_CONCURRENCY must be between 1 and 16")
        if not 1 <= self.max_queue_size <= 10_000:
            raise ValueError("MAX_QUEUE_SIZE must be between 1 and 10000")
        if not 0 <= self.download_retries <= 10:
            raise ValueError("DOWNLOAD_RETRIES must be between 0 and 10")
        if self.chunk_size < 8 * 1024:
            raise ValueError("CHUNK_SIZE must be at least 8KB")
        if self.max_file_size > 2000 * 1024 * 1024:
            raise ValueError("MAX_FILE_SIZE cannot exceed 2000MB (Telegram hard cap)")

        # Webhook mode switch: setting WEBHOOK_URL implies webhook unless the
        # user explicitly kept POLLING_MODE=true with no webhook url.
        if self.webhook_url and self.polling_mode:
            self.polling_mode = False
        if not self.polling_mode and not self.webhook_url:
            raise ValueError(
                "WEBHOOK_URL is required when POLLING_MODE=false "
                "(or just set WEBHOOK_URL=https://your.host and polling turns off automatically)"
            )
        if self.webhook_secret and not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", self.webhook_secret):
            raise ValueError("WEBHOOK_SECRET may only contain A-Z a-z 0-9 _ - (max 256 chars)")

        # Resolve storage paths to absolute ones relative to the CWD.
        self.download_dir = Path(self.download_dir).expanduser().resolve()
        self.db_path = Path(self.db_path).expanduser().resolve()

        return self

    # ── Derived properties ──────────────────────────────────────────────────

    @property
    def open_access(self) -> bool:
        """True when no admins and no allowed users are configured.

        In open mode everyone can use the bot (user tier). A startup warning
        is logged so operators notice.
        """
        return not self.admin_ids and not self.allowed_user_ids

    @property
    def webhook_listening(self) -> bool:
        return not self.polling_mode

    def summary(self) -> str:
        """One-line startup summary (safe to print: token is masked)."""
        token = "set" if self.bot_token else "MISSING"
        mode = "webhook" if self.webhook_listening else "polling"
        return (
            f"mode={mode} token={token} downloads={self.download_dir} "
            f"db={self.db_path} workers={self.download_concurrency} "
            f"admins={len(self.admin_ids)} allowed={len(self.allowed_user_ids)} "
            f"open_access={self.open_access}"
        )
