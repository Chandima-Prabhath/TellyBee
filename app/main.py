"""TellyBee entrypoint: wires config → bot → queue → web → run.

Run with::

    python -m app.main            # reads .env / environment
    python -m app.main --check    # validate configuration and exit
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path

from aiogram.exceptions import (
    TelegramConflictError,
    TelegramNetworkError,
    TelegramRetryAfter,
    TelegramServerError,
    TelegramUnauthorizedError,
)
from aiohttp import web

from app import __version__
from app.bot_factory import build_bot, build_dispatcher, cloud_logout, sync_bot_commands
from app.config import Settings
from app.db import Database
from app.logging_setup import setup_logging
from app.queue.manager import DownloadManager
from app.services.notifier import Notifier
from app.web.health import attach_webhook, create_web_app

logger = logging.getLogger(__name__)


def _load_settings() -> Settings:
    try:
        return Settings()  # type: ignore[call-arg]
    except Exception as err:  # noqa: BLE001 — fail with a friendly message
        print("❌ Configuration error:\n", file=sys.stderr)
        print(f"  {err}\n", file=sys.stderr)
        print("Fix the environment / .env file and try again.", file=sys.stderr)
        raise SystemExit(2) from err


async def _setup_webhook(bot, settings: Settings) -> None:
    from aiogram.methods import DeleteWebhook, SetWebhook

    await bot(DeleteWebhook(drop_pending_updates=False))
    await bot(
        SetWebhook(
            url=settings.webhook_url,
            secret_token=settings.webhook_secret or None,
            drop_pending_updates=False,
            allowed_updates=["message"],
        )
    )
    logger.info("webhook registered: %s", settings.webhook_url)


async def _poll_forever(
    poll: Callable[[], Awaitable[None]],
    stop: asyncio.Event,
    *,
    name: str = "polling",
    base_delay: float = 1.0,
    max_delay: float = 30.0,
) -> None:
    """Supervise polling with automatic recovery.

    aiogram's ``start_polling`` exits on the *first* transport-level failure —
    with an unreachable Bot API server (common at container boot, since the
    bot and the local server start in parallel) the process would otherwise
    keep running while doing nothing at all. This supervisor restarts
    polling with exponential backoff for transient errors and only gives up
    on unrecoverable ones, triggering a clean process shutdown so the
    container restarts fresh.
    """
    delay = base_delay
    attempt = 0
    while not stop.is_set():
        attempt += 1
        try:
            if attempt == 1:
                logger.info("%s: polling for updates…", name)
            await poll()
            logger.warning("%s: polling loop ended — shutting down", name)
            stop.set()
            return
        except asyncio.CancelledError:
            raise
        except TelegramUnauthorizedError as err:
            logger.critical(
                "%s: Telegram rejected the bot token (%s). Check BOT_TOKEN in .env — "
                "giving up.",
                name,
                err,
            )
            stop.set()
            return
        except TelegramConflictError as err:
            logger.warning(
                "%s: getUpdates conflict (%s) — another instance is polling or a "
                "webhook is still registered; retrying in %.0fs",
                name,
                err,
                delay,
            )
        except (TelegramRetryAfter, TelegramServerError, TelegramNetworkError, OSError) as err:
            logger.warning(
                "%s: Bot API server unreachable (%s: %s) — retrying in %.0fs",
                name,
                type(err).__name__,
                err,
                delay,
            )
        except Exception as err:  # noqa: BLE001 — unknown bug: restart cleanly
            logger.exception("%s: unexpected polling error (%r) — shutting down", name, err)
            stop.set()
            return
        # Cancellation-aware backoff sleep, then retry.
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            return
        except asyncio.TimeoutError:
            pass
        delay = min(delay * 2.0, max_delay)


def _ensure_writable_dir(directory: Path) -> None:
    """Create *directory* and verify the bot can actually write to it.

    Fails fast (SystemExit) with an actionable message instead of crashing
    on the first download — the classic cause is a docker bind mount that
    Docker created as root while the bot runs as uid 1000.
    """
    directory.mkdir(parents=True, exist_ok=True)
    probe = directory / ".tellybee-write-test"
    try:
        probe.write_text("", encoding="utf-8")
        probe.unlink()
    except OSError as err:
        logger.error(
            "download directory %s is not writable: %s — fix ownership "
            "(with docker bind mounts run once on the host: "
            "sudo chown -R 1000:1000 ./downloads ./data)",
            directory,
            err,
        )
        raise SystemExit(2) from err


async def run() -> None:
    settings = _load_settings()
    setup_logging(settings.log_level, settings.log_format)
    logger.info("🐝 TellyBee v%s starting", __version__)
    logger.info("config: %s", settings.summary())

    if settings.open_access:
        logger.warning(
            "OPEN ACCESS: no ADMIN_IDS / ALLOWED_USER_IDS configured — "
            "anyone can send files to this bot."
        )

    if not settings.bot_token:
        logger.error("BOT_TOKEN is not set. Get one from @BotFather and put it in .env")
        raise SystemExit(2)

    # Telegram requires a one-time logOut from the cloud API before a local
    # Bot API server will serve the bot (otherwise every call 404s).
    if settings.api_base:
        logger.info(
            "custom Bot API server configured — running the cloud logOut handshake"
        )
        await cloud_logout(settings.bot_token)

    _ensure_writable_dir(settings.download_dir)

    # ── wiring ──────────────────────────────────────────────────────────────
    db = Database(settings.db_path)
    await db.connect()
    await db.seed_users(settings.admin_ids, settings.allowed_user_ids)

    bot = build_bot(settings)
    notifier = Notifier(bot, settings)
    manager = DownloadManager(bot, db, settings, notifier=notifier)
    dp = build_dispatcher(settings, db, manager)

    await manager.start()

    # ── HTTP surface (health always; webhook when configured) ───────────────
    app = create_web_app(manager)
    if settings.webhook_listening:
        attach_webhook(app, dp, bot, settings.webhook_path, settings.webhook_secret)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, settings.listen_host, settings.listen_port)
    await site.start()
    logger.info("http health endpoint on %s:%s/healthz", settings.listen_host, settings.listen_port)

    # ── run ─────────────────────────────────────────────────────────────────
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # windows dev boxes
            pass

    await sync_bot_commands(bot, settings)

    polling_task: asyncio.Task | None = None
    if settings.polling_mode:
        # Clear any lingering webhook from earlier experiments — otherwise
        # Telegram answers every getUpdates with 409 Conflict.
        from aiogram.methods import DeleteWebhook

        try:
            await bot(DeleteWebhook(drop_pending_updates=False))
        except Exception as err:  # noqa: BLE001 — best effort
            logger.warning("could not clear a lingering webhook: %s", err)

        polling_task = asyncio.create_task(
            _poll_forever(
                lambda: dp.start_polling(
                    bot,
                    handle_signals=False,
                    allowed_updates=dp.resolve_used_update_types(),
                ),
                stop,
                name="tellybee-polling",
            ),
            name="tellybee-polling-supervisor",
        )

        # Belt and suspenders: if the supervisor itself ever dies, shut the
        # process down so the container restarts instead of idling forever.
        def _polling_died(task: asyncio.Task) -> None:
            if task.cancelled() or stop.is_set():
                return
            logger.critical(
                "polling supervisor exited unexpectedly (%r) — shutting down",
                task.exception(),
            )
            stop.set()

        polling_task.add_done_callback(_polling_died)
    else:
        await _setup_webhook(bot, settings)
        logger.info("webhook mode: waiting for updates on %s", settings.webhook_path)

    await stop.wait()
    logger.info("shutdown signal received — draining…")

    # ── graceful shutdown ───────────────────────────────────────────────────
    if polling_task is not None:
        polling_task.cancel()
        await asyncio.gather(polling_task, return_exceptions=True)
    if settings.webhook_listening:
        try:
            from aiogram.methods import DeleteWebhook

            await bot(DeleteWebhook(drop_pending_updates=False))
        except Exception:  # noqa: BLE001 — best effort during shutdown
            pass
    await manager.shutdown()
    await notifier.aclose()
    await runner.cleanup()
    await bot.session.close()
    await db.close()
    logger.info("🐝 TellyBee stopped cleanly. Bye!")


def main() -> None:
    if "--check" in sys.argv:
        settings = _load_settings()
        setup_logging(settings.log_level, settings.log_format)
        print(f"✅ configuration valid: {settings.summary()}")
        return
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
