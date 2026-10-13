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

from aiohttp import web

from app import __version__
from app.bot_factory import build_bot, build_dispatcher, sync_bot_commands
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

    settings.download_dir.mkdir(parents=True, exist_ok=True)

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
        polling_task = asyncio.create_task(
            dp.start_polling(
                bot,
                handle_signals=False,
                allowed_updates=dp.resolve_used_update_types(),
            ),
            name="tellybee-polling",
        )
        logger.info("polling for updates…")
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
