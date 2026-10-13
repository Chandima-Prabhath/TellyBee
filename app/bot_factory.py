"""Bot & dispatcher factories."""

from __future__ import annotations

import logging

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.enums import ParseMode
from aiogram.types import BotCommand, BotCommandScopeChat, BotCommandScopeDefault

from app.config import Settings
from app.db import Database
from app.handlers import build_router
from app.middleware.album import AlbumMiddleware
from app.middleware.auth import AuthMiddleware
from app.middleware.throttling import ThrottlingMiddleware
from app.queue.manager import DownloadManager

logger = logging.getLogger(__name__)

#: The one true cloud Bot API endpoint (used for the logOut handshake).
CLOUD_API_BASE = "https://api.telegram.org"

_USER_COMMANDS = [
    BotCommand(command="start", description="Welcome & capabilities"),
    BotCommand(command="help", description="Usage guide and limits"),
    BotCommand(command="status", description="Your recent downloads"),
    BotCommand(command="queue", description="Live download monitor"),
    BotCommand(command="cancel", description="Cancel a download by id"),
]

_ADMIN_COMMANDS = _USER_COMMANDS + [
    BotCommand(command="stats", description="All-time statistics"),
    BotCommand(command="users", description="Access list"),
    BotCommand(command="allow", description="Authorize a user"),
    BotCommand(command="disallow", description="Revoke a user"),
    BotCommand(command="config", description="Current settings"),
    BotCommand(command="pause", description="Pause the download queue"),
    BotCommand(command="resume", description="Resume the download queue"),
    BotCommand(command="health", description="Bot health snapshot"),
]


async def cloud_logout(token: str, *, base: str = CLOUD_API_BASE, timeout: float = 20.0) -> bool:
    """Log the bot out of the cloud Bot API — required before a local server serves it.

    Telegram's handshake rule for local Bot API servers: the bot must call
    ``logOut`` on ``api.telegram.org`` once, otherwise the local server
    answers every request with 404. Safe to call on every startup — an
    already-logged-out bot simply yields an error response that we ignore.

    Never raises: this is a best-effort one-off HTTP call, not a blocker.
    Returns True when the cloud acknowledged the logout.
    """
    url = f"{base.rstrip('/')}/bot{token}/logOut"
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as http:
            async with http.post(url) as resp:
                try:
                    payload = await resp.json(content_type=None)
                except ValueError:
                    payload = {}
                if payload.get("ok"):
                    logger.info(
                        "cloud logOut acknowledged — the local Bot API server can now "
                        "serve this bot (switching back to the cloud API takes ~10 minutes)"
                    )
                    return True
                logger.info(
                    "cloud logOut response: %s — fine, the bot is probably already "
                    "logged out",
                    payload.get("description") or f"HTTP {resp.status}",
                )
                return False
    except Exception as err:  # noqa: BLE001 — never block startup on this
        logger.warning(
            "could not reach the cloud Bot API for the logOut handshake (%s) — "
            "continuing; if the local server answers 404, restart after a minute",
            err,
        )
        return False


def build_bot(settings: Settings) -> Bot:
    session = None
    if settings.api_base:
        api = TelegramAPIServer.from_base(
            settings.api_base.rstrip("/"), is_local=settings.api_base.startswith("http://")
        )
        session = AiohttpSession(api=api)
        logger.info("using custom Bot API server: %s", settings.api_base)
    return Bot(
        token=settings.bot_token,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )


def build_dispatcher(settings: Settings, db: Database, manager: DownloadManager) -> Dispatcher:
    dp = Dispatcher(settings=settings, db=db, manager=manager)
    dp.include_router(build_router())

    # Outer middleware order = execution order:
    # throttling → auth → album buffering → filters → handlers.
    dp.message.outer_middleware(ThrottlingMiddleware(settings))
    dp.message.outer_middleware(AuthMiddleware(settings, db))
    dp.message.outer_middleware(AlbumMiddleware())

    @dp.errors()
    async def on_error(event, exception: Exception) -> bool:
        logger.exception("unhandled error processing update: %s", exception)
        # Inform the originating chat when we can identify it.
        update = getattr(event, "update", None)
        message = getattr(update, "message", None)
        if message is not None:
            try:
                await message.answer(
                    "🛑 Something went wrong processing that update. "
                    "The error was logged; try again in a moment."
                )
            except Exception:  # noqa: BLE001
                pass
        return True  # mark as handled

    return dp


async def sync_bot_commands(bot: Bot, settings: Settings) -> None:
    """Publish the command menu (default scope + per-admin scope)."""
    try:
        await bot.set_my_commands(_USER_COMMANDS, scope=BotCommandScopeDefault())
        for admin_id in settings.admin_ids:
            await bot.set_my_commands(_ADMIN_COMMANDS, scope=BotCommandScopeChat(chat_id=admin_id))
        logger.info("command menu synced (%d admin scopes)", len(settings.admin_ids))
    except Exception as err:  # noqa: BLE001
        logger.warning("could not sync command menu: %s", err)
