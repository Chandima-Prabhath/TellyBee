"""Admin-only commands (guarded by the IsAdmin router filter)."""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

from aiogram import Router
from aiogram.filters import BaseFilter, Command, CommandObject
from aiogram.types import Message

from app.config import Settings
from app.db import Database
from app.queue.manager import DownloadManager
from app.services.notifier import type_icon
from app.utils.formatting import escape
from app.utils.sizes import human_size, human_speed

logger = logging.getLogger(__name__)


class IsAdmin(BaseFilter):
    """Router-level guard: only updates with data['user_role'] == 'admin'."""

    async def __call__(self, message: Message, user_role: str | None = None) -> bool:
        return user_role == "admin"


router = Router(name="admin")
router.message.filter(IsAdmin())


@router.message(Command("stats"))
async def cmd_stats(
    message: Message, db: Database, manager: DownloadManager, settings: Settings
) -> None:
    stats = await db.global_stats()
    totals = stats["totals"]
    lines = ["📊 <b>TellyBee stats</b>\n"]

    lines.append(
        f"💾 <b>All-time</b>\n"
        f"• Files saved: {totals['completed']}\n"
        f"• Volume: {human_size(totals['total_bytes'])}\n"
        f"• Failed: {totals['failed']} · Cancelled: {totals['cancelled']}\n"
        f"• Average speed: {human_speed(totals['avg_speed'])}"
    )

    last24 = stats["last_24h"]
    lines.append(
        f"\n⏱ <b>Last 24h</b>\n"
        f"• {last24['count']} files · {human_size(last24['bytes'])}"
    )

    if stats["by_type"]:
        lines.append("\n🗂 <b>By type</b>")
        for row in stats["by_type"][:8]:
            lines.append(
                f"{type_icon(row['media_type'])} {row['media_type']}: "
                f"{row['count']} · {human_size(row['bytes'])}"
            )

    if stats["top_users"]:
        lines.append("\n🏆 <b>Top savers</b>")
        for row in stats["top_users"]:
            name = escape(str(row["name"]))
            lines.append(f"• {name}: {row['count']} · {human_size(row['bytes'])}")

    snapshot = manager.snapshot()
    lines.append(
        f"\n⚙️ <b>Now</b>\n"
        f"• Workers {snapshot.workers_busy}/{snapshot.workers_total} · "
        f"active {len(snapshot.active)} · pending {len(snapshot.pending)}"
        + (" · <b>PAUSED</b>" if snapshot.paused else "")
    )

    # Disk reality check: a big file occupies roughly 2x its size across the
    # botapi cache volume + the downloads dir, so operators need both numbers.
    disk_parts = []
    for label, path in (("downloads", settings.download_dir), ("botapi cache", None)):
        target = path if path is not None else _botapi_cache_dir()
        if target is None:
            continue
        try:
            usage = await asyncio.to_thread(shutil.disk_usage, str(target))
        except OSError:
            continue
        disk_parts.append(f"{label} {human_size(usage.free)} free")
    if disk_parts:
        lines.append(f"💾 <b>Disk</b>\n• {' · '.join(disk_parts)}")

    await message.answer("\n".join(lines))


def _botapi_cache_dir():
    """The local Bot API server's storage, when mounted in this container."""
    cache = Path("/var/lib/telegram-bot-api")
    return cache if cache.is_dir() else None


@router.message(Command("users"))
async def cmd_users(message: Message, db: Database) -> None:
    users = await db.list_users()
    if not users:
        await message.answer("👥 No users configured — bot is in open access mode.")
        return
    lines = ["👥 <b>Access list</b>\n"]
    for row in users:
        icon = "👑" if row["role"] == "admin" else "👤"
        name = escape(row["first_name"] or row["username"] or "")
        handle = f"@{escape(row['username'])}" if row["username"] else ""
        lines.append(f"{icon} <code>{row['user_id']}</code> {name} {handle}")
    lines.append("\n💡 /allow &lt;id&gt; or reply /allow to authorize someone.")
    await message.answer("\n".join(lines))


@router.message(Command("allow"))
async def cmd_allow(
    message: Message, command: CommandObject, db: Database, settings: Settings
) -> None:
    target = _target_user(message, command)
    if target is None:
        await message.answer(
            "ℹ️ Usage: <code>/allow 123456789</code> or reply /allow to their message."
        )
        return
    user_id, username, first_name = target
    await db.upsert_user(
        user_id, username=username, first_name=first_name, role="user",
        added_by=message.from_user.id,
    )
    await message.answer(
        f"✅ Authorized <code>{user_id}</code>"
        + (f" ({escape(first_name or username or '')})" if (first_name or username) else "")
        + " — they can now send files."
    )
    logger.info("admin %s allowed user %s", message.from_user.id, user_id)


@router.message(Command("disallow"))
async def cmd_disallow(message: Message, command: CommandObject, db: Database) -> None:
    target = _target_user(message, command, forward=False)
    if target is None:
        await message.answer("ℹ️ Usage: <code>/disallow 123456789</code>")
        return
    user_id, _, _ = target
    removed = await db.remove_user(user_id)
    if removed:
        await message.answer(f"🗑 Revoked access for <code>{user_id}</code>.")
    else:
        await message.answer(f"🤔 User <code>{user_id}</code> wasn't in the access list "
                             "(env-configured users must be removed from ALLOWED_USER_IDS).")


def _target_user(message: Message, command: CommandObject, forward: bool = True):
    """Resolve the target of /allow|/disallow: numeric arg, or the replied-to/forwarded author."""
    arg = (command.args or "").strip()
    if arg.lstrip("-").isdigit():
        return int(arg), None, None
    if message.reply_to_message and message.reply_to_message.from_user:
        user = message.reply_to_message.from_user
        return user.id, user.username, user.first_name
    if forward and message.forward_origin:
        # aiogram 3.31: ForwardOrigin variants expose sender_user
        origin = message.forward_origin
        sender = getattr(origin, "sender_user", None)
        if sender:
            return sender.id, sender.username, sender.first_name
    return None


@router.message(Command("config"))
async def cmd_config(message: Message, settings: Settings) -> None:
    mode = "webhook" if settings.webhook_listening else "polling"
    filters = "all"
    if settings.allowed_types:
        filters = f"only {', '.join(sorted(settings.allowed_types))}"
    elif settings.blocked_types:
        filters = f"except {', '.join(sorted(settings.blocked_types))}"
    text = (
        "⚙️ <b>Configuration</b> <i>(read-only — change via .env and restart)</i>\n\n"
        f"🤖 Mode: <b>{mode}</b>\n"
        f"📁 Download dir: <code>{escape(settings.download_dir.name)}</code>\n"
        f"🗂 Template: <code>{escape(settings.file_template)}</code>\n"
        f"⚡ Workers: <b>{settings.download_concurrency}</b> · "
        f"retries {settings.download_retries}\n"
        f"🚦 Queue cap: {settings.max_queue_size} · "
        f"throttle {settings.throttle_rate}/{int(settings.throttle_window)}s\n"
        f"📦 Max file: {human_size(settings.max_file_size)}\n"
        f"🎚 Types: {filters}\n"
        f"👑 Admins: {len(settings.admin_ids)} · allowed: {len(settings.allowed_user_ids)} · "
        f"open access: {'yes' if settings.open_access else 'no'}"
    )
    if settings.api_base:
        text += f"\n🌐 Custom Bot API: <code>{escape(settings.api_base)}</code>"
    await message.answer(text)


@router.message(Command("pause"))
async def cmd_pause(message: Message, manager: DownloadManager) -> None:
    manager.pause()
    await message.answer(
        "⏸ <b>Queue paused</b> — running downloads finish, nothing new starts.\n"
        "Use /resume to continue."
    )


@router.message(Command("resume"))
async def cmd_resume(message: Message, manager: DownloadManager) -> None:
    manager.resume()
    await message.answer("▶️ <b>Queue resumed</b>.")


@router.message(Command("health"))
async def cmd_health(message: Message, manager: DownloadManager) -> None:
    """Same info as the HTTP /healthz endpoint, delivered in-chat."""
    snapshot = manager.snapshot()
    text = (
        "❤️ <b>Health</b>\n"
        f"• Workers: {snapshot.workers_busy}/{snapshot.workers_total} busy\n"
        f"• Active: {len(snapshot.active)} · Pending: {len(snapshot.pending)}\n"
        f"• Session: {snapshot.completed} done · {snapshot.failed} failed\n"
        f"• State: {'PAUSED' if snapshot.paused else 'running'}"
    )
    await message.answer(text)
