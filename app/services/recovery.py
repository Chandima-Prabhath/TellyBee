"""Reboot recovery: reconcile the durable record with a fresh process.

The download queue lives in memory (an explicit design decision — the
SQLite ledger is the durable record, not a workflow engine). When the
process dies mid-job, three kinds of mess are left behind:

* ledger rows stuck at ``queued``/``running`` forever,
* half-written ``<name>.part`` files in the downloads tree,
* users staring at silence, with no idea which files died.

This module runs once at startup, right after the database connects and
before polling starts, and fixes all three:

1. every stale row is marked ``interrupted``;
2. every ``.part`` orphan is deleted (no worker exists yet at boot, so
   any ``.part`` is by definition abandoned);
3. each chat that lost work gets ONE report message with a Re-queue
   button — the stored ``file_id``/``file_unique_id``/path of each row is
   everything needed to put the job back into the live queue, so the user
   does not have to re-forward anything.

The report is best-effort: an unreachable chat is logged and skipped,
never fatal — recovery must never keep the bot from starting.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import Settings
from app.db import Database
from app.utils.formatting import escape
from app.utils.sizes import human_size

logger = logging.getLogger(__name__)

#: How many interrupted files are listed by name in the report.
_MAX_LISTED = 8


async def sweep_part_files(root: Path) -> tuple[int, int]:
    """Delete abandoned ``*.part`` files under *root*; (count, bytes).

    Runs only at startup, before any worker exists, so a ``.part`` file
    can never belong to a live transfer. The engine deletes its own
    ``.part`` on every clean cancel/failure path — what is left here is
    exactly the debris of a hard kill. The tree walk runs in a worker
    thread: a deep downloads tree is a blocking filesystem walk.
    """
    return await asyncio.to_thread(_sweep_parts_sync, root)


def _sweep_parts_sync(root: Path) -> tuple[int, int]:
    removed = 0
    freed = 0
    if not root.is_dir():
        return removed, freed
    for part in root.rglob("*.part"):
        try:
            freed += part.stat().st_size
            part.unlink()
            removed += 1
        except OSError as err:
            logger.warning("could not remove orphan %s: %s", part.name, err)
    return removed, freed


def interrupted_report(rows: list[dict]) -> str:
    """The chat-facing summary of what a reboot killed."""
    count = len(rows)
    lines = [
        f"🔁 <b>Rebooted mid-job</b> — {count} download"
        f"{'s' if count != 1 else ''} "
        f"{'were' if count != 1 else 'was'} interrupted:"
    ]
    for row in rows[:_MAX_LISTED]:
        name = escape(row.get("file_name") or row.get("media_type") or "file")
        size = human_size(row["file_size"]) if row.get("file_size") else ""
        suffix = f" — {size}" if size else ""
        lines.append(f"• {name}{suffix}")
    hidden = len(rows) - _MAX_LISTED
    if hidden > 0:
        lines.append(f"… and {hidden} more")
    lines.append("\nRe-send them, or press the button — the queue picks up where it can.")
    return "\n".join(lines)


def requeue_keyboard(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text=f"🔁 Re-queue {count} file{'s' if count != 1 else ''}",
                callback_data="tb:req",
            )
        ]]
    )


async def recover_after_reboot(bot: Bot, settings: Settings, db: Database) -> None:
    """Mark stale rows, sweep orphans, and report per chat. Never raises."""
    try:
        stale = await db.mark_stale_downloads()
    except Exception:  # noqa: BLE001 — recovery must never block startup
        logger.warning("could not mark stale downloads", exc_info=True)
        return
    removed, freed = await sweep_part_files(settings.download_dir)
    if stale:
        logger.info("reboot recovery: %d interrupted job(s) recorded", stale)
    if removed:
        logger.info(
            "reboot recovery: removed %d orphaned .part file(s), %s freed",
            removed, human_size(freed),
        )
    if not stale:
        return

    try:
        chats = await db.chats_with_interrupted()
    except Exception:  # noqa: BLE001
        logger.warning("could not list interrupted chats", exc_info=True)
        return

    for chat_id, _count in chats:
        try:
            rows = await db.interrupted_for_chat(chat_id)
        except Exception:  # noqa: BLE001
            logger.warning("could not list interrupted rows for chat %s", chat_id)
            continue
        if not rows:
            continue
        try:
            await bot.send_message(
                chat_id,
                interrupted_report(rows),
                reply_markup=requeue_keyboard(len(rows)),
            )
        except Exception as err:  # noqa: BLE001 — chat may be gone
            logger.warning(
                "could not deliver the reboot report to chat %s: %s", chat_id, err
            )
