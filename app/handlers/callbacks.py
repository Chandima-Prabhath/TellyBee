"""Inline-keyboard actions — the window's remote control (0.5).

The unified progress window carries a button row on every frame, so the
common actions no longer need typed commands:

``⏸ Pause / ▶️ Resume``  queue toggle (admin, like /pause /resume)
``✖ Cancel all``        cancel THIS chat's downloads (own jobs for users)
``📋 Queue``            live queue view (same scoping as /queue)
``📊 Stats``            all-time statistics (admin, like /stats)
``❓ Help``             the usage guide

The /folder picker carries its own two buttons (One folder /
Default folders) under the same ``tb:`` prefix, and the reboot report
(see app/services/recovery.py) adds ``🔁 Re-queue``.

Every handler ALWAYS answers the callback query (an unanswered button
just spins in the client), and permission tiers mirror the command
handlers exactly. Callbacks arrive on their own update type; the message
auth middleware does not run for them, so the role is resolved here.
"""

from __future__ import annotations

import asyncio
import logging

from aiogram import F, Router
from aiogram.types import CallbackQuery

from app.config import Settings
from app.db import Database
from app.handlers.admin import stats_text
from app.handlers.common import _render_queue, folder_keyboard, folder_text, help_text
from app.queue.manager import DownloadManager, QueueFullError, build_job
from app.services.filenaming import UserRef
from app.utils.formatting import plural_files

logger = logging.getLogger(__name__)

router = Router(name="callbacks")

#: Upper bound on files restored by one Re-queue press — the queue cap
#: still applies, this just keeps the loop obviously finite.
_REQUEUE_MAX = 100


async def _resolve_role(settings: Settings, db: Database, user_id: int) -> str | None:
    """Same priority chain as AuthMiddleware, for callback queries."""
    if user_id in settings.admin_ids:
        return "admin"
    role = await db.get_role(user_id)
    if role in ("admin", "user"):
        return role
    if user_id in settings.allowed_user_ids:
        return "user"
    if settings.open_access:
        return "user"
    return None


def _chat_id(callback: CallbackQuery) -> int | None:
    message = callback.message
    return getattr(getattr(message, "chat", None), "id", None)


async def _answer(callback: CallbackQuery, text: str, *, alert: bool = False) -> None:
    """Answer with a toast; an unanswered button spins forever in the client."""
    try:
        await callback.answer(text, show_alert=alert)
    except Exception:  # noqa: BLE001 — never fail an action over the ack
        logger.debug("callback answer failed: %s", text)


async def _send(callback: CallbackQuery, text: str, notifier=None) -> None:
    """Send a button reply, then keep the live window anchored at the bottom."""
    chat_id = _chat_id(callback)
    if chat_id is None:
        await _answer(callback, "Cannot answer here — ask me in our chat.")
        return
    try:
        await callback.bot.send_message(chat_id, text)
    except Exception as err:  # noqa: BLE001
        logger.warning("could not deliver button reply to chat %s: %s", chat_id, err)
        await _answer(callback, "Could not send the answer — try again in a moment.")
        return
    # The reply just buried the chat's progress window; move it back down.
    reanchor = getattr(notifier, "reanchor", None)
    if reanchor is not None:
        reanchor(chat_id)


@router.callback_query(F.data.startswith("tb:"))
async def on_window_button(
    callback: CallbackQuery,
    settings: Settings,
    db: Database,
    manager: DownloadManager,
    notifier=None,
) -> None:
    user_id = callback.from_user.id
    role = await _resolve_role(settings, db, user_id)
    if role is None:
        await _answer(callback, "⛔️ This bot is private.")
        return

    action = (callback.data or "").removeprefix("tb:")
    try:
        if action == "pause" or action == "resume":
            await _toggle_queue(callback, manager, notifier, role, action)
        elif action == "queue":
            await _show_queue(callback, manager, user_id, role, notifier)
        elif action == "stats":
            await _show_stats(callback, db, manager, settings, role, notifier)
        elif action == "cancel":
            await _cancel_chat_jobs(callback, manager, user_id, role)
        elif action == "help":
            await _answer(callback, "📖 Sending the guide…")
            await _send(callback, help_text(settings), notifier)
        elif action == "req":
            await _requeue_interrupted(callback, settings, db, manager)
        elif action.startswith("fld:"):
            await _set_folder(callback, settings, db, action.removeprefix("fld:"))
        else:
            await _answer(callback, "Unknown button — the window may be from an older version.")
    except Exception:  # noqa: BLE001 — a button must never crash the worker
        logger.exception("callback action %r failed", action)
        await _answer(callback, "Something went wrong — the error was logged.")


# ── individual actions ──────────────────────────────────────────────────────


async def _set_folder(callback: CallbackQuery, settings: Settings, db: Database, mode: str) -> None:
    """The /folder picker buttons: switch the user's destination layout."""
    if mode not in ("s", "d"):
        await _answer(callback, "Unknown button.")
        return
    user = callback.from_user
    if mode == "s":
        # Keep a previously chosen custom folder name, if any.
        _, name = await db.get_folder_pref(user.id)
        await db.set_folder_pref(user.id, "single", name)
        await _answer(callback, "🗂 One folder — new downloads land together.")
    else:
        await db.set_folder_pref(user.id, "default", None)
        await _answer(callback, "📂 Default folders — organized by type & date.")
    # Refresh the picker card in place so the ✅ marker follows the choice.
    chat_id = _chat_id(callback)
    message_id = getattr(callback.message, "message_id", None)
    if chat_id is None or message_id is None:
        return
    fresh_mode, fresh_name = await db.get_folder_pref(user.id)
    try:
        await callback.bot.edit_message_text(
            folder_text(
                settings,
                UserRef(user.id, user.username, user.first_name),
                fresh_mode,
                fresh_name,
            ),
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=folder_keyboard(fresh_mode),
        )
    except Exception:  # noqa: BLE001 — cosmetic only
        logger.debug("could not refresh the /folder card")


async def _toggle_queue(
    callback: CallbackQuery, manager: DownloadManager, notifier, role: str, action: str
) -> None:
    if role != "admin":
        await _answer(callback, "👑 Admins only — ask an admin to pause/resume.")
        return
    if action == "pause":
        manager.pause()
        await _answer(callback, "⏸ Paused — running downloads finish, nothing new starts.")
    else:
        manager.resume()
        await _answer(callback, "▶️ Resumed.")
    # The window's next refresh swaps the button label and the header's
    # paused marker; reset the edit throttle so it happens on the next tick.
    refresh = getattr(notifier, "request_refresh", None)
    if refresh is not None:
        refresh(_chat_id(callback))


async def _show_queue(
    callback: CallbackQuery, manager: DownloadManager, user_id: int, role: str, notifier=None
) -> None:
    await _answer(callback, "📋 Sending the queue…")
    if role == "admin":
        snapshot = manager.snapshot()
    else:
        from app.queue.manager import QueueSnapshot

        active, pending = manager.jobs_for_user(user_id)
        snapshot = QueueSnapshot(
            active=active,
            pending=pending,
            paused=manager.paused,
            workers_total=manager.snapshot().workers_total,
            workers_busy=len(active),
            completed=manager.completed,
            failed=manager.failed,
            cancelled=manager.cancelled,
        )
    await _send(callback, _render_queue(snapshot), notifier)


async def _show_stats(
    callback: CallbackQuery,
    db: Database,
    manager: DownloadManager,
    settings: Settings,
    role: str,
    notifier=None,
) -> None:
    if role != "admin":
        await _answer(callback, "👑 Admins only.")
        return
    await _answer(callback, "📊 Sending the stats…")
    await _send(callback, await stats_text(db, manager, settings), notifier)


async def _cancel_chat_jobs(
    callback: CallbackQuery, manager: DownloadManager, user_id: int, role: str
) -> None:
    chat_id = _chat_id(callback)
    if chat_id is None:
        await _answer(callback, "Cannot cancel from here.")
        return
    snapshot = manager.snapshot()
    targets = [
        job.job_id
        for job in [*snapshot.active, *snapshot.pending]
        if job.chat_id == chat_id and (role == "admin" or job.user.id == user_id)
    ]
    if not targets:
        await _answer(callback, "Nothing to cancel — the queue is already empty.")
        return
    for job_id in targets:
        manager.cancel(job_id)
    await _answer(callback, f"🛑 Cancelling {plural_files(len(targets))}…")


async def _requeue_interrupted(
    callback: CallbackQuery,
    settings: Settings,
    db: Database,
    manager: DownloadManager,
) -> None:
    chat_id = _chat_id(callback)
    if chat_id is None:
        await _answer(callback, "Cannot re-queue from here.")
        return
    rows = await db.interrupted_for_chat(chat_id)
    if not rows:
        await _answer(callback, "Already handled — nothing left to re-queue.")
        await _replace_report(callback, "🔁 Nothing left to re-queue.")
        return

    requeued: list[str] = []
    settled: list[str] = []  # meanwhile completed elsewhere / already queued
    queue_full = False
    for row in rows[:_REQUEUE_MAX]:
        unique_id = row.get("file_unique_id")
        if unique_id and manager.has_file(unique_id):
            settled.append(row["job_id"])
            continue
        if unique_id and await _already_on_disk(settings, db, unique_id):
            settled.append(row["job_id"])
            continue
        job = _job_from_row(row, settings)
        try:
            await manager.submit(job)
            requeued.append(row["job_id"])
        except QueueFullError:
            queue_full = True
            break

    await db.mark_requeued(requeued + settled)

    summary: list[str] = []
    if requeued:
        summary.append(f"🔁 Re-queued {plural_files(len(requeued))} — watch the window.")
    if settled:
        summary.append(f"✅ {plural_files(len(settled))} already saved or in the queue.")
    if queue_full:
        summary.append("🚦 Queue is full — press the button again later for the rest.")
    if not requeued and not settled and not queue_full:
        summary.append("Nothing could be re-queued.")
    text = "\n".join(summary)
    await _answer(callback, text.splitlines()[0])
    await _replace_report(callback, text)


async def _already_on_disk(settings: Settings, db: Database, unique_id: str) -> bool:
    """A completed ledger row whose file still exists = nothing to redo."""
    row = await db.find_completed_by_unique_id(unique_id)
    if row is None:
        return False
    try:
        exists = await asyncio.to_thread((settings.download_dir / row["path"]).is_file)
    except OSError:
        return False
    return exists


def _job_from_row(row: dict, settings: Settings):
    """Rebuild a runnable DownloadJob from an interrupted ledger row."""
    rel_path = row.get("path") or (row.get("file_name") or "file")
    return build_job(
        user=UserRef(row["user_id"], row.get("username"), None),
        chat_id=row["chat_id"],
        message_id=row.get("message_id") or 0,
        album_key=row.get("album_key"),
        media_type=row["media_type"],
        file_id=row["file_id"],
        file_unique_id=row.get("file_unique_id") or "",
        file_name=row.get("file_name"),
        file_size=row.get("file_size"),
        target_path=str(settings.download_dir / rel_path),
        rel_path=rel_path,
    )


async def _replace_report(callback: CallbackQuery, text: str) -> None:
    """Rewrite the report message and drop its button (press handled)."""
    chat_id = _chat_id(callback)
    message_id = getattr(callback.message, "message_id", None)
    if chat_id is None or message_id is None:
        return
    try:
        await callback.bot.edit_message_text(text, chat_id=chat_id, message_id=message_id)
    except Exception:  # noqa: BLE001 — cosmetic only
        logger.debug("could not rewrite the reboot report message")
