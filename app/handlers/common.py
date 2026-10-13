"""User-facing commands: /start /help /status /queue /cancel."""

from __future__ import annotations

import logging

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from app.config import Settings
from app.queue.jobs import DownloadJob, JobStatus
from app.queue.manager import DownloadManager
from app.services.notifier import type_icon
from app.utils.formatting import escape, pct, progress_bar
from app.utils.sizes import human_size, human_speed

logger = logging.getLogger(__name__)

router = Router(name="common")

_STATUS_ICONS = {
    JobStatus.QUEUED: "⏳",
    JobStatus.RUNNING: "⬇️",
    JobStatus.COMPLETED: "✅",
    JobStatus.FAILED: "❌",
    JobStatus.CANCELLED: "🚫",
}


@router.message(Command("start"))
async def cmd_start(message: Message, settings: Settings, user_role: str) -> None:
    who = "👑 admin" if user_role == "admin" else "👤 user"
    text = (
        "🐝 <b>TellyBee</b> — file downloader\n\n"
        "Send or forward me any file or media and I'll save it to the server:\n"
        "🖼 photos · 🎬 videos · 📄 documents · 🎵 audio · 🎙 voice\n"
        "🎞 GIFs · 🏷 stickers · albums & media groups\n\n"
        f"Your access: <b>{who}</b>\n\n"
        "🤖 <b>Commands</b>\n"
        "/help — usage guide\n"
        "/status — your recent downloads\n"
        "/queue — live download monitor\n"
        "/cancel &lt;id&gt; — cancel a download\n"
    )
    if user_role == "admin":
        text += (
            "\n👑 <b>Admin</b>\n"
            "/stats — all-time statistics\n"
            "/users — access list\n"
            "/allow /disallow — manage users\n"
            "/config — current settings\n"
            "/pause /resume — pause the queue\n"
        )
    if settings.open_access:
        text += (
            "\n⚠️ <i>Open access mode — anyone can use this bot. "
            "Set ADMIN_IDS / ALLOWED_USER_IDS to lock it down.</i>"
        )
    await message.answer(text)


@router.message(Command("help"))
async def cmd_help(message: Message, settings: Settings) -> None:
    filters = "all supported types"
    if settings.allowed_types:
        filters = f"only: {', '.join(sorted(settings.allowed_types))}"
    elif settings.blocked_types:
        filters = f"all except: {', '.join(sorted(settings.blocked_types))}"
    text = (
        "📖 <b>How to use TellyBee</b>\n\n"
        "1. Send or forward files, photos, videos, voice notes…\n"
        "2. Watch live progress for large transfers.\n"
        "3. Get a receipt with size, speed and location when done.\n\n"
        "🗂 <b>Storage</b>\n"
        f"Files land in <code>{escape(settings.file_template)}</code> "
        f"under the downloads root.\n\n"
        "🎚 <b>Limits</b>\n"
        f"• Max file size: {human_size(settings.max_file_size)}\n"
        f"• Parallel downloads: {settings.download_concurrency}\n"
        f"• Queue capacity: {settings.max_queue_size}\n"
        f"• Accepted: {filters}\n\n"
        "💡 Forward a whole album — it is saved as one batch with a single "
        "summary receipt."
    )
    await message.answer(text)


@router.message(Command("status"))
async def cmd_status(message: Message, db, user_role: str) -> None:
    history = await db.user_history(message.from_user.id, limit=10)  # type: ignore[attr-defined]
    if not history:
        await message.answer("📭 Nothing downloaded yet. Send me something! 🐝")
        return
    lines = ["📋 <b>Your recent downloads</b>\n"]
    for row in history:
        icon = {"completed": "✅", "failed": "❌", "cancelled": "🚫",
                "running": "⬇️", "queued": "⏳"}.get(row["status"], "•")
        name = escape(row["file_name"] or row["media_type"])
        size = human_size(row["file_size"]) if row["file_size"] else ""
        lines.append(f"{icon} {name} {size}")
    await message.answer("\n".join(lines))


@router.message(Command("queue"))
async def cmd_queue(message: Message, manager: DownloadManager, user_role: str) -> None:
    if user_role == "admin":
        await message.answer(_render_queue(manager.snapshot()))
        return
    active, pending = manager.jobs_for_user(message.from_user.id)  # type: ignore[union-attr]
    from app.queue.manager import QueueSnapshot
    snapshot = QueueSnapshot(
        active=active, pending=pending, paused=manager.paused,
        workers_total=manager.snapshot().workers_total, workers_busy=len(active),
        completed=manager.completed, failed=manager.failed, cancelled=manager.cancelled,
    )
    await message.answer(_render_queue(snapshot))


def _render_queue(snapshot) -> str:
    lines = ["🧮 <b>Download queue</b>\n"]

    if snapshot.paused:
        lines.append("⏸ <b>Queue is PAUSED</b> (admin can /resume)\n")

    lines.append(f"⚙️ Workers: {snapshot.workers_busy}/{snapshot.workers_total} busy")
    lines.append(
        f"📈 Session: {snapshot.completed} done · {snapshot.failed} failed · "
        f"{snapshot.cancelled} cancelled\n"
    )

    if snapshot.active:
        lines.append("▶️ <b>Active</b>")
        for job in snapshot.active[:8]:
            lines.append(_job_line(job))
    if snapshot.pending:
        lines.append("\n⏳ <b>Pending</b>")
        for job in snapshot.pending[:10]:
            lines.append(_pending_line(job))
        hidden = len(snapshot.pending) - 10
        if hidden > 0:
            lines.append(f"… and {hidden} more")
    if not snapshot.active and not snapshot.pending:
        lines.append("✨ Queue is empty — send me a file!")
    return "\n".join(lines)


def _job_line(job: DownloadJob) -> str:
    name = escape(job.file_name or job.rel_path)
    bar = progress_bar(job.progress_fraction)
    line = (
        f"<code>#{job.job_id}</code> {type_icon(job.media_type)} {name}\n"
        f"   <code>{bar} {pct(job.progress_fraction)}</code> "
        f"{human_size(job.bytes_done)}/{human_size(job.total_bytes or job.file_size)} "
        f"• {human_speed(job.speed_bps)}"
    )
    return line


def _pending_line(job: DownloadJob) -> str:
    name = escape(job.file_name or job.rel_path)
    size = human_size(job.file_size)
    return f"<code>#{job.job_id}</code> {type_icon(job.media_type)} {name} — {size}"


@router.message(Command("cancel"))
async def cmd_cancel(
    message: Message, command: CommandObject, manager: DownloadManager, user_role: str
) -> None:
    job_id = (command.argument or "").strip().lstrip("#").lower()
    if not job_id:
        await message.answer(
            "ℹ️ Usage: <code>/cancel &lt;id&gt;</code>\n"
            "Find ids with /queue (they look like <code>#a3f9c2</code>)."
        )
        return
    job = manager.find_job(
        job_id, user_id=message.from_user.id, admin=user_role == "admin"  # type: ignore[union-attr]
    )
    if job is None:
        await message.answer(f"🔍 No live job <code>#{escape(job_id)}</code> (yours).")
        return
    outcome = manager.cancel(job_id)
    if outcome == "running":
        await message.answer(f"🛑 Stopping <code>#{job_id}</code> …")
    elif outcome == "pending":
        await message.answer(f"🗑 Removed <code>#{job_id}</code> from the queue.")
    else:
        await message.answer(f"🔍 Job <code>#{job_id}</code> already finished.")
