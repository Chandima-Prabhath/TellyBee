"""User-facing commands: /start /help /folder /status /queue /cancel."""

from __future__ import annotations

import logging

from aiogram import Router
from aiogram.filters import Command, CommandObject
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.config import Settings
from app.db import Database
from app.queue.jobs import DownloadJob
from app.queue.manager import DownloadManager
from app.services.filenaming import UserRef, sanitize_segment
from app.services.notifier import type_icon
from app.utils.formatting import escape, fmt_eta, pct, plural_files, progress_bar
from app.utils.sizes import human_size, human_speed

logger = logging.getLogger(__name__)

router = Router(name="common")

#: Newline constant usable inside f-string expressions (readability).
NEWLINE = "\n"
#: Thin visual separator between a message header and its body.
DIVIDER = "─" * 22

#: Callback payload prefix shared by every inline button (handlers/callbacks.py).
_CB = "tb:"


# ── /start ─────────────────────────────────────────────────────────────────


@router.message(Command("start"))
async def cmd_start(message: Message, settings: Settings, user_role: str) -> None:
    who = "👑 <b>admin</b>" if user_role == "admin" else "👤 <b>user</b>"
    text = (
        "🐝 <b>TellyBee</b>\n"
        "<i>Your instant file vault — send it, it's saved.</i>\n"
        f"{DIVIDER}\n"
        "Anything you send or forward gets stored on the server:\n"
        "<blockquote>🖼 Photos · 🎬 Videos · 📄 Documents\n"
        "🎵 Audio · 🎙 Voice notes · 🎞 GIFs\n"
        "🏷 Stickers · 🗂 Albums — saved as one batch\n"
        "🌐 Direct links — no Telegram size limit</blockquote>\n"
        "Large downloads run in one live progress window with\n"
        "buttons — nothing to memorize.\n"
        f"{DIVIDER}\n"
        f"Access level: {who}\n"
        "\n<b>Commands</b>\n"
        "/link &lt;url&gt; — grab a direct link\n"
        "/folder — choose where files are saved\n"
        "/status — your recent downloads\n"
        "/queue — live download monitor\n"
        "/cancel &lt;id&gt; — stop a download\n"
        "/help — the full guide"
    )
    if user_role == "admin":
        text += (
            "\n\n<b>Admin</b>\n"
            "/stats · /users · /allow · /disallow\n"
            "/config · /pause · /resume · /health"
        )
    if settings.open_access:
        text += (
            "\n\n⚠️ <i>Open access mode — anyone can use this bot. "
            "Set ADMIN_IDS / ALLOWED_USER_IDS to lock it down.</i>"
        )
    await message.answer(text)


# ── /help ───────────────────────────────────────────────────────────────────


@router.message(Command("help"))
async def cmd_help(message: Message, settings: Settings) -> None:
    await message.answer(help_text(settings))


def help_text(settings: Settings) -> str:
    """The usage guide — shared by /help and the window's Help button."""
    filters = "all supported types"
    if settings.allowed_types:
        filters = f"only: {', '.join(sorted(settings.allowed_types))}"
    elif settings.blocked_types:
        filters = f"all except: {', '.join(sorted(settings.blocked_types))}"
    text = (
        "📖 <b>How to use TellyBee</b>\n"
        f"{DIVIDER}\n"
        "<b>1 · Send files</b>\n"
        "Forward messages, albums, photos, videos, voice notes…\n"
        "Each one is downloaded and saved to the server.\n"
        "\n<b>2 · Or send links</b>\n"
        "Paste any direct http(s) link (or use /link) — it's fetched\n"
        "straight from the source, with no Telegram size limit.\n"
        "\n<b>3 · Watch it happen</b>\n"
        "One live window per chat shows every active download\n"
        "with progress, speed and ETA, plus quick buttons.\n"
        "\n<b>4 · Get receipts</b>\n"
        "When a file lands you get a confirmation with size,\n"
        "speed and its on-disk location.\n"
        "\n🗂 <b>Where files go</b>\n"
        f"Organized as <code>{escape(settings.file_template)}</code> "
        "under the downloads root —\n"
        "or set your own layout with /folder:\n"
        "<blockquote>/folder — pick one of two layouts\n"
        "/folder Show Name — everything into one named folder\n"
        "(great for TV series)</blockquote>\n"
        "\n🎚 <b>Limits</b>\n"
        f"• Max file size: <b>{human_size(settings.max_file_size)}</b>\n"
        f"• Parallel downloads: <b>{settings.download_concurrency}</b>\n"
        f"• Queue capacity: <b>{settings.max_queue_size}</b>\n"
        f"• Accepted: {filters}\n"
    )
    if settings.links_enabled:
        text += f"• Link downloads: on — {settings.link_connections} connections per file\n"
    else:
        text += "• Link downloads: off\n"
    text += "\n💡 Forward a whole album — it is saved as one batch with a single summary receipt."
    return text


# ── /folder ────────────────────────────────────────────────────────────────


@router.message(Command("folder"))
async def cmd_folder(
    message: Message, command: CommandObject, settings: Settings, db: Database
) -> None:
    """Per-user destination layout: template folders vs one flat folder."""
    user = message.from_user
    if user is None:
        return
    argument = (command.args or "").strip()

    if argument:
        lowered = argument.lower()
        if lowered in {"reset", "default", "off"}:
            await db.set_folder_pref(user.id, "default", None)
            await message.answer(
                "📂 <b>Default folders</b> — new downloads are organized\n"
                "by type &amp; date again.\n\n"
                "Change your mind anytime with /folder"
            )
            return
        name = argument[:80]
        await db.set_folder_pref(user.id, "single", name)
        safe = sanitize_segment(name, fallback="folder")
        await message.answer(
            "🗂 <b>One folder</b> — new downloads land together in\n"
            f"<blockquote>📁 <code>{escape(user.username or user.first_name or user.id)}"
            f"/{escape(safe)}</code></blockquote>\n"
            "Perfect for a TV series — every episode in one place,\n"
            "no matter which day you forward it.\n\n"
            "Switch layouts anytime with /folder"
        )
        return

    mode, name = await db.get_folder_pref(user.id)
    await message.answer(
        folder_text(settings, UserRef(user.id, user.username, user.first_name), mode, name),
        reply_markup=folder_keyboard(mode),
    )


def folder_text(settings: Settings, user: UserRef, mode: str, name: str | None) -> str:
    """The /folder picker card — current state plus a tip."""
    if mode == "single":
        dest = user.safe_name + (f"/{sanitize_segment(name, fallback='folder')}" if name else "")
        current = (
            "🗂 <b>One folder — active</b>\n"
            f"Everything lands together in <code>{escape(dest)}</code>"
        )
    else:
        current = (
            "📂 <b>Default folders — active</b>\n"
            f"Organized as <code>{escape(settings.file_template)}</code> "
            "(user / type / date)"
        )
    return (
        "🗂 <b>Download folder</b>\n"
        f"{DIVIDER}\n"
        f"{current}\n"
        "\n💡 <b>Tip for TV series:</b> give the batch its own folder —\n"
        "every episode stays together across days:\n"
        "<blockquote>/folder Severance S02</blockquote>"
    )


def folder_keyboard(mode: str) -> InlineKeyboardMarkup:
    """Two layout choices; the active one is marked."""
    single = mode == "single"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=("✅ " if single else "") + "🗂 One folder",
                    callback_data=_CB + "fld:s",
                ),
                InlineKeyboardButton(
                    text=("✅ " if not single else "") + "📂 Default folders",
                    callback_data=_CB + "fld:d",
                ),
            ],
        ]
    )


# ── /status ─────────────────────────────────────────────────────────────────


@router.message(Command("status"))
async def cmd_status(message: Message, db, user_role: str) -> None:
    history = await db.user_history(message.from_user.id, limit=10)  # type: ignore[attr-defined]
    if not history:
        await message.answer("📭 Nothing downloaded yet — send me something!")
        return
    icon = {
        "completed": "✅",
        "failed": "❌",
        "cancelled": "🚫",
        "running": "⬇",
        "queued": "⏳",
        "interrupted": "💤",
        "requeued": "🔁",
    }
    body = []
    for row in history:
        glyph = icon.get(row["status"], "•")
        name = escape(row["file_name"] or row["media_type"])
        size = human_size(row["file_size"]) if row["file_size"] else ""
        size_part = f" · {size}" if size else ""
        body.append(f"{glyph} {name}{size_part}")
    text = (
        f"📋 <b>Your recent downloads</b>\n{DIVIDER}\n<blockquote>{NEWLINE.join(body)}</blockquote>"
    )
    await message.answer(text)


# ── /queue ──────────────────────────────────────────────────────────────────


@router.message(Command("queue"))
async def cmd_queue(message: Message, manager: DownloadManager, user_role: str) -> None:
    if user_role == "admin":
        await message.answer(_render_queue(manager.snapshot()))
        return
    active, pending = manager.jobs_for_user(message.from_user.id)  # type: ignore[union-attr]
    from app.queue.manager import QueueSnapshot

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
    await message.answer(_render_queue(snapshot))


def _render_queue(snapshot) -> str:
    chips = [f"{snapshot.workers_busy}/{snapshot.workers_total} workers"]
    chips.append(f"{snapshot.completed} done")
    if snapshot.failed:
        chips.append(f"{snapshot.failed} failed")
    if snapshot.cancelled:
        chips.append(f"{snapshot.cancelled} cancelled")
    lines = ["🧮 <b>Download queue</b>", " · ".join(chips)]
    if snapshot.paused:
        lines.append("⏸ <b>Queue is paused</b> — running downloads finish, nothing new starts")
    lines.append(DIVIDER)

    if snapshot.active:
        lines.append("▶️ <b>Active</b>")
        blocks = [_job_line(job) for job in snapshot.active[:8]]
        opening = "blockquote expandable" if len(blocks) > 5 else "blockquote"
        lines.append(f"<{opening}>{NEWLINE.join(blocks)}</{opening}>")
    if snapshot.pending:
        pending_bytes = sum(job.file_size or 0 for job in snapshot.pending)
        size_part = f" · {human_size(pending_bytes)}" if pending_bytes else ""
        lines.append(f"⏳ <b>Pending</b> · {plural_files(len(snapshot.pending))}{size_part}")
        entries = [_pending_line(job) for job in snapshot.pending[:10]]
        hidden = len(snapshot.pending) - 10
        if hidden > 0:
            entries.append(f"… and {hidden} more")
        opening = "blockquote expandable" if len(entries) > 5 else "blockquote"
        lines.append(f"<{opening}>{NEWLINE.join(entries)}</{opening}>")
    if not snapshot.active and not snapshot.pending:
        lines.append("✨ <b>Queue is empty</b> — send me a file!")
    return "\n".join(lines)


def _job_line(job: DownloadJob) -> str:
    """One active job as a two-line block: name + bar/meta.

    The server-side fetch phase of local Bot API mode renders exactly like
    the transfer phase — users just see download progress.
    """
    name = escape(job.file_name or job.rel_path)
    if job.waiting_since is not None and job.fetch_total:
        frac = job.fetch_fraction
        meta = (
            f"{human_size(job.fetch_done or 0)} / {human_size(job.fetch_total)} "
            f"· {human_speed(job.fetch_speed)}"
        )
        if job.fetch_speed > 0 and frac is not None and frac < 1.0:
            meta += f" · ETA {fmt_eta(job.fetch_eta_seconds)}"
    else:
        frac = job.progress_fraction
        meta = (
            f"{human_size(job.bytes_done)} / "
            f"{human_size(job.total_bytes or job.file_size)} · "
            f"{human_speed(job.speed_bps)}"
        )
        if job.speed_bps > 0 and frac is not None and frac < 1.0:
            meta += f" · ETA {fmt_eta(job.eta_seconds)}"
    bar = progress_bar(frac)
    percent = pct(frac)
    return f"{type_icon(job.media_type)} <b>{name}</b>\n<code>{bar} {percent}</code> · {meta}"


def _pending_line(job: DownloadJob) -> str:
    name = escape(job.file_name or job.rel_path)
    size = human_size(job.file_size) if job.file_size else ""
    size_part = f" · {size}" if size else ""
    return f"{type_icon(job.media_type)} {name}{size_part}"


# ── /cancel ─────────────────────────────────────────────────────────────────


@router.message(Command("cancel"))
async def cmd_cancel(
    message: Message, command: CommandObject, manager: DownloadManager, user_role: str
) -> None:
    job_id = (command.argument or "").strip().lstrip("#").lower()
    if not job_id:
        await message.answer(
            "ℹ️ <b>Usage:</b> <code>/cancel &lt;id&gt;</code>\n"
            "Find ids with /queue — they look like <code>#a3f9c2</code>."
        )
        return
    job = manager.find_job(
        job_id,
        user_id=message.from_user.id,
        admin=user_role == "admin",  # type: ignore[union-attr]
    )
    if job is None:
        await message.answer(f"🔍 No live job <code>#{escape(job_id)}</code> (yours).")
        return
    outcome = manager.cancel(job_id)
    if outcome == "running":
        await message.answer(f"🛑 Stopping <code>#{job_id}</code>…")
    elif outcome == "pending":
        await message.answer(f"🗑 Removed <code>#{job_id}</code> from the queue.")
    else:
        await message.answer(f"🔍 Job <code>#{job_id}</code> already finished.")
