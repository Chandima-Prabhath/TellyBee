"""Media intake: turns incoming files/media into queued download jobs."""

from __future__ import annotations

import asyncio
import logging
import time

from aiogram import F, Router
from aiogram.types import Message

from app.config import Settings
from app.db import Database
from app.queue.manager import DownloadManager, QueueFullError, build_job
from app.services.filenaming import ChatInfo, UserRef, resolve_destination
from app.services.media_types import extract_media
from app.utils.formatting import escape
from app.utils.sizes import human_size

logger = logging.getLogger(__name__)

router = Router(name="media")

_MEDIA_FILTER = (
    F.photo | F.video | F.document | F.audio | F.voice | F.video_note | F.sticker | F.animation
)

_nudge_cache: dict[int, float] = {}


@router.message(_MEDIA_FILTER)
async def receive_media(
    message: Message,
    settings: Settings,
    manager: DownloadManager,
    db: Database | None = None,
    album: list[Message] | None = None,
) -> None:
    album = album if album else [message]
    user = message.from_user
    chat_info = ChatInfo(message.chat.id, message.chat.type)

    accepted: list[tuple[Message, object]] = []
    rejections: list[tuple[str, str]] = []
    duplicates: list[tuple[str, str]] = []  # (name, saved rel path)

    # One headroom check per intake burst; decremented per accepted file so
    # an album can't overcommit the disk either.
    headroom = manager.disk_headroom()

    for msg in album:
        media = extract_media(msg)
        if media is None:
            continue
        reason = _filter_reason(settings, media)
        if reason:
            rejections.append((media.file_name or media.kind, reason))
            continue
        if media.file_size and headroom is not None and media.file_size > headroom:
            rejections.append(
                (
                    media.file_name or media.kind,
                    f"not enough disk space — need {human_size(media.file_size)}, "
                    f"only {human_size(max(headroom, 0))} free on the downloads volume",
                )
            )
            continue
        saved = await _already_saved(db, settings, manager, media)
        if saved is not None:
            duplicates.append((media.file_name or media.kind, saved))
            continue
        accepted.append((msg, media))
        if media.file_size and headroom is not None:
            headroom -= media.file_size

    # Everything rejected → explain politely and stop.
    if accepted:
        if rejections:
            await _reply_rejections(message, rejections, partial=True)
    else:
        await _reply_rejections(message, rejections, partial=False)
        if duplicates:
            await _reply_duplicates(message, duplicates)
        return
    if duplicates:
        await _reply_duplicates(message, duplicates)

    album_key = None
    if album and album[0].media_group_id:
        album_key = f"{message.chat.id}:{album[0].media_group_id}"

    queued_ids: list[str] = []
    for msg, media in accepted:
        user_ref = UserRef(user.id, user.username, user.first_name)
        target, rel = resolve_destination(
            settings.download_dir, settings.file_template, user_ref, media, chat_info
        )
        job = build_job(
            user=user_ref,
            chat_id=message.chat.id,
            message_id=msg.message_id,
            album_key=album_key,
            media_type=media.kind,
            file_id=media.file_id,
            file_unique_id=media.file_unique_id,
            file_name=media.file_name,
            file_size=media.file_size,
            target_path=str(target),
            rel_path=rel,
        )
        try:
            await manager.submit(job)
            queued_ids.append(job.job_id)
        except QueueFullError as err:
            await message.answer(
                f"🚦 <b>Queue is full</b> — {escape(str(err))}.\n"
                "Try again in a minute, or ask an admin to raise MAX_QUEUE_SIZE."
            )
            return

    logger.info(
        "accepted %d file(s) from user %s in chat %s",
        len(queued_ids), user.id, message.chat.id,
    )


def _filter_reason(settings: Settings, media) -> str | None:
    """Return a human reason why this media is not acceptable, else None."""
    if media.kind in settings.blocked_types:
        return "type blocked"
    if settings.allowed_types and media.kind not in settings.allowed_types:
        return "type not allowed"
    if media.file_size and media.file_size > settings.max_file_size:
        return f"{human_size(media.file_size)} exceeds {human_size(settings.max_file_size)} limit"
    return None


async def _already_saved(
    db: Database | None, settings: Settings, manager: DownloadManager, media
) -> str | None:
    """Rel path where this exact Telegram file was already saved, else None.

    ``file_unique_id`` is stable across re-sends and forwards, so a completed
    ledger row whose file is still on disk means a re-download would produce
    the identical bytes — skip it with a friendly note instead. Files deleted
    from disk fall through and get re-downloaded.
    """
    if not media.file_unique_id:
        return None
    if manager.has_file(media.file_unique_id):
        return "(already in the queue)"
    if db is None:
        return None
    row = await db.find_completed_by_unique_id(media.file_unique_id)
    if row is None:
        return None
    path = settings.download_dir / row["path"]
    try:
        exists = await asyncio.to_thread(path.is_file)
    except OSError:
        return None
    return row["path"] if exists else None


async def _reply_duplicates(message: Message, duplicates: list[tuple[str, str]]) -> None:
    lines = [f"♻️ <b>Already saved</b> — skipped {len(duplicates)} duplicate(s)"]
    for name, saved in duplicates[:8]:
        lines.append(f"• {escape(name)} → <code>{escape(saved)}</code>")
    if len(duplicates) > 8:
        lines.append(f"… and {len(duplicates) - 8} more")
    await message.answer("\n".join(lines))


async def _reply_rejections(message: Message, rejections, partial: bool) -> None:
    header = "⚠️ <b>Skipped</b>" if partial else "⛔️ <b>Can't accept</b>"
    lines = [header]
    for name, reason in rejections[:8]:
        lines.append(f"• {escape(name)} — {reason}")
    if len(rejections) > 8:
        lines.append(f"… and {len(rejections) - 8} more")
    await message.answer("\n".join(lines))


@router.message()
async def fallback(message: Message) -> None:
    """Unknown text — nudge users once in a while instead of nagging."""
    user_id = message.from_user.id if message.from_user else 0
    now = time.monotonic()
    if now - _nudge_cache.get(user_id, 0.0) < 120.0:
        return
    _nudge_cache[user_id] = now
    await message.answer(
        "🤖 I only understand files and media.\n"
        "Send me a photo, video, document… or try /help 🐝"
    )
