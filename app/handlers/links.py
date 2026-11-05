"""Direct-link intake: ``/link <url>`` and plain messages containing URLs.

Links are downloaded straight from the source server to TellyBee's downloads
folder — Telegram is not involved at all, so there is no Bot API size limit
and the transfer runs at whatever the host's connection allows (servers that
support byte ranges are fetched over several parallel connections — see
``app/services/urlfetch.py``).

Pipeline per URL: validate → probe (name, size, range support, redirects) →
size/disk/duplicate checks → queue a job with ``media_type="url"``. The job
then flows through the exact same engine, receipts, /stats and /cancel
machinery as Telegram files.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re

import aiohttp
from aiogram import F, Router
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from app.config import Settings
from app.db import Database
from app.handlers.media import _reply_duplicates, _reply_rejections, destination_for
from app.queue.errors import DownloadError
from app.queue.manager import DownloadManager, QueueFullError, build_job
from app.services.filenaming import ChatInfo, UserRef
from app.services.media_types import MediaInfo
from app.services.urlfetch import UrlRejected, _RateLimited
from app.utils.formatting import escape
from app.utils.sizes import human_size

logger = logging.getLogger(__name__)

router = Router(name="links")

_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_MAX_URLS_PER_MESSAGE = 5


@router.message(Command("link"))
async def cmd_link(
    message: Message,
    command: CommandObject,
    settings: Settings,
    manager: DownloadManager,
    db: Database | None = None,
) -> None:
    if not settings.links_enabled:
        await message.answer(
            "🔗 <b>Link downloads are disabled</b> on this server\n(LINKS_ENABLED=false)."
        )
        return
    raw = (command.args or "").strip()
    if not raw:
        await message.answer(
            "ℹ️ <b>Usage:</b> <code>/link https://example.com/file.zip</code>\n"
            "Or just send a message containing an http(s) link."
        )
        return
    await _handle_urls(message, [raw], settings, manager, db)


@router.message(F.text)
async def receive_links(
    message: Message,
    settings: Settings,
    manager: DownloadManager,
    db: Database | None = None,
) -> None:
    """Any plain-text message: pick out http(s) URLs and offer to fetch them.

    Messages without a URL fall through to the media router's fallback nudge.
    """
    if not settings.links_enabled:
        return
    urls = _URL_RE.findall(message.text or "")
    if not urls:
        return
    if len(urls) > _MAX_URLS_PER_MESSAGE:
        await message.answer(
            f"🔗 I take at most <b>{_MAX_URLS_PER_MESSAGE}</b> links per message — "
            f"fetching the first {_MAX_URLS_PER_MESSAGE}."
        )
        urls = urls[:_MAX_URLS_PER_MESSAGE]
    await _handle_urls(message, urls, settings, manager, db)


async def _handle_urls(
    message: Message,
    urls: list[str],
    settings: Settings,
    manager: DownloadManager,
    db: Database | None,
) -> None:
    user = message.from_user
    if user is None:  # channel posts — auth middleware normally filters these
        return
    chat_info = ChatInfo(message.chat.id, message.chat.type)

    accepted: list[MediaInfo] = []
    rejections: list[tuple[str, str]] = []
    duplicates: list[tuple[str, str]] = []

    headroom = manager.disk_headroom()
    cap = settings.link_max_size

    for raw in urls:
        label = _short_url(raw)
        try:
            probe = await manager.engine.probe_url(raw)
        except UrlRejected as err:
            rejections.append((label, str(err)))
            continue
        except DownloadError as err:  # links disabled at the engine level
            rejections.append((label, str(err)))
            return
        except _RateLimited as err:
            waited = f" (retry after {err.retry_after:.0f}s)" if err.retry_after else ""
            rejections.append(
                (
                    label,
                    f"the host is rate-limiting us (HTTP {err.status}){waited} — "
                    "try again in a few minutes",
                )
            )
            continue
        except (TimeoutError, aiohttp.ClientError, OSError) as err:
            rejections.append((label, f"unreachable ({type(err).__name__})"))
            continue

        name = probe.filename or label
        if cap and probe.size and probe.size > cap:
            rejections.append(
                (
                    name,
                    f"{human_size(probe.size)} exceeds the LINK_MAX_SIZE cap of {human_size(cap)}",
                )
            )
            continue
        if probe.size and headroom is not None and probe.size > headroom:
            rejections.append(
                (
                    name,
                    f"not enough disk space — need {human_size(probe.size)}, "
                    f"only {human_size(max(headroom, 0))} free on the downloads volume",
                )
            )
            continue

        unique_id = hashlib.sha256(probe.final_url.encode()).hexdigest()[:32]
        saved = await _already_saved(db, settings, manager, unique_id)
        if saved is not None:
            duplicates.append((name, saved))
            continue

        media = MediaInfo(
            kind="url",
            file_id=probe.final_url,
            file_unique_id=unique_id,
            file_name=probe.filename,
            file_size=probe.size,
            mime_type=probe.content_type,
        )
        accepted.append(media)
        if probe.size and headroom is not None:
            headroom -= probe.size

    if not accepted:
        if rejections:
            await _reply_rejections(message, rejections, partial=False)
        if duplicates:
            await _reply_duplicates(message, duplicates)
        return
    if rejections:
        await _reply_rejections(message, rejections, partial=True)
    if duplicates:
        await _reply_duplicates(message, duplicates)

    user_ref = UserRef(user.id, user.username, user.first_name)
    queued_ids: list[str] = []
    for media in accepted:
        target, rel = await destination_for(settings, db, user_ref, media, chat_info)
        job = build_job(
            user=user_ref,
            chat_id=message.chat.id,
            message_id=message.message_id,
            album_key=None,
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
        "accepted %d link(s) from user %s in chat %s",
        len(queued_ids),
        user.id,
        message.chat.id,
    )


async def _already_saved(
    db: Database | None, settings: Settings, manager: DownloadManager, unique_id: str
) -> str | None:
    """Rel path where this exact URL was already saved, else None."""
    if manager.has_file(unique_id):
        return "(already in the queue)"
    if db is None:
        return None
    row = await db.find_completed_by_unique_id(unique_id)
    if row is None:
        return None
    path = settings.download_dir / row["path"]
    try:
        exists = await asyncio.to_thread(path.is_file)
    except OSError:
        return None
    return row["path"] if exists else None


def _short_url(url: str) -> str:
    """Chat-friendly, secret-free label for a URL (host + last path bit)."""
    from app.services.urlfetch import strip_query

    cleaned = strip_query(url)
    parts = cleaned.rstrip("/").rsplit("/", 1)
    tail = parts[-1][:40] if len(parts) > 2 or "." in (parts[-1] or "") else ""
    host = parts[0].replace("https://", "").replace("http://", "").split("/")[0]
    return f"{tail} ({host})" if tail and tail != host else host
