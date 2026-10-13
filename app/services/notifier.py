"""In-chat progress & receipt UX.

Groups:

* A **single file** forms its own group of one job.
* An **album** (media group) forms one group with N jobs → exactly one chat
  message is created and edited through the whole lifecycle, ending in a
  combined receipt.

Messages are created lazily — only when a group's total size reaches
``PROGRESS_MIN_BYTES`` — so small files never spam the chat with progress
messages that would be instantly replaced by the receipt.
"""

from __future__ import annotations

import asyncio
import logging
import time

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter

from app.config import Settings
from app.queue.jobs import DownloadJob, JobStatus
from app.utils.formatting import escape, fmt_eta, pct, plural_files, progress_bar
from app.utils.sizes import human_size, human_speed

logger = logging.getLogger(__name__)

_TYPE_ICONS = {
    "photo": "🖼",
    "video": "🎬",
    "document": "📄",
    "audio": "🎵",
    "voice": "🎙",
    "video_note": "⭕️",
    "sticker": "🏷",
    "animation": "🎞",
}


def type_icon(media_type: str) -> str:
    return _TYPE_ICONS.get(media_type, "📁")


class _Group:
    """A set of jobs sharing one chat message."""

    __slots__ = ("key", "chat_id", "message_id", "jobs", "results", "lock",
                 "updater_task", "created_at", "broken", "_last_edit")

    def __init__(self, key: str, chat_id: int):
        self.key = key
        self.chat_id = chat_id
        self.message_id: int | None = None
        self.jobs: dict[str, DownloadJob] = {}
        self.results: list[DownloadJob] = []
        self.lock = asyncio.Lock()
        self.updater_task: asyncio.Task | None = None
        self.created_at = time.monotonic()
        self.broken = False  # set when sending failed (chat unreachable)
        self._last_edit = 0.0  # last progress-message edit (throttle)

    @property
    def total_bytes(self) -> int:
        return sum(j.total_bytes or j.file_size or 0 for j in self.jobs.values())

    @property
    def bytes_done(self) -> int:
        return sum(j.bytes_done for j in self.jobs.values())

    @property
    def expected_bytes(self) -> int:
        """Best-known total (declared size of every job, 0 when unknown)."""
        return sum((j.total_bytes or j.file_size or 0) for j in self.jobs.values())

    @property
    def fraction(self) -> float | None:
        total = self.expected_bytes
        return self.bytes_done / total if total else None

    @property
    def speed(self) -> float:
        return sum(j.speed_bps for j in self.jobs.values() if j.status is JobStatus.RUNNING)

    @property
    def any_running(self) -> bool:
        return any(j.status is JobStatus.RUNNING for j in self.jobs.values())

    def eta(self) -> float | None:
        speed = self.speed
        total = self.expected_bytes
        if not speed or not total:
            return None
        return max(0.0, (total - self.bytes_done) / speed)


class Notifier:
    """Turns queue events into chat messages."""

    def __init__(self, bot: Bot, settings: Settings):
        self._bot = bot
        self._settings = settings
        self._groups: dict[str, _Group] = {}

    async def aclose(self) -> None:
        for group in list(self._groups.values()):
            if group.updater_task and not group.updater_task.done():
                group.updater_task.cancel()
        await asyncio.gather(
            *(g.updater_task for g in self._groups.values() if g.updater_task),
            return_exceptions=True,
        )
        self._groups.clear()

    # ── events from the manager ─────────────────────────────────────────────

    def attach(self, job: DownloadJob) -> None:
        """Register a freshly queued job with its (possibly shared) group."""
        key = job.album_key or f"job:{job.job_id}"
        group = self._groups.get(key)
        if group is None:
            group = _Group(key, job.chat_id)
            self._groups[key] = group
            group.updater_task = asyncio.create_task(
                self._updater(group), name=f"tellybee-notify-{key}"
            )
        group.jobs[job.job_id] = job

    async def finish(self, job: DownloadJob) -> None:
        """A job reached a terminal state; finalize the group when last."""
        key = job.album_key or f"job:{job.job_id}"
        group = self._groups.get(key)
        if group is None:  # e.g. submit failed midway; nothing to show
            return
        group.jobs.pop(job.job_id, None)
        group.results.append(job)
        if not group.jobs:
            if group.updater_task and not group.updater_task.done():
                group.updater_task.cancel()
            await self._finalize_group(group)
            self._groups.pop(key, None)

    # ── internals ───────────────────────────────────────────────────────────

    async def _updater(self, group: _Group) -> None:
        """Periodically refresh a group's progress message."""
        try:
            while True:
                await asyncio.sleep(0.4)
                # Lazily create the message once transfer actually started
                # and the group is big enough to be worth it.
                if (
                    group.message_id is None
                    and not group.broken
                    and group.any_running
                    and group.expected_bytes >= self._settings.progress_min_bytes
                ):
                    async with group.lock:
                        if group.message_id is None and group.jobs:
                            await self._send_progress_message(group)
                if group.message_id is not None and group.any_running:
                    await self._edit_throttled(group)
        except asyncio.CancelledError:
            return

    async def _send_progress_message(self, group: _Group) -> None:
        try:
            message = await self._bot.send_message(
                group.chat_id, self._progress_text(group)
            )
            group.message_id = message.message_id
        except TelegramRetryAfter as err:
            await asyncio.sleep(err.retry_after or 1)
        except Exception as err:  # noqa: BLE001 — chat may be unreachable
            logger.warning("cannot send progress message to chat %s: %s", group.chat_id, err)
            group.broken = True

    async def _edit_throttled(self, group: _Group) -> None:
        now = time.monotonic()
        if now - group._last_edit < self._settings.progress_edit_interval:
            return
        group._last_edit = now
        await self._edit_message(group, self._progress_text(group))

    async def _edit_message(self, group: _Group, text: str) -> None:
        if group.message_id is None or group.broken:
            return
        try:
            await self._bot.edit_message_text(
                text, chat_id=group.chat_id, message_id=group.message_id
            )
        except TelegramBadRequest as err:
            if "message is not modified" in str(err).lower():
                return
            if "message to edit not found" in str(err).lower():
                group.broken = True
                return
            logger.debug("edit failed for group %s: %s", group.key, err)
        except TelegramRetryAfter as err:
            await asyncio.sleep(err.retry_after or 1)
        except Exception as err:  # noqa: BLE001
            logger.warning("cannot edit progress message in chat %s: %s", group.chat_id, err)
            group.broken = True

    async def _finalize_group(self, group: _Group) -> None:
        async with group.lock:
            text = self._receipt_text(group)
            if group.message_id is not None and not group.broken:
                await self._edit_message(group, text)
            else:
                try:
                    await self._bot.send_message(group.chat_id, text)
                except Exception as err:  # noqa: BLE001
                    logger.warning("cannot send receipt to chat %s: %s", group.chat_id, err)

    # ── text builders ───────────────────────────────────────────────────────

    def _progress_text(self, group: _Group) -> str:
        running = [j for j in group.jobs.values() if j.status is JobStatus.RUNNING]
        queued = [j for j in group.jobs.values() if j.status is JobStatus.QUEUED]
        count = len(group.jobs) + len(group.results)
        lines: list[str] = []

        if count > 1:
            done = len(group.results)
            lines.append(f"⬇️ <b>Downloading {plural_files(count)}</b> — {done}/{count} done")
        else:
            job = running[0] if running else next(iter(group.jobs.values()), None)
            name = job.file_name or job.rel_path if job else "file"
            lines.append(f"⬇️ <b>Downloading</b> {escape(name)}")

        fraction = group.fraction
        if fraction is not None:
            lines.append(
                f"<code>{progress_bar(fraction)} {pct(fraction)}</code>"
            )
        lines.append(
            f"{human_size(group.bytes_done)} / {human_size(group.expected_bytes)}"
            f" • {human_speed(group.speed)}"
        )
        eta = group.eta()
        if eta is not None and fraction is not None and fraction < 1:
            lines[-1] += f" • ETA {fmt_eta(eta)}"

        # Server-side fetch phase (local Bot API mode): the engine is parked
        # in getFile while Telegram streams the file to the local server —
        # nothing to show but a growing elapsed counter instead of a frozen
        # 0% bar that looks like a hang.
        waiting = [j for j in group.jobs.values() if j.waiting_since is not None]
        if waiting:
            elapsed = max(time.monotonic() - j.waiting_since for j in waiting)
            lines.append(
                f"⏳ Telegram is preparing {plural_files(len(waiting))} — "
                f"{fmt_eta(elapsed)} so far (first-time big downloads take a while)"
            )

        if running and count == 1:
            pass  # header already carries the file name
        elif running:
            for job in running[:3]:
                lines.append(
                    f"  {type_icon(job.media_type)} {escape(job.file_name or job.rel_path)} "
                    f"— {pct(job.progress_fraction)}"
                )
        if queued:
            lines.append(f"⏳ {plural_files(len(queued))} waiting in queue")
        return "\n".join(lines)

    def _receipt_text(self, group: _Group) -> str:
        results = group.results
        completed = [j for j in results if j.status is JobStatus.COMPLETED]
        failed = [j for j in results if j.status is JobStatus.FAILED]
        cancelled = [j for j in results if j.status is JobStatus.CANCELLED]
        lines: list[str] = []

        total_bytes = sum(j.bytes_done or j.file_size or 0 for j in completed)
        if completed and not failed and not cancelled:
            volume = human_size(total_bytes)
            if len(completed) > 1:
                header = f"✅ <b>Saved {plural_files(len(completed))}</b> · {volume}"
            else:
                header = f"✅ <b>Saved</b> — {volume}"
            lines.append(header)
        elif completed:
            lines.append(
                f"⚠️ <b>Saved {len(completed)}/{len(results)}</b> · {human_size(total_bytes)}"
            )
        elif cancelled and not failed:
            lines.append(f"🚫 <b>Cancelled</b> {plural_files(len(cancelled))}")
        else:
            lines.append("❌ <b>Download failed</b>")

        shown = 0
        for job in completed[:10]:
            lines.append(self._receipt_line(job))
            shown += 1
        for job in failed[:4]:
            lines.append(self._receipt_line(job))
            shown += 1
        if len(completed) > 10:
            lines.append(f"… and {len(completed) - 10} more")

        if completed:
            speeds = [j.avg_speed for j in completed if j.avg_speed]
            if speeds:
                lines.append(f"⚡ {human_speed(sum(speeds) / len(speeds))} average")
        job_ids = " ".join(f"#{j.job_id}" for j in results[:5])
        if job_ids:
            lines.append(f"🧷 {job_ids}")

        directory = self._common_dir([j for j in completed]) or (
            completed[0].rel_path.rsplit("/", 1)[0] if completed else ""
        )
        if directory:
            lines.append(f"📁 <code>{escape(directory)}</code>")
        return "\n".join(lines)

    def _receipt_line(self, job: DownloadJob) -> str:
        icon = type_icon(job.media_type)
        name = escape(job.file_name or job.rel_path)
        if job.status is JobStatus.COMPLETED:
            return f"{icon} {name} — {human_size(job.bytes_done or job.file_size)}"
        if job.status is JobStatus.CANCELLED:
            return f"🚫 {name} — cancelled"
        return f"❌ {name} — {escape((job.error or 'error')[:80])}"

    @staticmethod
    def _common_dir(jobs: list[DownloadJob]) -> str | None:
        if not jobs:
            return None
        dirs = [j.rel_path.rsplit("/", 1)[0] if "/" in j.rel_path else "" for j in jobs]
        common = dirs[0]
        for directory in dirs[1:]:
            if directory != common:
                return None
        return common or None
