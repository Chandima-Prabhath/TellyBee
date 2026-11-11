"""In-chat progress & receipt UX — the unified window model (v0.5).

Each chat gets at most ONE live "window" message: a single frame showing
everything the queue is doing for that chat right now, updated in place —
an application window, not a stack of task notifications.

Lifecycle:

* A new task arrives → the previous window message is **deleted** and a
  fresh one is sent, debounced ~0.9 s so a burst of forwards (albums,
  multi-select) collapses into a single replacement.
* The window is also **re-anchored** whenever the bot sends anything else
  to the chat (a receipt): the frame is re-sent below it, so the live
  window always stays the *latest* message in the chat instead of drifting
  up with every finished file.
* While work runs, the window is edited in place — one message per chat
  instead of one per job, so there is nothing to fight over the edit
  budget (the global pacer from 0.3.3 stays as a safety net).
* When the chat's queue drains, the window is deleted: receipts remain as
  the permanent record of what was saved.
* Window message ids are persisted in SQLite, so a crash/restart deletes
  orphaned frames instead of leaving a frozen "47%" message in the chat
  forever.

Receipts are always sent as fresh messages — the window is transient.

User-facing wording (v0.5): progress is just progress. The internal
fetch/copy split of local Bot API mode ("prefetch", "Telegram fetch") is
an implementation detail — users see one download bar; receipts report
the honest transfer speed without explaining where it came from.
"""

from __future__ import annotations

import asyncio
import logging
import time

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.config import Settings
from app.queue.jobs import DownloadJob, JobStatus
from app.utils.formatting import (
    escape,
    fmt_eta,
    pct,
    plural_files,
    progress_bar,
    transfer_meta,
)
from app.utils.sizes import human_size, human_speed

logger = logging.getLogger(__name__)

#: Updater loop tick (seconds) — how often a window's state is re-examined.
_TICK = 0.4
#: Sliding debounce (seconds) for (re)creating a window after an intake:
#: forwards that arrive within this window coalesce into ONE replacement.
_WINDOW_DEBOUNCE = 0.9
#: Minimum seconds between ANY two progress-message edits, across all chats.
#: With one window per chat this rarely binds, but it is a cheap safety net
#: against a bot-wide 429 storm.
_EDIT_MIN_GAP = 1.0
#: Upper bound (seconds) for how long a receipt send may sleep on a
#: Telegram ``Retry-After`` before falling back.
_RECEIPT_MAX_WAIT = 15.0
#: Cap (seconds) for parking the next window (re)send past a Retry-After —
#: the updater never sleeps a flood-wait away, it just retries later.
_WINDOW_FLOOD_PARK = 30.0
#: How many active entries (job or collapsed album) get detailed lines.
_MAX_ACTIVE_ENTRIES = 6
#: Visual separator between a header and the body of a message.
_DIVIDER = "\u2500" * 22

#: Callback payload prefix shared by every window button (see handlers/callbacks.py).
_CB = "tb:"

_TYPE_ICONS = {
    "photo": "🖼",
    "video": "🎬",
    "document": "📄",
    "audio": "🎵",
    "voice": "🎙",
    "video_note": "⭕️",
    "sticker": "🏷",
    "animation": "🎞",
    "url": "🌐",
}


def type_icon(media_type: str) -> str:
    return _TYPE_ICONS.get(media_type, "📁")


def _short(name: str | None, limit: int) -> str:
    text = name or "file"
    return text if len(text) <= limit else text[: limit - 1] + "…"


class _Group:
    """Receipt aggregation: the jobs of one single file or one album."""

    __slots__ = ("key", "chat_id", "jobs", "results")

    def __init__(self, key: str, chat_id: int):
        self.key = key
        self.chat_id = chat_id
        self.jobs: dict[str, DownloadJob] = {}
        self.results: list[DownloadJob] = []


class _Window:
    """The single live progress message of one chat.

    State mutated from handler/worker context (``attach``/``finish``):
    ``live``, the session counters and ``kick_at`` — plain dict/int writes,
    atomic under asyncio. The *message lifecycle* (send / edit / delete of
    the window message itself) belongs exclusively to the updater task:
    one writer per message, no locks needed.
    """

    __slots__ = (
        "chat_id",
        "message_id",
        "live",
        "done",
        "failed",
        "cancelled",
        "kick_at",
        "broken",
        "updater_task",
        "_last_edit",
    )

    def __init__(self, chat_id: int):
        self.chat_id = chat_id
        self.message_id: int | None = None
        self.live: dict[str, DownloadJob] = {}
        self.done = 0
        self.failed = 0
        self.cancelled = 0
        #: monotonic deadline after which the window (re)creation fires
        self.kick_at: float | None = None
        #: set when the chat turned out unreachable; a new intake resets it
        self.broken = False
        self.updater_task: asyncio.Task | None = None
        self._last_edit = 0.0  # last successful/attempted edit (throttle)

    @property
    def has_work(self) -> bool:
        return bool(self.live) or self.kick_at is not None


class Notifier:
    """Turns queue events into one window per chat plus receipts."""

    def __init__(self, bot: Bot, settings: Settings, db=None):
        self._bot = bot
        self._settings = settings
        self._db = db
        self._groups: dict[str, _Group] = {}
        self._windows: dict[int, _Window] = {}
        # The queue manager, bound after construction (main.py) — used only
        # to reflect the paused state in the window's button row. Duck-typed
        # on purpose: no import, no cycle.
        self._manager = None
        # Global pacing for window edits (see _EDIT_MIN_GAP): one edit in
        # flight at a time, routine refreshes dropped rather than queued.
        self._edit_lock = asyncio.Lock()
        self._next_edit_at = 0.0

    def bind_manager(self, manager) -> None:
        """Give the notifier a peek at queue state (paused flag) for buttons."""
        self._manager = manager

    def request_refresh(self, chat_id: int | None = None) -> None:
        """Reset the edit throttle so the next tick re-renders immediately.

        Called by the window buttons after a state change (pause/resume):
        the new button labels and the header's paused marker then appear
        within one updater tick instead of after the full edit interval.
        """
        windows = (
            [self._windows[chat_id]]
            if chat_id is not None and chat_id in self._windows
            else list(self._windows.values())
        )
        for window in windows:
            window._last_edit = 0.0
            self._ensure_updater(window)

    # ── lifecycle ───────────────────────────────────────────────────────────

    async def startup(self) -> None:
        """Delete progress windows orphaned by a crash/restart.

        Without this, a hard restart would leave the last frozen frame
        ("47%", 2.1 MB/s…) stuck in the chat forever — the ids only ever
        existed in memory. They are persisted in SQLite precisely so this
        cleanup can find them.
        """
        if self._db is None:
            return
        try:
            rows = await self._db.list_windows()
        except Exception:  # noqa: BLE001 — best effort, never block startup
            logger.warning("could not read orphaned progress windows", exc_info=True)
            return
        for chat_id, message_id in rows:
            await self._delete_quietly(chat_id, message_id)
            await self._db_drop(chat_id)
        if rows:
            logger.info("cleaned up %d orphaned progress window(s)", len(rows))

    async def aclose(self) -> None:
        for window in list(self._windows.values()):
            if window.updater_task and not window.updater_task.done():
                window.updater_task.cancel()
        await asyncio.gather(
            *(w.updater_task for w in self._windows.values() if w.updater_task),
            return_exceptions=True,
        )
        # Final sweep: a window still holding a message (queue drained
        # between the last updater tick and this close) is deleted now so
        # the normal shutdown path leaves no orphan either.
        for window in list(self._windows.values()):
            message_id = window.message_id
            if message_id is not None:
                window.message_id = None
                await self._delete_quietly(window.chat_id, message_id)
                await self._db_drop(window.chat_id)
        self._windows.clear()
        self._groups.clear()

    # ── events from the manager ─────────────────────────────────────────────

    def attach(self, job: DownloadJob) -> None:
        """Register a freshly queued job with its group and chat window."""
        key = job.album_key or f"job:{job.job_id}"
        group = self._groups.get(key)
        if group is None:
            group = _Group(key, job.chat_id)
            self._groups[key] = group
        group.jobs[job.job_id] = job

        window = self._windows.get(job.chat_id)
        if window is None:
            window = _Window(job.chat_id)
            self._windows[job.chat_id] = window
        window.live[job.job_id] = job
        window.broken = False  # a new intake is a new chance for the chat
        # Debounced (re)creation: bursts collapse into one replacement,
        # spaced intakes each get their own fresh "latest" window.
        window.kick_at = time.monotonic() + _WINDOW_DEBOUNCE
        self._ensure_updater(window)

    async def finish(self, job: DownloadJob) -> None:
        """A job reached a terminal state; receipt + window bookkeeping."""
        # Window first: the job is factually done, so it must leave the
        # live set immediately (even if the receipt below waits out a
        # flood-wait, the window must not show it as still running).
        window = self._windows.get(job.chat_id)
        if window is not None:
            window.live.pop(job.job_id, None)
            if job.status is JobStatus.COMPLETED:
                window.done += 1
            elif job.status is JobStatus.FAILED:
                window.failed += 1
            elif job.status is JobStatus.CANCELLED:
                window.cancelled += 1
            self._ensure_updater(window)

        key = job.album_key or f"job:{job.job_id}"
        group = self._groups.get(key)
        if group is None:  # e.g. submit failed midway; nothing to show
            return
        group.jobs.pop(job.job_id, None)
        group.results.append(job)
        if not group.jobs:
            self._groups.pop(key, None)
            await self._finalize_group(group)

    # ── the window updater (single writer of the window message) ────────────

    def _ensure_updater(self, window: _Window) -> None:
        if window.updater_task is None or window.updater_task.done():
            window.updater_task = asyncio.create_task(
                self._updater(window), name=f"tellybee-window-{window.chat_id}"
            )

    async def _updater(self, window: _Window) -> None:
        """Drive one chat's window: (re)create, refresh, close."""
        try:
            while True:
                await asyncio.sleep(_TICK)
                if window.kick_at is not None:
                    if time.monotonic() >= window.kick_at:
                        await self._recreate(window)
                        continue
                    if window.message_id is None:
                        continue  # nothing on screen yet — wait for the send
                    # A replacement is scheduled but not due yet: keep the
                    # current frame fresh instead of freezing it for the
                    # whole park.
                if window.live:
                    if window.message_id is not None and not window.broken:
                        await self._refresh(window)
                elif window.message_id is not None:
                    await self._close(window)  # queue drained → close the app
                    return
                elif not window.has_work:
                    return  # idle and nothing pending — stop the task
        except asyncio.CancelledError:
            return

    async def _recreate(self, window: _Window) -> None:
        """Replace the window message: send the fresh frame, retire the old.

        Send-then-delete: the new frame lands at the bottom of the chat
        (exactly where it belongs), and if the send hits a flood-wait the
        OLD window is still on screen and keeps refreshing — the chat
        never spends a second without a window. Two frames coexist only
        for the duration of one delete call.
        """
        window.kick_at = None
        if window.broken or not window.live:
            return

        text = self._window_text(window)
        try:
            message = await self._bot.send_message(
                window.chat_id, text, reply_markup=self._keyboard(window)
            )
        except TelegramRetryAfter as err:
            # No sleeping the updater away: park the next attempt beyond
            # the announced window (bounded) and try again then. The old
            # frame (if any) stays on screen and stays fresh.
            window.kick_at = time.monotonic() + min(float(err.retry_after or 1), _WINDOW_FLOOD_PARK)
            return
        except Exception as err:  # noqa: BLE001 — chat may be unreachable
            logger.warning("cannot send progress window to chat %s: %s", window.chat_id, err)
            window.broken = True
            return

        old_id = window.message_id
        window.message_id = message.message_id
        window._last_edit = time.monotonic()
        await self._db_upsert(window.chat_id, window.message_id)
        if old_id is not None and old_id != message.message_id:
            await self._delete_quietly(window.chat_id, old_id)

    async def _refresh(self, window: _Window) -> None:
        """Routine in-place edit of the window, paced and droppable.

        With one message per chat there is no contention to speak of; the
        per-window interval plus the global gap are kept so a restart of
        heavy activity can never re-create the 0.3.3 edit storm.
        """
        now = time.monotonic()
        if now - window._last_edit < self._settings.progress_edit_interval:
            return
        if self._edit_lock.locked() or now < self._next_edit_at:
            return  # droppable refresh — never queue edits up
        window._last_edit = now
        async with self._edit_lock:
            self._next_edit_at = time.monotonic() + _EDIT_MIN_GAP
            try:
                await self._bot.edit_message_text(
                    self._window_text(window),
                    chat_id=window.chat_id,
                    message_id=window.message_id,
                    reply_markup=self._keyboard(window),
                )
            except TelegramBadRequest as err:
                message = str(err).lower()
                if "message is not modified" in message:
                    return
                if "message to edit not found" in message:
                    # The user deleted the window himself — rebuild it on
                    # the next tick instead of giving up on the chat.
                    window.message_id = None
                    window.kick_at = time.monotonic()
                    return
                logger.debug("window edit failed in chat %s: %s", window.chat_id, err)
            except TelegramRetryAfter as err:
                # Back off hard: park this window's edit budget past the
                # flood wait so later ticks stay quiet.
                window._last_edit = time.monotonic() + (err.retry_after or 1)
            except Exception as err:  # noqa: BLE001
                logger.warning("cannot edit progress window in chat %s: %s", window.chat_id, err)

    async def _close(self, window: _Window) -> None:
        """The chat's queue drained: delete the window (receipts stay)."""
        message_id = window.message_id
        window.message_id = None
        if message_id is not None:
            await self._delete_quietly(window.chat_id, message_id)
        await self._db_drop(window.chat_id)
        # Reset the session counters — a later intake starts a fresh window.
        window.done = window.failed = window.cancelled = 0
        if self._windows.get(window.chat_id) is window and not window.live:
            self._windows.pop(window.chat_id, None)

    # ── helpers ─────────────────────────────────────────────────────────────

    async def _delete_quietly(self, chat_id: int, message_id: int) -> None:
        try:
            await self._bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as err:  # noqa: BLE001 — already gone is fine
            logger.debug("delete_message(%s, %s) failed: %s", chat_id, message_id, err)

    async def _db_upsert(self, chat_id: int, message_id: int) -> None:
        if self._db is None:
            return
        try:
            await self._db.upsert_window(chat_id, message_id)
        except Exception:  # noqa: BLE001 — bookkeeping must never break UX
            logger.warning("could not persist progress window row", exc_info=True)

    async def _db_drop(self, chat_id: int) -> None:
        if self._db is None:
            return
        try:
            await self._db.drop_window(chat_id)
        except Exception:  # noqa: BLE001
            logger.warning("could not drop progress window row", exc_info=True)

    # ── receipts ────────────────────────────────────────────────────────────

    async def _finalize_group(self, group: _Group) -> None:
        text = self._receipt_text(group)
        if not await self._send_receipt(group, text):
            logger.warning(
                "could not deliver the receipt for group %s to chat %s",
                group.key,
                group.chat_id,
            )
            return
        # The receipt just became the newest message in the chat — move the
        # live window below it (debounced, same as an intake kick) so the
        # progress frame is always the LAST message, never drifting up.
        self._reanchor(group.chat_id)

    def reanchor(self, chat_id: int) -> None:
        """Public bottom-anchor: move the chat's live window below the last
        bot message (call after any send that is meant to be read)."""
        self._reanchor(chat_id)

    def _reanchor(self, chat_id: int) -> None:
        """Schedule a delete + resend of the chat's window (bottom anchor).

        No-op unless the chat still has live work — when the last job of a
        session finishes, its receipt *is* the right bottom message and the
        window is closed instead.
        """
        window = self._windows.get(chat_id)
        if window is None or window.broken or not window.live:
            return
        when = time.monotonic() + _WINDOW_DEBOUNCE
        if window.kick_at is None or window.kick_at > when:
            window.kick_at = when
        self._ensure_updater(window)

    async def _send_receipt(self, group: _Group, text: str) -> bool:
        """Deliver the receipt as a new message, waiting out one flood-wait.

        Receipts are the permanent record of a download, so they may not be
        silently dropped: one bounded retry after the announced
        ``Retry-After``; a hard failure is logged loudly.
        """
        for attempt in (1, 2):
            try:
                await self._bot.send_message(group.chat_id, text)
                return True
            except TelegramRetryAfter as err:
                if attempt == 2:
                    break
                await asyncio.sleep(min(float(err.retry_after or 1), _RECEIPT_MAX_WAIT))
            except Exception as err:  # noqa: BLE001
                logger.warning("cannot send receipt to chat %s: %s", group.chat_id, err)
                return False
        return False

    # ── window keyboard ─────────────────────────────────────────────────────

    def _keyboard(self, window: _Window) -> InlineKeyboardMarkup | None:
        """The window's remote-control rows (None when no manager is bound).

        The same buttons ride along on every send AND every edit — an
        edit_message_text without ``reply_markup`` would strip them. The
        pause/resume button reflects the queue's live state, so the header
        text also carries a paused marker (an identical text + identical
        markup would trip Telegram's "message is not modified").
        """
        manager = self._manager
        if manager is None:
            return None
        paused = bool(getattr(manager, "paused", False))
        rows = [
            [
                InlineKeyboardButton(
                    text="▶\ufe0f Resume" if paused else "⏸ Pause",
                    callback_data=_CB + ("resume" if paused else "pause"),
                ),
            ]
        ]
        if window.live:
            rows[0].append(InlineKeyboardButton(text="✖ Cancel all", callback_data=_CB + "cancel"))
        rows.append(
            [
                InlineKeyboardButton(text="📋 Queue", callback_data=_CB + "queue"),
                InlineKeyboardButton(text="📊 Stats", callback_data=_CB + "stats"),
                InlineKeyboardButton(text="❓ Help", callback_data=_CB + "help"),
            ]
        )
        return InlineKeyboardMarkup(inline_keyboard=rows)

    # ── text builders ───────────────────────────────────────────────────────

    @staticmethod
    def _is_active(job: DownloadJob) -> bool:
        """Running, or queued-but-fetching (prefetch parks jobs in getFile)."""
        return job.status is JobStatus.RUNNING or job.waiting_since is not None

    def _window_text(self, window: _Window) -> str:
        """One glanceable frame: header chips, live blocks, queued summary."""
        live = sorted(window.live.values(), key=lambda j: j.seq)
        # Collapse each album into one display entry — ten near-identical
        # lines for one media group is noise, one bar with sums is signal.
        grouped: dict[str, list[DownloadJob]] = {}
        for job in live:
            grouped.setdefault(job.album_key or f"single:{job.job_id}", []).append(job)
        active_entries = [jobs for jobs in grouped.values() if any(map(self._is_active, jobs))]
        queued_jobs = [j for j in live if not self._is_active(j)]

        active_count = sum(1 for j in live if self._is_active(j))
        chips: list[str] = []
        if active_count:
            chips.append(f"{active_count} active")
        if queued_jobs:
            chips.append(f"{len(queued_jobs)} queued")
        if window.done:
            chips.append(f"{window.done} done")
        if window.failed:
            chips.append(f"{window.failed} failed")
        if window.cancelled:
            chips.append(f"{window.cancelled} cancelled")
        volume = sum((j.total_bytes or j.file_size or 0) for j in live)
        if volume:
            chips.append(human_size(volume))
        header = "🐝 <b>TellyBee</b>  <i>downloads</i>"
        if self._manager is not None and getattr(self._manager, "paused", False):
            header += " · <b>⏸ paused</b>"
        if chips:
            header += f"\n<code>{' · '.join(chips)}</code>"
        lines: list[str] = [header]
        if active_entries or queued_jobs:
            lines.append(_DIVIDER)

        shown = 0
        for jobs in active_entries[:_MAX_ACTIVE_ENTRIES]:
            if shown:
                lines.append("")  # breathing room between file blocks
            lines.extend(self._entry_lines(jobs))
            shown += 1
        if len(active_entries) > shown:
            lines.append(f"<i>… +{len(active_entries) - shown} more in progress</i>")

        if queued_jobs:
            if active_entries:
                lines.append("")
            queued_bytes = sum((j.file_size or 0) for j in queued_jobs)
            size_part = f" · {human_size(queued_bytes)}" if queued_bytes else ""
            lines.append(
                f"⏳ <b>Queued</b> · "
                f"<code>{plural_files(len(queued_jobs))}{size_part}</code>"
            )
            names = [escape(_short(j.file_name or j.rel_path, 26)) for j in queued_jobs[:20]]
            hidden = len(queued_jobs) - 20
            if hidden > 0:
                names.append(f"<i>… and {hidden} more</i>")
            if names:
                opening = "blockquote expandable" if len(names) > 3 else "blockquote"
                lines.append(f"<{opening}>{chr(10).join(names)}</blockquote>")
        return "\n".join(lines)

    def _entry_lines(self, jobs: list[DownloadJob]) -> list[str]:
        """Detailed block for one active job or one collapsed album.

        Both transport phases of local Bot API mode render identically:
        a progress bar plus a meta line. Users never see the internal
        split between the server-side fetch and the local copy.
        """
        first = jobs[0]
        icon = type_icon(first.media_type)
        name = _short(first.file_name or first.rel_path, 44)
        if len(jobs) > 1:
            name = f"{name} +{len(jobs) - 1}"
        lines = [f"{icon} <b>{escape(name)}</b>"]

        # Phase 1 — the server pulling the file from Telegram. The engine's
        # volume watcher fills fetch_done / fetch_speed, so this is real
        # progress; without it degrade to a preparing indicator, never a
        # frozen 0% bar.
        waiting = [j for j in jobs if j.waiting_since is not None]
        if waiting:
            fetch_total = sum(j.fetch_total or j.file_size or 0 for j in waiting)
            fetch_done = sum(j.fetch_done or 0 for j in waiting)
            speed = sum(j.fetch_speed for j in waiting)
            if fetch_total and fetch_done:
                frac = min(1.0, fetch_done / fetch_total)
                lines.append(
                    f"<pre>{progress_bar(frac)} {pct(frac)}\n"
                    f"{transfer_meta(fetch_done, fetch_total, speed)}</pre>"
                )
            else:
                elapsed = max(time.monotonic() - j.waiting_since for j in waiting)
                lines.append(
                    f"<pre>{progress_bar(0.0)} 0%\n"
                    f"⏳ preparing · {fmt_eta(elapsed)}</pre>"
                )
            return lines

        # Phase 2 — the transfer itself (cloud mode, or the fast local copy).
        transferring = [
            j for j in jobs if j.status is JobStatus.RUNNING and j.waiting_since is None
        ]
        if transferring:
            total = sum((j.total_bytes or j.file_size or 0) for j in jobs)
            done = sum(j.bytes_done for j in jobs)
            speed = sum(j.speed_bps for j in transferring)
            if total:
                frac = min(1.0, done / total)
                lines.append(
                    f"<pre>{progress_bar(frac)} {pct(frac)}\n"
                    f"{transfer_meta(done, total, speed)}</pre>"
                )
            else:
                lines.append("<pre>⏬ downloading…</pre>")
        return lines

    def _receipt_text(self, group: _Group) -> str:
        """The permanent record of one file / album: header, list, footer."""
        results = group.results
        completed = [j for j in results if j.status is JobStatus.COMPLETED]
        failed = [j for j in results if j.status is JobStatus.FAILED]
        cancelled = [j for j in results if j.status is JobStatus.CANCELLED]
        lines: list[str] = []

        total_bytes = sum(j.bytes_done or j.file_size or 0 for j in completed)
        if completed and not failed and not cancelled:
            volume = human_size(total_bytes)
            if len(completed) > 1:
                header = f"✅ <b>Saved {plural_files(len(completed))}</b> · <code>{volume}</code>"
            else:
                header = f"✅ <b>Saved</b> · <code>{volume}</code>"
        elif completed:
            header = (
                f"✅ <b>Saved {len(completed)}/{len(results)}</b> · "
                f"<code>{human_size(total_bytes)}</code>"
            )
        elif cancelled and not failed:
            header = f"🚫 <b>Cancelled</b> · <code>{plural_files(len(cancelled))}</code>"
        else:
            header = "❌ <b>Download failed</b>"
        lines.append(header)

        # The file list, quoted — long album lists collapse behind the
        # "expand" arrow instead of flooding the chat.
        body = [self._receipt_line(job) for job in completed[:10]]
        body += [self._receipt_line(job) for job in failed[:4]]
        if len(completed) > 10:
            body.append(f"… and {len(completed) - 10} more")
        if body:
            quote = "\n".join(body)
            opening = "blockquote expandable" if len(body) > 5 else "blockquote"
            lines.append(f"<{opening}>{quote}</blockquote>")

        if completed:
            # Honest speeds, quietly: a file that finished while still
            # queued (prefetch) was "transferred" by an instant local copy —
            # report its real transfer speed instead of a meaningless disk
            # burst. Where the number came from is an implementation detail.
            speeds = [j.fetch_bps if j.fetched_ahead else j.avg_speed for j in completed]
            speeds = [s for s in speeds if s]
            if speeds:
                average = sum(speeds) / len(speeds)
                suffix = " average" if len(completed) > 1 else ""
                lines.append(f"⚡ <code>{human_speed(average)}</code><i>{suffix}</i>")

        job_ids = " ".join(f"<code>#{j.job_id}</code>" for j in results[:5])
        directory = self._common_dir(completed) or (
            completed[0].rel_path.rsplit("/", 1)[0] if completed else ""
        )
        footer = []
        if job_ids:
            footer.append(f"🧷 {job_ids}")
        if directory:
            footer.append(f"📁 <code>{escape(directory)}</code>")
        if footer:
            lines.append(" · ".join(footer))
        return "\n".join(lines)

    def _receipt_line(self, job: DownloadJob) -> str:
        icon = type_icon(job.media_type)
        name = escape(job.file_name or job.rel_path)
        if job.status is JobStatus.COMPLETED:
            size = job.bytes_done or job.file_size
            return f"{icon} <i>{name}</i> · <code>{human_size(size)}</code>"
        if job.status is JobStatus.CANCELLED:
            return f"🚫 <i>{name}</i> · cancelled"
        return f"❌ <i>{name}</i> — {escape((job.error or 'error')[:80])}"

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
