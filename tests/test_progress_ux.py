"""Progress UX under load (0.3.3): honest receipts, paced edits, and
receipts that can no longer be silently lost to a Telegram flood-wait.

A fully busy queue with prefetching used to show three problems at once:

* receipts flashing "⚡ 252 MB/s average" for files that were merely
  disk-copied after their server-side fetch finished while still queued
  (fixed by ``fetch_bps`` / ``fetched_ahead``),
* frozen progress messages and *vanished* receipts — per-group edit
  throttling let N groups edit N× faster than Telegram tolerates per chat,
  and the resulting 429 flood-waits dropped edits, including the receipt
  edit itself (fixed by global pacing + urgent, retried receipts),
* every fetch crawling at ~0 B/s — the FETCH_CONCURRENCY half of that fix
  lives in ``test_prefetch.py``.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter

from app.queue.jobs import JobStatus, new_job_id
from app.queue.manager import build_job
from app.services.filenaming import UserRef
from app.services.notifier import Notifier

USER = UserRef(111, "tester", "Tester")


class _ScriptedBot:
    """Records sends/edits (attempts included) and can raise scripted
    exceptions before the first success."""

    def __init__(self, edit_errors=None, send_errors=None) -> None:
        self.sent: list[str] = []
        self.edits: list[str] = []
        self.edit_attempts = 0
        self.send_attempts = 0
        self._edit_errors = list(edit_errors or [])
        self._send_errors = list(send_errors or [])

    async def send_message(self, chat_id, text, **kwargs):
        self.send_attempts += 1
        if self._send_errors:
            raise self._send_errors.pop(0)
        self.sent.append(text)
        return SimpleNamespace(message_id=len(self.sent))

    async def edit_message_text(self, text, chat_id=None, message_id=None, **kwargs):
        self.edit_attempts += 1
        if self._edit_errors:
            raise self._edit_errors.pop(0)
        self.edits.append(text)


def _job(tmp_path, name="movie.mkv", size=126_000_000, album_key=None):
    target = tmp_path / "downloads" / name
    return build_job(
        job_id=new_job_id(),
        user=USER,
        chat_id=100,
        message_id=1,
        media_type="document",
        file_id=f"file-{name}",
        file_unique_id=f"uniq-{name}",
        file_name=name,
        file_size=size,
        target_path=str(target),
        rel_path=name,
        album_key=album_key,
    )


def _completed(job, bytes_done=None):
    job.status = JobStatus.COMPLETED
    job.bytes_done = bytes_done or job.file_size
    job.total_bytes = job.file_size
    return job


# ── honest receipts ─────────────────────────────────────────────────────────


async def test_receipt_prefetched_file_shows_network_speed(settings, tmp_path):
    """A prefetched file must report its Telegram speed, not the disk copy."""
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    job = _completed(_job(tmp_path, "E26.mkv"))
    job.fetched_ahead = True
    job.fetch_bps = 420_000.0
    job.avg_speed = 252_000_000.0  # the disk-copy figure — must not be shown

    notifier.attach(job)
    await notifier.finish(job)

    assert bot.sent, "receipt must be delivered"
    text = bot.sent[0]
    assert "from Telegram" in text
    assert "fetched ahead" in text
    assert "KB/s" in text  # ~410 KB/s, a network speed
    assert "252" not in text and "average" not in text
    await notifier.aclose()


async def test_receipt_cache_hit_without_stats_shows_instant(settings, tmp_path):
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    job = _completed(_job(tmp_path))
    job.fetched_ahead = True
    job.fetch_bps = None  # cache hit from an earlier job — no stats kept
    job.avg_speed = 252_000_000.0

    notifier.attach(job)
    await notifier.finish(job)

    text = bot.sent[0]
    assert "instant" in text
    assert "local server cache" in text
    assert "252" not in text
    await notifier.aclose()


async def test_receipt_normal_speed_unchanged(settings, tmp_path):
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    job = _completed(_job(tmp_path))
    job.avg_speed = 430_000.0

    notifier.attach(job)
    await notifier.finish(job)

    text = bot.sent[0]
    assert "average" in text
    assert "prefetched" not in text and "instant" not in text
    await notifier.aclose()


async def test_receipt_album_mix_notes_prefetched_count(settings, tmp_path):
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    live = _completed(_job(tmp_path, "a.mkv", album_key="album-1"))
    live.avg_speed = 430_000.0
    ahead = _completed(_job(tmp_path, "b.mkv", album_key="album-1"))
    ahead.fetched_ahead = True
    ahead.fetch_bps = 420_000.0
    ahead.avg_speed = 252_000_000.0

    for job in (live, ahead):
        notifier.attach(job)
    await notifier.finish(live)
    await notifier.finish(ahead)

    text = bot.sent[0]
    assert "average" in text
    assert "1 prefetched" in text
    assert "252" not in text
    await notifier.aclose()


# ── global edit pacing ──────────────────────────────────────────────────────


async def test_routine_edits_are_droppable_not_queued(settings, tmp_path):
    """Under pacing, a routine refresh is skipped, not piled onto a backlog."""
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    job = _job(tmp_path)
    notifier.attach(job)
    group = notifier._groups[f"job:{job.job_id}"]
    group.message_id = 7  # pretend the progress message exists

    # an edit is already in flight → the next routine refresh is dropped
    await notifier._edit_lock.acquire()
    assert await notifier._edit_message(group, "refresh 1") is False
    assert bot.edits == []

    # inside the global gap → dropped as well, never queued
    notifier._edit_lock.release()
    notifier._next_edit_at = time.monotonic() + 5.0
    assert await notifier._edit_message(group, "refresh 2") is False
    assert bot.edits == []

    # urgent (receipt) edits ignore the gap — receipts must not starve
    assert await notifier._edit_message(group, "receipt", urgent=True) is True
    assert bot.edits == ["receipt"]
    await notifier.aclose()


async def test_routine_edit_backs_off_after_flood_wait(settings, tmp_path):
    bot = _ScriptedBot(
        edit_errors=[TelegramRetryAfter(method=None, message="Too many requests",
                                        retry_after=5)],
    )
    notifier = Notifier(bot, settings)
    job = _job(tmp_path)
    notifier.attach(job)
    group = notifier._groups[f"job:{job.job_id}"]
    group.message_id = 7

    assert await notifier._edit_message(group, "x") is False  # 429 → dropped
    assert group._last_edit > time.monotonic()  # parked beyond the window
    assert bot.edits == []
    await notifier.aclose()


# ── receipts survive flood-waits and broken messages ───────────────────────


async def test_receipt_edit_retries_after_flood_wait(settings, tmp_path):
    """A 429 on the receipt edit is waited out once — the receipt lands."""
    bot = _ScriptedBot(
        edit_errors=[TelegramRetryAfter(method=None, message="Too many requests",
                                        retry_after=1)],
    )
    notifier = Notifier(bot, settings)
    job = _completed(_job(tmp_path))
    notifier.attach(job)
    notifier._groups[f"job:{job.job_id}"].message_id = 7

    await notifier.finish(job)

    assert bot.edit_attempts == 2  # first 429'd, retry delivered the receipt
    assert len(bot.edits) == 1
    assert bot.sent == []  # no duplicate fallback message needed
    assert "Saved" in bot.edits[0]
    await notifier.aclose()


async def test_receipt_falls_back_to_new_message(settings, tmp_path):
    """Edit target gone (user deleted the progress message) → send instead."""
    bot = _ScriptedBot(
        edit_errors=[TelegramBadRequest(
            method=None, message="Bad Request: message to edit not found",
        )],
    )
    notifier = Notifier(bot, settings)
    job = _completed(_job(tmp_path))
    notifier.attach(job)
    notifier._groups[f"job:{job.job_id}"].message_id = 7

    await notifier.finish(job)

    assert bot.sent, "receipt must arrive as a fresh message"
    assert "Saved" in bot.sent[0]
    await notifier.aclose()


async def test_receipt_send_waits_out_one_flood_wait(settings, tmp_path):
    bot = _ScriptedBot(
        send_errors=[TelegramRetryAfter(method=None, message="Too many requests",
                                        retry_after=1)],
    )
    notifier = Notifier(bot, settings)
    job = _completed(_job(tmp_path))  # no progress message → send path

    notifier.attach(job)
    await notifier.finish(job)

    assert bot.send_attempts == 2  # retried once after the announced wait
    assert len(bot.sent) == 1
    assert "Saved" in bot.sent[0]
    await notifier.aclose()


async def test_progress_message_send_parks_after_flood_wait(settings, tmp_path):
    """Creating the progress message must not sleep under the group lock."""
    bot = _ScriptedBot(
        send_errors=[TelegramRetryAfter(method=None, message="Too many requests",
                                        retry_after=3)],
    )
    notifier = Notifier(bot, settings)
    job = _job(tmp_path)
    notifier.attach(job)
    group = notifier._groups[f"job:{job.job_id}"]

    before = time.monotonic()
    async with group.lock:  # exactly like the updater does
        await notifier._send_progress_message(group)
    assert time.monotonic() - before < 1.0  # returned immediately
    assert group.message_id is None
    assert group._last_edit > before  # next attempt parked past the window
    await notifier.aclose()


async def test_many_groups_cannot_storm_the_chat(settings, tmp_path):
    """End-to-end pacing: 6 busy groups refresh, edits stay rate-limited."""
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    jobs = []
    for i in range(6):
        job = _job(tmp_path, f"ep{i:02d}.mkv")
        notifier.attach(job)
        group = notifier._groups[f"job:{job.job_id}"]
        group.message_id = 100 + i
        job.status = JobStatus.RUNNING
        job.waiting_since = time.monotonic()
        jobs.append(job)

    # 12 refresh attempts in quick succession (two rounds per group)
    for _ in range(2):
        for job in jobs:
            group = notifier._groups[f"job:{job.job_id}"]
            await notifier._edit_throttled(group)
    for job in jobs:
        job.status = JobStatus.COMPLETED
        job.waiting_since = None

    # ~6 groups × 2 rounds fired within milliseconds — pacing keeps the
    # actual API calls down to a handful instead of 12
    assert 1 <= len(bot.edits) <= 7
    await notifier.aclose()
