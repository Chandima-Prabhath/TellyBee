"""Progress UX (0.4.0): the unified window model.

One live "window" message per chat instead of one message per job:

* created after a debounced kick on intake (a burst of forwards collapses
  into ONE replacement, so a 35-file forward is not 35 messages),
* deleted + resent whenever a new task arrives — the window is always the
  latest message, right under what the user just sent,
* edited in place while work runs (paced; routine refreshes droppable),
* deleted when the chat's queue drains — receipts remain as the permanent
  record,
* its message id persisted in SQLite so a crash/restart can delete the
  orphaned frame instead of leaving a frozen "47%" in the chat forever.

Receipts keep their 0.3.3 honesty rules (fetch vs copy speeds, prefetched
files) and survive one bounded flood-wait — they are always fresh sends
now, because the window they used to be edited into is transient.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter

import app.services.notifier as notifier_mod
from app.queue.jobs import JobStatus, new_job_id
from app.queue.manager import build_job
from app.services.filenaming import UserRef
from app.services.notifier import Notifier

USER = UserRef(111, "tester", "Tester")
CHAT = 100


class _ScriptedBot:
    """Records sends/edits/deletes and can raise scripted exceptions."""

    def __init__(self, edit_errors=None, send_errors=None, delete_errors=None):
        self.sent: list[str] = []
        self.sends: list[tuple[int, str]] = []  # (chat_id, text)
        self.edits: list[str] = []
        self.deleted: list[tuple[int, int]] = []
        self.edit_attempts = 0
        self.send_attempts = 0
        self._edit_errors = list(edit_errors or [])
        self._send_errors = list(send_errors or [])
        self._delete_errors = list(delete_errors or [])
        self._next_id = 10

    async def send_message(self, chat_id, text, **kwargs):
        self.send_attempts += 1
        if self._send_errors:
            raise self._send_errors.pop(0)
        self.sent.append(text)
        self.sends.append((chat_id, text))
        self._next_id += 1
        return SimpleNamespace(message_id=self._next_id)

    async def edit_message_text(self, text, chat_id=None, message_id=None, **kwargs):
        self.edit_attempts += 1
        if self._edit_errors:
            raise self._edit_errors.pop(0)
        self.edits.append(text)

    async def delete_message(self, chat_id=None, message_id=None, **kwargs):
        if self._delete_errors:
            raise self._delete_errors.pop(0)
        self.deleted.append((chat_id, message_id))


class _FakeDb:
    """Just the progress-window surface of Database."""

    def __init__(self, rows: dict[int, int] | None = None):
        self.rows: dict[int, int] = dict(rows or {})
        self.upserts: list[tuple[int, int]] = []
        self.drops: list[int] = []

    async def upsert_window(self, chat_id, message_id):
        self.upserts.append((chat_id, message_id))
        self.rows[chat_id] = message_id

    async def drop_window(self, chat_id):
        self.drops.append(chat_id)
        self.rows.pop(chat_id, None)

    async def list_windows(self):
        return list(self.rows.items())


def _job(tmp_path, name="movie.mkv", size=126_000_000, album_key=None, chat_id=CHAT):
    target = tmp_path / "downloads" / name
    return build_job(
        job_id=new_job_id(),
        user=USER,
        chat_id=chat_id,
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


def _fast(monkeypatch):
    """Shrink the window machinery's timers so tests run in milliseconds."""
    monkeypatch.setattr(notifier_mod, "_TICK", 0.02)
    monkeypatch.setattr(notifier_mod, "_WINDOW_DEBOUNCE", 0.05)


# ── honest receipts (unchanged rules, now always fresh messages) ───────────


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


async def test_receipt_send_waits_out_one_flood_wait(settings, tmp_path):
    bot = _ScriptedBot(
        send_errors=[TelegramRetryAfter(method=None, message="Too many requests",
                                        retry_after=1)],
    )
    notifier = Notifier(bot, settings)
    job = _completed(_job(tmp_path))

    notifier.attach(job)
    await notifier.finish(job)

    assert bot.send_attempts == 2  # retried once after the announced wait
    assert len(bot.sent) == 1
    assert "Saved" in bot.sent[0]
    await notifier.aclose()


# ── window lifecycle ───────────────────────────────────────────────────────


async def test_intake_creates_single_window_message(settings, tmp_path, monkeypatch):
    """One job → exactly one live message listing it as queued."""
    _fast(monkeypatch)
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    job = _job(tmp_path, "E01.mkv")

    notifier.attach(job)
    await asyncio.sleep(0.25)

    assert len(bot.sent) == 1, "intake must produce exactly one window message"
    window = notifier._windows[CHAT]
    assert window.message_id is not None
    text = bot.sent[0]
    assert "TellyBee" in text
    assert "E01.mkv" in text
    assert "1 queued" in text
    await notifier.aclose()


async def test_burst_coalesces_into_one_window(settings, tmp_path, monkeypatch):
    """A burst of forwards (e.g. a 35-file batch) is ONE message, not 35."""
    _fast(monkeypatch)
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)

    for i in range(6):
        notifier.attach(_job(tmp_path, f"E{i:02d}.mkv"))
    await asyncio.sleep(0.3)

    assert len(bot.sent) == 1
    assert bot.deleted == []  # nothing to abort on first creation
    assert "6 queued" in bot.sent[0]
    await notifier.aclose()


async def test_new_intake_replaces_window(settings, tmp_path, monkeypatch):
    """A new task aborts the old window and sends a fresh latest message."""
    _fast(monkeypatch)
    db = _FakeDb()
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings, db=db)

    first = _job(tmp_path, "E01.mkv")
    notifier.attach(first)
    await asyncio.sleep(0.25)
    old_id = notifier._windows[CHAT].message_id
    assert old_id is not None

    notifier.attach(_job(tmp_path, "E02.mkv"))
    await asyncio.sleep(0.25)

    window = notifier._windows[CHAT]
    assert bot.deleted == [(CHAT, old_id)]  # previous frame aborted
    assert len(bot.sent) == 2  # …and replaced by a fresh message
    assert window.message_id == old_id + 1
    assert window.message_id not in {old_id}
    assert db.rows[CHAT] == window.message_id  # persisted for crash cleanup
    assert "2 queued" in bot.sent[1]
    await notifier.aclose()


async def test_window_edits_show_progress(settings, tmp_path, monkeypatch):
    _fast(monkeypatch)
    monkeypatch.setattr(notifier_mod, "_EDIT_MIN_GAP", 0.0)
    settings.progress_edit_interval = 0.05
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    job = _job(tmp_path, "E01.mkv")
    notifier.attach(job)
    await asyncio.sleep(0.25)
    assert len(bot.sent) == 1

    job.status = JobStatus.RUNNING
    job.started_at = time.monotonic() - 10
    job.total_bytes = job.file_size
    job.bytes_done = int(job.file_size * 0.42)
    job.speed_bps = 9_000_000

    await asyncio.sleep(0.25)
    assert bot.edits, "the window must be edited in place as progress moves"
    latest = bot.edits[-1]
    assert "42%" in latest
    assert "E01.mkv" in latest
    assert len(bot.sent) == 1  # still the SAME message — no new sends
    await notifier.aclose()


async def test_window_closes_when_queue_drains(settings, tmp_path, monkeypatch):
    """Drained queue → the window is deleted; only the receipt remains."""
    _fast(monkeypatch)
    db = _FakeDb()
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings, db=db)
    job = _job(tmp_path, "E01.mkv")
    notifier.attach(job)
    await asyncio.sleep(0.25)
    window_id = notifier._windows[CHAT].message_id
    assert window_id is not None
    assert db.rows == {CHAT: window_id}

    await notifier.finish(_completed(job))
    await asyncio.sleep(0.25)

    assert (CHAT, window_id) in bot.deleted  # window gone
    assert db.rows == {}  # …and its row too
    assert CHAT not in notifier._windows
    assert "Saved" in bot.sent[-1]  # the receipt survived
    await notifier.aclose()


async def test_done_counter_shown_while_queue_runs(settings, tmp_path, monkeypatch):
    """Session counters survive message replacements — 'N done' adds up."""
    _fast(monkeypatch)
    monkeypatch.setattr(notifier_mod, "_EDIT_MIN_GAP", 0.0)
    settings.progress_edit_interval = 0.05
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    jobs = [_job(tmp_path, f"E{i:02d}.mkv") for i in range(3)]
    for job in jobs:
        notifier.attach(job)
    await asyncio.sleep(0.25)

    await notifier.finish(_completed(jobs[0]))
    jobs[1].status = JobStatus.RUNNING
    jobs[1].total_bytes = jobs[1].file_size
    jobs[1].bytes_done = 1_000_000
    await asyncio.sleep(0.3)

    assert notifier._windows[CHAT].message_id is not None  # still open
    latest = bot.edits[-1] if bot.edits else bot.sent[-1]
    assert "1 done" in latest
    assert "2 queued" in latest or "1 active" in latest
    await notifier.aclose()


async def test_album_collapses_to_one_entry(settings, tmp_path, monkeypatch):
    """A media group is one bar with sums, not ten near-identical blocks."""
    _fast(monkeypatch)
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    a = _job(tmp_path, "a.jpg", size=1_000_000, album_key="alb")
    b = _job(tmp_path, "b.jpg", size=1_000_000, album_key="alb")
    for job in (a, b):
        notifier.attach(job)
    a.status = JobStatus.RUNNING
    a.total_bytes = a.file_size
    a.bytes_done = 500_000

    text = notifier._window_text(notifier._windows[CHAT])
    assert text.count("<code>") == 1  # exactly ONE bar for the whole album
    assert "a.jpg +1" in text
    assert "1 active" in text
    await notifier.aclose()


async def test_prefetched_job_shows_real_fetch_progress(settings, tmp_path):
    """A QUEUED-but-fetching job is active in the window with real numbers."""
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    job = _job(tmp_path, "movie.mkv", size=800_000_000)
    job.status = JobStatus.QUEUED
    notifier.attach(job)
    window = notifier._windows[CHAT]

    job.waiting_since = time.monotonic()
    job.fetch_total = 800_000_000
    job.fetch_done = 320_000_000
    job.fetch_speed = 2_100_000

    text = notifier._window_text(window)
    assert "Telegram fetch" in text
    assert "40%" in text
    assert "305 MB / 763 MB" in text
    assert "2.0 MB/s" in text
    assert "Queued" not in text  # fetching ≠ waiting in queue
    assert "0 B / 763 MB" not in text  # no fake transfer block

    # …without watcher data it degrades to the elapsed-time hint
    job.fetch_done = None
    job.fetch_total = None
    text = notifier._window_text(window)
    assert "fetching from Telegram" in text
    assert "so far" in text
    assert "Telegram fetch ·" not in text
    await notifier.aclose()


# ── pacing & resilience ────────────────────────────────────────────────────


async def test_routine_edits_are_droppable_not_queued(settings, tmp_path):
    """Under pacing, a routine refresh is skipped, not piled onto a backlog."""
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    job = _job(tmp_path)
    notifier.attach(job)
    window = notifier._windows[CHAT]
    window.message_id = 7
    settings.progress_edit_interval = 0.0

    # an edit is already in flight → the next routine refresh is dropped
    await notifier._edit_lock.acquire()
    await notifier._refresh(window)
    assert bot.edits == []

    # inside the global gap → dropped as well, never queued
    notifier._edit_lock.release()
    notifier._next_edit_at = time.monotonic() + 5.0
    await notifier._refresh(window)
    assert bot.edits == []
    await notifier.aclose()


async def test_window_edit_backs_off_after_flood_wait(settings, tmp_path):
    bot = _ScriptedBot(
        edit_errors=[TelegramRetryAfter(method=None, message="Too many requests",
                                        retry_after=5)],
    )
    notifier = Notifier(bot, settings)
    job = _job(tmp_path)
    notifier.attach(job)
    window = notifier._windows[CHAT]
    window.message_id = 7
    settings.progress_edit_interval = 0.0

    await notifier._refresh(window)  # 429 → dropped, budget parked
    assert window._last_edit > time.monotonic()
    assert bot.edits == []
    await notifier.aclose()


async def test_user_deleted_window_is_recreated(settings, tmp_path, monkeypatch):
    """A user deleting the window himself must not kill progress for good."""
    _fast(monkeypatch)
    bot = _ScriptedBot(
        edit_errors=[TelegramBadRequest(
            method=None, message="Bad Request: message to edit not found",
        )],
    )
    notifier = Notifier(bot, settings)
    job = _job(tmp_path, "E01.mkv")
    notifier.attach(job)
    await asyncio.sleep(0.25)
    window = notifier._windows[CHAT]
    first_id = window.message_id
    assert first_id is not None

    job.status = JobStatus.RUNNING
    job.bytes_done = 1
    window._last_edit = 0.0
    settings.progress_edit_interval = 0.0

    await asyncio.sleep(0.3)
    assert window.message_id is not None
    assert window.message_id != first_id  # rebuilt as a fresh message
    assert len(bot.sent) == 2
    await notifier.aclose()


async def test_window_send_flood_parks_instead_of_sleeping(settings, tmp_path):
    """Creating the window under a 429 parks the retry — no long sleep."""
    bot = _ScriptedBot(
        send_errors=[TelegramRetryAfter(method=None, message="Too many requests",
                                        retry_after=3)],
    )
    notifier = Notifier(bot, settings)
    job = _job(tmp_path)
    notifier.attach(job)
    window = notifier._windows[CHAT]

    before = time.monotonic()
    await notifier._recreate(window)
    assert time.monotonic() - before < 1.0  # returned immediately
    assert window.message_id is None
    assert window.kick_at is not None and window.kick_at > time.monotonic()
    assert bot.sent == []
    await notifier.aclose()


async def test_many_busy_chats_stay_paced(settings, tmp_path, monkeypatch):
    """End-to-end pacing: 6 busy chats, edits stay rate-limited globally."""
    _fast(monkeypatch)
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    windows = []
    for chat in range(200, 206):
        notifier.attach(_job(tmp_path, f"ep{chat}.mkv", chat_id=chat))
    await asyncio.sleep(0.3)
    assert len(bot.sent) == 6  # one window per chat
    for window in notifier._windows.values():
        windows.append(window)
        window._last_edit = 0.0
        window.live[list(window.live)[0]].status = JobStatus.RUNNING
    settings.progress_edit_interval = 0.0

    # 12 refresh attempts in quick succession (two rounds per chat)
    for _ in range(2):
        for window in windows:
            await notifier._refresh(window)

    # ~6 chats × 2 rounds fired within milliseconds — the global gap keeps
    # the actual API calls down to a handful instead of 12
    assert 1 <= len(bot.edits) <= 7
    await notifier.aclose()


# ── crash cleanup & shutdown ───────────────────────────────────────────────


async def test_startup_deletes_orphaned_windows(settings):
    """A crash leaves a frozen frame — startup() must sweep it away."""
    bot = _ScriptedBot()
    db = _FakeDb(rows={100: 55, 200: 66})
    notifier = Notifier(bot, settings, db=db)

    await notifier.startup()

    assert set(bot.deleted) == {(100, 55), (200, 66)}
    assert sorted(db.drops) == [100, 200]
    assert db.rows == {}
    assert bot.sent == []


async def test_aclose_sweeps_live_windows(settings, tmp_path, monkeypatch):
    """A clean shutdown must not leave the window behind either."""
    _fast(monkeypatch)
    db = _FakeDb()
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings, db=db)
    notifier.attach(_job(tmp_path, "E01.mkv"))
    await asyncio.sleep(0.25)
    window_id = notifier._windows[CHAT].message_id
    assert window_id is not None

    await notifier.aclose()

    assert (CHAT, window_id) in bot.deleted
    assert db.rows == {}
    assert notifier._windows == {}


async def test_window_db_errors_never_break_ux(settings, tmp_path, monkeypatch):
    """A broken window row is bookkeeping, not a download."""

    class _AngryDb(_FakeDb):
        async def upsert_window(self, chat_id, message_id):
            raise RuntimeError("db on fire")

        async def drop_window(self, chat_id):
            raise RuntimeError("db on fire")

    _fast(monkeypatch)
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings, db=_AngryDb())
    job = _job(tmp_path, "E01.mkv")
    notifier.attach(job)
    await asyncio.sleep(0.25)
    assert notifier._windows[CHAT].message_id is not None  # window alive

    await notifier.finish(_completed(job))
    await asyncio.sleep(0.25)
    assert "Saved" in bot.sent[-1]  # receipt still delivered
    await notifier.aclose()


# ── render edge cases ──────────────────────────────────────────────────────


def test_empty_window_renders_bare_header(settings):
    notifier = Notifier(bot=object(), settings=settings)
    from app.services.notifier import _Window

    window = _Window(CHAT)
    text = notifier._window_text(window)
    assert text == "🐝 <b>TellyBee</b>"  # nothing live → bare header


async def test_active_entry_cap_keeps_message_small(settings, tmp_path):
    """8 simultaneous transfers → 6 detailed lines + a '+2 more' note."""
    bot = _ScriptedBot()
    notifier = Notifier(bot, settings)
    jobs = []
    for i in range(8):
        job = _job(tmp_path, f"E{i:02d}.mkv", album_key=None)
        job.status = JobStatus.RUNNING
        job.waiting_since = time.monotonic() - 30
        job.fetch_total = job.file_size
        job.fetch_done = int(job.file_size * 0.5)
        notifier.attach(job)
        jobs.append(job)

    text = notifier._window_text(notifier._windows[CHAT])
    assert "+2 more in progress" in text
    assert "E06.mkv" not in text  # beyond the cap → counted, not shown
    assert "E07.mkv" not in text
    await notifier.aclose()
