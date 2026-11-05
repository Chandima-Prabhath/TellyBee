"""The unified window UX (0.5): bottom anchoring + de-jargonized text.

The two things this file pins down — both regressions seen live:
* the window must stay the chat's LATEST message: every delivered receipt
  re-anchors it (delete + resend) while live work remains, and does NOT
  resurrect it after the last job (the receipt is then the bottom line);
* user-facing progress is plain progress — the internal local-mode
  vocabulary ("prefetch", "Telegram fetch", "fetched ahead of the queue",
  "copy was instant") never leaks into window or receipt text, and
  receipts still report the honest transfer speed.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from app.config import Settings
from app.queue.jobs import DownloadJob, JobStatus, new_job_id
from app.services.notifier import Notifier

VALID_TOKEN = "1234567890:AAEhBOWe4ifRhnZ2dPq1fDz0r9RYmc8V4zQ"


def _settings(tmp_path) -> Settings:
    return Settings(
        bot_token=VALID_TOKEN,
        download_dir=tmp_path / "downloads",
        db_path=tmp_path / "data" / "tellybee.db",
    )


def _job(name: str, size: int, status=JobStatus.RUNNING) -> DownloadJob:
    return DownloadJob(
        job_id=new_job_id(),
        user=SimpleNamespace(id=1, username="user", first_name="User"),
        chat_id=100,
        message_id=1,
        media_type="document",
        file_id="f",
        file_unique_id="u" + name,
        file_name=name,
        file_size=size,
        target_path="/tmp/x",
        rel_path=f"user/document/{name}",
        status=status,
    )


class FakeBot:
    """Captures sends/edits/deletes; never touches the network."""

    def __init__(self):
        self.sent: list[str] = []
        self.deleted: list[int] = []
        self._next_id = 1000

    async def send_message(self, chat_id, text, **kwargs):
        self.sent.append(text)
        self._next_id += 1
        return SimpleNamespace(message_id=self._next_id)

    async def delete_message(self, chat_id, message_id):
        self.deleted.append(message_id)

    async def edit_message_text(self, text, **kwargs):
        return SimpleNamespace(ok=True)


async def _drain() -> None:
    for _ in range(4):
        await asyncio.sleep(0)


async def test_receipt_reanchors_window_while_work_remains(tmp_path):
    bot, settings = FakeBot(), _settings(tmp_path)
    notifier = Notifier(bot, settings, db=None)
    try:
        j1, j2 = _job("ep01.mkv", 100), _job("ep02.mkv", 100)
        notifier.attach(j1)
        notifier.attach(j2)
        await _drain()
        window = notifier._windows[100]
        window.kick_at = None  # simulate a settled, already-created window
        window.message_id = 555

        j1.status = JobStatus.COMPLETED
        j1.bytes_done = 100
        await notifier.finish(j1)

        # The receipt went out…
        assert bot.sent and "Saved" in bot.sent[-1]
        # …and the window is scheduled to move below it, because ep02 runs on.
        assert window.kick_at is not None, "window must be re-anchored after a receipt"
        assert j2.job_id in window.live
    finally:
        await notifier.aclose()


async def test_last_receipt_does_not_resurrect_window(tmp_path):
    bot, settings = FakeBot(), _settings(tmp_path)
    notifier = Notifier(bot, settings, db=None)
    try:
        j1 = _job("ep01.mkv", 100)
        notifier.attach(j1)
        await _drain()
        window = notifier._windows[100]
        window.kick_at = None
        window.message_id = 555

        j1.status = JobStatus.COMPLETED
        j1.bytes_done = 100
        await notifier.finish(j1)

        assert window.kick_at is None, "no re-anchor when the queue drained"
    finally:
        await notifier.aclose()


def _window_text_with_active_job(tmp_path) -> str:
    notifier = Notifier.__new__(Notifier)
    notifier._settings = _settings(tmp_path)
    notifier._manager = SimpleNamespace(paused=False)
    job = _job("ep01.mkv", 325 * 1024 * 1024)
    job.waiting_since = time.monotonic() - 30
    job.fetch_done = 8 * 1024 * 1024
    job.fetch_total = 325 * 1024 * 1024
    job.fetch_speed = 90 * 1024
    window = SimpleNamespace(live={job.job_id: job}, done=0, failed=0, cancelled=0)
    return notifier._window_text(window)


def test_window_text_is_plain_download_progress(tmp_path):
    text = _window_text_with_active_job(tmp_path)
    lowered = text.lower()
    for jargon in ("prefetch", "telegram fetch", "fetching from telegram"):
        assert jargon not in lowered, f"internal jargon leaked into the window: {jargon!r}"
    # the user sees a normal bar + meta line
    assert "▰" in text or "▱" in text
    assert "%" in text
    assert "ETA" in text


def test_receipt_text_hides_prefetch_mechanics_keeps_honest_speed(tmp_path):
    notifier = Notifier.__new__(Notifier)
    notifier._settings = _settings(tmp_path)
    job = _job("ep01.mkv", 310 * 1024 * 1024, status=JobStatus.COMPLETED)
    job.bytes_done = 310 * 1024 * 1024
    job.fetched_ahead = True
    job.fetch_bps = 5.5 * 1024 * 1024  # the honest transfer speed
    group = SimpleNamespace(results=[job])
    text = notifier._receipt_text(group)
    lowered = text.lower()
    for jargon in (
        "prefetch",
        "fetched ahead",
        "copy was instant",
        "local server cache",
    ):
        assert jargon not in lowered, f"internal jargon leaked into the receipt: {jargon!r}"
    assert "5.5 MB/s" in text  # honest number survives, quietly
    assert "310 MB" in text
    assert "user/document" in text  # on-disk location stays


async def test_recreate_never_leaves_chat_without_a_window(tmp_path):
    """Regression: a flood-wait during (re)creation must not delete the old
    frame first — 10 quick receipts once triggered a 429 and the chat lost
    its window until the park expired (seen live as the window vanishing
    around the 10th file of a forwarded series)."""
    from aiogram.exceptions import TelegramRetryAfter

    class FloodBot(FakeBot):
        def __init__(self):
            super().__init__()
            self.order: list[str] = []
            self.flood_next = True

        async def send_message(self, chat_id, text, **kwargs):
            if self.flood_next:
                self.flood_next = False
                raise TelegramRetryAfter(method=None, message="flood", retry_after=9)
            self.order.append("send")
            return await super().send_message(chat_id, text, **kwargs)

        async def delete_message(self, chat_id, message_id):
            self.order.append("delete")
            await super().delete_message(chat_id, message_id)

    bot, settings = FloodBot(), _settings(tmp_path)
    notifier = Notifier(bot, settings, db=None)
    try:
        j1, j2 = _job("ep01.mkv", 100), _job("ep02.mkv", 100)
        notifier.attach(j1)
        notifier.attach(j2)
        await _drain()
        window = notifier._windows[100]
        window.kick_at = None
        window.message_id = 555

        # First attempt floods: the old frame (555) must survive untouched.
        await notifier._recreate(window)
        assert window.message_id == 555, "old frame was dropped before the new one existed"
        assert window.kick_at is not None, "recreate is parked for a retry, not abandoned"

        # Second attempt succeeds: send lands BEFORE the old frame is deleted.
        await notifier._recreate(window)
        assert window.message_id != 555
        assert bot.order[:2] == ["send", "delete"], (
            f"send must precede delete, got {bot.order}"
        )
    finally:
        await notifier.aclose()
