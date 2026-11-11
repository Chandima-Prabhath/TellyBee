"""Stall fast-recovery (0.7.0): a parked getFile never idles silently.

Live incident: E02 sat at "425 MB / 454 MB · ⏸ stalled" while the client-side
getFile call was parked with GETFILE_TIMEOUT=1800 — a real stall meant up to
30 idle minutes per re-ask cycle. Now the watcher arms a per-file_id timer
when the matched server-side file shows zero growth, and once that exceeds
FETCH_STALL_TIMEOUT the parked HTTP call is cancelled and re-issued
immediately — free, because the server keeps its download progress and the
new query attaches to the same fetch.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import app.queue.engine as engine_module
from app.queue.engine import DownloadEngine, _FileCache
from app.queue.jobs import DownloadJob, JobStatus, new_job_id

MB = 1024 * 1024


class ParkedBot:
    """getFile double: first call parks forever, later calls answer."""

    def __init__(self, file_path: str = "/var/lib/x/files/video/f.mp4"):
        self._file_path = file_path
        self.calls = 0

    async def get_file(self, file_id, request_timeout=None):
        self.calls += 1
        if self.calls == 1:
            await asyncio.sleep(3600.0)  # parked: server still "fetching"
        return SimpleNamespace(file_path=self._file_path)


def _engine(bot, *, stall_timeout: float, local: bool = True) -> DownloadEngine:
    engine = DownloadEngine.__new__(DownloadEngine)
    engine._bot = bot
    engine._settings = SimpleNamespace(
        getfile_timeout=1800.0, fetch_stall_timeout=stall_timeout
    )
    engine._local_mode = local
    engine._fetch_sem = None
    engine._fetch_started = {}
    engine._stall_since = {}
    engine._getfile_calls = {}
    engine._cache = _FileCache(900.0)
    return engine


async def test_stalled_fetch_is_poked_and_recovered(monkeypatch):
    monkeypatch.setattr(engine_module, "_GETFILE_CANCEL_POLL", 0.02)
    bot = ParkedBot()
    engine = _engine(bot, stall_timeout=0.1)

    # The watcher already armed the stall timer far in the past:
    engine._stall_since["f1"] = time.monotonic() - 999.0

    file = await asyncio.wait_for(engine._fetch_with_reissue("f1"), timeout=5.0)

    assert bot.calls == 2, "the parked call was not cancelled and re-issued"
    assert file.file_path == bot._file_path
    assert "f1" not in engine._stall_since
    assert "f1" not in engine._getfile_calls


async def test_healthy_fetch_is_never_poked(monkeypatch):
    monkeypatch.setattr(engine_module, "_GETFILE_CANCEL_POLL", 0.02)
    bot = ParkedBot()
    engine = _engine(bot, stall_timeout=0.1)
    # No stall armed → nothing pokes; the task stays parked on call #1.
    task = asyncio.create_task(engine._fetch_with_reissue("f1"))

    await asyncio.sleep(0.3)
    assert not task.done(), "the fetch ended without any poke or answer"
    assert bot.calls == 1, "a healthy parked fetch was needlessly re-issued"

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def test_poke_disabled_entirely_when_timeout_is_zero(monkeypatch):
    monkeypatch.setattr(engine_module, "_GETFILE_CANCEL_POLL", 0.02)
    bot = ParkedBot()
    engine = _engine(bot, stall_timeout=0)
    # Even armed, FETCH_STALL_TIMEOUT=0 must never poke:
    engine._stall_since["f1"] = time.monotonic() - 999.0

    task = asyncio.create_task(engine._fetch_with_reissue("f1"))
    await asyncio.sleep(0.3)

    assert bot.calls == 1
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def test_poke_rearms_its_own_window():
    engine = _engine(ParkedBot(), stall_timeout=10.0)
    engine._stall_since["f1"] = time.monotonic() - 20.0

    assert engine._poke_stalled_fetch("f1") is True
    # The window re-armed to "now": an immediate second poke is refused.
    assert engine._poke_stalled_fetch("f1") is False
    assert engine._stall_since["f1"] > time.monotonic() - 5.0


def test_poke_ignores_cloud_mode():
    engine = _engine(ParkedBot(), stall_timeout=10.0, local=False)
    engine._stall_since["f1"] = time.monotonic() - 999.0

    assert engine._poke_stalled_fetch("f1") is False


# ── the watcher arms the timer from real volume scans ────────────────────


def _job(name: str, size: int, file_id: str = "f1") -> DownloadJob:
    return DownloadJob(
        job_id=new_job_id(),
        user=SimpleNamespace(id=1, username="u", first_name="U"),
        chat_id=100,
        message_id=1,
        media_type="document",
        file_id=file_id,
        file_unique_id="u" + name,
        file_name=name,
        file_size=size,
        target_path="/tmp/x",
        rel_path=f"user/document/{name}",
        status=JobStatus.RUNNING,
    )


def _engine_with_matcher() -> DownloadEngine:
    engine = DownloadEngine.__new__(DownloadEngine)
    engine._scan_prev = {}
    engine._fetch_prev = {}
    engine._fetch_started = {}
    engine._stall_since = {}
    return engine


def _candidates(*entries):
    return [
        (size * MB, time.time(), path, grew)
        for path, size, grew in entries
    ]


def test_matcher_arms_the_timer_on_zero_growth():
    engine = _engine_with_matcher()
    path = "cache/e02.mkv"
    job = _job("E02.mkv", 454 * MB)
    job.waiting_since = time.monotonic() - 60

    # First match (no history yet — the timer may arm) …
    engine._scan_prev = {}
    engine._match_fetch_progress([job], _candidates((path, 10, True)))
    # … then the file GROWS: speed turns positive, timer disarms:
    engine._scan_prev = {path: 10 * MB}
    engine._match_fetch_progress([job], _candidates((path, 20, True)))
    assert "f1" not in engine._stall_since

    # No-growth passes: the forced 0.0 speed arms the timer:
    for _ in range(4):
        engine._scan_prev = {path: 20 * MB}
        engine._match_fetch_progress([job], _candidates((path, 20, False)))
    assert "f1" in engine._stall_since

    # Completion disarms it (the honest 100% "finishing" tail):
    engine._scan_prev = {path: 454 * MB}
    engine._match_fetch_progress([job], _candidates((path, 454, False)))
    assert "f1" not in engine._stall_since


def test_scan_gap_keeps_the_timer_armed():
    """A matched file that drops out of the 120 s mtime scan window has had
    zero growth for at least that long — forgetting it would silence the
    poke exactly when it is needed most, so the arming must survive."""
    engine = _engine_with_matcher()
    job = _job("E02.mkv", 454 * MB)
    engine._stall_since["f1"] = time.monotonic() - 999.0

    engine._match_fetch_progress([job], [])  # scan sees nothing at all

    assert "f1" in engine._stall_since
