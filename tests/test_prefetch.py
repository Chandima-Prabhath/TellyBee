"""Prefetch pipeline, shared getFile tasks and the fetch-progress watcher.

Pins down the v0.3 local-mode performance contract:
* one shared getFile task per file_id (prefetcher + workers join it),
* prefetch fills the path cache and skips cancelled/finished jobs,
* the manager schedules a prefetch task per submit and cancels them on
  shutdown, and tripping a pending job's cancel event aborts its prefetch,
* the volume watcher maps growing files in the server's storage to waiting
  jobs (sticky, largest-expected-first) and feeds fetch progress,
* the notifier renders real fetch progress and creates the progress message
  for QUEUED-but-fetching jobs (nothing at all was shown before v0.3).
"""

from __future__ import annotations

import asyncio
import os
import time
from types import SimpleNamespace

import pytest
from aiogram.types import File

from app.config import Settings
from app.db import Database
from app.queue.engine import DownloadEngine
from app.queue.jobs import JobStatus, new_job_id
from app.queue.manager import DownloadManager, build_job
from app.services.filenaming import UserRef
from app.services.notifier import Notifier
from tests.conftest import VALID_TOKEN

USER = UserRef(111, "tester", "Tester")


class _FakeApi:
    def __init__(self, is_local: bool) -> None:
        self.is_local = is_local

    def file_url(self, token: str, path: str) -> str:
        return f"http://fake/bot{token}/{path}"


class _FakeBot:
    def __init__(self, is_local: bool, file: File, delay: float = 0.0) -> None:
        self.token = VALID_TOKEN
        self.session = SimpleNamespace(api=_FakeApi(is_local))
        self._file = file
        self._delay = delay
        self.calls: list[dict] = []

    async def get_file(self, file_id: str, request_timeout: int | None = None) -> File:
        self.calls.append({"file_id": file_id, "request_timeout": request_timeout})
        if self._delay:
            await asyncio.sleep(self._delay)
        return self._file


def _job(tmp_path, name: str = "movie.mp4", size: int = 100, media_type: str = "document"):
    target = tmp_path / "downloads" / name
    return build_job(
        job_id=new_job_id(),
        user=USER,
        chat_id=100,
        message_id=1,
        media_type=media_type,
        file_id="file-1",
        file_unique_id="uniq-1",
        file_name=name,
        file_size=size,
        target_path=str(target),
        rel_path=name,
    )


def _local_settings(settings) -> Settings:
    return Settings(
        bot_token=VALID_TOKEN,
        download_dir=settings.download_dir,
        db_path=settings.db_path,
        prefetch=True,
        prefetch_slots=4,
        fetch_watch_interval=0.2,  # validation floor; fast enough for tests
    )


# ── shared getFile task ────────────────────────────────────────────────────


async def test_concurrent_resolves_share_one_getfile(settings, tmp_path):
    bot = _FakeBot(is_local=True, file=File(file_id="f", file_unique_id="u", file_size=10,
                                            file_path="documents/file_0"), delay=0.2)
    engine = DownloadEngine(bot, settings)

    results = await asyncio.gather(
        engine.resolve_file_path("file-1"),
        engine.resolve_file_path("file-1"),
        engine.resolve_file_path("file-1"),
    )
    assert set(results) == {"documents/file_0"}
    assert len(bot.calls) == 1  # ONE shared request, three waiters
    await engine.aclose()


async def test_failed_fetch_task_is_replaced_on_retry(settings):
    class _FailingBot(_FakeBot):
        async def get_file(self, file_id, request_timeout=None):
            self.calls.append({"file_id": file_id, "request_timeout": request_timeout})
            raise RuntimeError("boom")

    bot = _FailingBot(is_local=False, file=None)  # type: ignore[arg-type]
    engine = DownloadEngine(bot, settings)

    with pytest.raises(RuntimeError, match="boom"):
        await engine.resolve_file_path("file-1")
    with pytest.raises(RuntimeError, match="boom"):
        await engine.resolve_file_path("file-1")
    assert len(bot.calls) == 2  # a failed task is not cached as "done"
    await engine.aclose()


# ── prefetch ───────────────────────────────────────────────────────────────


async def test_prefetch_fills_cache(settings, tmp_path):
    settings = _local_settings(settings)
    bot = _FakeBot(is_local=True, file=File(file_id="f", file_unique_id="u", file_size=10,
                                            file_path="documents/file_0"))
    engine = DownloadEngine(bot, settings)
    job = _job(tmp_path)
    job.status = JobStatus.QUEUED

    await engine.prefetch(job)

    assert engine._cache.get("file-1") == "documents/file_0"
    assert len(bot.calls) == 1
    # A worker picking the job up later hits the cache — no second request.
    path = await engine.resolve_file_path("file-1", job=job)
    assert path == "documents/file_0"
    assert len(bot.calls) == 1
    await engine.aclose()


async def test_prefetch_disabled_or_cloud_is_a_noop(settings, tmp_path):
    for kwargs in ({"prefetch": False}, {"prefetch_slots": 0}):
        tuned = Settings(
            bot_token=VALID_TOKEN,
            download_dir=settings.download_dir,
            db_path=settings.db_path,
            **kwargs,
        )
        bot = _FakeBot(is_local=True, file=File(file_id="f", file_unique_id="u",
                                                file_size=10, file_path="p"))
        engine = DownloadEngine(bot, tuned)
        await engine.prefetch(_job(tmp_path))
        assert bot.calls == []
        await engine.aclose()

    # cloud mode: getFile is a quick metadata call — prefetch must no-op
    bot = _FakeBot(is_local=False, file=File(file_id="f", file_unique_id="u",
                                             file_size=10, file_path="p"))
    engine = DownloadEngine(bot, _local_settings(settings))
    await engine.prefetch(_job(tmp_path))
    assert bot.calls == []
    await engine.aclose()


async def test_prefetch_aborts_when_job_cancelled(settings, tmp_path):
    settings = _local_settings(settings)
    bot = _FakeBot(is_local=True, file=File(file_id="f", file_unique_id="u", file_size=10,
                                            file_path="documents/file_0"), delay=30.0)
    engine = DownloadEngine(bot, settings)
    job = _job(tmp_path)
    job.status = JobStatus.QUEUED

    task = asyncio.create_task(engine.prefetch(job))
    await asyncio.sleep(0.15)
    job.cancel_event.set()
    await asyncio.wait_for(task, timeout=5.0)  # returns, no exception

    assert len(bot.calls) == 1  # the shared request was left running, not killed
    await engine.aclose()


async def test_prefetch_skips_terminal_jobs(settings, tmp_path):
    settings = _local_settings(settings)
    bot = _FakeBot(is_local=True, file=File(file_id="f", file_unique_id="u", file_size=10,
                                            file_path="documents/file_0"))
    engine = DownloadEngine(bot, settings)
    job = _job(tmp_path)
    job.status = JobStatus.COMPLETED

    await engine.prefetch(job)
    assert bot.calls == []
    await engine.aclose()


# ── manager wiring ──────────────────────────────────────────────────────────


class _RecordingEngine:
    def __init__(self) -> None:
        self.prefetched: list[str] = []
        self.cancelled_prefetch = False

    async def prefetch(self, job) -> None:
        self.prefetched.append(job.job_id)
        try:
            await asyncio.sleep(30.0)
        except asyncio.CancelledError:
            self.cancelled_prefetch = True
            raise

    async def run(self, job):  # pragma: no cover - not used here
        raise AssertionError("workers must not run in this test")

    async def aclose(self) -> None:
        pass


class _NullNotifier:
    def attach(self, job) -> None:
        pass

    async def finish(self, job) -> None:
        pass


async def test_manager_schedules_and_cancels_prefetch(tmp_path):
    settings = Settings(
        bot_token=VALID_TOKEN,
        download_dir=tmp_path / "downloads",
        db_path=tmp_path / "data" / "tellybee.db",
        prefetch=True,
        download_concurrency=1,
    )
    db = Database(settings.db_path)
    await db.connect()
    engine = _RecordingEngine()
    manager = DownloadManager(
        bot=None, db=db, settings=settings, notifier=_NullNotifier(), engine=engine
    )
    await manager.start()
    manager.pause()  # keep the worker parked: this test is about prefetch
    try:
        job = _job(tmp_path)
        await manager.submit(job)
        await asyncio.sleep(0.1)
        assert engine.prefetched == [job.job_id]

        await manager.shutdown()
        assert engine.cancelled_prefetch is True
    finally:
        await db.close()


async def test_cancel_pending_job_trips_prefetch(tmp_path):
    settings = Settings(
        bot_token=VALID_TOKEN,
        download_dir=tmp_path / "downloads",
        db_path=tmp_path / "data" / "tellybee.db",
        prefetch=True,
        download_concurrency=1,
    )
    db = Database(settings.db_path)
    await db.connect()
    engine = _RecordingEngine()
    manager = DownloadManager(
        bot=None, db=db, settings=settings, notifier=_NullNotifier(), engine=engine
    )
    await manager.start()
    manager.pause()  # keep the worker parked: this test is about prefetch
    try:
        job = _job(tmp_path)
        await manager.submit(job)
        assert manager.cancel(job.job_id) == "pending"
        assert job.is_cancelled()  # prefetch task will see this within a tick
        await manager.shutdown()
    finally:
        await db.close()


# ── fetch-progress watcher ──────────────────────────────────────────────────


def _watch_engine(settings, tmp_path, monkeypatch):
    bot = _FakeBot(is_local=True, file=File(file_id="f", file_unique_id="u", file_size=10,
                                            file_path="documents/file_0"))
    botapi = tmp_path / "botapi"
    token_dir = botapi / VALID_TOKEN / "documents"
    token_dir.mkdir(parents=True)
    monkeypatch.setattr(DownloadEngine, "LOCAL_FILES_DIR", botapi)
    engine = DownloadEngine(bot, _local_settings(settings))
    return engine, token_dir


async def test_watcher_matches_growing_file(settings, tmp_path, monkeypatch):
    engine, token_dir = _watch_engine(settings, tmp_path, monkeypatch)
    job = _job(tmp_path, "big.mkv", size=1000)
    engine._watching[job.job_id] = job

    growing = token_dir / "file_0"
    growing.write_bytes(b"x" * 400)

    candidates = engine._scan_server_files()
    assert len(candidates) == 1
    engine._match_fetch_progress([job], candidates)

    assert job.fetch_total == 1000
    assert job.fetch_done == 400
    assert job.fetch_path == str(growing)
    assert job.fetch_fraction == pytest.approx(0.4)

    # …the file keeps growing: progress and speed update, match stays sticky
    growing.write_bytes(b"x" * 900)
    await asyncio.sleep(0.05)
    engine._match_fetch_progress([job], engine._scan_server_files())
    assert job.fetch_done == 900
    assert job.fetch_speed > 0

    # …finished file (size == expected): 100 %
    growing.write_bytes(b"x" * 1000)
    engine._match_fetch_progress([job], engine._scan_server_files())
    assert job.fetch_fraction == 1.0
    await engine.aclose()


async def test_watcher_never_matches_larger_cache_files(settings, tmp_path, monkeypatch):
    engine, token_dir = _watch_engine(settings, tmp_path, monkeypatch)
    job = _job(tmp_path, "small.mkv", size=100)
    engine._watching[job.job_id] = job

    cached = token_dir / "already_done"
    cached.write_bytes(b"y" * 5000)  # far bigger than expected → cache, not match
    engine._match_fetch_progress([job], engine._scan_server_files())

    assert job.fetch_done is None
    assert job.fetch_speed == 0.0
    await engine.aclose()


async def test_watcher_largest_expected_matches_first(settings, tmp_path, monkeypatch):
    engine, token_dir = _watch_engine(settings, tmp_path, monkeypatch)
    big = _job(tmp_path, "big.mkv", size=2000)
    small = _job(tmp_path, "small.mkv", size=600)
    engine._watching[big.job_id] = big
    engine._watching[small.job_id] = small

    a = token_dir / "a"   # partial of the big file
    a.write_bytes(b"a" * 1200)
    b = token_dir / "b"   # complete-ish small file
    b.write_bytes(b"b" * 550)

    engine._match_fetch_progress([big, small], engine._scan_server_files())
    assert big.fetch_done == 1200
    assert big.fetch_path == str(a)
    assert small.fetch_done == 550
    assert small.fetch_path == str(b)
    await engine.aclose()


# ── notifier: fetch phase rendering ─────────────────────────────────────────


class _RecordingBot:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.edits: list[str] = []

    async def send_message(self, chat_id, text, **kwargs):
        self.sent.append(text)
        return SimpleNamespace(message_id=len(self.sent))

    async def edit_message_text(self, text, chat_id=None, message_id=None, **kwargs):
        self.edits.append(text)


async def test_notifier_progress_text_during_fetch(settings, tmp_path):
    bot = _RecordingBot()
    notifier = Notifier(bot, settings)
    job = _job(tmp_path, "movie.mkv", size=800_000_000)
    job.status = JobStatus.QUEUED
    notifier.attach(job)

    group = notifier._groups[f"job:{job.job_id}"]
    assert group.any_waiting is False

    # …the job enters the getFile wait with watcher data → real progress
    job.waiting_since = time.monotonic()
    job.fetch_total = 800_000_000
    job.fetch_done = 320_000_000
    job.fetch_speed = 2_100_000

    text = notifier._progress_text(group)
    assert "Fetching from Telegram" in text
    assert "40%" in text
    assert "305 MB / 763 MB" in text
    assert "2.0 MB/s" in text
    # a QUEUED-but-fetching job must not be double-counted as "waiting in queue"
    assert "waiting in queue" not in text
    # the transfer block stays hidden until the copy actually starts
    assert "0 B / 763 MB" not in text

    # …without watcher data it degrades to the elapsed-time hint
    job.fetch_done = None
    job.fetch_total = None
    text = notifier._progress_text(group)
    assert "Telegram is preparing" in text
    assert "Fetching from Telegram" not in text


async def test_notifier_message_created_for_queued_fetching_job(settings, tmp_path):
    """Regression: with prefetch, jobs fetch while still QUEUED.

    Before v0.3 the message was only created when a job reached RUNNING —
    a queue of big prefetched files showed nothing at all.
    """
    bot = _RecordingBot()
    notifier = Notifier(bot, settings)
    job = _job(tmp_path, "movie.mkv", size=800_000_000)
    job.status = JobStatus.QUEUED  # still queued…
    notifier.attach(job)

    # …but already fetching server-side:
    job.waiting_since = time.monotonic()
    job.fetch_total = 800_000_000
    job.fetch_done = 8_000_000

    await asyncio.sleep(0.9)  # a couple of updater ticks
    assert bot.sent, "progress message must exist for a QUEUED-but-fetching job"
    await notifier.aclose()


async def test_os_stat_growth_detection(settings, tmp_path, monkeypatch):
    """TDLib grows the file at its final path — the watcher sees it live."""
    engine, token_dir = _watch_engine(settings, tmp_path, monkeypatch)
    job = _job(tmp_path, "live.mkv", size=10_000)
    engine._watching[job.job_id] = job
    engine._ensure_watcher()

    growing = token_dir / "file_live"
    growing.write_bytes(b"z" * 100)
    for _ in range(30):  # up to ~1.5s at the 0.05s test interval
        await asyncio.sleep(0.05)
        if job.fetch_done is not None:
            break
        growing.write_bytes(growing.read_bytes() + b"z" * 300)

    assert job.fetch_done is not None, "watcher never matched the growing file"
    assert job.fetch_total == 10_000
    assert os.path.getsize(growing) >= 100  # noqa: ASYNC240 — test assertion
    await engine.aclose()
