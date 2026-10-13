"""The download manager: a priority queue driven by a worker pool.

Responsibilities:

* Bounded submission (``MAX_QUEUE_SIZE``) with a friendly QueueFull error.
* N workers (``DOWNLOAD_CONCURRENCY``) pulling jobs in priority + FIFO order.
* A live registry of active/pending jobs for the in-bot ``/queue`` monitor
  and the HTTP ``/healthz`` endpoint.
* Pause/resume, cancel (pending or running), graceful drain on shutdown.
* Disk headroom tracking so intake can reject files that cannot fit.
* Durable state transitions written to SQLite (best effort — DB hiccups
  never kill a transfer).
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import shutil
import time
from dataclasses import dataclass, field

from aiogram import Bot

from app.config import Settings
from app.db import Database
from app.queue.engine import DownloadEngine, DownloadError, JobCancelled
from app.queue.jobs import DownloadJob, JobStatus, new_job_id

logger = logging.getLogger(__name__)

#: Sentinel pushed into the queue once per worker on shutdown.
_STOP: tuple[int, int, None] = (-1, -1, None)


class QueueFullError(Exception):
    """Raised when submitting beyond MAX_QUEUE_SIZE."""


@dataclass(slots=True)
class QueueSnapshot:
    """Point-in-time view used by /queue and /healthz."""

    active: list[DownloadJob] = field(default_factory=list)
    pending: list[DownloadJob] = field(default_factory=list)
    paused: bool = False
    workers_total: int = 0
    workers_busy: int = 0
    completed: int = 0
    failed: int = 0
    cancelled: int = 0
    started_at: float = field(default_factory=time.monotonic)

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self.started_at


class DownloadManager:
    """Owns the queue, the workers and the download engine."""

    def __init__(
        self,
        bot: Bot,
        db: Database,
        settings: Settings,
        notifier=None,  # Notifier; injected to avoid an import cycle
        engine: DownloadEngine | None = None,
    ):
        self._bot = bot
        self._db = db
        self._settings = settings
        self._notifier = notifier
        self._engine = engine or DownloadEngine(bot, settings)

        self._queue: asyncio.PriorityQueue[tuple[int, int, DownloadJob]] = asyncio.PriorityQueue()
        self._seq = itertools.count()
        self._workers: list[asyncio.Task] = []
        self._active: dict[str, DownloadJob] = {}
        self._pending: dict[str, DownloadJob] = {}
        self._cancel_flags: set[str] = set()

        self._resume_event = asyncio.Event()
        self._resume_event.set()  # not paused by default
        self._stopping = False

        self.completed = 0
        self.failed = 0
        self.cancelled = 0
        self.started_at = time.monotonic()

    # ── lifecycle ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._stopping = False
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"tellybee-worker-{i}")
            for i in range(self._settings.download_concurrency)
        ]
        logger.info("download manager started with %d workers", len(self._workers))

    async def shutdown(self, timeout: float = 25.0) -> None:
        """Graceful drain: finish running jobs, cancel queued ones."""
        if self._stopping:
            return
        self._stopping = True
        self.resume()  # unblock parked workers so they can see sentinels
        logger.info("draining download queue (active=%d, pending=%d)…",
                    len(self._active), len(self._pending))

        # Cancel everything still queued so the DB reflects reality.
        await self._drain_pending()

        # One sentinel per worker: idle workers exit immediately, busy ones
        # exit right after finishing their current job.
        for _ in self._workers:
            await self._queue.put(_STOP)

        try:
            await asyncio.wait_for(
                asyncio.gather(*self._workers, return_exceptions=True), timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning("workers did not finish within %.0fs; forcing cancel", timeout)
            for job in list(self._active.values()):
                job.cancel_event.set()
            await asyncio.wait_for(
                asyncio.gather(*self._workers, return_exceptions=True), timeout=10
            )
        finally:
            for worker in self._workers:
                if not worker.done():
                    worker.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)
            await self._engine.aclose()
            logger.info("download manager stopped")

    async def _drain_pending(self) -> None:
        while not self._queue.empty():
            try:
                entry = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if entry is _STOP:
                continue
            _, _, job = entry
            self._pending.pop(job.job_id, None)
            await self._finalize(job, JobStatus.CANCELLED, error="shutdown")
            self._queue.task_done()

    # ── submission ──────────────────────────────────────────────────────────

    async def submit(self, job: DownloadJob) -> None:
        """Enqueue a job. Raises :class:`QueueFullError` when saturated."""
        if self._stopping:
            raise QueueFullError("bot is shutting down")
        # Count tracked pending jobs (not raw qsize) so the check is immune
        # to the brief window where a worker holds a parked entry.
        if len(self._pending) >= self._settings.max_queue_size:
            raise QueueFullError(
                f"queue is full ({self._settings.max_queue_size} pending)"
            )
        job.seq = next(self._seq)
        job.status = JobStatus.QUEUED
        self._pending[job.job_id] = job
        await self._queue.put((job.priority, job.seq, job))
        await self._db_insert(job)
        logger.info("queued job #%s (%s, %s) from user %s",
                    job.job_id, job.media_type, job.rel_path, job.user.id)
        if self._notifier:
            self._notifier.attach(job)

    # ── worker loop ─────────────────────────────────────────────────────────

    async def _worker(self, index: int) -> None:
        while True:
            await self._resume_event.wait()
            try:
                entry = await self._queue.get()
            except asyncio.CancelledError:
                return
            job: DownloadJob | None = None if entry is _STOP else entry[2]
            try:
                if entry is _STOP:
                    return

                # Paused after we dequeued? Park the job back (order is kept
                # thanks to the (priority, seq) tuple) and wait for resume.
                if self.paused:
                    await self._queue.put(entry)
                    continue

                if job.job_id in self._cancel_flags:
                    self._cancel_flags.discard(job.job_id)
                    self._pending.pop(job.job_id, None)
                    await self._finalize(job, JobStatus.CANCELLED, error="cancelled before start")
                    continue

                self._pending.pop(job.job_id, None)
                self._active[job.job_id] = job
                await self._run_isolated(job)
            finally:
                if job is not None:
                    self._active.pop(job.job_id, None)
                self._queue.task_done()

    async def _run_isolated(self, job: DownloadJob) -> None:
        """Run one job so that no exception can kill the worker."""
        job.status = JobStatus.RUNNING
        job.started_at = time.monotonic()
        await self._db_update(job, JobStatus.RUNNING)
        try:
            result = await self._engine.run(job)
        except JobCancelled:
            await self._finalize(job, JobStatus.CANCELLED, error="cancelled by user")
        except DownloadError as err:
            await self._finalize(job, JobStatus.FAILED, error=str(err))
        except asyncio.CancelledError:
            # Worker/task cancelled during shutdown: abort cleanly.
            job.cancel_event.set()
            await self._finalize(job, JobStatus.CANCELLED, error="shutdown")
            raise
        except Exception as err:  # noqa: BLE001 — isolation is the point
            logger.exception("unexpected error in job #%s", job.job_id)
            await self._finalize(job, JobStatus.FAILED, error=f"{type(err).__name__}: {err}")
        else:
            await self._finalize(
                job, JobStatus.COMPLETED,
                sha256=result.sha256,
                avg_speed=result.avg_speed,
                duration_ms=result.duration_ms,
            )

    # ── control plane ───────────────────────────────────────────────────────

    def pause(self) -> None:
        self._resume_event.clear()
        logger.info("download queue paused")

    def resume(self) -> None:
        self._resume_event.set()
        logger.info("download queue resumed")

    @property
    def paused(self) -> bool:
        return not self._resume_event.is_set()

    def cancel(self, job_id: str) -> str | None:
        """Cancel a job. Returns 'pending' | 'running' | None (not found)."""
        job = self._pending.get(job_id)
        if job:
            self._cancel_flags.add(job_id)
            return "pending"
        job = self._active.get(job_id)
        if job:
            job.cancel_event.set()  # engine aborts at the next chunk boundary
            return "running"
        return None

    def find_job(
        self, job_id: str, user_id: int | None = None, admin: bool = False
    ) -> DownloadJob | None:
        """Look up a live job, optionally scoped to a user."""
        job = self._active.get(job_id) or self._pending.get(job_id)
        if job is None:
            return None
        if user_id is not None and not admin and job.user.id != user_id:
            return None
        return job

    # ── introspection ───────────────────────────────────────────────────────

    def disk_headroom(self) -> int | None:
        """Bytes still available on the downloads volume, minus queued work.

        Accounts for every pending and active job's declared size so a burst
        of submissions can't collectively overcommit the disk. Returns None
        when the free space cannot be determined (never block intake on a
        stat failure — the engine still fails fast on ENOSPC).
        """
        try:
            free = shutil.disk_usage(str(self._settings.download_dir)).free
        except OSError:
            return None
        committed = sum(
            job.file_size or 0
            for job in (self._pending | self._active).values()
        )
        return free - committed

    def has_file(self, file_unique_id: str) -> bool:
        """True when the exact same Telegram file is already queued/running.

        Guards against double-submission races (the same file sent twice in
        quick succession before either job finishes and hits the DB ledger).
        """
        if not file_unique_id:
            return False
        return any(
            job.file_unique_id == file_unique_id
            for job in (self._pending | self._active).values()
        )

    def snapshot(self) -> QueueSnapshot:
        return QueueSnapshot(
            active=sorted(self._active.values(), key=lambda j: j.started_at or 0),
            pending=sorted(self._pending.values(), key=lambda j: j.seq),
            paused=self.paused,
            workers_total=len(self._workers),
            workers_busy=len(self._active),
            completed=self.completed,
            failed=self.failed,
            cancelled=self.cancelled,
            started_at=self.started_at,
        )

    def jobs_for_user(self, user_id: int) -> tuple[list[DownloadJob], list[DownloadJob]]:
        active = [j for j in self._active.values() if j.user.id == user_id]
        pending = [j for j in self._pending.values() if j.user.id == user_id]
        return active, pending

    # ── internal persistence ────────────────────────────────────────────────

    async def _finalize(
        self,
        job: DownloadJob,
        status: JobStatus,
        error: str | None = None,
        sha256: str | None = None,
        avg_speed: float | None = None,
        duration_ms: int | None = None,
    ) -> None:
        job.status = status
        job.error = error
        job.finished_at = time.monotonic()
        if status is JobStatus.COMPLETED:
            self.completed += 1
        elif status is JobStatus.FAILED:
            self.failed += 1
        elif status is JobStatus.CANCELLED:
            self.cancelled += 1
        await self._db_update(
            job, status, error=error, sha256=sha256,
            avg_speed=avg_speed, duration_ms=duration_ms, finished=True,
        )
        if self._notifier:
            try:
                await self._notifier.finish(job)
            except Exception:  # noqa: BLE001
                logger.exception("notifier failed for job #%s", job.job_id)

    async def _db_insert(self, job: DownloadJob) -> None:
        try:
            await self._db.insert_download(
                {
                    "job_id": job.job_id,
                    "user_id": job.user.id,
                    "username": job.user.username,
                    "chat_id": job.chat_id,
                    "message_id": job.message_id,
                    "album_key": job.album_key,
                    "media_type": job.media_type,
                    "file_id": job.file_id,
                    "file_unique_id": job.file_unique_id,
                    "file_name": job.file_name,
                    "file_size": job.file_size,
                    "path": job.rel_path,
                    "status": JobStatus.QUEUED.value,
                }
            )
        except Exception:  # noqa: BLE001
            logger.exception("failed to record job #%s in db", job.job_id)

    async def _db_update(
        self, job: DownloadJob, status: JobStatus, *, error: str | None = None,
        sha256: str | None = None, avg_speed: float | None = None,
        duration_ms: int | None = None, finished: bool = False,
    ) -> None:
        try:
            await self._db.update_download(
                job.job_id, status.value, error=error, sha256=sha256,
                avg_speed=avg_speed, duration_ms=duration_ms, finished=finished,
            )
        except Exception:  # noqa: BLE001
            logger.exception("failed to update job #%s in db", job.job_id)


def build_job(**kwargs) -> DownloadJob:
    """Factory used by handlers; fills in a fresh job id when missing."""
    kwargs.setdefault("job_id", new_job_id())
    return DownloadJob(**kwargs)
