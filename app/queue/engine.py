"""The download engine: streams files from Telegram to disk.

Design goals — fast, stable, observable:

* **Streaming** — constant memory (one chunk), no full-file buffers.
* **Atomic** — writes ``<name>.part`` then renames; a crash never leaves a
  half-written file that looks complete.
* **Resilient** — retries with exponential backoff + jitter, tolerant of
  Telegram's flaky ``getFile`` endpoint and expiring file links.
* **Observable** — every chunk updates the job's progress counters that both
  the in-bot monitor and the HTTP health endpoint read.
* **Cancelling** — checks the job's cancel event between chunks so /cancel
  aborts a running transfer immediately.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from aiogram import Bot
from aiogram.exceptions import TelegramNetworkError

from app.config import Settings
from app.queue.jobs import DownloadJob

logger = logging.getLogger(__name__)


class JobCancelled(Exception):
    """Raised when a job's cancel event fires mid-transfer."""


class DownloadError(Exception):
    """All retries exhausted; the job failed."""


@dataclass(frozen=True, slots=True)
class EngineResult:
    sha256: str
    bytes_written: int
    duration_ms: int
    avg_speed: float


class _FileCache:
    """file_id → file_path cache (Telegram links are valid ~1 hour)."""

    def __init__(self, ttl: float):
        self._ttl = ttl
        self._entries: dict[str, tuple[str, float]] = {}

    def get(self, file_id: str) -> str | None:
        entry = self._entries.get(file_id)
        if not entry:
            return None
        path, expires = entry
        if time.monotonic() >= expires:
            self._entries.pop(file_id, None)
            return None
        return path

    def put(self, file_id: str, path: str) -> None:
        self._entries[file_id] = (path, time.monotonic() + self._ttl)


class DownloadEngine:
    """Executes single download jobs on behalf of the queue workers."""

    def __init__(self, bot: Bot, settings: Settings):
        self._bot = bot
        self._settings = settings
        self._cache = _FileCache(settings.file_cache_ttl)
        self._http: aiohttp.ClientSession | None = None
        self._timeout = aiohttp.ClientTimeout(
            total=settings.download_timeout,
            connect=15,
            sock_read=90,
        )

    async def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(
                timeout=self._timeout,
                connector=aiohttp.TCPConnector(
                    limit=max(8, self._settings.download_concurrency * 2),
                    ttl_dns_cache=300,
                ),
            )
        return self._http

    async def aclose(self) -> None:
        if self._http and not self._http.closed:
            await self._http.close()

    # ── URL helpers ─────────────────────────────────────────────────────────

    def file_url(self, file_path: str) -> str:
        """Absolute download URL, honoring a custom (local) Bot API server."""
        return self._bot.session.api.file_url(self._bot.token, file_path)

    async def resolve_file_path(self, file_id: str) -> str:
        """getFile with caching; retryable wrapper handles network hiccups."""
        cached = self._cache.get(file_id)
        if cached:
            return cached
        file = await self._bot.get_file(file_id)
        path = file.file_path
        if not path:
            raise DownloadError("getFile returned an empty file_path")
        self._cache.put(file_id, path)
        return path

    # ── main entrypoint ─────────────────────────────────────────────────────

    async def run(self, job: DownloadJob) -> EngineResult:
        last_error: Exception | None = None
        attempts = self._settings.download_retries + 1
        for attempt in range(1, attempts + 1):
            if job.is_cancelled():
                raise JobCancelled(job.job_id)
            job.attempts = attempt
            try:
                return await self._attempt(job)
            except JobCancelled:
                self._cleanup_part(job)
                raise
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                TelegramNetworkError,
                OSError,
            ) as err:
                last_error = err
                self._cleanup_part(job)
                if attempt < attempts:
                    delay = self._backoff_delay(attempt)
                    logger.warning(
                        "job #%s attempt %d/%d failed (%s: %s); retrying in %.1fs",
                        job.job_id, attempt, attempts, type(err).__name__, err, delay,
                    )
                    await self._sleep_cancellable(job, delay)
                else:
                    logger.error(
                        "job #%s failed after %d attempts: %s",
                        job.job_id, attempts, err,
                    )
        raise DownloadError(str(last_error or "download failed"))

    async def _attempt(self, job: DownloadJob) -> EngineResult:
        started = time.monotonic()
        job.started_at = started
        file_path = await self.resolve_file_path(job.file_id)
        url = self.file_url(file_path)
        session = await self._session()

        target = Path(job.target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        part_file = target.with_name(target.name + ".part")

        digest = hashlib.sha256()
        bytes_written = 0
        try:
            async with session.get(url, raise_for_status=True) as resp:
                declared = resp.headers.get("Content-Length")
                if declared and declared.isdigit():
                    job.total_bytes = int(declared)
                elif job.file_size:
                    job.total_bytes = job.file_size

                job.bytes_done = 0
                job.speed_bps = 0.0
                with part_file.open("wb") as fh:
                    async for chunk in resp.content.iter_chunked(self._settings.chunk_size):
                        if job.is_cancelled():
                            raise JobCancelled(job.job_id)
                        if chunk:
                            fh.write(chunk)
                            digest.update(chunk)
                            bytes_written += len(chunk)
                            job.touch_progress(len(chunk))
            # Atomic finish: the rename is cheap and instant on POSIX.
            part_file.replace(target)
        except JobCancelled:
            raise
        except BaseException:
            part_file.unlink(missing_ok=True)
            raise

        duration_ms = int((time.monotonic() - started) * 1000)
        avg_speed = bytes_written / (duration_ms / 1000) if duration_ms else 0.0
        job.sha256 = digest.hexdigest()
        job.bytes_done = bytes_written
        job.duration_ms = duration_ms
        job.avg_speed = avg_speed
        return EngineResult(
            sha256=digest.hexdigest(),
            bytes_written=bytes_written,
            duration_ms=duration_ms,
            avg_speed=avg_speed,
        )

    # ── helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _cleanup_part(job: DownloadJob) -> None:
        part_file = Path(job.target_path + ".part")
        part_file.unlink(missing_ok=True)

    @staticmethod
    def _backoff_delay(attempt: int) -> float:
        base = min(2.0**attempt, 30.0)
        return base * (0.7 + 0.6 * random.random())

    @staticmethod
    async def _sleep_cancellable(job: DownloadJob, delay: float) -> None:
        """Sleep while staying responsive to cancellation."""
        try:
            await asyncio.wait_for(job.cancel_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass
