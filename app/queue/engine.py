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

Two transport modes:

* **Cloud Bot API** — files are fetched over HTTPS from
  ``https://api.telegram.org/file/bot<token>/<path>``.
* **Local Bot API server** (``--local`` mode) — ``getFile`` returns an
  *absolute path* inside the server's own storage, and the server has **no
  HTTP file endpoint at all** (any ``/file/...`` request is answered with
  404). The server's data directory must therefore be shared with this
  container — a read-only mount is enough — and we stream-copy the file
  straight from disk, which is also much faster than going through HTTP.
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

    #: Where the local Bot API server keeps received files. In ``--local``
    #: mode ``getFile`` returns absolute paths inside this directory and the
    #: server exposes no HTTP file endpoint — the directory must be shared
    #: with this container (read-only mount suffices, we only ever read).
    #: See the ``botapi-data`` volume in docker-compose.yml.example.
    LOCAL_FILES_DIR = Path("/var/lib/telegram-bot-api")

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
        api = getattr(getattr(bot, "session", None), "api", None)
        self._local_mode = bool(getattr(api, "is_local", False))
        if self._local_mode:
            if self.LOCAL_FILES_DIR.is_dir():
                logger.info(
                    "local Bot API mode: files are copied straight from %s",
                    self.LOCAL_FILES_DIR,
                )
            else:
                logger.warning(
                    "local Bot API mode, but %s is not mounted in this container — "
                    "downloads WILL fail. Share the botapi data volume with the "
                    "tellybee service: add 'botapi-data:/var/lib/telegram-bot-api:ro' "
                    "to its volumes in docker-compose.yml, then recreate with "
                    "docker compose up -d --build",
                    self.LOCAL_FILES_DIR,
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
        """Absolute download URL for the cloud Bot API."""
        return self._bot.session.api.file_url(self._bot.token, file_path)

    def _redact(self, text: str) -> str:
        """Mask the bot token so errors stay safe for logs and chat receipts."""
        token = getattr(self._bot, "token", "")
        return text.replace(token, "***") if token else text

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
                        job.job_id, attempt, attempts, type(err).__name__,
                        self._redact(str(err)), delay,
                    )
                    await self._sleep_cancellable(job, delay)
                else:
                    logger.error(
                        "job #%s failed after %d attempts: %s",
                        job.job_id, attempts, self._redact(str(err)),
                    )
        raise DownloadError(self._redact(str(last_error or "download failed")))

    async def _attempt(self, job: DownloadJob) -> EngineResult:
        started = time.monotonic()
        job.started_at = started
        file_path = await self.resolve_file_path(job.file_id)
        if self._local_mode and file_path.startswith("/"):
            # Local Bot API server: getFile returns an absolute path inside
            # the server's storage and there is no HTTP file endpoint —
            # stream-copy the file from the (read-only) shared volume.
            return await self._copy_local_file(job, file_path, started)
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

        return self._finish(job, started, digest, bytes_written)

    async def _copy_local_file(
        self, job: DownloadJob, server_path: str, started: float
    ) -> EngineResult:
        """Stream a file directly from the local Bot API server's storage.

        The chunked read+write runs in a worker thread so large copies never
        block the event loop (and with it the bot's polling); progress and
        cancellation are still checked between chunks like in HTTP mode. The
        ``exists``/``stat``/``open`` probes below are sub-millisecond
        metadata syscalls on local disk — the bulk I/O is what matters.
        """
        source = Path(server_path)
        if not source.exists():  # noqa: ASYNC240 — metadata probe, see docstring
            if not self.LOCAL_FILES_DIR.is_dir():  # noqa: ASYNC240 — same
                raise DownloadError(
                    f"{self.LOCAL_FILES_DIR} is not mounted in this container, but the "
                    f"local Bot API server stores files there — add "
                    f"'botapi-data:/var/lib/telegram-bot-api:ro' to the tellybee "
                    f"service volumes in docker-compose.yml and recreate the container"
                )
            # The server can still be flushing a large file to disk; let the
            # normal retry/backoff machinery handle that (FileNotFoundError
            # is an OSError, so it is retryable).
            raise FileNotFoundError(
                f"file not (yet) present in local server storage: {server_path}"
            )

        target = Path(job.target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        part_file = target.with_name(target.name + ".part")

        digest = hashlib.sha256()
        bytes_written = 0
        loop = asyncio.get_running_loop()
        try:
            job.total_bytes = source.stat().st_size or job.file_size  # noqa: ASYNC240 — probe
            job.bytes_done = 0
            job.speed_bps = 0.0

            with source.open("rb") as src, part_file.open("wb") as dst:  # noqa: ASYNC230 — open only

                def _pump() -> bytes:
                    chunk = src.read(self._settings.chunk_size)
                    if chunk:
                        dst.write(chunk)
                    return chunk

                while True:
                    chunk = await loop.run_in_executor(None, _pump)
                    if not chunk:
                        break
                    if job.is_cancelled():
                        raise JobCancelled(job.job_id)
                    digest.update(chunk)
                    bytes_written += len(chunk)
                    job.touch_progress(len(chunk))
            # Atomic finish: same semantics as the HTTP path.
            part_file.replace(target)
        except JobCancelled:
            raise
        except BaseException:
            part_file.unlink(missing_ok=True)
            raise

        return self._finish(job, started, digest, bytes_written)

    @staticmethod
    def _finish(
        job: DownloadJob, started: float, digest: hashlib._Hash, bytes_written: int
    ) -> EngineResult:
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
