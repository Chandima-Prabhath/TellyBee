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
  container (read-only mount suffices) and we stream-copy the file straight
  from disk, which is also much faster than going through HTTP. Because
  the server writes every file with owner-only permissions (TDLib creates
  them ``0600``) as its *own* user, it must run under the same uid as this
  bot — the compose example wires that up — otherwise every read fails
  with ``PermissionError``.

Local-mode performance pipeline (v0.3+):

* **Prefetch** — the *slow* part of local mode is the server-side download
  from Telegram (``getFile`` blocks until TDLib has fetched the whole
  file; TDLib keeps only ~2 MiB of a file in flight per Telegram DC).
  Queued jobs therefore trigger their ``getFile`` immediately instead of
  waiting for a worker slot, so the server downloads ahead while workers
  copy finished files.
* **Bounded fetch concurrency** — *every* server-side fetch (worker- or
  prefetch-driven) shares the ``FETCH_CONCURRENCY`` slots. TDLib's ~2 MiB
  in-flight window per DC is the real bottleneck: letting 3 workers + 6
  prefetch slots fetch big files simultaneously (the v0.3.2 defaults)
  split that window nine ways and every fetch crawled at ~0–200 B/s,
  stalling the whole queue — one or two fetches at a time finish fast and
  keep the pipeline busy instead.
* **Fetch progress** — while ``getFile`` is parked, a watcher polls the
  server's shared storage for growing files and maps them to waiting jobs
  by expected size, so the chat shows real % / speed / ETA during the
  otherwise opaque "preparing" phase.
* **Direct links** — jobs with ``media_type == "url"`` bypass Telegram
  entirely and are fetched by :class:`~app.services.urlfetch.UrlDownloader`
  (parallel ranged connections, SSRF-guarded).
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import logging
import os
import random
import re
import stat
import time
from pathlib import Path

import aiohttp
from aiogram import Bot
from aiogram.exceptions import TelegramNetworkError

from app.config import Settings
from app.queue.errors import DownloadError, EngineResult, JobCancelled
from app.queue.jobs import DownloadJob, JobStatus
from app.services.urlfetch import (
    UrlDownloader,
    UrlRejected,
    disk_full_error,
    strip_query,
)

__all__ = ["DownloadEngine", "DownloadError", "EngineResult", "JobCancelled"]

logger = logging.getLogger(__name__)

#: How often the getFile wait checks the job's cancel event. A local Bot API
#: server answers getFile only after fully fetching the file from Telegram,
#: which can take minutes — /cancel must stay responsive during that window.
_GETFILE_CANCEL_POLL = 1.0
#: Total wall-clock budget for one file's server-side fetch (getFile
#: re-issues). Generous by design: a 4 GB file on a very slow link can take
#: many hours, and re-issuing getFile does not restart the server's progress
#: (it parks the query and answers when the download completes).
_GETFILE_TOTAL_BUDGET = 24 * 3600.0
#: Files in the server's storage older than this (wall-clock seconds) are
#: ignored by the fetch-progress watcher — they are cache, not active fetches.
_FETCH_SCAN_MAX_AGE = 120.0
#: Match tolerance when pairing a growing server-side file with a job's
#: expected size (TDLib may report a few bytes more than the API declared).
_FETCH_SIZE_TOLERANCE = 4096
_URL_SCRUB_RE = re.compile(r"https?://\S+")


# JobCancelled / DownloadError / EngineResult now live in app.queue.errors
# and are re-exported above for backward compatibility (manager, handlers and
# tests import them from app.queue.engine).


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
    #: The server writes files with owner-only permissions as its own user,
    #: so it must run under the SAME uid as this bot (1000); the compose
    #: example's botapi entrypoint arranges exactly that.
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
        # ── local-mode fetch pipeline state ─────────────────────────────
        # file_id → the ONE shared getFile task (many jobs may await it)
        self._inflight: dict[str, asyncio.Task] = {}
        # file_id → monotonic time when that task's getFile loop actually
        # began (after the fetch semaphore granted a slot). Lets a
        # prefetch-driven wait tell "queued behind other fetches" apart
        # from "fetching right now", and keeps waiting_since honest (the
        # true fetch start, not the moment a late waiter joined).
        self._fetch_started: dict[str, float] = {}
        # job_id → job while it waits for the server-side fetch (watcher input)
        self._watching: dict[str, DownloadJob] = {}
        # job_id → (last observed server-side size, monotonic time), for speed
        self._fetch_prev: dict[str, tuple[int, float]] = {}
        self._watcher_task: asyncio.Task | None = None
        # ONE bound for ALL concurrent server-side fetches — workers and
        # prefetch take slots from the same semaphore. Cloud mode needs no
        # bound (getFile there is a quick metadata call).
        self._fetch_sem: asyncio.Semaphore | None = (
            asyncio.Semaphore(settings.fetch_concurrency)
            if self._local_mode
            else None
        )
        self._url = UrlDownloader(settings) if settings.links_enabled else None
        if self._local_mode:
            if self.LOCAL_FILES_DIR.is_dir():
                logger.info(
                    "local Bot API mode: files are read straight from %s — the "
                    "Bot API server must run as the same uid as this bot; see "
                    "the botapi service entrypoint in docker-compose.yml.example",
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
        for task in list(self._inflight.values()):
            task.cancel()
        if self._inflight:
            await asyncio.gather(*self._inflight.values(), return_exceptions=True)
            self._inflight.clear()
        if self._watcher_task and not self._watcher_task.done():
            self._watcher_task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(self._watcher_task, return_exceptions=True)
        if self._url is not None:
            await self._url.aclose()
        if self._http and not self._http.closed:
            await self._http.close()

    # ── URL helpers ─────────────────────────────────────────────────────────

    def file_url(self, file_path: str) -> str:
        """Absolute download URL for the cloud Bot API."""
        return self._bot.session.api.file_url(self._bot.token, file_path)

    async def probe_url(self, url: str):
        """Probe a direct link (name / size / range support) before queueing."""
        if self._url is None:
            raise DownloadError(
                "link downloads are disabled on this server (LINKS_ENABLED=false)"
            )
        return await self._url.probe(url)

    def _redact(self, text: str) -> str:
        """Mask the bot token so errors stay safe for logs and chat receipts."""
        token = getattr(self._bot, "token", "")
        return text.replace(token, "***") if token else text

    def _scrub(self, job: DownloadJob, text: str) -> str:
        """Redact secrets from error text: the bot token, and the query
        string of any URL (signed links carry tokens in theirs)."""
        text = self._redact(text)
        if job.media_type == "url":
            text = _URL_SCRUB_RE.sub(lambda m: strip_query(m.group(0)), text)
        return text

    async def resolve_file_path(
        self, file_id: str, job: DownloadJob | None = None,
        from_prefetch: bool = False,
    ) -> str:
        """getFile with caching; retryable wrapper handles network hiccups.

        In local mode the Bot API server parks the request and answers only
        AFTER it has downloaded the whole file from Telegram — minutes (or
        hours for 4 GB files on slow links). Each call therefore runs with an
        extended per-request timeout (``GETFILE_TIMEOUT``), and a *timeout*
        does NOT burn the job's retry budget: the server keeps its download
        progress, so the wait simply re-issues getFile until the file is ready
        (bounded by a 24h hard cap). Real network errors still propagate to
        the normal retry/backoff machinery, and the whole wait races the
        job's cancel event so ``/cancel`` aborts promptly.

        All callers for the same ``file_id`` share ONE underlying getFile
        task (prefetcher and workers), so a file is only ever asked for
        once at a time. In local mode every such task holds one of the
        ``FETCH_CONCURRENCY`` fetch slots for its whole server-side
        download; ``from_prefetch=True`` waits to register display state
        until the fetch actually begins, so a deep queue of prefetched
        files does not spawn a wall of "preparing…" messages for fetches
        that are merely queued behind others.
        """
        cached = self._cache.get(file_id)
        if cached:
            return cached
        if self._local_mode:
            logger.info(
                "job #%s: waiting for the local Bot API server to fetch the file "
                "— first-time downloads of large files can take a while "
                "(GETFILE_TIMEOUT=%ss)",
                job.job_id if job else file_id[:12],
                round(self._settings.getfile_timeout),
            )
        task = await self._fetch_task(file_id)
        if job is not None:
            if from_prefetch:
                # Parked behind other fetches? Stay invisible (plain
                # "waiting in queue") until our turn actually comes.
                # Deliberate polling (not an Event): it must also wake for
                # /cancel on a queued job within a second, not whenever the
                # fetch finally starts.
                while (  # noqa: ASYNC110 — poll keeps /cancel responsive
                    file_id not in self._fetch_started
                    and not task.done()
                    and not job.is_cancelled()
                ):
                    await asyncio.sleep(_GETFILE_CANCEL_POLL)
            self._begin_fetch_wait(job, self._fetch_started.get(file_id))
        try:
            file = await self._race_cancel(task, job)
        finally:
            if job is not None:
                self._end_fetch_wait(job)
        path = file.file_path
        if not path:
            raise DownloadError("getFile returned an empty file_path")
        self._cache.put(file_id, path)
        return path

    async def _fetch_task(self, file_id: str) -> asyncio.Task:
        """The ONE shared getFile task per file_id (create or join)."""
        task = self._inflight.get(file_id)
        if task is not None and not task.done():
            return task
        if task is not None:
            # A finished task (success is cached, failure will be retried) —
            # consume its outcome so asyncio logs no warnings, then replace.
            if not task.cancelled():
                with contextlib.suppress(Exception):
                    task.exception()
        task = asyncio.create_task(
            self._fetch_with_reissue(file_id),
            name=f"tellybee-fetch-{file_id[:12]}",
        )

        def _cleanup(done: asyncio.Task, fid: str = file_id) -> None:
            if self._inflight.get(fid) is done:
                self._inflight.pop(fid, None)
            if not done.cancelled():
                with contextlib.suppress(Exception):
                    done.exception()

        task.add_done_callback(_cleanup)
        self._inflight[file_id] = task
        return task

    async def _fetch_with_reissue(self, file_id: str):
        """getFile, re-issuing on client-side timeouts (local mode only).

        Cloud getFile is a quick metadata call → session default (60s), no
        re-issues. Local getFile blocks for the whole server-side download →
        allow minutes per call (``GETFILE_TIMEOUT``), and when the call times
        out just ask again — the server attaches the new query to the SAME
        in-flight download and answers it on completion.

        In local mode the whole wait holds one ``FETCH_CONCURRENCY`` slot:
        the server-side fetch IS the scarce resource, and workers must
        compete for the same slots as prefetch instead of piling on top of
        them (that pile-up is what made busy queues crawl at ~0 B/s).

        The task is deliberately never cancelled from the outside: waiters
        race their own cancel events around it (see ``_race_cancel``), so one
        waiter bailing out never kills the fetch for the others. Successful
        results are cached here so every future waiter is instant.
        """
        sem = self._fetch_sem
        if sem is not None:
            await sem.acquire()
        try:
            self._fetch_started[file_id] = time.monotonic()
            deadline = (
                time.monotonic() + _GETFILE_TOTAL_BUDGET if self._local_mode else None
            )
            attempt = 0
            while True:
                request_timeout = (
                    round(self._settings.getfile_timeout) if self._local_mode else None
                )
                try:
                    file = await self._bot.get_file(
                        file_id, request_timeout=request_timeout
                    )
                except TelegramNetworkError as err:
                    timed_out = "timeout" in str(err).lower()
                    if (
                        deadline is None
                        or not timed_out
                        or time.monotonic() >= deadline
                    ):
                        raise  # real error, cloud mode, or out of budget → retries
                    attempt += 1
                    logger.info(
                        "the local Bot API server is still fetching file %s "
                        "(getFile timed out client-side after %ss, wait #%d) — asking "
                        "again; the server keeps its download progress",
                        file_id[:12],
                        round(self._settings.getfile_timeout),
                        attempt,
                    )
                else:
                    if file.file_path:
                        self._cache.put(file_id, file.file_path)
                    return file
        finally:
            if sem is not None:
                sem.release()
            self._fetch_started.pop(file_id, None)

    async def _race_cancel(
        self, task: asyncio.Task, job: DownloadJob | None
    ):
        """Await the shared fetch task, aborting early if THIS job is cancelled.

        Without this race a ``/cancel`` issued while the local server is
        still fetching the file would be ignored until the transfer
        finished (up to ``GETFILE_TIMEOUT`` later) — the job would then
        complete and deliver its receipt despite being cancelled. The
        shared task itself keeps running for any other waiters.
        """
        if job is None:
            return await task
        while not job.is_cancelled():
            done, _ = await asyncio.wait({task}, timeout=_GETFILE_CANCEL_POLL)
            if done:
                return task.result()
        raise JobCancelled(job.job_id)

    # ── fetch-wait bookkeeping (local mode) ─────────────────────────────────

    def _begin_fetch_wait(self, job: DownloadJob, since: float | None) -> None:
        """Mark a job as waiting on the server-side fetch (watcher input).

        ``since`` is the fetch's true start time when known (the fetch may
        have begun while the job was still queued), so speed stats measure
        the whole fetch, not just the tail a late waiter observed.
        """
        job.waiting_since = since if since is not None else time.monotonic()
        self._watching[job.job_id] = job
        self._ensure_watcher()

    def _end_fetch_wait(self, job: DownloadJob) -> None:
        """Clear the wait state, capturing honest stats for the receipt.

        ``fetch_bps`` is the server-side fetch's real average speed. The
        receipt needs it for files that finished fetching while still queued
        (the worker then only copies the file locally in under a second,
        which must never be presented as a "download speed").
        """
        if job.waiting_since is not None:
            waited = time.monotonic() - job.waiting_since
            total = job.fetch_total or job.file_size
            if total and waited > 0:
                job.fetch_bps = total / waited
        job.waiting_since = None
        self._watching.pop(job.job_id, None)
        self._fetch_prev.pop(job.job_id, None)
        job.fetch_done = None
        job.fetch_total = None
        job.fetch_speed = 0.0

    # ── prefetch (local mode) ──────────────────────────────────────────────

    async def prefetch(self, job: DownloadJob) -> None:
        """Start the server-side fetch for a queued job right away.

        The server-side download IS the slow part of local mode, and workers
        only trigger it when they pick a job — with a queue of N big files
        the server would otherwise idle between worker slots. Prefetching
        keeps the local Bot API server busy while workers copy whatever is
        already finished. Concurrency is bounded globally by
        ``FETCH_CONCURRENCY`` — the very same slots workers take, so
        prefetching can never pile extra simultaneous fetches on top of a
        fully busy queue (that pile-up is what used to crawl at ~0 B/s).
        Failures are logged and left to the worker's retry machinery.

        Pausing the queue does NOT pause prefetch: the server-side cache
        filling up is harmless and makes /resume instant.
        """
        if not self._local_mode or not self._settings.prefetch:
            return
        if self._cache.get(job.file_id):
            return
        if (
            job.is_cancelled()
            or job.status not in (JobStatus.QUEUED, JobStatus.RUNNING)
        ):
            return
        logger.info(
            "prefetch: asking the local Bot API server to start fetching "
            "job #%s (%s) ahead of time",
            job.job_id, job.rel_path,
        )
        try:
            await self.resolve_file_path(job.file_id, job=job, from_prefetch=True)
        except JobCancelled:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 — the worker retries properly
            logger.info(
                "prefetch for job #%s gave up (%s: %s) — the worker will retry",
                job.job_id, type(err).__name__, self._redact(str(err)),
            )
        else:
            # Fetched before any worker needed it — the receipt will show
            # the Telegram fetch speed instead of the instant local copy.
            job.fetched_ahead = True

    # ── fetch-progress watcher (local mode) ─────────────────────────────────

    def _ensure_watcher(self) -> None:
        """Start the volume watcher lazily; it idles cheaply when unused."""
        if not self._local_mode:
            return
        if self._watcher_task is None or self._watcher_task.done():
            self._watcher_task = asyncio.create_task(
                self._watch_fetches(), name="tellybee-fetch-watcher"
            )

    async def _watch_fetches(self) -> None:
        """Poll the server's storage and update waiting jobs' fetch progress.

        TDLib writes received files directly at their final path with
        ``pwrite`` at part offsets, so a file's size grows monotonically
        while the server is still fetching it — a cheap ``stat`` scan of
        recently-modified files gives real progress for the otherwise opaque
        getFile wait. Runs the scan in a worker thread; paths under the
        token directory are never logged (they contain the bot token).
        """
        interval = max(0.2, self._settings.fetch_watch_interval)
        while True:
            await asyncio.sleep(interval)
            jobs = [job for job in self._watching.values() if job.file_size]
            if not jobs:
                continue
            try:
                candidates = await asyncio.to_thread(self._scan_server_files)
            except OSError:
                continue
            if not candidates:
                for job in jobs:
                    job.fetch_done = None
                    job.fetch_speed = 0.0
                continue
            self._match_fetch_progress(jobs, candidates)

    def _scan_server_files(self) -> list[tuple[int, float, str]]:
        """Snapshot of recently-modified files in the server's storage.

        Returns ``(size, mtime, path)`` tuples for files modified within
        ``_FETCH_SCAN_MAX_AGE``. Blocking (stat storm) — always call via
        ``asyncio.to_thread``. Paths are internal and must never be logged.
        """
        root = Path(self.LOCAL_FILES_DIR) / (getattr(self._bot, "token", "") or "")
        if not root.is_dir():
            return []
        now = time.time()
        results: list[tuple[int, float, str]] = []
        for dirpath, _dirs, files in os.walk(root):
            depth = dirpath[len(str(root)) :].count(os.sep)
            if depth >= 4:
                continue
            for name in files:
                path = os.path.join(dirpath, name)
                try:
                    st = os.stat(path)
                except OSError:
                    continue
                if not stat.S_ISREG(st.st_mode):
                    continue
                if st.st_mtime < now - _FETCH_SCAN_MAX_AGE:
                    continue
                results.append((st.st_size, st.st_mtime, path))
        return results

    def _match_fetch_progress(
        self, jobs: list[DownloadJob], candidates: list[tuple[int, float, str]]
    ) -> None:
        """Pair growing server-side files with waiting jobs by expected size.

        Sticky first (a job keeps its matched file while it still fits), then
        largest-expected-first greedy matching for the rest. Sizes above the
        expected size (plus tolerance) can never be the job's file still
        downloading — those are already-complete cache entries.
        """
        now = time.monotonic()
        used: set[str] = set()
        matched: dict[str, tuple[int, float, str]] = {}
        # Pass 1: sticky rematching — keeps the display stable.
        for job in sorted(jobs, key=lambda j: -(j.file_size or 0)):
            path = job.fetch_path
            if not path:
                continue
            hit = next((c for c in candidates if c[2] == path), None)
            if (
                hit is not None
                and hit[2] not in used
                and hit[0] <= (job.file_size or 0) + _FETCH_SIZE_TOLERANCE
            ):
                matched[job.job_id] = hit
                used.add(hit[2])
        # Pass 2: fresh matches — biggest expected size picks first.
        for job in sorted(jobs, key=lambda j: -(j.file_size or 0)):
            if job.job_id in matched:
                continue
            expected = job.file_size or 0
            best: tuple[int, float, str] | None = None
            for size, _mtime, path in candidates:
                if path in used or size > expected + _FETCH_SIZE_TOLERANCE:
                    continue
                if best is None or size > best[0]:
                    best = (size, _mtime, path)
            if best is not None:
                matched[job.job_id] = best
                used.add(best[2])
        for job in jobs:
            hit = matched.get(job.job_id)
            if hit is None:
                job.fetch_done = None
                job.fetch_speed = 0.0
                job.fetch_path = None
                continue
            size, _mtime, path = hit
            prev = self._fetch_prev.get(job.job_id)
            if prev is not None and now > prev[1]:
                dt = now - prev[1]
                inst = max(0.0, (size - prev[0]) / dt)
                job.fetch_speed = (
                    inst if job.fetch_speed <= 0
                    else 0.7 * job.fetch_speed + 0.3 * inst
                )
            self._fetch_prev[job.job_id] = (size, now)
            job.fetch_path = path
            job.fetch_total = job.file_size
            job.fetch_done = size

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
            except (TimeoutError, aiohttp.ClientError, TelegramNetworkError, OSError) as err:
                last_error = err
                self._cleanup_part(job)
                if attempt < attempts:
                    delay = max(
                        self._backoff_delay(attempt),
                        self._retry_after_hint(err),
                    )
                    logger.warning(
                        "job #%s attempt %d/%d failed (%s: %s); retrying in %.1fs",
                        job.job_id, attempt, attempts, type(err).__name__,
                        self._scrub(job, str(err)), delay,
                    )
                    await self._sleep_cancellable(job, delay)
                else:
                    logger.error(
                        "job #%s failed after %d attempts: %s",
                        job.job_id, attempts, self._scrub(job, str(err)),
                    )
        raise DownloadError(self._failure_message(job, last_error))

    async def _attempt(self, job: DownloadJob) -> EngineResult:
        started = time.monotonic()
        job.started_at = started
        if job.media_type == "url":
            # Direct-link download: bypasses Telegram entirely.
            if self._url is None:
                raise DownloadError(
                    "link downloads are disabled on this server (LINKS_ENABLED=false)"
                )
            try:
                return await self._url.download(job, started)
            except UrlRejected as err:
                # Non-retryable (bad URL, HTTP 4xx, size cap, SSRF block…).
                raise DownloadError(str(err)) from err
        # A cache hit means the server already holds this file (prefetch
        # finished while the job was queued, or an earlier job fetched it
        # within the cache TTL): the worker will only stream-copy it — flag
        # that so the receipt reports the real network speed instead of a
        # meaningless "250 MB/s" disk-copy figure.
        job.fetched_ahead = self._cache.get(job.file_id) is not None
        file_path = await self.resolve_file_path(job.file_id, job=job)
        if self._local_mode and file_path.startswith("/"):
            # Local Bot API server: getFile returns an absolute path inside
            # the server's storage and there is no HTTP file endpoint —
            # stream-copy the file from the (read-only) shared volume. Works
            # only when the server runs as the same uid as this process.
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
        except OSError as err:
            # ENOSPC never resolves on retry — fail fast with the remedy.
            part_file.unlink(missing_ok=True)
            if err.errno == errno.ENOSPC:
                raise disk_full_error(err) from err
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
        cancellation are still checked between chunks like in HTTP mode.

        The server writes files as its *own* user with owner-only
        permissions (TDLib creates them ``0600``), so this only works when
        the server runs under the same uid as this process — the compose
        example wires that up. A ``PermissionError`` can never be fixed by
        retrying, so it is converted into a single actionable
        :class:`DownloadError` instead of burning the retry budget. The same
        goes for a full disk (``ENOSPC``) — retrying never frees space.
        """
        try:
            return await self._copy_local_file_inner(job, server_path, started)
        except PermissionError as err:
            raise self._local_permission_error(err) from err
        except OSError as err:
            if err.errno == errno.ENOSPC:
                raise self._disk_full_error(err) from err
            raise

    def _local_permission_error(self, err: PermissionError) -> DownloadError:
        """Turn EACCES into an actionable, non-retryable error message."""
        root = str(self.LOCAL_FILES_DIR)
        filename = str(err.filename or "")
        if filename == root or filename.startswith(root.rstrip("/") + "/"):
            remedy = (
                "the local Bot API server stores files with owner-only "
                "permissions as its own user, but this bot runs as a different "
                "one — run the botapi container as the SAME uid as tellybee "
                "(see the botapi service entrypoint in "
                "docker-compose.yml.example)"
            )
        else:
            remedy = (
                "this bot lacks write permission for its downloads directory — "
                "fix ownership on the host (sudo chown -R 1000:1000 "
                "./downloads) and restart"
            )
        return DownloadError(f"{self._redact(str(err))} — {remedy}")

    @staticmethod
    def _disk_full_error(err: OSError) -> DownloadError:
        """ENOSPC is as non-retryable as EACCES — make the remedy explicit.

        Shared with the URL downloader via ``app.services.urlfetch``.
        """
        return disk_full_error(err)

    async def _copy_local_file_inner(
        self, job: DownloadJob, server_path: str, started: float
    ) -> EngineResult:
        try:
            source_stat = Path(server_path).stat()  # noqa: ASYNC240 — metadata probe, sub-ms
        except FileNotFoundError as err:
            if not self.LOCAL_FILES_DIR.is_dir():  # noqa: ASYNC240 — same
                raise DownloadError(
                    f"{self.LOCAL_FILES_DIR} is not mounted in this container, but the "
                    f"local Bot API server stores files there — add "
                    f"'botapi-data:/var/lib/telegram-bot-api:ro' to the tellybee "
                    f"service volumes in docker-compose.yml and recreate the container"
                ) from None
            # The server can still be flushing a large file to disk; let the
            # normal retry/backoff machinery handle that (FileNotFoundError
            # is an OSError, so it is retryable).
            raise FileNotFoundError(
                f"file not (yet) present in local server storage: {server_path}"
            ) from err

        source = Path(server_path)
        target = Path(job.target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        part_file = target.with_name(target.name + ".part")

        digest = hashlib.sha256()
        bytes_written = 0
        loop = asyncio.get_running_loop()
        try:
            job.total_bytes = source_stat.st_size or job.file_size
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
        if job.fetched_ahead and job.fetch_bps:
            # The "transfer" was a local disk handoff of an already-fetched
            # file — the number users care about is the Telegram fetch
            # speed, not how fast the volume was copied.
            avg_speed = job.fetch_bps
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
    def _retry_after_hint(err: Exception) -> float:
        """A server-asked minimum wait for rate-limit errors, else 0.

        Honored (capped at 90s) when the last error is a 429/503 carrying
        ``Retry-After`` — plain exponential backoff is often too short for
        the very window the server just described.
        """
        status = getattr(err, "status", None)
        if status not in (429, 503):
            return 0.0
        headers = getattr(err, "headers", None)
        if not headers:
            return 0.0
        value = (headers.get("Retry-After") or "").strip()
        if not value.isdigit():
            return 0.0
        return min(float(value), 90.0)

    def _failure_message(self, job: DownloadJob, err: Exception | None) -> str:
        """User-facing text for a failed job — actionable where possible.

        Rate-limit failures get a plain-language explanation and remedies
        instead of aiohttp's opaque ``429, message='expected HTTP 206…'``.
        """
        status = getattr(err, "status", None)
        if status in (429, 503):
            waited = getattr(err, "retry_after", None)
            hint = (
                f"the file host is rate-limiting this server (HTTP {status})"
                + (f" and asked to retry after {waited:.0f}s" if waited else "")
                + ". Wait a few minutes and send the link again, or set "
                "LINK_CONNECTIONS=1 — some hosts refuse bursts of parallel "
                "connections but allow a single steady stream (seedr-style "
                "CDNs do this on busy free accounts)"
            )
            return hint
        return self._scrub(job, str(err or "download failed"))

    @staticmethod
    async def _sleep_cancellable(job: DownloadJob, delay: float) -> None:
        """Sleep while staying responsive to cancellation."""
        try:
            await asyncio.wait_for(job.cancel_event.wait(), timeout=delay)
        except TimeoutError:
            pass
