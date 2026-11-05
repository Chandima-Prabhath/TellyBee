"""Download job model — one unit of work flowing through the queue."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum

from app.services.filenaming import UserRef


def new_job_id() -> str:
    """Short, human-typeable id shown to users (e.g. 🧷 #a3f9c2)."""
    return uuid.uuid4().hex[:6]


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    #: Set at startup for jobs whose process died before finishing — the
    #: queue lives in memory, so a reboot loses everything not yet done.
    INTERRUPTED = "interrupted"
    #: An interrupted job that was put back into the queue (Re-queue button).
    REQUEUED = "requeued"


@dataclass(slots=True)
class DownloadJob:
    """A single file to fetch from Telegram and persist to disk."""

    # Identity / origin
    job_id: str
    user: UserRef
    chat_id: int
    message_id: int

    # Media payload
    media_type: str
    file_id: str
    file_unique_id: str
    file_name: str | None
    file_size: int | None

    # Destination
    target_path: str  # absolute path as str (Path not needed everywhere)
    rel_path: str  # display path relative to download root

    # Album grouping
    album_key: str | None = None

    # Queue mechanics
    priority: int = 0  # lower value = dequeued sooner
    seq: int = 0  # FIFO tiebreaker
    status: JobStatus = JobStatus.QUEUED
    enqueued_at: float = field(default_factory=time.monotonic)
    started_at: float | None = None
    finished_at: float | None = None
    attempts: int = 0
    error: str | None = None

    # Live progress (updated by the engine, read by the notifier/monitor)
    bytes_done: int = 0
    total_bytes: int | None = None
    speed_bps: float = 0.0
    sha256: str | None = None
    avg_speed: float | None = None
    duration_ms: int | None = None
    #: Set while the engine waits for the local Bot API server to finish
    #: fetching the file from Telegram (the getFile call blocks for the whole
    #: server-side download). The notifier shows a "preparing" phase instead
    #: of a frozen 0% progress bar during that window. Monotonic timestamp.
    waiting_since: float | None = None
    #: Live progress of the SERVER-SIDE fetch (local Bot API mode), filled by
    #: the engine's volume watcher: it watches the server's cache directory
    #: while getFile is parked and maps growing files to waiting jobs by their
    #: expected size. Kept separate from the transfer counters so receipts,
    #: totals and average speeds stay honest.
    fetch_done: int | None = None
    fetch_total: int | None = None
    fetch_speed: float = 0.0
    #: Average network speed of the server-side fetch (bytes/s), captured when
    #: the getFile wait ends. Used by receipts so a file that was prefetched
    #: while queued reports its real Telegram speed instead of the (absurd)
    #: disk-copy speed the worker measures afterwards.
    fetch_bps: float | None = None
    #: True when the file was already fully fetched before a worker picked the
    #: job up (prefetch finished, or a recent copy left it in the path cache) —
    #: the worker then only stream-copies it locally, which takes under a
    #: second and must not be presented as a "download speed".
    fetched_ahead: bool = False
    #: Watcher-internal sticky candidate path (inside the server's storage,
    #: contains the bot token — NEVER log or send it anywhere).
    fetch_path: str | None = None
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)

    # ── progress helpers ────────────────────────────────────────────────────

    @property
    def progress_fraction(self) -> float | None:
        total = self.total_bytes or self.file_size
        if not total:
            return None
        return min(1.0, self.bytes_done / total)

    @property
    def eta_seconds(self) -> float | None:
        total = self.total_bytes or self.file_size
        if not total or self.speed_bps <= 0:
            return None
        return max(0.0, (total - self.bytes_done) / self.speed_bps)

    @property
    def runtime_seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.finished_at or time.monotonic()
        return max(0.0, end - self.started_at)

    @property
    def fetch_fraction(self) -> float | None:
        """Progress of the server-side fetch (local Bot API mode), 0..1."""
        if not self.fetch_total or self.fetch_done is None:
            return None
        return min(1.0, self.fetch_done / self.fetch_total)

    @property
    def fetch_eta_seconds(self) -> float | None:
        if not self.fetch_total or self.fetch_done is None or self.fetch_speed <= 0:
            return None
        return max(0.0, (self.fetch_total - self.fetch_done) / self.fetch_speed)

    def is_cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def touch_progress(self, chunk_len: int) -> None:
        """Called by the engine after every written chunk.

        Speed is an exponential moving average so the displayed value reacts
        quickly but does not flicker.
        """
        now = time.monotonic()
        self.bytes_done += chunk_len
        runtime = now - (self.started_at or now)
        if runtime > 0:
            instantaneous = self.bytes_done / runtime
            alpha = 0.25
            self.speed_bps = (
                instantaneous if self.speed_bps == 0
                else (1 - alpha) * self.speed_bps + alpha * instantaneous
            )
