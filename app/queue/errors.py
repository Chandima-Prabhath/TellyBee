"""Shared queue error types and the engine result record.

Lives in its own module so the engine, the URL fetcher and the manager can
import them without import cycles (``app.queue.engine`` re-exports them for
backward compatibility).
"""

from __future__ import annotations

from dataclasses import dataclass


class JobCancelled(Exception):
    """Raised when a job's cancel event fires mid-transfer."""


class DownloadError(Exception):
    """Terminal, non-retryable failure with a user-readable message."""


@dataclass(frozen=True, slots=True)
class EngineResult:
    sha256: str
    bytes_written: int
    duration_ms: int
    avg_speed: float
