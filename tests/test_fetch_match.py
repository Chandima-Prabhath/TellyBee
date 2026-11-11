"""Fetch-progress matching (0.6.1): never pair a leftover cache file with a new job.

Live regression: forwarding a series, E02's window froze at "425 MB / 454 MB ·
⏸ stalled" — E01 was exactly 425 MB. The matcher's fresh pass picked the
largest candidate that fit the size window, and E01's COMPLETE leftover entry
(finished ~80 s ago, still within the 120 s scan window, 425 ≤ 454 + 4 KB)
beat E02's own small growing file. A complete file never grows, so the
stall detector then told the truth about the wrong file.

Fix under test: fresh matches only consider files that GREW since the
previous scan; sticky matches keep a job's own completed tail (the honest
100% "finishing" before getFile returns).
"""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

from app.queue.engine import DownloadEngine
from app.queue.jobs import DownloadJob, JobStatus, new_job_id
from app.utils.formatting import transfer_meta

MB = 1024 * 1024


def _job(name: str, size: int) -> DownloadJob:
    return DownloadJob(
        job_id=new_job_id(),
        user=SimpleNamespace(id=1, username="u", first_name="U"),
        chat_id=100,
        message_id=1,
        media_type="document",
        file_id="f",
        file_unique_id="u" + name,
        file_name=name,
        file_size=size,
        target_path="/tmp/x",
        rel_path=f"user/document/{name}",
        status=JobStatus.RUNNING,
    )


def _engine() -> DownloadEngine:
    engine = DownloadEngine.__new__(DownloadEngine)
    engine._scan_prev = {}
    engine._fetch_prev = {}
    engine._fetch_started = {}
    return engine


def _candidates(*entries):
    # entries: (path, size_MB, grew) → engine's (size, mtime, path, grew)
    return [
        (size * MB, time.time(), path, grew)
        for path, size, grew in entries
    ]


def test_fresh_match_ignores_leftover_complete_file():
    """E01's finished 425 MB entry must not shadow E02's growing file."""
    engine = _engine()
    e01_path, e02_path = "cache/e01.mkv", "cache/e02.mkv"
    # Previous scan saw E01 already complete at 425 MB.
    engine._scan_prev = {e01_path: 425 * MB}
    candidates = _candidates(
        (e01_path, 425, False),  # leftover, never grows
        (e02_path, 10, True),    # the real, live fetch
    )
    job = _job("E02.mkv", 454 * MB)
    job.waiting_since = time.monotonic() - 5

    engine._match_fetch_progress([job], candidates)

    assert job.fetch_path == e02_path, "matched a leftover complete file instead of the live one"
    assert job.fetch_done == 10 * MB


def test_no_live_files_means_preparing_not_frozen():
    """Only leftovers on the volume → no match, the UI shows 'preparing'."""
    engine = _engine()
    e01_path = "cache/e01.mkv"
    engine._scan_prev = {e01_path: 425 * MB}
    candidates = _candidates((e01_path, 425, False))
    job = _job("E02.mkv", 454 * MB)
    job.waiting_since = time.monotonic() - 5

    engine._match_fetch_progress([job], candidates)

    assert job.fetch_done is None
    assert job.fetch_path is None


def test_sticky_match_keeps_own_completed_tail():
    """A job's own file, now complete, stays matched → honest 100%."""
    engine = _engine()
    own_path = "cache/e02.mkv"
    engine._scan_prev = {own_path: 454 * MB}
    candidates = _candidates((own_path, 454, False))
    job = _job("E02.mkv", 454 * MB)
    job.waiting_since = time.monotonic() - 60
    job.fetch_path = own_path

    engine._match_fetch_progress([job], candidates)

    assert job.fetch_path == own_path
    assert job.fetch_done == 454 * MB  # 100% — finishing, not stalled


def test_completed_tail_renders_as_finishing_not_stalled():
    meta = transfer_meta(454 * MB, 454 * MB, 0.0)
    assert "finishing" in meta
    assert "stalled" not in meta


def test_incomplete_zero_speed_is_still_stalled():
    meta = transfer_meta(425 * MB, 454 * MB, 0.0)
    assert "stalled" in meta


def test_scan_tags_growth(tmp_path: Path):
    """_scan_server_files marks new/grown files live; static ones not."""
    engine = _engine()
    cache = tmp_path / "tok"
    cache.mkdir()
    engine.LOCAL_FILES_DIR = tmp_path
    engine._bot = SimpleNamespace(token="tok")

    e01 = cache / "e01.mkv"
    e01.write_bytes(b"x" * 100)

    first = engine._scan_server_files()
    assert first and first[0][3] is True  # brand-new → live

    second = engine._scan_server_files()
    assert second and second[0][3] is False  # unchanged → not live

    with open(e01, "ab") as fh:
        fh.write(b"y" * 100)
    third = engine._scan_server_files()
    assert third and third[0][3] is True  # grew again → live
