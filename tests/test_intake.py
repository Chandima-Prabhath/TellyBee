"""Intake feature tests (0.2.0): duplicate skip, disk guard, waiting UX.

Covers:
* the DB ledger lookup by file_unique_id (latest completed row wins),
* _already_saved: skip when saved & still on disk, re-download when the
  file was deleted, "already in the queue" from the live registry,
* manager.disk_headroom subtracts committed work from free space,
* the notifier's "Telegram is preparing…" phase driven by waiting_since.
"""

from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace

from app.db import Database
from app.queue.jobs import JobStatus, new_job_id
from app.services.filenaming import UserRef

USER = UserRef(111, "tester", "Tester")


async def _memdb() -> Database:
    db = Database(Path(":memory:"))
    await db.connect()
    return db


async def _insert(db: Database, *, unique: str, status: str, path: str, job_id: str | None = None) -> None:
    await db.insert_download(
        {
            "job_id": job_id or new_job_id(),
            "user_id": 1,
            "username": "u",
            "chat_id": 1,
            "message_id": 1,
            "album_key": None,
            "media_type": "document",
            "file_id": "fid",
            "file_unique_id": unique,
            "file_name": "f.bin",
            "file_size": 10,
            "path": path,
            "status": status,
        }
    )
    if status != "queued":
        await db.update_download(job_id or "", status, finished=True)


class _Media:
    """Just the fields _already_saved cares about."""

    def __init__(self, unique: str):
        self.file_unique_id = unique


# ── ledger lookup ────────────────────────────────────────────────────────────


async def test_find_completed_by_unique_id():
    db = await _memdb()
    try:
        await _insert(db, unique="u1", status="failed", path="a/f.bin")
        assert await db.find_completed_by_unique_id("u1") is None

        await _insert(db, unique="u1", status="completed", path="a/f.bin", job_id="j1")
        row = await db.find_completed_by_unique_id("u1")
        assert row is not None and row["path"] == "a/f.bin"

        # A newer completed row for the same file wins (latest download).
        await _insert(db, unique="u1", status="completed", path="b/f.bin", job_id="j2")
        row = await db.find_completed_by_unique_id("u1")
        assert row is not None and row["path"] == "b/f.bin"

        assert await db.find_completed_by_unique_id("other") is None
    finally:
        await db.close()


# ── _already_saved ───────────────────────────────────────────────────────────


async def test_already_saved_skips_when_file_on_disk(settings, tmp_path):
    from app.handlers.media import _already_saved

    saved = settings.download_dir / "a" / "f.bin"
    saved.parent.mkdir(parents=True)
    saved.write_bytes(b"x")

    db = await _memdb()
    try:
        await _insert(db, unique="u1", status="completed", path="a/f.bin")
        manager = SimpleNamespace(has_file=lambda unique: False)
        result = await _already_saved(db, settings, manager, _Media("u1"))
        assert result == "a/f.bin"
    finally:
        await db.close()


async def test_already_saved_redownloads_when_deleted(settings, tmp_path):
    from app.handlers.media import _already_saved

    # Ledger row exists but the file was deleted from disk → fall through.
    db = await _memdb()
    try:
        await _insert(db, unique="u1", status="completed", path="gone/f.bin")
        manager = SimpleNamespace(has_file=lambda unique: False)
        assert await _already_saved(db, settings, manager, _Media("u1")) is None
    finally:
        await db.close()


async def test_already_saved_reports_queued_file(settings):
    from app.handlers.media import _already_saved

    manager = SimpleNamespace(has_file=lambda unique: True)
    assert await _already_saved(None, settings, manager, _Media("u1")) == "(already in the queue)"
    no_hit = SimpleNamespace(has_file=lambda u: False)
    assert await _already_saved(None, settings, no_hit, _Media("")) is None
    assert await _already_saved(None, settings, no_hit, _Media("u1")) is None  # no db → can't know


# ── disk headroom ────────────────────────────────────────────────────────────


def _manager(settings):
    from app.queue.manager import DownloadManager

    return DownloadManager(bot=object(), db=None, settings=settings, engine=object())


def _fake_job(size: int):
    from app.queue.manager import build_job

    return build_job(
        user=USER, chat_id=1, message_id=1, media_type="document",
        file_id="f", file_unique_id="u", file_name="n", file_size=size,
        target_path="/tmp/x", rel_path="x",
    )


def test_disk_headroom_subtracts_committed_work(settings, monkeypatch):
    import app.queue.manager as manager_mod

    monkeypatch.setattr(
        manager_mod.shutil, "disk_usage",
        lambda path: SimpleNamespace(free=10_000),
    )
    manager = _manager(settings)
    assert manager.disk_headroom() == 10_000

    manager._pending["a"] = _fake_job(3_000)
    manager._active["b"] = _fake_job(2_000)
    assert manager.disk_headroom() == 5_000


def test_disk_headroom_unknown_when_stat_fails(settings, monkeypatch):
    import app.queue.manager as manager_mod

    def boom(path):
        raise OSError("stat failed")

    monkeypatch.setattr(manager_mod.shutil, "disk_usage", boom)
    manager = _manager(settings)
    assert manager.disk_headroom() is None


def test_has_file_matches_pending_and_active(settings):
    manager = _manager(settings)
    assert manager.has_file("") is False
    job = _fake_job(5)
    manager._active[job.job_id] = job
    assert manager.has_file("u") is True
    assert manager.has_file("other") is False


# ── notifier waiting phase ───────────────────────────────────────────────────


def test_progress_text_shows_preparing_phase(settings):
    from app.services.notifier import Notifier, _Group

    notifier = Notifier(bot=object(), settings=settings)
    group = _Group("k", 1)
    job = _fake_job(5)
    job.status = JobStatus.RUNNING
    job.waiting_since = time.monotonic() - 93.0
    group.jobs[job.job_id] = job

    text = notifier._progress_text(group)
    assert "Telegram is preparing" in text
    assert "1m 33s" in text  # elapsed counter, not a frozen 0% bar

    job.waiting_since = None
    text = notifier._progress_text(group)
    assert "preparing" not in text
