"""Queue manager behaviour with a stubbed engine and a real SQLite DB."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.db import Database
from app.queue.engine import EngineResult
from app.queue.jobs import new_job_id
from app.queue.manager import DownloadManager, QueueFullError
from app.services.filenaming import UserRef

USER = UserRef(111, "tester", "Tester")


class StubEngine:
    """Deterministic engine: writes a file, reports progress."""

    def __init__(self, fail_job_ids: set[str] | None = None, delay: float = 0.01):
        self.fail_job_ids = fail_job_ids or set()
        self.delay = delay
        self.aclosed = False

    async def run(self, job):  # noqa: ASYNC240 — stub writes tiny test payloads
        if job.job_id in self.fail_job_ids:
            from app.queue.engine import DownloadError

            raise DownloadError("stub failure")
        await asyncio.sleep(self.delay)
        path = Path(job.target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"payload" * 100)  # noqa: ASYNC240
        job.bytes_done = len(b"payload" * 100)
        job.total_bytes = job.bytes_done
        return EngineResult(sha256="abc", bytes_written=job.bytes_done,
                            duration_ms=10, avg_speed=job.bytes_done * 100)

    async def aclose(self):
        self.aclosed = True


class NullNotifier:
    def attach(self, job):
        pass

    async def finish(self, job):
        pass


def _job(tmp_path: Path, name: str = "f.bin", size: int = 100) -> object:
    from app.queue.manager import build_job

    target = tmp_path / "downloads" / name
    return build_job(
        job_id=new_job_id(),
        user=USER,
        chat_id=100,
        message_id=1,
        media_type="document",
        file_id=f"file-{name}",
        file_unique_id=f"uniq-{name}",
        file_name=name,
        file_size=size,
        target_path=str(target),
        rel_path=name,
    )


@pytest.fixture
async def manager(tmp_path):
    settings = pytest.importorskip("app.config").Settings(
        bot_token="1234567890:AAEhBOWe4ifRhnZ2dPq1fDz0r9RYmc8V4zQ",
        download_dir=tmp_path / "downloads",
        db_path=tmp_path / "data" / "tellybee.db",
        download_concurrency=2,
        max_queue_size=5,
        download_retries=1,
    )
    db = Database(settings.db_path)
    await db.connect()
    engine = StubEngine()
    mgr = DownloadManager(bot=None, db=db, settings=settings, notifier=NullNotifier(), engine=engine)
    await mgr.start()
    yield mgr
    await mgr.shutdown(timeout=5)
    await db.close()


async def test_jobs_complete_in_order(manager, tmp_path):
    for i in range(4):
        await manager.submit(_job(tmp_path, f"f{i}.bin"))
    for _ in range(100):
        if manager.completed == 4:
            break
        await asyncio.sleep(0.02)
    assert manager.completed == 4
    assert manager.failed == 0
    for i in range(4):
        assert (tmp_path / "downloads" / f"f{i}.bin").exists()


async def test_failed_job_isolated(manager, tmp_path):
    from app.queue.manager import build_job

    manager._engine.fail_job_ids = {"bad001"}
    bad = build_job(
        job_id="bad001", user=USER, chat_id=1, message_id=1,
        media_type="document", file_id="x", file_unique_id="x",
        file_name="bad.bin", file_size=10,
        target_path=str(tmp_path / "downloads" / "bad.bin"), rel_path="bad.bin",
    )
    good = _job(tmp_path, "good.bin")
    await manager.submit(bad)
    await manager.submit(good)
    for _ in range(100):
        if manager.completed + manager.failed == 2:
            break
        await asyncio.sleep(0.02)
    assert manager.failed == 1
    assert manager.completed == 1
    assert (tmp_path / "downloads" / "good.bin").exists()


async def test_queue_full(manager, tmp_path):
    # fill pending queue beyond capacity while workers are paused
    manager.pause()
    for i in range(5):
        await manager.submit(_job(tmp_path, f"q{i}.bin"))
    with pytest.raises(QueueFullError):
        await manager.submit(_job(tmp_path, "overflow.bin"))
    manager.resume()


async def test_cancel_pending_job(manager, tmp_path):
    manager.pause()
    job = _job(tmp_path, "cancelme.bin")
    await manager.submit(job)
    outcome = manager.cancel(job.job_id)
    assert outcome == "pending"
    manager.resume()
    for _ in range(100):
        if manager.cancelled == 1:
            break
        await asyncio.sleep(0.02)
    assert manager.cancelled == 1
    assert not (tmp_path / "downloads" / "cancelme.bin").exists()


async def test_snapshot_reflects_state(manager, tmp_path):
    manager.pause()
    await manager.submit(_job(tmp_path, "snap.bin"))
    snap = manager.snapshot()
    assert snap.paused is True
    assert len(snap.pending) == 1
    assert snap.workers_total == 2
    manager.resume()
    for _ in range(100):
        if manager.completed == 1:
            break
        await asyncio.sleep(0.02)


async def test_persistence_records_job(manager, tmp_path):
    job = _job(tmp_path, "db.bin")
    await manager.submit(job)
    for _ in range(100):
        if manager.completed == 1:
            break
        await asyncio.sleep(0.02)
    history = await manager._db.user_history(USER.id)
    assert len(history) == 1
    row = history[0]
    assert row["status"] == "completed"
    assert row["file_name"] == "db.bin"
