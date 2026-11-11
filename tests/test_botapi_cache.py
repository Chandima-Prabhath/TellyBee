"""Bot API cache housekeeping (0.7.0): the botapi-data volume must not fill.

Live incident: the local Bot API server keeps every downloaded file for
1–25 h (retention NOT configurable — tdlib/telegram-bot-api#303), so after
a 26.5 GB series binge the volume ate the disk and had to be cleaned by
hand. The cleaner now (a) deletes each file the moment its copy is safe in
./downloads, and (b) sweeps leftovers older than the TTL — but never a
path that is protected (in-flight) or still growing, and never anything
outside the bot's ``files/`` subtree.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from types import SimpleNamespace

from app.queue.engine import DownloadEngine, _FileCache
from app.queue.jobs import DownloadJob, JobStatus, new_job_id
from app.services.botapi_cache import BotApiCacheCleaner


def _cleaner(
    tmp_path: Path,
    *,
    ttl: float = 120.0,
    evict_after_copy: bool = True,
    protected=None,
) -> BotApiCacheCleaner:
    return BotApiCacheCleaner(
        tmp_path / "tok",
        ttl_seconds=ttl,
        interval_seconds=900.0,
        evict_after_copy=evict_after_copy,
        protected=protected,
    )


def _stale_file(root: Path, rel: str, size: int = 1000) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    old = time.time() - 3600.0
    os.utime(path, (old, old))
    return path


# ── evict_now (the immediate delete after a successful copy) ────────────


def test_evict_now_deletes_a_file_inside_the_root(tmp_path):
    cleaner = _cleaner(tmp_path)
    served = _stale_file(tmp_path / "tok", "files/video/e02.mkv")

    assert cleaner.evict_now(str(served)) is True
    assert not served.exists()
    assert cleaner.evicted_files == 1


def test_evict_now_refuses_paths_outside_the_root(tmp_path):
    cleaner = _cleaner(tmp_path)
    outsider = tmp_path / "elsewhere" / "f.mp4"
    outsider.parent.mkdir(parents=True)
    outsider.write_bytes(b"x" * 10)

    assert cleaner.evict_now(str(outsider)) is False
    assert outsider.exists()


def test_evict_now_is_a_noop_when_disabled_by_config(tmp_path):
    cleaner = _cleaner(tmp_path, evict_after_copy=False)
    served = _stale_file(tmp_path / "tok", "files/video/e02.mkv")

    assert cleaner.evict_now(str(served)) is False
    assert served.exists()


def test_evict_now_treats_a_missing_file_as_already_done(tmp_path):
    cleaner = _cleaner(tmp_path)

    assert cleaner.evict_now(str(tmp_path / "tok" / "files" / "gone.mkv")) is False


# ── observe(): layout discovery gates the whole sweep ────────────────────


def test_sweep_only_runs_in_observed_files_subtree(tmp_path):
    cleaner = _cleaner(tmp_path, ttl=60.0)
    # A stale file OUTSIDE files/ (e.g. the server's own TDLib database
    # area) must never be touched even after two sweeps.
    database = _stale_file(tmp_path / "tok", "td_db/binlog")
    cleaner.observe(str(tmp_path / "tok" / "other" / "layout.bin"))

    cleaner._sweep_once(frozenset())
    cleaner._sweep_once(frozenset())

    assert database.exists(), "sweeper touched a path outside files/ without observing it"


def test_observe_derives_root_from_real_getfile_paths(tmp_path):
    cleaner = _cleaner(tmp_path)
    cleaner.observe(str(tmp_path / "tok" / "files" / "video" / "f.mp4"))

    assert cleaner._derived == {tmp_path / "tok" / "files"}


# ── the TTL sweeper ──────────────────────────────────────────────────────


def test_sweep_deletes_stale_unchanged_file_on_second_pass(tmp_path):
    cleaner = _cleaner(tmp_path, ttl=60.0)
    leftover = _stale_file(tmp_path / "tok", "files/video/e01.mkv")
    cleaner.observe(str(leftover))

    cleaner._sweep_once(frozenset())  # first pass: record only
    assert leftover.exists(), "deleted on a single observation"

    cleaner._sweep_once(frozenset())  # second pass: unchanged + old → gone
    assert not leftover.exists()
    assert cleaner.swept_files == 1


def test_sweep_keeps_files_younger_than_the_ttl(tmp_path):
    cleaner = _cleaner(tmp_path, ttl=3600.0)
    fresh = tmp_path / "tok" / "files" / "video" / "new.mkv"
    fresh.parent.mkdir(parents=True)
    fresh.write_bytes(b"x" * 50)
    cleaner.observe(str(fresh))

    cleaner._sweep_once(frozenset())
    cleaner._sweep_once(frozenset())

    assert fresh.exists()


def test_sweep_keeps_files_that_are_still_growing(tmp_path):
    cleaner = _cleaner(tmp_path, ttl=60.0)
    live = _stale_file(tmp_path / "tok", "files/video/part.mkv")
    cleaner.observe(str(live))

    cleaner._sweep_once(frozenset())
    with open(live, "ab") as fh:  # an unknown-but-active download grows
        fh.write(b"y" * 100)
    cleaner._sweep_once(frozenset())

    assert live.exists(), "deleted a file that was still growing between sweeps"


def test_sweep_keeps_protected_paths(tmp_path):
    cleaner = _cleaner(tmp_path, ttl=60.0)
    inflight = _stale_file(tmp_path / "tok", "files/video/e02.mkv")
    cleaner.observe(str(inflight))

    cleaner._sweep_once(frozenset())
    cleaner._sweep_once(frozenset({str(inflight)}))

    assert inflight.exists(), "deleted a path the engine declared protected"


def test_read_only_mount_disables_cleanup_without_crashing(tmp_path, monkeypatch):
    cleaner = _cleaner(tmp_path)
    served = _stale_file(tmp_path / "tok", "files/video/e02.mkv")

    import app.services.botapi_cache as module

    def forbidden(path):
        raise PermissionError(13, "Permission denied", path)

    monkeypatch.setattr(module.os, "unlink", forbidden)

    assert cleaner.evict_now(str(served)) is False
    assert cleaner._disabled is True
    # Further calls are silent no-ops, never exceptions:
    assert cleaner.evict_now(str(served)) is False
    cleaner._sweep_once(frozenset())
    assert served.exists()


# ── engine integration: the copy hands the server copy back ─────────────


def _job(name: str, size: int, target: Path, file_id: str = "fid-1") -> DownloadJob:
    return DownloadJob(
        job_id=new_job_id(),
        user=SimpleNamespace(id=1, username="u", first_name="U"),
        chat_id=100,
        message_id=1,
        media_type="document",
        file_id=file_id,
        file_unique_id="u" + name,
        file_name=name,
        file_size=size,
        target_path=str(target),
        rel_path=f"user/document/{name}",
        status=JobStatus.RUNNING,
    )


async def test_successful_copy_evicts_the_server_side_cache(tmp_path):
    engine = DownloadEngine.__new__(DownloadEngine)
    cleaner = _cleaner(tmp_path)
    engine._settings = SimpleNamespace(chunk_size=64)
    engine._cache = _FileCache(900.0)
    engine._cleaner = cleaner

    served = tmp_path / "tok" / "files" / "video" / "e02.mkv"
    served.parent.mkdir(parents=True)
    served.write_bytes(b"payload" * 100)
    size = served.stat().st_size
    engine._cache.put("fid-1", str(served))
    target = tmp_path / "downloads" / "e02.mkv"
    job = _job("e02.mkv", size, target)

    result = await engine._copy_local_file_inner(job, str(served), 0.0)

    assert result.bytes_written == size
    assert target.exists()
    assert not served.exists(), "server-side copy survived a successful copy"
    assert engine._cache.get("fid-1") is None, "ghost path left in the file_id cache"


async def test_copy_failure_keeps_the_server_side_cache(tmp_path):
    engine = DownloadEngine.__new__(DownloadEngine)
    cleaner = _cleaner(tmp_path)
    engine._settings = SimpleNamespace(chunk_size=64)
    engine._cache = _FileCache(900.0)
    engine._cleaner = cleaner

    served = tmp_path / "tok" / "files" / "video" / "e01.mkv"
    served.parent.mkdir(parents=True)
    served.write_bytes(b"payload" * 100)
    engine._cache.put("fid-1", str(served))
    # A FILE where the target's parent directory should go — the mkdir
    # fails with ENOTDIR on every platform/uid, deterministically.
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    job = _job("e01.mkv", served.stat().st_size, blocker / "sub" / "e01.mkv")

    try:
        await engine._copy_local_file_inner(job, str(served), 0.0)
        raised = False
    except OSError:
        raised = True

    assert raised, "copy to an unwritable target should fail"
    assert served.exists(), "evicted a cache file whose copy FAILED"
    assert engine._cache.get("fid-1") == str(served), "mapping dropped too early"
