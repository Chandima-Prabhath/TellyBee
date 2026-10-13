"""Startup path handling: db auto-mkdir, friendly permission errors.

The docker failure these tests pin down: bind mounts auto-created by docker
are owned by root, the bot runs as uid 1000 → `sqlite3.OperationalError:
unable to open database file`. The app must (a) create missing parent dirs
and (b) fail with an actionable message instead of a bare traceback.
"""

import os

import pytest

from app.db import Database
from app.main import _ensure_writable_dir

_CAN_TEST_PERMISSIONS = hasattr(os, "geteuid") and os.geteuid() != 0


async def test_connect_creates_missing_parent_dirs(tmp_path):
    db = Database(tmp_path / "nested" / "deep" / "tellybee.db")
    await db.connect()
    try:
        assert (tmp_path / "nested" / "deep" / "tellybee.db").exists()
    finally:
        await db.close()


@pytest.mark.skipif(
    not _CAN_TEST_PERMISSIONS,
    reason="permissions are not enforced for root / on this platform",
)
async def test_connect_unwritable_dir_raises_clear_error(tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o555)
    db = Database(locked / "tellybee.db")
    try:
        with pytest.raises(RuntimeError, match="chown"):
            await db.connect()
    finally:
        locked.chmod(0o755)  # let tmp_path cleanup succeed


def test_ensure_writable_dir_creates_missing(tmp_path):
    target = tmp_path / "downloads" / "sub"
    _ensure_writable_dir(target)
    assert target.is_dir()
    assert not list(target.iterdir())  # probe file cleaned up


@pytest.mark.skipif(
    not _CAN_TEST_PERMISSIONS,
    reason="permissions are not enforced for root / on this platform",
)
def test_ensure_writable_dir_denied_exits(tmp_path):
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o555)
    try:
        with pytest.raises(SystemExit):
            _ensure_writable_dir(locked)
    finally:
        locked.chmod(0o755)


def test_ensure_writable_dir_ok(tmp_path):
    _ensure_writable_dir(tmp_path / "dl")
    # A second call on the existing dir is a no-op (idempotent startup).
    _ensure_writable_dir(tmp_path / "dl")
