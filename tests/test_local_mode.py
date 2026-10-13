"""Regression tests for the local Bot API server mode (0.1.2 – 0.1.3).

Covers:
* engine stream-copies files from the server's shared storage (no HTTP),
* actionable errors when the volume is not shared / file not yet present,
* owner-only permissions on the server storage fail fast with the exact
  uid-alignment remedy (0.1.3) instead of burning retries on EACCES,
* the bot token is masked in errors and log output,
* the dispatcher error handler accepts aiogram 3.31's ErrorEvent,
* the notifier's edit throttle works (_Group slots),
* /allow|/disallow read CommandObject.args.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from aiogram.filters import CommandObject
from aiogram.types import Chat, File, Message, Update, User

from app.queue.engine import DownloadEngine, DownloadError
from app.queue.jobs import new_job_id
from app.services.filenaming import UserRef
from tests.conftest import VALID_TOKEN, make_message

USER = UserRef(111, "tester", "Tester")


# ── fakes ───────────────────────────────────────────────────────────────────


class _FakeApi:
    def __init__(self, is_local: bool) -> None:
        self.is_local = is_local

    def file_url(self, token: str, path: str) -> str:
        return f"http://fake/bot{token}/{path}"


class _FakeBot:
    def __init__(self, token: str, is_local: bool, file: File) -> None:
        self.token = token
        self.session = SimpleNamespace(api=_FakeApi(is_local))
        self._file = file

    async def get_file(self, file_id: str) -> File:
        return self._file


def _job(tmp_path, name: str = "movie.mp4", size: int = 100):
    from app.queue.manager import build_job

    target = tmp_path / "downloads" / name
    return build_job(
        job_id=new_job_id(),
        user=USER,
        chat_id=100,
        message_id=1,
        media_type="document",
        file_id="file-1",
        file_unique_id="uniq-1",
        file_name=name,
        file_size=size,
        target_path=str(target),
        rel_path=name,
    )


# ── engine: local mode copies the file straight from disk ───────────────────


async def test_local_copy_streams_file(settings, tmp_path, monkeypatch):
    botapi = tmp_path / "botapi"
    (botapi / "documents").mkdir(parents=True)
    src = botapi / "documents" / "file_0"
    data = os.urandom(600_000)  # > one 256 KiB chunk → multiple pump rounds
    src.write_bytes(data)

    bot = _FakeBot(
        VALID_TOKEN,
        is_local=True,
        file=File(file_id="f1", file_unique_id="u1", file_size=len(data), file_path=str(src)),
    )
    monkeypatch.setattr(DownloadEngine, "LOCAL_FILES_DIR", botapi)
    engine = DownloadEngine(bot, settings)

    job = _job(tmp_path, size=len(data))
    result = await engine.run(job)

    target = tmp_path / "downloads" / "movie.mp4"
    assert target.read_bytes() == data
    assert result.bytes_written == len(data)
    assert result.sha256 == hashlib.sha256(data).hexdigest()
    assert job.total_bytes == len(data)
    assert job.bytes_done == len(data)
    assert not target.with_name(target.name + ".part").exists()


async def test_local_mode_without_shared_volume_fails_fast(
    settings, tmp_path, monkeypatch
):
    missing = tmp_path / "not-mounted"  # does not exist → volume not shared
    src = missing / "documents" / "file_0"
    bot = _FakeBot(
        VALID_TOKEN,
        is_local=True,
        file=File(file_id="f1", file_unique_id="u1", file_size=10, file_path=str(src)),
    )
    monkeypatch.setattr(DownloadEngine, "LOCAL_FILES_DIR", missing)
    engine = DownloadEngine(bot, settings)

    job = _job(tmp_path, "x.bin")
    with pytest.raises(DownloadError) as exc:
        await engine.run(job)
    assert "botapi-data" in str(exc.value)  # actionable fix in the message
    assert job.attempts == 1  # not retried — mounting cannot fix itself


async def test_local_mode_missing_file_is_retryable(settings, tmp_path, monkeypatch):
    botapi = tmp_path / "botapi"
    botapi.mkdir()
    src = botapi / "documents" / "file_9"  # dir exists, file doesn't (yet)
    bot = _FakeBot(
        VALID_TOKEN,
        is_local=True,
        file=File(file_id="f1", file_unique_id="u1", file_size=10, file_path=str(src)),
    )
    monkeypatch.setattr(DownloadEngine, "LOCAL_FILES_DIR", botapi)
    monkeypatch.setattr(
        DownloadEngine, "_backoff_delay", staticmethod(lambda attempt: 0.01)
    )
    engine = DownloadEngine(bot, settings)

    job = _job(tmp_path, "y.bin")
    with pytest.raises(DownloadError) as exc:
        await engine.run(job)
    assert "local server storage" in str(exc.value)
    assert job.attempts == 2  # settings.download_retries == 1 → 2 attempts


# ── local mode: owner-only permissions on the server's storage ──────────────


def test_permission_error_on_server_volume_suggests_uid_alignment(settings):
    """EACCES inside the shared volume → the uid-alignment remedy."""
    bot = _FakeBot(
        VALID_TOKEN, is_local=True, file=File(file_id="f", file_unique_id="u")
    )
    engine = DownloadEngine(bot, settings)
    locked = f"/var/lib/telegram-bot-api/{VALID_TOKEN}/documents/file_1.mp4"
    err = engine._local_permission_error(PermissionError(13, "Permission denied", locked))

    text = str(err)
    assert "same uid" in text.lower()
    assert "docker-compose.yml.example" in text
    assert VALID_TOKEN not in text  # the token-named dir must be redacted


def test_permission_error_in_downloads_dir_suggests_chown(settings):
    """EACCES under the downloads dir → the classic root-owned bind mount."""
    bot = _FakeBot(
        VALID_TOKEN, is_local=True, file=File(file_id="f", file_unique_id="u")
    )
    engine = DownloadEngine(bot, settings)
    err = engine._local_permission_error(
        PermissionError(13, "Permission denied", "/app/downloads/movie.mp4.part")
    )

    text = str(err)
    assert "chown" in text
    assert "botapi" not in text


async def test_unreadable_server_storage_fails_fast(settings, tmp_path, monkeypatch):
    """Storage locked to a different uid → one attempt + the exact remedy.

    Simulates the 0.1.2 production failure: the Bot API server writes its
    per-bot directory (named after the token) with owner-only permissions,
    so a differently-uided reader gets EACCES on the very first stat.
    """
    if os.geteuid() == 0:
        pytest.skip("root ignores file permissions")

    botapi = tmp_path / "botapi"
    token_dir = botapi / VALID_TOKEN
    (token_dir / "documents").mkdir(parents=True)
    src = token_dir / "documents" / "file_0"
    src.write_bytes(b"payload")

    bot = _FakeBot(
        VALID_TOKEN,
        is_local=True,
        file=File(file_id="f1", file_unique_id="u1", file_size=7, file_path=str(src)),
    )
    monkeypatch.setattr(DownloadEngine, "LOCAL_FILES_DIR", botapi)
    engine = DownloadEngine(bot, settings)

    token_dir.chmod(0o000)  # only a different uid's owner could traverse
    job = _job(tmp_path, "locked.bin")
    try:
        with pytest.raises(DownloadError) as exc:
            await engine.run(job)
        assert "same uid" in str(exc.value).lower()
        assert job.attempts == 1  # EACCES never resolves on retry — no backoff
    finally:
        token_dir.chmod(0o700)  # let pytest's tmp_path cleanup remove it


# ── token redaction ─────────────────────────────────────────────────────────


def test_engine_redacts_token(settings):
    bot = _FakeBot(
        VALID_TOKEN, is_local=False, file=File(file_id="f", file_unique_id="u")
    )
    engine = DownloadEngine(bot, settings)
    leaked = f"404, message='Not Found', url='http://botapi:8081/file/bot{VALID_TOKEN}//var/lib/x'"
    cleaned = engine._redact(leaked)
    assert VALID_TOKEN not in cleaned
    assert "***" in cleaned


def test_log_formatter_scrubs_token(monkeypatch):
    import app.logging_setup as logging_setup

    monkeypatch.setattr(logging_setup, "_SECRETS", (VALID_TOKEN,))
    formatter = logging_setup.SecretScrubbingFormatter("%(message)s")
    record = logging.LogRecord(
        "test", logging.ERROR, __file__, 1,
        f"failed: url='http://x/file/bot{VALID_TOKEN}/y'", None, None,
    )
    out = formatter.format(record)
    assert VALID_TOKEN not in out
    assert "bot***/y" in out


# ── dispatcher error handler: aiogram 3.31 ErrorEvent contract ──────────────


async def test_error_handler_accepts_errorevent(settings):
    from aiogram.types import ErrorEvent

    from app.bot_factory import build_dispatcher

    dp = build_dispatcher(settings, db=None, manager=None)
    handler = dp.errors.handlers[0].callback

    event = ErrorEvent(update=Update(update_id=1), exception=ValueError("boom"))
    result = await handler(event)  # must not raise TypeError
    assert result is True


# ── notifier: edit throttle no longer crashes on _Group ─────────────────────


async def test_edit_throttled_updates_last_edit(settings):
    from app.services.notifier import Notifier, _Group

    notifier = Notifier(bot=object(), settings=settings)
    group = _Group("k", 1)

    await notifier._edit_throttled(group)  # crashed with AttributeError before
    assert group._last_edit > 0.0

    group._last_edit = time.monotonic()  # pretend we just edited
    stamp = group._last_edit
    await notifier._edit_throttled(group)  # throttled → untouched
    assert group._last_edit == stamp


# ── admin: _target_user reads CommandObject.args ────────────────────────────


def test_target_user_from_numeric_args():
    from app.handlers.admin import _target_user

    msg = make_message(photo=False)
    assert _target_user(msg, CommandObject(command="allow", args="12345")) == (12345, None, None)


def test_target_user_from_reply():
    from app.handlers.admin import _target_user

    now = datetime.now(UTC)
    replied = Message(
        message_id=1,
        date=now,
        chat=Chat(id=100, type="private"),
        from_user=User(id=999, is_bot=False, first_name="Ann", username="ann"),
    )
    msg = Message(
        message_id=2,
        date=now,
        chat=Chat(id=100, type="private"),
        reply_to_message=replied,
    )
    assert _target_user(msg, CommandObject(command="allow", args=None)) == (999, "ann", "Ann")


def test_target_user_unresolvable_returns_none():
    from app.handlers.admin import _target_user

    msg = make_message(photo=False)
    assert _target_user(msg, CommandObject(command="allow", args="not-a-number")) is None
