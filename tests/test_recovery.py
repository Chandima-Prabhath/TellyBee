"""Reboot recovery (0.4.1): stale rows, .part sweep, reports, Re-queue.

The queue lives in memory; the SQLite ledger is the durable record. A
reboot therefore leaves queued/running rows behind — recovery must mark
them interrupted, delete orphaned .part files, tell each affected chat
what it lost, and be able to rebuild runnable jobs from the stored
file_id/file_unique_id/path when the user presses Re-queue.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.db import Database
from app.queue.manager import DownloadManager
from app.services.recovery import (
    interrupted_report,
    recover_after_reboot,
    requeue_keyboard,
    sweep_part_files,
)

USER_ID = 111
CHAT = 42


class _RecordingBot:
    """Just the messaging surface recovery needs."""

    def __init__(self, fail_chats: set[int] | None = None):
        self.sent: list[tuple[int, str, object]] = []
        self._fail = fail_chats or set()

    async def send_message(self, chat_id, text, **kwargs):
        if chat_id in self._fail:
            raise RuntimeError("chat is gone")
        self.sent.append((chat_id, text, kwargs.get("reply_markup")))
        return SimpleNamespace(message_id=500 + len(self.sent))


async def _insert(db: Database, job_id: str, status: str, *, chat_id=CHAT,
                  name="movie.mkv", size=1_000_000, fuid=None, path=None):
    await db.conn.execute(
        """
        INSERT INTO downloads (job_id, user_id, username, chat_id, message_id,
            album_key, media_type, file_id, file_unique_id, file_name,
            file_size, path, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (job_id, USER_ID, "tester", chat_id, 1, None, "document",
         f"file-{job_id}", fuid or f"uniq-{job_id}", name, size,
         path or name, status),
    )
    await db.conn.commit()


async def _statuses(db: Database, user_id: int = USER_ID) -> dict[str, str]:
    """job_id → status for one user (user_history does not expose job_id)."""
    async with db.conn.execute(
        "SELECT job_id, status FROM downloads WHERE user_id = ? ORDER BY id",
        (user_id,),
    ) as cursor:
        return {row["job_id"]: row["status"] for row in await cursor.fetchall()}


# ── schema migration ────────────────────────────────────────────────────────


async def test_fresh_database_accepts_new_statuses(settings):
    db = Database(settings.db_path)
    await db.connect()
    await _insert(db, "j1", "interrupted")
    await _insert(db, "j2", "requeued")
    rows = await db.interrupted_for_chat(CHAT)
    assert [r["job_id"] for r in rows] == ["j1"]
    await db.close()


async def test_old_schema_migrates_in_place(tmp_path):
    """A v0.4.0 database (5-value CHECK) upgrades without losing rows."""
    import sqlite3

    path = tmp_path / "data" / "old.db"
    path.parent.mkdir(parents=True)
    old = sqlite3.connect(path)
    old.executescript(
        """
        CREATE TABLE downloads (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id         TEXT NOT NULL UNIQUE,
            user_id        INTEGER NOT NULL,
            username       TEXT,
            chat_id        INTEGER NOT NULL,
            message_id     INTEGER,
            album_key      TEXT,
            media_type     TEXT NOT NULL,
            file_id        TEXT NOT NULL,
            file_unique_id TEXT,
            file_name      TEXT,
            file_size      INTEGER,
            sha256         TEXT,
            path           TEXT,
            status         TEXT NOT NULL DEFAULT 'queued'
                           CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
            error          TEXT,
            avg_speed      REAL,
            duration_ms    INTEGER,
            created_at     TEXT NOT NULL DEFAULT (datetime('now')),
            finished_at    TEXT
        );
        CREATE INDEX idx_downloads_status ON downloads (status);
        INSERT INTO downloads (job_id, user_id, chat_id, media_type, file_id, path, status)
            VALUES ('old1', 7, 99, 'video', 'f1', 'v.mp4', 'completed');
        INSERT INTO downloads (job_id, user_id, chat_id, media_type, file_id, path, status)
            VALUES ('old2', 7, 99, 'video', 'f2', 'w.mkv', 'running');
        """
    )
    old.commit()
    old.close()

    db = Database(path)
    await db.connect()
    # data preserved, new statuses now legal
    assert set(await _statuses(db, 7)) == {"old1", "old2"}
    await _insert(db, "new1", "interrupted")
    # and the status index survived the rebuild
    async with db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='downloads'"
    ) as cursor:
        names = {row["name"] for row in await cursor.fetchall()}
    assert "idx_downloads_status" in names
    # reconnecting is a no-op (idempotent)
    await db.close()
    db2 = Database(path)
    await db2.connect()
    await _insert(db2, "new2", "requeued")
    await db2.close()


# ── stale-row marking & queries ─────────────────────────────────────────────


async def test_mark_stale_only_touches_live_statuses(settings):
    db = Database(settings.db_path)
    await db.connect()
    for jid, status in [
        ("a", "queued"), ("b", "running"), ("c", "completed"),
        ("d", "failed"), ("e", "cancelled"),
    ]:
        await _insert(db, jid, status)
    marked = await db.mark_stale_downloads()
    assert marked == 2
    history = await _statuses(db)
    assert history == {
        "a": "interrupted", "b": "interrupted", "c": "completed",
        "d": "failed", "e": "cancelled",
    }
    assert await db.mark_stale_downloads() == 0  # idempotent
    await db.close()


async def test_interrupted_queries_group_by_chat(settings):
    db = Database(settings.db_path)
    await db.connect()
    await _insert(db, "a", "queued", chat_id=1)
    await _insert(db, "b", "running", chat_id=1)
    await _insert(db, "c", "queued", chat_id=2)
    await db.mark_stale_downloads()
    assert await db.chats_with_interrupted() == [(1, 2), (2, 1)]
    rows = await db.interrupted_for_chat(1)
    assert [r["job_id"] for r in rows] == ["a", "b"]
    assert rows[0]["file_id"] == "file-a"
    assert await db.mark_requeued(["a"]) == 1
    assert await db.mark_requeued(["a"]) == 0  # already requeued
    assert [r["job_id"] for r in await db.interrupted_for_chat(1)] == ["b"]
    await db.close()


# ── .part sweep ─────────────────────────────────────────────────────────────


async def test_sweep_removes_only_part_files(tmp_path):
    root = tmp_path / "downloads"
    (root / "user" / "video").mkdir(parents=True)
    keep = root / "user" / "video" / "done.mkv"
    keep.write_bytes(b"x" * 100)
    part1 = root / "user" / "video" / "half.mkv.part"
    part1.write_bytes(b"x" * 500)
    part2 = root / "orphan.part"
    part2.write_bytes(b"x" * 50)

    removed, freed = await sweep_part_files(root)
    assert removed == 2
    assert freed == 550
    assert keep.exists()
    assert not part1.exists() and not part2.exists()

    # idempotent + missing dir safe
    assert await sweep_part_files(root) == (0, 0)
    assert await sweep_part_files(tmp_path / "nowhere") == (0, 0)


# ── report text & keyboard ──────────────────────────────────────────────────


def test_report_lists_files_and_counts():
    rows = [{"file_name": f"ep{i:02d}.mkv", "file_size": 1000, "media_type": "video"}
            for i in range(10)]
    text = interrupted_report(rows)
    assert "10 downloads were interrupted" in text
    assert "ep00.mkv" in text and "ep07.mkv" in text
    assert "… and 2 more" in text
    single = interrupted_report([{"file_name": "a.mkv", "file_size": 5,
                                  "media_type": "video"}])
    assert "1 download was" in single


def test_requeue_keyboard_carries_button():
    kb = requeue_keyboard(3)
    button = kb.inline_keyboard[0][0]
    assert button.callback_data == "tb:req"
    assert "3" in button.text


# ── the recovery pass ───────────────────────────────────────────────────────


async def test_recovery_reports_per_chat_and_sweeps_parts(settings):
    db = Database(settings.db_path)
    await db.connect()
    await _insert(db, "a", "running", chat_id=1)
    await _insert(db, "b", "queued", chat_id=2)
    await _insert(db, "c", "completed", chat_id=2)
    part = settings.download_dir / "leftover.part"
    settings.download_dir.mkdir(parents=True, exist_ok=True)
    part.write_bytes(b"junk")

    bot = _RecordingBot()
    await recover_after_reboot(bot, settings, db)

    assert not part.exists()
    chats = [chat for chat, _text, _kb in bot.sent]
    assert sorted(chats) == [1, 2]
    for _chat, text, kb in bot.sent:
        assert "interrupted" in text
        assert kb is not None and kb.inline_keyboard[0][0].callback_data == "tb:req"
    # chat 2's report covers exactly its one lost file
    chat2 = next(t for c, t, _ in bot.sent if c == 2)
    assert "1 download was interrupted" in chat2
    await db.close()


async def test_recovery_is_noop_after_clean_shutdown(settings):
    db = Database(settings.db_path)
    await db.connect()
    bot = _RecordingBot()
    await recover_after_reboot(bot, settings, db)
    assert bot.sent == []
    await db.close()


async def test_recovery_never_raises_on_unreachable_chat(settings):
    db = Database(settings.db_path)
    await db.connect()
    await _insert(db, "a", "queued", chat_id=99)
    bot = _RecordingBot(fail_chats={99})
    await recover_after_reboot(bot, settings, db)  # must not raise
    assert bot.sent == []
    # rows were still marked interrupted
    assert await db.chats_with_interrupted() == [(99, 1)]
    await db.close()


# ── re-queue (the button's engine) ──────────────────────────────────────────


class _StubEngine:
    def __init__(self):
        self.aclosed = False

    async def run(self, job):  # pragma: no cover — never reached
        raise AssertionError("stub engine must not run in these tests")

    async def aclose(self):
        self.aclosed = True


def _manager(settings, db, bot):
    return DownloadManager(bot, db, settings, engine=_StubEngine())


async def _requeue_via_handler(callback, settings, db, manager):
    from app.handlers.callbacks import _requeue_interrupted

    return await _requeue_interrupted(callback, settings, db, manager)


def _callback(chat_id=CHAT, data="tb:req", message=None):
    """A lightweight callback stand-in: the handlers only touch these attrs
    (aiogram models are frozen, so a plain fake is the easiest test double)."""

    class _Msg:
        def __init__(self, cid: int):
            self.chat = SimpleNamespace(id=cid)
            self.message_id = 7

    class _Bot:
        def __init__(self):
            self.messages: list[tuple[int, str]] = []
            self.edits: list[tuple[int, int, str]] = []

        async def send_message(self, chat_id, text, **kwargs):
            self.messages.append((chat_id, text))
            return SimpleNamespace(message_id=1)

        async def edit_message_text(self, text, chat_id=None, message_id=None, **kw):
            self.edits.append((chat_id, message_id, text))

    class _Callback:
        def __init__(self, cid: int, payload: str):
            self.from_user = SimpleNamespace(id=USER_ID)
            self.data = payload
            self.message = _Msg(cid)
            self.bot = _Bot()
            self.answers: list[str | None] = []

        async def answer(self, text=None, **kwargs):
            self.answers.append(text)

    return _Callback(chat_id, data)


async def test_requeue_restores_jobs_from_ledger(settings):
    db = Database(settings.db_path)
    await db.connect()
    await _insert(db, "a", "running", name="one.mkv")
    await _insert(db, "b", "queued", name="two.mkv", fuid="uniq-two")
    await db.mark_stale_downloads()
    callback = _callback()
    manager = _manager(settings, db, callback.bot)

    await _requeue_via_handler(callback, settings, db, manager)

    pending = manager.snapshot().pending
    assert len(pending) == 2
    by_fuid = {j.file_unique_id: j for j in pending}
    assert set(by_fuid) == {"uniq-a", "uniq-two"}
    job = by_fuid["uniq-two"]
    assert job.file_name == "two.mkv"
    assert job.target_path == str(settings.download_dir / "two.mkv")
    assert job.rel_path == "two.mkv"
    assert job.user.id == USER_ID
    assert job.chat_id == CHAT
    # ledger updated: both old rows are 'requeued' now, and the two
    # freshly submitted jobs appear as new 'queued' rows
    history = await _statuses(db)
    assert history["a"] == "requeued" and history["b"] == "requeued"
    assert list(history.values()).count("queued") == 2
    # the report message was rewritten and its button removed
    assert callback.bot.edits and "Re-queued 2" in callback.bot.edits[0][2]
    await manager.shutdown()
    await db.close()


async def test_requeue_skips_files_already_saved_or_queued(settings):
    db = Database(settings.db_path)
    await db.connect()
    # a previous download of the SAME file completed and still exists on disk
    await _insert(db, "done1", "completed", name="saved.mkv", fuid="uniq-saved")
    saved = settings.download_dir / "saved.mkv"
    settings.download_dir.mkdir(parents=True, exist_ok=True)
    saved.write_bytes(b"data")
    # the interrupted copy of that same file must NOT be re-downloaded
    await _insert(db, "lost1", "running", name="saved (1).mkv", fuid="uniq-saved")
    await _insert(db, "lost2", "running", name="lost.mkv", fuid="uniq-lost")
    await _insert(db, "live", "running", name="live.mkv", fuid="uniq-live")
    await db.mark_stale_downloads()

    callback = _callback()
    manager = _manager(settings, db, callback.bot)
    # and a live job already covers the "live" interrupted file
    from app.queue.manager import build_job
    from app.services.filenaming import UserRef

    live = build_job(
        user=UserRef(USER_ID, "tester", None), chat_id=CHAT, message_id=1,
        media_type="document", file_id="f", file_unique_id="uniq-live",
        file_name="live.mkv", file_size=10,
        target_path=str(settings.download_dir / "live.mkv"), rel_path="live.mkv",
    )
    await manager.submit(live)

    await _requeue_via_handler(callback, settings, db, manager)

    pending = {j.file_unique_id for j in manager.snapshot().pending}
    assert "uniq-saved" not in pending  # settled, not re-downloaded
    assert "uniq-lost" in pending
    history = await _statuses(db)
    assert history["done1"] == "completed"  # untouched
    assert history["lost1"] == "requeued"  # skipped as already saved
    assert history["live"] == "requeued"  # skipped as already queued
    await manager.shutdown()
    await db.close()


async def test_requeue_stops_when_queue_fills(settings):
    settings.max_queue_size = 1
    db = Database(settings.db_path)
    await db.connect()
    await _insert(db, "a", "running", name="one.mkv")
    await _insert(db, "b", "queued", name="two.mkv")
    await db.mark_stale_downloads()

    callback = _callback()
    manager = _manager(settings, db, callback.bot)

    await _requeue_via_handler(callback, settings, db, manager)

    assert len(manager.snapshot().pending) == 1
    assert any(
        "Queue is full" in text for _cid, _mid, text in callback.bot.edits
    )
    # the file that did not fit stays 'interrupted' for a later press; the
    # one that fit is a new 'queued' row
    history = await _statuses(db)
    assert list(history.values()).count("interrupted") == 1
    assert list(history.values()).count("requeued") == 1
    assert list(history.values()).count("queued") == 1
    await manager.shutdown()
    await db.close()


async def test_requeue_second_press_is_a_noop(settings):
    db = Database(settings.db_path)
    await db.connect()
    await _insert(db, "a", "running", name="one.mkv")
    await db.mark_stale_downloads()
    callback = _callback()
    manager = _manager(settings, db, callback.bot)

    await _requeue_via_handler(callback, settings, db, manager)
    await manager.shutdown()

    await _requeue_via_handler(callback, settings, db, manager)
    assert any("nothing left" in (t or "") for t in callback.answers)


@pytest.mark.parametrize("path", ["../escape.mkv", "sub/dir/file.mp4"])
async def test_job_from_row_paths_are_relative(settings, path):
    from app.handlers.callbacks import _job_from_row

    row = {
        "job_id": "x", "user_id": USER_ID, "username": None, "chat_id": CHAT,
        "message_id": 5, "album_key": None, "media_type": "document",
        "file_id": "f", "file_unique_id": "u", "file_name": "f.mp4",
        "file_size": 10, "path": path,
    }
    job = _job_from_row(row, settings)
    assert job.target_path == str(settings.download_dir / path)
