"""Inline-keyboard actions (0.4.1): the window's remote control.

Every button must (a) ALWAYS answer the callback query, (b) enforce the
same permission tiers as the equivalent commands, and (c) never crash —
an exception inside an action still answers with a logged-error toast.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.db import Database
from app.handlers.callbacks import (
    _resolve_role,
    on_window_button,
)
from app.queue.manager import DownloadManager, build_job
from app.services.filenaming import UserRef

USER_ID = 111
OTHER_ID = 222
ADMIN_ID = 999
CHAT = 42


class _StubEngine:
    async def run(self, job):  # pragma: no cover — must not run here
        raise AssertionError("stub engine must not run")

    async def aclose(self):
        pass


class _FakeNotifier:
    def __init__(self):
        self.refreshes: list[int | None] = []

    def request_refresh(self, chat_id=None):
        self.refreshes.append(chat_id)


def _callback(data: str, user_id: int = USER_ID, chat_id: int = CHAT):
    class _Callback:
        def __init__(self, payload: str, uid: int, cid: int):
            self.from_user = SimpleNamespace(id=uid)
            self.data = payload
            self.message = SimpleNamespace(chat=SimpleNamespace(id=cid), message_id=7)
            self.bot = _FakeBot()
            self.answers: list[str | None] = []

        async def answer(self, text=None, **kwargs):
            self.answers.append(text)

    return _Callback(data, user_id, chat_id)


class _FakeBot:
    def __init__(self):
        self.messages: list[tuple[int, str]] = []
        self.edits: list[tuple[int, int, str]] = []
        # raising this from send_message simulates an action blowing up
        self.fail_sends = False

    async def send_message(self, chat_id, text, **kwargs):
        if self.fail_sends:
            raise RuntimeError("boom")
        self.messages.append((chat_id, text))
        return SimpleNamespace(message_id=1)

    async def edit_message_text(self, text, chat_id=None, message_id=None, **kw):
        self.edits.append((chat_id, message_id, text))


async def _setup(settings, *, admin_ids=frozenset({ADMIN_ID}), allowed=frozenset({USER_ID})):
    settings.admin_ids = set(admin_ids)
    settings.allowed_user_ids = set(allowed)
    # db.get_role() is backed by a process-wide cache keyed by user id —
    # clear it so roles from earlier tests never leak across databases.
    Database.invalidate_role_cache()
    db = Database(settings.db_path)
    await db.connect()
    callback = _callback("tb:x")
    manager = DownloadManager(callback.bot, db, settings, engine=_StubEngine())
    notifier = _FakeNotifier()
    return db, callback.bot, manager, notifier


async def _submit(manager, *, chat_id=CHAT, user_id=USER_ID, fuid, name="f.mkv"):
    job = build_job(
        user=UserRef(user_id, "u", None), chat_id=chat_id, message_id=1,
        media_type="document", file_id=f"f-{fuid}", file_unique_id=fuid,
        file_name=name, file_size=10,
        target_path=f"/tmp/never/{name}", rel_path=name,
    )
    await manager.submit(job)
    return job


# ── role resolution mirrors the auth middleware ─────────────────────────────


async def test_resolve_role_priority(settings):
    db, _bot, _manager, _notifier = await _setup(
        settings, admin_ids={ADMIN_ID}, allowed={USER_ID}
    )
    assert await _resolve_role(settings, db, ADMIN_ID) == "admin"
    assert await _resolve_role(settings, db, USER_ID) == "user"
    assert await _resolve_role(settings, db, OTHER_ID) is None
    # a runtime /allow beats the env config
    Database.invalidate_role_cache()  # the cache would hide the new row
    await db.upsert_user(OTHER_ID, role="user")
    assert await _resolve_role(settings, db, OTHER_ID) == "user"
    # open access lets everyone in
    settings.admin_ids = set()
    settings.allowed_user_ids = set()
    assert await _resolve_role(settings, db, 333) == "user"
    await db.close()


# ── every button answers, even when denied or broken ────────────────────────


async def test_denied_user_gets_answered_toast(settings):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback("tb:queue", user_id=OTHER_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert callback.answers == ["⛔️ This bot is private."]
    assert bot.messages == []  # nothing sent
    await manager.shutdown()
    await db.close()


async def test_unknown_action_is_answered(settings):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback("tb:warp", user_id=ADMIN_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert callback.answers and "Unknown button" in callback.answers[0]
    await manager.shutdown()
    await db.close()


async def test_action_exception_still_answers(settings):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback("tb:queue", user_id=USER_ID)
    callback.bot.fail_sends = True  # the queue reply blows up mid-action
    await on_window_button(callback, settings, db, manager, notifier)
    # _send degrades to an apologetic toast instead of crashing the action
    assert callback.answers and any(
        "Could not send" in (t or "") for t in callback.answers
    )
    await manager.shutdown()
    await db.close()


# ── pause / resume ──────────────────────────────────────────────────────────


async def test_admin_can_pause_and_resume(settings):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback("tb:pause", user_id=ADMIN_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert manager.paused
    assert notifier.refreshes == [CHAT]  # window re-renders promptly

    callback = _callback("tb:resume", user_id=ADMIN_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert not manager.paused
    await manager.shutdown()
    await db.close()


async def test_pause_is_admin_only(settings):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback("tb:pause", user_id=USER_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert not manager.paused
    assert callback.answers and "Admins only" in callback.answers[0]
    await manager.shutdown()
    await db.close()


# ── queue / stats / help views ──────────────────────────────────────────────


async def test_queue_button_sends_rendered_queue(settings):
    db, bot, manager, notifier = await _setup(settings)
    await _submit(manager, fuid="u1")
    callback = _callback("tb:queue", user_id=USER_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert callback.answers == ["📋 Sending the queue…"]
    assert callback.bot.messages and "Download queue" in callback.bot.messages[0][1]
    await manager.shutdown()
    await db.close()


async def test_stats_button_is_admin_only(settings):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback("tb:stats", user_id=USER_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert callback.bot.messages == []
    assert "Admins only" in callback.answers[0]

    callback = _callback("tb:stats", user_id=ADMIN_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert callback.bot.messages and "TellyBee stats" in callback.bot.messages[0][1]
    await manager.shutdown()
    await db.close()


async def test_help_button_sends_guide(settings):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback("tb:help", user_id=USER_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert callback.bot.messages and "How to use TellyBee" in callback.bot.messages[0][1]
    await manager.shutdown()
    await db.close()


# ── cancel ──────────────────────────────────────────────────────────────────


async def test_cancel_scopes_to_chat_and_ownership(settings):
    db, bot, manager, notifier = await _setup(settings)
    mine = await _submit(manager, fuid="mine", user_id=USER_ID, chat_id=CHAT)
    other_chat = await _submit(manager, fuid="other", user_id=USER_ID, chat_id=555)
    other_user = await _submit(manager, fuid="theirs", user_id=OTHER_ID, chat_id=CHAT)

    # a regular user cancels only their own jobs in this chat
    callback = _callback("tb:cancel", user_id=USER_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert mine.is_cancelled()
    assert not other_chat.is_cancelled()
    assert not other_user.is_cancelled()
    assert callback.answers and "Cancelling 1" in callback.answers[0]

    # an admin cancels every job in the chat
    callback = _callback("tb:cancel", user_id=ADMIN_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert other_user.is_cancelled()
    assert not other_chat.is_cancelled()  # other chats untouched
    await manager.shutdown()
    await db.close()


async def test_cancel_with_empty_queue_answers_politely(settings):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback("tb:cancel", user_id=ADMIN_ID)
    await on_window_button(callback, settings, db, manager, notifier)
    assert callback.answers == ["Nothing to cancel — the queue is already empty."]
    await manager.shutdown()
    await db.close()


# ── the full re-queue round trip through the router ─────────────────────────


async def test_req_button_round_trip(settings):
    db, bot, manager, notifier = await _setup(settings)
    await db.conn.execute(
        """
        INSERT INTO downloads (job_id, user_id, username, chat_id, message_id,
            album_key, media_type, file_id, file_unique_id, file_name,
            file_size, path, status)
        VALUES ('r1', ?, 'u', ?, 1, NULL, 'document', 'f1', 'q1', 'back.mkv',
                10, 'back.mkv', 'interrupted')
        """,
        (USER_ID, CHAT),
    )
    await db.conn.commit()

    callback = _callback("tb:req", user_id=USER_ID)
    await on_window_button(callback, settings, db, manager, notifier)

    assert callback.answers and "Re-queued 1" in callback.answers[0]
    assert callback.bot.edits and "Re-queued 1" in callback.bot.edits[0][2]
    assert len(manager.snapshot().pending) == 1
    await manager.shutdown()
    await db.close()


@pytest.mark.parametrize(
    "data,user_id,expected_answer",
    [
        ("tb:pause", ADMIN_ID, "⏸"),
        ("tb:resume", ADMIN_ID, "▶️"),
        ("tb:queue", USER_ID, "📋"),
        ("tb:help", USER_ID, "📖"),
    ],
)
async def test_every_action_answers_its_toast(settings, data, user_id, expected_answer):
    db, bot, manager, notifier = await _setup(settings)
    callback = _callback(data, user_id=user_id)
    await on_window_button(callback, settings, db, manager, notifier)
    assert callback.answers and expected_answer in (callback.answers[0] or "")
    await manager.shutdown()
    await db.close()
