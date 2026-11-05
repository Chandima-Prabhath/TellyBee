"""Per-user flood control (sliding window) for text/command spam.

Media intake is **never** dropped: forwarding a whole series in one go
is a normal, supported flow, and every real bound lives downstream of
intake anyway — MAX_QUEUE_SIZE, disk headroom, duplicate detection, and
the engine's FETCH_CONCURRENCY pacing. Media messages are not even
counted against the window, so a 50-file forward can't starve the
user's ability to send /status right after.

Album messages (same ``media_group_id``) likewise sail through.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject

from app.config import Settings
from app.services.media_types import extract_media


class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, settings: Settings):
        self._rate = settings.throttle_rate
        self._window = settings.throttle_window
        self._hits: dict[int, deque[float]] = defaultdict(deque)
        self._last_notice: dict[int, float] = {}

    async def __call__(self, handler, event: TelegramObject, data: dict):
        if isinstance(event, Message) and (event.media_group_id or extract_media(event)):
            return await handler(event, data)

        user = data.get("event_from_user")
        if user is None:
            return await handler(event, data)

        now = time.monotonic()
        hits = self._hits[user.id]
        while hits and now - hits[0] > self._window:
            hits.popleft()
        if len(hits) >= self._rate:
            await self._notice(event, user.id, now)
            return None
        hits.append(now)
        return await handler(event, data)

    async def _notice(self, event: TelegramObject, user_id: int, now: float) -> None:
        if now - self._last_notice.get(user_id, 0.0) < 30.0:
            return
        self._last_notice[user_id] = now
        if isinstance(event, Message):
            try:
                await event.answer(
                    f"🐢 Easy there — that's more than {self._rate} messages in "
                    f"{int(self._window)}s. Files and media always get through; "
                    "give it a moment for the rest."
                )
            except Exception:  # noqa: BLE001
                pass
