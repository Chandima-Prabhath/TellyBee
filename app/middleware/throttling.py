"""Per-user flood control (sliding window).

Album messages (same ``media_group_id``) are exempt: Telegram delivers them
as a burst and the album middleware aggregates them anyway.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject

from app.config import Settings


class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, settings: Settings):
        self._rate = settings.throttle_rate
        self._window = settings.throttle_window
        self._hits: dict[int, deque[float]] = defaultdict(deque)
        self._last_notice: dict[int, float] = {}

    async def __call__(self, handler, event: TelegramObject, data: dict):
        if isinstance(event, Message) and event.media_group_id:
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
                    f"🐢 Easy there — max {self._rate} messages per "
                    f"{int(self._window)}s. Try again in a moment."
                )
            except Exception:  # noqa: BLE001
                pass
