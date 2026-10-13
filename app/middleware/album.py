"""Album (media group) middleware.

Telegram delivers each file of an album as a *separate* update sharing one
``media_group_id``. This middleware buffers them for a short window, then
calls the handler exactly once with ``data["album"] = [Message, ...]``
(sorted by ``message_id``).

Non-album messages pass straight through with ``data["album"] = [message]``
so media handlers see a uniform shape.
"""

from __future__ import annotations

import asyncio
import logging

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject

logger = logging.getLogger(__name__)


class AlbumMiddleware(BaseMiddleware):
    def __init__(self, delay: float = 0.8, max_buffer: int = 30):
        self._delay = delay
        self._max_buffer = max_buffer
        self._buffers: dict[str, dict] = {}

    async def __call__(self, handler, event: TelegramObject, data: dict):
        if not isinstance(event, Message):
            return await handler(event, data)

        group_id = event.media_group_id
        if group_id is None:
            data.setdefault("album", [event])
            return await handler(event, data)

        key = f"{event.chat.id}:{group_id}"
        buffer = self._buffers.get(key)
        if buffer is None:
            buffer = {
                "messages": [],
                "handler": handler,
                "data": data,
                "task": None,
            }
            self._buffers[key] = buffer
        buffer["messages"].append(event)

        # Flush early once the buffer hits the sane album size cap.
        if len(buffer["messages"]) >= self._max_buffer:
            await self._flush(key)
        elif buffer["task"] is None or buffer["task"].done():
            buffer["task"] = asyncio.create_task(self._flush_later(key))
        return None  # swallowed; handler runs on flush

    async def _flush_later(self, key: str) -> None:
        await asyncio.sleep(self._delay)
        await self._flush(key)

    async def _flush(self, key: str) -> None:
        buffer = self._buffers.pop(key, None)
        if not buffer:
            return
        messages = sorted(buffer["messages"], key=lambda m: m.message_id)
        # Deduplicate by message_id (defensive; Telegram shouldn't resend).
        seen: set[int] = set()
        unique = [m for m in messages if not (m.message_id in seen or seen.add(m.message_id))]
        data = dict(buffer["data"])
        data["album"] = unique
        try:
            await buffer["handler"](unique[0], data)
        except Exception:  # noqa: BLE001 — log, never kill the flush task silently
            logger.exception("album handler failed for %s", key)
