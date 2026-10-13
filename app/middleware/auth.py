"""Access control middleware — resolves the admin/user tier per update.

Priority of evidence:

1. ``ADMIN_IDS`` env (always wins)      → role ``admin``
2. users table role (runtime ``/allow``) → ``admin`` or ``user``
3. ``ALLOWED_USER_IDS`` env             → role ``user``
4. open access mode (nothing configured)→ role ``user`` (warned at startup)
5. otherwise                            → denied with a short explanation
"""

from __future__ import annotations

import logging
import time

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject, User

from app.config import Settings
from app.db import Database

logger = logging.getLogger(__name__)

_DENY_INTERVAL = 15.0  # seconds between denial replies for the same user


class AuthMiddleware(BaseMiddleware):
    def __init__(self, settings: Settings, db: Database):
        self._settings = settings
        self._db = db
        self._last_denial: dict[int, float] = {}

    async def __call__(self, handler, event: TelegramObject, data: dict):
        user: User | None = data.get("event_from_user")
        if user is None:
            # Channel-ish edge case; treat as anonymous user tier.
            data["user_role"] = "user"
            return await handler(event, data)

        role = await self._resolve(user.id)
        if role is None:
            await self._deny(event, user)
            return None
        data["user_role"] = role
        # Keep the users table fresh with names for /users and stats.
        await self._refresh_profile(user, role)
        return await handler(event, data)

    async def _resolve(self, user_id: int) -> str | None:
        if user_id in self._settings.admin_ids:
            return "admin"
        db_role = await self._db.get_role(user_id)
        if db_role == "admin":
            return "admin"
        if db_role == "user":
            return "user"
        if user_id in self._settings.allowed_user_ids:
            return "user"
        if self._settings.open_access:
            return "user"
        return None

    async def _refresh_profile(self, user: User, role: str) -> None:
        """Best-effort profile update; never blocks the update path."""
        try:
            if role == "user" and not user.is_bot:
                await self._db.upsert_user(user.id, role="user")
        except Exception:  # noqa: BLE001
            logger.debug("profile refresh failed for %s", user.id)

    async def _deny(self, event: TelegramObject, user: User) -> None:
        now = time.monotonic()
        last = self._last_denial.get(user.id, 0.0)
        if now - last < _DENY_INTERVAL:
            return
        self._last_denial[user.id] = now
        if isinstance(event, Message):
            try:
                await event.answer(
                    "⛔️ <b>Access denied</b>\n\n"
                    "This bot is private. Ask an admin to authorize you with "
                    f"<code>/allow {user.id}</code>\n\n"
                    f"Your Telegram ID: <code>{user.id}</code>"
                )
            except Exception:  # noqa: BLE001 — never crash middleware on reply
                logger.debug("deny reply failed for %s", user.id)
