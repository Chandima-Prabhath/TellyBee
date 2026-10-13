"""Shared fixtures: settings, tmp dirs, fake bot."""

from __future__ import annotations

import datetime

import pytest

from app.config import Settings

VALID_TOKEN = "1234567890:AAEhBOWe4ifRhnZ2dPq1fDz0r9RYmc8V4zQ"


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        bot_token=VALID_TOKEN,
        download_dir=tmp_path / "downloads",
        db_path=tmp_path / "data" / "tellybee.db",
        download_concurrency=1,
        max_queue_size=5,
        progress_min_bytes=1024 * 1024,
        download_retries=1,
    )


def make_message(photo: bool = True, caption: str | None = None):
    """Build a minimal aiogram Message without touching the network."""
    from aiogram.types import Chat, Message, PhotoSize

    return Message(
        message_id=42,
        date=datetime.datetime.now(datetime.timezone.utc),
        chat=Chat(id=100, type="private"),
        photo=(
            [PhotoSize(file_id="file-1", file_unique_id="uniq-1", width=800, height=600, file_size=123456)]
            if photo
            else None
        ),
        caption=caption,
    )
