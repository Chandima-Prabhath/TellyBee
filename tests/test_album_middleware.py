"""Album middleware behaviour with stub handlers."""

from __future__ import annotations

import asyncio
import datetime

from aiogram.types import Chat, Message

from app.middleware.album import AlbumMiddleware


def _album_message(index: int, group: str | None) -> Message:
    return Message(
        message_id=index,
        date=datetime.datetime.now(datetime.timezone.utc),
        chat=Chat(id=100, type="private"),
        media_group_id=group,
    )


async def test_single_message_passes_through():
    captured = {}

    async def handler(event, data):
        captured["event"] = event
        captured["album"] = data.get("album")

    mw = AlbumMiddleware(delay=0.05)
    msg = _album_message(1, group=None)
    await mw(handler, msg, {})
    assert captured["event"] is msg
    assert captured["album"] == [msg]


async def test_album_messages_are_buffered_and_merged():
    captured = {}

    async def handler(event, data):
        captured["event"] = event
        captured["album"] = data["album"]

    mw = AlbumMiddleware(delay=0.15)
    messages = [_album_message(i, group="g1") for i in range(1, 6)]

    # feed them "simultaneously" as Telegram would
    await asyncio.gather(*(mw(handler, m, {"seq": i}) for i, m in enumerate(messages)))

    # buffer period not elapsed yet → handler must not have run
    assert "album" not in captured

    await asyncio.sleep(0.3)
    assert "album" in captured
    album = captured["album"]
    assert len(album) == 5
    assert [m.message_id for m in album] == [1, 2, 3, 4, 5]
    # handler receives the first (head) message of the album
    assert captured["event"].message_id == 1


async def test_two_albums_stay_separate():
    captured = []

    async def handler(event, data):
        captured.append([m.message_id for m in data["album"]])

    mw = AlbumMiddleware(delay=0.1)
    await mw(handler, _album_message(1, "a"), {})
    await mw(handler, _album_message(2, "a"), {})
    await mw(handler, _album_message(1, "b"), {})
    await asyncio.sleep(0.35)
    captured.sort()
    assert captured == [[1], [1, 2]]
