"""Media extraction from real aiogram type instances."""

import datetime

from aiogram.types import Animation, Audio, Chat, Document, Message, PhotoSize, Sticker, Video

from app.services.media_types import extract_media


def _msg(**kwargs) -> Message:
    base = dict(
        message_id=1,
        date=datetime.datetime.now(datetime.timezone.utc),
        chat=Chat(id=1, type="private"),
    )
    base.update(kwargs)
    return Message(**base)


def test_photo_takes_largest():
    msg = _msg(
        photo=[
            PhotoSize(file_id="small", file_unique_id="u-small", width=1, height=1, file_size=10),
            PhotoSize(file_id="big", file_unique_id="u-big", width=2, height=2, file_size=20),
        ]
    )
    media = extract_media(msg)
    assert media is not None
    assert media.kind == "photo"
    assert media.file_id == "big"
    assert media.file_size == 20
    assert media.suggested_extension() == ".jpg"


def test_video():
    msg = _msg(
        video=Video(
            file_id="v1", file_unique_id="uv1", width=10, height=10, duration=5,
            file_name="clip.mp4", mime_type="video/mp4", file_size=999,
        )
    )
    media = extract_media(msg)
    assert media.kind == "video"
    assert media.file_name == "clip.mp4"
    assert media.suggested_extension() == ".mp4"


def test_document():
    msg = _msg(
        document=Document(
            file_id="d1", file_unique_id="ud1", file_name="notes.txt",
            mime_type="text/plain", file_size=42,
        )
    )
    media = extract_media(msg)
    assert media.kind == "document"
    assert media.file_name == "notes.txt"


def test_animation():
    msg = _msg(
        animation=Animation(
            file_id="a1", file_unique_id="ua1", width=10, height=10, duration=1,
            file_name="fun.gif", mime_type="video/mp4",
        )
    )
    media = extract_media(msg)
    assert media.kind == "animation"


def test_audio():
    msg = _msg(
        audio=Audio(file_id="au1", file_unique_id="uau1", duration=10, mime_type="audio/mpeg")
    )
    media = extract_media(msg)
    assert media.kind == "audio"


def test_sticker():
    msg = _msg(
        sticker=Sticker(
            file_id="s1", file_unique_id="us1", type="regular", width=512, height=512,
            is_animated=False, is_video=False,
        )
    )
    media = extract_media(msg)
    assert media.kind == "sticker"
    assert media.suggested_extension() == ".webp"


def test_text_message_has_no_media():
    msg = _msg(text="hello")
    assert extract_media(msg) is None
