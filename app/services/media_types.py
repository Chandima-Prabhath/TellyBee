"""Extract normalized media descriptors from Telegram messages."""

from __future__ import annotations

import mimetypes
from dataclasses import dataclass

from aiogram.types import Message

#: Default extension per media kind when the file carries no name/mime hint.
_DEFAULT_EXT: dict[str, str] = {
    "photo": ".jpg",
    "video": ".mp4",
    "document": ".bin",
    "audio": ".mp3",
    "voice": ".ogg",
    "video_note": ".mp4",
    "sticker": ".webp",
    "animation": ".mp4",
}


@dataclass(frozen=True, slots=True)
class MediaInfo:
    """Uniform description of any downloadable media object."""

    kind: str  # one of SUPPORTED_MEDIA_TYPES
    file_id: str
    file_unique_id: str
    file_name: str | None = None
    file_size: int | None = None
    mime_type: str | None = None

    def suggested_extension(self) -> str:
        """Best-effort extension: original name > mime type > kind default."""
        if self.file_name and "." in self.file_name:
            ext = self.file_name.rsplit(".", 1)[1].lower()
            if 1 <= len(ext) <= 5 and ext.isalnum():
                return f".{ext}"
        if self.mime_type:
            guessed = mimetypes.guess_extension(self.mime_type)
            if guessed:
                # Normalize a couple of ugly stdlib guesses.
                return {".jpe": ".jpg", ".oga": ".ogg",".webm": ".webm"}.get(guessed, guessed)
        return _DEFAULT_EXT.get(self.kind, ".bin")


def extract_media(message: Message) -> MediaInfo | None:
    """Return a :class:`MediaInfo` for a message, or ``None`` if it has no
    downloadable media. Photos resolve to their largest size variant.
    """
    if message.photo:
        best = message.photo[-1]
        return MediaInfo(
            kind="photo",
            file_id=best.file_id,
            file_unique_id=best.file_unique_id,
            file_name=None,
            file_size=best.file_size,
            mime_type="image/jpeg",
        )
    if message.video:
        return _from_filelike("video", message.video, message.video.file_name)
    if message.animation:
        return _from_filelike("animation", message.animation, message.animation.file_name)
    if message.document:
        return _from_filelike("document", message.document, message.document.file_name)
    if message.audio:
        return _from_filelike("audio", message.audio, message.audio.file_name)
    if message.voice:
        return _from_filelike("voice", message.voice, None)
    if message.video_note:
        return _from_filelike("video_note", message.video_note, None)
    if message.sticker:
        sticker = message.sticker
        info = _from_filelike("sticker", sticker, getattr(sticker, "file_name", None))
        if sticker.is_video:
            info = MediaInfo(
                kind=info.kind,
                file_id=info.file_id,
                file_unique_id=info.file_unique_id,
                file_name=info.file_name,
                file_size=info.file_size,
                mime_type="video/webm",
            )
        return info
    return None


def _from_filelike(kind: str, obj, file_name: str | None) -> MediaInfo:
    return MediaInfo(
        kind=kind,
        file_id=obj.file_id,
        file_unique_id=obj.file_unique_id,
        file_name=file_name,
        file_size=getattr(obj, "file_size", None),
        mime_type=getattr(obj, "mime_type", None),
    )
