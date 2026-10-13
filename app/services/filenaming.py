"""Resolve download destinations from the configurable folder template.

The template (``FILE_TEMPLATE`` env) describes a path *relative* to the
download root, e.g. ``{user}/{type}/{date}`` →
``downloads/chandima/video/2026-10-13/clip.mp4``.

Every path segment is sanitized so user-controlled values (usernames, file
names) can never escape the download root.
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from pathlib import Path

from app.services.media_types import MediaInfo

_FORBIDDEN = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_MAX_SEGMENT = 80  # per path segment
_MAX_FILENAME = 150


def sanitize_segment(
    name: str | None, fallback: str = "unnamed", max_len: int = _MAX_SEGMENT
) -> str:
    """Make an arbitrary string safe to use as a single path segment."""
    text = (name or "").strip()
    if not text:
        return fallback
    text = _FORBIDDEN.sub("_", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    # Emoji / exotic codepoints are fine on modern filesystems, but trim
    # combining-char garbage and cap length.
    text = text[:max_len].rstrip(" .")
    if not text:
        return fallback
    if text in {".", ".."}:
        return fallback
    return text


def sanitize_filename(name: str | None, fallback: str) -> str:
    """Sanitize a file name (keeps a single extension dot, caps length)."""
    text = sanitize_segment(name, fallback=fallback, max_len=_MAX_FILENAME)
    return text


class ChatInfo:
    """Minimal chat descriptor used for template tokens."""

    __slots__ = ("id", "type")

    def __init__(self, chat_id: int, chat_type: str):
        self.id = chat_id
        self.type = chat_type


class UserRef:
    """Minimal user descriptor used for template tokens."""

    __slots__ = ("id", "username", "first_name")

    def __init__(self, user_id: int, username: str | None, first_name: str | None):
        self.id = user_id
        self.username = username
        self.first_name = first_name

    @property
    def safe_name(self) -> str:
        """Preferred folder-safe name: @username, else first name, else id."""
        return sanitize_segment(self.username or self.first_name, fallback=str(self.id))


def build_template_tokens(
    user: UserRef, media: MediaInfo, chat: ChatInfo, when: datetime | None = None
) -> dict[str, str]:
    """Compute all tokens usable inside FILE_TEMPLATE."""
    when = when or datetime.now()
    media_kind = media.kind
    return {
        "user": user.safe_name,
        "username": sanitize_segment(user.username, fallback=user.safe_name),
        "user_id": str(user.id),
        "type": media_kind,
        "date": when.strftime("%Y-%m-%d"),
        "year": when.strftime("%Y"),
        "month": when.strftime("%m"),
        "day": when.strftime("%d"),
        "chat_id": str(chat.id),
        "chat_type": sanitize_segment(chat.type, fallback="chat"),
        # alias: {media_type} mirrors {type}
        "media_type": media_kind,
    }


def build_relative_dir(template: str, tokens: dict[str, str]) -> Path:
    """Render the template into a sanitized relative directory path."""
    rendered = template.format(**tokens)
    segments = [
        sanitize_segment(part, fallback="_")
        for part in rendered.split("/")
        if part.strip()
    ]
    # Defense in depth: drop anything weird that survived sanitization.
    segments = [s for s in segments if s not in {".", "..", ""}]
    if not segments:
        return Path("_misc")
    return Path(*segments)


def unique_path(directory: Path, filename: str) -> Path:
    """Return ``directory/filename`` or ``stem (2).ext`` … if it exists."""
    directory.mkdir(parents=True, exist_ok=True)
    candidate = directory / filename
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    for counter in range(2, 100):
        candidate = directory / f"{stem} ({counter}){suffix}"
        if not candidate.exists():
            return candidate
    # Extremely unlikely: fall back to a timestamped name.
    return directory / f"{stem} {int(time.time())}{suffix}"


def resolve_destination(
    root: Path,
    template: str,
    user: UserRef,
    media: MediaInfo,
    chat: ChatInfo,
    when: datetime | None = None,
) -> tuple[Path, str]:
    """Compute the final absolute destination for a media file.

    Returns ``(absolute_path, relative_display_path)``.
    """
    tokens = build_template_tokens(user, media, chat, when)
    rel_dir = build_relative_dir(template, tokens)
    ext = media.suggested_extension()
    original = media.file_name or f"{media.kind}_{media.file_unique_id}{ext}"
    filename = sanitize_filename(original, fallback=f"{media.kind}_{media.file_unique_id}{ext}")
    # Ensure the filename keeps an extension.
    if "." not in filename:
        filename = f"{filename}{ext}"
    final = unique_path(root / rel_dir, filename)
    return final, str(final.relative_to(root))
