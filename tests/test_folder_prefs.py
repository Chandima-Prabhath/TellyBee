"""Per-user folder preferences (/folder) — the "one folder" feature.

Pins down the contract users actually feel:
* the pref round-trips through SQLite (default ↔ single, with/without name),
* single mode puts every new download in ONE folder — a series forwarded
  across several days lands together instead of scattered by date,
* a custom folder name is sanitized (no traversal, no forbidden chars),
* default mode keeps rendering FILE_TEMPLATE exactly as before.
"""

from __future__ import annotations

from pathlib import Path

from app.db import Database
from app.services.filenaming import (
    ChatInfo,
    UserRef,
    resolve_destination,
    sanitize_segment,
)
from app.services.media_types import MediaInfo

USER = UserRef(111, "chandima", "Chandima")
CHAT = ChatInfo(100, "private")
EPISODE = MediaInfo(
    kind="document",
    file_id="f2",
    file_unique_id="u2",
    file_name="show.s01e05.mkv",
    file_size=2000,
    mime_type="video/x-matroska",
)


async def test_folder_pref_roundtrip(tmp_path: Path):
    db = Database(tmp_path / "tellybee.db")
    await db.connect()
    try:
        assert await db.get_folder_pref(111) == ("default", None)
        await db.set_folder_pref(111, "single", "No Tail to Tell")
        assert await db.get_folder_pref(111) == ("single", "No Tail to Tell")
        # switching back clears the custom name
        await db.set_folder_pref(111, "default", None)
        assert await db.get_folder_pref(111) == ("default", None)
        # re-enabling single keeps whatever name is stored
        await db.set_folder_pref(111, "single", "Severance S02")
        assert await db.get_folder_pref(111) == ("single", "Severance S02")
    finally:
        await db.close()


async def test_folder_pref_rejects_unknown_mode(tmp_path: Path):
    db = Database(tmp_path / "tellybee.db")
    await db.connect()
    try:
        try:
            await db.set_folder_pref(1, "sideways")
        except ValueError:
            pass  # expected
        else:
            raise AssertionError("invalid mode must raise ValueError")
    finally:
        await db.close()


def test_single_folder_ignores_dates(tmp_path: Path):
    """A series forwarded over three days still lands in one folder."""
    import datetime

    days = [
        datetime.datetime(2026, 11, 3),
        datetime.datetime(2026, 11, 4),
        datetime.datetime(2026, 11, 5),
    ]
    seen: set[str] = set()
    for day in days:
        target, rel = resolve_destination(
            tmp_path,
            "{user}/{type}/{date}",
            USER,
            EPISODE,
            CHAT,
            when=day,
            single_folder=True,
        )
        seen.add(str(target.parent))
    assert seen == {str(tmp_path / "chandima")}


def test_single_folder_with_name(tmp_path: Path):
    target, rel = resolve_destination(
        tmp_path,
        "{user}/{type}/{date}",
        USER,
        EPISODE,
        CHAT,
        single_folder=True,
        folder_name="No Tail to Tell",
    )
    assert target.parent == tmp_path / "chandima" / "No Tail to Tell"
    assert target.name == "show.s01e05.mkv"
    assert rel == "chandima/No Tail to Tell/show.s01e05.mkv"


def test_custom_folder_name_is_sanitized(tmp_path: Path):
    target, _ = resolve_destination(
        tmp_path,
        "{user}/{type}",
        USER,
        EPISODE,
        CHAT,
        single_folder=True,
        folder_name="../../etc/passwd",
    )
    assert ".." not in target.parts
    assert target.parent == tmp_path / "chandima" / sanitize_segment("../../etc/passwd")


def test_default_mode_unchanged(tmp_path: Path):
    target, rel = resolve_destination(tmp_path, "{user}/{type}", USER, EPISODE, CHAT)
    assert target.parent == tmp_path / "chandima" / "document"
    assert rel == "chandima/document/show.s01e05.mkv"
