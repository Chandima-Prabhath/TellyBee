from pathlib import Path

from app.services.filenaming import (
    ChatInfo,
    UserRef,
    build_relative_dir,
    build_template_tokens,
    sanitize_filename,
    sanitize_segment,
    unique_path,
)
from app.services.media_types import MediaInfo

USER = UserRef(111, "chandima", "Chandima")
CHAT = ChatInfo(100, "private")
PHOTO = MediaInfo(kind="photo", file_id="f1", file_unique_id="u1", file_name=None, file_size=1000, mime_type="image/jpeg")
DOC = MediaInfo(kind="document", file_id="f2", file_unique_id="u2", file_name="report.pdf", file_size=2000, mime_type="application/pdf")


def test_sanitize_segment_strips_dangerous_chars():
    assert sanitize_segment('a/b\\c:d*e?f"g<h>|') == "a_b_c_d_e_f_g_h__"
    assert sanitize_segment("  spaced  name  ") == "spaced name"
    assert sanitize_segment("..") == "unnamed"
    assert sanitize_segment(None, fallback="fallback") == "fallback"
    assert sanitize_segment(".") == "unnamed"
    long = "x" * 300
    assert len(sanitize_segment(long)) <= 80


def test_sanitize_filename_keeps_name():
    assert sanitize_filename("my report.pdf", fallback="x") == "my report.pdf"
    assert sanitize_filename(None, fallback="doc_u1.bin") == "doc_u1.bin"


def test_template_tokens():
    tokens = build_template_tokens(USER, PHOTO, CHAT)
    assert tokens["user"] == "chandima"
    assert tokens["type"] == "photo"
    assert tokens["user_id"] == "111"
    assert tokens["chat_type"] == "private"
    assert tokens["date"].count("-") == 2


def test_relative_dir_from_template():
    tokens = build_template_tokens(USER, DOC, CHAT)
    rel = build_relative_dir("{user}/{type}/{date}", tokens)
    parts = rel.parts
    assert parts[0] == "chandima"
    assert parts[1] == "document"
    assert parts[2] == tokens["date"]


def test_relative_dir_never_escapes():
    # Even a hostile template cannot produce traversal after sanitization.
    tokens = build_template_tokens(USER, DOC, CHAT)
    rel = build_relative_dir("{user}/../..", tokens)
    assert ".." not in rel.parts


def test_unique_path_collision(tmp_path: Path):
    first = unique_path(tmp_path, "a.txt")
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"x")
    second = unique_path(tmp_path, "a.txt")
    assert second.name == "a (2).txt"
    second.write_bytes(b"y")
    third = unique_path(tmp_path, "a.txt")
    assert third.name == "a (3).txt"


def test_resolve_destination_uses_filename(tmp_path: Path):
    from app.services.filenaming import resolve_destination

    target, rel = resolve_destination(tmp_path, "{user}/{type}", USER, DOC, CHAT)
    assert target.parent == tmp_path / "chandima" / "document"
    assert target.name == "report.pdf"
    assert rel == "chandima/document/report.pdf"


def test_photo_default_extension():
    assert PHOTO.suggested_extension() == ".jpg"


def test_document_extension_from_name():
    assert DOC.suggested_extension() == ".pdf"
