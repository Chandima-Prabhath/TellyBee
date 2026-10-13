import pytest

from app.utils.sizes import human_size, human_speed, parse_size


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1024", 1024),
        ("20MB", 20 * 1024**2),
        ("20 MB", 20 * 1024**2),
        ("512kb", 512 * 1024),
        ("1.5GB", int(1.5 * 1024**3)),
        ("2gb", 2 * 1024**3),
        ("10B", 10),
    ],
)
def test_parse_size(raw, expected):
    assert parse_size(raw) == expected


def test_parse_size_invalid():
    for bad in ("", "abc", "MB", "-5MB", "1.2.3MB"):
        with pytest.raises(ValueError):
            parse_size(bad)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (0, "0 B"),
        (999, "999 B"),
        (1024, "1.0 KB"),
        (1536, "1.5 KB"),
        (1024**2, "1.0 MB"),
        (27.4 * 1024**2, "27.4 MB"),
        (None, "—"),
    ],
)
def test_human_size(value, expected):
    assert human_size(value) == expected


def test_human_speed():
    assert human_speed(None) == "—"
    assert human_speed(0) == "—"
    assert human_speed(5 * 1024**2) == "5.0 MB/s"
