"""Size parsing/formatting helpers used by config and the bot UX."""

from __future__ import annotations

import re

_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(kb|mb|gb|tb|b)$", re.IGNORECASE)
_SIZE_FACTORS = {"b": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3, "tb": 1024**4}


def parse_size(value: str | int) -> int:
    """Parse a human size string such as ``"20MB"``, ``"512 KB"`` or ``1024``.

    Returns the size in bytes. Raises ``ValueError`` for malformed input.
    """
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip().lower().replace(" ", "")
    if not text:
        raise ValueError("empty size value")
    if text.isdigit():
        return int(text)
    match = _SIZE_RE.match(text)
    if not match:
        raise ValueError(
            f"invalid size value: {value!r} (expected e.g. '20MB', '512KB', '1048576')"
        )
    number, unit = match.groups()
    result = float(number) * _SIZE_FACTORS[unit]
    if result < 0:
        raise ValueError("size cannot be negative")
    return int(result)


def human_size(num: float | int | None, precision: int = 1) -> str:
    """Format a byte count as a compact human readable string."""
    if num is None:
        return "—"
    num = float(num)
    if num < 1024:
        return f"{int(num)} B"
    for unit in ("KB", "MB", "GB", "TB", "PB"):
        num /= 1024
        if num < 1024:
            if num >= 100 or precision == 0:
                return f"{num:.0f} {unit}"
            return f"{num:.{precision}f} {unit}"
    return f"{num:.{precision}f} EB"


def human_speed(bytes_per_second: float | None) -> str:
    """Format a throughput value in bytes per second."""
    if not bytes_per_second or bytes_per_second <= 0:
        return "—"
    return f"{human_size(bytes_per_second)}/s"
