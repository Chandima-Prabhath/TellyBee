"""Small presentation helpers: progress bars, ETA, HTML-safe text."""

from __future__ import annotations

import html


def escape(value: str | None) -> str:
    """HTML-escape user-provided text before sending with parse_mode=HTML."""
    return html.escape(value or "", quote=False)


def progress_bar(fraction: float | None, width: int = 10) -> str:
    """Render a compact unicode progress bar like ``▰▰▰▱▱▱▱▱▱▱``."""
    if fraction is None:
        return "▱" * width
    fraction = max(0.0, min(1.0, fraction))
    filled = round(fraction * width)
    return "▰" * filled + "▱" * (width - filled)


def fmt_eta(seconds: float | None) -> str:
    """Format an ETA in seconds as ``42s`` / ``3m 05s`` / ``1h 02m``."""
    if seconds is None or seconds < 0:
        return "—"
    seconds = int(seconds)
    if seconds < 1:
        return "0s"
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def pct(fraction: float | None) -> str:
    """Format a fraction as a percentage string."""
    if fraction is None:
        return "—"
    return f"{max(0.0, min(1.0, fraction)) * 100:.0f}%"


def plural_files(count: int) -> str:
    return f"{count} file" if count == 1 else f"{count} files"
