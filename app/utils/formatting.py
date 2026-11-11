"""Small presentation helpers: progress bars, ETA, HTML-safe text."""

from __future__ import annotations

import html

from app.utils.sizes import human_size, human_speed


def escape(value: str | None) -> str:
    """HTML-escape user-provided text before sending with parse_mode=HTML."""
    return html.escape(value or "", quote=False)


def transfer_meta(done: float, total: float, speed: float) -> str:
    """One honest meta line: bytes, speed, ETA — or an explicit stall.

    Speed 0.0 means the engine saw consecutive no-growth polls (a true
    stall), so that state is named instead of hinted at with a dash.
    Below 1 KB/s an ETA is meaningless — a decaying average once printed
    "ETA 3512250h" — so slow trickles keep the byte counters and drop
    the ETA instead of lying with one.
    """
    if total > 0 and done >= total:
        return f"{human_size(done)} / {human_size(total)} · ✔ finishing"
    if speed <= 0:
        return f"{human_size(done)} / {human_size(total)} · ⏸ stalled"
    meta = f"{human_size(done)} / {human_size(total)} · {human_speed(speed)}"
    frac = (total - done) / total if total else 1.0
    if speed >= 1024 and 0.0 < frac < 1.0:
        meta += f" · ETA {fmt_eta((total - done) / speed)}"
    return meta


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
