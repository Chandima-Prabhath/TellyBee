from app.utils.formatting import escape, fmt_eta, pct, plural_files, progress_bar


def test_escape():
    assert escape("<b>&x") == "&lt;b&gt;&amp;x"
    assert escape(None) == ""
    assert escape("plain") == "plain"


def test_progress_bar():
    assert progress_bar(0.0) == "▱" * 10
    assert progress_bar(1.0) == "▰" * 10
    assert progress_bar(0.5, width=4) == "▰▰▱▱"
    assert progress_bar(None) == "▱" * 10
    assert progress_bar(-1) == "▱" * 10  # clamped
    assert progress_bar(2) == "▰" * 10  # clamped


def test_fmt_eta():
    assert fmt_eta(None) == "—"
    assert fmt_eta(0) == "0s"
    assert fmt_eta(42) == "42s"
    assert fmt_eta(65) == "1m 05s"
    assert fmt_eta(3725) == "1h 02m"


def test_pct():
    assert pct(None) == "—"
    assert pct(0.456) == "46%"
    assert pct(1.2) == "100%"


def test_plural_files():
    assert plural_files(1) == "1 file"
    assert plural_files(5) == "5 files"
