"""Direct-link downloads: validation, filenames, SSRF guard, transfers.

Covers the v0.3 /link feature end-to-end against a real aiohttp test server:
* probe learns size / range support / filename (Content-Disposition variants),
* single-stream fallback when the server ignores byte ranges,
* parallel ranged transfers with per-part resume data written via pwrite,
* non-retryable rejections (bad URL, HTTP 4xx, size cap, SSRF to loopback),
* the LINK_SSRF_GUARD modes, and the v0.3.1 regression: aiohttp ≥ 3.10
  stores the resolved address in the ``host`` field of resolver entries
  (``hostname`` holds the queried name) — the old guard read ``hostname``
  and refused EVERY hostname link with a bogus "private address" message,
* cancellation mid-transfer and the query-string scrubbing of error text.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import re
import socket
import time
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from app.config import Settings
from app.queue.engine import DownloadEngine, DownloadError, JobCancelled
from app.queue.manager import build_job
from app.services import urlfetch
from app.services.filenaming import UserRef
from app.services.urlfetch import (
    UrlDownloader,
    UrlRejected,
    _allowed_ip,
    _SsrfGuard,
    filename_from_response,
    parse_content_disposition,
    strip_query,
    validate_url,
)
from tests.conftest import VALID_TOKEN

USER = UserRef(111, "tester", "Tester")


def _url_job(tmp_path, url: str, name: str = "file.bin", size: int | None = None):
    target = tmp_path / "downloads" / name
    return build_job(
        user=USER,
        chat_id=100,
        message_id=1,
        media_type="url",
        file_id=url,
        file_unique_id="uniq-url",
        file_name=name,
        file_size=size,
        target_path=str(target),
        rel_path=name,
    )


class _FakeApi:
    is_local = False

    def file_url(self, token: str, path: str) -> str:
        return f"http://fake/bot{token}/{path}"


class _FakeBot:
    def __init__(self) -> None:
        self.token = VALID_TOKEN
        self.session = SimpleNamespace(api=_FakeApi())

    async def get_file(self, file_id: str, request_timeout: int | None = None):
        raise AssertionError("url jobs must never call getFile")


def _engine(settings) -> DownloadEngine:
    return DownloadEngine(_FakeBot(), settings)


def _allow_loopback(monkeypatch) -> None:
    """Bypass the SSRF guard for tests that must hit the local test server.

    Patches the mode-aware predicate both guard layers consult (the
    URL-literal check in validate_url and the resolver), leaving everything
    else real.
    """
    monkeypatch.setattr(urlfetch._SsrfGuard, "allowed", lambda self, ip: True)


def _range_app(data: bytes, *, concurrency: list[int] | None = None,
               chunk_delay: float = 0.0, headers: dict | None = None):
    """Serves `data` with proper byte-range support (and optional slowness)."""

    async def _stream(request, body: bytes, extra_headers: dict) -> web.StreamResponse:
        resp = web.StreamResponse(
            status=extra_headers.pop("__status__", 200),
            headers={"Accept-Ranges": "bytes",
                     "Content-Length": str(len(body)),
                     **extra_headers},
        )
        await resp.prepare(request)
        for i in range(0, len(body), 65536):
            if chunk_delay:
                await asyncio.sleep(chunk_delay)
            await resp.write(body[i : i + 65536])
        await resp.write_eof()
        return resp

    async def handler(request: web.Request) -> web.StreamResponse:
        rng = request.headers.get("Range")
        if rng:
            match = re.match(r"bytes=(\d+)-(\d*)", rng)
            start = int(match.group(1))
            end = int(match.group(2)) if match.group(2) else len(data) - 1
            end = min(end, len(data) - 1)
            extra = {
                "Content-Range": f"bytes {start}-{end}/{len(data)}",
                "__status__": 206,
                **(headers or {}),
            }
            return await _stream(request, data[start : end + 1], extra)
        return await _stream(request, data, dict(headers or {}))

    app = web.Application()
    state = {"active": 0, "peak": 0}

    async def tracked(request: web.Request) -> web.StreamResponse:
        if concurrency is not None:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
        try:
            return await handler(request)
        finally:
            if concurrency is not None:
                state["active"] -= 1
                concurrency[:] = [state["peak"]]

    app.router.add_get("/file.bin", tracked)
    return handler, app


# ── pure helpers ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ('attachment; filename="naive file.mp4"', "naive file.mp4"),
        ("attachment; filename=stats.csv", "stats.csv"),
        ("attachment; filename*=UTF-8''percent%20encoded%20name.zip", "percent encoded name.zip"),
        ("attachment", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_content_disposition(value, expected):
    assert parse_content_disposition(value) == expected


def test_filename_from_response_sources():
    # Content-Disposition wins
    assert filename_from_response(
        "https://h/dl?id=1", 'attachment; filename="report.pdf"', None
    ) == "report.pdf"
    # URL path is decoded and used
    assert filename_from_response("https://h/files/my%20movie.mkv", None, None) == "my movie.mkv"
    # A bare URL path keeps its last segment
    assert filename_from_response("https://h/dl", None, "video/mp4") == "dl.mp4"
    # No path at all: generic name + extension from content type
    assert filename_from_response("https://h", None, "video/mp4") == "download.mp4"
    # Unsafe names are sanitized
    assert filename_from_response(
        "https://h/../../etc/passwd", None, None
    ) == "passwd"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/file",
        "file:///etc/passwd",
        "https://user:pass@example.com/file",
        "notaurl",
        "",
    ],
)
def test_validate_url_rejects(url):
    with pytest.raises(UrlRejected):
        validate_url(url)


def test_validate_url_accepts_and_cleans():
    assert validate_url("  https://example.com/file.bin  ") == "https://example.com/file.bin"
    assert validate_url("<http://h/x>") == "http://h/x"


def test_strip_query_removes_secrets():
    assert strip_query("https://h/f.zip?token=SECRET&x=1") == "https://h/f.zip"


@pytest.mark.parametrize(
    ("ip", "allowed"),
    [
        ("8.8.8.8", True),
        ("1.1.1.1", True),
        ("2606:4700::1111", True),
        ("10.0.0.1", False),
        ("192.168.1.1", False),
        ("172.16.0.1", False),
        ("127.0.0.1", False),
        ("169.254.169.254", False),  # cloud metadata
        ("100.64.1.1", False),  # CGNAT shared range
        ("0.0.0.0", False),
        ("224.0.0.1", False),  # multicast
        ("::1", False),
        ("fe80::1", False),
        ("fc00::1", False),
        ("::ffff:10.0.0.1", False),  # v4-mapped private
    ],
)
def test_allowed_ip(ip, allowed):
    assert _allowed_ip(ipaddress.ip_address(ip)) is allowed


# ── SSRF guard modes (LINK_SSRF_GUARD) ──────────────────────────────────


@pytest.mark.parametrize(
    ("mode", "ip", "allowed"),
    [
        # basic refuses genuinely internal targets …
        ("basic", "10.0.0.1", False),
        ("basic", "172.31.255.1", False),
        ("basic", "192.168.1.1", False),
        ("basic", "127.0.0.1", False),
        ("basic", "169.254.169.254", False),  # cloud metadata
        ("basic", "0.0.0.0", False),
        ("basic", "224.0.0.1", False),
        ("basic", "::1", False),
        ("basic", "fe80::1", False),
        ("basic", "fc00::1", False),
        ("basic", "::ffff:10.0.0.1", False),  # v4-mapped private
        # … but allows public-but-unusual space that strict refuses
        ("basic", "100.64.1.1", True),  # CGNAT shared range
        ("basic", "198.18.0.5", True),  # benchmarking
        ("basic", "95.211.142.134", True),  # a real seedr node
        ("strict", "100.64.1.1", False),
        ("strict", "95.211.142.134", True),
        # off disables the checks entirely
        ("off", "127.0.0.1", True),
        ("off", "10.0.0.1", True),
        ("off", "169.254.169.254", True),
    ],
)
def test_ssrf_guard_modes(mode, ip, allowed):
    assert _SsrfGuard(mode).allowed(ipaddress.ip_address(ip)) is allowed


def test_validate_url_ip_literals_respect_guard_mode():
    with pytest.raises(UrlRejected, match="SSRF"):
        validate_url("http://192.168.1.10/file.bin", guard=_SsrfGuard("basic"))
    assert (
        validate_url("http://192.168.1.10/file.bin", guard=_SsrfGuard("off"))
        == "http://192.168.1.10/file.bin"
    )
    with pytest.raises(UrlRejected, match="100.64.1.1"):
        validate_url("http://100.64.1.1/file.bin", guard=_SsrfGuard("strict"))
    assert validate_url("http://100.64.1.1/file.bin", guard=_SsrfGuard("basic"))


# ── the resolver guard (v0.3.1 regression) ──────────────────────────────


def _resolver_entry(name: str, ip: str, *, old_style: bool = False) -> dict:
    """One aiohttp resolver entry, in either historical field convention.

    aiohttp >= 3.10: address under ``host``, queried name under ``hostname``
    (a TypedDict — plain dict at runtime — since 3.11). aiohttp <= 3.9 kept
    plain dicts with the address under ``hostname`` instead.
    """
    entry: dict = {"port": 443, "family": socket.AF_INET, "proto": 6, "flags": 0}
    if old_style:
        entry |= {"hostname": ip, "host": name}
    else:
        entry |= {"hostname": name, "host": ip}
    return entry


class _FakeInnerResolver:
    def __init__(self, entries: list[dict]) -> None:
        self._entries = entries

    async def resolve(
        self, host: str, port: int = 0, family: int = socket.AF_INET
    ) -> list[dict]:
        return list(self._entries)

    async def close(self) -> None:
        return None


def _resolver(entries: list[dict], mode: str = "basic") -> urlfetch._SafeResolver:
    resolver = urlfetch._SafeResolver(_SsrfGuard(mode))
    resolver._inner = _FakeInnerResolver(entries)  # noqa: SLF001
    return resolver


@pytest.mark.parametrize("old_style", [False, True], ids=["new-style", "old-style"])
async def test_resolver_accepts_public_host(old_style):
    """Regression (v0.3.0): the guard read ``entry["hostname"]`` as the
    resolved IP; since aiohttp 3.10 that field holds the queried NAME, so
    every hostname link was refused with a bogus SSRF message.
    """
    resolver = _resolver(
        [_resolver_entry("nw34.seedr.cc", "95.211.142.134", old_style=old_style)]
    )
    entries = await resolver.resolve("nw34.seedr.cc", 443)
    assert entries[0]  # passed through unchanged, guard satisfied


async def test_resolver_blocks_private_target_with_named_message():
    resolver = _resolver([_resolver_entry("cache.lan", "192.168.1.50")])
    with pytest.raises(urlfetch._SsrfBlockedError) as excinfo:
        await resolver.resolve("cache.lan", 443)
    message = str(excinfo.value)
    assert "cache.lan" in message
    assert "192.168.1.50" in message
    assert "LINK_SSRF_GUARD=off" in message


async def test_resolver_off_mode_allows_internal_target():
    resolver = _resolver([_resolver_entry("nas.local", "10.0.0.5")], mode="off")
    entries = await resolver.resolve("nas.local", 443)
    assert entries[0]["host"] == "10.0.0.5"


async def test_resolver_rejects_entries_without_an_ip():
    resolver = _resolver(
        [{"hostname": "???", "host": "also-not-an-ip", "port": 443,
          "family": socket.AF_INET, "proto": 6, "flags": 0}]
    )
    with pytest.raises(urlfetch._SsrfBlockedError):
        await resolver.resolve("weird.example", 443)


# ── probing against a live server ──────────────────────────────────────────


async def test_probe_ranged_server(settings, monkeypatch):
    _allow_loopback(monkeypatch)
    handler, app = _range_app(b"x" * 1000)
    server = TestServer(app)
    await server.start_server()
    try:
        downloader = UrlDownloader(settings)
        probe = await downloader.probe(str(server.make_url("/file.bin")))
        assert probe.size == 1000
        assert probe.accept_ranges is True
        assert probe.filename == "file.bin"
        await downloader.aclose()
    finally:
        await server.close()


async def test_probe_server_without_ranges(settings, monkeypatch):
    _allow_loopback(monkeypatch)

    async def handler(request):
        return web.Response(
            body=b"y" * 500, headers={"Content-Length": "500"}
        )  # no Accept-Ranges, ignores Range

    app = web.Application()
    app.router.add_get("/plain.bin", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        downloader = UrlDownloader(settings)
        probe = await downloader.probe(str(server.make_url("/plain.bin")))
        assert probe.size == 500
        assert probe.accept_ranges is False
        await downloader.aclose()
    finally:
        await server.close()


async def test_probe_follows_redirects(settings, monkeypatch):
    _allow_loopback(monkeypatch)
    handler, app = _range_app(b"z" * 10)

    async def redirect(request):
        raise web.HTTPFound("/file.bin")

    app.router.add_get("/go", redirect)
    server = TestServer(app)
    await server.start_server()
    try:
        downloader = UrlDownloader(settings)
        probe = await downloader.probe(str(server.make_url("/go")))
        assert str(probe.final_url).endswith("/file.bin")
        assert probe.size == 10
        await downloader.aclose()
    finally:
        await server.close()


async def test_probe_404_is_rejected(settings, monkeypatch):
    _allow_loopback(monkeypatch)
    app = web.Application()

    async def handler(request):
        return web.Response(status=404, text="gone")

    app.router.add_get("/gone.bin", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        downloader = UrlDownloader(settings)
        with pytest.raises(UrlRejected, match="404"):
            await downloader.probe(str(server.make_url("/gone.bin")))
        await downloader.aclose()
    finally:
        await server.close()


async def test_probe_range_hating_403_falls_back_to_plain(settings, monkeypatch):
    _allow_loopback(monkeypatch)

    async def handler(request):
        if "Range" in request.headers:
            return web.Response(status=403, text="no ranges here")
        return web.Response(
            body=b"q" * 777,
            headers={"Content-Disposition": 'attachment; filename="real.mp4"'},
        )

    app = web.Application()
    app.router.add_get("/moody.bin", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        downloader = UrlDownloader(settings)
        probe = await downloader.probe(str(server.make_url("/moody.bin")))
        assert probe.size == 777
        assert probe.accept_ranges is False
        assert probe.filename == "real.mp4"
        await downloader.aclose()
    finally:
        await server.close()


async def test_probe_loopback_blocked_by_ssrf_guard(settings):
    """The real guard must refuse loopback targets (SSRF protection).

    No monkeypatching here — the guard itself is under test.
    """
    app = web.Application()

    async def handler(request):
        return web.Response(body=b"never reached")

    app.router.add_get("/secret.bin", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        downloader = UrlDownloader(settings)
        with pytest.raises(UrlRejected, match="SSRF"):
            await downloader.probe(str(server.make_url("/secret.bin")))
        await downloader.aclose()
    finally:
        await server.close()


async def test_probe_hostname_resolving_to_loopback_is_blocked(settings):
    """A URL whose HOSTNAME (not IP literal) resolves to loopback must be
    refused by the resolver layer — the path real SSRF attacks take.
    """
    app = web.Application()

    async def handler(request):
        return web.Response(body=b"never reached")

    app.router.add_get("/secret.bin", handler)
    server = TestServer(app)
    await server.start_server()
    downloader = UrlDownloader(settings)
    try:
        url = str(server.make_url("/secret.bin")).replace("127.0.0.1", "localhost", 1)
        with pytest.raises(UrlRejected, match="localhost") as excinfo:
            await downloader.probe(url)
        message = str(excinfo.value)
        # names the exact refused address (v4 or v6 loopback, host-dependent)
        assert "127.0.0.1" in message or "::1" in message
    finally:
        await downloader.aclose()
        await server.close()


# ── transfers through the engine ───────────────────────────────────────────


async def test_single_stream_download_via_engine(settings, tmp_path, monkeypatch):
    _allow_loopback(monkeypatch)
    payload = b"S" * 100_000
    handler, app = _range_app(payload)
    server = TestServer(app)
    await server.start_server()
    try:
        engine = _engine(settings)
        job = _url_job(tmp_path, str(server.make_url("/file.bin")), name="small.bin")
        result = await engine.run(job)

        assert result.bytes_written == len(payload)
        assert result.sha256 == hashlib.sha256(payload).hexdigest()
        data = (tmp_path / "downloads" / "small.bin").read_bytes()
        assert data == payload
        assert job.total_bytes == len(payload)
        assert job.bytes_done == len(payload)
        await engine.aclose()
    finally:
        await server.close()


async def test_ranged_multi_connection_download(settings, tmp_path, monkeypatch):
    _allow_loopback(monkeypatch)
    payload = bytes(range(256)) * ((5 * 1024 * 1024) // 256)  # 5 MB, ranged path
    peak = [0]
    handler, app = _range_app(payload, concurrency=peak)
    server = TestServer(app)
    await server.start_server()
    try:
        engine = _engine(settings)
        job = _url_job(tmp_path, str(server.make_url("/file.bin")), name="big.bin",
                       size=len(payload))
        result = await engine.run(job)

        assert result.bytes_written == len(payload)
        assert result.sha256 == hashlib.sha256(payload).hexdigest()
        # pwrite correctness: the reassembled file is byte-identical
        data = (tmp_path / "downloads" / "big.bin").read_bytes()
        assert data == payload
        assert job.bytes_done == len(payload)
        assert peak[0] >= 2, "expected genuinely parallel range connections"
        await engine.aclose()
    finally:
        await server.close()


async def test_link_size_cap_is_non_retryable(settings, tmp_path, monkeypatch):
    _allow_loopback(monkeypatch)
    capped = Settings(
        bot_token=VALID_TOKEN,
        download_dir=settings.download_dir,
        db_path=settings.db_path,
        download_retries=3,
        link_max_size="64KB",
    )
    payload = b"C" * 200_000
    handler, app = _range_app(payload)
    server = TestServer(app)
    await server.start_server()
    try:
        engine = _engine(capped)
        job = _url_job(tmp_path, str(server.make_url("/file.bin")))
        with pytest.raises(DownloadError, match="LINK_MAX_SIZE"):
            await engine.run(job)
        assert job.attempts == 1  # rejected once, no retry burn
        await engine.aclose()
    finally:
        await server.close()


async def test_cancel_mid_transfer(settings, tmp_path, monkeypatch):
    _allow_loopback(monkeypatch)
    payload = b"K" * (2 * 1024 * 1024)
    handler, app = _range_app(payload, chunk_delay=0.02)  # slow single stream
    server = TestServer(app)
    await server.start_server()
    try:
        engine = _engine(settings)
        job = _url_job(tmp_path, str(server.make_url("/file.bin")), name="slow.bin")

        async def cancel_soon() -> None:
            await asyncio.sleep(0.15)
            job.cancel_event.set()

        canceller = asyncio.ensure_future(cancel_soon())
        t0 = time.monotonic()
        with pytest.raises(JobCancelled):
            await engine.run(job)
        assert time.monotonic() - t0 < 5.0
        await canceller
        await engine.aclose()
        # atomic-write contract: no partial file left behind
        assert not (tmp_path / "downloads" / "slow.bin").exists()
        assert not (tmp_path / "downloads" / "slow.bin.part").exists()
    finally:
        await server.close()


async def test_disabled_links_fail_fast(settings, tmp_path):
    disabled = Settings(
        bot_token=VALID_TOKEN,
        download_dir=settings.download_dir,
        db_path=settings.db_path,
        links_enabled=False,
    )
    engine = _engine(disabled)
    job = _url_job(tmp_path, "https://example.com/x.bin")
    with pytest.raises(DownloadError, match="LINKS_ENABLED"):
        await engine.run(job)


def test_engine_scrubs_url_query_from_errors(settings, tmp_path):
    engine = _engine(settings)
    job = _url_job(tmp_path, "https://host/file.zip?token=TOPSECRET")
    scrubbed = engine._scrub(job, "GET https://host/file.zip?token=TOPSECRET failed")
    assert "TOPSECRET" not in scrubbed
    assert "https://host/file.zip" in scrubbed
    # telegram jobs keep the plain token redaction only
    tg_job = _url_job(tmp_path, "file-1")
    tg_job.media_type = "document"
    assert engine._scrub(tg_job, "https://host/file.zip?token=TOPSECRET").count(
        "TOPSECRET"
    ) == 1
