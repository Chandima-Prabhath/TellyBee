"""Direct HTTP(S) link downloads — the fast lane next to Telegram files.

Activated with ``/link <url>`` or by sending a plain message that contains
an http(s) URL (see ``app/handlers/links.py``; toggle with ``LINKS_ENABLED``).

Design goals:

* **Fast** — servers that support byte ranges get the file over several
  parallel connections (``LINK_CONNECTIONS``, default 4), which typically
  saturates the host's bandwidth where a single stream would not. Servers
  without range support silently fall back to a single streamed connection.
* **Safe** — an SSRF guard inside the connector's resolver refuses to
  connect to internal addresses (loopback, LAN, link-local incl. cloud
  metadata, unique-local IPv6 — a crafted link can never reach the host's
  internal network or the docker network's other services), only http/https
  schemes are allowed, and URLs with embedded credentials are rejected
  outright. The guard mode is configurable (``LINK_SSRF_GUARD``): ``basic``
  (default) blocks internal targets only, ``strict`` additionally refuses
  unusual non-global address space, ``off`` disables the checks.
* **Bounded** — ``LINK_MAX_SIZE`` caps the accepted size (when the server
  declares it), unknown sizes are enforced mid-transfer, and the queue's
  disk-headroom guard applies exactly like for Telegram files.
* **Robust** — every ranged part resumes itself from its current offset on
  transient errors; rate limits (429 / 503 + Retry-After) are waited out
  with backoff that honors the server's ``Retry-After``, and a throttled
  parallel run finishes its missing bytes over a single connection instead
  of restarting; the outer engine retry machinery treats the whole file
  like any other download (atomic ``.part`` → rename, cancel between
  chunks, ENOSPC fails fast).
"""

from __future__ import annotations

import asyncio
import errno
import functools
import hashlib
import ipaddress
import logging
import mimetypes
import os
import re
import socket
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

import aiohttp

from app.config import Settings
from app.queue.errors import DownloadError, EngineResult, JobCancelled
from app.queue.jobs import DownloadJob
from app.utils.sizes import human_size

logger = logging.getLogger(__name__)

#: Files smaller than this never get the multi-connection treatment — the
#: handshake overhead outweighs the parallelism.
_RANGED_MIN_BYTES = 4 * 1024 * 1024
#: Per-part retry delays (seconds) for transient transport errors before
#: the outer engine retry takes over.
_PART_RETRY_DELAYS = (1.0, 3.0)
#: Backoff (seconds) when the server rate-limits (429/503) without saying
#: for how long — generous, because "come back later" is exactly what a
#: rate limit means, and restarting the whole file via the engine retry
#: costs far more than waiting out the window.
_RATE_LIMIT_DELAYS = (2.0, 4.0, 8.0, 16.0, 30.0)
#: Cumulative patience for rate-limit waits per ranged part (parallel
#: phase) and for the whole sequential finish phase, in seconds.
_RATE_LIMIT_BUDGET = 90.0
#: Upper bound for honoring a single Retry-After value (some CDNs send
#: hours; waiting that long inside a job helps nobody — /cancel exists).
_RETRY_AFTER_CAP = 90.0
#: Ranged parts start staggered by this much: the probe plus N
#: simultaneous GETs is exactly the burst that per-second rate limiters
#: refuse with 429 (seen in the wild on seedr-style CDNs). Kept small —
#: big transfers lose nothing, and the retry machinery backstops the rest.
_PART_START_STAGGER = 0.25
#: Cool-down before the sequential finish phase starts (as asked by the
#: server, capped) — its burst was just refused, after all.
_RATE_FALLBACK_COOLDOWN = 5.0
#: Probe-side rate-limit patience: waits capped per-wait and in total (the
#: /link reply should not hang for minutes before the job even queues).
_PROBE_RATE_CAP = 30.0
_PROBE_RATE_BUDGET = 60.0


class UrlRejected(Exception):
    """Non-retryable, user-readable rejection of a link (bad URL, HTTP 4xx…)."""


class _RateLimited(aiohttp.ClientResponseError):
    """429 (or 503 + Retry-After): the server asked us to slow down.

    Subclasses :class:`aiohttp.ClientResponseError` so the engine's
    transient handling (retry with backoff) still applies when one escapes
    after our own patience ran out. ``retry_after`` carries the server's
    asked-for delay in seconds, when it said anything at all.
    """

    def __init__(self, resp: aiohttp.ClientResponse, retry_after: float | None) -> None:
        waited = f"; asked to retry after {retry_after:.0f}s" if retry_after else ""
        super().__init__(
            resp.request_info,
            resp.history,
            status=resp.status,
            message=f"rate limited (HTTP {resp.status}){waited}",
            headers=resp.headers,
        )
        self.retry_after = retry_after


def _rate_limited_error(resp: aiohttp.ClientResponse) -> _RateLimited:
    return _RateLimited(resp, _retry_after(resp.headers))


def _is_rate_limit_status(resp: aiohttp.ClientResponse) -> bool:
    """429 always; 503 only when the server says when to come back."""
    return resp.status == 429 or (
        resp.status == 503 and bool(resp.headers.get("Retry-After"))
    )


def _retry_after(headers: Mapping[str, str]) -> float | None:
    """Parse ``Retry-After`` (delta-seconds or HTTP-date), clamped sanely.

    Returns ``None`` when absent or unparseable. Values are clamped to
    [1, _RETRY_AFTER_CAP]: ``Retry-After: 0`` means "now" (still back off
    a little), and multi-hour values would park a worker for no gain.
    """
    value = (headers.get("Retry-After") or "").strip()
    if not value:
        return None
    if value.isdigit():
        return max(1.0, min(float(value), _RETRY_AFTER_CAP))
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=UTC)
    return max(1.0, min((when - datetime.now(UTC)).total_seconds(), _RETRY_AFTER_CAP))


#: Raised by the SSRF guard inside the connector's resolver. Subclasses
#: ValueError so it slips through aiohttp's OSError-wrapping untouched, and
#: is converted to a non-retryable UrlRejected at every call site.
class _SsrfBlockedError(ValueError):
    pass


def _blocked_message(host: str, ip: object) -> str:
    """Chat-friendly SSRF rejection that names the exact address refused."""
    return (
        f"refusing to connect: {host} resolves to {ip}, a private or "
        "otherwise blocked address (SSRF guard). If you trust this source, "
        "set LINK_SSRF_GUARD=off"
    )


# ── URL validation & filenames ──────────────────────────────────────────────


def validate_url(url: str, guard: _SsrfGuard | None = None) -> str:
    """Basic URL sanity for link downloads. Returns the cleaned URL.

    Raises :class:`UrlRejected` with a chat-friendly reason otherwise.
    ``guard`` selects the SSRF mode used for IP-literal hosts (defaults to
    ``basic``; the downloader passes its configured one).
    """
    url = (url or "").strip().strip("<>")
    if not url:
        raise UrlRejected("empty URL")
    parsed = urlparse(url)
    if parsed.scheme.lower() not in ("http", "https"):
        raise UrlRejected("only http:// and https:// links are supported")
    if not parsed.netloc:
        raise UrlRejected("the URL has no host")
    if parsed.username or parsed.password:
        raise UrlRejected("URLs with embedded credentials are not supported")
    # aiohttp's connector SKIPS the resolver for IP-literal hosts, so the
    # SSRF guard must check those here; DNS names are checked at connect
    # time by _SafeResolver (covers redirects + rebinding).
    host = parsed.hostname or ""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        pass  # a hostname — resolver guard applies
    else:
        if not (guard or _SsrfGuard()).allowed(ip):
            raise UrlRejected(_blocked_message(host, ip))
    return url


def strip_query(url: str) -> str:
    """Drop the query string — signed URLs may carry secrets in it."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


_CD_FILENAME_STAR_RE = re.compile(r"filename\*\s*=\s*(?:UTF-8|utf-8)''([^;]+)", re.IGNORECASE)
_CD_FILENAME_RE = re.compile(r'filename\s*=\s*"((?:[^"\\]|\\.)+)"', re.IGNORECASE)
_CD_FILENAME_BARE_RE = re.compile(r"filename\s*=\s*([^;\"]+)", re.IGNORECASE)


def parse_content_disposition(value: str | None) -> str | None:
    """Extract a filename from a Content-Disposition header, if present.

    Handles the RFC 6266 forms: ``filename="naïve file.mp4"``,
    ``filename=stats.csv`` and the RFC 5987 extended
    ``filename*=UTF-8''percent%20encoded`` variant.
    """
    if not value:
        return None
    match = _CD_FILENAME_STAR_RE.search(value)
    if match:
        name = unquote(match.group(1).strip())
        if name:
            return name
    match = _CD_FILENAME_RE.search(value)
    if match:
        return match.group(1).replace('\\"', '"').strip()
    match = _CD_FILENAME_BARE_RE.search(value)
    if match:
        return match.group(1).strip().strip("'")
    return None


def filename_from_response(
    final_url: str, content_disposition: str | None, content_type: str | None
) -> str:
    """Best-effort file name: Content-Disposition > URL path > generic."""
    from app.services.filenaming import sanitize_filename

    name = parse_content_disposition(content_disposition)
    if not name:
        path = urlparse(final_url).path
        name = unquote(path.rsplit("/", 1)[-1]) if path else ""
    name = sanitize_filename(name or "", fallback="")
    if not name or name == ".":
        name = "download"
    if "." not in name:
        mime = (content_type or "").split(";")[0].strip().lower()
        ext = mimetypes.guess_extension(mime) if mime else None
        name += {".jpe": ".jpg", ".oga": ".ogg"}.get(ext or "", ext or "")  # type: ignore[arg-type]
    return name


# ── SSRF guard ───────────────────────────────────────────────────────────────


def _allowed_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """True only for addresses that are unambiguously public (strict mode)."""
    if (
        ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
        or ip.is_private
    ):
        return False
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        return _allowed_ip(ip.ipv4_mapped)
    if not ip.is_global:  # documentation / benchmarking / shared ranges …
        return False
    return True


#: Explicitly internal targets — everything ``basic`` mode refuses. Kept as
#: an explicit list (not the ``ipaddress`` properties, whose ``is_private``
#: semantics drifted across Python releases) so behavior is deterministic:
#: loopback, RFC1918 LANs, link-local (incl. the 169.254.169.254 cloud
#: metadata endpoint), unique-local IPv6, multicast, unspecified/0.0.0.0.
_BASIC_BLOCKED_V4 = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("224.0.0.0/4"),
    ipaddress.ip_network("255.255.255.255/32"),
)
_BASIC_BLOCKED_V6 = (
    ipaddress.ip_network("::/128"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("ff00::/8"),
)


class _SsrfGuard:
    """Mode-aware SSRF predicate (``LINK_SSRF_GUARD``).

    * ``basic`` — refuse genuinely internal targets only. Public-but-unusual
      space (CGNAT 100.64/10, benchmarking 198.18/15, …) is allowed: real
      file hosts never sit there, but some ISP/CDN setups hand out such
      addresses and refusing them breaks links for no security gain.
    * ``strict`` — additionally refuse everything not unambiguously public
      (the pre-0.3.1 behavior).
    * ``off`` — no address checks at all (trusted bots, LAN file sources).
    """

    __slots__ = ("mode",)

    def __init__(self, mode: str = "basic") -> None:
        self.mode = (mode or "basic").lower()

    def allowed(self, ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
        if self.mode == "off":
            return True
        if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        blocked = _BASIC_BLOCKED_V6 if ip.version == 6 else _BASIC_BLOCKED_V4
        if any(ip in network for network in blocked):
            return False
        if self.mode == "strict":
            return _allowed_ip(ip)
        return True


def _resolved_ip(
    entry: object,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    """The resolved address of a resolver entry, whichever field holds it.

    aiohttp >= 3.10 records the address under ``host`` and the queried name
    under ``hostname`` (a NamedTuple in 3.10; a TypedDict — a plain dict at
    runtime — since 3.11, which is what string indexing needs). aiohttp
    <= 3.9 returned plain dicts with the address under ``hostname`` instead.
    Whichever field parses as an IP address IS the address; the other one is
    the name. Reading a fixed field breaks on half the aiohttp versions in
    the wild — v0.3.0 read ``entry["hostname"]`` and thereby rejected
    every hostname link with a bogus "private address" message.
    """
    if isinstance(entry, dict):
        candidates: tuple[object, ...] = (entry.get("host"), entry.get("hostname"))
    else:  # NamedTuple entries (aiohttp 3.10.x)
        candidates = (getattr(entry, "host", None), getattr(entry, "hostname", None))
    for value in candidates:
        if not isinstance(value, str):
            continue
        try:
            return ipaddress.ip_address(value)
        except ValueError:
            continue
    return None


class _SafeResolver(aiohttp.abc.AbstractResolver):
    """DNS resolver that refuses to connect to private/internal targets.

    aiohttp asks the connector's resolver for every connection attempt —
    including redirects — so hooking here blocks SSRF attempts (and DNS
    rebinding to internal addresses) at connect time, with no TOCTOU gap.
    Entries are returned unchanged, so aiohttp connects to exactly the
    addresses that were checked. The guard mode comes from
    ``LINK_SSRF_GUARD`` via the injected :class:`_SsrfGuard`.
    """

    def __init__(self, guard: _SsrfGuard) -> None:
        self._guard = guard
        self._inner = aiohttp.DefaultResolver()

    async def resolve(  # type: ignore[override]
        self, host: str, port: int = 0, family: int = socket.AF_INET
    ) -> list[dict]:
        entries = await self._inner.resolve(host, port, family)
        for entry in entries:
            ip = _resolved_ip(entry)
            if ip is None:
                # No parseable address in the entry — refuse rather than
                # connect to something unchecked. Defensive: stock aiohttp
                # resolvers always fill one of the two fields.
                raise _SsrfBlockedError(
                    "refusing to connect: the resolver answer for "
                    f"{host!r} contains no IP address (SSRF guard)"
                ) from None
            if not self._guard.allowed(ip):
                raise _SsrfBlockedError(_blocked_message(host, ip))
        return entries

    async def close(self) -> None:  # type: ignore[override]
        await self._inner.close()


# ── probing ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """What we learned about a link before queueing it."""

    final_url: str
    filename: str
    size: int | None
    accept_ranges: bool
    content_type: str | None


def _total_from_content_range(value: str) -> int | None:
    """Parse ``bytes 0-0/12345`` → 12345 (total size from a 206 probe)."""
    if not value:
        return None
    total = value.rsplit("/", 1)[-1].strip()
    return int(total) if total.isdigit() else None


# ── the downloader ───────────────────────────────────────────────────────────


class UrlDownloader:
    """Executes direct-link download jobs (one ``.part`` file, atomic rename)."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._guard = _SsrfGuard(settings.link_ssrf_guard)
        self._http: aiohttp.ClientSession | None = None

    async def _session(self) -> aiohttp.ClientSession:
        if self._http is None or self._http.closed:
            self._http = aiohttp.ClientSession(
                # No total timeout: a slow-but-alive transfer is legitimate;
                # stalled sockets are caught by sock_read, cancels by /cancel.
                # Redirect limit: aiohttp's per-request default of 10 applies.
                timeout=aiohttp.ClientTimeout(
                    total=None, connect=15, sock_connect=15, sock_read=120
                ),
                connector=aiohttp.TCPConnector(
                    resolver=_SafeResolver(self._guard),
                    limit=max(8, self._settings.link_connections * 3),
                    limit_per_host=max(8, self._settings.link_connections * 2),
                    ttl_dns_cache=300,
                ),
            )
        return self._http

    async def aclose(self) -> None:
        if self._http and not self._http.closed:
            await self._http.close()

    # ── public API used by the engine & the /link handler ────────────────────

    async def probe(self, url: str) -> ProbeResult:
        """HEAD-ish probe (a 1-byte ranged GET) to learn name/size/ranges.

        Raises :class:`UrlRejected` for unusable links; transient transport
        errors propagate as regular aiohttp/OSError exceptions so the caller
        can retry. Rate limits (429 / 503 + Retry-After) are waited out here
        within a bounded budget — far cheaper than a whole-file engine retry.
        """
        url = validate_url(url, guard=self._guard)
        session = await self._session()
        rate_waited = 0.0
        attempt = 0
        while True:
            attempt += 1
            try:
                async with session.get(
                    url, headers={"Range": "bytes=0-0"}, allow_redirects=True
                ) as resp:
                    if resp.status in (403, 416):
                        # Some servers dislike ranged probes — retry plain.
                        resp.close()
                        async with session.get(url, allow_redirects=True) as plain:
                            return await self._probe_from_response(plain, url)
                    return await self._probe_from_response(resp, url)
            except _SsrfBlockedError as err:
                raise UrlRejected(str(err)) from err
            except _RateLimited as err:
                # A Retry-After that alone exceeds the whole patience budget
                # is hopeless — fail now instead of waiting out half of it.
                if err.retry_after and err.retry_after > _PROBE_RATE_BUDGET:
                    raise
                delay = err.retry_after or min(_PROBE_RATE_CAP, 5.0 * attempt)
                delay = min(delay, _PROBE_RATE_CAP)
                if rate_waited + delay > _PROBE_RATE_BUDGET or attempt > 10:
                    raise
                rate_waited += delay
                logger.info(
                    "probe rate limited (HTTP %d) — waiting %.0fs before trying %s again",
                    err.status, delay, strip_query(url),
                )
                await asyncio.sleep(delay)

    async def _probe_from_response(
        self, resp: aiohttp.ClientResponse, url: str
    ) -> ProbeResult:
        if resp.status == 206:
            total = _total_from_content_range(resp.headers.get("Content-Range", ""))
            await resp.read()  # the single probe byte
            ranges_ok = total is not None
        elif resp.status == 200:
            declared = resp.headers.get("Content-Length", "")
            total = int(declared) if declared.isdigit() else None
            resp.close()  # never read the whole body during a probe
            ranges_ok = False
        elif _is_rate_limit_status(resp):
            err = _rate_limited_error(resp)
            resp.close()
            raise err
        elif 400 <= resp.status < 500:
            resp.close()
            raise UrlRejected(f"the server answered HTTP {resp.status}")
        else:
            status, message = resp.status, resp.reason or ""
            request_info, history, headers = resp.request_info, resp.history, resp.headers
            resp.close()
            raise aiohttp.ClientResponseError(
                request_info, history, status=status, message=message, headers=headers
            )
        content_type = resp.headers.get("Content-Type")
        filename = filename_from_response(
            str(resp.url), resp.headers.get("Content-Disposition"), content_type
        )
        return ProbeResult(str(resp.url), filename, total, ranges_ok, content_type)

    async def download(self, job: DownloadJob, started: float) -> EngineResult:
        """Fetch ``job.file_id`` (the URL) to ``job.target_path``."""
        url = job.file_id
        cap = self._settings.link_max_size
        probe = await self.probe(url)
        if cap and probe.size and probe.size > cap:
            raise UrlRejected(
                f"{human_size(probe.size)} exceeds the LINK_MAX_SIZE cap "
                f"of {human_size(cap)}"
            )

        target = Path(job.target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        part_file = target.with_name(target.name + ".part")

        job.total_bytes = probe.size
        job.bytes_done = 0
        job.speed_bps = 0.0

        try:
            if (
                probe.accept_ranges
                and probe.size
                and probe.size >= _RANGED_MIN_BYTES
                and self._settings.link_connections > 1
            ):
                digest, written = await self._download_ranged(job, probe, part_file)
            else:
                digest, written = await self._download_single(job, probe, part_file)
            part_file.replace(target)  # atomic finish, same as Telegram paths
        except JobCancelled:
            raise
        except _SsrfBlockedError as err:
            # SSRF block raised mid-transfer (e.g. redirect to an internal
            # host) — non-retryable, like every other guard rejection.
            part_file.unlink(missing_ok=True)
            raise UrlRejected(str(err)) from err
        except UrlRejected:
            part_file.unlink(missing_ok=True)
            raise
        except OSError as err:
            part_file.unlink(missing_ok=True)
            if err.errno == errno.ENOSPC:
                raise disk_full_error(err) from err
            raise
        except BaseException:
            part_file.unlink(missing_ok=True)
            raise

        duration_ms = int((time.monotonic() - started) * 1000)
        avg_speed = written / (duration_ms / 1000) if duration_ms else 0.0
        return EngineResult(
            sha256=digest.hexdigest(),
            bytes_written=written,
            duration_ms=duration_ms,
            avg_speed=avg_speed,
        )

    # ── single-stream path ───────────────────────────────────────────────────

    async def _download_single(
        self, job: DownloadJob, probe: ProbeResult, part_file: Path
    ) -> tuple[hashlib._Hash, int]:
        session = await self._session()
        cap = self._settings.link_max_size
        digest = hashlib.sha256()
        written = 0
        resp = await self._get_with_rate_patience(session, probe.final_url, job)
        async with resp:
            await self._check_response(resp)
            declared = resp.headers.get("Content-Length", "")
            if declared.isdigit():
                job.total_bytes = int(declared)
                if cap and int(declared) > cap:
                    raise UrlRejected(
                        f"{human_size(int(declared))} exceeds the LINK_MAX_SIZE cap "
                        f"of {human_size(cap)}"
                    )
            with part_file.open("wb") as fh:
                async for chunk in resp.content.iter_chunked(self._settings.chunk_size):
                    if job.is_cancelled():
                        raise JobCancelled(job.job_id)
                    if chunk:
                        fh.write(chunk)
                        digest.update(chunk)
                        written += len(chunk)
                        job.touch_progress(len(chunk))
                        if cap and written > cap:
                            raise UrlRejected(
                                f"the file grew past the LINK_MAX_SIZE cap of "
                                f"{human_size(cap)} mid-transfer"
                            )
        return digest, written

    # ── multi-connection ranged path ─────────────────────────────────────────

    async def _download_ranged(
        self, job: DownloadJob, probe: ProbeResult, part_file: Path
    ) -> tuple[hashlib._Hash, int]:
        session = await self._session()
        size = probe.size or 0
        parts = min(
            self._settings.link_connections,
            max(1, size // (1024 * 1024)),  # at least ~1 MB per part
        )
        bounds = self._split(size, parts)
        logger.info(
            "job #%s: downloading %s over %d parallel range connections",
            job.job_id, human_size(size), len(bounds),
        )
        loop = asyncio.get_running_loop()
        # Mutable per-part offsets so a throttled parallel run can finish
        # exactly the bytes it never wrote — never restarting from zero.
        progress = [[start] for start, _ in bounds]
        with part_file.open("w+b") as fh:
            # Preallocate so parallel pwrites land at their final offsets.
            await loop.run_in_executor(
                None, functools.partial(os.ftruncate, fh.fileno(), size)
            )
            rate_error: _RateLimited | None = None
            try:
                async with asyncio.TaskGroup() as tg:
                    for index, (start, end) in enumerate(bounds):
                        tg.create_task(
                            self._download_part(
                                session, probe.final_url, start, end, fh, job,
                                progress=progress[index],
                                first_delay=index * _PART_START_STAGGER,
                            )
                        )
            except* JobCancelled:
                raise JobCancelled(job.job_id) from None
            except* UrlRejected as eg:
                raise eg.exceptions[0] from None
            except* _RateLimited as eg:
                # The server throttled our parallel burst past all patience.
                # Many CDNs that refuse bursts happily serve one steady
                # stream — finish the missing bytes sequentially.
                rate_error = eg.exceptions[0]
            except* OSError as eg:
                raise eg.exceptions[0] from None
            except* (TimeoutError, aiohttp.ClientError) as eg:
                raise eg.exceptions[0] from None
            if rate_error is not None:
                logger.warning(
                    "job #%s: parallel ranges throttled (%s) — finishing the "
                    "remaining bytes over a single connection",
                    job.job_id, strip_query(str(rate_error)),
                )
                await self._sleep_cancellable(
                    job, min(rate_error.retry_after or _RATE_FALLBACK_COOLDOWN, 30.0)
                )
                await self._fill_ranges_sequentially(
                    session, job, probe.final_url, bounds, progress, fh
                )
        # The parts arrived out of order — hash the completed file in one
        # sequential pass (in a worker thread) for the receipt/ledger.
        digest = await asyncio.to_thread(self._hash_file, part_file)
        return digest, size

    async def _fill_ranges_sequentially(
        self,
        session: aiohttp.ClientSession,
        job: DownloadJob,
        url: str,
        bounds: list[tuple[int, int]],
        progress: list[list[int]],
        fh: object,
    ) -> None:
        """Finish the byte ranges a throttled parallel run left unwritten.

        One connection at a time, resuming each range from its tracked
        offset — the gentlest pattern a rate-limited server will tolerate
        (and the only one some per-account concurrency limits allow).
        """
        deadline = time.monotonic() + _RATE_LIMIT_BUDGET
        for index, (start, end) in enumerate(bounds):
            offset = progress[index][0]
            if offset >= end + 1:
                continue  # this range finished during the parallel phase
            logger.info(
                "job #%s: finishing range %d-%d sequentially from offset %d",
                job.job_id, start, end, offset,
            )
            await self._download_part(
                session, url, offset, end, fh, job,
                progress=progress[index], deadline=deadline,
            )

    async def _get_with_rate_patience(
        self, session: aiohttp.ClientSession, url: str, job: DownloadJob,
        headers: dict[str, str] | None = None,
    ) -> aiohttp.ClientResponse:
        """One GET, waiting out rate limits (bounded) instead of failing.

        HTTP 429 arrives as a normal response, not an exception — so this
        checks the status, closes the body unread and sleeps as asked.
        Returns the response UNENTERED — wrap it in ``async with``.
        """
        rate_waited = 0.0
        attempt = 0
        while True:
            attempt += 1
            resp = await session.get(
                url, headers=headers or {}, allow_redirects=True
            )
            if not _is_rate_limit_status(resp):
                return resp
            err = _rate_limited_error(resp)
            resp.close()
            delay = err.retry_after or _RATE_LIMIT_DELAYS[
                min(attempt - 1, len(_RATE_LIMIT_DELAYS) - 1)
            ]
            if rate_waited + delay > _RATE_LIMIT_BUDGET or attempt > 10:
                raise err
            rate_waited += delay
            logger.info(
                "job #%s: GET rate limited (HTTP %d) — waiting %.0fs",
                job.job_id, err.status, delay,
            )
            await self._sleep_cancellable(job, delay)

    async def _download_part(
        self,
        session: aiohttp.ClientSession,
        url: str,
        start: int,
        end: int,
        fh: object,
        job: DownloadJob,
        progress: list[int] | None = None,
        first_delay: float = 0.0,
        deadline: float | None = None,
    ) -> None:
        """Stream one byte range into the shared preallocated file.

        Retries itself from its current offset on transient errors, so one
        flaky connection never restarts the whole file. Rate limits are
        waited out with backoff (honoring Retry-After) up to a budget —
        only when that runs out does the error escape, and the caller then
        finishes the missing bytes sequentially instead of restarting.
        """
        loop = asyncio.get_running_loop()
        offset = start
        transient = 0  # index into _PART_RETRY_DELAYS
        rate_attempt = 0
        rate_waited = 0.0
        if first_delay:
            await self._sleep_cancellable(job, first_delay)
        while True:
            try:
                headers = {"Range": f"bytes={offset}-{end}"}
                async with session.get(url, headers=headers) as resp:
                    await self._check_response(resp, expect_206=True)
                    async for chunk in resp.content.iter_chunked(self._settings.chunk_size):
                        if job.is_cancelled():
                            raise JobCancelled(job.job_id)
                        if not chunk:
                            continue
                        # pwrite is positional → safe to share one fd across
                        # parallel parts (each in the executor's own thread).
                        await loop.run_in_executor(
                            None, functools.partial(os.pwrite, fh.fileno(), chunk, offset)  # type: ignore[attr-defined]
                        )
                        offset += len(chunk)
                        if progress is not None:
                            progress[0] = offset
                        job.touch_progress(len(chunk))
                if offset < end + 1:
                    raise aiohttp.ClientPayloadError(
                        f"range {start}-{end} ended early at {offset}"
                    )
                if progress is not None:
                    progress[0] = offset
                return
            except JobCancelled:
                raise
            except _SsrfBlockedError as err:
                raise UrlRejected(str(err)) from err
            except _RateLimited as err:
                if progress is not None:
                    progress[0] = offset
                delay = err.retry_after
                if delay is None:
                    delay = _RATE_LIMIT_DELAYS[
                        min(rate_attempt, len(_RATE_LIMIT_DELAYS) - 1)
                    ]
                rate_attempt += 1
                if rate_waited + delay > _RATE_LIMIT_BUDGET:
                    raise  # patience exhausted — caller goes sequential
                if deadline is not None and time.monotonic() + delay > deadline:
                    raise  # the sequential phase's shared budget is spent
                rate_waited += delay
                logger.info(
                    "job #%s: range %d-%d rate limited (HTTP %d) — waiting "
                    "%.0fs (waited %.0fs so far), resuming from offset %d",
                    job.job_id, start, end, err.status, delay, rate_waited, offset,
                )
                await self._sleep_cancellable(job, delay)
            except (TimeoutError, aiohttp.ClientError, OSError) as err:
                if progress is not None:
                    progress[0] = offset
                if transient >= len(_PART_RETRY_DELAYS):
                    raise
                delay = _PART_RETRY_DELAYS[transient]
                transient += 1
                logger.warning(
                    "job #%s: range %d-%d attempt %d failed (%s: %s) — resuming "
                    "from offset %d",
                    job.job_id, start, end, transient,
                    type(err).__name__, strip_query(str(err)), offset,
                )
                await self._sleep_cancellable(job, delay)

    @staticmethod
    async def _sleep_cancellable(job: DownloadJob, delay: float) -> None:
        """Sleep in slices so /cancel aborts a rate-limit wait immediately.

        (A plain ``asyncio.sleep(90)`` would park the job for the full
        window even after the user hit /cancel.)
        """
        deadline = time.monotonic() + max(0.0, delay)
        while True:
            if job.is_cancelled():
                raise JobCancelled(job.job_id)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 0.5))

    @staticmethod
    def _split(size: int, parts: int) -> list[tuple[int, int]]:
        """Split [0, size) into `parts` contiguous (start, end) bounds."""
        parts = max(1, min(parts, size))
        base, extra = divmod(size, parts)
        bounds: list[tuple[int, int]] = []
        offset = 0
        for index in range(parts):
            length = base + (1 if index < extra else 0)
            bounds.append((offset, offset + length - 1))
            offset += length
        return bounds

    @staticmethod
    async def _check_response(
        resp: aiohttp.ClientResponse, expect_206: bool = False
    ) -> None:
        if expect_206 and resp.status != 206:
            if _is_rate_limit_status(resp):
                # The probe said ranges work, but the server is now telling
                # us to slow down — wait it out, don't restart the file.
                raise _rate_limited_error(resp)
            # The probe said ranges work — the server changed its mind
            # mid-download; surface it so the engine retries from scratch.
            raise aiohttp.ClientResponseError(
                resp.request_info,
                resp.history,
                status=resp.status,
                message=f"expected HTTP 206 for a range request, got {resp.status}",
                headers=resp.headers,
            )
        if _is_rate_limit_status(resp):
            raise _rate_limited_error(resp)
        if 400 <= resp.status < 500:
            raise UrlRejected(f"the server answered HTTP {resp.status}")
        if resp.status >= 400:
            raise aiohttp.ClientResponseError(
                resp.request_info,
                resp.history,
                status=resp.status,
                message=resp.reason or "",
                headers=resp.headers,
            )

    @staticmethod
    def _hash_file(path: Path) -> hashlib._Hash:
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            while True:
                chunk = fh.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest


def disk_full_error(err: OSError) -> DownloadError:
    """ENOSPC is non-retryable — make the remedy explicit (shared wording)."""
    return DownloadError(
        f"{err} — the downloads volume is out of free space. A file needs "
        "roughly its own size free on the downloads disk on top of the "
        "Bot API server's cache; free space (also check 'docker system df' "
        "for the botapi-data volume) or move DOWNLOAD_DIR to a bigger disk"
    )
