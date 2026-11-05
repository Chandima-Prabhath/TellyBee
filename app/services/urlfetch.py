"""Direct HTTP(S) link downloads — the fast lane next to Telegram files.

Activated with ``/link <url>`` or by sending a plain message that contains
an http(s) URL (see ``app/handlers/links.py``; toggle with ``LINKS_ENABLED``).

Design goals:

* **Fast** — servers that support byte ranges get the file over several
  parallel connections (``LINK_CONNECTIONS``, default 4), which typically
  saturates the host's bandwidth where a single stream would not. Servers
  without range support silently fall back to a single streamed connection.
* **Safe** — an SSRF guard inside the connector's resolver refuses to
  connect to private / loopback / link-local / reserved addresses (so a
  crafted link can never reach the host's internal network or the docker
  network's other services), only http/https schemes are allowed, and URLs
  with embedded credentials are rejected outright.
* **Bounded** — ``LINK_MAX_SIZE`` caps the accepted size (when the server
  declares it), unknown sizes are enforced mid-transfer, and the queue's
  disk-headroom guard applies exactly like for Telegram files.
* **Robust** — every ranged part resumes itself from its current offset on
  transient errors; the outer engine retry machinery treats the whole file
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
from dataclasses import dataclass
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
#: Per-part retry delays (seconds) before the outer engine retry takes over.
_PART_RETRY_DELAYS = (1.0, 3.0)


class UrlRejected(Exception):
    """Non-retryable, user-readable rejection of a link (bad URL, HTTP 4xx…)."""


#: Raised by the SSRF guard inside the connector's resolver. Subclasses
#: ValueError so it slips through aiohttp's OSError-wrapping untouched, and
#: is converted to a non-retryable UrlRejected at every call site.
_SSRF_MESSAGE = (
    "refusing to connect: the host resolves to a private or otherwise "
    "blocked address (SSRF guard)"
)


class _SsrfBlockedError(ValueError):
    pass


# ── URL validation & filenames ──────────────────────────────────────────────


def validate_url(url: str) -> str:
    """Basic URL sanity for link downloads. Returns the cleaned URL.

    Raises :class:`UrlRejected` with a chat-friendly reason otherwise.
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
        if not _allowed_ip(ip):
            raise UrlRejected(_SSRF_MESSAGE)
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
    """True only for addresses that are unambiguously public."""
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


class _SafeResolver(aiohttp.abc.AbstractResolver):
    """DNS resolver that refuses private/resolved-to-private targets.

    aiohttp asks the connector's resolver for every connection attempt —
    including redirects — so hooking here blocks SSRF attempts (and DNS
    rebinding to internal addresses) at connect time, with no TOCTOU gap.
    """

    def __init__(self) -> None:
        self._inner = aiohttp.DefaultResolver()

    async def resolve(  # type: ignore[override]
        self, host: str, port: int = 0, family: int = socket.AF_INET
    ) -> list[dict]:
        entries = await self._inner.resolve(host, port, family)
        for entry in entries:
            try:
                ip = ipaddress.ip_address(entry["hostname"])
            except ValueError:
                raise _SsrfBlockedError("could not interpret the resolved address") from None
            if not _allowed_ip(ip):
                raise _SsrfBlockedError(_SSRF_MESSAGE)
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
                    resolver=_SafeResolver(),
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
        can retry.
        """
        url = validate_url(url)
        session = await self._session()
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
            raise UrlRejected(_SSRF_MESSAGE) from err

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
        elif 400 <= resp.status < 500 and resp.status != 429:
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
            raise UrlRejected(_SSRF_MESSAGE) from err
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
        async with session.get(probe.final_url, allow_redirects=True) as resp:
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
        with part_file.open("w+b") as fh:
            # Preallocate so parallel pwrites land at their final offsets.
            await loop.run_in_executor(
                None, functools.partial(os.ftruncate, fh.fileno(), size)
            )
            try:
                async with asyncio.TaskGroup() as tg:
                    for start, end in bounds:
                        tg.create_task(
                            self._download_part(session, probe.final_url, start, end, fh, job)
                        )
            except* JobCancelled:
                raise JobCancelled(job.job_id) from None
            except* UrlRejected as eg:
                raise eg.exceptions[0] from None
            except* OSError as eg:
                raise eg.exceptions[0] from None
            except* (TimeoutError, aiohttp.ClientError) as eg:
                raise eg.exceptions[0] from None
        # The parts arrived out of order — hash the completed file in one
        # sequential pass (in a worker thread) for the receipt/ledger.
        digest = await asyncio.to_thread(self._hash_file, part_file)
        return digest, size

    async def _download_part(
        self,
        session: aiohttp.ClientSession,
        url: str,
        start: int,
        end: int,
        fh: object,
        job: DownloadJob,
    ) -> None:
        """Stream one byte range into the shared preallocated file.

        Retries itself from its current offset on transient errors, so one
        flaky connection never restarts the whole file.
        """
        loop = asyncio.get_running_loop()
        offset = start
        last_error: Exception | None = None
        for attempt, delay in enumerate((0.0, *_PART_RETRY_DELAYS)):
            if delay:
                await asyncio.sleep(delay)
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
                        job.touch_progress(len(chunk))
                if offset < end + 1:
                    raise aiohttp.ClientPayloadError(
                        f"range {start}-{end} ended early at {offset}"
                    )
                return
            except JobCancelled:
                raise
            except _SsrfBlockedError as err:
                raise UrlRejected(_SSRF_MESSAGE) from err
            except (TimeoutError, aiohttp.ClientError, OSError) as err:
                last_error = err
                logger.warning(
                    "job #%s: range %d-%d attempt %d failed (%s: %s) — resuming "
                    "from offset %d",
                    job.job_id, start, end, attempt + 1,
                    type(err).__name__, strip_query(str(err)), offset,
                )
        assert last_error is not None
        raise last_error

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
            # The probe said ranges work — the server changed its mind
            # mid-download; surface it so the engine retries from scratch.
            raise aiohttp.ClientResponseError(
                resp.request_info,
                resp.history,
                status=resp.status,
                message=f"expected HTTP 206 for a range request, got {resp.status}",
                headers=resp.headers,
            )
        if 400 <= resp.status < 500 and resp.status != 429:
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
