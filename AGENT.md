# AGENT.md — TellyBee engineering guide for AI coding agents

> **Read this file completely before touching any code.** It encodes the
> project's architecture, conventions and hard-won invariants. When this
> file and your instincts disagree, this file wins. Update it whenever you
> change behavior that contradicts it.

## What this project is

TellyBee is a Telegram **file/media downloader bot**: users send or forward
files; the bot downloads them to the server with live progress, access
control, a durable SQLite ledger, and full queue observability. It must stay
**modern, fast, stable** — in that order of priority for feature decisions:
stability beats speed, speed beats cleverness.

- **Stack**: Python 3.12+, aiogram 3.31+,
  aiohttp, aiosqlite, pydantic v2 + pydantic-settings.
- **No ORM** — plain SQL in `app/db.py`. Do not introduce one.
- **Single process** — the queue is in-process `asyncio`. Do not add
  Celery/Redis unless the user explicitly asks.

## Golden rules

1. **Never block the event loop.** All I/O in request paths is async
   (aiogram, aiohttp, aiosqlite). Small sync file writes inside the
   download engine are acceptable (256KB buffered writes ≪ 1ms); anything
   bigger must go through `asyncio.to_thread`.
2. **One failure must never cascade.** Every job runs through
   `DownloadManager._run_isolated` which converts *any* exception into a
   FAILED job. Never let an exception escape a worker or a middleware.
3. **Atomic writes only.** Partial files are always `<name>.part`, renamed
   into place on success. Never write directly to the final path.
4. **Escape user text before sending.** All chat output uses
   `parse_mode=HTML`; every user-controlled string (file names, usernames)
   must pass through `app.utils.formatting.escape()`.
5. **Path safety is non-negotiable.** Anything derived from users passes
   through `sanitize_segment` / `sanitize_filename` and stays inside
   `settings.download_dir`. Template tokens are whitelisted in
   `config.TEMPLATE_TOKENS`.
6. **The token never leaks.** No logs, no `/config` output, no exceptions
   may contain `bot_token`. `Settings.summary()` is the canonical safe
   renderer.
7. **DB writes are best-effort on the hot path.** Queue logic never fails
   because SQLite hiccuped — `_db_insert`/`_db_update` swallow and log.
   (Final states should still be recorded; correctness of `/stats` matters.)

## Project map

```
app/
├── main.py               entrypoint: wiring, signals, graceful shutdown
├── config.py             ALL settings (pydantic-settings) + validators
├── bot_factory.py        Bot/Dispatcher construction, middleware order, command menu
├── db.py                 SQLite layer: users (ACL) + downloads (ledger)
├── logging_setup.py      text/json logging
├── queue/
│   ├── jobs.py           DownloadJob dataclass + JobStatus + progress math
│   ├── engine.py         streaming downloader: retries, hashing, .part→rename
│   └── manager.py        priority queue, workers, pause/resume/cancel, snapshots
├── middleware/
│   ├── throttling.py     per-user sliding window (albums exempt)
│   ├── auth.py           admin/user tier resolution + denial replies
│   └── album.py          media-group buffering → data["album"]
├── handlers/
│   ├── common.py         /start /help /status /queue /cancel
│   ├── admin.py          /stats /users /allow /disallow /config /pause /resume /health
│   └── media.py          media intake → jobs (type filters, size limits)
├── services/
│   ├── media_types.py    Message → MediaInfo normalization + extension guessing
│   ├── filenaming.py     template resolution, sanitization, collision handling
│   └── notifier.py       progress messages, receipts, album grouping
├── web/health.py         /healthz /readyz + webhook mounting
└── utils/                sizes (parse/format) and formatting (bars, escape)
tests/                    pytest suite (mirrors the modules above)
```

## Middleware & router order (DO NOT reorder casually)

Outer middleware execution order = registration order in
`build_dispatcher`:

1. `ThrottlingMiddleware` — drops floods before anything expensive.
2. `AuthMiddleware` — resolves `data["user_role"]`; denies strangers.
3. `AlbumMiddleware` — buffers media-group updates, injects
   `data["album"]: list[Message]`, calls the handler **once** per album
   (with the *first* message as the event). Non-album messages get
   `album == [message]`.

Router inclusion order in `app/handlers/__init__.py`: `common` → `admin`
(guarded by `IsAdmin` router-level filter) → `links` (`/link` + URL
auto-detect on `F.text`; must precede the media catch-all so links in
plain text are consumed there) → `media` (broad media filter + catch-all
fallback). Command handlers must stay before the media catch-all.

Handler dependency injection: the `Dispatcher(settings=…, db=…, manager=…)`
workflow data is injected by parameter name — handlers declare
`manager: DownloadManager`, `db: Database`, `settings: Settings`,
`user_role: str`.

## Queue mechanics you must preserve

- `submit()` counts `len(self._pending)` (not raw `qsize()`) against
  `MAX_QUEUE_SIZE` — a worker may briefly hold a "parked" entry during
  pause, and this check must stay immune to that window.
- Workers park dequeued entries back into the queue when paused *after*
  dequeue; ordering survives because entries are `(priority, seq, job)`
  tuples.
- Shutdown: set `_stopping`, `resume()`, drain pending (mark CANCELLED),
  push one `_STOP` sentinel per worker, wait the grace timeout, then
  force-cancel stragglers. Idle workers exit on the sentinel.
- Cancellation: pending → `_cancel_flags` set **and `cancel_event` tripped**
  (aborts any in-flight prefetch for that queued job), finalized when
  dequeued; running → `job.cancel_event`, the engine aborts at the next
  chunk boundary (or the next 1s poll while parked in `getFile`) and raises
  `JobCancelled`.
- `submit()` also schedules a **prefetch task** (`engine.prefetch(job)`, local
  mode only, semaphore-bounded by `PREFETCH_SLOTS`) — the task races the
  job's cancel event and is cancelled wholesale at shutdown, BEFORE the
  workers drain, so the server stops being asked for new files first.
- The notifier groups jobs by `album_key or f"job:{job_id}"`; a group owns
  at most one chat message, created lazily only when
  `expected_bytes >= PROGRESS_MIN_BYTES` **and a job is running or fetching
  (waiting in `getFile` — with prefetch that happens while still QUEUED)**,
  edited at most once per `PROGRESS_EDIT_INTERVAL`, finalized into exactly
  one receipt.

## Adding things (checklists)

**A new media type** (e.g. a future Telegram payload):
1. Add the kind name to `SUPPORTED_MEDIA_TYPES` in `config.py` (this
   automatically extends ALLOWED/BLOCKED validation and `/help`).
2. Extend `extract_media` in `services/media_types.py` + default extension
   in `_DEFAULT_EXT` + an icon in `services/notifier.py::_TYPE_ICONS`.
3. Add the filter clause in `handlers/media.py::_MEDIA_FILTER`.
4. Add a construction test in `tests/test_media_types.py`.

**A new setting**: add the field + validator to `config.py`, a row in the
README config table, a line in `.env.example`, and (if it affects behavior
worth asserting) a test in `tests/test_config.py`. Settings must fail fast
with human-readable messages — never coerce silently.

**A new admin command**: handler in `handlers/admin.py` (router is already
admin-gated), register in `_ADMIN_COMMANDS` in `bot_factory.py`, document
in README's command table.

**A new table/column**: bump the schema in `db.py::_SCHEMA`
(`CREATE TABLE IF NOT EXISTS` style — no migration framework on purpose;
additive `ALTER TABLE` guards are acceptable for a new column).

## Commands agents should run

```bash
uv sync                      # install everything (dev included)
uv run ruff check app tests  # MUST pass with zero findings
uv run pytest                # MUST pass fully before any commit
uv run python -m app.main --check   # validate config wiring without a token
```

Testing philosophy: unit-test pure logic (naming, parsing, formatting,
config validation, queue mechanics with `StubEngine` + `NullNotifier`).
No test may touch the network or require a real bot token. aiogram type
objects (Message, PhotoSize, …) are constructed directly as pydantic
models — see `tests/test_media_types.py` for the pattern.

## Gotchas learned the hard way

- **aiogram `Bot(token, session, default)`** — there is no `base=` kwarg;
  a custom Bot API server goes through
  `AiohttpSession(api=TelegramAPIServer.from_base(...))`.
- **Local Bot API servers have NO HTTP file endpoint.** Verified against
  10.3 sources: `HttpConnection::handle` only routes `/bot<token>/<method>`;
  `/file/...` always 404s. In `--local` mode `getFile` returns an *absolute
  path* inside the server's storage, so the engine stream-copies from the
  shared `botapi-data` volume (mounted read-only at the same path). HTTP
  fetching stays only for the cloud API.
- **The local Bot API server writes files as its own uid with owner-only
  permissions.** Verified against upstream sources: the server `setuid()`s
  itself via `--username` (`td::change_user` → `getpwnam`, *names only, no
  numeric uids*; the aiogram image hardcodes `telegram-bot-api`, uid 101)
  and TDLib creates downloaded files `0600` (`FileFd::open` default mode).
  tellybee runs as uid 1000, so the compose example overrides the botapi
  `entrypoint` to register a uid-1000 user, `chown -R` the volume, and run
  the server as the same uid (the stock entrypoint's own chown needs root,
  its `--username` is hardcoded, and `exec $COMMAND` drops extra
  `command:` args). Engine-side, `PermissionError` on the volume becomes a
  non-retryable `DownloadError` carrying the uid-alignment remedy — EACCES
  never fixes itself, never add it to the retryable set.
- Download URL (cloud mode) = `bot.session.api.file_url(bot.token,
  file.file_path)` — honors custom API bases automatically. `getFile`
  responses are cached (`_FileCache`, TTL 15 min) because Telegram
  rate-limits that call.
- **Local `getFile` BLOCKS until the server has downloaded the whole file.**
  Verified against 10.3 sources: `Client::do_get_file` parks the HTTP query
  in `file_download_listeners_` and answers it only from
  `on_update_file(...is_downloading_completed_...)`. aiogram's session
  default timeout is 60s (`DEFAULT_TIMEOUT`), which killed every first-time
  download of a large file with `TelegramNetworkError: Request timeout
  error` (0.1.4 production incident). Fix: per-request override —
  `bot.get_file(file_id, request_timeout=round(settings.getfile_timeout))`
  in local mode only (cloud `getFile` is a quick metadata call). A *timeout*
  re-issues `getFile` (up to a 24h budget) WITHOUT burning the job's retry
  budget — the server attaches the new query to the same in-flight
  download. The wait is raced against the job's cancel event
  (`_race_cancel`, 1s poll) so `/cancel` stays responsive, and
  `job.waiting_since` drives the notifier's "fetching from Telegram" phase.
- **TDLib's per-file download throughput is resource-capped, not
  priority-capped.** Verified against TDLib sources:
  `FileDownloadManager` keeps `max_download_resource_limit_ = 1 << 21`
  (**2 MiB in-flight per (DC, size-class) pool**) and multiplies it **×8**
  only when the TDLib option `session_count` > 1 (or premium — impossible
  for bots). `do_get_file` uses `downloadFile(priority=1)` but priority
  only orders *competing* files; a single file already gets its whole
  per-DC pool. Consequences: (a) many files on one DC split the same pool
  (prefetching helps wall-clock only via pipelining + cross-DC batches);
  (b) the only single-file speed lever is `session_count`, which bots
  cannot set via the Bot API — hence the optional patched-server build in
  `deploy/botapi-fast/Dockerfile` (injects `setOption("session_count", N)`
  before `setTdlibParameters`, where TDLib requires it to be set).
- **getFile requests are shared per file_id** (`_fetch_task` /
  `_inflight`): the prefetcher and any workers awaiting the same file join
  ONE task; never cancel that task from a waiter — each waiter races its
  own job's cancel event instead (`_race_cancel`). Cancellation of one job
  must not kill the server-side fetch another job still needs.
- **The fetch-progress watcher never logs paths under the server volume** —
  `<LOCAL_FILES_DIR>/<bot-token>/…` embeds the token. Only sizes/speeds are
  safe to log. Matching jobs to growing files is by expected size
  (sticky-first, largest-expected-first); files bigger than expected are
  completed cache entries, never the match.
- **URL jobs reuse the Telegram job shape**: `media_type="url"`,
  `file_id=<final URL>`, `file_unique_id=sha256(final_url)[:32]`. The URL
  downloader (`app/services/urlfetch.py`) runs INSIDE the engine's normal
  retry/backoff/atomic-rename machinery — only `UrlRejected` (bad URL,
  HTTP 4xx, size cap, SSRF block) short-circuits to a non-retryable
  `DownloadError`. SSRF protection is DUAL-LAYER: aiohttp's connector
  *skips the resolver for IP-literal hosts*, so `validate_url` checks those
  directly, while `_SafeResolver` guards DNS names at connect time
  (covers redirects + rebinding, no TOCTOU gap). CRITICAL aiohttp trap:
  resolver entries hold the address under `host` and the queried NAME under
  `hostname` since aiohttp 3.10 (TypedDict since 3.11; ≤3.9 dicts kept the
  address under `hostname`) — ALWAYS extract via `_resolved_ip`, never
  read a field directly: v0.3.0 read `entry["hostname"]` and thereby
  rejected EVERY hostname link with a bogus "private address" message.
  Guard mode is `LINK_SSRF_GUARD` (strict/basic/off, default basic),
  threaded into both layers via `_SsrfGuard`. Error text shown to users
  is query-string-scrubbed (`strip_query`) because signed URLs carry
  secrets in the query.
- **Ranged multi-connection downloads write through one shared fd** with
  positional `os.pwrite` (executor) into a preallocated `.part` — parts are
  safe to run concurrently, each resuming itself from its current offset.
  SHA-256 for ranged downloads is computed by one sequential re-read after
  the parts finish (parts arrive out of order; digests cannot be merged).
- **Rate limits (429 / 503+Retry-After) are waited out at every layer, in
  `urlfetch.py`** — probe loop, per ranged part (backoff honors
  `Retry-After`, capped at 90s; cumulative budget 90s), and single-stream
  GETs. `_RateLimited` subclasses `aiohttp.ClientResponseError`, so an
  exhausted one still reaches the engine's transient retry (whose backoff
  also honors `Retry-After`), and the engine's FINAL failure message for
  429/503 is plain language with remedies instead of aiohttp's opaque text.
  A throttled parallel run tracks per-part offsets and finishes the MISSING
  bytes sequentially over one connection (`_fill_ranges_sequentially`) —
  never restarts the file from zero. Parts start staggered
  (`_PART_START_STAGGER`, 0.25s) because probe+4 simultaneous GETs is the
  burst that per-second limiters refuse (real seedr case, v0.3.2). HTTP 429
  arrives as a normal response, NOT an exception — check
  `_is_rate_limit_status` after `session.get`, before reading the body.
- **The 2GB figure is an UPLOAD cap, not a download cap.** Verified against
  10.3 sources: `MAX_DOWNLOAD_FILE_SIZE = 20MB` and BOTH download-size
  checks are guarded by `!parameters_->local_mode_` — in local mode the
  server enforces NO download limit (the official README says exactly
  this). The real ceiling is Telegram's own per-file maximum of 4GB
  (Premium uploads; free users cap at 2GB). TellyBee's `MAX_FILE_SIZE`
  validation is mode-aware accordingly: ≤ 4000MB with `API_BASE` set,
  ≤ 20MB otherwise (fail fast on misconfig).
- **`file_unique_id` is the duplicate-detection key** — stable across
  re-sends, forwards and re-uploads of the same Telegram file. Intake skips
  files whose latest completed ledger row still exists on disk (deleted
  files re-download). "Already in the queue" comes from the manager's live
  registry, not the DB — the ledger row is only written at submit.
- **aiogram 3.7+ error handlers receive one `ErrorEvent`**
  (`event.update` / `event.exception`) — a `(event, exception)` signature
  crashes with `TypeError` at raise time.
- **aiogram's `CommandObject` exposes `.args`**, not `.argument`.
- **The bot token appears in Bot API URLs** — exception strings then leak it
  into logs and chat receipts. `logging_setup.SecretScrubbingFormatter`
  masks it at the sink, and the engine masks job error texts; keep both.
- **`__slots__` classes reject new attributes** — `setattr(obj, '_x', ...)`
  on a slotted instance raises `AttributeError`; declare the slot.
- **`parse_size` suffix matching must be longest-first** ("20mb" ends with
  "b", too) — it now uses an anchored regex; keep it that way.
- Telegram rate-limits message edits; the notifier's throttling
  (`PROGRESS_EDIT_INTERVAL`, default 2s) plus `RetryAfter` handling exists
  for a reason. Don't "optimize" it away.
- Albums arrive as N separate updates sharing `media_group_id`; without
  `AlbumMiddleware` you would download N times and spam N receipts.
- `asyncio.PriorityQueue` entries are compared as tuples — always include
  the monotonic `seq` tiebreaker or you'll get comparison errors between
  equal jobs.
- SQLite runs in WAL mode via aiosqlite's worker thread — fine for this
  write volume; do not wrap it in an extra lock.
- Python's `dataclass(slots=True)` requires non-default fields before
  defaulted ones — keep `DownloadJob` field order in mind when extending.

## Definition of done for any change

1. `ruff check app tests` clean.
2. `pytest` fully green, with new coverage for the changed logic.
3. README table / AGENT.md updated if behavior or config surface changed.
4. `.env.example` updated for any new setting.
5. No secrets, tokens, or user file names in logs.
