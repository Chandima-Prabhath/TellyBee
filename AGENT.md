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
                         + progress_windows (crash cleanup) + user_prefs (/folder)
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
│   ├── common.py         /start /help /folder /status /queue /cancel
│   ├── admin.py          /stats /users /allow /disallow /config /pause /resume /health
│   ├── callbacks.py      every inline button (window remote control, /folder picker, Re-queue)
│   ├── links.py          /link + URL auto-detect → jobs (probe, SSRF guard, caps)
│   └── media.py          media intake → jobs (type filters, size limits, /folder-aware paths)
├── services/
│   ├── media_types.py    Message → MediaInfo normalization + extension guessing
│   ├── filenaming.py     template resolution, sanitization, collision handling, /folder override
│   └── notifier.py       the unified window (anchored), receipts, album grouping
├── web/health.py         /healthz /readyz + webhook mounting
└── utils/                sizes (parse/format) and formatting (bars, escape)
tests/                    focused pytest suite — pure logic + the two UX
                          contracts (folder prefs, window anchoring) — see
                          "Commands agents should run" for the philosophy
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
  mode only) — the task races the job's cancel event and is cancelled
  wholesale at shutdown, BEFORE the workers drain, so the server stops being
  asked for new files first. Prefetch itself no longer owns a semaphore:
  the fetch task's `_fetch_with_reissue` body acquires the engine's ONE
  `FETCH_CONCURRENCY` semaphore, so workers' fetches and prefetch compete
  for the same slots (v0.3.2 let 3 workers + 6 prefetch slots fetch at once
  and every fetch crawled at ~0 B/s). A prefetch whose fetch is still
  parked on the semaphore leaves `waiting_since` unset — no "preparing"
  message, no fetch progress — until its turn actually comes.
- The notifier (0.5) gives each chat ONE live "window" message. Jobs are
  tracked per chat in `_Window.live`; `attach()` kicks a debounced (re)creation
  (`_WINDOW_DEBOUNCE`, sliding ~0.9 s — a burst of forwards coalesces into ONE
  replacement), and `_recreate()` deletes the old message BEFORE sending the
  fresh one. **Bottom anchoring (v0.5): every delivered receipt and every
  button reply calls `_reanchor(chat_id)`**, which re-kicks the debounced
  recreate so the window is re-sent below the bot's latest message — the
  window must stay the chat's NEWEST message while live work remains. When
  `live` is empty `_reanchor` is a no-op: the last receipt is the correct
  bottom message and the updater closes the window instead.
  The window's message lifecycle has a SINGLE writer: the per-window updater task
  (`_updater`) — `attach`/`finish`/`_reanchor` only mutate `live`, session counters and
  `kick_at` (plain atomic writes, no locks). When the chat's `live` drains,
  the updater deletes the window (`_close`) and pops the chat; receipts are
  ALWAYS fresh sends (`_send_receipt`, one bounded `Retry-After` retry)
  because the window is transient. Window message ids are persisted in the
  `progress_windows` table so `notifier.startup()` can delete orphans a crash
  left behind, and `aclose()` sweeps live windows on clean shutdown. Routine
  window edits go through `_refresh`: per-window `PROGRESS_EDIT_INTERVAL`
  plus the global pacer (one edit in flight, ~1/s across ALL chats — routine
  refreshes dropped, never queued). `PROGRESS_MIN_BYTES` is GONE — tiny files
  simply finish before the debounce fires and never produce a window.
- **User-facing text hides the machine room (v0.5).** The two transport
  phases of local Bot API mode (server-side fetch vs local copy) are an
  implementation detail: the window/queue render ONE bar + meta line for
  both, receipts just show a speed. Never emit "prefetch", "Telegram
  fetch", "fetched ahead", "copy was instant" or similar into chat text;
  `tests/test_window_ux.py` asserts the jargon stays out.
- Honest receipt speeds (v0.3.3, quiet since v0.5): when a job's server-side
  fetch finished before a worker picked it up (`fetched_ahead`, set by
  `prefetch()` on success or by the `_attempt` cache-hit check), `_finish`
  reports `job.fetch_bps` (fetch_total / waited, captured in `_end_fetch_wait`)
  as `avg_speed` instead of the instant disk-copy rate — the receipt shows
  the number, never a misleading "250 MB/s", and never an explanation of
  where it came from.

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

**A new user command**: handler in `handlers/common.py` (the common router
is registered first, so commands always beat the media catch-all), register
in `_USER_COMMANDS` in `bot_factory.py`, document in README's command table.
If it needs buttons, prefix callback data with `tb:` and dispatch it in
`handlers/callbacks.py::on_window_button` — every branch MUST answer the
callback query (see Gotchas).

**A new table/column**: bump the schema in `db.py::_SCHEMA`
(`CREATE TABLE IF NOT EXISTS` style — no migration framework on purpose;
additive `ALTER TABLE` guards are acceptable for a new column).
Existing tables get the new sibling automatically on `connect()` because
the whole script is `IF NOT EXISTS` (that is how `user_prefs` shipped).

## Commands agents should run

```bash
uv sync                      # install everything (dev included)
uv run ruff check app tests  # MUST pass with zero findings
uv run pytest                # MUST pass fully before any commit
uv run python -m app.main --check   # validate config wiring without a token
```

Testing philosophy (trimmed in v0.5 from ~4800 lines of mock-heavy suites
  to a focused core): keep fast, pure-logic tests (naming, parsing,
  formatting, config validation, path safety, media extraction) plus slim
  contract tests with plain fakes for the UX invariants users feel
  (`tests/test_folder_prefs.py`, `tests/test_window_ux.py`). No test may
  touch the network or require a real bot token; no deep-mocked bot
  frameworks. aiogram type objects (Message, PhotoSize, …) are constructed
  directly as pydantic models — see `tests/test_media_types.py` for the
  pattern.

## Gotchas learned the hard way

- **Chat text is Telegram's restricted HTML, not the web's.** Allowed tags:
`<b> <i> <u> <s> <a> <code> <pre> <blockquote> <blockquote expandable>
<tg-spoiler> <tg-emoji>`. There is **no `<br>`** — newlines are literal.
File lists belong in `<blockquote>` blocks, long ones in
`<blockquote expandable>` so Telegram collapses them; pair every opening
tag with its closing `</…>` (unbalanced tags make Telegram reject the whole
message). And every user-controlled string still goes through `escape()`
before any of the tags wrap it.
- **aiogram `Bot(token, session, default)`** — there is no `base=` kwarg;
  a custom Bot API server goes through
  `AiohttpSession(api=TelegramAPIServer.from_base(...))`.
- **Local Bot API servers have NO HTTP file endpoint.** Verified against
  10.3 sources: `HttpConnection::handle` only routes `/bot<token>/<method>`;
  `/file/...` always 404s. In `--local` mode `getFile` returns an *absolute
  path* inside the server's storage, so the engine stream-copies from the
  shared `botapi-data` volume (mounted read-write at the same path since
  v0.7 — the cache cleaner deletes served files; `:ro` still *reads* fine
  but cleanup disables itself). HTTP fetching stays only for the cloud API.
- **The server's cache retention is 1–25 h and NOT configurable**
  (tdlib/telegram-bot-api#303, maintainer's own words), and manual deletion
  of downloaded files is explicitly allowed. `app/services.botapi_cache`
  therefore deletes each server-side copy right after the copy to
  `./downloads` completes (plus the file_id→path cache entry, so waiters
  never receive a ghost path) and sweeps leftovers older than
  `BOTAPI_CACHE_TTL_MINUTES` — but only inside the bot's own `files/`
  subtree (learned from observed getFile paths; the server's TDLib
  database/binlogs elsewhere in the volume are never touched), only
  entries unchanged across two sweeps (a file that still grows is an
  active download, whatever bookkeeping says), and never a path the engine
  declared protected (in-flight watcher matches / live getFile mappings).
  Log lines must never contain volume paths — they embed the bot token.
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
- **TDLib's ~2 MiB in-flight window per DC is shared by ALL simultaneous
  downloads** — N concurrent server-side fetches each get ~1/N of it. The
  v0.3.2 defaults (3 workers + `PREFETCH_SLOTS=6`) let a busy queue run
  nine fetches at once and every one crawled at ~0–200 B/s while the bot
  looked stuck (real 35-episode batch case). Since 0.3.3 the ONE
  `FETCH_CONCURRENCY` semaphore (default 2) is acquired by the shared
  fetch task's body (`_fetch_with_reissue`) — workers' getFile waits and
  prefetch compete for the same slots; prefetch() itself is unbounded
  because its parked fetch tasks hold nothing.
- **Telegram tolerates ~1 message edit per second per chat sustained.**
  Per-group throttling alone under-counts: N live progress messages × 1
  edit / `PROGRESS_EDIT_INTERVAL` is N× the budget → 429 flood-waits → the
  old code DROPPED the failing edit, and for a receipt that meant the
  receipt vanished silently ("bot updates stuck"). 0.3.3 added droppable
  routine edits + urgent receipts; 0.4.0 removes the contention at the
  root: ONE window message per chat means one edit per interval, and the
  global pacer (`_EDIT_MIN_GAP`) plus the drop rule remain as a safety net
  across many busy chats. Window (re)creation NEVER sleeps out a
  `Retry-After` — it parks `kick_at` (bounded by `_WINDOW_FLOOD_PARK`)
  and retries on a later tick, keeping the updater responsive.
- **A prefetched file's receipt must not show its disk-copy speed.** When
  the server-side fetch finished while the job was still queued, the
  worker only stream-copies locally (<1 s) — `avg_speed` then reports
  ~250 MB/s of volume speed and users read it as a lie. Track
  `fetched_ahead` (prefetch success, or the `_attempt` cache-hit check)
  plus `fetch_bps` (`fetch_total / waited`, captured in `_end_fetch_wait`
  from the fetch's TRUE start time) and prefer them in `_finish` and the
  receipt ("⚡ … from Telegram — fetched ahead of the queue, copy was
  instant").
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
- **aiogram models are FROZEN pydantic models** (`model_config.frozen=True`)
  — you cannot monkey-patch `callback.answer` / `callback.bot` on a real
  `CallbackQuery` in tests; build plain fake objects with the attributes
  the handler touches, or bind via `obj.as_(bot)` for the private `_bot`
  attr. `CallbackQuery` also requires the `chat_instance` field when
  constructed directly.
- **Every callback handler MUST call `answer_callback_query`** — an
  unanswered inline button spins in the client until it times out. The
  callbacks router wraps all actions in try/except and answers even on
  failure; keep that pattern when adding buttons.
- **Callback queries bypass the message auth middleware** (it only wraps
  `dp.message`). Role resolution for buttons lives in
  `handlers/callbacks._resolve_role` and must mirror `AuthMiddleware`
  exactly: admin_ids env → DB role → allowed_user_ids env → open access.
- **`allowed_updates` must include `"callback_query"`** in webhook mode
  (polling uses `dp.resolve_used_update_types()` and picks it up
  automatically once a callback handler exists) — else buttons are dead.
- **`edit_message_text` without `reply_markup` STRIPS the keyboard** — the
  window's button row must be passed on EVERY send and EVERY edit, not just
  the initial one (`Notifier._keyboard`).
- **SQLite cannot ALTER a CHECK constraint** — extending the `downloads`
  status enum means the create-copy-drop-rename dance
  (`Database._migrate_downloads`), guarded by sniffing `sqlite_master.sql`
  for `'interrupted'` so it is idempotent. Column order has been stable
  since 0.1.0, making the positional `INSERT … SELECT` safe — do not
  reorder columns in `_SCHEMA`.
- **`db.get_role()` is backed by a process-wide `_role_cache` keyed by
  user id** — tests that create multiple databases must call
  `Database.invalidate_role_cache()` or roles leak across test databases
  (first observed while testing callbacks).
- **Reboot recovery runs BEFORE polling starts** (`main.run()` calls
  `recover_after_reboot` right after `manager.start()`): stale
  queued/running rows → `interrupted`, `*.part` orphans swept (a `.part` at
  boot is by definition abandoned — no worker exists yet), one report per
  chat with the Re-queue button. The button rebuilds jobs from ledger
  columns (`file_id`, `file_unique_id`, `path`, `album_key`) — those
  columns ARE the recovery contract; don't drop them from `insert_download`.
  A graceful shutdown marks pending jobs `cancelled` with receipts instead;
  the compose example's `stop_grace_period: 40s` gives the 25 s drain time
  to actually finish (docker's default is 10 s).
