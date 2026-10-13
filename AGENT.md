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

- **Stack**: Python 3.10+ (runtime images use 3.12), aiogram 3.31+,
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
(guarded by `IsAdmin` router-level filter) → `media` (broad media filter
+ catch-all fallback). Command handlers must stay before the media
catch-all.

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
- Cancellation: pending → `_cancel_flags` set, finalized when dequeued;
  running → `job.cancel_event`, the engine aborts at the next chunk
  boundary and raises `JobCancelled`.
- The notifier groups jobs by `album_key or f"job:{job_id}"`; a group owns
  at most one chat message, created lazily only when
  `expected_bytes >= PROGRESS_MIN_BYTES`, edited at most once per
  `PROGRESS_EDIT_INTERVAL`, finalized into exactly one receipt.

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
- Download URL = `bot.session.api.file_url(bot.token, file.file_path)` —
  honors custom API bases automatically. `getFile` responses are cached
  (`_FileCache`, TTL 15 min) because Telegram rate-limits that call.
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
