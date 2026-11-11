# 🐝 TellyBee

**A modern, fast and stable Telegram file & media downloader bot.**

Send or forward anything — photos, videos, documents, audio, voice notes,
GIFs, stickers, whole albums — and TellyBee streams it straight to your
server with live progress, receipts, and a fully monitorable download queue.

Built on **aiogram 3** (fully async), **Python 3.12**, and a
**SQLite** ledger — deployable in one `docker compose up -d`.

---

## ✨ Features

- 📥 **Downloads everything Telegram can send** — photo, video, document,
  audio, voice, video note, sticker, animation, and **media-group albums**
  (saved as one batch, one summary receipt).
- 🌐 **Direct link downloads** — send any http(s) URL (or `/link <url>`) and
  it is fetched straight from the source server to your disk with
  **parallel range connections** — no Telegram size limits, SSRF-guarded.
- ⚡ **Fast streaming engine** — constant memory usage, chunked transfer
  (configurable), SHA-256 hashing on the fly, `getFile` result caching, and
  **server-side prefetching** so the local Bot API server downloads ahead
  while workers copy finished files.
- 🛡 **Stable by design** — atomic `.part` → rename writes (no half-files),
  retries with exponential backoff + jitter, per-job error isolation
  (one bad file never stops the queue), graceful drain on shutdown.
- 🧮 **Live queue monitoring from inside the bot** — `/queue` shows active
  transfers with progress bars, pending jobs, worker utilization, and
  session counters; in chat, each download runs inside **one live window
  message** that updates in place — including **real progress while the
  local server is still fetching** the file from Telegram (watched on the
  shared volume).
- 🖲 **The window is a remote control** — its button row (Pause/Resume,
  Queue, Stats, Cancel, Help) does the job of the `/commands`, no typing
  needed. Permissions mirror the commands exactly (stats/pause are
  admin-only, users cancel their own files).
- 📌 **The window stays pinned to the bottom** — every receipt or button
  reply re-anchors it as the newest message in the chat, so live progress
  never drifts out of view while a long queue runs. Re-anchoring is
  **send-before-delete**: even when Telegram rate-limits the resend, the
  chat never spends a second without its window.
- 💎 **Modern Telegram rendering** — monospace figure chips (`<code>`),
  progress cards (`<pre>`), expandable blockquotes for long lists, bold
  headers and italic filenames: a dashboard look, not plain text.
- 🐢 **Forwards are never dropped** — the per-user throttle only guards
  text/command spam; media always reaches the queue, so forwarding an
  entire series in one go just works.
- 🗂 **Per-user folder layout** — `/folder` switches between organized
  template folders and **one flat folder** (`/folder Show Name` gives a
  series its own folder so episodes forwarded across days stay together).
- 🔁 **Survives reboots** — a startup reconciliation pass marks unfinished
  jobs *interrupted*, sweeps orphaned `.part` files, and sends each chat a
  report with a **Re-queue** button that restores the lost jobs from the
  ledger — no re-forwarding needed.
- 🎛 **Two-tier access control** — env-seeded admins & users, plus runtime
  `/allow` / `/disallow` / `/users` management persisted in SQLite.
- 🗂 **Flexible storage layout** — a configurable folder template like
  `{user}/{type}/{date}` with path-safety sanitization and collision-proof
  naming, plus each user's own `/folder` override.
- 🚦 **Flood-safe & honest** — per-user throttling (media exempt), bounded
  queue, file-type filters, size limits with friendly rejections, and
  progress that says what it means: a true stall renders as `⏸ stalled`,
  never a fantastical multi-year ETA.
- 🎯 **Progress tracks the right file** — fetch-progress matching only
  pairs a job with a server-side file that is actually *growing*; a
  leftover complete cache entry from an earlier download can never masquerade
  as the current one (once froze a new episode at the previous file's size,
  "stalled"). A fully-downloaded file awaiting its copy shows `✔ finishing`.
- 🧹 **Self-cleaning botapi cache** — the local Bot API server keeps every
  fetched file for 1–25 h and that is *not configurable* (the maintainers'
  own words); TellyBee instead deletes each server-side copy the moment the
  file is safely in `./downloads`, and a sweeper removes leftovers older
  than `BOTAPI_CACHE_TTL_MINUTES` (default 2 h) — the volume only ever holds
  in-flight fetches, not the whole day's downloads.
- ⚡ **Stalls recover in seconds, not half hours** — a server-side fetch with
  zero progress for `FETCH_STALL_TIMEOUT` (default 90 s) has its parked
  `getFile` call cancelled and re-issued — free, since the server keeps its
  progress — instead of idling until `GETFILE_TIMEOUT` (default 30 min)
  expires.
- ❤️ **Ops-friendly** — HTTP `/healthz` endpoint with queue metrics
  (docker healthcheck built in), text or JSON logs, polling *or* webhook
  mode, optional local Bot API server support for **files up to 4 GB**.

---

## 🚀 Quick start

### 1. Create the bot

Message [@BotFather](https://t.me/BotFather) on Telegram → `/newbot` → copy
the token.

### 2. Configure

```bash
git clone https://github.com/Chandima-Prabhath/TellyBee.git
cd TellyBee
cp .env.example .env
cp docker-compose.yml.example docker-compose.yml
```

Edit `.env` — the only *required* value is `BOT_TOKEN`. Recommended:

```dotenv
BOT_TOKEN=1234567890:AAE...
ADMIN_IDS=123456789          # your Telegram ID (ask @userinfobot)
```

> Leave both `ADMIN_IDS` and `ALLOWED_USER_IDS` empty and the bot runs in
> **open access** (anyone can write to your disk) — it will warn you at
> startup.

### 3. Run with Docker (recommended)

```bash
docker compose up -d --build
docker compose logs -f          # watch it come online
```

Files land in `./downloads/`, metadata in `./data/tellybee.db`.

> **Permissions:** on Linux, docker creates `./downloads/` and `./data/` as
> root on first start. The image's entrypoint fixes their ownership
> automatically (then drops back to the unprivileged `tellybee` user). If you
> prefer to do it manually:
> `sudo chown -R 1000:1000 ./data ./downloads`

> **`docker-compose.yml` is gitignored on purpose** — you copy it once from
> `docker-compose.yml.example`, and later `git pull` updates never overwrite
> your local edits. To see what changed in the example:
> `git diff HEAD -- docker-compose.yml.example`.

### 3b. Or run natively

```bash
# with uv (fast)
uv sync
uv run python -m app.main

# or plain pip
python -m venv .venv && source .venv/bin/activate
pip install -e .
python -m app.main
```

Validate your configuration any time:

```bash
python -m app.main --check
```

---

## 🤖 Bot commands

| Command | Who | What |
|---|---|---|
| `/start` | everyone | welcome card + your access level |
| `/help` | everyone | usage guide, limits, storage layout |
| `/link <url>` | everyone | download a direct http(s) link (or just send the URL as text) |
| `/folder [name]` | everyone | choose where downloads go — one folder (optionally named, ideal for series) or default template folders |
| `/status` | everyone | your 10 most recent downloads |
| `/queue` | everyone* | live queue monitor (*users see only their own jobs) |
| `/cancel <id>` | everyone* | cancel a running or pending download |
| `/stats` | admin | all-time + 24h statistics, per-type breakdown, top users |
| `/users` | admin | access list |
| `/allow <id>` / reply | admin | authorize a user at runtime |
| `/disallow <id>` | admin | revoke a runtime user |
| `/config` | admin | sanitized settings overview |
| `/pause` `/resume` | admin | pause/resume queue intake |
| `/health` | admin | same metrics as `/healthz`, in chat |

**What a download looks like in chat** — one live *window* message per chat,
not a notification per file. Forward a new task and the previous window is
replaced by a fresh one directly under your message; while anything runs it
updates in place; after every receipt (or button reply) it **re-anchors
itself as the newest message** so progress is always at the bottom of the
chat; when the queue drains it disappears and only the receipts (the
permanent records) remain:

```
🐝 TellyBee
2 active · 5 queued · 8 done · 12.3 GB
──────────────────────
🎬 E03.mkv
▰▰▰▰▱▱▱▱▱▱ 42% · 890 MB / 2.1 GB · 9.2 MB/s · ETA 2m 04s

📄 E04.mkv
▰▰▰▰▰▰▰▰▱▱ 84% · 1.4 GB / 1.7 GB · 4.1 MB/s

⏳ Queued · 5 files · 7.9 GB
> E05.mkv · E06.mkv · E07.mkv +2

[ ⏸ Pause · ✖ Cancel all ]
[ 📋 Queue · 📊 Stats · ❓ Help ]
```

The bottom rows are **inline buttons** — the window doubles as the bot's
remote control (see *Reboots & recovery* below for the Re-queue button):

| Button | Who | Effect |
|---|---|---|
| ⏸ Pause / ▶️ Resume | admin | same as `/pause` / `/resume`; the button flips and the header shows ⏸ paused |
| ✖ Cancel all | everyone* | cancels the chat's downloads (users: own files only) |
| 📋 Queue | everyone | same view as `/queue` (users see their own jobs) |
| 📊 Stats | admin | same as `/stats` |
| ❓ Help | everyone | same as `/help` |

Commands still work everywhere — the buttons are a shortcut, not a
replacement.

With a local Bot API server, big first-time downloads show the server-side
fetch as **real progress** (watched on the shared volume) before the fast
local copy takes over — to the user it is simply one download bar; if the
watcher has no numbers yet, the window says so instead of freezing:

```
⏳ Preparing · 1m 33s
```

When everything finishes, the window is deleted and each file/album leaves
its receipt behind — file lists arrive as quoted blocks (long album lists
collapse behind Telegram's expandable quote):

```
✅ Saved — 27.4 MB
> 🎬 clip.mp4 · 27.4 MB

⚡ 3.9 MB/s average
🧷 #a3f9c2 · 📁 chandima/video/2026-10-13
```

**Keeping a series together** — set it once with `/folder Severance S02`
and every episode lands in `downloads/<user>/Severance S02/`, no matter
which day you forward it. `/folder` (no argument) shows a picker with two
layout buttons; `reset` returns to the default template.

---

## ⚙️ Configuration reference

Everything is env-driven (`.env` file supported); human-friendly values like
`20MB` / `256KB` are accepted for sizes and `a,b,c` lists for IDs/types.

| Variable | Default | Description |
|---|---|---|
| `BOT_TOKEN` | — | **required**, from @BotFather |
| `ADMIN_IDS` | *(empty)* | comma list of admin Telegram IDs |
| `ALLOWED_USER_IDS` | *(empty)* | comma list of allowed user IDs |
| `DOWNLOAD_DIR` | `downloads` | download root (in container: `/app/downloads`) |
| `DB_PATH` | `data/tellybee.db` | SQLite path (in container: `/app/data/…`) |
| `FILE_TEMPLATE` | `{user}/{type}/{date}` | folder layout (tokens: `{user}` `{username}` `{user_id}` `{type}` `{date}` `{year}` `{month}` `{day}` `{chat_id}` `{chat_type}`) |
| `DOWNLOAD_CONCURRENCY` | `3` | parallel download workers (1–16) |
| `MAX_QUEUE_SIZE` | `100` | max pending jobs before rejecting |
| `DOWNLOAD_RETRIES` | `3` | attempts per file |
| `DOWNLOAD_TIMEOUT` | `900` | per-file hard timeout (seconds, cloud transfer) |
| `GETFILE_TIMEOUT` | `1800` | how long `getFile` may wait while the local server fetches a big file (seconds) |
| `PREFETCH` | `true` | start fetching queued files on the local server immediately (local mode) |
| `FETCH_CONCURRENCY` | `2` | max files the local server downloads from Telegram at once — workers **and** prefetch share these slots (1–8; low is right, see *Speeding up big-file downloads*) |
| `FETCH_WATCH_INTERVAL` | `1.5` | seconds between fetch-progress volume scans |
| `FETCH_STALL_TIMEOUT` | `90` | seconds of zero fetch progress before the parked `getFile` is cancelled and re-asked — the server keeps its download progress, so this is free (local mode; `0` = off) |
| `BOTAPI_EVICT_AFTER_COPY` | `true` | delete the server-side cache copy as soon as the file is safely in `./downloads` (needs the `botapi-data` volume mounted read-write) |
| `BOTAPI_CACHE_TTL_MINUTES` | `120` | sweeper deletes leftover botapi cache files older than this (`0` = no sweeping; eviction above still runs) |
| `BOTAPI_CLEANUP_INTERVAL_MINUTES` | `15` | how often the cache sweeper runs |
| `CHUNK_SIZE` | `256KB` | streaming chunk size |
| `MAX_FILE_SIZE` | `20MB` | reject bigger files (≤ 20MB cloud · ≤ 4000MB with a local Bot API server) |
| `LINKS_ENABLED` | `true` | allow direct http(s) link downloads |
| `LINK_CONNECTIONS` | `4` | parallel range connections per link (1–16) |
| `LINK_MAX_SIZE` | `0` | per-link size cap, `0` = uncapped (e.g. `10GB`) |
| `LINK_SSRF_GUARD` | `basic` | link target filter: `basic` (internal IPs only) / `strict` (non-global too) / `off` (no checks) |
| `ALLOWED_TYPES` | *(all)* | only accept these media types |
| `BLOCKED_TYPES` | *(none)* | never accept these media types |
| `PROGRESS_EDIT_INTERVAL` | `2.0` | seconds between edits of the chat's live window message (edits are also paced globally, so many busy chats cannot flood Telegram) |
| `POLLING_MODE` | `true` | `false` (or set `WEBHOOK_URL`) for webhook |
| `WEBHOOK_URL` | *(empty)* | e.g. `https://host/tellybee/webhook` (auto-switches mode) |
| `WEBHOOK_SECRET` | *(empty)* | secret token Telegram echoes back |
| `LISTEN_HOST` / `LISTEN_PORT` | `0.0.0.0` / `8080` | health/webhook HTTP listener |
| `API_BASE` | *(empty)* | custom Bot API server base URL |
| `THROTTLE_RATE` / `THROTTLE_WINDOW` | `20` / `60` | per-user flood control |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `text` | `DEBUG…CRITICAL`, `text|json` |

---

## 🏗 Architecture

```
Telegram ──updates──► aiogram Dispatcher
                        │  middleware: throttle → auth → album-buffer
                        ▼
                      Routers (common → admin → callbacks → links → media)
                        │  builds DownloadJob (template + sanitized path)
                        ▼
                 DownloadManager (priority queue + N workers)
                        │                              │
                        ▼                              ▼
              DownloadEngine (stream, retry,      Notifier (window +
              hash, atomic rename)               buttons + receipts)
                        │                              │
                        ▼                              ▼
                SQLite ledger (users, downloads)   chat messages
                        ▲
   HTTP /healthz + /readyz (queue metrics) ────────┘
```

Key design decisions (also documented for AI agents in `AGENT.md`):

- **Queue in memory, truth on disk** — the live queue is an
  `asyncio.PriorityQueue`; every state transition is mirrored to SQLite so
  `/stats` and post-mortems survive restarts.
- **One window per chat, one receipt per album** — all of a chat's live
  activity is rendered into a single *window* message (replaced whenever a
  new task arrives so it always sits under the user's latest message,
  deleted when the queue drains); a media group still ends in exactly one
  combined receipt, which is the permanent record.
- **Reboot reconciliation** — because the queue is memory-only, startup
  runs a recovery pass (mark stale rows `interrupted`, sweep orphaned
  `.part` files, report per chat with a Re-queue button that rebuilds jobs
  from the ledger's stored `file_id`s).
- **Atomicity everywhere** — partial downloads write `<name>.part` and are
  renamed on success; cancellation and shutdown clean up reliably.
- **Isolation** — handlers never block (no sync I/O on the hot path), and a
  failing job is finalized with an error without touching other jobs.

---

## 📦 Big files (up to 4 GB)

The standard Bot API caps downloads at **20 MB per file**. With a self-hosted
[telegram-bot-api](https://github.com/tdlib/telegram-bot-api) server in
`--local` mode, **downloads have no size limit at all** (per the server's
own docs) — the practical ceiling is Telegram's absolute per-file maximum
of **4 GB**, because only *Premium users can upload files larger than 2 GB*.
You don't need any paid plan for the bot itself:

* Free senders cap at 2 GB per upload — those files always work.
* 2–4 GB files only exist on Telegram when a Premium user uploaded them;
  **forwarding** such a file to the bot (as a free user) works fine.

The `botapi` service is **enabled by default** in `docker-compose.yml.example`
(arm64 and amd64 images are published):

1. Get `TELEGRAM_API_ID` / `TELEGRAM_API_HASH` from
   <https://my.telegram.org> ("API development tools") and put both in `.env`.
2. In `.env` set `API_BASE=http://botapi:8081` and `MAX_FILE_SIZE=4000MB`.
3. `docker compose up -d --build`.

Notes:

- On startup the bot automatically performs Telegram's required one-time
  `logOut` handshake against the cloud API — the local server will not serve
  a bot that is still registered with the cloud. After logging out, the bot
  cannot use the cloud API for ~10 minutes (relevant only if you switch
  `API_BASE` back off).
- **How the files actually reach `./downloads`:** in `--local` mode
  `getFile` returns an *absolute path* inside the botapi container and the
  server has **no HTTP file endpoint at all**. The compose file therefore
  mounts the `botapi-data` volume into the tellybee container as well
  (read-write), and the bot stream-copies files straight from it — fast and
  token-safe. If you maintain your own compose file, make sure the tellybee
  service includes:

  ```yaml
  volumes:
    - botapi-data:/var/lib/telegram-bot-api
  ```

  (read-write — since v0.7 the bot deletes served files from the volume
  after copying them; a `:ro` mount still *works*, but the disk-fill
  protection silently disables itself with a warning in the logs)

- **Why the first download of a big file takes a while:** the local server
  answers `getFile` only *after* it has fetched the whole file from Telegram
  — the request legitimately blocks for minutes (a 4 GB file on a slow link
  can take hours). The bot waits it out (`GETFILE_TIMEOUT` per request,
  automatically re-asking for up to 24 hours) and stays responsive to
  `/cancel` in the meantime; timeouts never burn the retry budget because
  the server keeps its download progress. While it waits, the engine
  **watches the server's shared volume** and shows real fetch progress
  (% / speed / ETA) in chat instead of a frozen bar. A fetch that shows
  *zero* progress for `FETCH_STALL_TIMEOUT` (default 90 s) is re-asked on
  the spot instead of idling. Re-sending a file that
  is already in the `botapi-data` volume is near-instant — and since v0.2.0
  re-sending a file that is already in `./downloads` is skipped outright
  (duplicate detection by `file_unique_id`).

- **Why the `botapi` service needs its custom `entrypoint`:** the Bot API
  server drops to its own unprivileged user (uid 101 in the stock image)
  and writes every received file with **owner-only permissions** — TDLib
  creates them `0600`. Combined with the missing HTTP file endpoint, the
  only reader that can stream files off the shared volume is a process
  running as the *same uid* as the server. The wrapper in
  `docker-compose.yml.example` therefore runs the server as uid 1000 (the
  `tellybee` user) and hands the volume over to it (`chown -R`) on every
  start, using the server's native `--username`/`--groupname` flags. If you
  keep your own compose file, copy the whole `botapi` service block —
  without it every download fails with
  `[Errno 13] Permission denied: '/var/lib/telegram-bot-api/…'`.

- The `botapi` container is deliberately **not** published to the host — only
  the bot talks to it over the internal compose network. Exposing an
  unauthenticated Bot API server would let anyone control any bot token they
  know. Uncomment the `127.0.0.1:8081` mapping in the compose file for local
  debugging only.
- Telegram files are cached inside the `botapi-data` **named volume** before
  the bot copies them to `./downloads` — since v0.7 the bot **deletes each
  server-side copy right after the copy completes** and sweeps leftovers
  older than `BOTAPI_CACHE_TTL_MINUTES` (default 2 h), so the volume only
  ever holds in-flight fetches plus a small TTL window: a 4 GB file needs
  roughly 4 GB of free disk at download time, not ~8 GB. (The Bot API
  server's own retention is 1–25 h and not configurable — see
  [tdlib/telegram-bot-api#303](https://github.com/tdlib/telegram-bot-api/issues/303),
  which also confirms manual deletion is safe.) The bot checks free space
  at intake and refuses files that cannot fit; `/stats` shows free space on
  both volumes. Check usage with `docker system df`.
- The bot token is masked (`***`) everywhere in the bot's own logs, and error
  messages shown in chat receipts never contain it — Bot API download URLs
  embed the token, so keep `docker compose logs` sharing safe too.
- Don't want the local server? Comment out the whole `botapi` service, the
  `depends_on` entry **and** the `botapi-data` volume mount above, remove
  `API_BASE` from `.env`, and set `MAX_FILE_SIZE=20MB`.

### 🏎 Speeding up big-file downloads

Five layers, all free:

1. **Prefetching (on by default).** The slow part of local mode is the
   server downloading from Telegram — normally that only starts when a
   worker picks the job. TellyBee instead asks the local server to start
   fetching every queued file immediately (`PREFETCH=true`), so the server
   keeps downloading while workers copy finished files. Batches that span
   different Telegram DCs genuinely parallelize (each DC has its own pool);
   a batch on one DC finishes in roughly the same total time but stays
   pipelined.
2. **Bounded fetch concurrency (`FETCH_CONCURRENCY`, default 2).** *Every*
   server-side fetch — from workers or prefetch — takes one of these
   shared slots. TDLib keeps only ~2 MiB of a file in flight per Telegram
   DC, so parallel big-file fetches just split that window between them:
   with the v0.3.2 defaults a fully busy queue ran **nine** fetches at once
   and every one crawled at ~0–200 B/s while the bot looked stuck. Two at a
   time (one on weak hosts) finish fast and keep the pipeline moving; raise
   it only if your link demonstrably sustains more.
3. **Your host's link and disk.** The fetch runs at whatever the ARM box's
   internet connection allows, and the `botapi-data` volume must keep up
   with writes — an SD card can throttle a Pi-class host; USB SSDs help.
4. **The optional `botapi-fast` build (biggest single-file win).** TDLib
   keeps only **~2 MiB of one file in flight per Telegram DC**
   (`max_download_resource_limit_ = 1 << 21` in TDLib's
   `FileDownloadManager.h`) — on a high-latency link that alone caps a
   single big file at roughly `2 MiB / RTT`. TDLib raises the limit **8×**
   when its `session_count` option is > 1, but the stock server never sets
   it and the Bot API gives bots no way to set TDLib options. The
   `deploy/botapi-fast/Dockerfile` builds a patched server that sets
   `session_count` before authorization — swap the `botapi` service from
   `image:` to that `build:` (see the commented block in
   `docker-compose.yml.example`). First build compiles TDLib (hours on
   ARM, then cached); afterwards single-file downloads typically run
   several times faster until your bandwidth becomes the ceiling. Keep the
   custom entrypoint either way.
5. **Stalls never idle (v0.7+).** A server-side fetch showing zero progress
   for `FETCH_STALL_TIMEOUT` (default 90 s) has its parked `getFile` call
   cancelled and re-issued on the spot — the server keeps its download
   progress, so the re-ask costs nothing and recovers wedged waits in
   seconds instead of waiting out `GETFILE_TIMEOUT` (default 30 min).

---

## 🌐 Direct link downloads

Besides Telegram files, TellyBee can fetch **any http(s) URL** straight to
the downloads folder — Telegram is not involved, so the Bot API's limits do
not apply; the transfer runs at your host's connection speed.

- Send a message containing a link, or use `/link <url>` (up to 5 links per
  message). Files land under the usual template with `{type}` = `url`, get
  the same receipts, live window, `/cancel`, `/stats` rows and
  duplicate detection (by the final URL) as Telegram files.
- **Multi-connection:** servers that support byte ranges are downloaded over
  `LINK_CONNECTIONS` (default 4) parallel connections, which usually
  saturates the host's bandwidth where a single stream would not; servers
  without range support fall back to one stream automatically. Each ranged
  part resumes itself from its current offset on transient errors.
- **Rate-limit aware:** hosts that answer `429` (or `503` + `Retry-After`)
  are waited out with backoff that honors the server's `Retry-After` — at
  the probe, per ranged part, and on single streams. If the parallel burst
  stays throttled, the missing bytes are finished over **one connection**
  instead of restarting the file, and parts start staggered to avoid
  tripping per-second limits in the first place. A permanently throttling
  host finally fails with a plain-language remedy (wait, or set
  `LINK_CONNECTIONS=1`) instead of aiohttp's opaque `429, message=…` text.
- **Safety:** only `http`/`https` URLs, redirects followed but re-checked,
  and an SSRF guard inside the HTTP connector **refuses to connect to
  internal addresses** (loopback, your LAN, the docker network, cloud
  metadata endpoints) — a crafted link can never reach them. The mode is
  `LINK_SSRF_GUARD`: `basic` (default) blocks internal targets only,
  `strict` additionally refuses unusual non-global address space, and `off`
  disables the checks entirely — handy to download from a host on your own
  LAN. URLs with embedded credentials (`user:pass@…`) are rejected, and
  signed query strings are stripped from any error text shown in chat/logs.
  When the guard refuses, its message names the exact address it rejected.
- **Limits:** `LINK_MAX_SIZE` (default uncapped) caps the accepted size;
  unknown sizes are enforced mid-transfer; the queue's disk-headroom guard
  and ENOSPC fail-fast apply exactly as for Telegram files. Disable the
  whole feature with `LINKS_ENABLED=false`.

---

## 🔁 Reboots & recovery

The download queue lives in memory (fast, simple, no stale locks); SQLite is
the durable record. When the process dies mid-job, three kinds of mess used
to be left behind — all three are handled automatically since v0.4.1:

**1. Dead ledger rows.** Anything still `queued`/`running` at boot belongs to
a previous life. Startup marks those rows `interrupted` (a one-time schema
migration adds the status; your existing database upgrades in place, data
preserved).

**2. Orphaned `.part` files.** A hard kill leaves half-written
`<name>.part` files behind. No worker exists yet at boot, so any `.part` is
by definition abandoned — the recovery pass sweeps them all.

**3. Silence.** Each chat that lost work gets exactly one report:

```
🔁 Rebooted mid-job — 3 downloads were interrupted:
• E07.mkv — 1.4 GB
• E08.mkv — 1.3 GB
• E09.mkv — 1.2 GB

Re-send them, or press the button — the queue picks up where it can.

[ 🔁 Re-queue 3 files ]
```

**Re-queue** rebuilds the jobs straight from the ledger (stored
`file_id`, `file_unique_id`, destination path, album grouping) and puts them
back into the live queue — receipts and the progress window work exactly as
if you had re-sent the files. Files that meanwhile completed (or are already
queued) are skipped and reported; if the queue is full the rest stays
`interrupted` and the button can be pressed again later. `/status` shows
💤 interrupted and 🔁 re-queued entries.

Two more reboot facts worth knowing:

- **Forwarded-while-down messages are not lost** — Telegram keeps
  unconfirmed updates for 24 h and re-delivers them when the bot returns.
- **The botapi cache keeps completed files** — files the local server had
  *finished* fetching survive a tellybee-only restart until the TTL (2 h
  by default): re-send (or re-queue) within that window and the copy is
  near-instant. Files already copied to `./downloads` are deleted from the
  volume immediately (duplicate detection handles re-sends anyway).
  Half-finished server-side fetches are *not* resumed by the server; those
  restart from zero (and the sweeper removes their orphaned partials).

For graceful shutdowns (`docker compose down/up --build`), the compose
example sets `stop_grace_period: 40s` so the bot's 25 s drain can actually
finish — pending jobs are then cleanly `cancelled` with receipts instead of
being interrupted. Users with an older local compose file can add that one
line under the `tellybee` service.

---

## 🔧 Development

```bash
uv sync                      # install with dev tools
uv run pytest                # run the test suite
uv run ruff check app tests  # lint
uv run python -m app.main --check
```

Project layout and conventions are documented in **`AGENT.md`** — read it
before contributing (humans and AI assistants alike).

---

## 🛡 Troubleshooting

| Symptom | Fix |
|---|---|
| `Cannot connect to host botapi:8081` / `Connect call failed` | The two containers aren't on the same network — **`docker compose restart` does not apply compose-file edits**. Run `docker compose down && docker compose up -d --build` instead. The bot retries on its own in the meantime. |
| botapi log shows no `--api-id=` flag | Its environment didn't get your credentials — same cause as above: recreate with `docker compose up -d --build` (never `restart`). |
| Local server answers `404 Not Found` | Missing cloud `logOut` handshake — the bot performs it automatically at startup; if it could not reach the internet, restart after a minute. |
| Every download fails with `404 … /file/bot…` | Old image downloaded via an HTTP file endpoint — local Bot API servers don't have one. Update to ≥ 0.1.2 and make sure the tellybee service mounts `botapi-data:/var/lib/telegram-bot-api`, then `docker compose up -d --build`. |
| `…is not mounted in this container…` in job errors | The `botapi-data` volume is not shared with the tellybee service — add `botapi-data:/var/lib/telegram-bot-api` to its `volumes:` and recreate the container. |
| The `botapi-data` volume slowly fills the disk | Pre-0.7.0 the bot never deleted anything from it (the server's own retention is 1–25 h and not configurable). Update to ≥ 0.7.0 **and** drop `:ro` from the volume mount — the bot then deletes each file right after copying it and sweeps leftovers (`BOTAPI_CACHE_TTL_MINUTES`, default 2 h). On an older version, clean by hand: `docker compose exec tellybee find /var/lib/telegram-bot-api -path '*/files/*' -type f -mmin +120 -delete`. |
| Every download fails with `[Errno 13] Permission denied: '/var/lib/telegram-bot-api/…'` | The botapi server runs as its own uid (101) and writes files `0600`, so tellybee (uid 1000) can't read them from the shared volume. Copy the current `botapi` service block — especially its `entrypoint:` uid-alignment wrapper — from `docker-compose.yml.example` into your compose file, then `docker compose down && docker compose up -d --build`. On SELinux-enforcing hosts (RHEL/Fedora) also add `:z` to the `botapi-data` volume mounts. |
| Every large first-time download fails with `HTTP Client says - Request timeout error` | Pre-0.1.4 images killed the `getFile` wait at aiogram's default 60s — but a local server legitimately takes minutes to fetch a big file before answering. Update to ≥ 0.1.4 (`git pull && docker compose up -d --build`). On very slow links raise `GETFILE_TIMEOUT` (seconds; default 1800). Already-failed files are usually cached in the `botapi-data` volume by now — re-send them, they complete near-instantly. |
| `unable to open database file` / `not writable` | Bind mounts owned by root — the entrypoint fixes this automatically on new images; manually: `sudo chown -R 1000:1000 ./data ./downloads`, then `docker compose restart`. |
| `BOT_TOKEN does not look like…` | Re-copy the token from @BotFather — no spaces, full string. |
| Bot silent for non-admins | You set `ADMIN_IDS` only — add your friends via `/allow` or `ALLOWED_USER_IDS`. |
| `ApiTokenValidationError` in logs | Token revoked/regenerated — update `.env` and restart. |
| Files > 20 MB fail | Expected with the standard Bot API — see the *Big files* section. |
| Files > 2 GB rejected / do bigger files work? | Yes — with the local Bot API server, downloads have **no size limit** (4 GB is Telegram's own per-file ceiling). Set `MAX_FILE_SIZE=4000MB` and `API_BASE=http://botapi:8081`. A >2 GB file can only originate from a Premium upload — forwarding one to the bot as a free user works fine. |
| Link download says *“refusing to connect: … resolves to …, a private or otherwise blocked address”* | The SSRF guard refused an internal/loopback address — by design, and the message now names it. If you trust the source (e.g. a host on your own LAN), set `LINK_SSRF_GUARD=off`. On v0.3.0 this message appeared for **every** link — aiohttp moved the resolver's address field and the guard read the wrong one; update to ≥ 0.3.1 (`git pull && docker compose up -d --build`). |
| Link download fails with *“the server answered HTTP 404/403…”* | The server rejected the request (expired signed URL, auth required, gone). Nothing retryable — fetch a fresh link. |
| Link download fails with *“rate-limited”* / `429` | The file host is throttling (seen with seedr-style CDNs on busy free accounts). TellyBee already waits out `Retry-After` and falls back to a single connection; if it still fails, wait a few minutes and re-send the link, or set `LINK_CONNECTIONS=1` for single-stream transfers. |
| Window line stuck at “fetching from Telegram · Ns so far” with no % | The volume watcher could not match a growing file in the server's storage (e.g. the job's size is unknown). Display-only — the download itself is unaffected. |
| With the queue full, every download crawls at ~0 B/s and the bot feels stuck | Too many simultaneous server-side fetches split TDLib's ~2 MiB per-DC window between them (v0.3.2 ran up to 9 at once). Update to ≥ 0.3.3 — workers and prefetch now share the `FETCH_CONCURRENCY` slots (default 2); on weak links set `FETCH_CONCURRENCY=1`. |
| Receipts arrive late or some never showed up (v0.3.2) | A busy queue's progress-message edits exceeded what Telegram allows per chat — the flood-waits swallowed receipt edits whole. Update to ≥ 0.3.3: edits are paced globally and receipts are retried / re-sent, never dropped. |
| Big files download slowly | See *Speeding up big-file downloads* — prefetching is on by default and fetch concurrency is capped sensibly; the `botapi-fast` build multiplies single-file speed; check the host's link/disk. |
| `…exceeds 4.0 GB limit` on intake | Telegram's absolute per-file maximum (Premium uploads cap at 4 GB) — nothing larger exists to download. Use a direct link instead. |
| Queue full rejections | Raise `MAX_QUEUE_SIZE` and/or `DOWNLOAD_CONCURRENCY`. |
| Healthcheck failing | `curl http://localhost:8080/healthz` — if the bot is crash-looping, check `docker compose logs`. |
| Old database on upgrade to ≥ 0.4.1 | Nothing to do — the `downloads` table (status CHECK) is migrated in place at startup, data preserved; first boot also marks stale rows `interrupted`, sweeps `.part` orphans, and reports per chat. |
| `docker compose down` kills the bot mid-drain | Docker's default 10 s stop grace is shorter than the bot's 25 s drain — add `stop_grace_period: 40s` under the `tellybee` service (current compose example already has it). |

---

## 📄 License

MIT © Chandima Prabhath — see [LICENSE](LICENSE).
