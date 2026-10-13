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
- ⚡ **Fast streaming engine** — constant memory usage, chunked transfer
  (configurable), SHA-256 hashing on the fly, `getFile` result caching.
- 🛡 **Stable by design** — atomic `.part` → rename writes (no half-files),
  retries with exponential backoff + jitter, per-job error isolation
  (one bad file never stops the queue), graceful drain on shutdown.
- 🧮 **Live queue monitoring from inside the bot** — `/queue` shows active
  transfers with progress bars, pending jobs, worker utilization, and
  session counters; big downloads get an auto-updating progress message.
- 🎛 **Two-tier access control** — env-seeded admins & users, plus runtime
  `/allow` / `/disallow` / `/users` management persisted in SQLite.
- 🗂 **Flexible storage layout** — configurable folder template like
  `{user}/{type}/{date}` with path-safety sanitization and collision-proof
  naming.
- 🚦 **Flood-safe** — per-user throttling, bounded queue, file-type filters,
  size limits with friendly rejections.
- ❤️ **Ops-friendly** — HTTP `/healthz` endpoint with queue metrics
  (docker healthcheck built in), text or JSON logs, polling *or* webhook
  mode, optional local Bot API server support for **2 GB files**.

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

**What a download looks like in chat:**

```
⬇️ Downloading clip.mp4
▰▰▰▰▰▰▱▱▱▱ 52%
14.2 / 27.4 MB • 3.4 MB/s • ETA 4s
```

```
✅ Saved — 27.4 MB
🎬 clip.mp4 — 27.4 MB
⚡ 3.9 MB/s average
🧷 #a3f9c2
📁 chandima/video/2026-10-13
```

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
| `CHUNK_SIZE` | `256KB` | streaming chunk size |
| `MAX_FILE_SIZE` | `20MB` | reject bigger files (≤ 20MB cloud · ≤ 4000MB with a local Bot API server) |
| `ALLOWED_TYPES` | *(all)* | only accept these media types |
| `BLOCKED_TYPES` | *(none)* | never accept these media types |
| `PROGRESS_MIN_BYTES` | `2MB` | live progress message threshold |
| `PROGRESS_EDIT_INTERVAL` | `2.0` | seconds between progress edits |
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
                      Routers (common → admin → media)
                        │  builds DownloadJob (template + sanitized path)
                        ▼
                 DownloadManager (priority queue + N workers)
                        │                              │
                        ▼                              ▼
              DownloadEngine (stream, retry,      Notifier (progress
              hash, atomic rename)                edits + receipts)
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
- **One receipt per album** — a media group shares a single chat message
  that evolves: *progress* → *combined receipt*.
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
  (read-only), and the bot stream-copies files straight from it — fast and
  token-safe. If you maintain your own compose file, make sure the tellybee
  service includes:

  ```yaml
  volumes:
    - botapi-data:/var/lib/telegram-bot-api:ro
  ```

- **Why the first download of a big file takes a while:** the local server
  answers `getFile` only *after* it has fetched the whole file from Telegram
  — the request legitimately blocks for minutes (a 4 GB file on a slow link
  can take hours). The bot waits it out (`GETFILE_TIMEOUT` per request,
  automatically re-asking for up to 24 hours) and stays responsive to
  `/cancel` in the meantime; timeouts never burn the retry budget because
  the server keeps its download progress. Re-sending a file that is already
  in the `botapi-data` volume is near-instant — and since v0.2.0 re-sending
  a file that is already in `./downloads` is skipped outright (duplicate
  detection by `file_unique_id`).
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
  the bot copies them to `./downloads` — a 4 GB file needs ~8 GB of free disk
  at download time. The bot checks free space at intake and refuses files
  that cannot fit; `/stats` shows free space on both volumes. Check usage
  with `docker system df`.
- The bot token is masked (`***`) everywhere in the bot's own logs, and error
  messages shown in chat receipts never contain it — Bot API download URLs
  embed the token, so keep `docker compose logs` sharing safe too.
- Don't want the local server? Comment out the whole `botapi` service, the
  `depends_on` entry **and** the `botapi-data` volume mount above, remove
  `API_BASE` from `.env`, and set `MAX_FILE_SIZE=20MB`.

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
| Every download fails with `404 … /file/bot…` | Old image downloaded via an HTTP file endpoint — local Bot API servers don't have one. Update to ≥ 0.1.2 and make sure the tellybee service mounts `botapi-data:/var/lib/telegram-bot-api:ro`, then `docker compose up -d --build`. |
| `…is not mounted in this container…` in job errors | The `botapi-data` volume is not shared with the tellybee service — add `botapi-data:/var/lib/telegram-bot-api:ro` to its `volumes:` and recreate the container. |
| Every download fails with `[Errno 13] Permission denied: '/var/lib/telegram-bot-api/…'` | The botapi server runs as its own uid (101) and writes files `0600`, so tellybee (uid 1000) can't read them from the shared volume. Copy the current `botapi` service block — especially its `entrypoint:` uid-alignment wrapper — from `docker-compose.yml.example` into your compose file, then `docker compose down && docker compose up -d --build`. On SELinux-enforcing hosts (RHEL/Fedora) also add `:z` to the `botapi-data` volume mounts. |
| Every large first-time download fails with `HTTP Client says - Request timeout error` | Pre-0.1.4 images killed the `getFile` wait at aiogram's default 60s — but a local server legitimately takes minutes to fetch a big file before answering. Update to ≥ 0.1.4 (`git pull && docker compose up -d --build`). On very slow links raise `GETFILE_TIMEOUT` (seconds; default 1800). Already-failed files are usually cached in the `botapi-data` volume by now — re-send them, they complete near-instantly. |
| `unable to open database file` / `not writable` | Bind mounts owned by root — the entrypoint fixes this automatically on new images; manually: `sudo chown -R 1000:1000 ./data ./downloads`, then `docker compose restart`. |
| `BOT_TOKEN does not look like…` | Re-copy the token from @BotFather — no spaces, full string. |
| Bot silent for non-admins | You set `ADMIN_IDS` only — add your friends via `/allow` or `ALLOWED_USER_IDS`. |
| `ApiTokenValidationError` in logs | Token revoked/regenerated — update `.env` and restart. |
| Files > 20 MB fail | Expected with the standard Bot API — see the *Big files* section. |
| Files > 2 GB rejected / do bigger files work? | Yes — with the local Bot API server, downloads have **no size limit** (4 GB is Telegram's own per-file ceiling). Set `MAX_FILE_SIZE=4000MB` and `API_BASE=http://botapi:8081`. A >2 GB file can only originate from a Premium upload — forwarding one to the bot as a free user works fine. |
| `…exceeds 4.0 GB limit` on intake | Telegram's absolute per-file maximum (Premium uploads cap at 4 GB) — nothing larger exists to download. |
| Queue full rejections | Raise `MAX_QUEUE_SIZE` and/or `DOWNLOAD_CONCURRENCY`. |
| Healthcheck failing | `curl http://localhost:8080/healthz` — if the bot is crash-looping, check `docker compose logs`. |

---

## 📄 License

MIT © Chandima Prabhath — see [LICENSE](LICENSE).
