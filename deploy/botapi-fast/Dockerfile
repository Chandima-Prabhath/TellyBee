# ══════════════════════════════════════════════════════════════════════════
#  TellyBee — OPTIONAL high-throughput build of the local Bot API server
#
#  WHY: TDLib caps how much of ONE file it keeps in flight per Telegram DC
#  (max_download_resource_limit_ = 2 MiB in FileDownloadManager.h). That
#  limit is multiplied by 8 (→ 16 MiB) when the TDLib option "session_count"
#  is > 1 — but the stock telegram-bot-api server never sets it, and bots
#  cannot call TDLib setOption through the Bot API. This build injects
#
#      setOption("session_count", N)
#
#  into the server's startup sequence (before authorization, where TDLib
#  requires it to be set), raising the per-file in-flight window 8×. On
#  high-latency links this typically multiplies big-file download speed
#  several times — until the host's own bandwidth becomes the ceiling.
#
#  COST: the first build compiles all of TDLib — on a Raspberry Pi-class ARM
#  box expect HOURS (and ~2 GB RAM; add swap if the build gets OOM-killed).
#  Every later build is cached and instant unless you change the ref.
#
#  USAGE (docker-compose.yml, botapi service — replace the `image:` line):
#
#      botapi:
#        build:
#          context: ./deploy/botapi-fast
#          args:
#            TELEGRAM_BOT_API_REF: master     # or a commit hash to pin
#            SESSION_COUNT: "4"              # 2-16; 4 is a good default
#        # (keep the custom entrypoint from docker-compose.yml.example!)
#  ══════════════════════════════════════════════════════════════════════════

# ── Stage 1: build the server + bundled TDLib ─────────────────────────────
FROM ubuntu:24.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
# Branch / tag / commit of github.com/tdlib/telegram-bot-api to build.
ARG TELEGRAM_BOT_API_REF=master
# TDLib "session_count" to inject (2..16). 4 ≈ Telegram Desktop territory.
ARG SESSION_COUNT=4
# Lower this on low-RAM builders (2 is safe for 2 GB machines).
ARG BUILD_JOBS=0

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        make git zlib1g-dev libssl-dev gperf cmake g++ ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone --recursive https://github.com/tdlib/telegram-bot-api.git . && \
    git checkout "${TELEGRAM_BOT_API_REF}" && \
    git submodule update --init --recursive

# Inject the session_count option right before setTdlibParameters is sent
# (that anchor line has been stable across every recent server version; the
# build fails loudly if it ever changes, instead of silently skipping).
RUN SESSION_COUNT="${SESSION_COUNT}" python3 - <<'PY'
import os
from pathlib import Path

p = Path("td/telegram/Client.cpp")
s = p.read_text()
anchor = "auto request = make_object<td_api::setTdlibParameters>();"
count = s.count(anchor)
assert count == 1, f"anchor found {count} times — pin a different TELEGRAM_BOT_API_REF"
sessions = int(os.environ.get("SESSION_COUNT", "4"))
inject = (
    "send_request(make_object<td_api::setOption>(\"session_count\", "
    f"make_object<td_api::optionValueInteger>({sessions})), "
    "td::make_unique<TdOnOkCallback>());\n\n      "
)
p.write_text(s.replace(anchor, inject + anchor, 1))
print(f"patched Client.cpp: session_count={sessions} will be set before authorization")
PY

RUN mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX:PATH=/usr/local .. && \
    cmake --build . --target install \
        -j "$([ "${BUILD_JOBS:-0}" -gt 0 ] && echo "${BUILD_JOBS}" || nproc)" && \
    strip /usr/local/bin/telegram-bot-api

# ── Stage 2: minimal runtime ───────────────────────────────────────────────
FROM ubuntu:24.04
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl3t64 zlib1g ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/telegram-bot-api /usr/local/bin/telegram-bot-api

# Same contract as the stock aiogram/telegram-bot-api image: the compose file's
# custom entrypoint runs the server as uid 1000 via --username/--groupname.
EXPOSE 8081
