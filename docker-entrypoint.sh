#!/bin/sh
# TellyBee container entrypoint.
#
# Docker (on Linux) auto-creates bind-mount directories as root:root, which
# would leave the unprivileged app user unable to write. When the container
# starts as root we fix ownership of the runtime mount roots and then drop
# privileges via gosu. If the container is already started as an unprivileged
# user (e.g. `docker run --user 1000:1000`), everything is a no-op passthrough.

set -eu

APP_USER="tellybee"
APP_GROUP="tellybee"

if [ "$(id -u)" = "0" ]; then
    # Mount roots get a non-recursive chown (downloads can hold thousands of
    # files — walking the whole tree on every start would be needlessly slow;
    # everything the app creates below the root is owned by the app anyway).
    for dir in /app/downloads /app/data "${DOWNLOAD_DIR:-}"; do
        if [ -n "$dir" ] && [ -d "$dir" ]; then
            chown "$APP_USER:$APP_GROUP" "$dir" 2>/dev/null || true
        fi
    done

    # The data dir is tiny (SQLite db + WAL files) — fix it recursively so a
    # pre-existing root-owned database file from an old container also heals.
    chown -R "$APP_USER:$APP_GROUP" /app/data 2>/dev/null || true

    # Custom DB_PATH override: fix its parent directory too.
    if [ -n "${DB_PATH:-}" ] && [ -d "$(dirname "$DB_PATH")" ]; then
        chown "$APP_USER:$APP_GROUP" "$(dirname "$DB_PATH")" 2>/dev/null || true
    fi

    echo "entrypoint: fixed mount ownership, dropping to $APP_USER"
    exec gosu "$APP_USER" "$@"
fi

exec "$@"
