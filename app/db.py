"""SQLite persistence layer (aiosqlite).

Kept intentionally thin: plain SQL, WAL mode, no ORM. Stores users (access
control) and a download ledger powering /stats, /status and post-mortems.

The queue itself lives in memory; this table is the durable record.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType

import aiosqlite

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER PRIMARY KEY,
    username    TEXT,
    first_name  TEXT,
    role        TEXT NOT NULL CHECK (role IN ('admin', 'user')) DEFAULT 'user',
    added_by    INTEGER,
    added_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS downloads (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id         TEXT NOT NULL UNIQUE,
    user_id        INTEGER NOT NULL,
    username       TEXT,
    chat_id        INTEGER NOT NULL,
    message_id     INTEGER,
    album_key      TEXT,
    media_type     TEXT NOT NULL,
    file_id        TEXT NOT NULL,
    file_unique_id TEXT,
    file_name      TEXT,
    file_size      INTEGER,
    sha256         TEXT,
    path           TEXT,
    status         TEXT NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
    error          TEXT,
    avg_speed      REAL,
    duration_ms    INTEGER,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_downloads_user_id    ON downloads (user_id);
CREATE INDEX IF NOT EXISTS idx_downloads_status     ON downloads (status);
CREATE INDEX IF NOT EXISTS idx_downloads_created_at ON downloads (created_at);
"""

# Role cache lives here so /allow & /disallow can invalidate it centrally.
_role_cache: dict[int, str] = {}


class Database:
    """Async wrapper around the TellyBee SQLite database."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await aiosqlite.connect(self._path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.executescript(_SCHEMA)
        await self._conn.commit()
        logger.info("database ready at %s", self._path)

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("database not connected; call connect() first")
        return self._conn

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            logger.info("database closed")

    async def __aenter__(self) -> Database:
        await self.connect()
        return self

    async def __aexit__(self, exc_t: type, exc_v: BaseException, tb: TracebackType) -> None:
        await self.close()

    # ── users ───────────────────────────────────────────────────────────────

    async def seed_users(self, admin_ids: set[int], user_ids: set[int]) -> None:
        """Persist env-configured admins/users as the baseline ACL."""
        for uid in admin_ids:
            await self.upsert_user(uid, role="admin")
        for uid in user_ids:
            await self.upsert_user(uid, role="user")
        _role_cache.clear()

    async def upsert_user(
        self,
        user_id: int,
        username: str | None = None,
        first_name: str | None = None,
        role: str = "user",
        added_by: int | None = None,
    ) -> None:
        await self.conn.execute(
            """
            INSERT INTO users (user_id, username, first_name, role, added_by)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = COALESCE(excluded.username, username),
                first_name = COALESCE(excluded.first_name, first_name),
                role = MAX(role, excluded.role)
            """,
            (user_id, username, first_name, role, added_by),
        )
        await self.conn.commit()
        _role_cache.pop(user_id, None)

    async def remove_user(self, user_id: int) -> bool:
        cursor = await self.conn.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
        await self.conn.commit()
        _role_cache.pop(user_id, None)
        return cursor.rowcount > 0

    async def get_role(self, user_id: int) -> str | None:
        """Return 'admin' | 'user' | None, with a small in-memory cache."""
        if user_id in _role_cache:
            return _role_cache[user_id]
        async with self.conn.execute(
            "SELECT role FROM users WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
        role = row["role"] if row else None
        if role:
            _role_cache[user_id] = role
        return role

    async def list_users(self) -> list[dict]:
        async with self.conn.execute(
            "SELECT user_id, username, first_name, role, added_at FROM users ORDER BY role, user_id"
        ) as cursor:
            return [dict(row) for row in await cursor.fetchall()]

    @staticmethod
    def invalidate_role_cache() -> None:
        _role_cache.clear()

    # ── downloads ───────────────────────────────────────────────────────────

    async def insert_download(self, record: dict) -> None:
        await self.conn.execute(
            """
            INSERT OR IGNORE INTO downloads
                (job_id, user_id, username, chat_id, message_id, album_key,
                 media_type, file_id, file_unique_id, file_name, file_size, path, status)
            VALUES (:job_id, :user_id, :username, :chat_id, :message_id, :album_key,
                    :media_type, :file_id, :file_unique_id, :file_name, :file_size, :path, :status)
            """,
            record,
        )
        await self.conn.commit()

    async def update_download(
        self, job_id: str, status: str, *, error: str | None = None, sha256: str | None = None,
        avg_speed: float | None = None, duration_ms: int | None = None, finished: bool = False,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE downloads SET
                status = ?,
                error = ?,
                sha256 = COALESCE(?, sha256),
                avg_speed = COALESCE(?, avg_speed),
                duration_ms = COALESCE(?, duration_ms),
                finished_at = CASE WHEN ? THEN datetime('now') ELSE finished_at END
            WHERE job_id = ?
            """,
            (status, error, sha256, avg_speed, duration_ms, finished, job_id),
        )
        await self.conn.commit()

    async def user_history(self, user_id: int, limit: int = 10) -> list[dict]:
        async with self.conn.execute(
            """
            SELECT media_type, file_name, file_size, status, path, error, finished_at
            FROM downloads WHERE user_id = ?
            ORDER BY id DESC LIMIT ?
            """,
            (user_id, limit),
        ) as cursor:
            return [dict(row) for row in await cursor.fetchall()]

    async def global_stats(self) -> dict:
        stats: dict = {}
        async with self.conn.execute(
            """
            SELECT
                COUNT(*)                                             AS total,
                COALESCE(SUM(file_size), 0)                          AS total_bytes,
                COALESCE(SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END), 0)   AS completed,
                COALESCE(SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END), 0)      AS failed,
                COALESCE(SUM(CASE WHEN status='cancelled' THEN 1 ELSE 0 END), 0)   AS cancelled,
                COALESCE(AVG(CASE WHEN status='completed' THEN avg_speed END), 0)  AS avg_speed
            FROM downloads
            """
        ) as cursor:
            stats["totals"] = dict(await cursor.fetchone())

        async with self.conn.execute(
            """
            SELECT media_type, COUNT(*) AS count, COALESCE(SUM(file_size), 0) AS bytes
            FROM downloads WHERE status = 'completed'
            GROUP BY media_type ORDER BY bytes DESC
            """
        ) as cursor:
            stats["by_type"] = [dict(row) for row in await cursor.fetchall()]

        async with self.conn.execute(
            """
            SELECT COALESCE(username, CAST(user_id AS TEXT)) AS name,
                   COUNT(*) AS count, COALESCE(SUM(file_size), 0) AS bytes
            FROM downloads WHERE status = 'completed'
            GROUP BY user_id ORDER BY bytes DESC LIMIT 5
            """
        ) as cursor:
            stats["top_users"] = [dict(row) for row in await cursor.fetchall()]

        since = (datetime.now(UTC) - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        async with self.conn.execute(
            """
            SELECT COUNT(*) AS count, COALESCE(SUM(file_size), 0) AS bytes
            FROM downloads
            WHERE status = 'completed' AND created_at >= ?
            """,
            (since,),
        ) as cursor:
            stats["last_24h"] = dict(await cursor.fetchone())

        return stats
