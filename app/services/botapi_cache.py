"""Self-service housekeeping for the local Bot API server's file cache.

Why this exists
---------------

In ``--local`` mode every file the bot downloads is written *twice*: once
by the telegram-bot-api server into its own storage (the ``botapi-data``
volume) and once by the bot into ``./downloads``. The server's copy is pure
cache — but its retention is server-controlled and NOT configurable:
per the maintainer (tdlib/telegram-bot-api issue #303, Jul 2022) files are
"guaranteed to be available for one hour after download, but can be kept
for about 25 hours", deletion after an hour "is an implementation detail",
and — the part this module relies on — "you are free to delete the files
manually". Left alone, a busy queue (tens of GB per day) therefore slowly
fills the volume until someone cleans it by hand.

What TellyBee does about it (v0.7+)
-----------------------------------

* **Evict after copy** — as soon as a worker has safely copied a file into
  ``./downloads`` (atomic rename done), the server-side copy is deleted
  immediately. The cache then only ever holds *in-flight* fetches, so a
  4 GB download needs ~4 GB of disk instead of ~8 GB.
* **Sweeper** — a periodic pass removes *leftovers*: files of jobs that
  died (crash, restart, cancel), never got copied, or were produced by a
  previous run. Only files older than ``BOTAPI_CACHE_TTL_MINUTES`` are
  candidates, and a file must additionally be unchanged across two
  sweeps — a file that is still growing is an active download, whatever
  the bot's own bookkeeping thinks of it.

Safety rails
------------

* Deletes only inside the bot's own subtree of the server's storage, under
  the ``files/`` directory the getFile paths live in — TDLib's per-bot
  database/binlogs in the same volume are never touched, matching the
  maintainer's "only temp files and the working directory itself must not
  be accessed" rule (``/temp`` is a separate directory and not ours).
* Paths are never logged: they embed the bot token as a directory name.
* A read-only mount (the pre-0.7 compose default) is detected on the first
  failed delete; one clear warning is logged and cleanup quietly disables
  itself — the server's own 1–25 h expiry remains the only retention.
"""

from __future__ import annotations

import asyncio
import contextlib
import errno
import logging
import os
import stat
import time
from collections.abc import Callable
from pathlib import Path

from app.utils.sizes import human_size

__all__ = ["BotApiCacheCleaner"]

logger = logging.getLogger(__name__)

#: Deleting with any of these errnos means the mount forbids writes —
#: log the remedy once and stop trying (no retry storms).
_PERMANENT_ERRNOS = frozenset({errno.EROFS, errno.EACCES, errno.EPERM})


def _errcode(err: OSError) -> str:
    """errno name without the OSError text — the text embeds the file path,
    and every path under the server's storage contains the bot token."""
    return errno.errorcode.get(err.errno) or f"errno {err.errno}"


class BotApiCacheCleaner:
    """Deletes served files out of the local Bot API server's cache.

    The engine feeds it: ``observe()`` on every real getFile path (which
    also teaches it where the ``files/`` subtree lives), ``evict_now()``
    after each successful copy, and ``run()`` as a background sweeper.
    """

    def __init__(
        self,
        root: Path,
        *,
        ttl_seconds: float,
        interval_seconds: float,
        evict_after_copy: bool = True,
        protected: Callable[[], set[str]] | None = None,
    ) -> None:
        self.root = Path(root)
        #: Whether ``evict_now`` may delete (mirrors BOTAPI_EVICT_AFTER_COPY).
        self.evict_after_copy = evict_after_copy
        self._ttl = max(0.0, ttl_seconds)
        self._interval = interval_seconds
        #: Callback the engine provides: paths that must survive a sweep
        #: (in-flight watcher matches + still-valid file_id→path cache).
        self._protected = protected
        #: Observed "files" roots (learned from real getFile paths).
        self._derived: set[Path] = set()
        #: path → size at the previous sweep; unchanged+old entries die.
        self._prev: dict[str, int] = {}
        self._disabled = False
        self._warned = False
        self.evicted_files = 0
        self.evicted_bytes = 0
        self.swept_files = 0
        self.swept_bytes = 0

    # ── engine feed points ──────────────────────────────────────────────

    def observe(self, path: str) -> None:
        """Register a path getFile just returned / the watcher matched.

        Doubles as layout discovery: the first observed path under
        ``<root>/files/...`` unlocks directory-sweeping for that subtree;
        anything the server stores elsewhere is never swept.
        """
        if self._disabled or not path:
            return
        try:
            rel = Path(path).relative_to(self.root)
        except ValueError:
            return
        if len(rel.parts) > 1 and rel.parts[0] == "files":
            self._derived.add(self.root / "files")

    def evict_now(self, path: str, *, reason: str = "copied to downloads") -> bool:
        """Delete one served file immediately. True when it is gone.

        Called only with paths getFile itself returned, so scope is safe by
        construction; the root containment check is belt and suspenders.
        """
        if self._disabled or not self.evict_after_copy or not path:
            return False
        if not self._within_root(path):
            return False
        size = 0
        with contextlib.suppress(OSError):
            size = os.stat(path).st_size
        if self._unlink(path):
            self.evicted_files += 1
            self.evicted_bytes += size
            logger.debug(
                "botapi cache: deleted the server-side copy (%s) — %d file(s), %s total",
                reason, self.evicted_files, human_size(self.evicted_bytes),
            )
            return True
        return False

    async def run(self) -> None:
        """Sweep forever (until cancelled); never raises, never exits early.

        The first pass only *records* file sizes; deletion needs an entry to
        be unchanged across two passes, so nothing is ever deleted on the
        basis of a single observation.
        """
        if self._ttl <= 0.0:
            return
        interval = max(30.0, self._interval)
        while True:
            await asyncio.sleep(interval)
            try:
                protected = (
                    frozenset(self._protected() or set())
                    if self._protected is not None
                    else frozenset()
                )
                await asyncio.to_thread(self._sweep_once, protected)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001 — the sweeper must survive
                logger.warning(
                    "botapi cache sweep failed unexpectedly (%s); retrying next interval",
                    type(err).__name__,
                )
                logger.debug("botapi cache sweep failure detail", exc_info=err)

    # ── internals ───────────────────────────────────────────────────────

    def _within_root(self, path: str) -> bool:
        try:
            Path(path).relative_to(self.root)
        except ValueError:
            return False
        return True

    def _sweep_once(self, protected: frozenset[str]) -> tuple[int, int]:
        """One TTL pass. Blocking (stat storm) — call via ``to_thread``."""
        if self._ttl <= 0.0 or self._disabled or not self._derived:
            return (0, 0)
        cutoff = time.time() - self._ttl
        prev = self._prev
        fresh: dict[str, int] = {}
        deleted = freed = 0
        for start_dir in sorted(self._derived):
            if not start_dir.is_dir():  # noqa: ASYNC240 — background thread
                continue
            for dirpath, _dirs, files in os.walk(start_dir):
                depth = dirpath[len(str(start_dir)) :].count(os.sep)
                if depth >= 4:
                    continue
                for name in files:
                    path = os.path.join(dirpath, name)
                    try:
                        st = os.stat(path)
                    except OSError:
                        continue
                    if not stat.S_ISREG(st.st_mode):
                        continue
                    fresh[path] = st.st_size
                    if path in protected:
                        continue  # in-flight / still-mapped — hands off
                    if st.st_mtime > cutoff:
                        continue  # younger than the TTL
                    if prev.get(path) != st.st_size:
                        continue  # new here, or still growing — not proven dead
                    if self._unlink(path):
                        deleted += 1
                        freed += st.st_size
                        fresh.pop(path, None)
        self._prev = fresh
        if deleted:
            self.swept_files += deleted
            self.swept_bytes += freed
            logger.info(
                "botapi cache sweeper: removed %d stale file(s), freed %s — "
                "%d tracked, %d protected entries kept",
                deleted, human_size(freed), len(fresh), len(protected),
            )
        return (deleted, freed)

    def _unlink(self, path: str) -> bool:
        try:
            os.unlink(path)
        except FileNotFoundError:
            return False
        except OSError as err:
            if err.errno in _PERMANENT_ERRNOS:
                self._disable(err)
            else:
                logger.debug(
                    "botapi cache: could not remove a stale entry (%s)",
                    _errcode(err),
                )
            return False
        return True

    def _disable(self, err: OSError) -> None:
        """Give up cleanly on a write-forbidden volume, warn exactly once."""
        self._disabled = True
        if not self._warned:
            self._warned = True
            logger.warning(
                "botapi cache cleanup is not permitted (%s) — the shared volume "
                "is mounted read-only. Mount it read-write: drop ':ro' from the "
                "botapi-data line of the tellybee service in docker-compose.yml "
                "and recreate the container. Until then the Bot API server's own "
                "1–25 h expiry is the only thing keeping the volume from filling",
                _errcode(err),
            )
