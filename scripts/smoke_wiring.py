"""End-to-end wiring smoke test: boots the app like main.py with a stub
engine, exercises the health endpoint, submits a job, and shuts down.

Run: .venv/bin/python scripts/smoke_wiring.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiohttp import web

from app.bot_factory import build_bot, build_dispatcher
from app.config import Settings
from app.db import Database
from app.queue.engine import EngineResult
from app.queue.manager import DownloadManager
from app.services.filenaming import UserRef
from app.services.notifier import Notifier
from app.web.health import create_web_app

TOKEN = "1234567890:AAEhBOWe4ifRhnZ2dPq1fDz0r9RYmc8V4zQ"
PORT = 8901


async def fetch_health() -> dict:
    """Fetch /healthz from a worker thread (blocking urllib must never
    run on the event loop that also serves the request)."""
    import json
    import urllib.request

    def _get() -> dict:
        with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/healthz", timeout=3) as resp:
            return json.loads(resp.read())

    return await asyncio.to_thread(_get)


class SlowStubEngine:
    async def run(self, job):
        await asyncio.sleep(0.6)
        from pathlib import Path as _P

        target = _P(job.target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"smoke-payload")  # noqa: ASYNC240 — stub
        job.bytes_done = target.stat().st_size  # noqa: ASYNC240 — stub
        job.total_bytes = job.bytes_done
        return EngineResult(
            sha256="x" * 64, bytes_written=job.bytes_done,
            duration_ms=600, avg_speed=job.bytes_done / 0.6,
        )

    async def aclose(self):
        pass


async def main() -> int:
    tmp = Path("/tmp/tellybee-smoke")
    settings = Settings(
        bot_token=TOKEN,
        download_dir=tmp / "downloads",
        db_path=tmp / "data" / "tellybee.db",
        download_concurrency=2,
    )
    db = Database(settings.db_path)
    await db.connect()
    await db.seed_users({111}, set())

    bot = build_bot(settings)

    # Sandbox has no route to Telegram: stub the messaging surface so the
    # notifier can be exercised without real API calls.
    sent: list[str] = []

    async def fake_send(chat_id, text, **kw):  # noqa: ANN001, ANN003
        sent.append(text)
        return None

    async def fake_edit(chat_id, message_id, text, **kw):  # noqa: ANN001, ANN003
        sent.append(text)
        return None

    bot.send_message = fake_send  # type: ignore[method-assign]
    bot.edit_message_text = fake_edit  # type: ignore[method-assign]

    notifier = Notifier(bot, settings)
    manager = DownloadManager(bot, db, settings, notifier=notifier, engine=SlowStubEngine())
    dp = build_dispatcher(settings, db, manager)
    assert dp is not None

    await manager.start()
    app = create_web_app(manager)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", PORT)
    await site.start()

    # 1) health before any job
    body = await fetch_health()
    assert body["status"] == "ok", body
    assert body["queue"]["pending"] == 0
    print("✓ healthz ok:", body)

    # 2) submit a job and see it reflected
    from app.queue.manager import build_job

    target = tmp / "downloads" / "smoke.bin"
    job = build_job(
        user=UserRef(111, "smoker", "Smoke"),
        chat_id=42, message_id=1,
        media_type="document", file_id="f", file_unique_id="u",
        file_name="smoke.bin", file_size=1000,
        target_path=str(target), rel_path="smoke.bin",
    )
    await manager.submit(job)
    await asyncio.sleep(0.2)
    body = await fetch_health()
    print("✓ healthz during:", body)
    assert body["queue"]["active"] + body["queue"]["pending"] == 1, body

    # 3) wait for completion, check disk + DB
    await asyncio.sleep(0.7)
    assert target.exists()
    history = await db.user_history(111)
    assert history and history[0]["status"] == "completed", history
    stats = await db.global_stats()
    assert stats["totals"]["completed"] == 1, stats
    print("✓ job completed, db recorded, stats work")

    # 4) graceful shutdown
    await manager.shutdown()
    await notifier.aclose()
    await runner.cleanup()
    await bot.session.close()
    await db.close()
    print("✓ graceful shutdown clean")
    print("\nALL SMOKE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
