"""HTTP surface: /healthz & /readyz always; webhook route when configured.

A single aiohttp app serves everything on LISTEN_HOST:LISTEN_PORT, so the
docker healthcheck and uptime monitors work in both polling and webhook
modes.
"""

from __future__ import annotations

import logging
import time

from aiohttp import web

from app.queue.manager import DownloadManager

logger = logging.getLogger(__name__)


def create_web_app(manager: DownloadManager) -> web.Application:
    app = web.Application()
    started = time.monotonic()

    async def health(_: web.Request) -> web.Response:
        snapshot = manager.snapshot()
        return web.json_response(
            {
                "status": "ok",
                "uptime_seconds": round(time.monotonic() - started, 1),
                "queue": {
                    "active": len(snapshot.active),
                    "pending": len(snapshot.pending),
                    "paused": snapshot.paused,
                },
                "workers": {
                    "total": snapshot.workers_total,
                    "busy": snapshot.workers_busy,
                },
                "session": {
                    "completed": snapshot.completed,
                    "failed": snapshot.failed,
                    "cancelled": snapshot.cancelled,
                },
            }
        )

    async def ready(_: web.Request) -> web.Response:
        return web.json_response({"ready": True})

    app.router.add_get("/healthz", health)
    app.router.add_get("/readyz", ready)
    return app


def attach_webhook(app: web.Application, dispatcher, bot, path: str, secret: str) -> None:
    """Register the aiogram webhook request handler on an existing app."""
    from aiogram.webhook.aiohttp_server import SimpleRequestHandler

    handler = SimpleRequestHandler(dispatcher=dispatcher, bot=bot, secret_token=secret or None)
    handler.register(app, path=path)
    logger.info("webhook endpoint mounted at %s", path)
