"""Polling resilience + the local Bot API cloud-logOut handshake.

The failure these tests pin down (seen live): aiogram's start_polling exits
on the first network error — bot pointed at a still-starting (or
unresolvable) local Bot API server idles forever while looking "alive".
The supervisor must retry transient errors, give up only on auth errors,
and the logOut handshake must never block startup.
"""

import asyncio

import pytest
from aiogram.exceptions import (
    TelegramConflictError,
    TelegramNetworkError,
    TelegramUnauthorizedError,
)
from aiohttp import web
from aiohttp.test_utils import TestServer

from app.bot_factory import cloud_logout
from app.main import _poll_forever

# ── cloud_logout ────────────────────────────────────────────────────────────


def _net_error(message: str) -> TelegramNetworkError:
    """Build a TelegramNetworkError without a real API method object."""
    return TelegramNetworkError(method=None, message=message)  # type: ignore[arg-type]


def _conflict_error(message: str) -> TelegramConflictError:
    return TelegramConflictError(method=None, message=message)  # type: ignore[arg-type]


def _auth_error(message: str) -> TelegramUnauthorizedError:
    return TelegramUnauthorizedError(method=None, message=message)  # type: ignore[arg-type]


async def test_cloud_logout_acknowledged():
    seen = {}

    async def handler(request: web.Request) -> web.Response:
        seen["path"] = request.path
        seen["method"] = request.method
        return web.json_response({"ok": True, "result": True})

    app = web.Application()
    app.router.add_post("/bot123:ABC/logOut", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        ok = await cloud_logout("123:ABC", base=str(server.make_url("/")))
        assert ok is True
        assert seen["path"] == "/bot123:ABC/logOut"
        assert seen["method"] == "POST"
    finally:
        await server.close()


async def test_cloud_logout_already_logged_out_is_fine():
    async def handler(request: web.Request) -> web.Response:
        return web.json_response(
            {"ok": False, "error_code": 404, "description": "Not Found"}, status=404
        )

    app = web.Application()
    app.router.add_post("/bot123:ABC/logOut", handler)
    server = TestServer(app)
    await server.start_server()
    try:
        assert await cloud_logout("123:ABC", base=str(server.make_url("/"))) is False
    finally:
        await server.close()


async def test_cloud_logout_unreachable_never_raises():
    # Nothing listens on this port → connection refused → must return False.
    assert await cloud_logout("123:ABC", base="http://127.0.0.1:1") is False


# ── _poll_forever supervisor ────────────────────────────────────────────────


async def test_poll_forever_retries_transient_errors_then_recovers():
    stop = asyncio.Event()
    calls = {"n": 0}

    async def poll() -> None:
        calls["n"] += 1
        if calls["n"] == 1:
            raise _net_error("DNS is down")
        if calls["n"] == 2:
            raise _conflict_error("409: terminated by other getUpdates")
        await stop.wait()  # third attempt: healthy polling

    task = asyncio.create_task(_poll_forever(poll, stop, base_delay=0.01, max_delay=0.02))
    await asyncio.sleep(0.1)
    assert calls["n"] == 3  # two failures retried, third run is live
    assert not task.done()  # supervisor still supervising
    stop.set()
    await asyncio.wait_for(task, timeout=2)


async def test_poll_forever_fatal_auth_error_triggers_shutdown():
    stop = asyncio.Event()

    async def poll() -> None:
        raise _auth_error("Unauthorized")

    await asyncio.wait_for(_poll_forever(poll, stop, base_delay=0.01), timeout=2)
    assert stop.is_set()  # process would exit, container restarts


async def test_poll_forever_unexpected_error_triggers_shutdown():
    stop = asyncio.Event()

    async def poll() -> None:
        raise RuntimeError("bug in supervisor-land")

    await asyncio.wait_for(_poll_forever(poll, stop, base_delay=0.01), timeout=2)
    assert stop.is_set()


async def test_poll_forever_clean_exit_sets_stop():
    stop = asyncio.Event()

    async def poll() -> None:
        return None  # start_polling returning cleanly means shutdown

    await asyncio.wait_for(_poll_forever(poll, stop, base_delay=0.01), timeout=2)
    assert stop.is_set()


async def test_poll_forever_cancellation_propagates():
    stop = asyncio.Event()

    async def poll() -> None:
        await asyncio.sleep(10)  # blocks until cancelled

    task = asyncio.create_task(_poll_forever(poll, stop, base_delay=0.01))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(task), timeout=2)
