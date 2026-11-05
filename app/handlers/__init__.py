"""Router composition: order matters.

``common``     — command handlers (registered first so commands win)
``admin``      — admin commands, guarded by the IsAdmin router filter
``callbacks``  — inline-keyboard actions for the window + reboot report
``links``      — direct-link intake (/link + URLs in plain text)
``media``      — media intake (broad filter) + fallback nudge, registered last
"""

from __future__ import annotations

from aiogram import Router

from app.handlers import admin, callbacks, common, links, media


def build_router() -> Router:
    root = Router(name="tellybee")
    root.include_router(common.router)
    root.include_router(admin.router)
    root.include_router(callbacks.router)
    root.include_router(links.router)
    root.include_router(media.router)
    return root
