# syntax=docker/dockerfile:1

# ── Stage 1: build a slim virtualenv with uv ────────────────────────────────
FROM python:3.12-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.13 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Install dependencies first for optimal layer caching.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Then the application itself.
COPY README.md pyproject.toml uv.lock ./
COPY app ./app
RUN uv sync --frozen --no-dev

# ── Stage 2: minimal runtime image ──────────────────────────────────────────
FROM python:3.12-slim

# Run as an unprivileged user.
RUN groupadd --gid 1000 tellybee \
    && useradd --uid 1000 --gid tellybee --shell /bin/bash --create-home tellybee

WORKDIR /app
COPY --from=builder --chown=tellybee:tellybee /app/.venv /app/.venv
COPY --chown=tellybee:tellybee app ./app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DOWNLOAD_DIR=/app/downloads \
    DB_PATH=/app/data/tellybee.db

RUN mkdir -p /app/downloads /app/data \
    && chown -R tellybee:tellybee /app/downloads /app/data

USER tellybee

# Health endpoint (works in both polling and webhook modes).
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["python", "-c", "import urllib.request, sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/healthz', timeout=3).status == 200 else 1)"]

CMD ["python", "-m", "app.main"]
