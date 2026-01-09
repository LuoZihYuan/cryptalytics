FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder
# Compile Python to bytecode for faster startup; copy files instead of linking
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
# Exclude dev dependencies from production image
ENV UV_NO_DEV=1
# Use system Python instead of downloading managed Python
ENV UV_PYTHON_DOWNLOADS=0
WORKDIR /app/services/backfill
COPY shared/pylib /app/shared/pylib
COPY services/backfill/pyproject.toml services/backfill/uv.lock ./
# Remove editable flag so pylib is copied to site-packages, not symlinked
RUN sed -i 's/, editable = true//' pyproject.toml
# Install dependencies only (cached layer)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project
COPY services/backfill/backfill ./backfill
# Install project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked

FROM python:3.13-slim-bookworm
RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot
COPY --from=builder --chown=nonroot:nonroot /app/services/backfill /app
ENV PATH="/app/.venv/bin:$PATH"
USER nonroot
WORKDIR /app
ENTRYPOINT ["python", "-m", "backfill.main"]