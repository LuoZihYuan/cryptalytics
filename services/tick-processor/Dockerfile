FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
ENV UV_NO_DEV=1
ENV UV_PYTHON_DOWNLOADS=0
WORKDIR /app/services/tick-processor
COPY shared/pylib /app/shared/pylib
COPY services/tick-processor/pyproject.toml services/tick-processor/uv.lock ./
RUN sed -i 's/, editable = true//' pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project
COPY services/tick-processor/tick_processor ./tick_processor
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen
RUN apt-get update && apt-get install -y --no-install-recommends wget \
    && wget -q -P /app/services/tick-processor/.venv/lib/python3.12/site-packages/pyflink/lib/ \
       https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar \
    && rm -rf /var/lib/apt/lists/*

FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot
COPY --from=builder --chown=nonroot:nonroot /app/services/tick-processor /app
COPY --from=builder --chown=nonroot:nonroot /app/shared/pylib /app/shared/pylib
RUN mkdir -p /data/delta && chmod 777 /data/delta
ENV PATH="/app/.venv/bin:$PATH"
USER nonroot
WORKDIR /app
ENTRYPOINT ["python", "-m", "tick_processor.main"]
