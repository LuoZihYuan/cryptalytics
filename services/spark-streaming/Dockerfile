FROM apache/spark:4.0.2-python3 AS builder
USER root
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
WORKDIR /opt/spark/work-dir
COPY services/spark-streaming/pyproject.toml services/spark-streaming/uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev
RUN /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 \
    --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/opt/spark/.ivy2 -Divy.home=/opt/spark/.ivy2" \
    --class org.apache.spark.version /dev/null 2>/dev/null; exit 0
COPY services/spark-streaming/spark_streaming ./spark_streaming
COPY services/spark-streaming/scripts ./scripts

FROM apache/spark:4.0.2-python3
USER root
RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot
COPY --from=builder --chown=nonroot:nonroot /opt/spark/work-dir/.venv /opt/spark/work-dir/.venv
COPY --from=builder --chown=nonroot:nonroot /opt/spark/work-dir/spark_streaming /opt/spark/work-dir/spark_streaming
COPY --from=builder --chown=nonroot:nonroot /opt/spark/work-dir/scripts /opt/spark/work-dir/scripts
COPY --from=builder --chown=nonroot:nonroot /opt/spark/.ivy2 /opt/spark/.ivy2
RUN chmod -R 777 /opt/spark/.ivy2 && mkdir -p /data/delta && chmod 777 /data/delta
ENV PATH="/opt/spark/work-dir/.venv/bin:$PATH"
USER nonroot
WORKDIR /opt/spark/work-dir
ENTRYPOINT ["python", "-m", "spark_streaming.main"]