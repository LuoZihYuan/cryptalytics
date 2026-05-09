import asyncio
import json
import time
from dataclasses import dataclass

import pyarrow as pa
import structlog
from pyflink.common import Configuration, Duration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import ProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

from pylib.model.tick import Tick
from pylib.repository.delta import CANDLE_SCHEMA, DeltaRepository
from tick_processor.aggregator import CandleAggregator
from tick_processor.heartbeat import (
  ensure_heartbeat_topic_blocking,
  start_heartbeat_thread,
)
from tick_processor.settings import settings

structlog.configure(
  wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)
log = structlog.get_logger()


@dataclass
class Heartbeat:
  """Internal sentinel — drives the watermark when real ticks are sparse."""

  timestamp: int


def parse_record(raw: str):
  """Deserialize a Kafka record into a Tick or Heartbeat.

  Both types expose a `.timestamp` attribute (Unix ms), which the watermark
  strategy uses uniformly. Heartbeats are filtered out downstream before
  reaching the aggregator.
  """
  data = json.loads(raw)
  if data.get("type") == "heartbeat":
    return Heartbeat(timestamp=int(data["timestamp"]))
  return Tick.model_validate(data)


class _TimestampAssigner(TimestampAssigner):
  def extract_timestamp(self, value, record_timestamp: int) -> int:
    return value.timestamp


class DeltaSink(ProcessFunction):
  def __init__(self, table_path: str, storage_options: dict):
    self._table_path = table_path
    self._storage_options = storage_options
    self._repo = None

  def process_element(self, candle: dict, ctx):
    received_ms = int(time.time() * 1000)

    if self._repo is None:
      self._repo = DeltaRepository(self._table_path, self._storage_options)

    table = pa.table(
      {
        "symbol": [candle["symbol"]],
        "start": [candle["start"]],
        "end": [candle["end"]],
        "open": pa.array([candle["open"]], type=pa.decimal128(18, 8)),
        "high": pa.array([candle["high"]], type=pa.decimal128(18, 8)),
        "low": pa.array([candle["low"]], type=pa.decimal128(18, 8)),
        "close": pa.array([candle["close"]], type=pa.decimal128(18, 8)),
        "volume": pa.array([candle["volume"]], type=pa.decimal128(18, 8)),
        "trades": [candle["trades"]],
      },
      schema=CANDLE_SCHEMA,
    )

    write_started_ms = int(time.time() * 1000)
    asyncio.run(self._repo.save_table(table))
    write_completed_ms = int(time.time() * 1000)

    log.info(
      "Candle written",
      symbol=candle["symbol"],
      start=candle["start"],
      sink_overhead_ms=write_started_ms - received_ms,
      write_ms=write_completed_ms - write_started_ms,
      total_sink_ms=write_completed_ms - received_ms,
    )


def create_env() -> StreamExecutionEnvironment:
  config = Configuration()
  config.set_string("state.checkpoints.dir", settings.checkpoint_path)
  # Tighten watermark emission cadence (default 200 ms). With a 500 ms
  # out-of-orderness tolerance and 100 ms heartbeat cadence, a 50 ms
  # emit interval lets the watermark advance close to the rate that
  # records actually arrive, rather than in 200 ms steps.
  config.set_string(
    "pipeline.auto-watermark-interval",
    f"{settings.auto_watermark_interval_ms} ms",
  )
  # PyFlink micro-batches records across the JVM ↔ Python boundary.
  # Lowering bundle-time and bundle-size shortens the buffering delay
  # between record arrival in the JVM and processing in Python, which
  # lets watermarks propagate to the keyed operator faster.
  config.set_string(
    "python.fn-execution.bundle-time", str(settings.python_bundle_time_ms)
  )
  config.set_string("python.fn-execution.bundle-size", str(settings.python_bundle_size))
  env = StreamExecutionEnvironment.get_execution_environment(config)
  env.set_parallelism(1)
  env.enable_checkpointing(60000)
  return env


def create_kafka_source() -> KafkaSource:
  """Single source reading both ticks and heartbeats.

  Watermarks are applied later, on the parsed stream — heartbeats and ticks
  contribute equally to watermark advancement, but only ticks reach the
  aggregator.

  Lower `fetch.max.wait.ms` than the default (500 ms) so the consumer
  doesn't sit waiting up to 500 ms when small numbers of records are
  available — heartbeats are tiny and arrive every 100 ms, so there's
  always something to fetch immediately.
  """
  return (
    KafkaSource.builder()
    .set_bootstrap_servers(settings.kafka_bootstrap_servers)
    .set_topics(settings.kafka_topic_ticks, settings.kafka_topic_heartbeats)
    .set_group_id(settings.app_name)
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .set_property("fetch.max.wait.ms", str(settings.kafka_fetch_max_wait_ms))
    .set_property("fetch.min.bytes", "1")
    .build()
  )


def main():
  storage_options = {}
  if settings.s3_endpoint:
    storage_options = {
      "AWS_ENDPOINT_URL": settings.s3_endpoint,
      "AWS_ACCESS_KEY_ID": settings.s3_access_key,
      "AWS_SECRET_ACCESS_KEY": settings.s3_secret_key,
      "AWS_ALLOW_HTTP": "true",
    }

  # Provision the heartbeat topic up-front so the Kafka source doesn't
  # autocreate it with default retention.
  ensure_heartbeat_topic_blocking()

  # Start the heartbeat publisher on a background thread before env.execute()
  # blocks the main thread on the JVM.
  _, heartbeat_stop = start_heartbeat_thread()

  env = create_env()
  kafka_source = create_kafka_source()

  log.info(
    "Starting tick processor",
    kafka=settings.kafka_bootstrap_servers,
    ticks_topic=settings.kafka_topic_ticks,
    heartbeats_topic=settings.kafka_topic_heartbeats,
    delta_path=settings.delta_candles_path,
    out_of_orderness_ms=settings.watermark_out_of_orderness_ms,
    heartbeat_interval_ms=settings.heartbeat_interval_ms,
    auto_watermark_interval_ms=settings.auto_watermark_interval_ms,
    kafka_fetch_max_wait_ms=settings.kafka_fetch_max_wait_ms,
    python_bundle_time_ms=settings.python_bundle_time_ms,
    python_bundle_size=settings.python_bundle_size,
  )

  watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_millis(settings.watermark_out_of_orderness_ms)
  ).with_timestamp_assigner(_TimestampAssigner())

  (
    env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "kafka-source")
    .map(parse_record, output_type=Types.PICKLED_BYTE_ARRAY())
    .assign_timestamps_and_watermarks(watermark_strategy)
    .filter(lambda r: isinstance(r, Tick))
    .key_by(lambda tick: tick.symbol)
    .process(CandleAggregator())
    .process(DeltaSink(settings.delta_candles_path, storage_options))
  )

  try:
    env.execute(settings.app_name)
  finally:
    heartbeat_stop.set()


if __name__ == "__main__":
  main()
