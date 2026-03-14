import asyncio

import pyarrow as pa
import structlog
from pyflink.common import Configuration, WatermarkStrategy
from pyflink.datastream import ProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.serialization import SimpleStringSchema

from pylib.model.tick import Tick
from pylib.repository.delta import CANDLE_SCHEMA, DeltaRepository
from tick_processor.aggregator import CandleAggregator
from tick_processor.settings import settings

structlog.configure(
  wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)
log = structlog.get_logger()


class DeltaSink(ProcessFunction):
  def __init__(self, table_path: str, storage_options: dict):
    self._table_path = table_path
    self._storage_options = storage_options
    self._repo = None

  def process_element(self, candle: dict, ctx):
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
    asyncio.run(self._repo.save_table(table))
    log.info("Candle written", symbol=candle["symbol"], start=candle["start"])


def create_env() -> StreamExecutionEnvironment:
  config = Configuration()
  config.set_string("state.checkpoints.dir", settings.checkpoint_path)
  env = StreamExecutionEnvironment.get_execution_environment(config)
  env.set_parallelism(1)
  env.enable_checkpointing(60000)
  return env


def create_kafka_source() -> KafkaSource:
  return (
    KafkaSource.builder()
    .set_bootstrap_servers(settings.kafka_bootstrap_servers)
    .set_topics(settings.kafka_topic_ticks)
    .set_group_id(settings.app_name)
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
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

  env = create_env()
  kafka_source = create_kafka_source()

  log.info(
    "Starting tick processor",
    kafka=settings.kafka_bootstrap_servers,
    topic=settings.kafka_topic_ticks,
    delta_path=settings.delta_candles_path,
  )

  (
    env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "kafka-source")
    .map(lambda raw: Tick.model_validate_json(raw))
    .key_by(lambda tick: tick.symbol)
    .process(CandleAggregator())
    .process(DeltaSink(settings.delta_candles_path, storage_options))
  )

  env.execute(settings.app_name)


if __name__ == "__main__":
  main()
