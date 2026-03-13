from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
  DecimalType,
  LongType,
  StringType,
  StructField,
  StructType,
)

from spark_streaming.settings import settings

# Must match pylib.model.tick.Tick serialized via model_dump_json()
TICK_SCHEMA = StructType(
  [
    StructField("symbol", StringType()),
    StructField("price", DecimalType(18, 8)),
    StructField("quantity", DecimalType(18, 8)),
    StructField("timestamp", LongType()),
    StructField("trade_id", LongType()),
  ]
)


def aggregate_to_candles(ticks_df: DataFrame) -> DataFrame:
  """Deduplicate and aggregate raw ticks into 1-minute OHLCV candles."""
  ticks = (
    ticks_df.withColumn("event_time", F.timestamp_millis("timestamp"))
    .withWatermark("event_time", settings.watermark_delay)
    .dropDuplicatesWithinWatermark(["trade_id"])
  )
  return (
    ticks.groupBy(F.col("symbol"), F.window("event_time", settings.window_duration))
    .agg(
      F.min_by("price", "timestamp").alias("open"),
      F.max("price").alias("high"),
      F.min("price").alias("low"),
      F.max_by("price", "timestamp").alias("close"),
      F.sum("quantity").alias("volume"),
      F.count("*").alias("trades"),
    )
    .filter(F.col("trades") > 0)
    .select(
      F.col("symbol"),
      F.unix_millis(F.col("window.start")).alias("start"),
      F.unix_millis(F.col("window.end")).alias("end"),
      F.col("open").cast(DecimalType(18, 8)),
      F.col("high").cast(DecimalType(18, 8)),
      F.col("low").cast(DecimalType(18, 8)),
      F.col("close").cast(DecimalType(18, 8)),
      F.col("volume").cast(DecimalType(18, 8)),
      F.col("trades"),
    )
  )
