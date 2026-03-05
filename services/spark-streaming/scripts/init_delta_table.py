"""Create an empty Delta table with delta-spark protocol if it doesn't exist."""

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

CANDLE_SCHEMA = StructType(
  [
    StructField("symbol", StringType()),
    StructField("start", LongType()),
    StructField("end", LongType()),
    StructField("open", DecimalType(18, 8)),
    StructField("high", DecimalType(18, 8)),
    StructField("low", DecimalType(18, 8)),
    StructField("close", DecimalType(18, 8)),
    StructField("volume", DecimalType(18, 8)),
    StructField("trades", LongType()),
  ]
)

TABLE_PATH = "/data/delta/candles"

builder = (
  pyspark.sql.SparkSession.builder.appName("delta-init")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config(
    "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )
  .config("spark.jars.ivy", "/opt/spark/.ivy2")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
  spark.read.format("delta").load(TABLE_PATH)
  print("Delta table already exists")
except Exception:
  spark.createDataFrame([], CANDLE_SCHEMA).write.format("delta").save(TABLE_PATH)
  print("Delta table created")

spark.stop()
