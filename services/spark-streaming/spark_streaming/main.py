import pyspark
import structlog
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

from spark_streaming.aggregator import TICK_SCHEMA, aggregate_to_candles
from spark_streaming.settings import settings

structlog.configure(
  wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)
log = structlog.get_logger()


class BatchLogger(StreamingQueryListener):
  def onQueryStarted(self, event):
    log.info("Query started", query_id=str(event.id))

  def onQueryProgress(self, event):
    p = event.progress
    log.info(
      "Batch progress",
      batch_id=p.batchId,
      num_input_rows=p.numInputRows,
      input_rows_per_second=round(p.inputRowsPerSecond, 2),
      processed_rows_per_second=round(p.processedRowsPerSecond, 2),
    )

  def onQueryTerminated(self, event):
    log.info("Query terminated", query_id=str(event.id))

  def onQueryIdle(self, event):
    pass


def create_spark_session() -> pyspark.sql.SparkSession:
  builder = (
    pyspark.sql.SparkSession.builder.appName(settings.app_name)
    .config(
      "spark.sql.extensions",
      "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
      "spark.sql.catalog.spark_catalog",
      "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.jars.ivy", "/opt/spark/.ivy2")
    .config(
      "spark.jars.packages",
      "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,io.delta:delta-spark_2.13:4.0.1",
    )
    .config("spark.sql.shuffle.partitions", settings.shuffle_partitions)
  )

  if settings.s3_endpoint:
    builder = (
      builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
      .config("spark.hadoop.fs.s3a.access.key", settings.s3_access_key)
      .config("spark.hadoop.fs.s3a.secret.key", settings.s3_secret_key)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
      )
    )

  return builder.getOrCreate()


def main():
  spark = create_spark_session()
  spark.sparkContext.setLogLevel(settings.spark_log_level)
  spark.streams.addListener(BatchLogger())

  log.info(
    "Starting spark streaming",
    kafka=settings.kafka_bootstrap_servers,
    topic=settings.kafka_topic_ticks,
    delta_path=settings.delta_candles_path,
  )

  raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
    .option("subscribe", settings.kafka_topic_ticks)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
  )

  ticks_df = (
    raw_stream.selectExpr("CAST(value AS STRING) as json_str")
    .select(F.from_json(F.col("json_str"), TICK_SCHEMA).alias("tick"))
    .select("tick.*")
  )

  candles_df = aggregate_to_candles(ticks_df)

  query = (
    candles_df.writeStream.format("delta")
    .outputMode("append")
    .trigger(processingTime=settings.trigger_interval)
    .option("checkpointLocation", settings.checkpoint_path)
    .start(settings.delta_candles_path)
  )

  query.awaitTermination()


if __name__ == "__main__":
  main()
