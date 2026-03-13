from pydantic_settings import BaseSettings


class Settings(BaseSettings):
  # Kafka
  kafka_bootstrap_servers: str = "localhost:9092"
  kafka_topic_ticks: str = "raw_ticks"

  # Delta Lake
  delta_candles_path: str = "s3://cryptalytics/candles"

  # S3
  s3_endpoint: str = ""
  s3_access_key: str = ""
  s3_secret_key: str = ""

  # Spark
  app_name: str = "cryptalytics-streaming"
  spark_log_level: str = "WARN"
  shuffle_partitions: int = 8
  trigger_interval: str = "30 seconds"

  # Watermark & window
  watermark_delay: str = "30 seconds"
  window_duration: str = "1 minute"

  # Checkpoint
  checkpoint_path: str = "s3://cryptalytics/checkpoints/spark-streaming"

  # Logging
  log_level: str = "INFO"

  model_config = {"env_prefix": "SPARK_STREAMING_"}


settings = Settings()
