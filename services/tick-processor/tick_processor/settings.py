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

  # Flink
  app_name: str = "tick-processor"
  watermark_delay_ms: int = 5000
  window_size_ms: int = 60000

  # Checkpoint
  checkpoint_path: str = "file:///data/delta/checkpoints/tick-processor"

  # Logging
  log_level: str = "INFO"

  model_config = {"env_prefix": "TICK_PROCESSOR_"}


settings = Settings()
