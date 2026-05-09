from pydantic_settings import BaseSettings


class Settings(BaseSettings):
  # Kafka
  kafka_bootstrap_servers: str = "localhost:9092"
  kafka_topic_ticks: str = "raw_ticks"
  kafka_topic_heartbeats: str = "_heartbeats"
  kafka_topic_heartbeats_retention_ms: int = 60000
  kafka_topic_heartbeats_segment_ms: int = 60000
  # Lower than Kafka default (500 ms) — heartbeats arrive every 100 ms so
  # the broker rarely needs to wait long; tighter wait reduces watermark
  # advancement lag at the cost of slightly more fetch RPCs.
  kafka_fetch_max_wait_ms: int = 50

  # Delta Lake
  delta_candles_path: str = "s3://cryptalytics/candles"

  # S3
  s3_endpoint: str = ""
  s3_access_key: str = ""
  s3_secret_key: str = ""

  # Flink
  app_name: str = "tick-processor"
  watermark_out_of_orderness_ms: int = 500
  window_size_ms: int = 60000
  # Tightened from Flink default (200 ms). With a 100 ms heartbeat cadence,
  # 50 ms means the watermark generator emits at roughly the rate records
  # actually arrive.
  auto_watermark_interval_ms: int = 50
  # PyFlink batches records across the JVM ↔ Python boundary. Defaults
  # (1000 ms / 1000 records) hold records for up to a second before the
  # Python operator processes them — which also delays the watermark.
  # Tightening these to 50 ms / 100 records lets watermarks propagate
  # close to record-arrival speed.
  python_bundle_time_ms: int = 50
  python_bundle_size: int = 100

  # Heartbeat
  heartbeat_interval_ms: int = 100

  # Checkpoint
  checkpoint_path: str = "file:///data/delta/checkpoints/tick-processor"

  # Logging
  log_level: str = "INFO"

  model_config = {"env_prefix": "TICK_PROCESSOR_"}


settings = Settings()
