from pydantic_settings import BaseSettings


class Settings(BaseSettings):
  # Binance
  binance_ws_url: str = "wss://stream.binance.us:9443/ws"
  binance_rest_url: str = "https://api.binance.us/api/v3"

  # Kafka
  kafka_bootstrap_servers: str = "localhost:9092"
  kafka_topic_ticks: str = "raw_ticks"

  # Delta Lake
  delta_candles_path: str = "s3://cryptalytics/candles"
  delta_storage_options: dict = {}

  # Airflow
  airflow_base_url: str = "http://airflow-webserver:8080"
  airflow_dag_id: str = "symbol_onboarding"
  airflow_task_id: str = "realtime_ready"

  # gRPC
  grpc_port: int = 50051

  # Logging
  log_level: str = "INFO"

  model_config = {"env_prefix": "INGESTION_"}


settings = Settings()
