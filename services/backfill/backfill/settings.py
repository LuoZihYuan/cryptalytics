from pydantic_settings import BaseSettings


class Settings(BaseSettings):
  # Binance
  binance_bulk_url: str = "https://data.binance.us/public_data/spot"

  # Delta Lake
  delta_candles_path: str = "s3://cryptalytics/candles"
  delta_storage_options: dict = {}

  # Concurrency
  download_concurrency: int = 10

  # Retry
  max_retries: int = 3
  retry_base_delay: float = 10

  # Logging
  log_level: str = "INFO"

  model_config = {"env_prefix": "BACKFILL_"}


settings = Settings()
