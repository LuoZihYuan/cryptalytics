from pydantic_settings import BaseSettings


class Settings(BaseSettings):
  # Binance
  binance_rest_url: str = "https://api.binance.us/api/v3"

  # Delta Lake
  delta_candles_path: str = "s3://cryptalytics/candles"
  delta_storage_options: dict = {}

  # Logging
  log_level: str = "INFO"

  model_config = {"env_prefix": "BACKFILL_"}


settings = Settings()
