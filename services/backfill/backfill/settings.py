from pydantic_settings import BaseSettings


class Settings(BaseSettings):
  # Binance
  binance_bulk_url: str = "https://data.binance.us/public_data/spot/daily/klines"

  # Delta Lake
  delta_candles_path: str = "s3://cryptalytics/candles"
  delta_storage_options: dict = {}

  # Logging
  log_level: str = "INFO"

  model_config = {"env_prefix": "BACKFILL_"}


settings = Settings()
