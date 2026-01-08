from datetime import datetime, timezone

import httpx
import structlog

from ingestion.settings import settings
from pylib.model.candle import Candle
from pylib.repository.delta import DeltaRepository

log = structlog.get_logger()


class RestService:
  def __init__(self, delta_repository: DeltaRepository):
    self.delta_repository = delta_repository
    self.client: httpx.AsyncClient | None = None

  async def start(self):
    self.client = httpx.AsyncClient(base_url=settings.binance_rest_url)
    log.info("REST service started")

  async def stop(self):
    if self.client:
      await self.client.aclose()
      log.info("REST service stopped")

  async def fetch_and_save_same_day_candles(
    self, symbol: str, until_timestamp: int
  ) -> int:
    """
    Fetch 1m candles from midnight until the given timestamp
    and save directly to Delta Lake.

    Returns:
      Number of candles saved
    """
    if not self.client:
      raise RuntimeError("Client not started")

    now = datetime.fromtimestamp(until_timestamp / 1000, tz=timezone.utc)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_timestamp = int(midnight.timestamp() * 1000)

    log.info(
      "Fetching same-day candles",
      symbol=symbol,
      start=midnight.isoformat(),
      until=now.isoformat(),
    )

    candles: list[Candle] = []
    current_start = start_timestamp

    while current_start < until_timestamp:
      response = await self.client.get(
        "/klines",
        params={
          "symbol": symbol,
          "interval": "1m",
          "startTime": current_start,
          "endTime": until_timestamp,
          "limit": 1000,
        },
      )
      response.raise_for_status()
      batch = response.json()

      if not batch:
        break

      for kline in batch:
        candles.append(Candle.from_binance(symbol, kline))

      current_start = batch[-1][6] + 1

    # Save directly to Delta Lake
    await self.delta_repository.save_candles(candles)

    log.info("Same-day candles saved", symbol=symbol, count=len(candles))
    return len(candles)
