import argparse
import asyncio
from datetime import datetime, timezone

import httpx
import structlog

from backfill.settings import settings
from pylib.model.candle import Candle
from pylib.repository.delta import DeltaRepository

structlog.configure(
  wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)
log = structlog.get_logger()


async def fetch_candles(
  client: httpx.AsyncClient,
  symbol: str,
  start_timestamp: int,
  end_timestamp: int,
) -> list[Candle]:
  """Fetch 1m candles for the given time range."""
  candles: list[Candle] = []
  current_start = start_timestamp

  while current_start < end_timestamp:
    response = await client.get(
      "/klines",
      params={
        "symbol": symbol,
        "interval": "1m",
        "startTime": current_start,
        "endTime": end_timestamp,
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

  return candles


async def run(symbol: str, start_date: str, end_date: str):
  # Parse dates to timestamps
  start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
  end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
    hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
  )
  start_timestamp = int(start_dt.timestamp() * 1000)
  end_timestamp = int(end_dt.timestamp() * 1000)

  log.info(
    "Starting backfill",
    symbol=symbol,
    start=start_dt.isoformat(),
    end=end_dt.isoformat(),
  )

  # Initialize
  delta_repository = DeltaRepository(
    table_path=settings.delta_candles_path,
    storage_options=settings.delta_storage_options,
  )
  await delta_repository.start()

  async with httpx.AsyncClient(base_url=settings.binance_rest_url) as client:
    candles = await fetch_candles(client, symbol, start_timestamp, end_timestamp)
    await delta_repository.save_candles(candles)

  await delta_repository.stop()

  log.info("Backfill complete", symbol=symbol, count=len(candles))


def main():
  parser = argparse.ArgumentParser(description="Backfill historical candles")
  parser.add_argument("--symbol", required=True, help="Trading pair (e.g., BTCUSDT)")
  parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
  parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
  args = parser.parse_args()

  asyncio.run(run(args.symbol, args.start_date, args.end_date))


if __name__ == "__main__":
  main()
