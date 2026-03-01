import argparse
import asyncio
from datetime import datetime, timezone

import httpx
import structlog

from backfill.settings import settings
from backfill.plan import build_download_plan
from backfill.download import download_parse_save
from pylib.repository.delta import DeltaRepository

structlog.configure(
  wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)
log = structlog.get_logger()


async def run(symbol: str, start_date: str, end_date: str):
  start = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
  end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

  plan = build_download_plan(start, end)
  log.info(
    "Starting backfill",
    symbol=symbol,
    start_date=start_date,
    end_date=end_date,
    monthly_files=len(plan["monthly"]),
    daily_files=len(plan["daily"]),
  )

  delta_repository = DeltaRepository(
    table_path=settings.delta_candles_path,
    storage_options=settings.delta_storage_options,
  )

  semaphore = asyncio.Semaphore(settings.download_concurrency)

  async with httpx.AsyncClient() as client:
    tasks = []

    for interval, dates in plan.items():
      for date in dates:
        url = f"{settings.binance_bulk_url}/{interval}/klines/{symbol}/1m/{symbol}-1m-{date}.zip"
        tasks.append(
          download_parse_save(
            client,
            semaphore,
            delta_repository,
            symbol,
            url,
            max_retries=settings.max_retries,
            retry_base_delay=settings.retry_base_delay,
          )
        )

    results = await asyncio.gather(*tasks)
    total_candles = sum(results)

  log.info("Backfill complete", symbol=symbol, candles=total_candles)


def main():
  parser = argparse.ArgumentParser(description="Backfill historical candles")
  parser.add_argument("--symbol", required=True, help="Trading pair (e.g., BTCUSDT)")
  parser.add_argument(
    "--start-date", required=True, help="Start date inclusive, UTC (YYYY-MM-DD)"
  )
  parser.add_argument(
    "--end-date", required=True, help="End date exclusive, UTC (YYYY-MM-DD)"
  )
  args = parser.parse_args()

  asyncio.run(run(args.symbol, args.start_date, args.end_date))


if __name__ == "__main__":
  main()
