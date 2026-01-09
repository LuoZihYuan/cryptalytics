import argparse
import asyncio
import io
import zipfile

import httpx
import structlog

from backfill.settings import settings
from pylib.model.candle import Candle
from pylib.repository.delta import DeltaRepository

structlog.configure(
  wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)
log = structlog.get_logger()


async def download_candles(client: httpx.AsyncClient, symbol: str, date: str) -> bytes:
  """Download ZIP file for a single date."""
  url = f"{settings.binance_bulk_url}/{symbol}/1m/{symbol}-1m-{date}.zip"
  log.info("Downloading", url=url)

  response = await client.get(url)
  response.raise_for_status()
  return response.content


def parse_candles(symbol: str, zip_content: bytes) -> list[Candle]:
  """Extract CSV from ZIP and parse into Candle objects."""
  candles = []

  with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
    csv_filename = zf.namelist()[0]
    with zf.open(csv_filename) as f:
      for line in f:
        row = line.decode("utf-8").strip().split(",")
        candles.append(Candle.from_binance(symbol, row))

  return candles


async def run(symbol: str, date: str):
  log.info("Starting backfill", symbol=symbol, date=date)

  delta_repository = DeltaRepository(
    table_path=settings.delta_candles_path,
    storage_options=settings.delta_storage_options,
  )
  await delta_repository.start()

  async with httpx.AsyncClient() as client:
    zip_content = await download_candles(client, symbol, date)
    candles = parse_candles(symbol, zip_content)
    await delta_repository.save_candles(candles)

  await delta_repository.stop()

  log.info("Backfill complete", symbol=symbol, date=date, count=len(candles))


def main():
  parser = argparse.ArgumentParser(description="Backfill historical candles")
  parser.add_argument("--symbol", required=True, help="Trading pair (e.g., BTCUSDT)")
  parser.add_argument("--date", required=True, help="Date (YYYY-MM-DD)")
  args = parser.parse_args()

  asyncio.run(run(args.symbol, args.date))


if __name__ == "__main__":
  main()
