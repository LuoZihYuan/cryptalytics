from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pyarrow as pa
import structlog

from ingestion.settings import settings
from pylib.repository.delta import DeltaRepository, CANDLE_SCHEMA

log = structlog.get_logger()

# Binance REST kline index -> our schema name
KLINE_FIELDS = {
  0: "start",
  1: "open",
  2: "high",
  3: "low",
  4: "close",
  5: "volume",
  6: "end",
  8: "trades",
}

DECIMAL_COLUMNS = {"open", "high", "low", "close", "volume"}


def klines_to_table(symbol: str, klines: list[list]) -> pa.Table:
  """Convert raw Binance kline lists to a PyArrow table."""
  if not klines:
    return pa.table({f.name: pa.array([], type=f.type) for f in CANDLE_SCHEMA})

  columns = {}
  for idx, name in KLINE_FIELDS.items():
    values = [k[idx] for k in klines]
    target_type = CANDLE_SCHEMA.field(name).type
    if name in DECIMAL_COLUMNS:
      values = [Decimal(v) for v in values]
    columns[name] = pa.array(values, type=target_type)

  columns["symbol"] = pa.array(
    [symbol] * len(klines),
    type=CANDLE_SCHEMA.field("symbol").type,
  )

  return pa.table(columns)


class RestService:
  def __init__(self, delta_repository: DeltaRepository):
    self.delta_repository = delta_repository
    self.client = httpx.AsyncClient(base_url=settings.binance_rest_url)

  async def close(self):
    await self.client.aclose()
    log.info("REST service closed")

  async def fetch_and_save_same_day_candles(
    self, symbol: str, until_timestamp: int
  ) -> int:
    """Fetch 1m candles from midnight until the given timestamp and save to Delta Lake."""
    now = datetime.fromtimestamp(until_timestamp / 1000, tz=timezone.utc)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_timestamp = int(midnight.timestamp() * 1000)

    log.info(
      "Fetching same-day candles",
      symbol=symbol,
      start=midnight.isoformat(),
      until=now.isoformat(),
    )

    klines: list[list] = []
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

      klines.extend(batch)
      current_start = batch[-1][6] + 1

    table = klines_to_table(symbol, klines)
    await self.delta_repository.save_table(table)

    log.info("Same-day candles saved", symbol=symbol, count=table.num_rows)
    return table.num_rows
