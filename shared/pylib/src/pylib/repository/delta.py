import structlog
from deltalake import write_deltalake
import pyarrow as pa

from pylib.model.candle import Candle

log = structlog.get_logger()


class DeltaRepository:
  def __init__(self, table_path: str, storage_options: dict | None = None):
    self.table_path = table_path
    self.storage_options = storage_options or {}

  async def start(self):
    log.info("Delta repository started", path=self.table_path)

  async def stop(self):
    log.info("Delta repository stopped")

  async def save_candles(self, candles: list[Candle]):
    if not candles:
      return

    data = {
      "symbol": [c.symbol for c in candles],
      "start": [c.start for c in candles],
      "end": [c.end for c in candles],
      "open": [str(c.open) for c in candles],
      "high": [str(c.high) for c in candles],
      "low": [str(c.low) for c in candles],
      "close": [str(c.close) for c in candles],
      "volume": [str(c.volume) for c in candles],
    }
    table = pa.table(data)

    write_deltalake(
      self.table_path,
      table,
      mode="append",
      storage_options=self.storage_options,
    )

    log.info("Candles saved to Delta Lake", count=len(candles))
