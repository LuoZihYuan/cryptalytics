import asyncio

import structlog
from deltalake import write_deltalake
import pyarrow as pa

from pylib.model.candle import Candle

log = structlog.get_logger()

CANDLE_SCHEMA = pa.schema(
  [
    ("symbol", pa.string()),
    ("start", pa.int64()),
    ("end", pa.int64()),
    ("open", pa.decimal128(18, 8)),
    ("high", pa.decimal128(18, 8)),
    ("low", pa.decimal128(18, 8)),
    ("close", pa.decimal128(18, 8)),
    ("volume", pa.decimal128(18, 8)),
  ]
)


class DeltaRepository:
  def __init__(self, table_path: str, storage_options: dict | None = None):
    self.table_path = table_path
    self.storage_options = storage_options or {}
    self._write_lock = asyncio.Lock()

  async def save_candles(self, candles: list[Candle]):
    if not candles:
      return

    data = [c.model_dump() for c in candles]
    table = pa.Table.from_pylist(data, schema=CANDLE_SCHEMA)

    async with self._write_lock:
      await asyncio.to_thread(
        write_deltalake,
        self.table_path,
        table,
        mode="append",
        storage_options=self.storage_options,
      )

    log.info("Candles saved to Delta Lake", count=len(candles))
