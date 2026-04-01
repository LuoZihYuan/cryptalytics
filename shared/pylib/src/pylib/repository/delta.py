import asyncio

import structlog
from deltalake import write_deltalake
import pyarrow as pa

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
    ("trades", pa.int64()),
  ]
)

TABLE_CONFIG = {
  "delta.isolationLevel": "SnapshotIsolation",
}


class DeltaRepository:
  def __init__(self, table_path: str, storage_options: dict | None = None):
    self.table_path = table_path
    self.storage_options = storage_options or {}

  async def save_table(self, table: pa.Table):
    if table.num_rows == 0:
      return

    table = table.select([f.name for f in CANDLE_SCHEMA])
    await asyncio.to_thread(self._append, table)
    log.info("Candles saved to Delta Lake", count=table.num_rows)

  def _append(self, table: pa.Table):
    write_deltalake(
      self.table_path,
      table,
      mode="append",
      configuration=TABLE_CONFIG,
      storage_options=self.storage_options,
    )
