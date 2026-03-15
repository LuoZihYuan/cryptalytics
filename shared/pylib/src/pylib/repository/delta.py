import asyncio

import structlog
from deltalake import DeltaTable, write_deltalake
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


class DeltaRepository:
  def __init__(self, table_path: str, storage_options: dict | None = None):
    self.table_path = table_path
    self.storage_options = storage_options or {}
    self._write_lock = asyncio.Lock()

  async def save_table(self, table: pa.Table):
    if table.num_rows == 0:
      return

    table = table.select([f.name for f in CANDLE_SCHEMA])

    async with self._write_lock:
      await asyncio.to_thread(self._upsert, table)

    log.info("Candles saved to Delta Lake", count=table.num_rows)

  def _upsert(self, table: pa.Table):
    if not DeltaTable.is_deltatable(self.table_path, storage_options=self.storage_options):
      write_deltalake(
        self.table_path,
        table,
        mode="append",
        storage_options=self.storage_options,
      )
      return

    dt = DeltaTable(self.table_path, storage_options=self.storage_options)
    (
      dt.merge(
        source=table,
        predicate="t.symbol = s.symbol AND t.start = s.start",
        source_alias="s",
        target_alias="t",
      )
      .when_matched_update_all()
      .when_not_matched_insert_all()
      .execute()
    )
