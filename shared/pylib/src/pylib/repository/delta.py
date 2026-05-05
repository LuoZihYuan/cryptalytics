import asyncio
import time

import pyarrow as pa
import structlog
from deltalake import DeltaTable, write_deltalake
from deltalake._internal import CommitFailedError

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

PARTITION_BY = ["symbol"]

MAX_RETRIES = 3
BASE_BACKOFF_S = 0.1


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
    for attempt in range(1, MAX_RETRIES + 1):
      try:
        is_new = not DeltaTable.is_deltatable(
          self.table_path, storage_options=self.storage_options
        )

        write_deltalake(
          self.table_path,
          table,
          mode="append",
          partition_by=PARTITION_BY if is_new else None,
          configuration=TABLE_CONFIG if is_new else None,
          storage_options=self.storage_options,
        )
        return
      except CommitFailedError:
        if attempt == MAX_RETRIES:
          raise
        backoff = BASE_BACKOFF_S * (2 ** (attempt - 1))
        log.warning(
          "Delta append conflict, retrying",
          attempt=attempt,
          backoff=backoff,
        )
        time.sleep(backoff)
