import asyncio
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import structlog
from deltalake import DeltaTable, write_deltalake
from deltalake._internal import CommitFailedError

log = structlog.get_logger()

# Public contract for save_table. Callers (streaming sink, REST same-day
# fetch, historical backfill) construct a pa.Table matching this schema.
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

# Internal storage shape. Adds `month` (YYYYMM as int32) for partitioning.
# Not exported — callers should not need to know the partition scheme.
_PARTITIONED_CANDLE_SCHEMA = pa.schema(
  list(CANDLE_SCHEMA) + [pa.field("month", pa.int32())]
)

TABLE_CONFIG = {
  "delta.isolationLevel": "SnapshotIsolation",
}

PARTITION_BY = ["symbol", "month"]

MAX_RETRIES = 3
BASE_BACKOFF_S = 0.1

# 1-minute candle. Used to derive the log's exclusive end from min(start)
# and max(start), independent of the `end` column (which differs by writer:
# streaming uses exclusive end, Binance REST/backfill use inclusive end).
WINDOW_SIZE_MS = 60000


def _add_month_column(table: pa.Table) -> pa.Table:
  """Derive `month` (YYYYMM as int32) from the `start` column (Unix ms)."""
  ts = pc.cast(pc.divide(table["start"], 1000), pa.timestamp("s", tz="UTC"))
  year = pc.year(ts)
  month = pc.month(ts)
  yyyymm = pc.add(
    pc.multiply(pc.cast(year, pa.int32()), 100),
    pc.cast(month, pa.int32()),
  )
  return table.append_column("month", yyyymm)


def _summarize_symbols(table: pa.Table) -> dict:
  """Return either {'symbol': X} for one symbol or {'symbols': [...]} for many."""
  unique = pc.unique(table["symbol"]).to_pylist()
  if len(unique) == 1:
    return {"symbol": unique[0]}
  return {"symbols": unique}


def _iso(ms: int) -> str:
  """Render a Unix ms timestamp as a UTC ISO 8601 string."""
  return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


class DeltaRepository:
  def __init__(self, table_path: str, storage_options: dict | None = None):
    self.table_path = table_path
    self.storage_options = storage_options or {}

  async def save_table(self, table: pa.Table):
    """Append candles to the Delta table.

    `table` must match CANDLE_SCHEMA. The partition column is derived here
    so callers don't need to know the partition scheme.
    """
    if table.num_rows == 0:
      return

    # Project to the public input schema (enforces column set and order)
    # before deriving the internal partition column.
    table = table.select([f.name for f in CANDLE_SCHEMA])
    table = _add_month_column(table)
    # Final shape matches the internal storage schema.
    table = table.select([f.name for f in _PARTITIONED_CANDLE_SCHEMA])

    symbol_fields = _summarize_symbols(table)
    start_ms = pc.min(table["start"]).as_py()
    # Derive an exclusive end from the latest window's start. Avoids reading
    # the `end` column directly because writers disagree on its convention
    # (streaming: exclusive; REST/backfill from Binance: inclusive).
    end_ms = pc.max(table["start"]).as_py() + WINDOW_SIZE_MS

    started_at = time.monotonic()
    await asyncio.to_thread(self._append, table)
    write_ms = int((time.monotonic() - started_at) * 1000)

    log.info(
      "delta: wrote candles",
      **symbol_fields,
      count=table.num_rows,
      start=_iso(start_ms),
      end=_iso(end_ms),
      write_ms=write_ms,
    )

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
          "delta: append retry",
          attempt=attempt,
          backoff=backoff,
        )
        time.sleep(backoff)
