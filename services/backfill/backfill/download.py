import asyncio
import io
import zipfile

import httpx
import pyarrow as pa
import pyarrow.csv as pcsv
import structlog

from pylib.repository.delta import DeltaRepository, CANDLE_SCHEMA

log = structlog.get_logger()

BINANCE_TO_SCHEMA = {
  "open_time": "start",
  "open": "open",
  "high": "high",
  "low": "low",
  "close": "close",
  "volume": "volume",
  "close_time": "end",
  "count": "trades",
}

BINANCE_CONVERT_OPTIONS = pcsv.ConvertOptions(
  column_types={
    binance_col: CANDLE_SCHEMA.field(schema_col).type
    for binance_col, schema_col in BINANCE_TO_SCHEMA.items()
  },
  include_columns=list(BINANCE_TO_SCHEMA.keys()),
)


async def download_file(
  client: httpx.AsyncClient,
  semaphore: asyncio.Semaphore,
  url: str,
  max_retries: int = 3,
  retry_base_delay: float = 10,
) -> bytes | None:
  """Download a single file with concurrency control and retry."""
  for attempt in range(max_retries):
    async with semaphore:
      try:
        log.debug("Downloading", url=url, attempt=attempt + 1)
        response = await client.get(url, timeout=30)
        response.raise_for_status()
        return response.content
      except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
          log.warning("File not found", url=url)
          return None
        if e.response.status_code >= 500 and attempt < max_retries - 1:
          log.warning(
            "Server error, retrying",
            url=url,
            status=e.response.status_code,
          )
        else:
          raise
      except (httpx.TimeoutException, httpx.ConnectError) as e:
        if attempt < max_retries - 1:
          log.warning("Network error, retrying", url=url, error=str(e))
        else:
          raise

    delay = retry_base_delay ** (attempt + 1)
    log.debug("Waiting before retry", url=url, delay=delay)
    await asyncio.sleep(delay)

  return None


def parse_zip(symbol: str, zip_content: bytes) -> pa.Table:
  """Extract CSV from ZIP and parse into a PyArrow table."""
  with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
    csv_filename = zf.namelist()[0]
    with zf.open(csv_filename) as f:
      table = pcsv.read_csv(
        f,
        convert_options=BINANCE_CONVERT_OPTIONS,
      )

  table = table.rename_columns([BINANCE_TO_SCHEMA[c] for c in table.column_names])
  table = table.append_column(
    "symbol", pa.array([symbol] * table.num_rows, type=pa.string())
  )

  return table


async def download_parse_save(
  client: httpx.AsyncClient,
  semaphore: asyncio.Semaphore,
  delta_repository: DeltaRepository,
  symbol: str,
  url: str,
  max_retries: int = 3,
  retry_base_delay: float = 10,
) -> int:
  """Download, parse, and save a single file. Returns row count."""
  try:
    content = await download_file(client, semaphore, url, max_retries, retry_base_delay)
    if content is None:
      return 0

    table = parse_zip(symbol, content)
    await delta_repository.save_table(table)
    return table.num_rows
  except Exception:
    log.error("Failed to process", url=url, exc_info=True)
    return 0
