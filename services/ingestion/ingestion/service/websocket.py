from __future__ import annotations

import asyncio
import json
import uuid
from collections import defaultdict
from dataclasses import dataclass, field

import httpx
import structlog
import websockets

from ingestion.client.airflow import AirflowClient
from ingestion.repository.kafka import KafkaRepository
from ingestion.service.rest import RestService
from ingestion.settings import settings
from pylib.model.tick import Tick

log = structlog.get_logger()

MAX_STREAMS_PER_CONNECTION = 1024


def _short(bucket_id: str) -> str:
  """Display form of a UUID — first 8 hex chars."""
  return bucket_id[:8]


@dataclass(eq=False)
class Bucket:
  id: str = field(default_factory=lambda: str(uuid.uuid4()))
  symbols: set[str] = field(default_factory=set)
  ws: websockets.asyncio.client.ClientConnection | None = None
  task: asyncio.Task | None = None
  _msg_id: int = 0

  def next_id(self) -> int:
    self._msg_id += 1
    return self._msg_id


class WebSocketService:
  def __init__(
    self,
    kafka_repository: KafkaRepository,
    rest_service: RestService,
    airflow_client: AirflowClient,
  ):
    self.kafka_repository = kafka_repository
    self.rest_service = rest_service
    self.airflow_client = airflow_client
    self._buckets: list[Bucket] = []
    self._symbol_to_bucket: dict[str, Bucket] = {}
    self.same_day_fetched: set[str] = set()
    self.pending_callbacks: dict[str, str] = {}  # symbol -> dag_run_id
    self._first_tick_minute: dict[str, int] = {}  # symbol -> minute of first tick

  def _service_distribution(self) -> dict[str, int]:
    """Map of {short_connection_id: symbol_count} across all active buckets."""
    return {_short(b.id): len(b.symbols) for b in self._buckets}

  async def subscribe(self, symbols: list[str], dag_run_id: str | None = None):
    new_symbols = [s for s in symbols if s not in self._symbol_to_bucket]
    already = [s for s in symbols if s in self._symbol_to_bucket]

    if already:
      # Group already-subscribed symbols by the connection they live on so
      # the warning identifies where each one already exists.
      by_connection: dict[str, list[str]] = defaultdict(list)
      for s in already:
        by_connection[self._symbol_to_bucket[s].id].append(s)
      for connection_id, syms in by_connection.items():
        log.warning(
          "websocket: already subscribed",
          connection=_short(connection_id),
          symbols=syms,
        )

    if not new_symbols:
      return

    if dag_run_id:
      for symbol in new_symbols:
        self.pending_callbacks[symbol] = dag_run_id

    added: dict[Bucket, list[str]] = {}
    for symbol in new_symbols:
      bucket = next(
        (b for b in self._buckets if len(b.symbols) < MAX_STREAMS_PER_CONNECTION),
        None,
      )
      if bucket is None:
        bucket = Bucket()
        self._buckets.append(bucket)
        bucket.task = asyncio.create_task(
          self._receive_loop(bucket),
          name=f"ws-bucket-{_short(bucket.id)}",
        )

      bucket.symbols.add(symbol)
      self._symbol_to_bucket[symbol] = bucket
      added.setdefault(bucket, []).append(symbol)

    for bucket, syms in added.items():
      if bucket.ws is not None:
        # Adding to an already-connected bucket. The connection's
        # `websocket: connected` log fired previously, so emit a state-change
        # log here so it's visible in the trail.
        await self._send_subscribe(bucket, syms)
        log.info(
          "websocket: streams added",
          connection=_short(bucket.id),
          added=syms,
          service=self._service_distribution(),
        )
      # If bucket.ws is None, the receive loop will send the SUBSCRIBE on
      # connect and emit `websocket: connected` for the full set.

  async def unsubscribe(self, symbol: str):
    bucket = self._symbol_to_bucket.pop(symbol, None)
    if bucket is None:
      log.warning(
        "websocket: unsubscribe miss",
        symbol=symbol,
      )
      return

    bucket.symbols.discard(symbol)
    self.same_day_fetched.discard(symbol)
    self.pending_callbacks.pop(symbol, None)
    self._first_tick_minute.pop(symbol, None)

    if bucket.ws is not None:
      await self._send_unsubscribe(bucket, [symbol])

    if not bucket.symbols and bucket.task:
      bucket.task.cancel()
      self._buckets.remove(bucket)

  def list_subscriptions(self) -> list[str]:
    return list(self._symbol_to_bucket.keys())

  async def _send_subscribe(self, bucket: Bucket, symbols: list[str]):
    await bucket.ws.send(
      json.dumps(
        {
          "method": "SUBSCRIBE",
          "params": [f"{s.lower()}@trade" for s in symbols],
          "id": bucket.next_id(),
        }
      )
    )

  async def _send_unsubscribe(self, bucket: Bucket, symbols: list[str]):
    await bucket.ws.send(
      json.dumps(
        {
          "method": "UNSUBSCRIBE",
          "params": [f"{s.lower()}@trade" for s in symbols],
          "id": bucket.next_id(),
        }
      )
    )

  async def _receive_loop(self, bucket: Bucket):
    backoff = 1
    while True:
      try:
        async with websockets.connect(settings.binance_ws_url) as ws:
          bucket.ws = ws
          backoff = 1
          if bucket.symbols:
            await self._send_subscribe(bucket, list(bucket.symbols))
          log.info(
            "websocket: connected",
            connection=_short(bucket.id),
            service=self._service_distribution(),
          )

          async for raw in ws:
            msg = json.loads(raw)
            if "stream" not in msg:
              continue
            await self._handle_message(msg["stream"], msg["data"])

      except asyncio.CancelledError:
        break
      except Exception as e:
        log.warning(
          "websocket: reconnecting",
          connection=_short(bucket.id),
          error=str(e),
          backoff=backoff,
          service=self._service_distribution(),
        )
        for symbol in bucket.symbols:
          self.same_day_fetched.discard(symbol)
          self._first_tick_minute.pop(symbol, None)
        bucket.ws = None
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)

    bucket.ws = None

  async def _handle_message(self, stream: str, data: dict):
    symbol = stream.split("@")[0].upper()
    tick = Tick.from_binance(data)

    if symbol not in self.same_day_fetched:
      tick_minute = self._get_minute(tick.timestamp)
      first_minute = self._first_tick_minute.get(symbol)

      if first_minute is None:
        self._first_tick_minute[symbol] = tick_minute
      elif tick_minute >= first_minute + 2 * settings.window_size_ms:
        await self._fetch_same_day_candles(symbol, tick_minute)
        self.same_day_fetched.add(symbol)

    await self.kafka_repository.save_tick(tick)

  def _get_minute(self, timestamp_ms: int) -> int:
    return (timestamp_ms // 60000) * 60000

  async def _fetch_same_day_candles(self, symbol: str, until_minute: int):
    try:
      count = await self.rest_service.fetch_and_save_same_day_candles(
        symbol=symbol,
        until_timestamp=until_minute,
      )
      log.info(
        "rest: same-day fetched",
        symbol=symbol,
        count=count,
      )
    except Exception:
      log.error(
        "rest: same-day fetch failed",
        symbol=symbol,
        exc_info=True,
      )
      return

    dag_run_id = self.pending_callbacks.pop(symbol, None)
    if dag_run_id:
      max_attempts = 3
      for attempt in range(1, max_attempts + 1):
        try:
          await self.airflow_client.mark_realtime_ready(dag_run_id, symbol)
          return
        except httpx.TransportError as e:
          if attempt < max_attempts:
            log.warning(
              "airflow: signal retry",
              symbol=symbol,
              dag_run_id=dag_run_id,
              attempt=attempt,
              error=f"{type(e).__name__}: {e}",
            )
            await asyncio.sleep(2**attempt)
          else:
            log.error(
              "airflow: signal failed",
              symbol=symbol,
              dag_run_id=dag_run_id,
              attempts=max_attempts,
              exc_info=True,
            )
        except Exception:
          log.error(
            "airflow: signal error",
            symbol=symbol,
            dag_run_id=dag_run_id,
            exc_info=True,
          )
          break

  async def close(self):
    for bucket in list(self._buckets):
      if bucket.task:
        bucket.task.cancel()

    tasks = [b.task for b in self._buckets if b.task]
    if tasks:
      await asyncio.gather(*tasks, return_exceptions=True)

    log.info("websocket: closed")
