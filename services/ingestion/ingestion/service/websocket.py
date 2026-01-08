import asyncio
import json

import structlog
import websockets
from websockets.asyncio.client import ClientConnection

from ingestion.settings import settings
from ingestion.repository.kafka import KafkaRepository
from ingestion.service.rest import RestService
from pylib.model.tick import Tick

log = structlog.get_logger()


class WebSocketService:
  def __init__(self, kafka_repository: KafkaRepository, rest_service: RestService):
    self.kafka_repository = kafka_repository
    self.rest_service = rest_service
    self.connections: dict[str, ClientConnection] = {}
    self.tasks: dict[str, asyncio.Task] = {}
    self.same_day_fetched: set[str] = set()

  async def subscribe(self, symbol: str):
    if symbol in self.tasks:
      log.warning("Already subscribed", symbol=symbol)
      return

    task = asyncio.create_task(
      self._handle_symbol(symbol),
      name=f"ws-{symbol}",
    )
    self.tasks[symbol] = task
    task.add_done_callback(lambda t: self._on_task_done(symbol, t))
    log.info("Subscribed", symbol=symbol)

  async def unsubscribe(self, symbol: str):
    if symbol not in self.tasks:
      log.warning("Not subscribed", symbol=symbol)
      return

    self.tasks[symbol].cancel()
    self.same_day_fetched.discard(symbol)
    log.info("Unsubscribed", symbol=symbol)

  def list_subscriptions(self) -> list[str]:
    return list(self.tasks.keys())

  def _on_task_done(self, symbol: str, task: asyncio.Task):
    self.tasks.pop(symbol, None)
    self.connections.pop(symbol, None)

    if task.cancelled():
      log.info("Task cancelled", symbol=symbol)
    elif exc := task.exception():
      log.error("Task failed", symbol=symbol, error=str(exc))
      asyncio.create_task(self._reconnect(symbol))

  async def _reconnect(self, symbol: str, delay: int = 5):
    log.info("Reconnecting", symbol=symbol, delay=delay)
    await asyncio.sleep(delay)
    await self.subscribe(symbol)

  async def _handle_symbol(self, symbol: str):
    stream = f"{symbol.lower()}@trade"
    url = f"{settings.binance_ws_url}/{stream}"

    async with websockets.connect(url) as ws:
      self.connections[symbol] = ws
      log.info("Connected", symbol=symbol, url=url)

      current_minute: int | None = None

      async for msg in ws:
        data = json.loads(msg)
        tick = Tick.from_binance(data)

        tick_minute = self._get_minute(tick.timestamp)

        if current_minute is None:
          current_minute = tick_minute
          log.info(
            "First tick received (partial minute)",
            symbol=symbol,
            minute=tick_minute,
          )
        elif tick_minute != current_minute:
          log.info(
            "New minute started",
            symbol=symbol,
            previous=current_minute,
            current=tick_minute,
          )

          if symbol not in self.same_day_fetched:
            await self._fetch_same_day_candles(symbol, current_minute)
            self.same_day_fetched.add(symbol)

          current_minute = tick_minute

        await self.kafka_repository.save_tick(tick)

  def _get_minute(self, timestamp_ms: int) -> int:
    return (timestamp_ms // 60000) * 60000

  async def _fetch_same_day_candles(self, symbol: str, until_minute: int):
    try:
      count = await self.rest_service.fetch_and_save_same_day_candles(
        symbol=symbol,
        until_timestamp=until_minute,
      )
      log.info("Same-day candles fetched", symbol=symbol, count=count)
    except Exception as e:
      log.error("Failed to fetch same-day candles", symbol=symbol, error=str(e))

  async def stop(self):
    for symbol in list(self.tasks.keys()):
      await self.unsubscribe(symbol)

    if self.tasks:
      await asyncio.gather(*self.tasks.values(), return_exceptions=True)

    log.info("WebSocket service stopped")
