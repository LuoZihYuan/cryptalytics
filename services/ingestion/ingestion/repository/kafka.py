import structlog
from aiokafka import AIOKafkaProducer

from ingestion.settings import settings
from pylib.model.tick import Tick

log = structlog.get_logger()


class KafkaRepository:
  def __init__(self):
    self.producer: AIOKafkaProducer | None = None

  async def start(self):
    self.producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await self.producer.start()
    log.info("Kafka repository started")

  async def stop(self):
    if self.producer:
      await self.producer.stop()
      log.info("Kafka repository stopped")

  async def save_tick(self, tick: Tick):
    if not self.producer:
      raise RuntimeError("Producer not started")

    await self.producer.send(
      topic=settings.kafka_topic_ticks,
      key=tick.symbol.encode(),
      value=tick.model_dump_json().encode(),
    )
