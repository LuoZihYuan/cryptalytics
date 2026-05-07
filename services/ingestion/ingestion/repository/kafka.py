import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from ingestion.settings import settings
from pylib.model.tick import Tick

log = structlog.get_logger()


class KafkaRepository:
  def __init__(self):
    self.producer: AIOKafkaProducer | None = None

  async def start(self):
    await self._ensure_topic()
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

  async def _ensure_topic(self):
    """Create the ticks topic with explicit config if it doesn't already exist."""
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    await admin.start()
    try:
      await admin.create_topics(
        [
          NewTopic(
            name=settings.kafka_topic_ticks,
            num_partitions=settings.kafka_topic_ticks_partitions,
            replication_factor=settings.kafka_topic_ticks_replication_factor,
            topic_configs={
              "retention.ms": str(settings.kafka_topic_ticks_retention_ms),
            },
          )
        ]
      )
      log.info(
        "Kafka topic created",
        topic=settings.kafka_topic_ticks,
        partitions=settings.kafka_topic_ticks_partitions,
        retention_ms=settings.kafka_topic_ticks_retention_ms,
      )
    except TopicAlreadyExistsError:
      log.info("Kafka topic already exists", topic=settings.kafka_topic_ticks)
    finally:
      await admin.close()
