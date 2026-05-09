import asyncio
import json
import threading
import time

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from tick_processor.settings import settings

log = structlog.get_logger()


async def _ensure_heartbeat_topic():
  """Create the heartbeat topic with short retention if it doesn't exist."""
  admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
  await admin.start()
  try:
    await admin.create_topics(
      [
        NewTopic(
          name=settings.kafka_topic_heartbeats,
          num_partitions=1,
          replication_factor=1,
          topic_configs={
            "retention.ms": str(settings.kafka_topic_heartbeats_retention_ms),
            "segment.ms": str(settings.kafka_topic_heartbeats_segment_ms),
          },
        )
      ]
    )
    log.info(
      "Heartbeat topic created",
      topic=settings.kafka_topic_heartbeats,
      retention_ms=settings.kafka_topic_heartbeats_retention_ms,
    )
  except TopicAlreadyExistsError:
    log.info("Heartbeat topic already exists", topic=settings.kafka_topic_heartbeats)
  finally:
    await admin.close()


def ensure_heartbeat_topic_blocking():
  """Synchronous wrapper for use during startup, before env.execute()."""
  asyncio.run(_ensure_heartbeat_topic())


async def _publish_loop(stop_event: asyncio.Event):
  """Publish heartbeat messages until stop_event is set.

  Uses absolute scheduling (start + i * cadence) to avoid drift accumulation,
  so the gap-fill emit latency stays predictable.
  """
  producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
  await producer.start()
  log.info(
    "Heartbeat publisher started",
    topic=settings.kafka_topic_heartbeats,
    interval_ms=settings.heartbeat_interval_ms,
  )

  cadence_s = settings.heartbeat_interval_ms / 1000.0
  start = time.monotonic()
  i = 0

  try:
    while not stop_event.is_set():
      next_fire = start + i * cadence_s
      delay = next_fire - time.monotonic()
      if delay > 0:
        try:
          await asyncio.wait_for(stop_event.wait(), timeout=delay)
          break  # stop_event was set during the wait
        except asyncio.TimeoutError:
          pass

      payload = json.dumps(
        {"type": "heartbeat", "timestamp": int(time.time() * 1000)}
      ).encode()
      await producer.send(topic=settings.kafka_topic_heartbeats, value=payload)
      i += 1
  finally:
    await producer.stop()
    log.info("Heartbeat publisher stopped")


def start_heartbeat_thread() -> tuple[threading.Thread, "threading.Event"]:
  """Start the heartbeat publisher on a background thread with its own loop.

  Returns (thread, stop_event). Set stop_event to request graceful shutdown.
  """
  stop_signal = threading.Event()

  def _run():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async_stop = asyncio.Event()

    # Bridge the threading.Event to the asyncio.Event.
    def _watch_stop():
      stop_signal.wait()
      loop.call_soon_threadsafe(async_stop.set)

    watcher = threading.Thread(target=_watch_stop, daemon=True)
    watcher.start()

    try:
      loop.run_until_complete(_publish_loop(async_stop))
    finally:
      loop.close()

  thread = threading.Thread(target=_run, name="heartbeat-publisher", daemon=True)
  thread.start()
  return thread, stop_signal
