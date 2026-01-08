import asyncio
import signal

import structlog
from grpc import aio

from ingestion.settings import settings
from ingestion.pb import ingestion_pb2_grpc
from ingestion.handler.ingestion import IngestionHandler
from ingestion.repository.kafka import KafkaRepository
from ingestion.service.rest import RestService
from ingestion.service.websocket import WebSocketService
from pylib.repository.delta import DeltaRepository

structlog.configure(
  wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)
log = structlog.get_logger()


async def run():
  # Initialize repositories
  kafka_repository = KafkaRepository()
  delta_repository = DeltaRepository(
    table_path=settings.delta_candles_path,
    storage_options=settings.delta_storage_options,
  )

  # Initialize services
  rest_service = RestService(delta_repository=delta_repository)
  websocket_service = WebSocketService(
    kafka_repository=kafka_repository,
    rest_service=rest_service,
  )

  # Start repositories and services
  await kafka_repository.start()
  await delta_repository.start()
  await rest_service.start()

  # Configure gRPC server
  server = aio.server()
  handler = IngestionHandler(websocket_service)
  ingestion_pb2_grpc.add_IngestionServiceServicer_to_server(handler, server)
  server.add_insecure_port(f"[::]:{settings.grpc_port}")
  await server.start()

  log.info("Ingestion server started", grpc_port=settings.grpc_port)

  # Set up shutdown signal handling
  shutdown_event = asyncio.Event()

  def handle_shutdown():
    log.info("Shutdown signal received")
    shutdown_event.set()

  loop = asyncio.get_running_loop()
  for sig in (signal.SIGTERM, signal.SIGINT):
    loop.add_signal_handler(sig, handle_shutdown)

  # Wait for shutdown signal
  await shutdown_event.wait()

  # Graceful shutdown (order matters)
  # 1. Stop accepting new gRPC requests, allow in-flight to complete
  # 2. Stop WebSocket connections (stops producing to Kafka)
  # 3. Stop services
  # 4. Stop repositories (flushes buffered data)
  log.info("Shutting down...")
  await server.stop(grace=5)
  await websocket_service.stop()
  await rest_service.stop()
  await delta_repository.stop()
  await kafka_repository.stop()
  log.info("Shutdown complete")


def main():
  asyncio.run(run())


if __name__ == "__main__":
  main()
