import asyncio
import signal
from contextlib import asynccontextmanager

import structlog
from grpc import aio

from ingestion.settings import settings
from ingestion.pb import ingestion_pb2_grpc
from ingestion.handler.ingestion import IngestionHandler
from ingestion.client.airflow import AirflowClient
from ingestion.repository.kafka import KafkaRepository
from ingestion.service.rest import RestService
from ingestion.service.websocket import WebSocketService
from pylib.repository.delta import DeltaRepository

structlog.configure(
  wrapper_class=structlog.make_filtering_bound_logger(settings.log_level),
)
log = structlog.get_logger()


@asynccontextmanager
async def lifecycle():
  """Manage the full server lifecycle: init → yield → shutdown."""
  # --- Init ---
  airflow_client = AirflowClient()
  kafka_repository = KafkaRepository()
  delta_repository = DeltaRepository(
    table_path=settings.delta_candles_path,
    storage_options=settings.delta_storage_options,
  )
  rest_service = RestService(delta_repository=delta_repository)
  websocket_service = WebSocketService(
    kafka_repository=kafka_repository,
    rest_service=rest_service,
    airflow_client=airflow_client,
  )

  await kafka_repository.start()

  server = aio.server()
  handler = IngestionHandler(websocket_service)
  ingestion_pb2_grpc.add_IngestionServiceServicer_to_server(handler, server)
  server.add_insecure_port(f"[::]:{settings.grpc_port}")
  await server.start()

  log.info("Ingestion server started", grpc_port=settings.grpc_port)

  yield

  # --- Shutdown (order matters) ---
  log.info("Shutting down...")
  await server.stop(grace=5)
  await websocket_service.close()
  await rest_service.close()
  await kafka_repository.stop()
  await airflow_client.close()
  log.info("Shutdown complete")


async def run():
  """Wait for a shutdown signal."""
  shutdown_event = asyncio.Event()
  loop = asyncio.get_running_loop()
  for sig in (signal.SIGTERM, signal.SIGINT):
    loop.add_signal_handler(sig, shutdown_event.set)
  await shutdown_event.wait()


async def main():
  async with lifecycle():
    await run()


if __name__ == "__main__":
  asyncio.run(main())
