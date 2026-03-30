import structlog

from pb import ingestion_pb2, ingestion_pb2_grpc
from ingestion.service.websocket import WebSocketService

log = structlog.get_logger()


class IngestionHandler(ingestion_pb2_grpc.IngestionServiceServicer):
  def __init__(self, websocket_service: WebSocketService):
    self.websocket_service = websocket_service

  async def Subscribe(self, request, context):
    symbols = [s.upper() for s in request.symbols]
    dag_run_id = request.dag_run_id if request.HasField("dag_run_id") else None
    log.info("Subscribe request", symbols=symbols, dag_run_id=dag_run_id)

    try:
      await self.websocket_service.subscribe(symbols, dag_run_id)
      return ingestion_pb2.SubscribeResponse(
        success=True,
        message=f"Subscribed to {', '.join(symbols)}",
      )
    except Exception as e:
      log.error("Subscribe failed", symbols=symbols, error=str(e))
      return ingestion_pb2.SubscribeResponse(
        success=False,
        message=str(e),
      )

  async def Unsubscribe(self, request, context):
    symbols = [s.upper() for s in request.symbols]
    log.info("Unsubscribe request", symbols=symbols)

    try:
      for symbol in symbols:
        await self.websocket_service.unsubscribe(symbol)
      return ingestion_pb2.UnsubscribeResponse(
        success=True,
        message=f"Unsubscribed from {', '.join(symbols)}",
      )
    except Exception as e:
      log.error("Unsubscribe failed", symbols=symbols, error=str(e))
      return ingestion_pb2.UnsubscribeResponse(
        success=False,
        message=str(e),
      )

  async def ListSubscriptions(self, request, context):
    symbols = self.websocket_service.list_subscriptions()
    return ingestion_pb2.ListSubscriptionsResponse(symbols=symbols)
