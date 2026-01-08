import structlog

from ingestion.pb import ingestion_pb2, ingestion_pb2_grpc
from ingestion.service.websocket import WebSocketService

log = structlog.get_logger()


class IngestionHandler(ingestion_pb2_grpc.IngestionServiceServicer):
  def __init__(self, websocket_service: WebSocketService):
    self.websocket_service = websocket_service

  async def Subscribe(self, request, context):
    symbol = request.symbol.upper()
    log.info("Subscribe request", symbol=symbol)

    try:
      await self.websocket_service.subscribe(symbol)
      return ingestion_pb2.SubscribeResponse(
        success=True,
        message=f"Subscribed to {symbol}",
      )
    except Exception as e:
      log.error("Subscribe failed", symbol=symbol, error=str(e))
      return ingestion_pb2.SubscribeResponse(
        success=False,
        message=str(e),
      )

  async def Unsubscribe(self, request, context):
    symbol = request.symbol.upper()
    log.info("Unsubscribe request", symbol=symbol)

    try:
      await self.websocket_service.unsubscribe(symbol)
      return ingestion_pb2.UnsubscribeResponse(
        success=True,
        message=f"Unsubscribed from {symbol}",
      )
    except Exception as e:
      log.error("Unsubscribe failed", symbol=symbol, error=str(e))
      return ingestion_pb2.UnsubscribeResponse(
        success=False,
        message=str(e),
      )

  async def ListSubscriptions(self, request, context):
    symbols = self.websocket_service.list_subscriptions()
    return ingestion_pb2.ListSubscriptionsResponse(symbols=symbols)
