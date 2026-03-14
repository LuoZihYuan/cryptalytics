from decimal import Decimal

import structlog
from pyflink.common.typeinfo import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from tick_processor.settings import settings

log = structlog.get_logger()


class CandleAggregator(KeyedProcessFunction):
  def open(self, runtime_context: RuntimeContext):
    self._acc = runtime_context.get_state(
      ValueStateDescriptor("acc", Types.PICKLED_BYTE_ARRAY())
    )
    self._last_close = runtime_context.get_state(
      ValueStateDescriptor("last_close", Types.PICKLED_BYTE_ARRAY())
    )
    self._timer_ts = runtime_context.get_state(
      ValueStateDescriptor("timer_ts", Types.LONG())
    )
    self._seen_trade_ids = runtime_context.get_state(
      ValueStateDescriptor("seen_trade_ids", Types.PICKLED_BYTE_ARRAY())
    )

  def process_element(self, tick, ctx: KeyedProcessFunction.Context):
    seen = self._seen_trade_ids.value() or set()
    if tick.trade_id in seen:
      return
    seen.add(tick.trade_id)
    self._seen_trade_ids.update(seen)

    window_start = (tick.timestamp // settings.window_size_ms) * settings.window_size_ms
    window_end = window_start + settings.window_size_ms

    acc = self._acc.value()

    if acc is not None and acc["window_start"] != window_start:
      yield from self._build_candle(acc)
      self._last_close.update(acc["close_price"])
      self._acc.clear()
      self._seen_trade_ids.update({tick.trade_id})
      old_timer = self._timer_ts.value()
      if old_timer is not None:
        ctx.timer_service().delete_processing_time_timer(old_timer)
      acc = None

    if acc is None:
      acc = {
        "symbol": tick.symbol,
        "window_start": window_start,
        "window_end": window_end,
        "open_price": tick.price,
        "open_ts": tick.timestamp,
        "high": tick.price,
        "low": tick.price,
        "close_price": tick.price,
        "close_ts": tick.timestamp,
        "volume": tick.quantity,
        "trades": 1,
      }
      timer = window_end + settings.watermark_delay_ms
      ctx.timer_service().register_processing_time_timer(timer)
      self._timer_ts.update(timer)
    else:
      if tick.timestamp < acc["open_ts"]:
        acc["open_price"] = tick.price
        acc["open_ts"] = tick.timestamp
      if tick.timestamp > acc["close_ts"]:
        acc["close_price"] = tick.price
        acc["close_ts"] = tick.timestamp
      acc["high"] = max(acc["high"], tick.price)
      acc["low"] = min(acc["low"], tick.price)
      acc["volume"] += tick.quantity
      acc["trades"] += 1

    self._acc.update(acc)

  def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
    acc = self._acc.value()

    if acc is not None:
      yield from self._build_candle(acc)
      self._last_close.update(acc["close_price"])
      self._acc.clear()
      self._seen_trade_ids.clear()
    else:
      last_close = self._last_close.value()
      if last_close is not None:
        window_end = timestamp
        window_start = window_end - settings.window_size_ms
        log.debug("Gap-filling candle", symbol=ctx.get_current_key(), start=window_start)
        yield {
          "symbol": ctx.get_current_key(),
          "start": window_start,
          "end": window_end,
          "open": last_close,
          "high": last_close,
          "low": last_close,
          "close": last_close,
          "volume": Decimal(0),
          "trades": 0,
        }

    next_timer = timestamp + settings.window_size_ms
    ctx.timer_service().register_processing_time_timer(next_timer)
    self._timer_ts.update(next_timer)

  def _build_candle(self, acc: dict):
    yield {
      "symbol": acc["symbol"],
      "start": acc["window_start"],
      "end": acc["window_end"],
      "open": acc["open_price"],
      "high": acc["high"],
      "low": acc["low"],
      "close": acc["close_price"],
      "volume": acc["volume"],
      "trades": acc["trades"],
    }
