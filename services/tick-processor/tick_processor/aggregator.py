import time
from decimal import Decimal

import structlog
from pyflink.common.typeinfo import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

from tick_processor.settings import settings

log = structlog.get_logger()


class CandleAggregator(KeyedProcessFunction):
  def open(self, runtime_context: RuntimeContext):
    # Multiple windows for one key may be open simultaneously (a new tick can
    # arrive for window N+1 before the watermark fires window N's timer).
    # Keying acc state by window_start lets each window live independently
    # until its own timer fires.
    self._accs = runtime_context.get_map_state(
      MapStateDescriptor("accs", Types.LONG(), Types.PICKLED_BYTE_ARRAY())
    )
    # Per-window dedup: same trade_id arriving twice within the same window
    # must be de-duplicated, but trade_ids in different windows are
    # independent. Map<window_start, set[trade_id]>.
    self._seen_trade_ids = runtime_context.get_map_state(
      MapStateDescriptor("seen_trade_ids", Types.LONG(), Types.PICKLED_BYTE_ARRAY())
    )
    # Last close is shared across windows for the gap-fill path.
    self._last_close = runtime_context.get_state(
      ValueStateDescriptor("last_close", Types.PICKLED_BYTE_ARRAY())
    )
    # Smallest window_start still eligible for new ticks. Once a window has
    # been sealed (its candle emitted), late ticks for that window must be
    # rejected — otherwise they would rebuild the accumulator and the next
    # timer would emit a duplicate candle.
    self._min_open_window = runtime_context.get_state(
      ValueStateDescriptor("min_open_window", Types.LONG())
    )

  def process_element(self, tick, ctx: KeyedProcessFunction.Context):
    window_start = (tick.timestamp // settings.window_size_ms) * settings.window_size_ms
    window_end = window_start + settings.window_size_ms

    min_open = self._min_open_window.value() or 0
    if window_start < min_open:
      log.warning(
        "Dropping late tick for sealed window",
        symbol=tick.symbol,
        tick_ts=tick.timestamp,
        window_start=window_start,
        min_open_window=min_open,
        trade_id=tick.trade_id,
      )
      return

    seen = (
      self._seen_trade_ids.get(window_start)
      if self._seen_trade_ids.contains(window_start)
      else set()
    )
    if tick.trade_id in seen:
      return
    seen.add(tick.trade_id)
    self._seen_trade_ids.put(window_start, seen)

    acc = self._accs.get(window_start) if self._accs.contains(window_start) else None

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
      ctx.timer_service().register_event_time_timer(window_end)
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

    self._accs.put(window_start, acc)

  def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
    # For event-time timers, `timestamp` is exactly the registered fire time
    # — i.e., the window_end we passed to register_event_time_timer.
    window_end = timestamp
    window_start = window_end - settings.window_size_ms

    fire_wall_ms = int(time.time() * 1000)
    log.info(
      "Timer fired",
      symbol=ctx.get_current_key(),
      window_end=window_end,
      fire_wall_ms=fire_wall_ms,
      delay_ms=fire_wall_ms - window_end,
    )

    # If this window has already been sealed by a prior timer fire (e.g. due
    # to a duplicate timer registration that the late-tick guard now blocks
    # but pre-existing state may still trigger), do nothing further.
    min_open = self._min_open_window.value() or 0
    if window_start < min_open:
      log.warning(
        "Timer fired for already-sealed window — skipping",
        symbol=ctx.get_current_key(),
        window_start=window_start,
        min_open_window=min_open,
      )
      return

    acc = self._accs.get(window_start) if self._accs.contains(window_start) else None

    if acc is not None:
      yield from self._build_candle(acc)
      self._last_close.update(acc["close_price"])
      self._accs.remove(window_start)
      if self._seen_trade_ids.contains(window_start):
        self._seen_trade_ids.remove(window_start)
    else:
      # No accumulator for this window — emit a flat gap-fill candle from
      # the last known close, if we have one.
      last_close = self._last_close.value()
      if last_close is not None:
        log.debug(
          "Gap-filling candle", symbol=ctx.get_current_key(), start=window_start
        )
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

    # Mark every window up to and including this one as sealed. New ticks
    # for these windows will be rejected by the late-tick guard above.
    self._min_open_window.update(window_end)

    # Schedule the next window's timer so gap-fills continue even when no
    # ticks arrive to register fresh timers.
    next_window_end = window_end + settings.window_size_ms
    ctx.timer_service().register_event_time_timer(next_window_end)

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
