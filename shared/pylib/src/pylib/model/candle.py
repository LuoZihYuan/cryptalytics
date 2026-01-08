from __future__ import annotations
from decimal import Decimal
from pydantic import BaseModel


class Candle(BaseModel):
  symbol: str
  start: int  # Unix (ms)
  end: int  # Unix (ms)
  open: Decimal
  high: Decimal
  low: Decimal
  close: Decimal
  volume: Decimal

  @classmethod
  def from_binance(cls, symbol: str, kline: list) -> Candle:
    return cls(
      symbol=symbol,
      start=kline[0],
      end=kline[6],
      open=kline[1],
      high=kline[2],
      low=kline[3],
      close=kline[4],
      volume=kline[5],
    )
