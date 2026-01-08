from __future__ import annotations
from decimal import Decimal
from pydantic import BaseModel


class Tick(BaseModel):
  symbol: str
  price: Decimal
  quantity: Decimal
  timestamp: int  # Unix (ms)
  trade_id: int

  @classmethod
  def from_binance(cls, data: dict) -> Tick:
    return cls(
      symbol=data["s"],
      price=data["p"],
      quantity=data["q"],
      timestamp=data["T"],
      trade_id=data["t"],
    )
