from __future__ import annotations
from pydantic import BaseModel, field_validator
from typing import Optional

class StockDailyRecord(BaseModel):
    symbol: str
    date: str  # ISO YYYY-MM-DD
    open: float
    high: float
    low: float
    close: float
    volume: int
    daily_change_percentage: float

    @field_validator("symbol")
    @classmethod
    def symbol_upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("date")
    @classmethod
    def valid_date_format(cls, v: str) -> str:
        # very lightweight validation
        parts = v.split("-")
        if len(parts) != 3 or any(not p for p in parts):
            raise ValueError("date must be YYYY-MM-DD")
        return v