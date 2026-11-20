"""
Pydantic-схемы для REST API.

SignalRequest  — тело запроса POST /signals
SignalResponse — тело ответа  POST /signals
"""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Candle(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    timestamp: datetime
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: float = Field(ge=0)

    @field_validator("timestamp")
    @classmethod
    def timestamp_must_include_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include a timezone")
        return value

    @model_validator(mode="after")
    def validate_ohlc(self) -> "Candle":
        if self.low > min(self.open, self.close):
            raise ValueError("low must not exceed open or close")
        if self.high < max(self.open, self.close):
            raise ValueError("high must not be less than open or close")
        if self.low > self.high:
            raise ValueError("low must not exceed high")
        return self


class SignalRequest(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    symbol: str = Field(min_length=1, max_length=50)
    candles: list[Candle] = Field(min_length=1)

    @field_validator("symbol")
    @classmethod
    def symbol_must_not_be_blank(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("symbol must not be blank")
        return normalized

    @field_validator("candles")
    @classmethod
    def candles_must_be_chronological(cls, candles: list[Candle]) -> list[Candle]:
        if any(
            current.timestamp <= previous.timestamp
            for previous, current in zip(candles, candles[1:])
        ):
            raise ValueError("candles must be ordered by strictly increasing timestamp")
        return candles


class SignalResponse(BaseModel):
    signal_id: UUID
    symbol: str
    action: Literal["BUY", "SELL", "HOLD"]
    timestamp: datetime
    confidence: float = Field(ge=0, le=1)
    fast_sma: float
    slow_sma: float
