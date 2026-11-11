import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

load_dotenv()

SERVICE_NAME = os.getenv("SERVICE_NAME", "sma-crossover-strategy")
APP_VERSION = os.getenv("APP_VERSION", "1.0.0")


def _read_period(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        period = int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be a positive integer") from error
    if period < 1:
        raise ValueError(f"{name} must be a positive integer")
    return period


PORT = _read_period("PORT", 8000)
SMA_FAST_PERIOD = _read_period("SMA_FAST_PERIOD", 10)
SMA_SLOW_PERIOD = _read_period("SMA_SLOW_PERIOD", 30)
if SMA_FAST_PERIOD >= SMA_SLOW_PERIOD:
    raise ValueError("SMA_FAST_PERIOD must be less than SMA_SLOW_PERIOD")


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


def calculate_signal(
    request: SignalRequest,
    fast_period: int = SMA_FAST_PERIOD,
    slow_period: int = SMA_SLOW_PERIOD,
) -> SignalResponse:
    if fast_period < 1 or slow_period <= fast_period:
        raise ValueError("periods must be positive and fast_period must be less than slow_period")
    if len(request.candles) <= slow_period:
        raise ValueError(
            f"at least {slow_period + 1} candles are required to detect an SMA crossover"
        )

    closes = [candle.close for candle in request.candles]
    previous_fast = sum(closes[-fast_period - 1 : -1]) / fast_period
    previous_slow = sum(closes[-slow_period - 1 : -1]) / slow_period
    fast_sma = sum(closes[-fast_period:]) / fast_period
    slow_sma = sum(closes[-slow_period:]) / slow_period

    action: Literal["BUY", "SELL", "HOLD"] = "HOLD"
    if previous_fast <= previous_slow and fast_sma > slow_sma:
        action = "BUY"
    elif previous_fast >= previous_slow and fast_sma < slow_sma:
        action = "SELL"

    return SignalResponse(
        signal_id=uuid4(),
        symbol=request.symbol,
        action=action,
        timestamp=request.candles[-1].timestamp,
        confidence=1.0 if action != "HOLD" else 0.0,
        fast_sma=fast_sma,
        slow_sma=slow_sma,
    )


@asynccontextmanager
async def lifespan(_: FastAPI):
    print(f"Сервис {SERVICE_NAME} v{APP_VERSION} запущен.")
    yield
    print(f"Сервис {SERVICE_NAME} корректно останавливается...")


app = FastAPI(title=SERVICE_NAME, version=APP_VERSION, lifespan=lifespan)


@app.get("/health")
async def health_check() -> JSONResponse:
    return JSONResponse(
        status_code=200,
        content={
            "status": "UP",
            "service": SERVICE_NAME,
            "version": APP_VERSION,
            "components": {},
        },
    )


@app.post("/signals", response_model=SignalResponse)
async def create_signal(request: SignalRequest) -> SignalResponse:
    try:
        return calculate_signal(request)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT)
