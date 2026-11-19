import os
import ssl
import json
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

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

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_SECRET_KEY = os.getenv("KAFKA_SECRET_KEY", "dummy-secret")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sma-crossover-group")
STUDENT_ID = os.getenv("STUDENT_ID", "student-id")
STRATEGY_ID = os.getenv("STRATEGY_ID", "sma-crossover")

KAFKA_TOPIC_REG_REQUEST = os.getenv("KAFKA_TOPIC_REG_REQUEST", "account.registration.request")
KAFKA_TOPIC_REG_RESULT = os.getenv("KAFKA_TOPIC_REG_RESULT", "account.registration.result")
KAFKA_TOPIC_CANDLE_CLOSED = os.getenv("KAFKA_TOPIC_CANDLE_CLOSED", "market.candle.closed")
KAFKA_TOPIC_RECOMMENDATION = os.getenv("KAFKA_TOPIC_RECOMMENDATION", "strategy.recommendation.created")


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
        return self


class SignalRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=50)
    candles: list[Candle] = Field(min_length=1)


class SignalResponse(BaseModel):
    signal_id: UUID
    symbol: str
    action: Literal["BUY", "SELL", "HOLD"]
    timestamp: datetime
    confidence: float
    fast_sma: float
    slow_sma: float


def calculate_signal(
    request: SignalRequest,
    fast_period: int = SMA_FAST_PERIOD,
    slow_period: int = SMA_SLOW_PERIOD,
) -> SignalResponse:
    if len(request.candles) <= slow_period:
        raise ValueError(f"at least {slow_period + 1} candles are required")
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


class RegistrationRequest(BaseModel):
    schemaVersion: int = 1
    secretKey: str
    studentId: str
    strategyId: str
    serviceId: str


class RegistrationResult(BaseModel):
    schemaVersion: int = 1
    isSuccessful: bool
    reason: str | None = None


class CandleEvent(BaseModel):
    schemaVersion: int = 1
    eventId: str
    correlationId: str
    causationId: str | None = None
    runId: str | None = None
    sequenceNumber: int | None = None
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class RecommendationEvent(BaseModel):
    schemaVersion: int = 1
    eventId: str
    sourceEventId: str
    correlationId: str
    causationId: str | None = None
    runId: str | None = None
    sequenceNumber: int | None = None
    studentId: str
    strategyId: str
    serviceId: str
    symbol: str
    action: str
    signal: str
    confidence: float
    suggestedQuantity: float
    dataQuality: str
    reason: str
    processingTimeMs: int


def create_hold_recommendation(
    candle: CandleEvent,
    student_id: str,
    strategy_id: str,
    service_id: str,
) -> RecommendationEvent:
    start_time = time.perf_counter()
    processing_time = int((time.perf_counter() - start_time) * 1000)

    return RecommendationEvent(
        eventId=str(uuid4()),
        sourceEventId=candle.eventId,
        correlationId=candle.correlationId,
        causationId=candle.causationId,
        runId=candle.runId,
        sequenceNumber=candle.sequenceNumber,
        studentId=student_id,
        strategyId=strategy_id,
        serviceId=service_id,
        symbol=candle.symbol,
        action="HOLD",
        signal="HOLD",
        confidence=0.0,
        suggestedQuantity=0.0,
        dataQuality="OK",
        reason="Сквозная обработка HOLD (Лаб 2)",
        processingTimeMs=processing_time,
    )


kafka_status = "DOWN"
error_details = "Not connected"
is_registered = False


async def kafka_worker():
    global kafka_status, error_details, is_registered

    while True:
        producer = None
        consumer = None
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC_REG_RESULT,
                KAFKA_TOPIC_CANDLE_CLOSED,
                bootstrap_servers=KAFKA_BROKER,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="latest",
            )

            await producer.start()
            await consumer.start()
            kafka_status = "UP"
            error_details = ""
            logger.info(f"Подключено к Kafka: {KAFKA_BROKER}")

            reg_msg = RegistrationRequest(
                secretKey=KAFKA_SECRET_KEY,
                studentId=STUDENT_ID,
                strategyId=STRATEGY_ID,
                serviceId=SERVICE_NAME,
            ).model_dump()

            await producer.send_and_wait(KAFKA_TOPIC_REG_REQUEST, reg_msg)
            logger.info("Отправлен запрос на регистрацию.")

            async for msg in consumer:
                if msg.topic == KAFKA_TOPIC_REG_RESULT:
                    result = RegistrationResult(**msg.value)
                    if not result.isSuccessful:
                        logger.warning(f"Отказ в регистрации: {result.reason}")
                        kafka_status = "DEGRADED"
                        error_details = f"Rejected: {result.reason}"
                        is_registered = False
                    else:
                        logger.info("Регистрация успешна. Ожидание свечей...")
                        is_registered = True

                elif msg.topic == KAFKA_TOPIC_CANDLE_CLOSED:
                    if not is_registered:
                        continue

                    candle_event = CandleEvent(**msg.value)
                    logger.info(f"Свеча {candle_event.symbol} получена.")

                    rec_event = create_hold_recommendation(
                        candle_event, STUDENT_ID, STRATEGY_ID, SERVICE_NAME
                    )

                    await producer.send_and_wait(
                        KAFKA_TOPIC_RECOMMENDATION, rec_event.model_dump()
                    )
                    logger.info(f"Сигнал HOLD отправлен для {candle_event.symbol}")

        except Exception as e:
            kafka_status = "DOWN"
            error_details = str(e)
            logger.error(f"Сбой Kafka: {e}. Переподключение через 5 сек...")
            await asyncio.sleep(5)
        finally:
            if consumer is not None:
                try:
                    await consumer.stop()
                except Exception:
                    pass
            if producer is not None:
                try:
                    await producer.stop()
                except Exception:
                    pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(kafka_worker())
    yield
    task.cancel()


app = FastAPI(title=SERVICE_NAME, version=APP_VERSION, lifespan=lifespan)


@app.get("/health")
async def health_check() -> JSONResponse:
    return JSONResponse(
        status_code=200,
        content={
            "status": "UP" if kafka_status == "UP" else "DEGRADED",
            "service": SERVICE_NAME,
            "version": APP_VERSION,
            "components": {
                "kafka": {"status": kafka_status, "details": error_details}
            },
        },
    )


@app.post("/signals", response_model=SignalResponse)
async def create_signal_api(request: SignalRequest) -> SignalResponse:
    try:
        return calculate_signal(request)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT)

    