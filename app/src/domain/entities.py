import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> str:
    """Текущее время UTC в формате ISO 8601 с суффиксом Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Универсальный конверт платформы
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MessageEnvelope:
    """Общая обёртка для всех событий платформы."""

    eventType: str
    producer: str
    payload: dict[str, Any]
    schemaVersion: str = "1.0"
    eventId: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurredAt: str = field(default_factory=_utc_now)
    correlationId: str | None = None
    causationId: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        if data["correlationId"] is None:
            data["correlationId"] = self.eventId
        return data


# ---------------------------------------------------------------------------
# Регистрация
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RegistrationRequestPayload:
    """Полезная нагрузка account.registration.request."""

    secretKey: str
    accountId: str
    accountName: str
    groupId: str
    strategyId: str
    strategyName: str
    serviceName: str
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RegistrationResultPayload:
    """Полезная нагрузка account.registration.result."""

    secretKey: str
    accountId: str
    groupId: str
    strategyId: str
    serviceName: str
    status: str        # "REGISTERED" | "REJECTED"
    reason: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RegistrationResultPayload":
        return cls(
            secretKey=data.get("secretKey", ""),
            accountId=data.get("accountId", ""),
            groupId=data.get("groupId", ""),
            strategyId=data.get("strategyId", ""),
            serviceName=data.get("serviceName", ""),
            status=data.get("status", ""),
            reason=data.get("reason", ""),
        )


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class HeartbeatPayload:
    """Полезная нагрузка account.service.heartbeat."""

    secretKey: str
    accountId: str
    groupId: str
    strategyId: str
    serviceName: str
    instanceId: str
    status: str
    startedAt: str
    sentAt: str
    processedCandles: int
    publishedRecommendations: int
    lastProcessedSequenceNumber: int | None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Свечи
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class InstrumentInfo:
    ticker: str
    figi: str = ""
    currency: str = "RUB"
    lotSize: int = 1


@dataclass(frozen=True)
class CandleClosedPayload:
    """Полезная нагрузка market.candle.closed."""

    runId: str
    groupId: str
    instrument: InstrumentInfo
    timeframe: str
    sequenceNumber: int
    candleTime: str
    tradingDate: str
    sessionPhase: str
    open: str
    high: str
    low: str
    close: str
    volume: int
    isWarmup: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CandleClosedPayload":
        inst = data.get("instrument", {})
        return cls(
            runId=data["runId"],
            groupId=data.get("groupId", ""),
            instrument=InstrumentInfo(
                ticker=inst.get("ticker", ""),
                figi=inst.get("figi", ""),
                currency=inst.get("currency", "RUB"),
                lotSize=int(inst.get("lotSize", 1)),
            ),
            timeframe=data.get("timeframe", "1m"),
            sequenceNumber=int(data["sequenceNumber"]),
            candleTime=data.get("candleTime", ""),
            tradingDate=data.get("tradingDate", ""),
            sessionPhase=data.get("sessionPhase", "MAIN_SESSION"),
            open=str(data["open"]),
            high=str(data["high"]),
            low=str(data["low"]),
            close=str(data["close"]),
            volume=int(data.get("volume", 0)),
            isWarmup=bool(data.get("isWarmup", False)),
        )


# ---------------------------------------------------------------------------
# Рекомендация
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RecommendationPayload:
    """Полезная нагрузка strategy.recommendation.created."""

    recommendationId: str
    sourceEventId: str
    runId: str
    secretKey: str
    strategyId: str
    accountId: str
    groupId: str
    instrument: str       # ticker
    sequenceNumber: int
    signal: str           # "BUY" | "SELL" | "HOLD"
    confidence: float
    suggestedQuantity: int
    dataQuality: str      # "OK" | "DEGRADED"
    reason: str
    processingTimeMs: int
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
