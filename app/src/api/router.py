"""
FastAPI-роутер: GET /health и POST /signals.
"""

from uuid import uuid4

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from app.src.api.schemas import SignalRequest, SignalResponse
from app.src.core.config import AppConfig
from app.src.domain.sma import calculate_signal_from_candles
from app.src.infrastructure.kafka_worker import KafkaWorker

router = APIRouter()


def build_router(cfg: AppConfig, worker: KafkaWorker) -> APIRouter:
    """
    Создаёт роутер с доступом к конфигурации и воркеру.
    Вызывается один раз при старте приложения в main.py.
    """

    @router.get("/health")
    async def health_check() -> JSONResponse:
        return JSONResponse(
            status_code=200,
            content={
                "status": "UP" if worker.status == "UP" else "DEGRADED",
                "service": cfg.service_name,
                "version": cfg.app_version,
                "components": {
                    "kafka": {
                        "status": worker.status,
                        "details": worker.error_details,
                    }
                },
            },
        )

    @router.post("/signals", response_model=SignalResponse)
    async def create_signal(request: SignalRequest) -> SignalResponse:
        try:
            closes = [c.close for c in request.candles]
            action, confidence, fast_sma, slow_sma = calculate_signal_from_candles(
                closes, cfg.sma_fast_period, cfg.sma_slow_period
            )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

        return SignalResponse(
            signal_id=uuid4(),
            symbol=request.symbol,
            action=action,
            timestamp=request.candles[-1].timestamp,
            confidence=confidence,
            fast_sma=fast_sma,
            slow_sma=slow_sma,
        )

    return router
