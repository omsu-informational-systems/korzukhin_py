"""
Точка входа приложения.

Инициализирует конфигурацию, Kafka-воркер и FastAPI-приложение.
"""

import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from app.src.core.config import AppConfig
from app.src.infrastructure.kafka_worker import KafkaWorker
from app.src.api.router import build_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

cfg = AppConfig.load()
worker = KafkaWorker(cfg)


@asynccontextmanager
async def lifespan(_: FastAPI):
    worker.start()
    yield
    worker.stop()


app = FastAPI(
    title=cfg.service_name,
    version=cfg.app_version,
    lifespan=lifespan,
)

app.include_router(build_router(cfg, worker))


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=cfg.port)
