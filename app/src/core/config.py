import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    value = os.environ.get(key, "").strip()
    if not value:
        raise ValueError(f"Отсутствует обязательная переменная окружения: {key}")
    return value


@dataclass(frozen=True)
class AppConfig:
    # Сервис
    service_name: str
    app_version: str
    port: int
    sma_fast_period: int
    sma_slow_period: int

    # Kafka — подключение
    bootstrap_servers: str
    kafka_username: str
    kafka_password: str

    # Идентификаторы аккаунта
    secret_key: str
    account_id: str
    account_name: str
    group_id: str
    strategy_id: str
    kafka_group_id: str

    # Топики
    topic_reg_request: str
    topic_reg_result: str
    topic_candle_closed: str
    topic_recommendation: str
    topic_heartbeat: str

    @classmethod
    def load(cls) -> "AppConfig":
        fast = int(os.environ.get("SMA_FAST_PERIOD", "10"))
        slow = int(os.environ.get("SMA_SLOW_PERIOD", "30"))
        if fast >= slow:
            raise ValueError("SMA_FAST_PERIOD must be less than SMA_SLOW_PERIOD")
        return cls(
            service_name=os.environ.get("SERVICE_NAME", "sma-crossover-strategy"),
            app_version=os.environ.get("APP_VERSION", "1.0.0"),
            port=int(os.environ.get("PORT", "8000")),
            sma_fast_period=fast,
            sma_slow_period=slow,
            bootstrap_servers=os.environ.get(
                "KAFKA_BOOTSTRAP_SERVERS", "omsu.7bits.it:9094"
            ),
            kafka_username=_require("KAFKA_USERNAME"),
            kafka_password=_require("KAFKA_PASSWORD"),
            secret_key=_require("ACCOUNT_SECRET_KEY"),
            account_id=_require("ACCOUNT_ID"),
            account_name=os.environ.get("ACCOUNT_NAME", ""),
            group_id=_require("GROUP_ID"),
            strategy_id=_require("STRATEGY_ID"),
            kafka_group_id=_require("KAFKA_GROUP_ID"),
            topic_reg_request=os.environ.get(
                "KAFKA_TOPIC_REG_REQUEST", "account.registration.request"
            ),
            topic_reg_result=os.environ.get(
                "KAFKA_TOPIC_REG_RESULT", "account.registration.result"
            ),
            topic_candle_closed=os.environ.get(
                "KAFKA_TOPIC_CANDLE_CLOSED", "market.candle.closed"
            ),
            topic_recommendation=os.environ.get(
                "KAFKA_TOPIC_RECOMMENDATION", "strategy.recommendation.created"
            ),
            topic_heartbeat=os.environ.get(
                "KAFKA_TOPIC_HEARTBEAT", "account.service.heartbeat"
            ),
        )
