import asyncio
import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from pydantic import ValidationError

from app.src.api.schemas import Candle, SignalRequest
from app.src.core.config import AppConfig
from app.src.domain.sma import SmaState, calculate_signal_from_candles


def make_candles(closes: list[float]) -> list[Candle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    return [
        Candle(
            timestamp=start + timedelta(minutes=index),
            open=price,
            high=price,
            low=price,
            close=price,
            volume=1.0,
        )
        for index, price in enumerate(closes)
    ]


class SmaCrossoverTests(unittest.TestCase):

    def test_buy_crossover(self) -> None:
        closes = [c.close for c in make_candles([5, 4, 3, 6])]
        action, confidence, fast_sma, slow_sma = calculate_signal_from_candles(closes, fast=2, slow=3)
        self.assertEqual(action, "BUY")
        self.assertEqual(confidence, 1.0)

    def test_sell_crossover(self) -> None:
        closes = [c.close for c in make_candles([3, 4, 5, 2])]
        action, confidence, fast_sma, slow_sma = calculate_signal_from_candles(closes, fast=2, slow=3)
        self.assertEqual(action, "SELL")
        self.assertEqual(confidence, 1.0)

    def test_hold_when_no_crossover(self) -> None:
        closes = [c.close for c in make_candles([1, 2, 3, 4])]
        action, confidence, fast_sma, slow_sma = calculate_signal_from_candles(closes, fast=2, slow=3)
        self.assertEqual(action, "HOLD")
        self.assertEqual(confidence, 0.0)

    def test_insufficient_candles_raises_error(self) -> None:
        closes = [c.close for c in make_candles([1, 2, 3])]
        with self.assertRaises(ValueError):
            calculate_signal_from_candles(closes, fast=2, slow=3)

    def test_non_chronological_candles_rejected(self) -> None:
        candles = make_candles([1, 2])
        candles[1].timestamp = candles[0].timestamp
        with self.assertRaises(ValidationError):
            SignalRequest(symbol="TEST", candles=candles)

    def test_sma_state_accumulates_prices(self) -> None:
        state = SmaState(fast=2, slow=3)
        self.assertFalse(state.ready())
        for price in [1, 2, 3, 4]:
            state.push(float(price))
        self.assertTrue(state.ready())

    def test_kafka_connection_settings_are_loaded_from_environment(self) -> None:
        environment = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker.example:9094",
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
            "KAFKA_AUTO_OFFSET_RESET": "latest",
            "KAFKA_USERNAME": "test-user",
            "KAFKA_PASSWORD": "test-password",
            "ACCOUNT_SECRET_KEY": "test-secret",
            "ACCOUNT_ID": "test-account",
            "GROUP_ID": "test-group",
            "STRATEGY_ID": "test-strategy",
            "KAFKA_GROUP_ID": "test-consumer",
        }
        with patch.dict(os.environ, environment, clear=True):
            cfg = AppConfig.load()

        self.assertEqual(cfg.bootstrap_servers, "broker.example:9094")
        self.assertEqual(cfg.kafka_security_protocol, "SASL_SSL")
        self.assertEqual(cfg.kafka_sasl_mechanism, "SCRAM-SHA-512")
        self.assertEqual(cfg.kafka_auto_offset_reset, "latest")

        from app.src.infrastructure.kafka_worker import KafkaWorker

        with patch("app.src.infrastructure.kafka_worker.Consumer") as consumer:
            KafkaWorker(cfg)._make_consumer("candles")

        consumer_config = consumer.call_args.args[0]
        self.assertEqual(consumer_config["security.protocol"], "SASL_SSL")
        self.assertEqual(consumer_config["sasl.mechanism"], "SCRAM-SHA-512")
        self.assertEqual(consumer_config["auto.offset.reset"], "latest")
        self.assertEqual(consumer_config["group.id"], "test-consumer.candles")
        self.assertFalse(consumer_config["enable.auto.commit"])

    def test_health_endpoint_format(self) -> None:
        from app.src.api.router import build_router
        from app.src.infrastructure.kafka_worker import KafkaWorker

        mock_cfg = MagicMock(spec=AppConfig)
        mock_cfg.service_name = "sma-crossover-strategy"
        mock_cfg.app_version = "1.0.0"
        mock_cfg.sma_fast_period = 10
        mock_cfg.sma_slow_period = 30

        mock_worker = MagicMock(spec=KafkaWorker)
        mock_worker.status = "UP"
        mock_worker.error_details = ""

        router = build_router(mock_cfg, mock_worker)
        health_route = next(route for route in router.routes if route.path == "/health")
        response = asyncio.run(health_route.endpoint())
        content = json.loads(response.body)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            content,
            {
                "status": "UP",
                "service": "sma-crossover-strategy",
                "version": "1.0.0",
                "components": {"kafka": {"status": "UP", "details": ""}},
            },
        )

        mock_worker.status = "DOWN"
        mock_worker.error_details = "TimeoutError"
        response = asyncio.run(health_route.endpoint())
        content = json.loads(response.body)
        self.assertEqual(content["status"], "DEGRADED")
        self.assertEqual(content["components"]["kafka"]["details"], "TimeoutError")


if __name__ == "__main__":
    unittest.main()
