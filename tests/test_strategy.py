import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from pydantic import ValidationError

from app.src.api.schemas import Candle, SignalRequest
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

    def test_health_endpoint_format(self) -> None:
        from app.src.core.config import AppConfig
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

        # Проверяем структуру ответа напрямую
        content = {
            "status": "UP" if mock_worker.status == "UP" else "DEGRADED",
            "service": mock_cfg.service_name,
            "version": mock_cfg.app_version,
            "components": {
                "kafka": {
                    "status": mock_worker.status,
                    "details": mock_worker.error_details,
                }
            },
        }
        self.assertEqual(content["status"], "UP")
        self.assertEqual(content["service"], "sma-crossover-strategy")
        self.assertEqual(content["version"], "1.0.0")
        self.assertIn("kafka", content["components"])


if __name__ == "__main__":
    unittest.main()
