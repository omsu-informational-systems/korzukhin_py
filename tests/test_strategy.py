import asyncio
import json
import unittest
from datetime import datetime, timedelta, timezone

from pydantic import ValidationError

from app.main import Candle, SignalRequest, calculate_signal, health_check


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
        request = SignalRequest(symbol="TEST", candles=make_candles([5, 4, 3, 6]))
        response = calculate_signal(request, fast_period=2, slow_period=3)
        self.assertEqual(response.action, "BUY")
        self.assertEqual(response.confidence, 1.0)

    def test_sell_crossover(self) -> None:
        request = SignalRequest(symbol="TEST", candles=make_candles([3, 4, 5, 2]))
        response = calculate_signal(request, fast_period=2, slow_period=3)
        self.assertEqual(response.action, "SELL")
        self.assertEqual(response.confidence, 1.0)

    def test_hold_when_no_crossover(self) -> None:
        request = SignalRequest(symbol="TEST", candles=make_candles([1, 2, 3, 4]))
        response = calculate_signal(request, fast_period=2, slow_period=3)
        self.assertEqual(response.action, "HOLD")
        self.assertEqual(response.confidence, 0.0)

    def test_insufficient_candles_raises_error(self) -> None:
        request = SignalRequest(symbol="TEST", candles=make_candles([1, 2, 3]))
        with self.assertRaises(ValueError):
            calculate_signal(request, fast_period=2, slow_period=3)

    def test_non_chronological_candles_rejected(self) -> None:
        candles = make_candles([1, 2])
        candles[1].timestamp = candles[0].timestamp
        with self.assertRaises(ValidationError):
            SignalRequest(symbol="TEST", candles=candles)

    def test_health_endpoint_format(self) -> None:
        response = asyncio.run(health_check())
        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["status"], "UP")
        self.assertEqual(payload["service"], "sma-crossover-strategy")
        self.assertEqual(payload["version"], "1.0.0")
        self.assertEqual(payload["components"], {})
        
    def test_hold_recommendation_mapping(self) -> None:
        from app.main import CandleEvent, create_hold_recommendation
        from datetime import datetime, timezone

        test_candle = CandleEvent(
            eventId="source-uuid-123",
            correlationId="corr-uuid-456",
            causationId="cause-uuid-789",
            runId="run-uuid-000",
            sequenceNumber=42,
            symbol="TEST-HOLD",
            timestamp=datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc),
            open=100.0, high=105.0, low=95.0, close=100.0, volume=1000.0
        )

        recommendation = create_hold_recommendation(
            test_candle, "test_student", "test_strategy", "test_service"
        )

        self.assertEqual(recommendation.action, "HOLD")
        self.assertEqual(recommendation.signal, "HOLD")
        self.assertEqual(recommendation.confidence, 0.0)
        self.assertEqual(recommendation.suggestedQuantity, 0.0)
        self.assertEqual(recommendation.sourceEventId, test_candle.eventId)
        self.assertEqual(recommendation.correlationId, test_candle.correlationId)
        self.assertEqual(recommendation.runId, test_candle.runId)
        self.assertEqual(recommendation.sequenceNumber, test_candle.sequenceNumber)


if __name__ == "__main__":
    unittest.main()