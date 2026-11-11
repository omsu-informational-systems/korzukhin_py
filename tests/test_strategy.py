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


if __name__ == "__main__":
    unittest.main()
