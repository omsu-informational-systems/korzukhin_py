"""
Алгоритм SMA Crossover.

Содержит:
- SmaState  — хранит скользящее окно цен закрытия для одного тикера
              и вычисляет торговый сигнал по методу пересечения SMA.
- calculate_signal_from_candles — stateless-версия для REST API (/signals).
"""

from collections import deque
from typing import Literal


class SmaState:
    """
    Stateful-хранилище скользящего окна для потоковой обработки свечей из Kafka.

    Держит ровно slow+1 последних цен закрытия — минимум, необходимый
    для детектирования пересечения Fast SMA и Slow SMA.
    """

    def __init__(self, fast: int, slow: int) -> None:
        if fast < 1 or slow <= fast:
            raise ValueError("fast must be >= 1 and slow > fast")
        self.fast = fast
        self.slow = slow
        self._prices: deque[float] = deque(maxlen=slow + 1)

    def push(self, close: float) -> None:
        """Добавить новую цену закрытия."""
        self._prices.append(close)

    def ready(self) -> bool:
        """True, если накоплено достаточно данных для расчёта пересечения."""
        return len(self._prices) >= self.slow + 1

    def signal(self) -> tuple[Literal["BUY", "SELL", "HOLD"], float, float, float]:
        """
        Рассчитать сигнал по текущему окну.

        Возвращает: (action, confidence, fast_sma, slow_sma)
        Вызывать только если ready() == True.
        """
        prices = list(self._prices)
        prev_fast = sum(prices[-(self.fast + 1):-1]) / self.fast
        prev_slow = sum(prices[-(self.slow + 1):-1]) / self.slow
        cur_fast  = sum(prices[-self.fast:]) / self.fast
        cur_slow  = sum(prices[-self.slow:]) / self.slow

        if prev_fast <= prev_slow and cur_fast > cur_slow:
            return "BUY", 1.0, cur_fast, cur_slow
        if prev_fast >= prev_slow and cur_fast < cur_slow:
            return "SELL", 1.0, cur_fast, cur_slow
        return "HOLD", 0.0, cur_fast, cur_slow


def calculate_signal_from_candles(
    closes: list[float],
    fast: int,
    slow: int,
) -> tuple[Literal["BUY", "SELL", "HOLD"], float, float, float]:
    """
    Stateless-расчёт сигнала по готовому списку цен закрытия.

    Используется в REST-эндпоинте POST /signals.
    Возвращает: (action, confidence, fast_sma, slow_sma)
    Выбрасывает ValueError, если данных недостаточно.
    """
    if len(closes) <= slow:
        raise ValueError(
            f"Требуется минимум {slow + 1} свечей для расчёта пересечения SMA"
        )

    prev_fast = sum(closes[-(fast + 1):-1]) / fast
    prev_slow = sum(closes[-(slow + 1):-1]) / slow
    cur_fast  = sum(closes[-fast:]) / fast
    cur_slow  = sum(closes[-slow:]) / slow

    if prev_fast <= prev_slow and cur_fast > cur_slow:
        return "BUY", 1.0, cur_fast, cur_slow
    if prev_fast >= prev_slow and cur_fast < cur_slow:
        return "SELL", 1.0, cur_fast, cur_slow
    return "HOLD", 0.0, cur_fast, cur_slow
