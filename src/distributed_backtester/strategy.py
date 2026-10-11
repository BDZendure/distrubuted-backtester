"""Strategies decide target regimes using only completed close observations."""

from typing import Protocol

from distributed_backtester.models import (
    CloseHistory,
    Portfolio,
    StrategyConfig,
    TargetIntent,
)


class Strategy(Protocol):
    """Receive completed closes and the current portfolio to decide an intent."""

    def on_close(
        self, history: CloseHistory, portfolio: Portfolio
    ) -> TargetIntent | None: ...


class MovingAverageCrossover:
    """Emit an intent only when the exact SMA comparison changes the target.

    The target tracks the signal regime independently of portfolio holdings.
    An unfilled intent therefore is not retried until the regime changes.
    """

    def __init__(self, config: StrategyConfig) -> None:
        self._config = config
        self._target = "flat"
        self._sequence = 0

    def on_close(
        self, history: CloseHistory, portfolio: Portfolio
    ) -> TargetIntent | None:
        fast = self._config.fast_window
        slow = self._config.slow_window
        if len(history) < slow:
            return None

        fast_sum = sum(observation.close for observation in history[-fast:])
        slow_sum = sum(observation.close for observation in history[-slow:])
        comparison = fast_sum * slow - slow_sum * fast
        if comparison > 0:
            target = "invested"
        elif comparison < 0:
            target = "flat"
        else:
            return None

        if target == self._target:
            return None
        self._target = target
        self._sequence += 1
        return TargetIntent(target, history[-1].session, self._sequence)
