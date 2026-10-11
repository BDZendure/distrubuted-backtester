"""SMA signals depend on exact observed closes and changes in target regime."""

from datetime import date, timedelta

import pytest

from distributed_backtester.models import (
    CloseHistory,
    CloseObservation,
    Portfolio,
    StrategyConfig,
    TargetIntent,
)
from distributed_backtester.strategy import MovingAverageCrossover, Strategy


def history(*closes: int) -> CloseHistory:
    return CloseHistory(
        tuple(
            CloseObservation(date(2026, 1, 2) + timedelta(days=offset), close)
            for offset, close in enumerate(closes)
        )
    )


@pytest.mark.parametrize("closes", [(), (10,), (10, 20)])
def test_warmup_emits_nothing(closes: tuple[int, ...]) -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    assert strategy.on_close(history(*closes), Portfolio(100, 0)) is None


@pytest.mark.parametrize("closes", [(10, 10, 10), (10, 5, 15)])
def test_initial_equality_retains_flat(closes: tuple[int, ...]) -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    assert strategy.on_close(history(*closes), Portfolio(100, 0)) is None


def test_initial_negative_comparison_retains_flat() -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    assert strategy.on_close(history(30, 20, 10), Portfolio(100, 0)) is None


@pytest.mark.parametrize("closes", [(10, 10, 10), (30, 20, 10)])
def test_initial_flat_comparisons_do_not_consume_sequences(
    closes: tuple[int, ...],
) -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    portfolio = Portfolio(100, 0)
    assert strategy.on_close(history(*closes), portfolio) is None
    assert strategy.on_close(history(*closes, 100), portfolio) == TargetIntent(
        "invested", date(2026, 1, 5), 1
    )


def test_first_ready_positive_comparison_enters() -> None:
    strategy: Strategy = MovingAverageCrossover(StrategyConfig())
    portfolio = Portfolio(100, 0)
    assert strategy.on_close(history(10), portfolio) is None
    assert strategy.on_close(history(10, 20), portfolio) is None
    assert strategy.on_close(history(10, 20, 30), portfolio) == TargetIntent(
        "invested", date(2026, 1, 4), 1
    )
    assert portfolio == Portfolio(100, 0)


def test_strict_negative_comparison_exits() -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    assert strategy.on_close(history(10, 20, 30), Portfolio(100, 0)) == TargetIntent(
        "invested", date(2026, 1, 4), 1
    )
    assert strategy.on_close(history(10, 20, 30, 1), Portfolio(0, 3)) == TargetIntent(
        "flat", date(2026, 1, 5), 2
    )
    assert strategy.on_close(history(10, 20, 30, 1), Portfolio(0, 3)) is None


def test_equality_retains_invested_until_strict_negative_comparison() -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    assert strategy.on_close(history(10, 20, 30), Portfolio(100, 0)) == TargetIntent(
        "invested", date(2026, 1, 4), 1
    )
    # The fast mean is (30 + 10) / 2 = 20; the slow mean is also 20.
    assert strategy.on_close(history(10, 20, 30, 10), Portfolio(0, 3)) is None
    assert strategy.on_close(
        history(10, 20, 30, 10, 1), Portfolio(0, 3)
    ) == TargetIntent("flat", date(2026, 1, 6), 2)


def test_unchanged_regimes_do_not_rebalance_or_consume_sequences() -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    assert strategy.on_close(history(10, 20, 30), Portfolio(100, 0)) == TargetIntent(
        "invested", date(2026, 1, 4), 1
    )
    assert strategy.on_close(history(10, 20, 30, 40), Portfolio(5, 3)) is None
    assert strategy.on_close(history(10, 20, 30, 40, 50), Portfolio(5, 3)) is None
    assert strategy.on_close(
        history(10, 20, 30, 40, 50, 10), Portfolio(5, 3)
    ) == TargetIntent("flat", date(2026, 1, 7), 2)
    assert (
        strategy.on_close(history(10, 20, 30, 40, 50, 10, 5), Portfolio(35, 0)) is None
    )
    assert strategy.on_close(
        history(10, 20, 30, 40, 50, 10, 5, 20), Portfolio(35, 0)
    ) == TargetIntent("invested", date(2026, 1, 9), 3)


def test_unfilled_entry_is_not_reissued_without_a_regime_change() -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    empty_portfolio = Portfolio(1, 0)
    assert strategy.on_close(history(10, 20, 30), empty_portfolio) == TargetIntent(
        "invested", date(2026, 1, 4), 1
    )
    assert strategy.on_close(history(10, 20, 30), empty_portfolio) is None
    assert strategy.on_close(history(10, 20, 30, 40), empty_portfolio) is None
    assert strategy.on_close(
        history(10, 20, 30, 40, 10), empty_portfolio
    ) == TargetIntent("flat", date(2026, 1, 6), 2)
    assert strategy.on_close(
        history(10, 20, 30, 40, 10, 100), empty_portfolio
    ) == TargetIntent("invested", date(2026, 1, 7), 3)


def test_instances_have_independent_targets_and_sequences() -> None:
    config = StrategyConfig()
    first = MovingAverageCrossover(config)
    second = MovingAverageCrossover(config)
    portfolio = Portfolio(100, 0)
    rising = history(10, 20, 30)
    assert first.on_close(rising, portfolio) == TargetIntent(
        "invested", date(2026, 1, 4), 1
    )
    assert first.on_close(history(10, 20, 30, 1), portfolio) == TargetIntent(
        "flat", date(2026, 1, 5), 2
    )
    assert second.on_close(rising, portfolio) == TargetIntent(
        "invested", date(2026, 1, 4), 1
    )
    assert first.on_close(history(10, 20, 30, 1, 100), portfolio) == TargetIntent(
        "invested", date(2026, 1, 6), 3
    )
    assert second.on_close(rising, portfolio) is None


@pytest.mark.parametrize(("fast", "slow"), [(1, 2), (1, 4), (3, 5)])
def test_configured_windows_control_readiness_and_comparison(
    fast: int, slow: int
) -> None:
    strategy = MovingAverageCrossover(StrategyConfig(fast, slow))
    portfolio = Portfolio(100, 0)
    assert strategy.on_close(history(*range(1, slow)), portfolio) is None
    assert strategy.on_close(history(*range(1, slow + 1)), portfolio) == TargetIntent(
        "invested", date(2026, 1, 2) + timedelta(days=slow - 1), 1
    )


def test_only_latest_windows_affect_comparison() -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    assert strategy.on_close(history(10**30, 10, 20, 30), Portfolio(100, 0)) == (
        TargetIntent("invested", date(2026, 1, 5), 1)
    )


def test_history_bounds_hide_later_appended_observations() -> None:
    observations = [
        CloseObservation(date(2026, 1, 2), 10),
        CloseObservation(date(2026, 1, 5), 20),
        CloseObservation(date(2026, 1, 9), 30),
    ]
    bounded = CloseHistory(observations)
    observations.append(CloseObservation(date(2026, 1, 12), 1))
    strategy = MovingAverageCrossover(StrategyConfig())
    assert strategy.on_close(bounded, Portfolio(100, 0)) == TargetIntent(
        "invested", date(2026, 1, 9), 1
    )
    assert bounded[:] == tuple(observations[:3])


@pytest.mark.parametrize(
    "base", [2**60, 10**400], ids=["beyond-float-precision", "beyond-float-range"]
)
def test_huge_integer_closes_preserve_strict_entry_and_exit(base: int) -> None:
    strategy = MovingAverageCrossover(StrategyConfig())
    portfolio = Portfolio(100, 0)
    assert strategy.on_close(history(base, base, base + 1), portfolio) == TargetIntent(
        "invested", date(2026, 1, 4), 1
    )
    assert strategy.on_close(
        history(base, base, base + 1, base - 2), portfolio
    ) == TargetIntent("flat", date(2026, 1, 5), 2)
