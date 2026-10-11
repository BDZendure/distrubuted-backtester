"""Known return/drawdown paths with initial cash as an independent anchor."""

from datetime import date, timedelta
from fractions import Fraction

import pytest

from distributed_backtester.metrics import calculate_metrics
from distributed_backtester.models import EquityPoint


def points(values: list[int]) -> tuple[EquityPoint, ...]:
    return tuple(
        EquityPoint(date(2026, 1, 1) + timedelta(days=index), value, 0, 1, value)
        for index, value in enumerate(values)
    )


@pytest.mark.parametrize(
    ("values", "total_return", "drawdown"),
    [
        ([100, 100], Fraction(0), Fraction(0)),
        ([110, 120], Fraction(1, 5), Fraction(0)),
        ([90, 80], Fraction(-1, 5), Fraction(1, 5)),
        ([120, 90, 110], Fraction(1, 10), Fraction(1, 4)),
        ([0], Fraction(-1), Fraction(1)),
    ],
)
def test_exact_metrics(
    values: list[int], total_return: Fraction, drawdown: Fraction
) -> None:
    result = calculate_metrics(100, points(values))
    assert result.total_return == total_return
    assert result.maximum_drawdown == drawdown
    assert result.final_equity == values[-1]
    assert result.fill_count == 0
    assert result.total_fees == 0


def test_reference_loss_path_matches_exact_expected_ratios() -> None:
    result = calculate_metrics(
        1_000_000_000, points([1_000_000_000, 916_004_000, 749_257_000])
    )
    assert result.total_return == Fraction(-250743, 1_000_000)
    assert result.maximum_drawdown == Fraction(250743, 1_000_000)


def test_metrics_reject_empty_path_and_invalid_initial_cash() -> None:
    with pytest.raises(ValueError, match="empty"):
        calculate_metrics(100, ())
    with pytest.raises(ValueError, match="initial_cash"):
        calculate_metrics(0, points([100]))
