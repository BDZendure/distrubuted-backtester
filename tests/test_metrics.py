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


def test_metrics_constructs_only_final_rational_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import distributed_backtester.metrics as module

    calls = 0

    def counted(numerator: int = 0, denominator: int = 1) -> Fraction:
        nonlocal calls
        calls += 1
        return Fraction(numerator, denominator)

    monkeypatch.setattr(module, "Fraction", counted)
    result = module.calculate_metrics(100, points([100, 120, 90, 180, 135, 0]))
    assert result.maximum_drawdown == Fraction(1)
    assert calls == 2


def test_metrics_matches_independent_fraction_reference() -> None:
    import random

    generator = random.Random(1729)
    paths = [
        [100, 120, 90, 180, 135],
        [100, 0, 100, 0],
        [10**400, 10**400 - 1, 2 * 10**400, 10**400],
    ]
    paths.extend([generator.randrange(0, 1000) for _ in range(300)] for _ in range(30))
    for values in paths:
        initial = max(100, values[0])
        peak = initial
        reference = Fraction(0)
        for value in values:
            peak = max(peak, value)
            reference = max(reference, Fraction(peak - value, peak))
        actual = calculate_metrics(initial, points(values))
        assert actual.maximum_drawdown == reference
        assert actual.total_return == Fraction(values[-1] - initial, initial)
        assert actual.final_equity == values[-1]


@pytest.mark.parametrize("bad", [-1, True, 1.5])
def test_metrics_preserves_validation_of_late_equity(bad: object) -> None:
    from dataclasses import replace
    from typing import cast

    path = list(points([100, 90, 0]))
    path[-1] = replace(path[-1], equity=cast(int, bad))
    with pytest.raises(ValueError, match="equity"):
        calculate_metrics(100, path)
