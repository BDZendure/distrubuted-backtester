"""Exact monetary boundaries and rounding, independent of Decimal context."""

from decimal import localcontext

import pytest

from distributed_backtester.money import (
    format_money,
    format_ratio,
    parse_money,
    round_half_even,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("0", 0),
        ("12.0000000", 12_000_000),
        ("0.000001", 1),
        ("-3.004", -3_004_000),
        ("+1.5", 1_500_000),
        (".5", 500_000),
        ("1.", 1_000_000),
        (" 2.00 ", 2_000_000),
    ],
)
def test_parse_exact_decimal_money(text: str, expected: int) -> None:
    assert parse_money(text) == expected


@pytest.mark.parametrize(
    "text",
    ["", " ", "NaN", "Infinity", "1e2", "1_000", "1,000", "0.0000001", "1.2345678"],
)
def test_reject_invalid_or_unrepresentable_money(text: str) -> None:
    with pytest.raises(ValueError):
        parse_money(text)


def test_money_parsing_does_not_depend_on_decimal_precision() -> None:
    with localcontext() as context:
        context.prec = 2
        assert parse_money("1234567890.123456") == 1_234_567_890_123_456


@pytest.mark.parametrize(
    ("numerator", "denominator", "expected"),
    [(24, 10, 2), (25, 10, 2), (35, 10, 4), (36, 10, 4), (-25, 10, -2), (-35, 10, -4)],
)
def test_half_even_integer_rounding(
    numerator: int, denominator: int, expected: int
) -> None:
    assert round_half_even(numerator, denominator) == expected


@pytest.mark.parametrize("denominator", [0, -1])
def test_rounding_rejects_invalid_denominator(denominator: int) -> None:
    with pytest.raises(ValueError, match="denominator"):
        round_half_even(1, denominator)


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0, "0.000000"), (1, "0.000001"), (3_004_000, "3.004000"), (-1, "-0.000001")],
)
def test_money_format_is_exact(value: int, expected: str) -> None:
    assert format_money(value) == expected


def test_ratio_format_uses_twelve_places_and_exact_half_even() -> None:
    assert format_ratio(1, 3) == "0.333333333333"
    assert format_ratio(-250743, 1000000) == "-0.250743000000"
    assert format_ratio(5, 10**13) == "0.000000000000"
    assert format_ratio(15, 10**13) == "0.000000000002"
    assert format_ratio(-1, 10**13) == "0.000000000000"
