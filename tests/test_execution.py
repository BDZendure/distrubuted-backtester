"""Execution expectations are calculated independently of the production ledger."""

from datetime import date
from fractions import Fraction

import pytest

from distributed_backtester.execution import (
    execute_intent,
    fee_for_notional,
    max_affordable_quantity,
)
from distributed_backtester.models import Portfolio, TargetIntent

DECISION = date(2026, 1, 2)
EXECUTION = date(2026, 1, 5)


@pytest.mark.parametrize(
    ("notional", "bps", "expected"),
    [
        (996_000_000, 10, 996_000),
        (747_000_000, 10, 747_000),
        (1, 5000, 0),
        (3, 5000, 2),
        (5, 5000, 2),
        (10, 0, 0),
        (10, 10_000, 10),
    ],
)
def test_fee_is_proportional_and_half_even(
    notional: int, bps: int, expected: int
) -> None:
    assert fee_for_notional(notional, bps) == expected


@pytest.mark.parametrize("bps", [-1, 10_001, True])
def test_fee_rejects_invalid_configuration(bps: int) -> None:
    with pytest.raises(ValueError, match="fee_bps"):
        fee_for_notional(100, bps)


def test_affordable_quantity_matches_independent_exhaustive_small_cases() -> None:
    for cash in range(61):
        for price in range(1, 9):
            for bps in (0, 1, 10, 3333, 5000, 10_000):
                expected = max(
                    quantity
                    for quantity in range(cash // price + 1)
                    if quantity * price
                    + round(Fraction(quantity * price * bps, 10_000))
                    <= cash
                )
                assert max_affordable_quantity(cash, price, bps) == expected


def test_hand_calculated_gap_buy_and_sell() -> None:
    initial = Portfolio(cash=1_000_000_000, holdings=0)
    portfolio, buy = execute_intent(
        TargetIntent("invested", DECISION, 1), EXECUTION, 12_000_000, initial, 10
    )
    assert initial == Portfolio(1_000_000_000, 0)
    assert buy.status == "filled"
    assert buy.fill is not None
    assert buy.fill.quantity == 83
    assert buy.fill.fee == 996_000
    assert buy.fill.price == 12_000_000
    assert buy.fill.decision_session == DECISION
    assert buy.fill.execution_session == EXECUTION
    assert portfolio == Portfolio(cash=3_004_000, holdings=83)

    final, sell = execute_intent(
        TargetIntent("flat", EXECUTION, 2), date(2026, 1, 6), 9_000_000, portfolio, 10
    )
    assert sell.fill is not None
    assert sell.fill.side == "sell"
    assert sell.fill.quantity == 83
    assert sell.fill.fee == 747_000
    assert final == Portfolio(cash=749_257_000, holdings=0)


def test_fee_is_included_before_quantity_is_chosen() -> None:
    portfolio, outcome = execute_intent(
        TargetIntent("invested", DECISION, 1),
        EXECUTION,
        10_000_000,
        Portfolio(100_000_000, 0),
        10,
    )
    assert outcome.fill is not None
    assert outcome.fill.quantity == 9
    assert outcome.fill.fee == 90_000
    assert portfolio == Portfolio(9_910_000, 9)


def test_zero_fee_allows_exact_cash_purchase() -> None:
    portfolio, outcome = execute_intent(
        TargetIntent("invested", DECISION, 1), EXECUTION, 10, Portfolio(100, 0), 0
    )
    assert outcome.fill is not None
    assert outcome.fill.quantity == 10
    assert portfolio == Portfolio(0, 10)


def test_insufficient_cash_rejects_without_mutation_or_fee() -> None:
    initial = Portfolio(9_999_999, 0)
    portfolio, outcome = execute_intent(
        TargetIntent("invested", DECISION, 1), EXECUTION, 10_000_000, initial, 10
    )
    assert portfolio is initial
    assert outcome.status == "rejected"
    assert outcome.reason == "insufficient_cash"
    assert outcome.fill is None


@pytest.mark.parametrize(
    ("intent", "reason"),
    [
        (TargetIntent("short", DECISION, 1), "invalid_target"),
        (TargetIntent("invested", DECISION, 0), "invalid_sequence"),
        (TargetIntent("invested", DECISION, True), "invalid_sequence"),
        (TargetIntent("flat", EXECUTION, 1), "invalid_timing"),
        (TargetIntent("flat", date(2026, 1, 6), 1), "invalid_timing"),
    ],
)
def test_invalid_intents_are_explicit_rejections(
    intent: TargetIntent, reason: str
) -> None:
    initial = Portfolio(100, 1)
    portfolio, outcome = execute_intent(intent, EXECUTION, 10, initial, 0)
    assert portfolio is initial
    assert outcome.status == "rejected"
    assert outcome.reason == reason
    assert outcome.fill is None


@pytest.mark.parametrize(
    ("target", "initial", "reason"),
    [
        ("flat", Portfolio(100, 0), "already_flat"),
        ("invested", Portfolio(100, 2), "already_invested"),
    ],
)
def test_unchanged_target_does_not_rebalance(
    target: str, initial: Portfolio, reason: str
) -> None:
    portfolio, outcome = execute_intent(
        TargetIntent(target, DECISION, 1), EXECUTION, 1, initial, 10
    )
    assert portfolio is initial
    assert outcome.status == "no_change"
    assert outcome.reason == reason
    assert outcome.fill is None


@pytest.mark.parametrize("price", [0, -1, True])
def test_execution_rejects_invalid_price(price: int) -> None:
    with pytest.raises(ValueError, match="open_price"):
        execute_intent(
            TargetIntent("flat", DECISION, 1), EXECUTION, price, Portfolio(100, 0), 0
        )
