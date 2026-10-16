"""Next-open target sizing and exact, cash-constrained portfolio transitions."""

from datetime import date
from typing import Literal

from distributed_backtester.models import Fill, OrderOutcome, Portfolio, TargetIntent
from distributed_backtester.money import require_integer, round_half_even


def _validate_fee(fee_bps: int) -> None:
    require_integer(fee_bps, "fee_bps")
    if fee_bps > 10_000:
        raise ValueError("fee_bps must be <= 10000")


def fee_for_notional(notional: int, fee_bps: int) -> int:
    """Round the proportional fee once per fill to a micro-dollar."""
    require_integer(notional, "notional")
    _validate_fee(fee_bps)
    return round_half_even(notional * fee_bps, 10_000)


def max_affordable_quantity(cash: int, open_price: int, fee_bps: int) -> int:
    """Greatest affordable whole-share quantity under the final rounded fee.

    Nonnegative fees make total cost monotone in quantity. Binary search handles
    rounding boundaries directly, without a float approximation or decrement loop.
    """
    require_integer(cash, "cash")
    require_integer(open_price, "open_price", minimum=1)
    _validate_fee(fee_bps)
    low, high = 0, cash // open_price
    while low < high:
        quantity = (low + high + 1) // 2
        notional = quantity * open_price
        if notional + fee_for_notional(notional, fee_bps) <= cash:
            low = quantity
        else:
            high = quantity - 1
    return low


def intent_rejection_reason(intent: TargetIntent) -> str | None:
    """Validate untrusted order semantics without altering a portfolio."""
    if intent.target not in ("flat", "invested"):
        return "invalid_target"
    if type(intent.sequence) is not int or intent.sequence <= 0:
        return "invalid_sequence"
    if type(intent.decision_session) is not date:
        return "invalid_timing"
    return None


def execute_intent(
    intent: TargetIntent,
    execution_session: date,
    open_price: int,
    portfolio: Portfolio,
    fee_bps: int,
) -> tuple[Portfolio, OrderOutcome]:
    """Fill a declared target at a strictly later session's open or reject it."""
    require_integer(open_price, "open_price", minimum=1)
    _validate_fee(fee_bps)
    if type(execution_session) is not date:
        raise ValueError("execution_session must be a date")
    reason = intent_rejection_reason(intent)
    if reason is None and intent.decision_session >= execution_session:
        reason = "invalid_timing"
    if reason is not None:
        return portfolio, OrderOutcome(intent, "rejected", execution_session, reason)

    side: Literal["buy", "sell"]
    if intent.target == "invested":
        if portfolio.holdings:
            return portfolio, OrderOutcome(
                intent, "no_change", execution_session, "already_invested"
            )
        quantity = max_affordable_quantity(portfolio.cash, open_price, fee_bps)
        if quantity == 0:
            return portfolio, OrderOutcome(
                intent, "rejected", execution_session, "insufficient_cash"
            )
        fee = fee_for_notional(quantity * open_price, fee_bps)
        updated = Portfolio(portfolio.cash - quantity * open_price - fee, quantity)
        side = "buy"
    else:
        if portfolio.holdings == 0:
            return portfolio, OrderOutcome(
                intent, "no_change", execution_session, "already_flat"
            )
        quantity = portfolio.holdings
        fee = fee_for_notional(quantity * open_price, fee_bps)
        updated = Portfolio(portfolio.cash + quantity * open_price - fee, 0)
        side = "sell"

    fill = Fill(
        sequence=intent.sequence,
        decision_session=intent.decision_session,
        execution_session=execution_session,
        side=side,
        quantity=quantity,
        price=open_price,
        fee=fee,
        cash_after=updated.cash,
        holdings_after=updated.holdings,
    )
    return updated, OrderOutcome(intent, "filled", execution_session, fill=fill)
