"""Exact metrics from recorded equity, anchored to pre-simulation capital."""

from collections.abc import Sequence
from fractions import Fraction

from distributed_backtester.models import EquityPoint, Fill, Metrics
from distributed_backtester.money import require_integer


def calculate_metrics(
    initial_cash: int,
    equity: Sequence[EquityPoint],
    fills: Sequence[Fill] = (),
) -> Metrics:
    require_integer(initial_cash, "initial_cash", minimum=1)
    if not equity:
        raise ValueError("equity path cannot be empty")
    peak = initial_cash
    drawdown_numerator, drawdown_denominator = 0, 1
    for point in equity:
        require_integer(point.equity, "equity")
        peak = max(peak, point.equity)
        loss = peak - point.equity
        # Positive peaks permit exact comparison without reducing each fraction.
        if loss * drawdown_denominator > drawdown_numerator * peak:
            drawdown_numerator, drawdown_denominator = loss, peak
    return Metrics(
        total_return=Fraction(equity[-1].equity - initial_cash, initial_cash),
        maximum_drawdown=Fraction(drawdown_numerator, drawdown_denominator),
        final_equity=equity[-1].equity,
        fill_count=len(fills),
        total_fees=sum(fill.fee for fill in fills),
    )
