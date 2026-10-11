"""Deterministic open-fill, close-mark, close-decision simulation."""

from importlib.metadata import version

from distributed_backtester.execution import execute_intent, intent_rejection_reason
from distributed_backtester.metrics import calculate_metrics
from distributed_backtester.models import (
    BacktestConfig,
    BacktestResult,
    CloseHistory,
    CloseObservation,
    EquityPoint,
    Fill,
    MarketDataset,
    OrderOutcome,
    Portfolio,
    TargetIntent,
)
from distributed_backtester.strategy import MovingAverageCrossover, Strategy

ENGINE_ID = (
    f"distributed-backtester/{version('distributed-backtester')}:daily-long-only-v1"
)
SMA_ID = "sma-crossover-v1"


def run_backtest(
    config: BacktestConfig, dataset: MarketDataset, *, strategy: Strategy | None = None
) -> BacktestResult:
    """Run one simulation without file IO, random state, or wall-clock metadata.

    The default SMA instance is fresh on every call. A custom strategy instance
    belongs to the caller, who must provide fresh/reset state for repeatability.
    Custom strategy parameters are not described by the SMA configuration.
    """
    if strategy is None:
        active: Strategy = MovingAverageCrossover(config.strategy)
        strategy_id = SMA_ID
    else:
        active = strategy
        strategy_id = f"{type(strategy).__module__}.{type(strategy).__qualname__}"

    portfolio = Portfolio(config.initial_cash, 0)
    pending: TargetIntent | None = None
    last_sequence = 0
    observations: list[CloseObservation] = []
    fills: list[Fill] = []
    outcomes: list[OrderOutcome] = []
    equity: list[EquityPoint] = []

    for bar in dataset.bars:
        if pending is not None:
            portfolio, outcome = execute_intent(
                pending, bar.session, bar.open, portfolio, config.fee_bps
            )
            outcomes.append(outcome)
            if outcome.fill is not None:
                fills.append(outcome.fill)
            pending = None

        equity.append(
            EquityPoint(
                bar.session,
                portfolio.cash,
                portfolio.holdings,
                bar.close,
                portfolio.cash + portfolio.holdings * bar.close,
            )
        )
        observations.append(CloseObservation(bar.session, bar.close))
        intent = active.on_close(CloseHistory(observations), portfolio)
        if intent is None:
            continue
        reason = intent_rejection_reason(intent)
        if reason is None and intent.decision_session != bar.session:
            reason = "invalid_timing"
        if reason is None and intent.sequence <= last_sequence:
            reason = "invalid_sequence"
        if reason is not None:
            outcomes.append(OrderOutcome(intent, "rejected", None, reason))
        else:
            last_sequence = intent.sequence
            pending = intent

    if pending is not None:
        outcomes.append(OrderOutcome(pending, "canceled", None, "end_of_data"))
    return BacktestResult(
        config=config,
        dataset_metadata=dataset.metadata,
        engine_id=ENGINE_ID,
        fills=tuple(fills),
        order_outcomes=tuple(outcomes),
        equity=tuple(equity),
        final_portfolio=portfolio,
        metrics=calculate_metrics(config.initial_cash, equity, fills),
        strategy_id=strategy_id,
    )
