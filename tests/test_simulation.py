from datetime import date, timedelta
from fractions import Fraction

import pytest

from distributed_backtester.models import (
    BacktestConfig,
    Bar,
    CloseHistory,
    DatasetMetadata,
    MarketDataset,
    Portfolio,
    StrategyConfig,
    TargetIntent,
)
from distributed_backtester.money import parse_money
from distributed_backtester.simulation import run_backtest
from distributed_backtester.strategy import MovingAverageCrossover


def dataset(closes: list[int], opens: list[int] | None = None) -> MarketDataset:
    opens = opens if opens is not None else closes
    return MarketDataset(
        tuple(
            Bar(
                date(2026, 1, 1) + timedelta(days=i),
                opening * 1_000_000,
                max(opening, close) * 1_000_000,
                min(opening, close) * 1_000_000,
                close * 1_000_000,
                100,
            )
            for i, (opening, close) in enumerate(zip(opens, closes, strict=True))
        ),
        DatasetMetadata("SYNTH", "a" * 64),
    )


class ScriptedStrategy:
    """Explicit signals isolate the real engine's timing and accounting."""

    def __init__(self, targets: dict[int, str]) -> None:
        self.targets = targets
        self.histories: list[CloseHistory] = []
        self.sequence = 0

    def on_close(
        self, history: CloseHistory, portfolio: Portfolio
    ) -> TargetIntent | None:
        self.histories.append(history)
        if len(history) not in self.targets:
            return None
        self.sequence += 1
        return TargetIntent(
            self.targets[len(history)], history[-1].session, self.sequence
        )


def test_hand_calculated_sma_entry_exit_and_full_equity_path() -> None:
    result = run_backtest(
        BacktestConfig(fee_bps=10), dataset([10, 11, 12, 9, 9], [10, 11, 12, 12, 9])
    )
    buy, sell = result.fills
    assert (buy.decision_session, buy.execution_session) == (
        date(2026, 1, 3),
        date(2026, 1, 4),
    )
    assert (buy.quantity, buy.price, buy.fee, buy.cash_after) == (
        83,
        parse_money("12"),
        parse_money("0.996"),
        parse_money("3.004"),
    )
    assert (sell.quantity, sell.fee, sell.cash_after) == (
        83,
        parse_money("0.747"),
        parse_money("749.257"),
    )
    assert [point.equity for point in result.equity] == [
        parse_money(value) for value in ["1000", "1000", "1000", "750.004", "749.257"]
    ]
    assert result.final_portfolio == Portfolio(parse_money("749.257"), 0)
    assert result.metrics.total_return == Fraction(-250743, 1000000)
    assert result.metrics.maximum_drawdown == Fraction(250743, 1000000)
    assert result.metrics.total_fees == parse_money("1.743")
    assert result.metrics.fill_count == 2
    for point in result.equity:
        assert point.equity == point.cash + point.holdings * point.close


def test_next_open_gap_cannot_execute_at_signal_close() -> None:
    strategy = ScriptedStrategy({1: "invested"})
    result = run_backtest(
        BacktestConfig(initial_cash=parse_money("100")),
        dataset([1, 20], [1, 25]),
        strategy=strategy,
    )
    assert result.fills[0].price == parse_money("25")
    assert result.fills[0].quantity == 4
    assert result.equity[0].holdings == 0
    assert result.equity[1].equity == parse_money("80")


def test_future_suffix_cannot_change_earlier_decisions_or_fills() -> None:
    original = run_backtest(BacktestConfig(), dataset([10, 11, 12, 13, 14, 15]))
    changed = run_backtest(BacktestConfig(), dataset([10, 11, 12, 13, 1, 200]))
    assert original.equity[:4] == changed.equity[:4]
    assert original.fills[0] == changed.fills[0]
    assert original.order_outcomes[0] == changed.order_outcomes[0]


def test_changing_next_open_preserves_signal_but_changes_fill_and_quantity() -> None:
    ordinary = run_backtest(BacktestConfig(), dataset([10, 11, 12, 13]))
    gap = run_backtest(BacktestConfig(), dataset([10, 11, 12, 13], [10, 11, 12, 25]))
    assert ordinary.equity[:3] == gap.equity[:3]
    assert ordinary.order_outcomes[0].intent == gap.order_outcomes[0].intent
    assert ordinary.fills[0].quantity == 76
    assert gap.fills[0].quantity == 40
    assert gap.fills[0].price == parse_money("25")


def test_truncation_preserves_completed_fills_and_equity() -> None:
    full = run_backtest(BacktestConfig(), dataset([10, 11, 12, 13, 9, 8]))
    prefix = run_backtest(BacktestConfig(), dataset([10, 11, 12, 13]))
    assert prefix.equity == full.equity[:4]
    assert prefix.fills == full.fills[:1]


def test_strategy_receives_only_completed_closes_with_fixed_bounds() -> None:
    strategy = ScriptedStrategy({})
    run_backtest(BacktestConfig(), dataset([10, 11, 12]), strategy=strategy)
    assert [len(history) for history in strategy.histories] == [1, 2, 3]
    assert [history[-1].close for history in strategy.histories] == [
        parse_money("10"),
        parse_money("11"),
        parse_money("12"),
    ]
    with pytest.raises(IndexError):
        _ = strategy.histories[0][1]
    assert not hasattr(strategy.histories[0][0], "open")


def test_final_intent_is_canceled_without_forced_liquidation() -> None:
    result = run_backtest(
        BacktestConfig(),
        dataset([10, 11]),
        strategy=ScriptedStrategy({1: "invested", 2: "flat"}),
    )
    assert len(result.fills) == 1
    assert result.final_portfolio.holdings == 90
    terminal = result.order_outcomes[-1]
    assert (terminal.status, terminal.reason, terminal.execution_session) == (
        "canceled",
        "end_of_data",
        None,
    )


@pytest.mark.parametrize("closes", [[10], [10, 10], [10, 10, 10, 10]])
def test_no_signal_retains_cash(closes: list[int]) -> None:
    result = run_backtest(BacktestConfig(), dataset(closes))
    assert not result.fills
    assert not result.order_outcomes
    assert result.metrics.total_return == 0
    assert result.final_portfolio == Portfolio(parse_money("1000"), 0)


def test_signal_on_only_ready_last_bar_is_canceled() -> None:
    result = run_backtest(BacktestConfig(), dataset([10, 11, 12]))
    assert not result.fills
    assert result.order_outcomes[0].status == "canceled"


def test_insufficient_cash_is_reported_without_auto_retry() -> None:
    result = run_backtest(
        BacktestConfig(initial_cash=parse_money("1")), dataset([10, 11, 12, 13, 14])
    )
    assert not result.fills
    assert len(result.order_outcomes) == 1
    assert result.order_outcomes[0].reason == "insufficient_cash"


def test_repeated_invocations_create_fresh_strategy_state() -> None:
    config = BacktestConfig(strategy=StrategyConfig(1, 2), fee_bps=10)
    bars = dataset([10, 11, 9, 12, 8])
    assert run_backtest(config, bars) == run_backtest(config, bars)


def test_supplied_sma_config_must_match_reported_parameters() -> None:
    with pytest.raises(ValueError, match="strategy configuration"):
        run_backtest(
            BacktestConfig(),
            dataset([10]),
            strategy=MovingAverageCrossover(StrategyConfig(5, 20)),
        )


def test_supplied_matching_sma_retains_parameter_provenance() -> None:
    config = BacktestConfig(strategy=StrategyConfig(1, 2))
    result = run_backtest(
        config, dataset([10]), strategy=MovingAverageCrossover(config.strategy)
    )
    assert result.strategy_id == "sma-crossover-v1"


@pytest.mark.parametrize("target", ["short", "unknown"])
def test_invalid_strategy_intent_cannot_mutate_portfolio(target: str) -> None:
    result = run_backtest(
        BacktestConfig(), dataset([10, 11]), strategy=ScriptedStrategy({1: target})
    )
    assert not result.fills
    assert result.order_outcomes[0].reason == "invalid_target"
    assert result.final_portfolio == Portfolio(parse_money("1000"), 0)


class InvalidTimingStrategy:
    def on_close(self, history: CloseHistory, portfolio: Portfolio) -> TargetIntent:
        return TargetIntent("invested", date(2030, 1, 1), 1)


def test_future_dated_strategy_intent_is_rejected_immediately() -> None:
    result = run_backtest(
        BacktestConfig(), dataset([10]), strategy=InvalidTimingStrategy()
    )
    assert result.order_outcomes[0].status == "rejected"
    assert result.order_outcomes[0].reason == "invalid_timing"


class RepeatedSequenceStrategy:
    def on_close(self, history: CloseHistory, portfolio: Portfolio) -> TargetIntent:
        return TargetIntent("invested", history[-1].session, 1)


def test_duplicate_sequence_is_rejected() -> None:
    result = run_backtest(
        BacktestConfig(), dataset([10, 11]), strategy=RepeatedSequenceStrategy()
    )
    assert [outcome.status for outcome in result.order_outcomes] == [
        "filled",
        "rejected",
    ]
    assert result.order_outcomes[-1].reason == "invalid_sequence"


class BrokenStrategy:
    def on_close(self, history: CloseHistory, portfolio: Portfolio) -> TargetIntent:
        raise RuntimeError("strategy failed")


def test_strategy_error_is_not_a_success_result() -> None:
    with pytest.raises(RuntimeError, match="strategy failed"):
        run_backtest(BacktestConfig(), dataset([10]), strategy=BrokenStrategy())
