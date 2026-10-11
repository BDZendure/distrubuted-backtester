"""Canonical semantic JSON and publication of complete local result files."""

import json
import logging
import os
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile

from distributed_backtester.models import BacktestResult, Metrics
from distributed_backtester.money import format_money, format_ratio
from distributed_backtester.simulation import SMA_ID

_LOG = logging.getLogger(__name__)


def metrics_record(metrics: Metrics) -> dict[str, object]:
    """Compact metrics for a result or experiment index, with identical units."""
    return {
        "total_return": format_ratio(
            metrics.total_return.numerator, metrics.total_return.denominator
        ),
        "maximum_drawdown": format_ratio(
            metrics.maximum_drawdown.numerator, metrics.maximum_drawdown.denominator
        ),
        "final_equity": format_money(metrics.final_equity),
        "fill_count": metrics.fill_count,
        "total_fees": format_money(metrics.total_fees),
    }


def result_to_dict(result: BacktestResult) -> dict[str, object]:
    """Money is six-place USD text; ratios are twelve-place decimal text."""
    if result.strategy_id != SMA_ID:
        raise ValueError(
            "custom strategy results need captured parameters before export"
        )
    config = result.config
    metadata = result.dataset_metadata
    parameters = {
        "fast_window": config.strategy.fast_window,
        "slow_window": config.strategy.slow_window,
    }
    return {
        "schema_version": result.schema_version,
        "engine_id": result.engine_id,
        "config": {
            "strategy": {"id": result.strategy_id, "parameters": parameters},
            "initial_cash": format_money(config.initial_cash),
            "fee_bps": config.fee_bps,
            "fee_rounding": "half-even-micro-USD",
            "sizing": "maximum-affordable-whole-shares-on-target-change",
            "timing": "close-decision-next-supplied-open-fill-close-mark",
        },
        "dataset": {
            "symbol": metadata.symbol,
            "source_sha256": metadata.source_sha256,
            "currency": metadata.currency,
            "price_basis": metadata.price_basis,
            "validation_version": metadata.validation_version,
        },
        "fills": [
            {
                "sequence": fill.sequence,
                "decision_session": fill.decision_session.isoformat(),
                "execution_session": fill.execution_session.isoformat(),
                "side": fill.side,
                "quantity": fill.quantity,
                "price": format_money(fill.price),
                "fee": format_money(fill.fee),
                "cash_after": format_money(fill.cash_after),
                "holdings_after": fill.holdings_after,
            }
            for fill in result.fills
        ],
        "order_outcomes": [
            {
                "sequence": (
                    outcome.intent.sequence
                    if type(outcome.intent.sequence) is int
                    else None
                ),
                "target": (
                    outcome.intent.target
                    if isinstance(outcome.intent.target, str)
                    else None
                ),
                "decision_session": (
                    outcome.intent.decision_session.isoformat()
                    if type(outcome.intent.decision_session) is date
                    else None
                ),
                "execution_session": (
                    outcome.execution_session.isoformat()
                    if outcome.execution_session is not None
                    else None
                ),
                "status": outcome.status,
                "reason": outcome.reason,
                "fill_sequence": outcome.fill.sequence if outcome.fill else None,
            }
            for outcome in result.order_outcomes
        ],
        "equity": [
            {
                "session": point.session.isoformat(),
                "cash": format_money(point.cash),
                "holdings": point.holdings,
                "close": format_money(point.close),
                "equity": format_money(point.equity),
            }
            for point in result.equity
        ],
        "final_portfolio": {
            "cash": format_money(result.final_portfolio.cash),
            "holdings": result.final_portfolio.holdings,
        },
        "metrics": metrics_record(result.metrics),
    }


def serialize_result(result: BacktestResult) -> str:
    return (
        json.dumps(result_to_dict(result), sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    )


def write_result(
    result: BacktestResult, path: Path, *, overwrite: bool = False
) -> None:
    write_text(serialize_result(result), path, overwrite=overwrite)


def write_text(text: str, path: Path, *, overwrite: bool = False) -> None:
    """Publish a complete sibling file, with no overwrite unless requested.

    Hard-link publication refuses an existing destination atomically. Replacement
    uses os.replace. This is local file publication, not durable job persistence;
    directory fsync, crash recovery, and distributed idempotency are later work.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                # Publication is the commit point. Cleanup cannot reverse it or
                # mask an earlier error from writing/fsync/link/replacement.
                _LOG.warning(
                    "Temporary result cleanup failed for %s: %s", temporary, error
                )
