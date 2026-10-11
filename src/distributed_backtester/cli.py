"""Command-line IO around the single-process simulation core."""

import argparse
import os
import sys
from collections.abc import Sequence
from importlib.metadata import version
from pathlib import Path

from distributed_backtester.data import load_csv
from distributed_backtester.models import BacktestConfig, StrategyConfig
from distributed_backtester.money import parse_money
from distributed_backtester.results import serialize_result, write_result
from distributed_backtester.simulation import run_backtest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="distributed-backtester")
    parser.add_argument(
        "--version", action="version", version=version("distributed-backtester")
    )
    commands = parser.add_subparsers(dest="command", required=True)
    backtest = commands.add_parser(
        "backtest", help="run one daily long-only SMA simulation"
    )
    backtest.add_argument("--data", type=Path, required=True, help="OHLCV CSV path")
    backtest.add_argument("--symbol", required=True)
    backtest.add_argument("--price-basis", choices=["raw"], required=True)
    backtest.add_argument("--fast", type=int, default=2)
    backtest.add_argument("--slow", type=int, default=3)
    backtest.add_argument(
        "--initial-cash", default="1000", help="USD decimal, up to six places"
    )
    backtest.add_argument(
        "--fee-bps", type=int, default=0, help="integer basis points, 0..10000"
    )
    backtest.add_argument(
        "--output", type=Path, help="JSON destination; otherwise stdout"
    )
    backtest.add_argument(
        "--overwrite", action="store_true", help="replace an existing output"
    )
    return parser


def _check_output(source: Path, output: Path | None) -> None:
    if output is None:
        return
    if source.resolve() == output.resolve() or (
        source.exists() and output.exists() and os.path.samefile(source, output)
    ):
        raise ValueError("output must not refer to the input CSV")


def main(argv: Sequence[str] | None = None) -> int:
    options = _parser().parse_args(argv)
    try:
        config = BacktestConfig(
            strategy=StrategyConfig(options.fast, options.slow),
            initial_cash=parse_money(options.initial_cash),
            fee_bps=options.fee_bps,
        )
        _check_output(options.data, options.output)
        dataset = load_csv(
            options.data, symbol=options.symbol, price_basis=options.price_basis
        )
        result = run_backtest(config, dataset)
        if options.output is None:
            sys.stdout.write(serialize_result(result))
        else:
            write_result(result, options.output, overwrite=options.overwrite)
    except FileExistsError as error:
        print(
            f"error: output exists; use --overwrite to replace it: {error}",
            file=sys.stderr,
        )
        return 2
    except (ValueError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0
