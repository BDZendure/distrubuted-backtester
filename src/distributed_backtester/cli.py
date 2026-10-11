"""Command-line IO for simulations and local parameter sweeps."""

import argparse
import json
import os
import sys
from collections.abc import Sequence
from importlib.metadata import version
from pathlib import Path

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.backends.process import ProcessBackend
from distributed_backtester.backends.ray import RayBackend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.data import load_csv
from distributed_backtester.experiments import run_experiment
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
    sweep = commands.add_parser("sweep", help="run a grid of independent SMA jobs")
    sweep.add_argument("--data", type=Path, required=True)
    sweep.add_argument("--symbol", required=True)
    sweep.add_argument("--price-basis", choices=["raw"], required=True)
    sweep.add_argument("--fast", type=int, nargs="+", required=True)
    sweep.add_argument("--slow", type=int, nargs="+", required=True)
    sweep.add_argument("--initial-cash", default="1000")
    sweep.add_argument("--fee-bps", type=int, default=0)
    sweep.add_argument(
        "--backend", choices=["sequential", "process", "ray"], default="sequential"
    )
    sweep.add_argument("--workers", type=int, default=1)
    sweep.add_argument("--max-in-flight", type=int)
    sweep.add_argument("--ray-address", help="existing GCS host:port or auto")
    sweep.add_argument("--cpus-per-worker", type=float)
    sweep.add_argument(
        "--worker-resource",
        action="append",
        default=[],
        help="Ray custom resource NAME=QUANTITY per worker",
    )
    sweep.add_argument(
        "--object-store-memory",
        type=int,
        help="local Ray object store capacity in bytes",
    )
    sweep.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="new directory for manifest, per-job results, and index",
    )
    return parser


def select_backend(
    name: str,
    workers: int,
    max_in_flight: int | None = None,
    *,
    ray_address: str | None = None,
    cpus_per_worker: float | None = None,
    worker_resources: Sequence[str] = (),
    object_store_memory: int | None = None,
) -> ExecutionBackend:
    if name == "ray":
        resources: dict[str, float] = {}
        for item in worker_resources:
            key, separator, value = item.partition("=")
            if not separator or key in resources:
                raise ValueError(
                    "worker resources require unique NAME=QUANTITY entries"
                )
            resources[key] = float(value)
        return RayBackend(
            workers,
            max_in_flight=max_in_flight,
            address=ray_address,
            cpus_per_worker=1.0 if cpus_per_worker is None else cpus_per_worker,
            resources=resources,
            object_store_memory=object_store_memory,
        )
    if (
        ray_address is not None
        or cpus_per_worker is not None
        or worker_resources
        or object_store_memory is not None
    ):
        raise ValueError("Ray resource and address options require --backend ray")
    if name == "process":
        return ProcessBackend(workers, max_in_flight=max_in_flight)
    if name != "sequential" or workers != 1 or max_in_flight is not None:
        raise ValueError(
            "sequential execution requires --workers 1 and no --max-in-flight"
        )
    return SequentialBackend()


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
        if options.command == "sweep":
            backend = select_backend(
                options.backend,
                options.workers,
                options.max_in_flight,
                ray_address=options.ray_address,
                cpus_per_worker=options.cpus_per_worker,
                worker_resources=options.worker_resource,
                object_store_memory=options.object_store_memory,
            )
            summary = run_experiment(
                options.data,
                symbol=options.symbol,
                fast=options.fast,
                slow=options.slow,
                initial_cash=parse_money(options.initial_cash),
                fee_bps=options.fee_bps,
                output=options.output_dir,
                backend=backend,
            )
            print(
                json.dumps(
                    {
                        "experiment_id": summary.experiment_id,
                        "submitted": summary.submitted,
                        "succeeded": summary.succeeded,
                        "failed": summary.failed,
                        "index": str(summary.index_path),
                        "execution": str(summary.execution_path),
                    },
                    sort_keys=True,
                )
            )
            return 1 if summary.failed else 0
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
        guidance = (
            "choose a new output directory"
            if options.command == "sweep"
            else "use --overwrite to replace it"
        )
        print(
            f"error: output exists; {guidance}: {error}",
            file=sys.stderr,
        )
        return 2
    except (ValueError, OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0
