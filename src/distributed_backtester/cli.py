"""Command-line IO for simulations and local parameter sweeps."""

import argparse
import json
import os
import sqlite3
import sys
from collections.abc import Sequence
from importlib.metadata import version
from pathlib import Path

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.backends.process import ProcessBackend
from distributed_backtester.backends.ray import RayBackend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.cli_lifecycle import (
    CommandInterrupted,
    SignalState,
    cli_signals,
)
from distributed_backtester.cli_options import (
    ExplicitAppend,
    configuration_defaults,
    protect_configuration,
    requested_log_level,
)
from distributed_backtester.data import load_csv
from distributed_backtester.experiments import run_experiment
from distributed_backtester.jobs import metadata_record
from distributed_backtester.market_data import load_dataset
from distributed_backtester.models import BacktestConfig, StrategyConfig
from distributed_backtester.money import parse_money
from distributed_backtester.operational_logging import LEVELS, emit_event, log_session
from distributed_backtester.operational_status import read_status
from distributed_backtester.results import serialize_result, write_result
from distributed_backtester.scheduler.experiments import (
    resume_experiment,
    start_experiment,
)
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.simulation import run_backtest


def _parser(
    configured_command: str | None = None,
    defaults: dict[str, object] | None = None,
) -> argparse.ArgumentParser:
    values = {} if defaults is None else defaults

    def required(command: str, name: str) -> bool:
        return command != configured_command or name not in values

    parser = argparse.ArgumentParser(prog="distributed-backtester")
    parser.add_argument(
        "--version", action="version", version=version("distributed-backtester")
    )
    commands = parser.add_subparsers(dest="command", required=True)
    backtest = commands.add_parser(
        "backtest", help="run one daily long-only SMA simulation"
    )
    backtest.add_argument(
        "--data",
        type=Path,
        required=required("backtest", "data"),
        help="OHLCV CSV or project Parquet path",
    )
    _data_options(backtest)
    backtest.add_argument("--symbol", required=required("backtest", "symbol"))
    backtest.add_argument(
        "--price-basis", choices=["raw"], required=required("backtest", "price_basis")
    )
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
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="replace an existing output",
    )
    sweep = commands.add_parser("sweep", help="run a grid of independent SMA jobs")
    sweep.add_argument("--data", type=Path, required=required("sweep", "data"))
    _data_options(sweep)
    sweep.add_argument("--symbol", required=required("sweep", "symbol"))
    sweep.add_argument(
        "--price-basis", choices=["raw"], required=required("sweep", "price_basis")
    )
    sweep.add_argument(
        "--fast", type=int, nargs="+", required=required("sweep", "fast")
    )
    sweep.add_argument(
        "--slow", type=int, nargs="+", required=required("sweep", "slow")
    )
    sweep.add_argument("--initial-cash", default="1000")
    sweep.add_argument("--fee-bps", type=int, default=0)
    sweep.add_argument(
        "--backend", choices=["sequential", "process", "ray"], default="sequential"
    )
    _ray_options(sweep)
    sweep.add_argument(
        "--durable",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="journal and retry Ray jobs",
    )
    sweep.add_argument(
        "--max-attempts", type=int, help="durable total attempts, default 3"
    )
    sweep.add_argument(
        "--job-timeout", type=float, help="durable attempt seconds, default 60"
    )
    sweep.add_argument(
        "--retry-delay", type=float, help="durable retry seconds, default 0.1"
    )
    sweep.add_argument(
        "--output-dir",
        type=Path,
        required=required("sweep", "output_dir"),
        help="new directory for manifest, per-job results, and index",
    )
    resume = commands.add_parser("resume", help="recover a durable Ray experiment")
    resume.add_argument("--data", type=Path, required=required("resume", "data"))
    _data_options(resume)
    resume.add_argument(
        "--output-dir", type=Path, required=required("resume", "output_dir")
    )
    _ray_options(resume)
    status = commands.add_parser("status", help="read durable job and attempt state")
    status.add_argument("--output-dir", type=Path, required=True)
    status.add_argument("--job-id", help="select one job while retaining total counts")
    convert = commands.add_parser(
        "convert-data", help="convert validated raw CSV to exact versioned Parquet"
    )
    convert.add_argument("--data", type=Path, required=True)
    convert.add_argument("--symbol", required=True)
    convert.add_argument("--price-basis", choices=["raw"], required=True)
    convert.add_argument("--output", type=Path, required=True)
    convert.add_argument("--row-group-size", type=int, default=65536)
    convert.add_argument("--overwrite", action="store_true")
    importing = commands.add_parser(
        "results-import", help="import complete committed outcomes into DuckDB"
    )
    importing.add_argument("--experiment-dir", type=Path, required=True)
    importing.add_argument("--database", type=Path, required=True)
    querying = commands.add_parser(
        "results-query", help="query stored experiment outcome snapshots"
    )
    querying.add_argument("--database", type=Path, required=True)
    querying.add_argument("--snapshot-id")
    querying.add_argument("--limit", type=int, default=20)
    configured = {"backtest": backtest, "sweep": sweep, "resume": resume}
    for command in configured.values():
        command.add_argument(
            "--config", type=Path, help="TOML file of command defaults"
        )
    for command in [backtest, sweep, resume, status, convert, importing, querying]:
        command.add_argument("--log-level", choices=LEVELS, default="OFF")
    if configured_command is not None:
        configured[configured_command].set_defaults(**values)
    return parser


def _data_options(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--data-format", choices=["auto", "csv", "parquet"], default="auto"
    )
    command.add_argument(
        "--dataset-version", help="expected logical dataset SHA-256 version"
    )


def _ray_options(command: argparse.ArgumentParser) -> None:
    command.add_argument("--workers", type=int, default=1)
    command.add_argument("--max-in-flight", type=int)
    command.add_argument("--ray-address", help="existing GCS host:port or auto")
    command.add_argument("--cpus-per-worker", type=float)
    command.add_argument(
        "--worker-resource",
        action=ExplicitAppend,
        default=[],
        help="Ray custom resource NAME=QUANTITY per worker",
    )
    command.add_argument(
        "--object-store-memory",
        type=int,
        help="local Ray object store capacity in bytes",
    )


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
        raise ValueError("output must not refer to the input dataset")


def main(argv: Sequence[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else list(argv)
    try:
        command, defaults = configuration_defaults(arguments)
    except (ValueError, OSError) as error:
        level = requested_log_level(arguments)
        with log_session(level):
            emit_event(
                "command_failed",
                level="ERROR",
                error_type=type(error).__name__,
                detail=str(error),
                exit_code=2,
            )
        if level == "OFF":
            print(f"error: {error}", file=sys.stderr)
        return 2
    options = _parser(command, defaults).parse_args(arguments)
    with log_session(options.log_level):
        emit_event("command_started", command=options.command)
        signal_state = SignalState()
        try:
            with cli_signals() as signal_state:
                status = _execute_options(options)
        except CommandInterrupted as error:
            emit_event(
                "command_interrupted",
                level="WARNING",
                command=options.command,
                signal=error.signum,
                exit_code=error.exit_code,
                detail=str(error),
            )
            if options.log_level == "OFF":
                print(f"error: {error}", file=sys.stderr)
            return error.exit_code
        except KeyboardInterrupt as error:
            exit_code = 128 + (signal_state.signum or 2)
            detail = "; ".join(
                [str(error) or "interrupted", *getattr(error, "__notes__", ())]
            )
            emit_event(
                "command_interrupted",
                level="WARNING",
                exit_code=exit_code,
                signal=signal_state.signum,
                detail=detail,
            )
            if options.log_level == "OFF":
                print(f"error: {detail}", file=sys.stderr)
            return exit_code
        emit_event("command_finished", command=options.command, exit_code=status)
        return status


def _execute_options(options: argparse.Namespace) -> int:
    try:
        protect_configuration(options)
        if options.command == "convert-data":
            from distributed_backtester.parquet_data import write_parquet

            _check_output(options.data, options.output)
            dataset = load_csv(
                options.data, symbol=options.symbol, price_basis=options.price_basis
            )
            metadata = write_parquet(
                dataset,
                options.output,
                row_group_size=options.row_group_size,
                overwrite=options.overwrite,
            )
            print(json.dumps(metadata_record(metadata), sort_keys=True))
            return 0
        if options.command in {"results-import", "results-query"}:
            from distributed_backtester.analytics import (
                import_experiment,
                query_results,
            )

            if options.command == "results-import":
                report = import_experiment(options.experiment_dir, options.database)
            else:
                report = {
                    "outcomes": query_results(
                        options.database,
                        snapshot_id=options.snapshot_id,
                        limit=options.limit,
                    )
                }
            print(json.dumps(report, sort_keys=True))
            return 0
        if options.command == "status":
            report = (
                read_status(options.output_dir)
                if options.job_id is None
                else read_status(options.output_dir, job_id=options.job_id)
            )
            print(json.dumps(report, sort_keys=True))
            return 0
        if options.command in {"sweep", "resume"}:
            policy = None
            if options.command == "sweep":
                reliability = (
                    options.max_attempts,
                    options.job_timeout,
                    options.retry_delay,
                )
                if not options.durable and any(x is not None for x in reliability):
                    raise ValueError("retry and deadline options require --durable")
                if options.durable:
                    if options.backend != "ray":
                        raise ValueError("durable execution requires --backend ray")
                    policy = RetryPolicy(
                        3 if options.max_attempts is None else options.max_attempts,
                        60.0 if options.job_timeout is None else options.job_timeout,
                        0.1 if options.retry_delay is None else options.retry_delay,
                    )
            backend = select_backend(
                "ray" if options.command == "resume" else options.backend,
                options.workers,
                options.max_in_flight,
                ray_address=options.ray_address,
                cpus_per_worker=options.cpus_per_worker,
                worker_resources=options.worker_resource,
                object_store_memory=options.object_store_memory,
            )
            if options.command == "resume":
                if not isinstance(backend, RayBackend):
                    raise RuntimeError("resume requires the Ray backend")
                summary = resume_experiment(
                    options.data,
                    output=options.output_dir,
                    backend=backend,
                    data_format=options.data_format,
                    expected_version=options.dataset_version,
                )
            elif policy is not None:
                if not isinstance(backend, RayBackend):
                    raise RuntimeError("durable execution requires the Ray backend")
                summary = start_experiment(
                    options.data,
                    symbol=options.symbol,
                    fast=options.fast,
                    slow=options.slow,
                    initial_cash=parse_money(options.initial_cash),
                    fee_bps=options.fee_bps,
                    output=options.output_dir,
                    backend=backend,
                    policy=policy,
                    data_format=options.data_format,
                    expected_version=options.dataset_version,
                )
            else:
                summary = run_experiment(
                    options.data,
                    symbol=options.symbol,
                    fast=options.fast,
                    slow=options.slow,
                    initial_cash=parse_money(options.initial_cash),
                    fee_bps=options.fee_bps,
                    output=options.output_dir,
                    backend=backend,
                    data_format=options.data_format,
                    expected_version=options.dataset_version,
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
        dataset = load_dataset(
            options.data,
            symbol=options.symbol,
            price_basis=options.price_basis,
            data_format=options.data_format,
            expected_version=options.dataset_version,
        )
        emit_event("backtest_started", symbol=options.symbol)
        result = run_backtest(config, dataset)
        if options.output is None:
            sys.stdout.write(serialize_result(result))
        else:
            write_result(result, options.output, overwrite=options.overwrite)
        emit_event("backtest_finished", symbol=options.symbol)
    except FileExistsError as error:
        guidance = (
            "choose a new output directory, or use resume for a durable experiment"
            if options.command in {"sweep", "resume"}
            else "use --overwrite to replace it"
        )
        detail = f"output exists; {guidance}: {error}"
        emit_event(
            "command_failed",
            level="ERROR",
            error_type=type(error).__name__,
            detail=detail,
        )
        if options.log_level == "OFF":
            print(f"error: {detail}", file=sys.stderr)
        return 2
    except (ValueError, OSError, RuntimeError, sqlite3.Error) as error:
        emit_event(
            "command_failed",
            level="ERROR",
            error_type=type(error).__name__,
            detail=str(error),
        )
        if options.log_level == "OFF":
            print(f"error: {error}", file=sys.stderr)
        return 2
    return 0
