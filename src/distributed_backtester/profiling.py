"""Local CPU, live Python allocation and uninstrumented scoped timing evidence.

Run real measurements only with package source frozen. Simulation retains every
result until independent hashing; experiment exports through the sequential
backend. CPU seconds and allocation traces describe this interpreter only.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter, process_time
from typing import TYPE_CHECKING, Any, Literal, cast

from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.data import load_csv
from distributed_backtester.experiment_snapshot import read_experiment_snapshot
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import canonical_json, config_record, create_job
from distributed_backtester.models import BacktestConfig
from distributed_backtester.money import require_integer
from distributed_backtester.provenance import (
    ImplementationIdentity,
    capture_environment,
    current_implementation,
    source_fingerprint,
)
from distributed_backtester.results import result_to_dict, write_text
from distributed_backtester.simulation import run_backtest
from distributed_backtester.storage_measurement import peak_rss
from distributed_backtester.workloads import job_configs, write_synthetic_csv

if TYPE_CHECKING:
    import cProfile
    import tracemalloc

Kind = Literal["cpu", "memory", "timing"]
Scope = Literal["simulation", "experiment"]


def _destination(output: Path) -> Path:
    if not isinstance(output, Path):
        raise ValueError("output must be a Path")
    if output.exists() or output.is_symlink():
        raise ValueError("profile output must be a fresh directory")
    resolved = output.resolve()
    package = Path(__file__).resolve().parent
    repository = next((p for p in package.parents if (p / ".git").exists()), None)
    if (
        resolved.is_relative_to(package)
        or package.is_relative_to(resolved)
        or (repository is not None and resolved.is_relative_to(repository))
        or any(name in {".git", ".hg", ".svn"} for name in resolved.parts)
    ):
        raise ValueError("protected profile output destination")
    return resolved


def _guard(implementation: ImplementationIdentity) -> None:
    if source_fingerprint() != implementation.source_sha256:
        raise RuntimeError("package source changed during profiling; freeze source")


def _guard_data(source: Path, expected: str) -> None:
    if sha256(source.read_bytes()).hexdigest() != expected:
        raise RuntimeError("generated dataset changed during profiling")


def _digest(records: Sequence[dict[str, object]]) -> str:
    return sha256(canonical_json(records).encode("utf-8")).hexdigest()


def _experiment_digest(
    output: Path,
    configs: tuple[BacktestConfig, ...],
    implementation: ImplementationIdentity,
    data_sha256: str,
) -> str:
    """Consume the entire validating snapshot iterator before claiming success."""
    with read_experiment_snapshot(output) as snapshot:
        if (
            snapshot.submitted != len(configs)
            or snapshot.succeeded != len(configs)
            or snapshot.failed
            or snapshot.manifest["implementation"] != asdict(implementation)
        ):
            raise RuntimeError("profile experiment must succeed with expected identity")
        ordered = sorted(
            snapshot.jobs, key=lambda j: canonical_json(config_record(j.config))
        )
        if tuple(job.config for job in ordered) != configs or any(
            job.implementation != implementation
            or job.dataset.source_sha256 != data_sha256
            or job.dataset.symbol != "BENCH"
            or job.dataset.price_basis != "raw"
            for job in ordered
        ):
            raise RuntimeError("profile experiment differs from workload")
        records = {
            job_id: json.loads(content)["result"]
            for job_id, content, _ in snapshot.iter_artifacts()
        }
        return _digest([records[job.job_id] for job in ordered])


def _record_secondary(
    primary: BaseException, stage: str, error: BaseException, record: dict[str, Any]
) -> None:
    diagnostic = {
        "stage": stage,
        "error_type": type(error).__name__,
        "message": str(error),
    }
    record.setdefault("diagnostics", []).append(diagnostic)
    primary.add_note(f"Secondary {stage} failure: {type(error).__name__}: {error}")


def _cpu_record(
    profiler: cProfile.Profile, output: Path, repetition: int
) -> dict[str, Any]:
    import pstats

    filename = f"run-{repetition}.pstats"
    profiler.dump_stats(str(output / filename))
    stats = pstats.Stats(profiler)
    raw_stats = cast(
        dict[tuple[str, int, str], tuple[int, int, float, float, object]],
        cast(Any, stats).stats,
    )
    return {
        "raw_stats": filename,
        "functions": [
            {
                "filename": key[0],
                "line": key[1],
                "name": key[2],
                "primitive_calls": values[0],
                "total_calls": values[1],
                "self_seconds": values[2],
                "cumulative_seconds": values[3],
            }
            for key, values in sorted(raw_stats.items())
        ],
    }


def _allocation_record(
    traced: tuple[int, int],
    before: tracemalloc.Snapshot | None,
    after: tracemalloc.Snapshot | None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "current_traced_bytes": traced[0],
        "peak_traced_bytes": traced[1],
    }
    if before is not None and after is not None:
        record["live_differences"] = [
            {
                "traceback": [
                    {"filename": frame.filename, "line": frame.lineno}
                    for frame in stat.traceback
                ],
                "size_diff_bytes": stat.size_diff,
                "count_diff": stat.count_diff,
                "size_bytes": stat.size,
                "count": stat.count,
            }
            for stat in after.compare_to(before, "traceback")
        ]
    return record


def _measure[T](
    operation: Callable[[], T], kind: Kind, output: Path, row: dict[str, Any]
) -> T:
    """Release instrumentation and preserve a primary setup/workload exception."""
    profiler: cProfile.Profile | None = None
    owns_tracer = False
    before: tracemalloc.Snapshot | None = None
    after: tracemalloc.Snapshot | None = None
    traced: tuple[int, int] | None = None
    started: float | None = None
    cpu_started: float | None = None
    failure: BaseException | None = None

    def observe[U](stage: str, observation: Callable[[], U]) -> U | None:
        nonlocal failure
        try:
            return observation()
        except BaseException as error:
            if failure is None:
                failure = error
            else:
                _record_secondary(failure, stage, error, row)
            return None

    try:
        if kind == "cpu":
            import cProfile

            profiler = cProfile.Profile()
            profiler.enable()
        elif kind == "memory":
            import tracemalloc

            if tracemalloc.is_tracing():
                raise ValueError("memory profiling requires no existing tracing")
            owns_tracer = True
            tracemalloc.start()
            before = tracemalloc.take_snapshot()
            tracemalloc.reset_peak()
        started = perf_counter()
        cpu_started = process_time()
        return operation()
    except BaseException as error:
        failure = error
        raise
    finally:
        failed_operation = failure is not None
        cpu_finished = (
            observe("CPU clock", process_time) if cpu_started is not None else None
        )
        finished = observe("wall clock", perf_counter) if started is not None else None
        if profiler is not None:
            observe("CPU profiler shutdown", profiler.disable)
        if owns_tracer:
            # Capture the high water before snapshot/report/verification work.
            traced = observe("allocation counters", tracemalloc.get_traced_memory)
            after = observe("allocation snapshot", tracemalloc.take_snapshot)
            observe("allocation tracer shutdown", tracemalloc.stop)
        if cpu_finished is not None and cpu_started is not None:
            row["cpu_seconds"] = cpu_finished - cpu_started
            row["cpu_scope"] = "own_process_this_interpreter"
        if finished is not None and started is not None:
            row["elapsed_seconds"] = finished - started
        memory = observe("RSS observation", peak_rss)
        if memory is not None:
            row["memory"] = memory
        if profiler is not None:
            cpu_profile = observe(
                "CPU statistics export",
                lambda: _cpu_record(profiler, output, row["repetition"]),
            )
            if cpu_profile is not None:
                row["cpu_profile"] = cpu_profile
        if traced is not None:
            allocations = observe(
                "allocation differences",
                lambda: _allocation_record(traced, before, after),
            )
            if allocations is not None:
                row["allocations"] = allocations
        if not failed_operation and failure is not None:
            raise failure


def profile_workload(
    output: Path,
    *,
    bars: int,
    jobs: int,
    kind: Kind,
    scope: Scope = "simulation",
    repetitions: int = 1,
    seed: int = 1729,
) -> dict[str, Any]:
    """Profile a fresh deterministic workload and verify complete result equality.

    Invalid inputs and active caller instrumentation for timing are rejected
    before output creation. Terminal publication uses the atomic write_text API.
    On failure, publication is attempted; an unwritable report cannot replace
    the primary exception. Secondary failures are retained as notes/diagnostics.
    """
    for name, value in (("bars", bars), ("jobs", jobs), ("repetitions", repetitions)):
        require_integer(value, name, minimum=1)
    require_integer(seed, "seed")
    if bars > (date.max - date(2000, 1, 1)).days + 1:
        raise ValueError("bars exceeds the supported synthetic date range")
    if kind not in ("cpu", "memory", "timing"):
        raise ValueError("kind must be cpu, memory or timing")
    if scope not in ("simulation", "experiment"):
        raise ValueError("scope must be simulation or experiment")
    if kind != "timing" and repetitions != 1:
        raise ValueError("CPU and memory profiling require one repetition")
    if kind in ("memory", "timing"):
        import tracemalloc

        if tracemalloc.is_tracing():
            raise ValueError(f"{kind} profiling requires no existing tracing")
    if kind in ("cpu", "timing") and sys.getprofile() is not None:
        raise ValueError(f"{kind} profiling requires no existing profiler")
    if kind == "timing" and sys.gettrace() is not None:
        raise ValueError("timing requires no existing tracing callback")
    output = _destination(output)
    implementation = current_implementation()
    report: dict[str, Any] = {
        "schema_version": 1,
        "started_utc": datetime.now(UTC).isoformat(),
        "kind": kind,
        "scope": scope,
        "status": "running",
        "implementation": asdict(implementation),
        "environment": capture_environment(),
        "method": {
            "wall_timer": "perf_counter",
            "cpu_timer": "process_time",
            "cpu_scope": "own process CPU seconds, this interpreter only",
            "instrumented": kind != "timing",
            "includes": (
                "fresh sequential run_backtest calls and retained complete results"
                if scope == "simulation"
                else "CSV loading, job construction, sequential simulation, detailed "
                "export and experiment finalization"
            ),
            "excludes": "dataset generation, full result verification and hashing",
            "memory_scope": (
                "own interpreter traced Python bytes; current and peak captured "
                "before final snapshot; before/after live allocation differences, "
                "not allocation frequency; "
                + (
                    "simulation results retained through measurement for hashing"
                    if scope == "simulation"
                    else "experiment result retention follows sequential export"
                )
            ),
            "rss_scope": "own process lifetime high water, not workload-only peak",
            "cache": "same interpreter / OS cache uncontrolled",
            "repetitions": repetitions,
        },
        "workload": {
            "bars": bars,
            "jobs": jobs,
            "seed": seed,
            "symbol": "BENCH",
            "price_basis": "raw",
            "fee_bps": 10,
            "config_generator": "bounded-sma-windows-cash-batches-v1",
        },
        "runs": [],
        "source_unchanged": False,
        "semantic_outputs_equal": False,
    }
    output.mkdir(parents=True, exist_ok=False)
    failure: BaseException | None = None
    try:
        _guard(implementation)
        source = output / "data.csv"
        write_synthetic_csv(source, bars=bars, seed=seed)
        data_sha256 = sha256(source.read_bytes()).hexdigest()
        configs = tuple(
            sorted(
                job_configs(jobs, fee_bps=10),
                key=lambda c: canonical_json(config_record(c)),
            )
        )
        config_records = [config_record(config) for config in configs]
        report["workload"].update(
            source_sha256=data_sha256,
            configs=config_records,
            config_sha256=sha256(canonical_json(config_records).encode()).hexdigest(),
        )
        report["workload_sha256"] = sha256(
            canonical_json(report["workload"]).encode()
        ).hexdigest()
        dataset = (
            load_csv(source, symbol="BENCH", price_basis="raw")
            if scope == "simulation"
            else None
        )
        expected: str | None = None
        for repetition in range(1, repetitions + 1):
            _guard(implementation)
            _guard_data(source, data_sha256)
            row: dict[str, Any] = {"repetition": repetition}
            report["runs"].append(row)
            if dataset is not None:
                results = _measure(
                    lambda: tuple(run_backtest(config, dataset) for config in configs),
                    kind,
                    output,
                    row,
                )
                _guard(implementation)
                digest = _digest([result_to_dict(result) for result in results])
                del results
            else:
                trial = output / f"experiment-{repetition}"
                row["experiment"] = trial.name

                def experiment(trial: Path = trial) -> None:
                    loaded = load_csv(source, symbol="BENCH", price_basis="raw")
                    specs = tuple(
                        create_job(config, loaded.metadata, implementation)
                        for config in job_configs(jobs, fee_bps=10)
                    )
                    summary = collect_experiment(
                        specs,
                        loaded,
                        implementation,
                        trial,
                        SequentialBackend(),
                        source_path=source,
                    )
                    if summary.failed:
                        raise RuntimeError("profile experiment has failed jobs")

                _measure(experiment, kind, output, row)
                _guard(implementation)
                digest = _experiment_digest(trial, configs, implementation, data_sha256)
            _guard(implementation)
            _guard_data(source, data_sha256)
            row["semantic_sha256"] = digest
            if expected is not None and expected != digest:
                raise RuntimeError("complete profile results differ across repetitions")
            expected = digest
        _guard(implementation)
        _guard_data(source, data_sha256)
        report.update(
            status="succeeded",
            source_unchanged=True,
            semantic_outputs_equal=True,
            semantic_sha256=expected,
        )
    except BaseException as error:
        failure = error
        report.update(
            status="failed",
            source_unchanged=False,
            semantic_outputs_equal=False,
            error={"error_type": type(error).__name__, "message": str(error)},
        )
        report.pop("semantic_sha256", None)
        raise
    finally:
        try:
            report["finished_utc"] = datetime.now(UTC).isoformat()
            write_text(canonical_json(report) + "\n", output / "report.json")
        except BaseException as error:
            if failure is None:
                raise
            _record_secondary(failure, "report publication", error, report)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m distributed_backtester.profiling")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--bars", type=int, required=True)
    run.add_argument("--jobs", type=int, required=True)
    run.add_argument("--kind", choices=("cpu", "memory", "timing"), required=True)
    run.add_argument(
        "--scope", choices=("simulation", "experiment"), default="simulation"
    )
    run.add_argument("--repetitions", type=int, default=1)
    run.add_argument("--seed", type=int, default=1729)
    run.add_argument("--output-dir", type=Path, required=True)
    options = parser.parse_args(argv)
    try:
        report = profile_workload(
            options.output_dir,
            bars=options.bars,
            jobs=options.jobs,
            kind=options.kind,
            scope=options.scope,
            repetitions=options.repetitions,
            seed=options.seed,
        )
    except (ValueError, OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
