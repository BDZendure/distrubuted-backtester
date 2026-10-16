"""Fresh-process, end-to-end benchmarks with verified detailed output equality."""

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import cast

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.backends.process import ProcessBackend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.data import load_csv
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import canonical_json, create_job
from distributed_backtester.money import require_integer
from distributed_backtester.provenance import (
    capture_environment,
    current_implementation,
)
from distributed_backtester.results import write_text
from distributed_backtester.workloads import job_configs, write_synthetic_csv


def _read_record(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"expected a JSON record: {path}")
    return cast(dict[str, object], value)


def verify_trial_identity(output: Path, expected: Mapping[str, object]) -> None:
    manifest = _read_record(output / "manifest.json")
    if manifest.get("implementation") != expected:
        raise RuntimeError("benchmark trial implementation differs from report")


def semantic_digest(output: Path) -> str:
    """Verify every detailed artifact, then fingerprint all semantic outputs."""
    manifest = (output / "manifest.json").read_bytes()
    index_bytes = (output / "index.json").read_bytes()
    index = _read_record(output / "index.json")
    if index.get("failed") != 0 or index.get("succeeded") != index.get("submitted"):
        raise RuntimeError("benchmark experiment must succeed completely")
    outcomes = index.get("outcomes")
    if (
        not isinstance(outcomes, list)
        or not outcomes
        or len(outcomes) != index["submitted"]
    ):
        raise RuntimeError("benchmark index has an invalid outcome count")
    seen: set[str] = set()
    for record in outcomes:
        if not isinstance(record, dict) or record.get("status") != "succeeded":
            raise RuntimeError("benchmark index has an invalid outcome")
        job_id = record.get("job_id")
        if not isinstance(job_id, str) or job_id in seen:
            raise RuntimeError("benchmark index has an invalid job ID")
        seen.add(job_id)
        relative = record.get("artifact")
        if relative != f"results/{job_id}.json" or "/" in job_id or "\\" in job_id:
            raise RuntimeError("benchmark index has an invalid artifact path")
        digest = sha256((output / relative).read_bytes()).hexdigest()
        if digest != record.get("content_sha256"):
            raise RuntimeError(f"artifact digest mismatch: {relative}")
    # Index contains each verified artifact digest, so these two files cover all.
    return sha256(len(manifest).to_bytes(8, "big") + manifest + index_bytes).hexdigest()


def summarize(
    rows: Sequence[dict[str, object]], *, jobs: int
) -> list[dict[str, object]]:
    groups: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        name = cast(str, row["configuration"])
        groups.setdefault(name, []).append(row)
    baseline = median(
        cast(float, row["elapsed_seconds"]) for row in groups["sequential"]
    )
    summaries: list[dict[str, object]] = []
    for name, group in groups.items():
        times = [cast(float, row["elapsed_seconds"]) for row in group]
        mid = median(times)
        workers = cast(int, group[0]["workers"])
        tree = [row["peak_tree_rss_bytes"] for row in group]
        coordinator = [row["peak_coordinator_rss_bytes"] for row in group]
        summaries.append(
            {
                "configuration": name,
                "workers": workers,
                "repetitions": len(group),
                "median_seconds": mid,
                "min_seconds": min(times),
                "max_seconds": max(times),
                "jobs_per_second": jobs / mid,
                "speedup": baseline / mid,
                "parallel_efficiency": baseline / mid / workers,
                "median_peak_tree_rss_bytes": (
                    median(cast(list[int], tree))
                    if all(value is not None for value in tree)
                    else None
                ),
                "median_peak_coordinator_rss_bytes": (
                    median(cast(list[int], coordinator))
                    if all(value is not None for value in coordinator)
                    else None
                ),
            }
        )
    return summaries


def run_benchmarks(
    output: Path,
    *,
    bars: int,
    jobs: int,
    workers: Sequence[int],
    repetitions: int = 5,
    seed: int = 1729,
    interval_seconds: float = 0.05,
) -> dict[str, object]:
    """One warm-up per configuration, then rotate timed order each repetition."""
    from distributed_backtester.measurement import hardware_environment, measure_command

    for name, value in [("bars", bars), ("jobs", jobs), ("repetitions", repetitions)]:
        require_integer(value, name, minimum=1)
    require_integer(seed, "seed")
    if len(set(workers)) != len(workers):
        raise ValueError("benchmark worker counts must be unique")
    environment = capture_environment()
    hardware = hardware_environment()
    available = environment["available_cores"]
    for count in workers:
        require_integer(count, "workers", minimum=1)
        if isinstance(available, int) and count > available:
            raise ValueError(f"{count} workers exceeds {available} available cores")
    # Validate optional measurement support and interval before creating output.
    if type(interval_seconds) not in (int, float) or not 0 < interval_seconds < float(
        "inf"
    ):
        raise ValueError("interval_seconds must be finite and positive")
    implementation = current_implementation()
    output.mkdir(parents=True, exist_ok=False)
    source = output / "data.csv"
    write_synthetic_csv(source, bars=bars, seed=seed)
    configurations = [
        ("sequential", 0),
        *[(f"process-{count}", count) for count in workers],
    ]
    warmups: list[dict[str, object]] = []
    runs: list[dict[str, object]] = []
    expected_digest: str | None = None
    report: dict[str, object] = {
        "schema_version": 1,
        "started_utc": datetime.now(UTC).isoformat(),
        "environment": environment,
        "hardware": hardware,
        "implementation": asdict(implementation),
        "workload": {
            "bars": bars,
            "jobs": jobs,
            "seed": seed,
            "symbol": "BENCH",
            "source_sha256": sha256(source.read_bytes()).hexdigest(),
            "config_generator": "bounded-sma-windows-cash-batches-v1",
            "fee_bps": 10,
        },
        "method": {
            "timer": "perf_counter before Popen through child exit",
            "includes": (
                "imports, CSV load, job creation, pool startup, simulation, "
                "collection, detailed export, finalization, shutdown"
            ),
            "excludes": (
                "dataset generation, observer process RSS, "
                "artifact verification after exit"
            ),
            "cache": "process-cold / OS cache uncontrolled",
            "warmups_per_configuration": 1,
            "memory": (
                "sampled simultaneous RSS sum of coordinator and live descendants"
            ),
            "interval_seconds": interval_seconds,
            "repetitions": repetitions,
        },
        "warmups": warmups,
        "runs": runs,
        "semantic_outputs_equal": False,
    }

    def trial(
        name: str, count: int, label: str, destination: list[dict[str, object]]
    ) -> None:
        nonlocal expected_digest
        trial_dir = output / label
        command = [
            sys.executable,
            "-m",
            "distributed_backtester.benchmark",
            "_trial",
            "--data",
            str(source.resolve()),
            "--jobs",
            str(jobs),
            "--workers",
            str(count),
            "--output-dir",
            str(trial_dir.resolve()),
        ]
        trial_started = datetime.now(UTC).isoformat()
        measured = measure_command(
            command,
            stdout=output / f"{label}.stdout",
            stderr=output / f"{label}.stderr",
            interval_seconds=interval_seconds,
        )
        row: dict[str, object] = {
            "configuration": name,
            "workers": count or 1,
            "label": label,
            "started_utc": trial_started,
            **asdict(measured),
        }
        destination.append(row)
        write_text(
            canonical_json(report) + "\n", output / "observations.json", overwrite=True
        )
        if measured.returncode != 0:
            raise RuntimeError(
                f"benchmark trial {label} failed; "
                "inspect its stderr and observations.json"
            )
        index = _read_record(trial_dir / "index.json")
        execution = _read_record(trial_dir / "execution.json")
        row["component_timings"] = execution["timings"]
        row["experiment_id"] = index["experiment_id"]
        if index.get("submitted") != jobs:
            raise RuntimeError("benchmark submitted count differs from workload")
        digest = semantic_digest(trial_dir)
        verify_trial_identity(trial_dir, asdict(implementation))
        row["semantic_sha256"] = digest
        if expected_digest is None:
            expected_digest = digest
        elif digest != expected_digest:
            raise RuntimeError("benchmark semantic outputs differ across trials")
        print(
            f"{label}: {measured.elapsed_seconds:.3f}s, verified {jobs} jobs",
            flush=True,
        )

    for name, count in configurations:
        trial(name, count, f"warmup-{name}", warmups)
    for repetition in range(repetitions):
        shift = repetition % len(configurations)
        for name, count in [*configurations[shift:], *configurations[:shift]]:
            trial(name, count, f"run-{repetition + 1}-{name}", runs)
    report["semantic_outputs_equal"] = True
    report["semantic_sha256"] = expected_digest
    report["summary"] = summarize(runs, jobs=jobs)
    report["finished_utc"] = datetime.now(UTC).isoformat()
    write_text(canonical_json(report) + "\n", output / "report.json")
    return report


def _trial(data: Path, jobs: int, workers: int, output: Path) -> None:
    started = perf_counter()
    dataset = load_csv(data, symbol="BENCH", price_basis="raw")
    loaded = perf_counter()
    identity = current_implementation()
    specs = tuple(
        create_job(config, dataset.metadata, identity) for config in job_configs(jobs)
    )
    backend: ExecutionBackend = (
        SequentialBackend() if workers == 0 else ProcessBackend(workers)
    )
    summary = collect_experiment(
        specs,
        dataset,
        identity,
        output,
        backend,
        started=started,
        source_path=data,
        setup_timings={
            "csv_load_seconds": loaded - started,
            "prepare_jobs_seconds": perf_counter() - loaded,
        },
    )
    if summary.failed:
        raise RuntimeError(f"{summary.failed} benchmark jobs failed")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m distributed_backtester.benchmark")
    commands = parser.add_subparsers(dest="command", required=True)
    public = commands.add_parser("run")
    public.add_argument("--bars", type=int, required=True)
    public.add_argument("--jobs", type=int, required=True)
    public.add_argument("--workers", type=int, nargs="*", default=[2, 4, 8])
    public.add_argument("--repetitions", type=int, default=5)
    public.add_argument("--seed", type=int, default=1729)
    public.add_argument("--interval-seconds", type=float, default=0.05)
    public.add_argument("--output-dir", type=Path, required=True)
    child = commands.add_parser("_trial", help=argparse.SUPPRESS)
    child.add_argument("--data", type=Path, required=True)
    child.add_argument("--jobs", type=int, required=True)
    child.add_argument("--workers", type=int, required=True)
    child.add_argument("--output-dir", type=Path, required=True)
    options = parser.parse_args(argv)
    try:
        if options.command == "_trial":
            _trial(options.data, options.jobs, options.workers, options.output_dir)
        else:
            report = run_benchmarks(
                options.output_dir,
                bars=options.bars,
                jobs=options.jobs,
                workers=options.workers,
                repetitions=options.repetitions,
                seed=options.seed,
                interval_seconds=options.interval_seconds,
            )
            print(json.dumps(report["summary"], indent=2))
    except (ValueError, OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
