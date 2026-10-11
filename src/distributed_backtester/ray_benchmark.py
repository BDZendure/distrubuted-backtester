"""Cold, verified sequential/process/owned-local-Ray comparisons.

Run with ``python -m distributed_backtester.ray_benchmark run --bars 4096
--jobs 64 --workers 2 4 --repetitions 3 --output-dir artifacts/ray-benchmark``.
Use a stable source checkout during a run. Ray and psutil remain lazy optional
dependencies. This CLI deliberately has no remote-cluster or retry options.
"""

import argparse
import importlib
import json
import os
import sys
from collections.abc import Iterator, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from hashlib import sha256
from importlib import metadata
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import cast

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.backends.process import ProcessBackend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.benchmark import (
    _read_record,
    semantic_digest,
    verify_trial_identity,
)
from distributed_backtester.data import load_csv
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import (
    JobSpec,
    canonical_json,
    create_job,
    metadata_record,
)
from distributed_backtester.models import MarketDataset
from distributed_backtester.money import require_integer
from distributed_backtester.provenance import (
    ImplementationIdentity,
    capture_environment,
    current_implementation,
)
from distributed_backtester.ray_measurement import (
    _require_psutil,
    _session_argument,
    measure_command,
    validate_interval,
)
from distributed_backtester.results import write_text
from distributed_backtester.worker import JobOutcome
from distributed_backtester.workloads import job_configs, write_synthetic_csv


def _preflight() -> dict[str, object]:
    """Check optional dependencies without paying Ray import cost in the parent."""
    psutil = _require_psutil()
    try:
        version = metadata.version("ray")
    except metadata.PackageNotFoundError as error:
        raise RuntimeError(
            "Ray benchmark requires pip install '.[distributed,benchmark]'"
        ) from error
    if version != "2.59.0":
        raise RuntimeError("Ray benchmark requires pinned Ray 2.59.0")
    if os.name != "posix":
        raise RuntimeError("Owned local benchmark inventory requires POSIX")
    return {"ray_version": version, "psutil_version": psutil.__version__}


def summarize(
    rows: Sequence[dict[str, object]], *, jobs: int
) -> list[dict[str, object]]:
    groups: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        groups.setdefault(cast(str, row["configuration"]), []).append(row)
    baseline = median(
        cast(float, row["elapsed_seconds"]) for row in groups["sequential"]
    )
    summaries: list[dict[str, object]] = []
    for name, group in groups.items():
        times = [cast(float, row["elapsed_seconds"]) for row in group]
        mid = median(times)
        workers = cast(int, group[0]["workers"])
        peaks = [row["peak_private_uss_bytes"] for row in group]
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
                "median_peak_private_uss_bytes": (
                    median(cast(list[int], peaks))
                    if all(value is not None for value in peaks)
                    else None
                ),
                "private_memory_peak_scope": (
                    "maximum over complete owned-inventory samples; any discovery "
                    "or scoped USS denial/unavailability invalidates the run peak"
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
    repetitions: int = 3,
    seed: int = 1729,
    interval_seconds: float = 0.05,
    discard_trial_results: bool = False,
) -> dict[str, object]:
    """Warm each configuration once, then rotate repeated cold trials."""
    for name, value in (("bars", bars), ("jobs", jobs), ("repetitions", repetitions)):
        require_integer(value, name, minimum=1)
    require_integer(seed, "seed")
    if type(discard_trial_results) is not bool:
        raise ValueError("discard_trial_results must be a bool")
    validate_interval(interval_seconds)
    if not workers:
        raise ValueError("benchmark requires at least one worker count")
    for count in workers:
        require_integer(count, "workers", minimum=1)
    if len(set(workers)) != len(workers):
        raise ValueError("benchmark worker counts must be unique")
    environment = capture_environment()
    available = environment["available_cores"]
    if isinstance(available, int) and max(workers) > available:
        raise ValueError(f"worker count exceeds {available} available cores")
    dependencies = _preflight()
    from distributed_backtester.measurement import hardware_environment

    hardware = hardware_environment()
    implementation = current_implementation()
    output.mkdir(parents=True, exist_ok=False)
    source = output / "data.csv"
    write_synthetic_csv(source, bars=bars, seed=seed)
    # load_csv hashes the exact byte snapshot it parses. Capture expected jobs
    # now, before any subprocess can become the reference for input identity.
    parent_dataset = load_csv(source, symbol="BENCH", price_basis="raw")
    if len(parent_dataset.bars) != bars:
        raise RuntimeError("generated CSV bar count differs from declared workload")
    expected_jobs = tuple(
        create_job(config, parent_dataset.metadata, implementation)
        for config in job_configs(jobs)
    )
    expected_payload: dict[str, object] = {
        "schema_version": 1,
        "dataset": metadata_record(parent_dataset.metadata),
        "implementation": asdict(implementation),
        "jobs": [
            job.to_dict() for job in sorted(expected_jobs, key=lambda job: job.job_id)
        ],
        "grid_membership": [job.job_id for job in expected_jobs],
    }
    expected_experiment = sha256(
        canonical_json(expected_payload).encode("utf-8")
    ).hexdigest()
    expected_manifest = {"experiment_id": expected_experiment, **expected_payload}
    expected_ids = {job.job_id for job in expected_jobs}
    expected_csv_sha256 = parent_dataset.metadata.source_sha256
    write_text(
        canonical_json(expected_manifest) + "\n", output / "expected_manifest.json"
    )
    configurations = [
        ("sequential", "sequential", 1),
        *[(f"process-{count}", "process", count) for count in workers],
        *[(f"ray-{count}", "ray", count) for count in workers],
    ]
    warmups: list[dict[str, object]] = []
    runs: list[dict[str, object]] = []
    expected_digest: str | None = None
    report: dict[str, object] = {
        "schema_version": 1,
        "started_utc": datetime.now(UTC).isoformat(),
        "environment": environment,
        "hardware": hardware,
        "dependencies": dependencies,
        "python_executable": sys.executable,
        "implementation": asdict(implementation),
        "revision_clean": environment["git_dirty"] is False,
        "workload": {
            "bars": bars,
            "jobs": jobs,
            "seed": seed,
            "symbol": "BENCH",
            "source_sha256": expected_csv_sha256,
            "expected_experiment_id": expected_experiment,
            "expected_manifest": "expected_manifest.json",
            "config_generator": "bounded-sma-windows-cash-batches-v1",
            "fee_bps": 10,
        },
        "method": {
            "timer": "perf_counter before Popen through child exit",
            "includes": (
                "Python/package/Ray imports, CSV load, jobs, local cluster startup, "
                "dataset placement, actor readiness, simulation, detailed export, "
                "finalization, backend shutdown and process exit"
            ),
            "excludes": (
                "data generation, parent expected manifest, observer memory, "
                "post-exit verification "
                "and orphan cleanup"
            ),
            "cache": "fresh process per trial; OS caches uncontrolled",
            "warmups_per_configuration": 1,
            "order": "rotate one position per repetition",
            "interval_seconds": interval_seconds,
            "repetitions": repetitions,
            "memory": (
                "Owned local POSIX process group plus exact published session/PID "
                "identities. Per-process RSS never summed. Aggregate USS only for "
                "complete inventory samples; uncertain discovery or unavailable "
                "USS invalidates "
                "peak. Sampled peaks can miss short-lived services. Serialized "
                "dataset size and store capacity are separate logical quantities; "
                "live object-store occupancy is unobserved. Before readiness "
                "publication session coverage is incomplete. No remote memory."
            ),
        },
        "warmups": warmups,
        "runs": runs,
        "semantic_outputs_equal": False,
        "detailed_results_retained": not discard_trial_results,
        "discard_method": (
            "After identity, every artifact digest and cross-trial semantic "
            "equality are verified, unlink only indexed detailed results from "
            "new trial directories. Removed paths/bytes are retained per trial."
            if discard_trial_results
            else None
        ),
    }

    def persist() -> None:
        write_text(
            canonical_json(report) + "\n", output / "observations.json", overwrite=True
        )

    def trial(
        name: str,
        backend: str,
        count: int,
        label: str,
        destination: list[dict[str, object]],
    ) -> None:
        nonlocal expected_digest
        trial_dir = output / label
        ownership = output / f"{label}.ownership.json"
        command = [
            sys.executable,
            "-m",
            "distributed_backtester.ray_benchmark",
            "_trial",
            "--data",
            str(source.resolve()),
            "--jobs",
            str(jobs),
            "--backend",
            backend,
            "--workers",
            str(count),
            "--output-dir",
            str(trial_dir.resolve()),
            "--ownership",
            str(ownership.resolve()),
        ]
        measured = measure_command(
            command,
            stdout=output / f"{label}.stdout",
            stderr=output / f"{label}.stderr",
            ownership=ownership,
            ray=backend == "ray",
            interval_seconds=interval_seconds,
        )
        row: dict[str, object] = {
            "configuration": name,
            "workers": count,
            "label": label,
            "command": command,
            "detailed_results_retained": True,
            "discarded_artifacts": [],
            "discarded_artifact_bytes": 0,
            **asdict(measured),
        }
        destination.append(row)
        persist()
        try:
            if measured.returncode != 0:
                raise RuntimeError(
                    f"benchmark trial {label} failed; "
                    "inspect stderr and observations.json"
                )
            index = _read_record(trial_dir / "index.json")
            execution = _read_record(trial_dir / "execution.json")
            row["component_timings"] = execution["timings"]
            row["experiment_id"] = index["experiment_id"]
            diagnostics = execution.get("backend_diagnostics")
            row["backend_diagnostics"] = diagnostics
            if backend == "ray":
                if (
                    not isinstance(diagnostics, dict)
                    or diagnostics.get("local_owned") is not True
                ):
                    raise RuntimeError("Ray benchmark requires an owned local cluster")
                ray_timings = diagnostics.get("timings")
                if not isinstance(ray_timings, dict) or any(
                    key not in ray_timings
                    for key in (
                        "startup_seconds",
                        "dataset_put_seconds",
                        "actors_ready_seconds",
                        "shutdown_seconds",
                    )
                ):
                    raise RuntimeError(
                        "Ray benchmark diagnostics lack component timings"
                    )
                row["ray_component_timings"] = ray_timings
            if index.get("submitted") != jobs:
                raise RuntimeError("benchmark submitted count differs from workload")
            verify_trial_identity(trial_dir, asdict(implementation))
            actual_manifest = _read_record(trial_dir / "manifest.json")
            changed = sorted(
                key
                for key in expected_manifest.keys() | actual_manifest.keys()
                if key not in expected_manifest
                or key not in actual_manifest
                or canonical_json(expected_manifest[key])
                != canonical_json(actual_manifest[key])
            )
            dataset_record = actual_manifest.get("dataset")
            row["input_validation"] = {
                "expected_csv_sha256": expected_csv_sha256,
                "actual_csv_sha256": (
                    dataset_record.get("source_sha256")
                    if isinstance(dataset_record, dict)
                    else None
                ),
                "expected_experiment_id": expected_experiment,
                "actual_experiment_id": actual_manifest.get("experiment_id"),
                "changed_manifest_fields": changed,
            }
            if changed:
                raise RuntimeError(
                    "benchmark manifest differs from captured workload: "
                    + ", ".join(changed)
                )
            if index.get("experiment_id") != expected_experiment:
                raise RuntimeError(
                    "benchmark index experiment differs from captured workload"
                )
            outcomes = index.get("outcomes")
            if not isinstance(outcomes, list) or any(
                not isinstance(record, dict)
                or not isinstance(record.get("job_id"), str)
                for record in outcomes
            ):
                raise RuntimeError("benchmark index lacks valid declared job IDs")
            actual_ids = {record["job_id"] for record in outcomes}
            if len(outcomes) != jobs or actual_ids != expected_ids:
                raise RuntimeError(
                    "benchmark index jobs differ from declared configurations"
                )
            digest = semantic_digest(trial_dir)
            row["semantic_sha256"] = digest
            if expected_digest is None:
                expected_digest = digest
            elif digest != expected_digest:
                raise RuntimeError("benchmark semantic outputs differ across trials")
            if discard_trial_results:
                removed = cast(list[str], row["discarded_artifacts"])
                for outcome in cast(list[dict[str, object]], index["outcomes"]):
                    relative = cast(str, outcome["artifact"])
                    artifact = trial_dir / relative
                    size = artifact.stat().st_size
                    artifact.unlink()
                    removed.append(relative)
                    row["discarded_artifact_bytes"] = (
                        cast(int, row["discarded_artifact_bytes"]) + size
                    )
                    row["detailed_results_retained"] = False
        except (OSError, ValueError, RuntimeError, KeyError) as error:
            row["verification_error"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            persist()
        print(
            f"{label}: {measured.elapsed_seconds:.3f}s, verified {jobs} jobs",
            flush=True,
        )

    persist()
    for name, backend, count in configurations:
        trial(name, backend, count, f"warmup-{name}", warmups)
    for repetition in range(repetitions):
        shift = repetition % len(configurations)
        for name, backend, count in [*configurations[shift:], *configurations[:shift]]:
            trial(name, backend, count, f"run-{repetition + 1}-{name}", runs)
    report["semantic_outputs_equal"] = True
    report["semantic_sha256"] = expected_digest
    report["summary"] = summarize(runs, jobs=jobs)
    report["finished_utc"] = datetime.now(UTC).isoformat()
    persist()
    write_text(canonical_json(report) + "\n", output / "report.json")
    return report


class _ObservedBackend:
    """Publish ownership while actors are alive; preserve the streaming contract."""

    def __init__(self, backend: ExecutionBackend, ownership: Path) -> None:
        self.backend = backend
        self.ownership = ownership
        self.name = backend.name
        self.workers = backend.workers
        self.max_in_flight = getattr(backend, "max_in_flight", None)
        self.last_record: str | None = None
        self.driver_pid = os.getpid()
        self.driver_created: float | None = None
        self.driver_error: str | None = None
        if self.name == "ray":
            psutil = _require_psutil()
            try:
                self.driver_created = float(
                    psutil.Process(self.driver_pid).create_time()
                )
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError) as error:
                self.driver_error = f"{type(error).__name__}: {error}"

    @property
    def diagnostics(self) -> dict[str, object]:
        return cast(dict[str, object], getattr(self.backend, "diagnostics", {}))

    @property
    def max_observed_in_flight(self) -> object:
        return getattr(self.backend, "max_observed_in_flight", None)

    def publish(self) -> None:
        diagnostics = self.diagnostics
        if diagnostics.get("local_owned") is not True or not diagnostics.get(
            "session_dir"
        ):
            return
        psutil = _require_psutil()
        processes: list[dict[str, object]] = []
        errors: list[dict[str, object]] = []
        driver_alive = False
        driver_group: int | None = None
        try:
            driver = psutil.Process(self.driver_pid)
            driver_alive = (
                self.driver_created is not None
                and float(driver.create_time()) == self.driver_created
                and driver.is_running()
            )
            if driver_alive:
                driver_group = os.getpgid(self.driver_pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError) as error:
            self.driver_error = f"{type(error).__name__}: {error}"
        if not driver_alive:
            errors.append(
                {
                    "pid": self.driver_pid,
                    "error": self.driver_error
                    or "original driver identity is no longer live",
                }
            )
        session = cast(str, diagnostics["session_dir"])
        for worker in cast(list[dict[str, object]], diagnostics.get("workers", [])):
            pid = cast(int, worker["pid"])
            if not driver_alive:
                continue
            try:
                process = psutil.Process(pid)
                created = float(process.create_time())
                evidence: str | None = None
                if (
                    driver_group == self.driver_pid
                    and os.getpgid(pid) == self.driver_pid
                ):
                    evidence = "private_group"
                elif _session_argument(cast(list[str], process.cmdline()), session):
                    evidence = "owned_session_argument"
                if evidence is None:
                    raise ValueError(
                        "actor PID has no independent group/session ownership proof"
                    )
                # Bare PID lookup never certifies ownership. Revalidate both
                # identities after ownership checks, while the original driver lives.
                if (
                    self.driver_created is None
                    or created < self.driver_created
                    or not process.is_running()
                    or float(process.create_time()) != created
                    or not driver.is_running()
                    or float(driver.create_time()) != self.driver_created
                ):
                    raise ValueError(
                        "process identity changed during actor ownership certification"
                    )
                processes.append(
                    {
                        "pid": pid,
                        "create_time": created,
                        "role": "actor",
                        "ownership": evidence,
                    }
                )
            except (
                psutil.NoSuchProcess,
                psutil.AccessDenied,
                OSError,
                ValueError,
            ) as error:
                errors.append({"pid": pid, "error": f"{type(error).__name__}: {error}"})
        record = canonical_json(
            {
                **diagnostics,
                "driver_pid": self.driver_pid,
                "driver_create_time": self.driver_created,
                "processes": processes,
                "publication_errors": errors,
            }
        )
        if record == self.last_record:
            return
        # A rename keeps the observer from reading a partially written record.
        temporary = self.ownership.with_suffix(".tmp")
        temporary.write_text(record + "\n", encoding="utf-8")
        temporary.replace(self.ownership)
        self.last_record = record

    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
    ) -> Iterator[JobOutcome]:
        outcomes = self.backend.iter_outcomes(jobs, dataset, implementation)
        try:
            for outcome in outcomes:
                self.publish()
                yield outcome
        finally:
            close = getattr(outcomes, "close", None)
            if callable(close):
                close()
            # Keep readiness PID identities: processes may already have exited.
            if self.last_record is None:
                self.publish()


def _trial(
    data: Path,
    jobs: int,
    backend_name: str,
    workers: int,
    output: Path,
    ownership: Path,
) -> None:
    started = perf_counter()
    require_integer(jobs, "jobs", minimum=1)
    require_integer(workers, "workers", minimum=1)
    if backend_name == "sequential" and workers != 1:
        raise ValueError("sequential benchmark must use one worker")
    dataset = load_csv(data, symbol="BENCH", price_basis="raw")
    loaded = perf_counter()
    identity = current_implementation()
    specs = tuple(
        create_job(config, dataset.metadata, identity) for config in job_configs(jobs)
    )
    prepared = perf_counter()
    ray_import_seconds = 0.0
    backend: ExecutionBackend
    if backend_name == "sequential":
        backend = SequentialBackend()
    elif backend_name == "process":
        backend = ProcessBackend(workers)
    elif backend_name == "ray":
        importing = perf_counter()
        ray_backend = importlib.import_module("distributed_backtester.backends.ray")
        importlib.import_module("ray")
        ray_import_seconds = perf_counter() - importing
        backend = cast(
            ExecutionBackend, ray_backend.RayBackend(workers, address="local")
        )
    else:
        raise ValueError("unknown benchmark backend")
    observed = _ObservedBackend(backend, ownership)
    collecting = perf_counter()
    summary = collect_experiment(
        specs,
        dataset,
        identity,
        output,
        observed,
        started=started,
        source_path=data,
        setup_timings={
            "csv_load_seconds": loaded - started,
            "prepare_jobs_seconds": prepared - loaded,
            "ray_import_seconds": ray_import_seconds,
        },
    )
    finished = perf_counter()
    execution = _read_record(summary.execution_path)
    timings = cast(dict[str, object], execution["timings"])
    before_finalization = cast(float, execution["elapsed_before_finalization_seconds"])
    timings["finalization_seconds"] = finished - started - before_finalization
    timings["collection_seconds"] = finished - collecting
    execution["backend_diagnostics"] = observed.diagnostics
    execution["python_executable"] = sys.executable
    write_text(canonical_json(execution) + "\n", summary.execution_path, overwrite=True)
    if summary.failed:
        raise RuntimeError(f"{summary.failed} benchmark jobs failed")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m distributed_backtester.ray_benchmark"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    public = commands.add_parser("run")
    public.add_argument("--bars", type=int, required=True)
    public.add_argument("--jobs", type=int, required=True)
    public.add_argument("--workers", type=int, nargs="+", default=[2, 4])
    public.add_argument("--repetitions", type=int, default=3)
    public.add_argument("--seed", type=int, default=1729)
    public.add_argument("--interval-seconds", type=float, default=0.05)
    public.add_argument("--output-dir", type=Path, required=True)
    public.add_argument(
        "--discard-trial-results",
        action="store_true",
        help="remove new verified detailed artifacts; retain metadata and logs",
    )
    child = commands.add_parser("_trial", help=argparse.SUPPRESS)
    child.add_argument("--data", type=Path, required=True)
    child.add_argument("--jobs", type=int, required=True)
    child.add_argument(
        "--backend", choices=("sequential", "process", "ray"), required=True
    )
    child.add_argument("--workers", type=int, required=True)
    child.add_argument("--output-dir", type=Path, required=True)
    child.add_argument("--ownership", type=Path, required=True)
    options = parser.parse_args(argv)
    try:
        if options.command == "_trial":
            _trial(
                options.data,
                options.jobs,
                options.backend,
                options.workers,
                options.output_dir,
                options.ownership,
            )
        else:
            report = run_benchmarks(
                options.output_dir,
                bars=options.bars,
                jobs=options.jobs,
                workers=options.workers,
                repetitions=options.repetitions,
                seed=options.seed,
                interval_seconds=options.interval_seconds,
                discard_trial_results=options.discard_trial_results,
            )
            print(json.dumps(report["summary"], indent=2))
    except (ValueError, OSError, RuntimeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
