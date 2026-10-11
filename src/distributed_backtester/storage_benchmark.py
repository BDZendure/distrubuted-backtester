"""Phase 5 deterministic storage harness. Run only after freezing source.

This sidecar uses sequential experiments and one local spawn receiver. Timings
are observational, OS cache is uncontrolled, and RSS is a process lifetime
high water. It makes no cold-disk, cluster-memory or optimization claim.
"""

import argparse
import json
import re
import stat
import sys
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from functools import partial
from hashlib import sha256
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any, Literal, cast
from uuid import uuid4

from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.experiment_snapshot import read_experiment_snapshot
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import canonical_json, config_record, create_job
from distributed_backtester.market_data import DatasetCache, load_dataset
from distributed_backtester.models import DatasetMetadata, MarketDataset
from distributed_backtester.money import require_integer
from distributed_backtester.provenance import (
    ImplementationIdentity,
    capture_environment,
    current_implementation,
)
from distributed_backtester.results import write_text
from distributed_backtester.storage_measurement import (
    MeasurementFailure,
    measure_command,
    measure_operation,
    peak_rss,
    transfer_payload,
    validate_timeout,
    verify_dataset,
)
from distributed_backtester.workloads import job_configs, write_synthetic_csv

_KINDS = (
    "experiment",
    "first_process_read",
    "warm_read",
    "cache_hit",
    "pickle_protocol5",
    "spawn_transfer",
)
_OWNER = ".storage-benchmark-owner.json"


@dataclass(frozen=True, slots=True)
class Layout:
    name: str
    path: Path
    data_format: Literal["csv", "parquet"]
    row_group_size: int | None
    metadata: DatasetMetadata
    identity: dict[str, object]
    file_bytes: int


def _write_record(value: object, path: Path, *, overwrite: bool = False) -> None:
    write_text(canonical_json(value) + "\n", path, overwrite=overwrite)


def _read_record(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return cast(dict[str, Any], value)


def _destination(output: Path) -> Path:
    if not isinstance(output, Path):
        raise ValueError("output must be a Path")
    if output.exists() or output.is_symlink():
        raise ValueError("benchmark output must be a fresh directory")
    resolved = output.resolve()
    package = Path(__file__).resolve().parent
    if resolved.is_relative_to(package) or any(
        name in {".git", ".hg", ".svn"} for name in resolved.parts
    ):
        raise ValueError("protected benchmark output destination")
    return resolved


def _validate(
    *,
    bars: int,
    jobs: int,
    repetitions: int,
    row_group_sizes: Sequence[int],
    seed: int,
    discard_trial_results: bool,
    timeout_seconds: float,
) -> None:
    for name, value in (("bars", bars), ("jobs", jobs), ("repetitions", repetitions)):
        require_integer(value, name, minimum=1)
    require_integer(seed, "seed")
    if bars > (date.max - date(2000, 1, 1)).days + 1:
        raise ValueError("bars exceeds the supported synthetic date range")
    if not row_group_sizes:
        raise ValueError("row_group_sizes must not be empty")
    for size in row_group_sizes:
        require_integer(size, "row_group_sizes", minimum=1)
        if size > 67108864:
            raise ValueError("row_group_sizes must be <= 67108864")
    if len(set(row_group_sizes)) != len(row_group_sizes):
        raise ValueError("row_group_sizes must be unique")
    if type(discard_trial_results) is not bool:
        raise ValueError("discard_trial_results must be a bool")
    validate_timeout(timeout_seconds)


def _load(layout: Layout, cache: DatasetCache | None = None) -> MarketDataset:
    return load_dataset(
        layout.path,
        symbol="BENCH",
        price_basis="raw",
        data_format=layout.data_format,
        expected_version=layout.metadata.version_id,
        cache=cache,
    )


def _verify_identity(actual: object, layout: Layout) -> None:
    if canonical_json(actual) != canonical_json(layout.identity):
        raise RuntimeError(f"dataset identity mismatch for {layout.name}")
    if sha256(layout.path.read_bytes()).hexdigest() != layout.metadata.source_sha256:
        raise RuntimeError(f"dataset snapshot changed for {layout.name}")


def _layouts(
    output: Path, *, bars: int, seed: int, row_group_sizes: Sequence[int]
) -> list[Layout]:
    from distributed_backtester.parquet_data import write_parquet

    csv_path = output / "data" / "synthetic.csv"
    write_synthetic_csv(csv_path, bars=bars, seed=seed)
    csv = load_dataset(csv_path, symbol="BENCH", price_basis="raw", data_format="csv")
    identity = verify_dataset(csv)
    layouts = [
        Layout(
            "csv",
            csv_path,
            "csv",
            None,
            csv.metadata,
            identity,
            csv_path.stat().st_size,
        )
    ]
    for size in row_group_sizes:
        path = output / "data" / f"synthetic-rg{size}.parquet"
        metadata = write_parquet(csv, path, row_group_size=size)
        dataset = load_dataset(
            path,
            symbol="BENCH",
            price_basis="raw",
            data_format="parquet",
            expected_version=metadata.version_id,
        )
        actual = verify_dataset(dataset)
        if dataset.metadata != metadata or any(
            actual[key] != identity[key] for key in ("bars_sha256", "bar_count")
        ):
            raise RuntimeError("converted dataset differs from synthetic CSV")
        layouts.append(
            Layout(
                f"parquet-rg{size}",
                path,
                "parquet",
                size,
                metadata,
                actual,
                path.stat().st_size,
            )
        )
    return layouts


def _expected_manifest(
    layout: Layout, jobs: int, implementation: ImplementationIdentity
) -> dict[str, object]:
    specs = tuple(
        create_job(config, layout.metadata, implementation)
        for config in job_configs(jobs)
    )
    return manifest_record(specs, layout.metadata, implementation)


def _experiment(
    layout: Layout, jobs: int, implementation: ImplementationIdentity, output: Path
) -> MarketDataset:
    started = perf_counter()
    dataset = _load(layout)
    loaded = perf_counter()
    specs = tuple(
        create_job(config, dataset.metadata, implementation)
        for config in job_configs(jobs)
    )
    summary = collect_experiment(
        specs,
        dataset,
        implementation,
        output,
        SequentialBackend(),
        started=started,
        source_path=layout.path,
        setup_timings={
            f"{layout.data_format}_load_seconds": loaded - started,
            "prepare_jobs_seconds": perf_counter() - loaded,
        },
    )
    if summary.failed or summary.submitted != jobs:
        raise RuntimeError("sequential benchmark experiment did not succeed completely")
    return dataset


def verify_experiment(output: Path, expected: dict[str, object]) -> dict[str, Any]:
    """Verify complete artifacts; compare full economics without physical lineage.

    The economic digest includes config, fills, orders, equity, final portfolio,
    metrics, engine and schema. Only result.dataset is excluded. Config hashes
    determine order, since job IDs change with CSV/Parquet physical identities.
    """
    manifest_bytes = (output / "manifest.json").read_bytes()
    index_bytes = (output / "index.json").read_bytes()
    economics: list[dict[str, str]] = []
    artifacts: list[dict[str, object]] = []
    with read_experiment_snapshot(output) as snapshot:
        if canonical_json(snapshot.manifest) != canonical_json(expected):
            raise RuntimeError("experiment manifest differs from captured workload")
        if snapshot.failed or snapshot.succeeded != snapshot.submitted:
            raise RuntimeError("experiment must succeed completely")
        for job, (_, content, record) in zip(
            snapshot.jobs, snapshot.iter_artifacts(), strict=True
        ):
            artifact = json.loads(content)
            result = artifact["result"]
            economic = {key: value for key, value in result.items() if key != "dataset"}
            economics.append(
                {
                    "config_sha256": sha256(
                        canonical_json(config_record(job.config)).encode()
                    ).hexdigest(),
                    "result_sha256": sha256(
                        canonical_json(economic).encode()
                    ).hexdigest(),
                }
            )
            artifacts.append(
                {
                    "artifact": record["artifact"],
                    "content_sha256": record["content_sha256"],
                    "bytes": len(content),
                }
            )
        if (output / "manifest.json").read_bytes() != manifest_bytes or (
            output / "index.json"
        ).read_bytes() != index_bytes:
            raise RuntimeError("experiment snapshot changed during verification")
    execution = _read_record(output / "execution.json")
    if execution.get("backend") != "sequential" or execution.get("workers") != 1:
        raise RuntimeError("storage benchmark requires a sequential experiment")
    return {
        "artifact_sha256": sha256(
            len(manifest_bytes).to_bytes(8, "big") + manifest_bytes + index_bytes
        ).hexdigest(),
        "economic_sha256": sha256(
            canonical_json(
                sorted(economics, key=lambda row: row["config_sha256"])
            ).encode()
        ).hexdigest(),
        "artifacts": artifacts,
        "component_timings": execution.get("timings"),
    }


def _discard_results(root: Path, trial: Path, token: str, row: dict[str, Any]) -> None:
    """Unlink only this fresh run's exact, fully verified regular result files."""
    if row.get("verified") is not True:
        raise RuntimeError("cannot discard an unverified trial")
    if (
        root.resolve() != root
        or trial.resolve() != trial
        or trial.parent != root / "trials"
    ):
        raise RuntimeError("unowned or symlinked trial directory")
    owner = {"owner_token": token, "benchmark_root": str(root)}
    for directory in (root, trial):
        marker = directory / _OWNER
        if marker.is_symlink() or _read_record(marker) != owner:
            raise RuntimeError("unowned benchmark trial")
    results = trial / "results"
    if results.is_symlink() or results.resolve() != results:
        raise RuntimeError("symlinked trial results directory")
    artifacts = row["artifacts"]

    def checked(record: dict[str, Any]) -> Path:
        relative = record["artifact"]
        if not isinstance(relative, str) or not re.fullmatch(
            r"results/[0-9a-f]{64}\.json", relative
        ):
            raise RuntimeError("unowned result artifact path")
        path = trial / relative
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError("result artifact must be an owned regular file")
        if (
            info.st_size != record["bytes"]
            or sha256(path.read_bytes()).hexdigest() != record["content_sha256"]
        ):
            raise RuntimeError("result artifact changed after validation")
        return path

    # Validate all entries before removing the first file. Recheck before unlink.
    for record in artifacts:
        checked(record)
    for record in artifacts:
        checked(record).unlink()
        row["discarded_artifacts"].append(record["artifact"])
        row["discarded_artifact_bytes"] += record["bytes"]
    row["detailed_results_retained"] = False


def summarize(samples: Sequence[dict[str, Any]]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in samples:
        if row["verified"]:
            groups.setdefault((row["layout"], row["kind"]), []).append(row)
    return [
        {
            "layout": layout,
            "kind": kind,
            "repetitions": len(rows),
            "median_seconds": median(row["elapsed_seconds"] for row in rows),
            "min_seconds": min(row["elapsed_seconds"] for row in rows),
            "max_seconds": max(row["elapsed_seconds"] for row in rows),
            "median_end_to_end_seconds": median(
                row["end_to_end_seconds"] for row in rows
            ),
            "time_unit": "seconds",
        }
        for (layout, kind), rows in groups.items()
    ]


def run_benchmarks(
    output: Path,
    *,
    bars: int,
    jobs: int,
    repetitions: int = 5,
    row_group_sizes: Sequence[int] = (8192, 65536),
    seed: int = 1729,
    discard_trial_results: bool = False,
    timeout_seconds: float = 3600,
) -> dict[str, Any]:
    """Retain each raw repetition and fail closed on any identity/parity error."""
    import pickle

    _validate(
        bars=bars,
        jobs=jobs,
        repetitions=repetitions,
        row_group_sizes=row_group_sizes,
        seed=seed,
        discard_trial_results=discard_trial_results,
        timeout_seconds=timeout_seconds,
    )
    output = _destination(output)
    from distributed_backtester.parquet_data import _arrow

    _arrow()  # Optional availability is checked before creating the destination.
    implementation = current_implementation()
    environment = capture_environment()
    output.mkdir(parents=True, exist_ok=False)
    token = uuid4().hex
    owner = {"owner_token": token, "benchmark_root": str(output)}
    _write_record(owner, output / _OWNER)
    samples: list[dict[str, Any]] = []
    workload = {
        "bars": bars,
        "jobs": jobs,
        "seed": seed,
        "symbol": "BENCH",
        "data_generator": "seeded-bounded-price-cycle-v1",
        "config_generator": "bounded-sma-windows-cash-batches-v1",
        "configs": [config_record(config) for config in job_configs(jobs)],
    }
    workload_id = sha256(canonical_json(workload).encode()).hexdigest()
    report: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "started_utc": datetime.now(UTC).isoformat(),
        "workload": workload,
        "workload_id": workload_id,
        "implementation": asdict(implementation),
        "environment": environment,
        "samples": samples,
        "economic_sha256": None,
        "detailed_results_retained": not discard_trial_results,
        "method": {
            "timer": "time.perf_counter",
            "time_unit": "seconds",
            "cache_state": "OS cache uncontrolled",
            "repetitions": repetitions,
            "row_group_sizes": list(row_group_sizes),
            "timeout_seconds": timeout_seconds,
            "order": "rotate layouts each repetition; six operations per layout",
            "read_timing": (
                "load_dataset includes file snapshot, hashing, parsing and internal "
                "validation; independent harness verification excluded"
            ),
            "first_process_read": (
                "child import/setup separately; parent before Popen through child "
                "exit includes interpreter, imports, read, verification and report"
            ),
            "warm_read": (
                "same interpreter, no DatasetCache; already exercised layout, "
                "OS cache uncontrolled"
            ),
            "cache_hit": (
                "DatasetCache(max_entries=1) primed outside timing; every hit "
                "reads and hashes source bytes"
            ),
            "experiment": (
                "load, identity-bound job construction, complete sequential "
                "simulation, detailed export and finalization; independent "
                "artifact verification excluded"
            ),
            "pickle": (
                "protocol 5 in-band dumps and loads timed separately; verification "
                "excluded; end-to-end includes both and verification"
            ),
            "transfer": (
                "same-host multiprocessing spawn, pre-serialized protocol-5 "
                "payload over Pipe; transfer through decode ACK excludes "
                "independent child verification by handshake; end-to-end "
                "through verified child exit"
            ),
            "economic_digest": (
                "complete economic result per configuration, excluding only dataset "
                "lineage; non-experiment rows reference the verified baseline "
                "workload after bar identity verification"
            ),
            "memory": (
                "resource.getrusage(RUSAGE_SELF).ru_maxrss normalized to bytes; "
                "lifetime own-process high water includes earlier work; parent "
                "and child reported separately, never summed"
            ),
            "discard": (
                "after complete artifact, workload and parity validation, only "
                "token-owned results/<job_id>.json files are unlinked; manifests, "
                "indexes, hashes, observations and child reports remain"
            ),
        },
    }

    def persist() -> None:
        _write_record(report, output / "observations.json", overwrite=True)

    persist()
    artifact_digests: dict[str, str] = {}
    try:
        layouts = _layouts(
            output, bars=bars, seed=seed, row_group_sizes=row_group_sizes
        )
        expected = {
            layout.name: _expected_manifest(layout, jobs, implementation)
            for layout in layouts
        }
        report["layouts"] = [
            {
                "name": layout.name,
                "path": str(layout.path.relative_to(output)),
                "data_format": layout.data_format,
                "row_group_size": layout.row_group_size,
                "metadata": asdict(layout.metadata),
                "identity": layout.identity,
                "file_bytes": layout.file_bytes,
            }
            for layout in layouts
        ]
        _write_record(
            {
                "schema_version": 1,
                "workload": workload,
                "workload_id": workload_id,
                "implementation": asdict(implementation),
                "layouts": report["layouts"],
                "expected_experiments": expected,
            },
            output / "manifest.json",
        )
        persist()
        for repetition in range(1, repetitions + 1):
            shift = (repetition - 1) % len(layouts)
            for layout in [*layouts[shift:], *layouts[:shift]]:
                cache = DatasetCache(max_entries=1)
                dataset: MarketDataset | None = None
                payload: bytes | None = None
                for kind in _KINDS:
                    label = f"run-{repetition}-{layout.name}-{kind}"
                    row: dict[str, Any] = {
                        "schema_version": 1,
                        "label": label,
                        "repetition": repetition,
                        "kind": kind,
                        "layout": layout.name,
                        "row_group_size": layout.row_group_size,
                        "data_format": layout.data_format,
                        "workload_id": workload_id,
                        **layout.identity,
                        "file_bytes": layout.file_bytes,
                        "implementation": asdict(implementation),
                        "environment": environment,
                        "time_unit": "seconds",
                        "timer": "time.perf_counter",
                        "cache_state": "OS cache uncontrolled",
                        "elapsed_seconds": None,
                        "end_to_end_seconds": None,
                        "import_setup_seconds": None,
                        "payload_bytes": None,
                        "memory": None,
                        "economic_sha256": report["economic_sha256"],
                        "economic_digest_scope": (
                            "reference experiment for the same verified bars "
                            "and workload"
                        ),
                        "verified": False,
                        "started_utc": datetime.now(UTC).isoformat(),
                    }
                    samples.append(row)
                    persist()
                    sample_started = perf_counter()
                    try:
                        if kind == "experiment":
                            trial = (
                                output / "trials" / f"run-{repetition}-{layout.name}"
                            )
                            row.update(
                                trial_directory=str(trial.relative_to(output)),
                                discarded_artifacts=[],
                                discarded_artifact_bytes=0,
                                detailed_results_retained=True,
                            )
                            measured = measure_operation(
                                partial(
                                    _experiment, layout, jobs, implementation, trial
                                )
                            )
                            row.update(
                                elapsed_seconds=measured.elapsed_seconds,
                                end_to_end_seconds=measured.elapsed_seconds,
                                memory=measured.memory,
                            )
                            dataset = measured.value
                            _write_record(owner, trial / _OWNER)
                            _verify_identity(verify_dataset(dataset), layout)
                            verified = verify_experiment(trial, expected[layout.name])
                            row.update(verified)
                            row["economic_digest_scope"] = "this complete experiment"
                            if report["economic_sha256"] is None:
                                report["economic_sha256"] = verified["economic_sha256"]
                            elif (
                                report["economic_sha256"] != verified["economic_sha256"]
                            ):
                                raise RuntimeError(
                                    "economic digests differ across layouts "
                                    "or repetitions"
                                )
                            previous = artifact_digests.setdefault(
                                layout.name, verified["artifact_sha256"]
                            )
                            if previous != verified["artifact_sha256"]:
                                raise RuntimeError(
                                    "full artifact digests differ for identical sources"
                                )
                            row["verified"] = True
                            persist()  # Persist verification evidence before deletion.
                            if discard_trial_results:
                                _discard_results(output, trial, token, row)
                        elif kind == "first_process_read":
                            raw = output / "raw"
                            request = raw / f"{label}.request.json"
                            child_output = raw / f"{label}.json"
                            _write_record(
                                {
                                    "path": str(layout.path),
                                    "data_format": layout.data_format,
                                    "expected_version": layout.metadata.version_id,
                                },
                                request,
                            )
                            command = [
                                sys.executable,
                                "-m",
                                "distributed_backtester.storage_measurement",
                                "_read",
                                "--request",
                                str(request),
                                "--output",
                                str(child_output),
                            ]
                            command_run = measure_command(
                                command,
                                stdout=raw / f"{label}.stdout",
                                stderr=raw / f"{label}.stderr",
                                timeout_seconds=timeout_seconds,
                            )
                            row.update(
                                returncode=command_run.returncode,
                                end_to_end_seconds=command_run.elapsed_seconds,
                            )
                            if command_run.returncode:
                                raise RuntimeError(
                                    f"first-read child failed: {label}; "
                                    "inspect raw stderr"
                                )
                            child = _read_record(child_output)
                            row.update(
                                elapsed_seconds=child["elapsed_seconds"],
                                import_setup_seconds=child["import_setup_seconds"],
                                memory=child["memory"],
                                environment=child["environment"],
                                implementation=child["implementation"],
                            )
                            _verify_identity(child["identity"], layout)
                            if child["implementation"] != asdict(implementation):
                                raise RuntimeError(
                                    "first-read child source differs from frozen source"
                                )
                        elif kind in {"warm_read", "cache_hit"}:
                            if kind == "cache_hit":
                                _load(layout, cache)  # Prime outside timing.
                            hits, misses = cache.hits, cache.misses
                            measured = measure_operation(
                                partial(
                                    _load,
                                    layout,
                                    cache if kind == "cache_hit" else None,
                                )
                            )
                            row.update(
                                elapsed_seconds=measured.elapsed_seconds,
                                end_to_end_seconds=measured.elapsed_seconds,
                                memory=measured.memory,
                            )
                            dataset = measured.value
                            _verify_identity(verify_dataset(dataset), layout)
                            if kind == "cache_hit":
                                row.update(
                                    cache_hits_delta=cache.hits - hits,
                                    cache_misses_delta=cache.misses - misses,
                                    cache_max_entries=cache.max_entries,
                                )
                                if (
                                    row["cache_hits_delta"] != 1
                                    or row["cache_misses_delta"] != 0
                                ):
                                    raise RuntimeError(
                                        "expected one content-verified cache hit"
                                    )
                        elif kind == "pickle_protocol5":
                            if dataset is None:
                                raise RuntimeError(
                                    "pickle sample requires verified warm dataset"
                                )
                            serialized = measure_operation(
                                partial(pickle.dumps, dataset, protocol=5)
                            )
                            payload = serialized.value
                            decoded = measure_operation(partial(pickle.loads, payload))
                            row.update(
                                elapsed_seconds=serialized.elapsed_seconds,
                                deserialize_seconds=decoded.elapsed_seconds,
                                memory=serialized.memory,
                                deserialize_memory=decoded.memory,
                                payload_bytes=len(payload),
                                payload_sha256=sha256(payload).hexdigest(),
                                pickle_protocol=5,
                            )
                            _verify_identity(verify_dataset(decoded.value), layout)
                            row["end_to_end_seconds"] = perf_counter() - sample_started
                        else:
                            if payload is None:
                                raise RuntimeError(
                                    "transfer sample requires verified "
                                    "protocol-5 payload"
                                )
                            transferred = transfer_payload(
                                payload, timeout_seconds=timeout_seconds
                            )
                            child = transferred.child
                            _write_record(child, output / "raw" / f"{label}.json")
                            row.update(
                                elapsed_seconds=transferred.transfer_seconds,
                                end_to_end_seconds=transferred.elapsed_seconds,
                                memory=child["memory"],
                                parent_memory=peak_rss(),
                                environment=child["environment"],
                                implementation=child["implementation"],
                                payload_bytes=len(payload),
                                payload_sha256=sha256(payload).hexdigest(),
                                pickle_protocol=5,
                                receive_seconds=child["receive_seconds"],
                                deserialize_seconds=child["deserialize_seconds"],
                                spawn_method="spawn",
                            )
                            _verify_identity(child["identity"], layout)
                            if (
                                child["payload_bytes"] != len(payload)
                                or child["payload_sha256"]
                                != sha256(payload).hexdigest()
                                or child["implementation"] != asdict(implementation)
                            ):
                                raise RuntimeError(
                                    "transfer payload or source differs "
                                    "from frozen workload"
                                )
                        row["economic_sha256"] = report["economic_sha256"]
                        row["verified"] = True
                        row["verified_end_to_end_seconds"] = (
                            perf_counter() - sample_started
                        )
                    except BaseException as error:
                        row["verified"] = False
                        row["error"] = f"{type(error).__name__}: {error}"
                        if isinstance(error, MeasurementFailure):
                            row["measurement_failure"] = error.observations
                            row["end_to_end_seconds"] = error.observations.get(
                                "end_to_end_seconds"
                            )
                            row["returncode"] = error.observations.get("returncode")
                            _write_record(
                                error.observations,
                                output / "raw" / f"{label}.failure.json",
                            )
                        raise
                    finally:
                        persist()
        if current_implementation() != implementation:
            raise RuntimeError(
                "package source changed during benchmark; "
                "freeze source before measuring"
            )
        report.update(
            status="completed",
            finished_utc=datetime.now(UTC).isoformat(),
            summary=summarize(samples),
        )
        persist()
        _write_record(report, output / "report.json")
    except BaseException as error:
        report.update(
            status=(
                "interrupted"
                if isinstance(error, (KeyboardInterrupt, SystemExit))
                else "failed"
            ),
            error=f"{type(error).__name__}: {error}",
        )
        persist()
        raise
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m distributed_backtester.storage_benchmark"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    public = commands.add_parser(
        "run", help="measure storage after freezing package source"
    )
    public.add_argument("--bars", type=int, required=True)
    public.add_argument("--jobs", type=int, required=True)
    public.add_argument("--repetitions", type=int, default=5)
    public.add_argument("--row-group-sizes", type=int, nargs="+", default=[8192, 65536])
    public.add_argument("--seed", type=int, default=1729)
    public.add_argument("--output-dir", type=Path, required=True)
    public.add_argument("--timeout-seconds", type=float, default=3600)
    public.add_argument(
        "--discard-trial-results",
        action="store_true",
        help=(
            "retain verified manifests, indexes, hashes and raw reports; "
            "unlink only owned detailed trial results"
        ),
    )
    options = parser.parse_args(argv)
    try:
        report = run_benchmarks(
            options.output_dir,
            bars=options.bars,
            jobs=options.jobs,
            repetitions=options.repetitions,
            row_group_sizes=options.row_group_sizes,
            seed=options.seed,
            discard_trial_results=options.discard_trial_results,
            timeout_seconds=options.timeout_seconds,
        )
        print(json.dumps(report["summary"], indent=2))
    except (ValueError, OSError, RuntimeError, KeyError, TypeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
