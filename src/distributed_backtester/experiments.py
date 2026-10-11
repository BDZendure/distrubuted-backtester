"""Stream detailed artifacts and finalize a deterministic compact index."""

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.experiment_artifacts import (
    index_record,
    manifest_record,
    outcome_artifact,
)
from distributed_backtester.jobs import JobSpec, canonical_json, expand_grid
from distributed_backtester.market_data import DataFormat, DatasetCache, load_dataset
from distributed_backtester.models import MarketDataset
from distributed_backtester.provenance import (
    ImplementationIdentity,
    capture_environment,
    current_implementation,
)
from distributed_backtester.results import write_text


@dataclass(frozen=True, slots=True)
class ExperimentSummary:
    experiment_id: str
    submitted: int
    succeeded: int
    failed: int
    index_path: Path
    execution_path: Path
    elapsed_seconds: float


def collect_experiment(
    jobs: Sequence[JobSpec],
    dataset: MarketDataset,
    implementation: ImplementationIdentity,
    output: Path,
    backend: ExecutionBackend,
    *,
    membership: Sequence[str] | None = None,
    started: float | None = None,
    source_path: Path | None = None,
    setup_timings: dict[str, float] | None = None,
) -> ExperimentSummary:
    """Publish index.json last, only after all expected outcomes are exported.

    Existing directories are refused. A fatal backend/protocol/export error
    leaves partial artifacts without an index; ordinary job failures receive
    artifacts and count toward a complete experiment. No retries or recovery
    are promised here. Full result retention is bounded by backend buffering.
    Job definitions, membership, and compact records remain O(number of jobs).
    """
    start = perf_counter() if started is None else started
    manifest_started = perf_counter()
    expected = {job.job_id: job for job in jobs}
    manifest = manifest_record(jobs, dataset.metadata, implementation, membership)
    experiment_id = str(manifest["experiment_id"])
    output.mkdir(parents=True, exist_ok=False)
    write_text(canonical_json(manifest) + "\n", output / "manifest.json")
    records: dict[str, dict[str, object]] = {}
    timings = dict(setup_timings or {})
    timings["manifest_prepare_export_seconds"] = perf_counter() - manifest_started
    backend_seconds = export_seconds = 0.0
    first_seconds: float | None = None
    collection_started = perf_counter()
    outcomes = backend.iter_outcomes(jobs, dataset, implementation)
    try:
        while True:
            waiting = perf_counter()
            try:
                outcome = next(outcomes)
            except StopIteration:
                backend_seconds += perf_counter() - waiting
                break
            backend_seconds += perf_counter() - waiting
            if first_seconds is None:
                first_seconds = perf_counter() - collection_started
            exporting = perf_counter()
            if outcome.job_id not in expected or outcome.job_id in records:
                raise RuntimeError(f"unknown or duplicate outcome: {outcome.job_id}")
            job = expected[outcome.job_id]
            content, record = outcome_artifact(job, outcome)
            write_text(content, output / str(record["artifact"]))
            records[outcome.job_id] = record
            del content, outcome
            export_seconds += perf_counter() - exporting
    finally:
        close = getattr(outcomes, "close", None)
        if callable(close):
            close()
    if set(records) != set(expected):
        raise RuntimeError("backend did not return every submitted job")
    succeeded = sum(record["status"] == "succeeded" for record in records.values())
    elapsed = perf_counter() - start
    timings["backend_next_seconds"] = backend_seconds
    timings["artifact_export_seconds"] = export_seconds
    execution_path = output / "execution.json"
    write_text(
        canonical_json(
            {
                "schema_version": 1,
                "experiment_id": experiment_id,
                "backend": backend.name,
                "workers": backend.workers,
                "max_in_flight": getattr(backend, "max_in_flight", None),
                "max_observed_in_flight": getattr(
                    backend, "max_observed_in_flight", None
                ),
                "backend_diagnostics": getattr(backend, "diagnostics", None),
                "elapsed_before_finalization_seconds": elapsed,
                "timings": {**timings, "time_to_first_outcome_seconds": first_seconds},
                "source_path": str(source_path.resolve()) if source_path else None,
                "environment": capture_environment(),
            }
        )
        + "\n",
        execution_path,
    )
    index_path = output / "index.json"
    write_text(
        canonical_json(index_record(experiment_id, list(records.values()))) + "\n",
        index_path,
    )
    return ExperimentSummary(
        experiment_id,
        len(jobs),
        succeeded,
        len(jobs) - succeeded,
        index_path,
        execution_path,
        perf_counter() - start,
    )


def run_experiment(
    data: Path,
    *,
    symbol: str,
    fast: Sequence[int],
    slow: Sequence[int],
    initial_cash: int,
    fee_bps: int,
    output: Path,
    backend: ExecutionBackend,
    data_format: DataFormat = "auto",
    expected_version: str | None = None,
    cache: DatasetCache | None = None,
) -> ExperimentSummary:
    started = perf_counter()
    dataset = load_dataset(
        data,
        symbol=symbol,
        price_basis="raw",
        data_format=data_format,
        expected_version=expected_version,
        cache=cache,
    )
    loaded = perf_counter()
    implementation = current_implementation()
    jobs, membership = expand_grid(
        fast,
        slow,
        initial_cash=initial_cash,
        fee_bps=fee_bps,
        metadata=dataset.metadata,
        implementation=implementation,
    )
    return collect_experiment(
        jobs,
        dataset,
        implementation,
        output,
        backend,
        membership=membership,
        started=started,
        source_path=data,
        setup_timings={
            f"{dataset.metadata.source_format}_load_seconds": loaded - started,
            "prepare_jobs_seconds": perf_counter() - loaded,
        },
    )
