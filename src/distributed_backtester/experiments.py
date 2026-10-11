"""Stream detailed artifacts and finalize a deterministic compact index."""

from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from time import perf_counter

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.data import load_csv
from distributed_backtester.jobs import (
    JobSpec,
    canonical_json,
    expand_grid,
    metadata_record,
)
from distributed_backtester.models import MarketDataset
from distributed_backtester.provenance import (
    ImplementationIdentity,
    capture_environment,
    current_implementation,
)
from distributed_backtester.results import metrics_record, result_to_dict, write_text
from distributed_backtester.simulation import SMA_ID
from distributed_backtester.worker import JobOutcome


@dataclass(frozen=True, slots=True)
class ExperimentSummary:
    experiment_id: str
    submitted: int
    succeeded: int
    failed: int
    index_path: Path
    execution_path: Path
    elapsed_seconds: float


def _validate_outcome(outcome: JobOutcome, job: JobSpec) -> None:
    result = outcome.result
    if result is not None and (
        result.config != job.config
        or result.dataset_metadata != job.dataset
        or result.engine_id != job.implementation.engine_id
        or result.strategy_id != SMA_ID
        or type(result.schema_version) is not int
        or result.schema_version != 1
    ):
        raise RuntimeError(f"result does not match job {job.job_id}")


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
    if not jobs or len(expected) != len(jobs):
        raise ValueError("an experiment needs nonempty, unique job IDs")
    members = list(expected) if membership is None else list(membership)
    if set(members) != set(expected):
        raise ValueError("membership must reference all and only submitted jobs")
    ordered_jobs = [expected[key].to_dict() for key in sorted(expected)]
    payload = {
        "schema_version": 1,
        "dataset": metadata_record(dataset.metadata),
        "implementation": {
            "engine_id": implementation.engine_id,
            "source_sha256": implementation.source_sha256,
        },
        "jobs": ordered_jobs,
        "grid_membership": members,
    }
    experiment_id = sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    output.mkdir(parents=True, exist_ok=False)
    write_text(
        canonical_json({"experiment_id": experiment_id, **payload}) + "\n",
        output / "manifest.json",
    )
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
            _validate_outcome(outcome, job)
            failure = (
                {
                    "error_type": outcome.failure.error_type,
                    "message": outcome.failure.message,
                }
                if outcome.failure is not None
                else None
            )
            status = "succeeded" if outcome.result is not None else "failed"
            artifact = f"results/{outcome.job_id}.json"
            content = (
                canonical_json(
                    {
                        "schema_version": 1,
                        "job": job.to_dict(),
                        "status": status,
                        "result": (
                            result_to_dict(outcome.result)
                            if outcome.result is not None
                            else None
                        ),
                        "failure": failure,
                    }
                )
                + "\n"
            )
            write_text(content, output / artifact)
            records[outcome.job_id] = {
                "job_id": outcome.job_id,
                "status": status,
                "artifact": artifact,
                "content_sha256": sha256(content.encode("utf-8")).hexdigest(),
                "metrics": (
                    metrics_record(outcome.result.metrics)
                    if outcome.result is not None
                    else None
                ),
                "failure": failure,
            }
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
        canonical_json(
            {
                "schema_version": 1,
                "experiment_id": experiment_id,
                "submitted": len(jobs),
                "succeeded": succeeded,
                "failed": len(jobs) - succeeded,
                "outcomes": [records[key] for key in sorted(records)],
            }
        )
        + "\n",
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
) -> ExperimentSummary:
    started = perf_counter()
    dataset = load_csv(data, symbol=symbol, price_basis="raw")
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
            "csv_load_seconds": loaded - started,
            "prepare_jobs_seconds": perf_counter() - loaded,
        },
    )
