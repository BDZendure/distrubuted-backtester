"""Create and resume durable Ray sweeps without changing simulation semantics."""

from collections.abc import Sequence
from pathlib import Path
from time import time

from distributed_backtester.backends.ray import RayBackend
from distributed_backtester.experiment_artifacts import (
    manifest_record,
    validate_manifest,
)
from distributed_backtester.experiments import ExperimentSummary
from distributed_backtester.jobs import expand_grid
from distributed_backtester.market_data import DataFormat, load_dataset
from distributed_backtester.provenance import current_implementation
from distributed_backtester.scheduler.coordinator import run_durable
from distributed_backtester.scheduler.journal import Journal, read_manifest
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.scheduler.ray_runtime import RayAttemptRuntime


def _check_input(data: Path, output: Path) -> None:
    if data.resolve().is_relative_to(output.resolve()):
        raise ValueError(
            "input dataset must be outside the durable experiment directory"
        )


def start_experiment(
    data: Path,
    *,
    symbol: str,
    fast: Sequence[int],
    slow: Sequence[int],
    initial_cash: int,
    fee_bps: int,
    output: Path,
    backend: RayBackend,
    policy: RetryPolicy,
    data_format: DataFormat = "auto",
    expected_version: str | None = None,
) -> ExperimentSummary:
    _check_input(data, output)
    dataset = load_dataset(
        data,
        symbol=symbol,
        price_basis="raw",
        data_format=data_format,
        expected_version=expected_version,
    )
    identity = current_implementation()
    jobs, membership = expand_grid(
        fast,
        slow,
        initial_cash,
        fee_bps,
        dataset.metadata,
        identity,
    )
    manifest = manifest_record(jobs, dataset.metadata, identity, membership)
    with Journal.create(output, manifest, policy, now=time()) as journal:
        return run_durable(journal, RayAttemptRuntime(backend, dataset, identity))


def resume_experiment(
    data: Path,
    *,
    output: Path,
    backend: RayBackend,
    data_format: DataFormat = "auto",
    expected_version: str | None = None,
) -> ExperimentSummary:
    _check_input(data, output)
    _manifest, jobs = validate_manifest(read_manifest(output))
    selected = jobs[0].dataset.source_format if data_format == "auto" else data_format
    dataset = load_dataset(
        data,
        symbol=jobs[0].dataset.symbol,
        price_basis="raw",
        data_format=selected,
        expected_version=expected_version,
    )
    identity = current_implementation()
    with Journal.resume(
        output,
        dataset_metadata=dataset.metadata,
        implementation=identity,
        now=time(),
    ) as journal:
        return run_durable(journal, RayAttemptRuntime(backend, dataset, identity))
