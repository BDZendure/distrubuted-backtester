"""Create and resume durable Ray sweeps without changing simulation semantics."""

from collections.abc import Sequence
from pathlib import Path
from time import time

from distributed_backtester.backends.ray import RayBackend
from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import (
    manifest_record,
    validate_manifest,
)
from distributed_backtester.experiments import ExperimentSummary
from distributed_backtester.jobs import expand_grid
from distributed_backtester.provenance import current_implementation
from distributed_backtester.scheduler.coordinator import run_durable
from distributed_backtester.scheduler.journal import Journal, read_manifest
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.scheduler.ray_runtime import RayAttemptRuntime


def _check_input(data: Path, output: Path) -> None:
    if data.resolve().is_relative_to(output.resolve()):
        raise ValueError("input CSV must be outside the durable experiment directory")


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
) -> ExperimentSummary:
    _check_input(data, output)
    dataset = load_csv(data, symbol=symbol, price_basis="raw")
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
    data: Path, *, output: Path, backend: RayBackend
) -> ExperimentSummary:
    _check_input(data, output)
    _manifest, jobs = validate_manifest(read_manifest(output))
    dataset = load_csv(data, symbol=jobs[0].dataset.symbol, price_basis="raw")
    identity = current_implementation()
    with Journal.resume(
        output,
        dataset_metadata=dataset.metadata,
        implementation=identity,
        now=time(),
    ) as journal:
        return run_durable(journal, RayAttemptRuntime(backend, dataset, identity))
