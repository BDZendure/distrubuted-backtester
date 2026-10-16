"""Actual format-aware worker parity and durable Parquet recovery."""

from pathlib import Path

import pytest

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.backends.process import ProcessBackend
from distributed_backtester.backends.ray import RayBackend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.benchmark import semantic_digest
from distributed_backtester.data import load_csv
from distributed_backtester.experiments import run_experiment
from distributed_backtester.scheduler.experiments import (
    resume_experiment,
    start_experiment,
)
from distributed_backtester.scheduler.journal import read_status
from distributed_backtester.scheduler.models import RetryPolicy


def test_real_parquet_backends_and_durable_resume(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    pytest.importorskip("ray")
    from distributed_backtester.parquet_data import write_parquet

    path = tmp_path / "history.parquet"
    data = load_csv(
        Path(__file__).parents[1] / "examples/sma.csv",
        symbol="SYNTH",
        price_basis="raw",
    )
    metadata = write_parquet(data, path)
    backends: list[tuple[str, ExecutionBackend]] = [
        ("sequential", SequentialBackend()),
        ("process", ProcessBackend(2)),
        ("ray", RayBackend(2)),
    ]
    for name, backend in backends:
        summary = run_experiment(
            path,
            symbol="SYNTH",
            fast=[1, 2],
            slow=[3, 4],
            initial_cash=1_000_000_000,
            fee_bps=10,
            output=tmp_path / name,
            backend=backend,
            expected_version=metadata.version_id,
        )
        assert summary.succeeded == 4
    durable = tmp_path / "durable"
    summary = start_experiment(
        path,
        symbol="SYNTH",
        fast=[1, 2],
        slow=[3, 4],
        initial_cash=1_000_000_000,
        fee_bps=10,
        output=durable,
        backend=RayBackend(2),
        policy=RetryPolicy(),
        expected_version=metadata.version_id,
    )
    assert summary.succeeded == 4
    before = read_status(durable)
    (durable / "index.json").unlink()
    assert resume_experiment(path, output=durable, backend=RayBackend(2)).succeeded == 4
    assert read_status(durable)["attempts"] == before["attempts"]
    assert (
        len(
            {
                semantic_digest(tmp_path / name)
                for name in ("sequential", "process", "ray", "durable")
            }
        )
        == 1
    )
