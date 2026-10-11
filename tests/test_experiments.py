import json
from collections.abc import Iterator, Sequence
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from distributed_backtester.backends.base import ExecutionBackend
from distributed_backtester.backends.process import ProcessBackend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.experiments import collect_experiment, run_experiment
from distributed_backtester.jobs import JobSpec, create_job
from distributed_backtester.models import BacktestConfig, MarketDataset
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.worker import JobOutcome, execute_job
from tests.test_simulation import dataset


def test_real_sweeps_export_identical_semantic_artifacts(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "examples/sma.csv"
    backends: list[tuple[str, ExecutionBackend]] = [
        ("seq", SequentialBackend()),
        ("proc", ProcessBackend(2)),
    ]
    summaries = [
        run_experiment(
            source,
            symbol="SYNTH",
            fast=[1, 2, 1],
            slow=[3, 4],
            initial_cash=1_000_000_000,
            fee_bps=10,
            output=tmp_path / name,
            backend=backend,
        )
        for name, backend in backends
    ]
    first, second = summaries
    assert first.submitted == second.submitted == 4
    assert first.succeeded == second.succeeded == 4
    assert first.failed == second.failed == 0
    assert (tmp_path / "seq/manifest.json").read_bytes() == (
        tmp_path / "proc/manifest.json"
    ).read_bytes()
    assert first.index_path.read_bytes() == second.index_path.read_bytes()
    manifest = json.loads((tmp_path / "seq/manifest.json").read_text())
    assert len(manifest["grid_membership"]) == 6
    index = json.loads(first.index_path.read_text())
    ids = [record["job_id"] for record in index["outcomes"]]
    assert ids == sorted(ids)
    for record in index["outcomes"]:
        artifact = tmp_path / "seq" / record["artifact"]
        assert sha256(artifact.read_bytes()).hexdigest() == record["content_sha256"]
        assert (
            artifact.read_bytes()
            == (tmp_path / "proc" / record["artifact"]).read_bytes()
        )
        assert "equity" not in record
    assert json.loads(first.execution_path.read_text())["backend"] == "sequential"
    assert json.loads(second.execution_path.read_text())["backend"] == "process"
    timings = json.loads(first.execution_path.read_text())["timings"]
    for key in [
        "csv_load_seconds",
        "prepare_jobs_seconds",
        "backend_next_seconds",
        "artifact_export_seconds",
        "time_to_first_outcome_seconds",
    ]:
        assert timings[key] >= 0


def test_mixed_success_failure_exports_explicit_counts(tmp_path: Path) -> None:
    data = dataset([10, 11, 12])
    identity = current_implementation()
    jobs = (
        create_job(BacktestConfig(), data.metadata, identity),
        create_job(BacktestConfig(), replace(data.metadata, symbol="OTHER"), identity),
    )
    summary = collect_experiment(
        jobs, data, identity, tmp_path / "mixed", SequentialBackend()
    )
    assert (summary.succeeded, summary.failed) == (1, 1)
    index = json.loads(summary.index_path.read_text())
    failure = next(
        record for record in index["outcomes"] if record["status"] == "failed"
    )
    assert failure["failure"]["error_type"] == "DatasetMismatch"
    assert failure["metrics"] is None


class InvalidBackend:
    name = "invalid-test-backend"
    workers = 1

    def __init__(self, mode: str) -> None:
        self.mode = mode

    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        data: MarketDataset,
        identity: ImplementationIdentity,
    ) -> Iterator[JobOutcome]:
        outcome = execute_job(jobs[0], data, identity)
        if self.mode == "missing":
            return
        if self.mode == "unknown":
            outcome = replace(outcome, job_id="f" * 64)
        if self.mode == "wrong_result":
            assert outcome.result is not None
            outcome = replace(
                outcome,
                result=replace(outcome.result, config=BacktestConfig(fee_bps=1)),
            )
        if self.mode == "wrong_schema":
            assert outcome.result is not None
            outcome = replace(
                outcome, result=replace(outcome.result, schema_version=99)
            )
        yield outcome
        if self.mode == "duplicate":
            yield outcome


@pytest.mark.parametrize(
    "mode", ["missing", "unknown", "duplicate", "wrong_result", "wrong_schema"]
)
def test_invalid_backend_output_cannot_finalize_index(
    tmp_path: Path, mode: str
) -> None:
    data = dataset([10])
    identity = current_implementation()
    job = create_job(BacktestConfig(), data.metadata, identity)
    output = tmp_path / "incomplete"
    with pytest.raises(RuntimeError):
        collect_experiment((job,), data, identity, output, InvalidBackend(mode))
    assert not (output / "index.json").exists()


def test_existing_output_directory_is_protected(tmp_path: Path) -> None:
    data = dataset([10])
    identity = current_implementation()
    job = create_job(BacktestConfig(), data.metadata, identity)
    with pytest.raises(FileExistsError):
        collect_experiment((job,), data, identity, tmp_path, SequentialBackend())
    assert list(tmp_path.iterdir()) == []


def test_invalid_grid_creates_no_output(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "examples/sma.csv"
    output = tmp_path / "bad"
    with pytest.raises(ValueError):
        run_experiment(
            source,
            symbol="SYNTH",
            fast=[3],
            slow=[3],
            initial_cash=1,
            fee_bps=0,
            output=output,
            backend=SequentialBackend(),
        )
    assert not output.exists()


class ReversedBackend(SequentialBackend):
    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        data: MarketDataset,
        identity: ImplementationIdentity,
    ) -> Iterator[JobOutcome]:
        yield from super().iter_outcomes(tuple(reversed(jobs)), data, identity)


def test_reversed_completion_produces_identical_index(tmp_path: Path) -> None:
    data = dataset([10, 11, 12, 9])
    identity = current_implementation()
    jobs = tuple(
        create_job(BacktestConfig(fee_bps=fee), data.metadata, identity)
        for fee in [0, 10, 25]
    )
    normal = collect_experiment(
        jobs, data, identity, tmp_path / "normal", SequentialBackend()
    )
    reverse = collect_experiment(
        jobs, data, identity, tmp_path / "reverse", ReversedBackend()
    )
    assert normal.index_path.read_bytes() == reverse.index_path.read_bytes()


def test_artifact_write_failure_does_not_finalize_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from distributed_backtester.results import write_text

    def injected(text: str, path: Path, *, overwrite: bool = False) -> None:
        if path.parent.name == "results":
            raise OSError("injected artifact write failure")
        write_text(text, path, overwrite=overwrite)

    monkeypatch.setattr("distributed_backtester.experiments.write_text", injected)
    data = dataset([10])
    identity = current_implementation()
    jobs = (create_job(BacktestConfig(), data.metadata, identity),)
    output = tmp_path / "incomplete"
    with pytest.raises(OSError, match="artifact write"):
        collect_experiment(jobs, data, identity, output, SequentialBackend())
    assert (output / "manifest.json").exists()
    assert not (output / "index.json").exists()
