from dataclasses import replace
from typing import NoReturn

import pytest

from distributed_backtester.jobs import create_job
from distributed_backtester.models import BacktestConfig, MarketDataset
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.simulation import ENGINE_ID, run_backtest
from distributed_backtester.worker import JobFailure, JobOutcome, execute_job
from tests.test_simulation import dataset

IDENTITY = ImplementationIdentity(ENGINE_ID, "b" * 64)


def test_worker_calls_reference_engine_with_fresh_state() -> None:
    data = dataset([10, 11, 12, 9, 9])
    job = create_job(BacktestConfig(fee_bps=10), data.metadata, IDENTITY)
    outcome = execute_job(job, data, IDENTITY)
    assert outcome.result == run_backtest(job.config, data)
    assert outcome.failure is None
    assert outcome.job_id == job.job_id
    assert execute_job(job, data, IDENTITY) == outcome


def test_dataset_mismatch_is_one_structured_failure() -> None:
    data = dataset([10])
    job = create_job(BacktestConfig(), replace(data.metadata, symbol="OTHER"), IDENTITY)
    outcome = execute_job(job, data, IDENTITY)
    assert outcome.result is None
    assert outcome.failure is not None
    assert outcome.failure.error_type == "DatasetMismatch"


def test_implementation_mismatch_is_one_structured_failure() -> None:
    data = dataset([10])
    job = create_job(BacktestConfig(), data.metadata, IDENTITY)
    outcome = execute_job(job, data, replace(IDENTITY, source_sha256="c" * 64))
    assert outcome.failure is not None
    assert outcome.failure.error_type == "ImplementationMismatch"


def test_job_outcome_requires_exactly_one_result_or_failure() -> None:
    data = dataset([10])
    job = create_job(BacktestConfig(), data.metadata, IDENTITY)
    failure = JobFailure("SomeError", "explanation")
    with pytest.raises(ValueError):
        JobOutcome(job.job_id, None, None)
    with pytest.raises(ValueError):
        JobOutcome(job.job_id, run_backtest(job.config, data), failure)


def test_ordinary_simulation_exception_isolated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Narrow fault injection tests the worker exception boundary, not process transport.
    def fail(config: BacktestConfig, data: MarketDataset) -> NoReturn:
        raise ValueError("injected strategy failure")

    monkeypatch.setattr("distributed_backtester.worker.run_backtest", fail)
    data = dataset([10])
    job = create_job(BacktestConfig(), data.metadata, IDENTITY)
    assert execute_job(job, data, IDENTITY).failure == JobFailure(
        "ValueError", "injected strategy failure"
    )


def test_interrupt_is_not_converted_into_job_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def interrupt(config: BacktestConfig, data: MarketDataset) -> NoReturn:
        raise KeyboardInterrupt

    monkeypatch.setattr("distributed_backtester.worker.run_backtest", interrupt)
    data = dataset([10])
    job = create_job(BacktestConfig(), data.metadata, IDENTITY)
    with pytest.raises(KeyboardInterrupt):
        execute_job(job, data, IDENTITY)
