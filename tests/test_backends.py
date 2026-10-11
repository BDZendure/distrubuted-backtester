import gc
import os
import time
import weakref
from collections.abc import Callable
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import replace
from multiprocessing.context import BaseContext
from pathlib import Path

import pytest

from distributed_backtester.backends.process import ProcessBackend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.jobs import JobSpec, create_job
from distributed_backtester.models import BacktestConfig, MarketDataset, StrategyConfig
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.worker import JobOutcome
from tests.test_simulation import dataset


@pytest.mark.parametrize("workers,limit", [(1, 1), (2, 3), (4, 8), (8, 16)])
def test_real_spawn_output_matches_sequential_including_failures(
    workers: int, limit: int
) -> None:
    data = dataset([10, 11, 12, 9, 10, 13, 7, 9])
    identity = current_implementation()
    valid = tuple(
        create_job(
            BacktestConfig(StrategyConfig(1, slow), fee_bps=10), data.metadata, identity
        )
        for slow in [2, 3, 4, 5, 6, 7, 8]
    )
    jobs = (
        *valid,
        create_job(BacktestConfig(), replace(data.metadata, symbol="OTHER"), identity),
    )
    before = data
    reference = {
        o.job_id: o for o in SequentialBackend().iter_outcomes(jobs, data, identity)
    }
    backend = ProcessBackend(workers, max_in_flight=limit)
    parallel = {o.job_id: o for o in backend.iter_outcomes(jobs, data, identity)}
    assert reference == parallel
    assert len(parallel) == len(jobs)
    assert sum(o.failure is not None for o in parallel.values()) == 1
    assert data == before
    assert backend.max_observed_in_flight == min(limit, len(jobs))


@pytest.mark.parametrize(
    "workers,limit", [(0, 1), (-1, 1), (True, 1), (2, 0), (2, True)]
)
def test_process_configuration_rejected(workers: int, limit: int) -> None:
    with pytest.raises(ValueError):
        ProcessBackend(workers, max_in_flight=limit)


def test_empty_jobs_do_not_start_work() -> None:
    data = dataset([10])
    identity = current_implementation()
    assert list(SequentialBackend().iter_outcomes((), data, identity)) == []
    assert list(ProcessBackend(2).iter_outcomes((), data, identity)) == []


def test_changed_worker_source_aborts_real_pool() -> None:
    data = dataset([10])
    identity = replace(current_implementation(), source_sha256="f" * 64)
    job = create_job(BacktestConfig(), data.metadata, identity)
    with pytest.raises(BrokenProcessPool):
        list(ProcessBackend(1).iter_outcomes((job,), data, identity))
    with pytest.raises(RuntimeError, match="implementation"):
        list(SequentialBackend().iter_outcomes((job,), data, identity))


def test_early_consumer_close_shuts_down_real_pool() -> None:
    data = dataset([10, 11, 12])
    identity = current_implementation()
    jobs = tuple(
        create_job(BacktestConfig(fee_bps=fee), data.metadata, identity)
        for fee in range(20)
    )
    backend = ProcessBackend(2, max_in_flight=3)
    iterator = backend.iter_outcomes(jobs, data, identity)
    assert next(iterator).result is not None
    iterator.close()
    assert backend.max_observed_in_flight == 3
    assert list(iterator) == []


# Instrumented boundaries still run a real spawn pool and the production worker.
_EVENT_DIRECTORY: Path | None = None


def observed_initialize(
    data: MarketDataset, identity: ImplementationIdentity, directory: Path
) -> None:
    from distributed_backtester.backends.process import _initialize

    global _EVENT_DIRECTORY
    _EVENT_DIRECTORY = directory
    _initialize(data, identity)
    with (directory / f"{os.getpid()}.init").open("a") as stream:
        stream.write("initialized\n")


def observed_execute(job: JobSpec) -> JobOutcome:
    from distributed_backtester.backends.process import _execute

    assert _EVENT_DIRECTORY is not None
    with (_EVENT_DIRECTORY / f"{os.getpid()}.jobs").open("a") as stream:
        stream.write(job.job_id + "\n")
    time.sleep(0.1)  # Keep both test workers busy; this is not a benchmark.
    return _execute(job)


def test_real_worker_reuse_bounds_and_future_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    submitted: list[str] = []
    references: list[weakref.ReferenceType[Future[JobOutcome]]] = []

    def pool_factory(
        *,
        max_workers: int,
        mp_context: BaseContext,
        initializer: Callable[[MarketDataset, ImplementationIdentity], None],
        initargs: tuple[MarketDataset, ImplementationIdentity],
    ) -> ProcessPoolExecutor:
        from distributed_backtester.backends.process import _initialize

        assert initializer is _initialize
        pool = ProcessPoolExecutor(
            max_workers=max_workers,
            mp_context=mp_context,
            initializer=observed_initialize,
            initargs=(*initargs, tmp_path),
        )
        original_submit = pool.submit

        def submit(
            function: Callable[[JobSpec], JobOutcome], job: JobSpec
        ) -> Future[JobOutcome]:
            submitted.append(job.job_id)
            future = original_submit(observed_execute, job)
            references.append(weakref.ref(future))
            return future

        monkeypatch.setattr(pool, "submit", submit)
        return pool

    monkeypatch.setattr(
        "distributed_backtester.backends.process.ProcessPoolExecutor", pool_factory
    )
    data = dataset([10, 11, 12])
    identity = current_implementation()
    jobs = tuple(
        create_job(BacktestConfig(fee_bps=fee), data.metadata, identity)
        for fee in range(12)
    )
    iterator = ProcessBackend(2, max_in_flight=3).iter_outcomes(jobs, data, identity)
    assert next(iterator).result is not None
    assert len(submitted) == 3  # Independent of the backend's instrumentation counter.
    time.sleep(0.2)
    assert len(submitted) == 3  # A paused consumer cannot cause more submissions.
    assert len(list(iterator)) == 11
    assert len(submitted) == 12
    initializations = list(tmp_path.glob("*.init"))
    assert len(initializations) == 2
    assert all(
        path.read_text().splitlines() == ["initialized"] for path in initializations
    )
    counts = [len(path.read_text().splitlines()) for path in tmp_path.glob("*.jobs")]
    assert len(counts) == 2 and min(counts) > 1 and sum(counts) == 12
    gc.collect()
    assert all(reference() is None for reference in references)
