"""Bounded spawn execution with one immutable dataset per initialized worker."""

import multiprocessing
from collections.abc import Generator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait

from distributed_backtester.jobs import JobSpec
from distributed_backtester.models import MarketDataset
from distributed_backtester.money import require_integer
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.worker import JobOutcome, execute_job

_DATASET: MarketDataset | None = None
_IMPLEMENTATION: ImplementationIdentity | None = None


def _initialize(dataset: MarketDataset, implementation: ImplementationIdentity) -> None:
    global _DATASET, _IMPLEMENTATION
    if current_implementation() != implementation:
        raise RuntimeError("installed implementation differs in worker initialization")
    _DATASET = dataset
    _IMPLEMENTATION = implementation


def _execute(job: JobSpec) -> JobOutcome:
    if _DATASET is None or _IMPLEMENTATION is None:
        raise RuntimeError("worker was not initialized")
    return execute_job(job, _DATASET, _IMPLEMENTATION)


class ProcessBackend:
    name = "process"

    def __init__(self, workers: int, *, max_in_flight: int | None = None) -> None:
        require_integer(workers, "workers", minimum=1)
        if max_in_flight is None:
            max_in_flight = 2 * workers
        require_integer(max_in_flight, "max_in_flight", minimum=1)
        self.workers = workers
        self.max_in_flight = max_in_flight
        self.max_observed_in_flight = 0

    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
    ) -> Generator[JobOutcome, None, None]:
        self.max_observed_in_flight = 0
        if not jobs:
            return
        iterator = iter(jobs)
        pending: dict[Future[JobOutcome], str] = {}
        exhausted = False
        with ProcessPoolExecutor(
            max_workers=self.workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=_initialize,
            initargs=(dataset, implementation),
        ) as pool:
            try:
                while pending or not exhausted:
                    while len(pending) < self.max_in_flight and not exhausted:
                        job = next(iterator, None)
                        if job is None:
                            exhausted = True
                        else:
                            pending[pool.submit(_execute, job)] = job.job_id
                            self.max_observed_in_flight = max(
                                self.max_observed_in_flight, len(pending)
                            )
                    if not pending:
                        break
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        expected_id = pending.pop(future)
                        outcome = future.result()
                        if outcome.job_id != expected_id:
                            raise RuntimeError(
                                "worker outcome does not match submitted job"
                            )
                        yield outcome
                        del outcome
                    # Finished futures retain their result until references go away.
                    del done, future
            finally:
                for future in pending:
                    future.cancel()
