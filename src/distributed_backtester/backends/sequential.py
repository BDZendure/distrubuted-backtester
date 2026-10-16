"""One-job-at-a-time reference backend."""

from collections.abc import Iterator, Sequence

from distributed_backtester.jobs import JobSpec
from distributed_backtester.models import MarketDataset
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.worker import JobOutcome, execute_job


class SequentialBackend:
    name = "sequential"
    workers = 1

    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
    ) -> Iterator[JobOutcome]:
        if implementation != current_implementation():
            raise RuntimeError("installed implementation changed before execution")
        for job in jobs:
            yield execute_job(job, dataset, implementation)
