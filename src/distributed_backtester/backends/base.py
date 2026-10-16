"""Streaming execution contract; experiment managers own result publication."""

from collections.abc import Iterator, Sequence
from typing import Protocol

from distributed_backtester.jobs import JobSpec
from distributed_backtester.models import MarketDataset
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.worker import JobOutcome


class ExecutionBackend(Protocol):
    name: str
    workers: int

    def iter_outcomes(
        self,
        jobs: Sequence[JobSpec],
        dataset: MarketDataset,
        implementation: ImplementationIdentity,
    ) -> Iterator[JobOutcome]: ...
