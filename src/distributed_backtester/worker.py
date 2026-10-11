"""Execute one independent job without owning scheduling or persistence."""

import re
from dataclasses import dataclass

from distributed_backtester.jobs import JobSpec
from distributed_backtester.models import BacktestResult, MarketDataset
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.simulation import ENGINE_ID, run_backtest


@dataclass(frozen=True, slots=True)
class JobFailure:
    error_type: str
    message: str

    def __post_init__(self) -> None:
        if not isinstance(self.error_type, str) or not self.error_type:
            raise ValueError("failure error_type must be nonempty text")
        if not isinstance(self.message, str):
            raise ValueError("failure message must be text")


@dataclass(frozen=True, slots=True)
class JobOutcome:
    job_id: str
    result: BacktestResult | None
    failure: JobFailure | None

    def __post_init__(self) -> None:
        if (
            not isinstance(self.job_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", self.job_id) is None
        ):
            raise ValueError("outcome job_id must be a lowercase SHA-256 digest")
        if (self.result is None) == (self.failure is None):
            raise ValueError("outcome requires exactly one result or failure")


def execute_job(
    job: JobSpec, dataset: MarketDataset, implementation: ImplementationIdentity
) -> JobOutcome:
    if job.dataset != dataset.metadata:
        return JobOutcome(
            job.job_id,
            None,
            JobFailure(
                "DatasetMismatch", "job metadata does not match initialized data"
            ),
        )
    if job.implementation != implementation or implementation.engine_id != ENGINE_ID:
        return JobOutcome(
            job.job_id,
            None,
            JobFailure(
                "ImplementationMismatch",
                "job implementation does not match initialized engine",
            ),
        )
    try:
        result = run_backtest(job.config, dataset)
    except Exception as error:
        return JobOutcome(
            job.job_id, None, JobFailure(type(error).__name__, str(error))
        )
    return JobOutcome(job.job_id, result, None)
