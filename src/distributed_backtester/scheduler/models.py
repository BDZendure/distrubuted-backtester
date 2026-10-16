"""Immutable retry policy, attempt leases, and asynchronous runtime protocol."""

import math
from dataclasses import dataclass
from typing import Protocol, cast

from distributed_backtester.jobs import JobSpec
from distributed_backtester.worker import JobFailure, JobOutcome


def _positive_integer(value: object, name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def finite_number(value: object, name: str) -> float:
    """Reject booleans, strings, nonfinite numbers, and float overflow."""
    if type(value) not in (int, float):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = float(cast(int | float, value))
    except OverflowError as error:
        raise ValueError(f"{name} must be a finite number") from error
    if not math.isfinite(number):
        raise ValueError(f"{name} must be a finite number")
    return number


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    job_timeout_seconds: float = 60.0
    retry_delay_seconds: float = 0.1

    def __post_init__(self) -> None:
        _positive_integer(self.max_attempts, "max_attempts")
        timeout = finite_number(self.job_timeout_seconds, "job_timeout_seconds")
        delay = finite_number(self.retry_delay_seconds, "retry_delay_seconds")
        if timeout <= 0:
            raise ValueError("job_timeout_seconds must be positive")
        if delay < 0:
            raise ValueError("retry_delay_seconds must be nonnegative")

    def to_dict(self) -> dict[str, object]:
        return {
            "max_attempts": self.max_attempts,
            "job_timeout_seconds": self.job_timeout_seconds,
            "retry_delay_seconds": self.retry_delay_seconds,
        }

    @classmethod
    def from_dict(cls, value: object) -> "RetryPolicy":
        if not isinstance(value, dict) or set(value) != {
            "max_attempts",
            "job_timeout_seconds",
            "retry_delay_seconds",
        }:
            raise ValueError("retry policy must contain exactly its three fields")
        return cls(
            _positive_integer(value["max_attempts"], "max_attempts"),
            finite_number(value["job_timeout_seconds"], "job_timeout_seconds"),
            finite_number(value["retry_delay_seconds"], "retry_delay_seconds"),
        )


@dataclass(frozen=True, slots=True)
class AttemptSpec:
    job: JobSpec
    number: int
    generation: int
    attempt_id: str
    deadline_utc: float

    def __post_init__(self) -> None:
        if not isinstance(self.job, JobSpec):
            raise ValueError("attempt job must be a JobSpec")
        _positive_integer(self.number, "number")
        _positive_integer(self.generation, "generation")
        if self.attempt_id != f"{self.job.job_id}:{self.number}":
            raise ValueError("attempt_id does not match job and attempt number")
        finite_number(self.deadline_utc, "deadline_utc")


@dataclass(frozen=True, slots=True)
class AttemptReply:
    attempt_id: str
    generation: int
    outcome: JobOutcome | None
    failure: JobFailure | None
    diagnostic: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.attempt_id, str) or not self.attempt_id:
            raise ValueError("reply attempt_id must be nonempty text")
        _positive_integer(self.generation, "generation")
        if (self.outcome is None) == (self.failure is None):
            raise ValueError("reply requires exactly one outcome or failure")
        if self.outcome is not None and not isinstance(self.outcome, JobOutcome):
            raise ValueError("reply outcome must be a JobOutcome")
        if self.failure is not None and not isinstance(self.failure, JobFailure):
            raise ValueError("reply failure must be a JobFailure")
        if self.diagnostic is not None and not isinstance(self.diagnostic, str):
            raise ValueError("reply diagnostic must be text or None")


class AttemptRuntime(Protocol):
    """Single-use runtime with bounded, one-call-per-actor capacity."""

    name: str
    workers: int
    capacity: int
    diagnostics: dict[str, object]

    def start(self) -> None: ...

    def submit(self, attempt: AttemptSpec) -> None: ...

    def poll(self, timeout: float) -> tuple[AttemptReply, ...]: ...

    def cancel(self, attempt_id: str) -> None: ...

    def close(self) -> None: ...
