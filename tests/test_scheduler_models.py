import json
import pickle
from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from distributed_backtester.jobs import create_job
from distributed_backtester.models import BacktestConfig, DatasetMetadata
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.scheduler.models import (
    AttemptReply,
    AttemptSpec,
    RetryPolicy,
)
from distributed_backtester.worker import JobFailure, JobOutcome

JOB = create_job(
    BacktestConfig(),
    DatasetMetadata("SYNTH", "a" * 64),
    ImplementationIdentity("test-engine", "b" * 64),
)


def test_retry_policy_defaults_and_round_trips() -> None:
    policy = RetryPolicy()
    assert policy.to_dict() == {
        "max_attempts": 3,
        "job_timeout_seconds": 60.0,
        "retry_delay_seconds": 0.1,
    }
    assert RetryPolicy.from_dict(json.loads(json.dumps(policy.to_dict()))) == policy
    assert pickle.loads(pickle.dumps(policy)) == policy
    assert RetryPolicy(retry_delay_seconds=0).retry_delay_seconds == 0
    assert not hasattr(policy, "__dict__")
    with pytest.raises(FrozenInstanceError):
        policy.max_attempts = 4  # type: ignore[misc]


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.0, "3", None])
def test_retry_policy_rejects_invalid_attempt_budget(value: Any) -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        RetryPolicy(max_attempts=value)


@pytest.mark.parametrize(
    "value",
    [True, None, "1", 0, -1, float("nan"), float("inf"), float("-inf"), 10**400],
)
def test_retry_policy_rejects_invalid_timeout(value: Any) -> None:
    with pytest.raises(ValueError, match="job_timeout_seconds"):
        RetryPolicy(job_timeout_seconds=value)


@pytest.mark.parametrize(
    "value", [False, None, "0", -1, float("nan"), float("inf"), float("-inf"), 10**400]
)
def test_retry_policy_rejects_invalid_delay(value: Any) -> None:
    with pytest.raises(ValueError, match="retry_delay_seconds"):
        RetryPolicy(retry_delay_seconds=value)


@pytest.mark.parametrize("value", [None, [], {}, {"max_attempts": 3}])
def test_policy_decoder_requires_exact_fields(value: object) -> None:
    with pytest.raises(ValueError):
        RetryPolicy.from_dict(value)


def test_policy_decoder_rejects_unknown_fields_and_bool_numbers() -> None:
    record = RetryPolicy().to_dict()
    record["extra"] = 1
    with pytest.raises(ValueError):
        RetryPolicy.from_dict(record)
    record = RetryPolicy().to_dict()
    record["job_timeout_seconds"] = True
    with pytest.raises(ValueError):
        RetryPolicy.from_dict(record)


def test_attempt_is_immutable_and_picklable() -> None:
    attempt = AttemptSpec(JOB, 1, 2, f"{JOB.job_id}:1", 100.0)
    assert pickle.loads(pickle.dumps(attempt)) == attempt
    assert not hasattr(attempt, "__dict__")
    with pytest.raises(FrozenInstanceError):
        attempt.generation = 3  # type: ignore[misc]


@pytest.mark.parametrize(
    "field,value",
    [
        ("job", None),
        ("number", True),
        ("number", 0),
        ("number", 1.0),
        ("generation", False),
        ("generation", 0),
        ("generation", "1"),
        ("attempt_id", "wrong"),
        ("attempt_id", f"{JOB.job_id}:01"),
        ("deadline_utc", True),
        ("deadline_utc", float("inf")),
        ("deadline_utc", float("nan")),
        ("deadline_utc", "100"),
    ],
)
def test_attempt_rejects_invalid_lease(field: str, value: Any) -> None:
    attempt = AttemptSpec(JOB, 1, 1, f"{JOB.job_id}:1", 100)
    with pytest.raises(ValueError):
        replace(attempt, **{field: value})


def test_reply_distinguishes_simulation_failure_from_infrastructure() -> None:
    failure = JobFailure("TestFailure", "simulation failed")
    outcome = JobOutcome(JOB.job_id, None, failure)
    simulation = AttemptReply(f"{JOB.job_id}:1", 1, outcome, None)
    infrastructure = AttemptReply(f"{JOB.job_id}:1", 1, None, failure, "detail")
    assert simulation.outcome == outcome and simulation.failure is None
    assert infrastructure.outcome is None and infrastructure.failure == failure
    assert pickle.loads(pickle.dumps(infrastructure)) == infrastructure
    assert not hasattr(infrastructure, "__dict__")


@pytest.mark.parametrize(
    "outcome,failure",
    [
        (None, None),
        (JobOutcome(JOB.job_id, None, JobFailure("X", "x")), JobFailure("X", "x")),
        ("not an outcome", None),
        (None, "not a failure"),
    ],
)
def test_reply_requires_exactly_one_typed_payload(outcome: Any, failure: Any) -> None:
    with pytest.raises(ValueError):
        AttemptReply(f"{JOB.job_id}:1", 1, outcome, failure)


@pytest.mark.parametrize(
    "field,value",
    [
        ("attempt_id", ""),
        ("attempt_id", 1),
        ("generation", True),
        ("generation", 0),
        ("diagnostic", 1),
    ],
)
def test_reply_rejects_invalid_envelope(field: str, value: Any) -> None:
    reply = AttemptReply(f"{JOB.job_id}:1", 1, None, JobFailure("X", "x"))
    with pytest.raises(ValueError):
        replace(reply, **{field: value})
