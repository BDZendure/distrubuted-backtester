import json
import pickle
from dataclasses import replace

import pytest

from distributed_backtester.jobs import JobSpec, create_job, expand_grid
from distributed_backtester.models import (
    BacktestConfig,
    DatasetMetadata,
    StrategyConfig,
)
from distributed_backtester.provenance import ImplementationIdentity

METADATA = DatasetMetadata("SYNTH", "a" * 64)
IDENTITY = ImplementationIdentity("daily-test-v1", "b" * 64)


def test_job_round_trips_json_and_pickle() -> None:
    job = create_job(BacktestConfig(fee_bps=10), METADATA, IDENTITY)
    assert JobSpec.from_dict(json.loads(json.dumps(job.to_dict()))) == job
    assert pickle.loads(pickle.dumps(job)) == job
    assert len(job.job_id) == 64
    assert create_job(BacktestConfig(fee_bps=10), METADATA, IDENTITY) == job


@pytest.mark.parametrize(
    "change", ["cash", "fee", "strategy", "dataset", "symbol", "code", "engine"]
)
def test_identity_changes_when_semantic_input_changes(change: str) -> None:
    config = BacktestConfig()
    metadata = METADATA
    identity = IDENTITY
    baseline = create_job(config, metadata, identity)
    if change == "cash":
        config = replace(config, initial_cash=2_000_000_000)
    elif change == "fee":
        config = replace(config, fee_bps=1)
    elif change == "strategy":
        config = replace(config, strategy=StrategyConfig(1, 4))
    elif change == "dataset":
        metadata = replace(metadata, source_sha256="c" * 64)
    elif change == "symbol":
        metadata = replace(metadata, symbol="OTHER")
    elif change == "code":
        identity = replace(identity, source_sha256="c" * 64)
    else:
        identity = replace(identity, engine_id="daily-test-v2")
    assert create_job(config, metadata, identity).job_id != baseline.job_id


def test_corrupted_job_identity_is_rejected() -> None:
    job = create_job(BacktestConfig(), METADATA, IDENTITY)
    with pytest.raises(ValueError, match="job_id"):
        replace(job, job_id="f" * 64)
    record = job.to_dict()
    record["job_id"] = "f" * 64
    with pytest.raises(ValueError, match="job_id"):
        JobSpec.from_dict(record)


@pytest.mark.parametrize("record", [None, [], {}, {"schema_version": 2}])
def test_invalid_record_shape_rejected(record: object) -> None:
    with pytest.raises(ValueError):
        JobSpec.from_dict(record)


def test_unknown_fields_rejected() -> None:
    record = create_job(BacktestConfig(), METADATA, IDENTITY).to_dict()
    record["extra"] = 1
    with pytest.raises(ValueError):
        JobSpec.from_dict(record)


@pytest.mark.parametrize(
    "field,value", [("fee_bps", True), ("initial_cash", "1000"), ("fast_window", 0)]
)
def test_decoding_validates_nested_configuration(field: str, value: object) -> None:
    record = json.loads(
        json.dumps(create_job(BacktestConfig(), METADATA, IDENTITY).to_dict())
    )
    record["config"][field] = value
    with pytest.raises(ValueError):
        JobSpec.from_dict(record)


def test_grid_preserves_membership_and_executes_unique_jobs_once() -> None:
    jobs, membership = expand_grid(
        [1, 2, 1], [3, 4], 1_000_000_000, 10, METADATA, IDENTITY
    )
    assert len(jobs) == 4
    assert len(membership) == 6
    assert membership[:2] == membership[4:]
    assert [job.job_id for job in jobs] == sorted(set(membership))


@pytest.mark.parametrize(
    "fast,slow", [([], [3]), ([1], []), ([2, 3], [3]), ([True], [3])]
)
def test_bad_grid_is_not_silently_filtered(fast: list[int], slow: list[int]) -> None:
    with pytest.raises(ValueError):
        expand_grid(fast, slow, 1_000_000_000, 0, METADATA, IDENTITY)
