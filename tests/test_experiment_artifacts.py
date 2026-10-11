"""Shared semantic records stay independent of transport and recovery."""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import (
    index_record,
    manifest_record,
    outcome_artifact,
    validate_manifest,
)
from distributed_backtester.jobs import canonical_json, expand_grid
from distributed_backtester.money import parse_money
from distributed_backtester.provenance import current_implementation
from distributed_backtester.worker import JobFailure, JobOutcome, execute_job


def test_manifest_round_trip_and_digest() -> None:
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    implementation = current_implementation()
    jobs, membership = expand_grid(
        [1, 2, 2], [3], parse_money("1000"), 10, data.metadata, implementation
    )
    manifest = manifest_record(jobs, data.metadata, implementation, membership)
    restored, restored_jobs = validate_manifest(manifest)
    assert restored == manifest
    assert restored_jobs == jobs
    assert len(manifest["grid_membership"]) == 3  # type: ignore[arg-type]
    bad = deepcopy(manifest)
    bad["experiment_id"] = "0" * 64
    with pytest.raises(ValueError, match="experiment ID"):
        validate_manifest(bad)
    bad = deepcopy(manifest)
    bad["schema_version"] = True
    with pytest.raises(ValueError, match="schema"):
        validate_manifest(bad)


def test_artifact_validation_and_index() -> None:
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    implementation = current_implementation()
    jobs, _ = expand_grid(
        [2], [3], parse_money("1000"), 10, data.metadata, implementation
    )
    job = jobs[0]
    outcome = execute_job(job, data, implementation)
    content, record = outcome_artifact(job, outcome)
    assert content.endswith("\n")
    assert record["status"] == "succeeded"
    assert index_record("a" * 64, [record])["succeeded"] == 1
    assert outcome.result is not None
    wrong = replace(outcome, result=replace(outcome.result, engine_id="wrong"))
    with pytest.raises(RuntimeError, match="does not match"):
        outcome_artifact(job, wrong)
    failure = JobOutcome(job.job_id, None, JobFailure("Expected", "test"))
    _failed_content, failed_record = outcome_artifact(job, failure)
    assert failed_record["failure"] == {"error_type": "Expected", "message": "test"}
    assert failed_record["metrics"] is None
    assert index_record("a" * 64, [failed_record])["failed"] == 1
    assert canonical_json({"x": 1}) == '{"x":1}'


def test_manifest_rejects_mismatched_dataset_and_membership() -> None:
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    implementation = current_implementation()
    jobs, _ = expand_grid([2], [3], 1, 0, data.metadata, implementation)
    with pytest.raises(ValueError, match="membership"):
        manifest_record(jobs, data.metadata, implementation, [])
    mismatched = manifest_record(
        jobs, replace(data.metadata, symbol="WRONG"), implementation
    )
    with pytest.raises(ValueError, match=r"experiment ID|dataset"):
        validate_manifest(mismatched)


@pytest.mark.parametrize("replacement", [True, 1.0])
def test_manifest_checks_exact_supplied_payload_digest(replacement: object) -> None:
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    identity = current_implementation()
    jobs, _ = expand_grid([2], [3], 1, 0, data.metadata, identity)
    manifest = manifest_record(jobs, data.metadata, identity)
    dataset_record = manifest["dataset"]
    assert isinstance(dataset_record, dict)
    dataset_record["validation_version"] = replacement
    with pytest.raises(ValueError, match=r"digest|experiment ID|dataset"):
        validate_manifest(manifest)
