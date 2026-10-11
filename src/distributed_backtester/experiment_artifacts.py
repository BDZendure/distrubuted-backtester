"""Semantic experiment serialization shared by classic and durable execution."""

from collections.abc import Sequence
from hashlib import sha256

from distributed_backtester.jobs import JobSpec, canonical_json, metadata_record
from distributed_backtester.models import DatasetMetadata
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.results import metrics_record, result_to_dict
from distributed_backtester.simulation import SMA_ID
from distributed_backtester.worker import JobOutcome


def manifest_record(
    jobs: Sequence[JobSpec],
    metadata: DatasetMetadata,
    implementation: ImplementationIdentity,
    membership: Sequence[str] | None = None,
) -> dict[str, object]:
    expected = {job.job_id: job for job in jobs}
    if not jobs or len(expected) != len(jobs):
        raise ValueError("an experiment needs nonempty, unique job IDs")
    members = list(expected) if membership is None else list(membership)
    if set(members) != set(expected):
        raise ValueError("membership must reference all and only submitted jobs")
    payload: dict[str, object] = {
        "schema_version": 1,
        "dataset": metadata_record(metadata),
        "implementation": {
            "engine_id": implementation.engine_id,
            "source_sha256": implementation.source_sha256,
        },
        "jobs": [expected[key].to_dict() for key in sorted(expected)],
        "grid_membership": members,
    }
    experiment_id = sha256(canonical_json(payload).encode("utf-8")).hexdigest()
    return {"experiment_id": experiment_id, **payload}


def validate_manifest(value: object) -> tuple[dict[str, object], tuple[JobSpec, ...]]:
    if not isinstance(value, dict) or set(value) != {
        "experiment_id",
        "schema_version",
        "dataset",
        "implementation",
        "jobs",
        "grid_membership",
    }:
        raise ValueError("invalid experiment manifest fields")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("unsupported experiment manifest schema")
    supplied_payload = {
        key: item for key, item in value.items() if key != "experiment_id"
    }
    supplied_digest = sha256(
        canonical_json(supplied_payload).encode("utf-8")
    ).hexdigest()
    if value["experiment_id"] != supplied_digest:
        raise ValueError(
            "manifest experiment ID does not match supplied payload digest"
        )
    raw_jobs = value["jobs"]
    members = value["grid_membership"]
    if not isinstance(raw_jobs, list) or not raw_jobs:
        raise ValueError("manifest needs nonempty jobs")
    if not isinstance(members, list) or any(not isinstance(x, str) for x in members):
        raise ValueError("invalid manifest membership")
    jobs = tuple(JobSpec.from_dict(item) for item in raw_jobs)
    if any(job.dataset != jobs[0].dataset for job in jobs):
        raise ValueError("job dataset does not match experiment dataset")
    if any(job.implementation != jobs[0].implementation for job in jobs):
        raise ValueError("job implementation does not match experiment implementation")
    rebuilt = manifest_record(jobs, jobs[0].dataset, jobs[0].implementation, members)
    if value["experiment_id"] != rebuilt["experiment_id"]:
        raise ValueError("manifest experiment ID does not match content")
    if value["dataset"] != rebuilt["dataset"]:
        raise ValueError("manifest dataset does not match job definitions")
    if canonical_json(value) != canonical_json(rebuilt):
        raise ValueError("manifest content is inconsistent with job definitions")
    return rebuilt, jobs


def outcome_artifact(
    job: JobSpec, outcome: JobOutcome
) -> tuple[str, dict[str, object]]:
    result = outcome.result
    if outcome.job_id != job.job_id or (
        result is not None
        and (
            result.config != job.config
            or result.dataset_metadata != job.dataset
            or result.engine_id != job.implementation.engine_id
            or result.strategy_id != SMA_ID
            or type(result.schema_version) is not int
            or result.schema_version != 1
        )
    ):
        raise RuntimeError(f"result does not match job {job.job_id}")
    failure = (
        {"error_type": outcome.failure.error_type, "message": outcome.failure.message}
        if outcome.failure is not None
        else None
    )
    status = "succeeded" if result is not None else "failed"
    content = (
        canonical_json(
            {
                "schema_version": 1,
                "job": job.to_dict(),
                "status": status,
                "result": result_to_dict(result) if result is not None else None,
                "failure": failure,
            }
        )
        + "\n"
    )
    return content, {
        "job_id": job.job_id,
        "status": status,
        "artifact": f"results/{job.job_id}.json",
        "content_sha256": sha256(content.encode("utf-8")).hexdigest(),
        "metrics": metrics_record(result.metrics) if result is not None else None,
        "failure": failure,
    }


def index_record(
    experiment_id: str, records: Sequence[dict[str, object]]
) -> dict[str, object]:
    ordered = sorted(records, key=lambda item: str(item["job_id"]))
    if len({item["job_id"] for item in ordered}) != len(ordered):
        raise ValueError("duplicate index job IDs")
    succeeded = sum(item["status"] == "succeeded" for item in ordered)
    return {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "submitted": len(ordered),
        "succeeded": succeeded,
        "failed": len(ordered) - succeeded,
        "outcomes": ordered,
    }
