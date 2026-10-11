"""Validated, read-only status for durable journals and complete classic exports."""

import re
from pathlib import Path
from typing import cast

from distributed_backtester.experiment_snapshot import read_experiment_snapshot
from distributed_backtester.scheduler.journal import read_status as read_journal_status


def _classic_status(output: Path) -> dict[str, object]:
    with read_experiment_snapshot(output) as snapshot:
        if snapshot.observation["source_kind"] != "classic":
            raise ValueError("status source changed during read")
        # Headers alone cannot establish artifact validity or a stable export.
        # Exhaust the stream, including its final compact-metadata recheck.
        for artifact in snapshot.iter_artifacts():
            del artifact
        journal = output / "journal.sqlite3"
        if journal.exists() or journal.is_symlink():
            raise ValueError("status source changed during read")
        status: dict[str, object] = {
            "schema_version": 1,
            "experiment_id": snapshot.experiment_id,
            "source_kind": "classic",
            "publication_complete": True,
            "counts": {
                "pending": 0,
                "running": 0,
                "succeeded": snapshot.succeeded,
                "failed": snapshot.failed,
            },
            "jobs": list(snapshot.records),
        }
        execution = snapshot.observation.get("execution")
        if isinstance(execution, dict):
            diagnostics = execution.get("backend_diagnostics")
            if isinstance(diagnostics, dict):
                workers = diagnostics.get("workers")
                if (
                    isinstance(workers, list)
                    and workers
                    and all(isinstance(worker, dict) for worker in workers)
                ):
                    status["worker_observations"] = {
                        "source": "execution.json",
                        "kind": "last_observed",
                        "workers": workers,
                    }
        return status


def read_status(output: Path, *, job_id: str | None = None) -> dict[str, object]:
    """Read validated status without creating output, locking, or starting workers.

    A present journal is authoritative, including pending and running records;
    its validated fields are preserved. Invalid journals never fall back to
    classic exports. Classic exports must contain every final artifact and index;
    partial exports raise errors and have no resumability or liveness guarantee.

    When selecting a job, counts and publication metadata remain experiment-wide.
    Jobs and attempts are filtered by membership; events retain the selected job
    and experiment-wide events with no job ID. ``selected_job_id`` identifies the
    selection. Validation covers all jobs before filtering.

    Optional classic worker observations are retained execution diagnostics for
    the whole experiment, never current readiness or alive/dead assertions. The
    journal reader does not retain worker identities, so durable reads omit them.
    """
    if job_id is not None and (
        not isinstance(job_id, str) or re.fullmatch(r"[0-9a-f]{64}", job_id) is None
    ):
        raise ValueError("job_id must be a lowercase SHA-256 digest")
    if not isinstance(output, Path) or ".." in output.parts:
        raise ValueError("output must be a Path without traversal")
    absolute = output.absolute()
    if any(path.is_symlink() for path in (absolute, *absolute.parents)):
        raise ValueError("status paths must not be symlinks")
    journal = absolute / "journal.sqlite3"
    if journal.exists() or journal.is_symlink():
        status = {**read_journal_status(absolute), "source_kind": "journal"}
    else:
        status = _classic_status(absolute)
    if job_id is None:
        return status
    jobs = cast(list[dict[str, object]], status["jobs"])
    selected = [job for job in jobs if job["job_id"] == job_id]
    if not selected:
        raise ValueError(f"unknown job_id in experiment: {job_id}")
    status["jobs"] = selected
    if "attempts" in status:
        attempts = cast(list[dict[str, object]], status["attempts"])
        status["attempts"] = [item for item in attempts if item["job_id"] == job_id]
    if "events" in status:
        events = cast(list[dict[str, object]], status["events"])
        status["events"] = [item for item in events if item["job_id"] in (None, job_id)]
    status["selected_job_id"] = job_id
    return status
