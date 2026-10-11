"""Sequential exports and real SQLite journals exercise read-only status."""

import json
import sqlite3
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from distributed_backtester import experiment_snapshot as snapshot_module
from distributed_backtester.backends import ray as ray_backend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import (
    index_record,
    manifest_record,
    outcome_artifact,
)
from distributed_backtester.experiment_snapshot import (
    ExperimentSnapshot,
    read_experiment_snapshot,
)
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import JobSpec, canonical_json, expand_grid
from distributed_backtester.models import MarketDataset
from distributed_backtester.operational_status import read_status
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.scheduler import journal as journal_module
from distributed_backtester.scheduler.journal import Journal
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.worker import JobFailure, JobOutcome, execute_job

SOURCE = Path(__file__).resolve().parents[1] / "examples/sma.csv"


@pytest.fixture
def inputs() -> tuple[MarketDataset, ImplementationIdentity, tuple[JobSpec, ...]]:
    data = load_csv(SOURCE, symbol="SMA", price_basis="raw")
    identity = current_implementation()
    jobs, _members = expand_grid(
        [1, 2], [3], 1_000_000_000, 10, data.metadata, identity
    )
    return data, identity, tuple(jobs)


@pytest.fixture
def classic(
    tmp_path: Path,
    inputs: tuple[MarketDataset, ImplementationIdentity, tuple[JobSpec, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    data, identity, jobs = inputs
    # Environment collection is irrelevant here and normally invokes Git children.
    monkeypatch.setattr("distributed_backtester.experiments.capture_environment", dict)
    output = tmp_path / "classic"
    collect_experiment(jobs, data, identity, output, SequentialBackend())
    return output


@pytest.fixture
def journal(
    tmp_path: Path,
    inputs: tuple[MarketDataset, ImplementationIdentity, tuple[JobSpec, ...]],
) -> Iterator[Journal]:
    data, identity, jobs = inputs
    with Journal.create(
        tmp_path / "durable",
        manifest_record(jobs, data.metadata, identity),
        RetryPolicy(max_attempts=2, retry_delay_seconds=1),
        now=100,
    ) as writer:
        yield writer


def records(status: dict[str, object], key: str) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], status[key])


def load_json(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_bytes()))


def files(output: Path) -> dict[str, bytes]:
    return {
        path.relative_to(output).as_posix(): path.read_bytes()
        for path in output.rglob("*")
        if path.is_file()
    }


def failed_classic(output: Path, jobs: tuple[JobSpec, ...]) -> str:
    job = jobs[-1]
    content, record = outcome_artifact(
        job, JobOutcome(job.job_id, None, JobFailure("WorkerFailure", "stopped"))
    )
    (output / str(record["artifact"])).write_text(content)
    index = load_json(output / "index.json")
    index["outcomes"] = [
        record if item["job_id"] == job.job_id else item for item in index["outcomes"]
    ]
    (output / "index.json").write_text(
        canonical_json(index_record(index["experiment_id"], index["outcomes"])) + "\n"
    )
    return job.job_id


def test_real_sequential_exports_report_complete_without_scheduler_inventions(
    classic: Path,
) -> None:
    before = files(classic)
    index = load_json(classic / "index.json")
    status = read_status(classic)
    assert status == {
        "schema_version": 1,
        "experiment_id": index["experiment_id"],
        "source_kind": "classic",
        "publication_complete": True,
        "counts": {"pending": 0, "running": 0, "succeeded": 2, "failed": 0},
        "jobs": index["outcomes"],
    }
    assert json.loads(json.dumps(status)) == status
    assert files(classic) == before


def test_complete_classic_can_contain_failed_outcomes_and_filter_counts_are_global(
    classic: Path,
    inputs: tuple[MarketDataset, ImplementationIdentity, tuple[JobSpec, ...]],
) -> None:
    selected = failed_classic(classic, inputs[2])
    before = files(classic)
    full = read_status(classic)
    filtered = read_status(classic, job_id=selected)
    assert (
        filtered["counts"]
        == full["counts"]
        == {
            "pending": 0,
            "running": 0,
            "succeeded": 1,
            "failed": 1,
        }
    )
    assert filtered["selected_job_id"] == selected
    assert records(filtered, "jobs") == [
        row for row in records(full, "jobs") if row["job_id"] == selected
    ]
    assert records(filtered, "jobs")[0]["failure"]["message"] == "stopped"
    assert filtered["publication_complete"] is True
    assert files(classic) == before


def test_classic_execution_metadata_is_optional(classic: Path) -> None:
    (classic / "execution.json").unlink()
    status = read_status(classic)
    assert status["publication_complete"] is True
    assert "worker_observations" not in status


@pytest.mark.parametrize("change", ["version", "experiment", "nonfinite"])
def test_execution_observation_headers_are_validated(
    classic: Path, change: str
) -> None:
    path = classic / "execution.json"
    execution = load_json(path)
    if change == "version":
        execution["schema_version"] = True
    elif change == "experiment":
        execution["experiment_id"] = "f" * 64
    else:
        execution["backend_diagnostics"] = {"workers": [{"pid": float("nan")}]}
    path.write_text(json.dumps(execution))
    before = files(classic)
    with pytest.raises(ValueError):
        read_status(classic)
    assert files(classic) == before


def test_retained_worker_records_are_explicitly_last_observed_metadata(
    classic: Path,
) -> None:
    execution_path = classic / "execution.json"
    execution = load_json(execution_path)
    workers = [
        {
            "pid": 123,
            "node_id": "retired-node",
            "source_sha256": "b" * 64,
            "dataset_sha256": "a" * 64,
            "initialized_once": True,
            "jobs_completed": 2,
        }
    ]
    execution["backend_diagnostics"] = {"workers": workers}
    execution_path.write_text(json.dumps(execution))
    before = files(classic)
    status = read_status(classic)
    assert status["worker_observations"] == {
        "source": "execution.json",
        "kind": "last_observed",
        "workers": workers,
    }
    filtered = read_status(classic, job_id=records(status, "jobs")[0]["job_id"])
    assert filtered["worker_observations"] == status["worker_observations"]
    assert files(classic) == before


@pytest.mark.parametrize("diagnostics", [None, {}, {"workers": []}, {"workers": 2}])
def test_no_worker_identity_is_invented_from_backend_counts(
    classic: Path, diagnostics: object
) -> None:
    execution_path = classic / "execution.json"
    execution = load_json(execution_path)
    execution["backend_diagnostics"] = diagnostics
    execution_path.write_text(canonical_json(execution) + "\n")
    assert "worker_observations" not in read_status(classic)


@pytest.mark.parametrize(
    "missing", ["manifest.json", "index.json", "last_artifact", "all_artifacts"]
)
def test_partial_classic_is_an_explicit_error_without_mutation(
    classic: Path, missing: str
) -> None:
    if missing in {"last_artifact", "all_artifacts"}:
        paths = sorted((classic / "results").iterdir())
        for path in paths if missing == "all_artifacts" else paths[-1:]:
            path.unlink()
    else:
        (classic / missing).unlink()
    before = files(classic)
    with pytest.raises((FileNotFoundError, ValueError)):
        read_status(classic)
    assert files(classic) == before


@pytest.mark.parametrize("filtered", [False, True])
@pytest.mark.parametrize("corruption", ["hash", "rehashed_schema", "rehashed_ledger"])
def test_every_full_artifact_is_validated_even_when_another_job_is_selected(
    classic: Path, filtered: bool, corruption: str
) -> None:
    index = load_json(classic / "index.json")
    selected = index["outcomes"][0]["job_id"]
    last = index["outcomes"][-1]
    artifact_path = classic / last["artifact"]
    artifact = load_json(artifact_path)
    if corruption in {"hash", "rehashed_schema"}:
        artifact["schema_version"] = 99
    else:
        artifact["result"]["equity"][-1]["cash"] = "0.000000"
    content = (canonical_json(artifact) + "\n").encode()
    artifact_path.write_bytes(content)
    if corruption != "hash":
        last["content_sha256"] = sha256(content).hexdigest()
        (classic / "index.json").write_text(canonical_json(index) + "\n")
    before = files(classic)
    with pytest.raises(ValueError):
        read_status(classic, job_id=selected if filtered else None)
    assert files(classic) == before


@pytest.mark.parametrize("corruption", ["manifest", "index", "execution", "counts"])
def test_corrupt_classic_headers_are_rejected(classic: Path, corruption: str) -> None:
    if corruption == "counts":
        index = load_json(classic / "index.json")
        index["succeeded"] = 0
        (classic / "index.json").write_text(canonical_json(index) + "\n")
    else:
        (classic / f"{corruption}.json").write_text("{truncated")
    before = files(classic)
    with pytest.raises(ValueError):
        read_status(classic)
    assert files(classic) == before


def test_classic_stream_is_exhausted_to_detect_metadata_changes(
    classic: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Path.read_bytes
    index = load_json(classic / "index.json")
    last = classic / index["outcomes"][-1]["artifact"]

    def read_then_change(path: Path) -> bytes:
        content = original(path)
        if path == last:
            (classic / "execution.json").write_text("{}\n")
        return content

    monkeypatch.setattr(Path, "read_bytes", read_then_change)
    with pytest.raises(ValueError, match="changed during read"):
        read_status(classic)


def test_journal_appearing_during_classic_read_is_never_mislabeled_complete(
    classic: Path,
    journal: Journal,
    inputs: tuple[MarketDataset, ImplementationIdentity, tuple[JobSpec, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import distributed_backtester.operational_status as module

    data, identity, _jobs = inputs
    for _ in journal.jobs:
        attempt = journal.claim(now=100)
        assert attempt is not None
        journal.commit(attempt, execute_job(attempt.job, data, identity), now=101)
    original = read_experiment_snapshot

    @contextmanager
    def changed_source(output: Path) -> Iterator[ExperimentSnapshot]:
        destination = sqlite3.connect(output / "journal.sqlite3")
        try:
            journal._connection.backup(destination)
        finally:
            destination.close()
        with original(output) as snapshot:
            yield snapshot

    monkeypatch.setattr(module, "read_experiment_snapshot", changed_source)
    with pytest.raises(ValueError, match="source changed during read"):
        read_status(classic)


@pytest.mark.parametrize(
    "kind", ["pending", "corrupt", "empty", "directory", "dangling_link"]
)
def test_journal_appearing_during_last_artifact_validation_is_rejected(
    classic: Path, journal: Journal, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    last = load_json(classic / "index.json")["outcomes"][-1]["job_id"]
    path = classic / "journal.sqlite3"
    original = snapshot_module._validate_artifact
    injected = False

    def validate_then_add_journal(
        content: bytes, record: dict[str, object], job: JobSpec
    ) -> None:
        nonlocal injected
        original(content, record, job)
        if job.job_id != last or injected:
            return
        injected = True
        if kind == "pending":
            destination = sqlite3.connect(path)
            try:
                journal._connection.backup(destination)
            finally:
                destination.close()
        elif kind == "directory":
            path.mkdir()
        elif kind == "dangling_link":
            path.symlink_to(classic / "absent.sqlite3")
        else:
            path.write_bytes(b"not SQLite" if kind == "corrupt" else b"")

    monkeypatch.setattr(
        snapshot_module, "_validate_artifact", validate_then_add_journal
    )
    with pytest.raises(ValueError, match=r"^status source changed during read$"):
        read_status(classic)
    assert injected
    if kind == "pending":
        assert journal_module.read_status(classic) == journal.snapshot()
        assert journal.snapshot()["counts"] == {
            "pending": 2,
            "running": 0,
            "succeeded": 0,
            "failed": 0,
        }
    elif kind == "dangling_link":
        assert path.is_symlink() and not path.exists()


def test_pending_running_retry_and_terminal_journal_fields_are_preserved(
    journal: Journal,
    inputs: tuple[MarketDataset, ImplementationIdentity, tuple[JobSpec, ...]],
) -> None:
    data, identity, _jobs = inputs

    def check() -> dict[str, object]:
        expected = journal_module.read_status(journal.output)
        status = read_status(journal.output)
        assert status == {**expected, "source_kind": "journal"}
        assert json.loads(json.dumps(status)) == status
        return status

    assert cast(dict[str, int], check()["counts"])["pending"] == 2
    first = journal.claim(now=100)
    assert first is not None
    assert cast(dict[str, int], check()["counts"])["running"] == 1
    journal.fail(first, JobFailure("WorkerFailure", "retry me"), now=101)
    assert cast(dict[str, int], check()["counts"])["pending"] == 2
    second = journal.claim(now=101)
    assert second is not None and second.job.job_id != first.job.job_id
    journal.commit(second, execute_job(second.job, data, identity), now=102)
    assert cast(dict[str, int], check()["counts"])["succeeded"] == 1
    retry = journal.claim(now=102)
    assert retry is not None and retry.number == 2
    journal.fail(retry, JobFailure("WorkerFailure", "terminal"), now=103)
    assert check()["publication_complete"] is False
    journal.mark_complete()
    assert check()["publication_complete"] is True
    assert check()["counts"] == {
        "pending": 0,
        "running": 0,
        "succeeded": 1,
        "failed": 1,
    }


def test_filter_journal_jobs_attempts_and_events_keeps_global_events_and_counts(
    journal: Journal,
) -> None:
    first = journal.claim(now=100)
    second = journal.claim(now=100)
    assert first is not None and second is not None
    journal.fail(first, JobFailure("WorkerFailure", "stopped"), now=101)
    full = read_status(journal.output)
    filtered = read_status(journal.output, job_id=first.job.job_id)
    assert filtered["selected_job_id"] == first.job.job_id
    assert filtered["counts"] == full["counts"]
    assert records(filtered, "jobs") == [
        item for item in records(full, "jobs") if item["job_id"] == first.job.job_id
    ]
    assert records(filtered, "attempts") == [
        item for item in records(full, "attempts") if item["job_id"] == first.job.job_id
    ]
    assert records(filtered, "events") == [
        item
        for item in records(full, "events")
        if item["job_id"] in (None, first.job.job_id)
    ]
    assert any(item["job_id"] is None for item in records(filtered, "events"))
    assert filtered["generation"] == full["generation"] == 1
    assert filtered["policy"] == full["policy"]


def test_read_with_active_uncommitted_writer_observes_only_committed_state(
    journal: Journal,
) -> None:
    before = journal_module.read_status(journal.output)
    with journal._transaction():
        journal._connection.execute("UPDATE jobs SET status = 'running'")
        assert read_status(journal.output) == {**before, "source_kind": "journal"}
        # Undo the deliberately invalid pending update before the writer commits.
        journal._connection.execute("UPDATE jobs SET status = 'pending'")
    assert journal.snapshot() == before


def test_expired_running_attempt_is_retained_without_resume_or_liveness_claim(
    journal: Journal,
) -> None:
    attempt = journal.claim(now=100)
    assert attempt is not None
    before = journal.snapshot()
    status = read_status(journal.output)
    assert records(status, "jobs")[0]["status"] == "running"
    assert records(status, "attempts")[0]["deadline_utc"] == attempt.deadline_utc
    assert status == {**before, "source_kind": "journal"}
    assert journal.snapshot() == before
    assert "worker_observations" not in status


def test_interrupted_generation_remains_authoritative(
    journal: Journal,
    inputs: tuple[MarketDataset, ImplementationIdentity, tuple[JobSpec, ...]],
) -> None:
    data, identity, _jobs = inputs
    assert journal.claim(now=100) is not None
    journal.close()
    with Journal.resume(
        journal.output, dataset_metadata=data.metadata, implementation=identity, now=200
    ) as resumed:
        before = resumed.snapshot()
        status = read_status(journal.output)
        assert status == {**before, "source_kind": "journal"}
        assert status["generation"] == 2
        assert records(status, "attempts")[0]["status"] == "interrupted"


def test_journal_is_preferred_to_corrupt_or_partial_derived_exports(
    journal: Journal,
) -> None:
    for name in ("manifest.json", "index.json", "execution.json"):
        (journal.output / name).write_text("{truncated")
    before = journal.snapshot()
    assert read_status(journal.output) == {**before, "source_kind": "journal"}


def test_closed_journal_is_read_without_creating_lock_or_rewriting_durable_bytes(
    journal: Journal,
) -> None:
    expected = journal.snapshot()
    journal.close()
    before = files(journal.output)
    assert read_status(journal.output) == {**expected, "source_kind": "journal"}
    after = files(journal.output)
    assert {name: after[name] for name in before} == before
    # The existing SQLite mode=ro reader may create WAL bookkeeping sidecars.
    assert after.keys() - before.keys() <= {
        "journal.sqlite3-wal",
        "journal.sqlite3-shm",
    }
    if "journal.sqlite3-wal" in after:
        assert after["journal.sqlite3-wal"] == b""


@pytest.mark.parametrize("kind", ["corrupt", "empty", "directory", "dangling_link"])
def test_present_invalid_journal_never_falls_back_to_complete_classic(
    classic: Path, kind: str
) -> None:
    path = classic / "journal.sqlite3"
    if kind == "directory":
        path.mkdir()
    elif kind == "dangling_link":
        path.symlink_to(classic / "absent.sqlite3")
    else:
        path.write_bytes(b"not SQLite" if kind == "corrupt" else b"")
    with pytest.raises((ValueError, RuntimeError, FileNotFoundError, sqlite3.Error)):
        read_status(classic)


@pytest.mark.parametrize("filtered", [False, True])
def test_journal_validates_unselected_committed_content_before_returning(
    journal: Journal,
    inputs: tuple[MarketDataset, ImplementationIdentity, tuple[JobSpec, ...]],
    filtered: bool,
) -> None:
    data, identity, _jobs = inputs
    selected = journal.jobs[0].job_id
    for _ in journal.jobs:
        attempt = journal.claim(now=100)
        assert attempt is not None
        journal.commit(attempt, execute_job(attempt.job, data, identity), now=101)
    journal._connection.execute(
        "UPDATE commits SET content = ? WHERE job_id != ?", ("{}\n", selected)
    )
    with pytest.raises(RuntimeError, match="hash"):
        read_status(journal.output, job_id=selected if filtered else None)


@pytest.mark.parametrize("source", ["classic", "journal"])
@pytest.mark.parametrize(
    "value", ["", "a" * 63, "a" * 65, "A" * 64, "g" * 64, " a" * 32]
)
def test_job_filter_requires_lowercase_sha256(
    classic: Path, journal: Journal, source: str, value: str
) -> None:
    output = classic if source == "classic" else journal.output
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        read_status(output, job_id=value)


@pytest.mark.parametrize("value", [True, 1, [], {}])
def test_job_filter_rejects_nontext_without_creating_output(
    tmp_path: Path, value: object
) -> None:
    output = tmp_path / "absent"
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        read_status(output, job_id=cast(Any, value))
    assert not output.exists()


@pytest.mark.parametrize("value", [None, "not-a-path", 1])
def test_output_must_be_a_path(value: object) -> None:
    with pytest.raises(ValueError, match="Path"):
        read_status(cast(Any, value))


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_unknown_job_is_rejected_and_membership_is_not_invented(
    classic: Path, journal: Journal, source: str
) -> None:
    output = classic if source == "classic" else journal.output
    with pytest.raises(ValueError, match="unknown job"):
        read_status(output, job_id="f" * 64)


@pytest.mark.parametrize("source", ["classic", "journal"])
@pytest.mark.parametrize("location", ["output", "ancestor", "artifact"])
def test_symlink_paths_are_rejected(
    classic: Path, journal: Journal, tmp_path: Path, source: str, location: str
) -> None:
    output = classic if source == "classic" else journal.output
    if location == "output":
        alias = tmp_path / "alias"
        alias.symlink_to(output, target_is_directory=True)
        output = alias
    elif location == "ancestor":
        alias = tmp_path / "alias"
        alias.symlink_to(tmp_path, target_is_directory=True)
        output = alias / output.name
    else:
        path = (
            sorted((output / "results").iterdir())[-1]
            if source == "classic"
            else output / "journal.sqlite3"
        )
        retained = path.with_suffix(".retained")
        path.rename(retained)
        path.symlink_to(retained)
    with pytest.raises(ValueError, match="symlink"):
        read_status(output)


def test_execution_symlink_is_rejected(classic: Path) -> None:
    execution = classic / "execution.json"
    retained = classic / "execution.retained"
    execution.rename(retained)
    execution.symlink_to(retained)
    with pytest.raises(ValueError, match="symlink"):
        read_status(classic)


@pytest.mark.parametrize("name", ["manifest.json", "index.json", "results"])
def test_classic_header_and_artifact_directory_symlinks_are_rejected(
    classic: Path, name: str
) -> None:
    path = classic / name
    retained = classic / f"{name}.retained"
    directory = path.is_dir()
    path.rename(retained)
    path.symlink_to(retained, target_is_directory=directory)
    with pytest.raises(ValueError, match="symlink"):
        read_status(classic)


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_path_traversal_is_rejected(
    classic: Path, journal: Journal, source: str
) -> None:
    output = classic if source == "classic" else journal.output
    with pytest.raises(ValueError, match="traversal"):
        read_status(output / ".." / output.name)


@pytest.mark.parametrize("selected", [None, "f" * 64])
def test_nonexistent_output_never_creates_any_directory(
    tmp_path: Path, selected: str | None
) -> None:
    output = tmp_path / "missing" / "nested"
    with pytest.raises(FileNotFoundError):
        read_status(output, job_id=selected)
    assert not output.parent.exists()


@pytest.mark.parametrize("source", ["classic", "journal"])
def test_status_never_starts_runtime_takes_coordinator_lock_or_writes_exports(
    classic: Path,
    journal: Journal,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
) -> None:
    output = classic if source == "classic" else journal.output
    before = files(output)
    modules = {name for name in sys.modules if name == "ray" or name.startswith("ray.")}

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("status tried to mutate output or initialize runtime")

    original_connect = journal_module._connect

    def readonly_connect(
        path: Path, *, readonly: bool, create: bool = False
    ) -> sqlite3.Connection:
        assert readonly and not create
        return original_connect(path, readonly=readonly, create=create)

    monkeypatch.setattr(journal_module, "_connect", readonly_connect)
    monkeypatch.setattr(journal_module, "_lock", forbidden)
    monkeypatch.setattr(Journal, "create", forbidden)
    monkeypatch.setattr(Journal, "resume", forbidden)
    monkeypatch.setattr(Path, "mkdir", forbidden)
    monkeypatch.setattr(Path, "write_text", forbidden)
    monkeypatch.setattr(Path, "write_bytes", forbidden)
    monkeypatch.setattr(ray_backend, "_require_ray", forbidden)
    assert read_status(output)["source_kind"] == source
    after = files(output)
    assert after.keys() == before.keys()
    # SQLite records reader marks in shared memory even with mode=ro/query_only.
    # Database, WAL, exports and coordinator lock bytes must remain unchanged.
    assert {
        name: content for name, content in after.items() if not name.endswith("-shm")
    } == {
        name: content for name, content in before.items() if not name.endswith("-shm")
    }
    assert modules == {
        name for name in sys.modules if name == "ray" or name.startswith("ray.")
    }
