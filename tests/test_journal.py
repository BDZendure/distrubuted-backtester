import json
import selectors
import sqlite3
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from distributed_backtester.experiment_artifacts import (
    manifest_record,
    outcome_artifact,
)
from distributed_backtester.jobs import JobSpec, canonical_json, create_job
from distributed_backtester.models import BacktestConfig, MarketDataset
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.scheduler.journal import Journal, read_manifest, read_status
from distributed_backtester.scheduler.models import AttemptSpec, RetryPolicy
from distributed_backtester.simulation import ENGINE_ID, run_backtest
from distributed_backtester.worker import JobFailure, JobOutcome
from tests.test_simulation import dataset

# Fixed source bytes keep fixtures valid while other workers edit package source.
IDENTITY = ImplementationIdentity(ENGINE_ID, "b" * 64)
DATA = dataset([10, 11, 12, 9, 10])
JOBS = tuple(
    sorted(
        (
            create_job(BacktestConfig(fee_bps=fee), DATA.metadata, IDENTITY)
            for fee in (0, 10)
        ),
        key=lambda job: job.job_id,
    )
)
FAILURE = JobFailure("WorkerFailure", "worker exited")


def manifest(jobs: tuple[JobSpec, ...] = JOBS) -> dict[str, object]:
    return manifest_record(jobs, DATA.metadata, IDENTITY)


def outcome(job: JobSpec, data: MarketDataset = DATA) -> JobOutcome:
    return JobOutcome(job.job_id, run_backtest(job.config, data), None)


def resume(output: Path, *, now: float = 100) -> Journal:
    return Journal.resume(
        output, dataset_metadata=DATA.metadata, implementation=IDENTITY, now=now
    )


@contextmanager
def connection(output: Path) -> Iterator[sqlite3.Connection]:
    db = sqlite3.connect(output / "journal.sqlite3")
    try:
        with db:
            yield db
    finally:
        db.close()


def records(status: dict[str, object], key: str) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], status[key])


@pytest.fixture
def journal(tmp_path: Path) -> Iterator[Journal]:
    with Journal.create(tmp_path / "run", manifest(), RetryPolicy(), now=100) as j:
        yield j


def test_create_persists_inputs_before_claim_and_uses_required_pragmas(
    journal: Journal,
) -> None:
    assert journal.generation == 1
    assert journal.jobs == JOBS and journal.manifest == manifest()
    assert journal.policy == RetryPolicy()
    assert not (journal.output / "manifest.json").exists()
    assert not (journal.output / "index.json").exists()
    assert not (journal.output / "results").exists()
    assert read_manifest(journal.output) == manifest()
    assert read_status(journal.output) == journal.snapshot()
    with connection(journal.output) as db:
        assert db.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    # FULL and foreign_keys are connection-local; inspect the actual writer.
    assert journal._connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    assert journal._connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert journal._connection.execute("PRAGMA busy_timeout").fetchone()[0] > 0


def test_read_committed_snapshot_is_consistent_without_writer_lock(
    journal: Journal,
) -> None:
    import distributed_backtester.scheduler.journal as module

    assert hasattr(module, "read_committed_snapshot"), "committed reader is missing"
    first = journal.claim(now=100)
    assert first is not None
    journal.commit(first, outcome(first.job), now=101)
    before = journal.snapshot()
    with module.read_committed_snapshot(journal.output) as snapshot:
        assert snapshot.manifest == manifest()
        assert snapshot.jobs == JOBS
        assert len(snapshot.records) == 1
        second = journal.claim(now=102)
        assert second is not None
        journal.commit(second, outcome(second.job), now=103)
        assert snapshot.status == before
        content, record = outcome_artifact(first.job, outcome(first.job))
        assert list(snapshot.iter_artifacts()) == [
            (first.job.job_id, content.encode(), record)
        ]
    assert journal.generation == 1 and journal.all_terminal()
    with pytest.raises(RuntimeError, match="closed"):
        list(snapshot.iter_artifacts())


def test_read_committed_snapshot_validates_corruption_and_never_creates(
    tmp_path: Path,
) -> None:
    import distributed_backtester.scheduler.journal as module

    assert hasattr(module, "read_committed_snapshot"), "committed reader is missing"
    output = tmp_path / "run"
    with pytest.raises(FileNotFoundError), module.read_committed_snapshot(output):
        pass
    assert not output.exists()
    with Journal.create(output, manifest(), RetryPolicy(), now=100) as journal:
        attempt = journal.claim(now=100)
        assert attempt is not None
        journal.commit(attempt, outcome(attempt.job), now=101)
    with connection(output) as db:
        db.execute("UPDATE commits SET content='{}'")
    with (
        pytest.raises(RuntimeError, match=r"artifact|hash"),
        module.read_committed_snapshot(output),
    ):
        pass


@pytest.mark.parametrize(
    "replacement",
    [
        RetryPolicy(max_attempts=1),
        RetryPolicy(job_timeout_seconds=1),
        RetryPolicy(retry_delay_seconds=7),
    ],
)
def test_policy_reassignment_preserves_retry_budget_and_recovery(
    tmp_path: Path, replacement: RetryPolicy
) -> None:
    output = tmp_path / "run"
    policy = RetryPolicy(2, 10, 2)
    with Journal.create(output, manifest((JOBS[0],)), policy, now=100) as j:
        before = j.snapshot()
        with pytest.raises(AttributeError, match="policy"):
            j.policy = replacement  # type: ignore[misc]
        assert j.policy == policy and j.snapshot() == before
        first = j.claim(now=100)
        assert first is not None and first.deadline_utc == 110
        assert j.fail(first, FAILURE, now=101) == "retry"
        assert j.next_eligible() == 103
        second = j.claim(now=103)
        assert second is not None and second.number == 2
        assert second.deadline_utc == 113
        assert j.fail(second, FAILURE, now=104) == "terminal"
    assert read_status(output)["policy"] == policy.to_dict()
    with resume(output, now=105) as j:
        assert j.policy == policy and j.all_terminal()


@pytest.mark.parametrize(
    "attribute,value", [("jobs", tuple(reversed(JOBS))), ("generation", 999)]
)
def test_session_jobs_and_generation_cannot_be_reassigned(
    journal: Journal, attribute: str, value: object
) -> None:
    before = journal.snapshot()
    with pytest.raises(AttributeError, match=attribute):
        setattr(journal, attribute, value)
    assert journal.snapshot() == before
    attempt = journal.claim(now=100)
    assert attempt is not None
    assert attempt.job == JOBS[0] and attempt.generation == 1


def test_claim_sorted_unique_attempts_and_deadlines(journal: Journal) -> None:
    first = journal.claim(now=100)
    second = journal.claim(now=101)
    assert first == AttemptSpec(JOBS[0], 1, 1, f"{JOBS[0].job_id}:1", 160)
    assert second == AttemptSpec(JOBS[1], 1, 1, f"{JOBS[1].job_id}:1", 161)
    assert journal.claim(now=102) is None
    assert journal.next_eligible() is None
    assert not journal.all_terminal()
    assert journal.snapshot()["counts"] == {
        "pending": 0,
        "running": 2,
        "succeeded": 0,
        "failed": 0,
    }


def test_find_attempt_reads_active_and_completed_history(journal: Journal) -> None:
    first = journal.claim(now=100)
    assert first is not None
    before = journal.snapshot()
    assert journal.find_attempt(first.attempt_id) == first
    assert journal.find_attempt(f"{first.job.job_id}:99") is None
    assert journal.find_attempt(f"{JOBS[1].job_id}:1") is None
    assert journal.snapshot() == before
    assert journal.fail(first, FAILURE, now=101) == "retry"
    second = journal.claim(now=101.1)
    assert second is not None and second.job == first.job and second.number == 2
    assert journal.commit(second, outcome(second.job), now=102) == "committed"
    before = journal.snapshot()
    assert journal.find_attempt(first.attempt_id) == first
    assert journal.find_attempt(second.attempt_id) == second
    assert journal.snapshot() == before


def test_find_attempt_preserves_interrupted_generation_on_resume(
    tmp_path: Path,
) -> None:
    output = tmp_path / "run"
    with Journal.create(output, manifest((JOBS[0],)), RetryPolicy(), now=100) as j:
        old = j.claim(now=100)
        assert old is not None
    with resume(output, now=101) as j:
        current = j.claim(now=101.1)
        assert current is not None and current.generation == 2
        before = j.snapshot()
        assert j.find_attempt(old.attempt_id) == old
        assert j.find_attempt(current.attempt_id) == current
        assert j.snapshot() == before


def test_find_attempt_refuses_closed_session(journal: Journal) -> None:
    journal.close()
    with pytest.raises(RuntimeError, match="closed"):
        journal.find_attempt(f"{JOBS[0].job_id}:1")


def test_retry_delay_budget_and_terminal_failure_artifact(tmp_path: Path) -> None:
    policy = RetryPolicy(2, 10, 2)
    with Journal.create(tmp_path / "run", manifest((JOBS[0],)), policy, now=100) as j:
        first = j.claim(now=100)
        assert first is not None
        assert (
            j.fail(
                first,
                FAILURE,
                now=101,
                reason="worker_failure",
                diagnostic="original exception",
            )
            == "retry"
        )
        assert j.next_eligible() == 103
        assert j.claim(now=102.999) is None
        second = j.claim(now=103)
        assert second is not None and second.number == 2
        assert j.fail(second, FAILURE, now=104, reason="timeout") == "terminal"
        assert j.all_terminal() and j.next_eligible() is None
        assert j.claim(now=1000) is None
        content, compact = outcome_artifact(
            JOBS[0], JobOutcome(JOBS[0].job_id, None, FAILURE)
        )
        assert list(j.committed()) == [(JOBS[0].job_id, content, compact)]
        attempts = records(j.snapshot(), "attempts")
        assert [x["reason"] for x in attempts] == ["worker_failure", "timeout"]
        assert attempts[0]["diagnostic"] == "original exception"
        assert compact["failure"] == {
            "error_type": FAILURE.error_type,
            "message": FAILURE.message,
        }


def test_commit_duplicate_lost_ack_and_sorted_exports(journal: Journal) -> None:
    attempts = [journal.claim(now=100), journal.claim(now=100)]
    for attempt in reversed(attempts):
        assert attempt is not None
        result = outcome(attempt.job)
        assert journal.commit(attempt, result, now=101) == "committed"
        assert journal.commit(attempt, result, now=1000) == "duplicate"
    assert journal.all_terminal()
    assert [item[0] for item in journal.committed()] == [job.job_id for job in JOBS]
    assert len(records(journal.snapshot(), "attempts")) == 2
    journal.mark_complete()
    assert read_status(journal.output)["publication_complete"] is True


def test_conflicting_same_completed_attempt_raises(journal: Journal) -> None:
    attempt = journal.claim(now=100)
    assert attempt is not None
    result = outcome(attempt.job)
    assert result.result is not None
    assert journal.commit(attempt, result, now=101) == "committed"
    different = replace(
        result,
        result=replace(
            result.result, metrics=replace(result.result.metrics, fill_count=99)
        ),
    )
    with pytest.raises(RuntimeError, match="conflict"):
        journal.commit(attempt, different, now=102)
    assert next(journal.committed())[1:] == outcome_artifact(attempt.job, result)


@pytest.mark.parametrize(
    "change", ["job", "config", "dataset", "engine", "strategy", "schema"]
)
def test_commit_rejects_result_mismatch_without_mutation(
    journal: Journal,
    change: str,
) -> None:
    attempt = journal.claim(now=100)
    assert attempt is not None
    result = outcome(attempt.job)
    assert result.result is not None
    simulation_result = result.result
    if change == "job":
        result = replace(result, job_id="f" * 64)
    else:
        values: dict[str, Any] = {
            "config": BacktestConfig(fee_bps=100),
            "dataset": replace(DATA.metadata, symbol="OTHER"),
            "engine": "other-engine",
            "strategy": "other-strategy",
            "schema": True,
        }
        field = {
            "dataset": "dataset_metadata",
            "engine": "engine_id",
            "strategy": "strategy_id",
            "schema": "schema_version",
        }.get(change, change)
        result = replace(
            result, result=replace(simulation_result, **{field: values[change]})
        )
    before = journal.snapshot()
    with pytest.raises((ValueError, RuntimeError)):
        journal.commit(attempt, result, now=101)
    assert journal.snapshot() == before


def test_commit_refuses_simulation_failure_and_incomplete_publication(
    journal: Journal,
) -> None:
    attempt = journal.claim(now=100)
    assert attempt is not None
    with pytest.raises(ValueError):
        journal.commit(attempt, JobOutcome(attempt.job.job_id, None, FAILURE), now=101)
    with pytest.raises(RuntimeError):
        journal.mark_complete()


def test_deadline_and_full_lease_fencing(journal: Journal) -> None:
    attempt = journal.claim(now=100)
    assert attempt is not None
    for forged in (replace(attempt, generation=2), replace(attempt, deadline_utc=161)):
        assert journal.commit(forged, outcome(attempt.job), now=101) == "stale"
        assert journal.fail(forged, FAILURE, now=101) == "stale"
    assert journal.commit(attempt, outcome(attempt.job), now=160) == "stale"
    assert journal.fail(attempt, FAILURE, now=160, reason="timeout") == "retry"
    assert not list(journal.committed())


def test_resume_interrupts_and_fences_old_reply_and_clamps_wait(tmp_path: Path) -> None:
    output = tmp_path / "run"
    policy = RetryPolicy(3, 60, 2)
    with Journal.create(output, manifest(), policy, now=100) as j:
        old = j.claim(now=100)
        other = j.claim(now=100)
        assert old is not None and other is not None
        assert j.fail(other, FAILURE, now=110) == "retry"
    # UTC jumped backwards; an existing pending wait must never exceed one delay.
    with resume(output, now=50) as j:
        assert j.generation == 2
        assert j.next_eligible() == 52
        assert j.claim(now=51.99) is None
        attempts = [j.claim(now=52), j.claim(now=52)]
        assert all(a is not None and a.number == 2 for a in attempts)
        assert j.commit(old, outcome(old.job), now=53) == "stale"
        assert j.fail(old, FAILURE, now=53) == "stale"
        current = next(a for a in attempts if a is not None and a.job == old.job)
        assert j.commit(current, outcome(current.job), now=53) == "committed"
        different = replace(
            outcome(old.job), result=run_backtest(BacktestConfig(fee_bps=99), DATA)
        )
        assert j.commit(old, different, now=54) == "stale"
        history = records(j.snapshot(), "attempts")
        assert history[0]["reason"] == "interrupted"


def test_resume_exhausted_interrupted_budget_stores_terminal_failure(
    tmp_path: Path,
) -> None:
    output = tmp_path / "run"
    with Journal.create(output, manifest((JOBS[0],)), RetryPolicy(1), now=100) as j:
        assert j.claim(now=100) is not None
    with resume(output, now=101) as j:
        assert j.all_terminal() and j.claim(now=101) is None
        content = json.loads(next(j.committed())[1])
        assert content["failure"]["error_type"] == "Interrupted"


def test_resume_preserves_commits_and_resets_publication_flag(tmp_path: Path) -> None:
    output = tmp_path / "run"
    with Journal.create(output, manifest((JOBS[0],)), RetryPolicy(), now=100) as j:
        attempt = j.claim(now=100)
        assert attempt is not None
        assert j.commit(attempt, outcome(attempt.job), now=101) == "committed"
        j.mark_complete()
        exported = list(j.committed())
    (output / "manifest.json").write_text("corrupt derived file")
    assert read_manifest(output) == manifest((JOBS[0],))
    with resume(output) as j:
        assert j.generation == 2 and j.snapshot()["publication_complete"] is False
        assert j.claim(now=100) is None and list(j.committed()) == exported
        assert j.commit(attempt, outcome(attempt.job), now=101) == "duplicate"


@pytest.mark.parametrize("change", ["dataset", "symbol", "source", "engine"])
def test_resume_identity_mismatch_does_not_mutate(tmp_path: Path, change: str) -> None:
    output = tmp_path / "run"
    with Journal.create(output, manifest(), RetryPolicy(), now=100) as j:
        assert j.claim(now=100) is not None
    metadata, identity = DATA.metadata, IDENTITY
    if change == "dataset":
        metadata = replace(metadata, source_sha256="c" * 64)
    elif change == "symbol":
        metadata = replace(metadata, symbol="OTHER")
    elif change == "source":
        identity = replace(identity, source_sha256="c" * 64)
    else:
        identity = replace(identity, engine_id="other-engine")
    before = read_status(output)
    with pytest.raises(ValueError, match=r"dataset|implementation|identity"):
        Journal.resume(
            output, dataset_metadata=metadata, implementation=identity, now=101
        )
    assert read_status(output) == before
    with resume(output) as j:
        assert j.generation == 2


def test_create_refuses_existing_output_and_invalid_manifest(tmp_path: Path) -> None:
    with pytest.raises(FileExistsError):
        Journal.create(tmp_path, manifest(), RetryPolicy(), now=100)
    wrong = manifest()
    wrong["experiment_id"] = "f" * 64
    with pytest.raises(ValueError):
        Journal.create(tmp_path / "invalid", wrong, RetryPolicy(), now=100)
    assert not (tmp_path / "invalid").exists()
    mismatched = create_job(
        BacktestConfig(fee_bps=30), replace(DATA.metadata, symbol="OTHER"), IDENTITY
    )
    with pytest.raises(ValueError):
        Journal.create(
            tmp_path / "mixed", manifest((JOBS[0], mismatched)), RetryPolicy(), now=100
        )


@pytest.mark.parametrize("reader", [read_status, read_manifest])
def test_readers_do_not_create_missing_database(tmp_path: Path, reader: Any) -> None:
    output = tmp_path / "missing"
    with pytest.raises((ValueError, RuntimeError, FileNotFoundError)):
        reader(output)
    assert not output.exists()
    output.mkdir()
    with pytest.raises((ValueError, RuntimeError, FileNotFoundError)):
        reader(output)
    assert list(output.iterdir()) == []


@pytest.mark.parametrize("invalid", [b"", b"not sqlite"])
def test_resume_refuses_uninitialized_or_corrupt_database(
    tmp_path: Path,
    invalid: bytes,
) -> None:
    output = tmp_path / "run"
    output.mkdir()
    (output / "coordinator.lock").touch()
    (output / "journal.sqlite3").write_bytes(invalid)
    with pytest.raises((ValueError, RuntimeError, sqlite3.DatabaseError)):
        resume(output)
    assert (output / "journal.sqlite3").read_bytes() == invalid


@pytest.mark.parametrize(
    "tamper",
    ["content", "record", "path", "job", "policy", "manifest", "state", "schema"],
)
def test_corruption_rejected_on_resume_and_read(tmp_path: Path, tamper: str) -> None:
    output = tmp_path / "run"
    with Journal.create(output, manifest((JOBS[0],)), RetryPolicy(), now=100) as j:
        attempt = j.claim(now=100)
        assert attempt is not None
        j.commit(attempt, outcome(attempt.job), now=101)
    with connection(output) as db:
        if tamper == "content":
            db.execute("UPDATE commits SET content = content || ' '")
        elif tamper in ("record", "path"):
            text = db.execute("SELECT record FROM commits").fetchone()[0]
            compact = json.loads(text)
            if tamper == "path":
                compact["artifact"] = "../escape.json"
            else:
                compact["metrics"]["fill_count"] = 999
            text = canonical_json(compact)
            db.execute(
                "UPDATE commits SET record = ?, record_sha256 = ?",
                (text, sha256(text.encode()).hexdigest()),
            )
        elif tamper == "job":
            db.execute("UPDATE jobs SET spec = '{}' ")
        elif tamper == "policy":
            db.execute("UPDATE metadata SET policy = '{}' ")
        elif tamper == "manifest":
            db.execute("UPDATE metadata SET manifest = '{}' ")
        elif tamper == "state":
            db.execute("UPDATE jobs SET status = 'pending'")
        else:
            db.execute("PRAGMA user_version = 999")
    with pytest.raises((ValueError, RuntimeError, sqlite3.DatabaseError)):
        read_status(output)
    with pytest.raises((ValueError, RuntimeError, sqlite3.DatabaseError)):
        read_manifest(output)
    with pytest.raises((ValueError, RuntimeError, sqlite3.DatabaseError)):
        resume(output)


def test_committed_checks_hash_during_open_session(journal: Journal) -> None:
    attempt = journal.claim(now=100)
    assert attempt is not None
    journal.commit(attempt, outcome(attempt.job), now=101)
    with connection(journal.output) as db:
        db.execute("UPDATE commits SET content = '{}' ")
    with pytest.raises((ValueError, RuntimeError)):
        list(journal.committed())


def test_live_manifest_validation_before_commit(journal: Journal) -> None:
    attempt = journal.claim(now=100)
    assert attempt is not None
    invalid = manifest()
    invalid["experiment_id"] = "f" * 64
    text = canonical_json(invalid)
    with connection(journal.output) as db:
        db.execute(
            "UPDATE metadata SET manifest = ?, manifest_sha256 = ?",
            (text, sha256(text.encode()).hexdigest()),
        )
    with pytest.raises((ValueError, RuntimeError)):
        journal.commit(attempt, outcome(attempt.job), now=101)


def test_close_is_idempotent_releases_lock_and_refuses_use(journal: Journal) -> None:
    output = journal.output
    journal.close()
    journal.close()
    with pytest.raises(RuntimeError, match="closed"):
        journal.claim(now=100)
    with resume(output) as j:
        assert j.generation == 2


def test_competing_process_writer_is_excluded_but_status_can_read(
    journal: Journal,
) -> None:
    script = """
import sys
from pathlib import Path
from distributed_backtester.models import DatasetMetadata
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.scheduler.journal import Journal, read_status
from distributed_backtester.simulation import ENGINE_ID
output = Path(sys.argv[1])
assert read_status(output)["generation"] == 1
try:
    Journal.resume(output, dataset_metadata=DatasetMetadata("SYNTH", "a" * 64),
        implementation=ImplementationIdentity(ENGINE_ID, "b" * 64), now=101)
except RuntimeError as error:
    assert "lock" in str(error).lower() or "coordinator" in str(error).lower()
else:
    raise AssertionError("second writer acquired the coordinator lock")
"""
    before = journal.snapshot()
    completed = subprocess.run(
        [sys.executable, "-c", script, str(journal.output)],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    assert journal.snapshot() == before


CRASH_SCRIPT = """
import sys
from pathlib import Path
from distributed_backtester.models import DatasetMetadata
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.scheduler.journal import Journal
from distributed_backtester.simulation import ENGINE_ID, run_backtest
from distributed_backtester.worker import JobOutcome, JobFailure
from tests.test_simulation import dataset
j = Journal.resume(Path(sys.argv[1]),
    dataset_metadata=DatasetMetadata("SYNTH", "a" * 64),
    implementation=ImplementationIdentity(ENGINE_ID, "b" * 64), now=101)
phase = sys.argv[2]
def pause():
    print("TRANSACTION_READY", flush=True)
    sys.stdin.readline()
    return 0
if phase == "claim":
    j._connection.create_function("pause", 0, pause)
    j._connection.execute("CREATE TEMP TRIGGER crash_claim AFTER INSERT ON attempts "
                          "BEGIN SELECT pause(); END")
    j.claim(now=101)
else:
    a = j.claim(now=101)
    if phase in ("commit", "failure"):
        j._connection.create_function("pause", 0, pause)
        j._connection.execute("CREATE TEMP TRIGGER crash_commit "
                              "AFTER INSERT ON commits "
                              "BEGIN SELECT pause(); END")
    if phase == "failure":
        j.fail(a, JobFailure("WorkerFailure", "died"), now=102)
    else:
        j.commit(a, JobOutcome(a.job.job_id,
            run_backtest(a.job.config, dataset([10,11,12,9,10])), None), now=102)
    if phase == "ack":
        pause()
"""


@pytest.mark.parametrize("phase", ["claim", "commit", "failure", "ack"])
def test_abrupt_process_kill_rolls_back_partial_transactions_and_lost_ack(
    tmp_path: Path,
    phase: str,
) -> None:
    output = tmp_path / "run"
    policy = RetryPolicy(max_attempts=1 if phase == "failure" else 3)
    with Journal.create(output, manifest((JOBS[0],)), policy, now=100):
        pass
    with subprocess.Popen(
        [sys.executable, "-c", CRASH_SCRIPT, str(output), phase],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as child:
        assert child.stdout is not None
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=15), "child did not reach real transaction"
        line = child.stdout.readline()
        if line != "TRANSACTION_READY\n":
            child.kill()
            _, error = child.communicate(timeout=10)
            pytest.fail(f"unexpected child output {line!r}: {error}")
        child.kill()
        child.communicate(timeout=10)
        assert child.returncode is not None and child.returncode < 0
    status = read_status(output)
    assert status["generation"] == 2
    if phase == "claim":
        assert records(status, "attempts") == []
        assert records(status, "jobs")[0]["attempt_count"] == 0
    elif phase == "ack":
        assert records(status, "jobs")[0]["status"] == "succeeded"
    else:
        assert records(status, "jobs")[0]["status"] == "running"
    with resume(output, now=103) as j:
        assert j.generation == 3
        exported = list(j.committed())
        assert len(exported) == int(phase in ("ack", "failure"))
        if phase == "ack":
            assert j.all_terminal() and j.claim(now=1000) is None
        elif phase == "failure":
            assert j.all_terminal()
            assert json.loads(exported[0][1])["failure"]["error_type"] == "Interrupted"
        else:
            attempt = j.claim(now=103.1)
            assert attempt is not None
            assert attempt.number == (1 if phase == "claim" else 2)
