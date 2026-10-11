"""Deterministic deadline and retry tests with actual journal transactions."""

import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest

from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.jobs import expand_grid
from distributed_backtester.provenance import current_implementation
from distributed_backtester.scheduler import coordinator
from distributed_backtester.scheduler.coordinator import Clock, run_durable
from distributed_backtester.scheduler.journal import Journal, read_status
from distributed_backtester.scheduler.models import (
    AttemptReply,
    AttemptSpec,
    RetryPolicy,
)
from distributed_backtester.worker import JobFailure, JobOutcome, execute_job


class FakeClock:
    def __init__(self) -> None:
        self.elapsed = 0.0

    def utc(self) -> float:
        return 1000.0 + self.elapsed

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        self.elapsed += seconds


class FakeRuntime:
    name = "test-attempts"
    workers = capacity = 1

    def __init__(self, clock: FakeClock, mode: str = "success") -> None:
        self.clock = clock
        self.mode = mode
        self.diagnostics: dict[str, object] = {}
        self.started = self.closed = False
        self.submitted: list[AttemptSpec] = []
        self.active: AttemptSpec | None = None
        self.cancelled: list[str] = []

    def start(self) -> None:
        self.started = True

    def submit(self, attempt: AttemptSpec) -> None:
        self.submitted.append(attempt)
        self.active = attempt

    def poll(self, timeout: float) -> tuple[AttemptReply, ...]:
        self.clock.sleep(timeout)
        attempt = self.active
        if attempt is None or self.mode == "timeout":
            return ()
        self.active = None
        if self.mode in {"DatasetMismatch", "ImplementationMismatch"}:
            return (
                AttemptReply(
                    attempt.attempt_id,
                    attempt.generation,
                    JobOutcome(
                        attempt.job.job_id,
                        None,
                        JobFailure(self.mode, "identity mismatch"),
                    ),
                    None,
                ),
            )
        if self.mode == "worker_failure" or (
            self.mode == "once" and attempt.number == 1
        ):
            return (
                AttemptReply(
                    attempt.attempt_id,
                    attempt.generation,
                    None,
                    JobFailure("WorkerFailure", "injected worker death"),
                ),
            )
        data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
        reply = AttemptReply(
            attempt.attempt_id,
            attempt.generation,
            execute_job(attempt.job, data, attempt.job.implementation),
            None,
        )
        if self.mode == "wrong":
            return (
                AttemptReply("0" * 64 + ":1", attempt.generation, reply.outcome, None),
            )
        return (reply, reply) if self.mode == "duplicate" else (reply,)

    def cancel(self, attempt_id: str) -> None:
        self.cancelled.append(attempt_id)
        self.active = None

    def close(self) -> None:
        self.closed = True


def make_journal(tmp_path: Path, policy: RetryPolicy) -> Journal:
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    identity = current_implementation()
    jobs, membership = expand_grid([2], [3], 1_000_000_000, 10, data.metadata, identity)
    return Journal.create(
        tmp_path / "experiment",
        manifest_record(jobs, data.metadata, identity, membership),
        policy,
        now=1000.0,
    )


@pytest.mark.parametrize(
    "mode,attempts,failed",
    [
        ("success", 1, 0),
        ("once", 2, 0),
        ("worker_failure", 3, 1),
        ("timeout", 3, 1),
        ("duplicate", 1, 0),
    ],
)
def test_retry_deadline_and_duplicate_delivery(
    tmp_path: Path,
    mode: str,
    attempts: int,
    failed: int,
) -> None:
    clock = FakeClock()
    runtime = FakeRuntime(clock, mode)
    with make_journal(tmp_path, RetryPolicy(3, 0.2, 0.1)) as journal:
        summary = run_durable(
            journal,
            runtime,
            clock=Clock(
                clock.utc,
                clock.monotonic,
                clock.sleep,
            ),
        )
        assert summary.failed == failed
        assert len(runtime.submitted) == attempts
        assert runtime.started and runtime.closed
        assert journal.snapshot()["publication_complete"] is True
        if mode == "timeout":
            assert len(runtime.cancelled) == 3
            assert clock.elapsed < 1.1
    assert read_status(tmp_path / "experiment")["publication_complete"] is True


def test_completed_resume_repairs_exports_without_starting_runtime(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    output = tmp_path / "experiment"
    with make_journal(tmp_path, RetryPolicy()) as journal:
        run_durable(
            journal,
            FakeRuntime(clock),
            clock=Clock(
                clock.utc,
                clock.monotonic,
                clock.sleep,
            ),
        )
    original = (output / "index.json").read_bytes()
    artifact = next((output / "results").glob("*.json"))
    artifact.write_text("partial")
    (output / "index.json").unlink()
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    with Journal.resume(
        output,
        dataset_metadata=data.metadata,
        implementation=current_implementation(),
        now=clock.utc(),
    ) as journal:
        runtime = FakeRuntime(clock)
        run_durable(
            journal,
            runtime,
            clock=Clock(
                clock.utc,
                clock.monotonic,
                clock.sleep,
            ),
        )
        assert not runtime.started
        assert not runtime.submitted
    assert (output / "index.json").read_bytes() == original
    assert json.loads(artifact.read_text())["status"] == "succeeded"


def test_wrong_reply_aborts_without_index_and_closes_runtime(tmp_path: Path) -> None:
    clock = FakeClock()
    runtime = FakeRuntime(clock, "wrong")
    with make_journal(tmp_path, RetryPolicy()) as journal:
        with pytest.raises(RuntimeError, match="unknown"):
            run_durable(
                journal,
                runtime,
                clock=Clock(
                    clock.utc,
                    clock.monotonic,
                    clock.sleep,
                ),
            )
        assert runtime.closed
    assert not (tmp_path / "experiment/index.json").exists()


def test_interrupted_export_keeps_commit_and_resume_repairs_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    original = coordinator._publish

    def partial(content: str, path: Path) -> None:
        if path.parent.name == "results":
            path.parent.mkdir(exist_ok=True)
            path.write_text(content[:40])
            raise OSError("injected interrupted export")
        original(content, path)

    with make_journal(tmp_path, RetryPolicy()) as journal:
        monkeypatch.setattr(coordinator, "_publish", partial)
        with pytest.raises(OSError, match="interrupted export"):
            run_durable(
                journal,
                FakeRuntime(clock),
                clock=Clock(
                    clock.utc,
                    clock.monotonic,
                    clock.sleep,
                ),
            )
        assert journal.all_terminal()
        assert len(list(journal.committed())) == 1
        assert journal.snapshot()["publication_complete"] is False
    output = tmp_path / "experiment"
    assert not (output / "index.json").exists()
    monkeypatch.setattr(coordinator, "_publish", original)
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    with Journal.resume(
        output,
        dataset_metadata=data.metadata,
        implementation=current_implementation(),
        now=clock.utc(),
    ) as journal:
        runtime = FakeRuntime(clock)
        run_durable(
            journal, runtime, clock=Clock(clock.utc, clock.monotonic, clock.sleep)
        )
        assert not runtime.started
    assert (
        json.loads(next((output / "results").glob("*.json")).read_text())["status"]
        == "succeeded"
    )


def test_reply_deadline_uses_receipt_time_before_other_actor_cleanup(
    tmp_path: Path,
) -> None:
    clock = FakeClock()
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    identity = current_implementation()
    jobs, members = expand_grid([1, 2], [3], 1_000_000_000, 10, data.metadata, identity)

    class Runtime(FakeRuntime):
        capacity = 2

        def __init__(self) -> None:
            super().__init__(clock)
            self.pending: dict[str, AttemptSpec] = {}

        def submit(self, attempt: AttemptSpec) -> None:
            self.submitted.append(attempt)
            self.pending[attempt.attempt_id] = attempt
            if attempt.number == 1:
                clock.sleep(0.1)

        def poll(self, timeout: float) -> tuple[AttemptReply, ...]:
            clock.sleep(timeout)
            attempt = list(self.pending.values())[-1]
            self.pending.pop(attempt.attempt_id)
            return (
                AttemptReply(
                    attempt.attempt_id,
                    attempt.generation,
                    execute_job(attempt.job, data, identity),
                    None,
                ),
            )

        def cancel(self, attempt_id: str) -> None:
            self.pending.pop(attempt_id, None)
            clock.sleep(0.2)  # Another worker's termination takes time.

    with Journal.create(
        tmp_path / "experiment",
        manifest_record(jobs, data.metadata, identity, members),
        RetryPolicy(2, 0.2, 0),
        now=clock.utc(),
    ) as journal:
        runtime = Runtime()
        summary = run_durable(
            journal, runtime, clock=Clock(clock.utc, clock.monotonic, clock.sleep)
        )
        assert summary.succeeded == 2
        assert len(runtime.submitted) == 3


def test_storage_and_cleanup_failures_are_both_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()

    class Runtime(FakeRuntime):
        def close(self) -> None:
            super().close()
            raise RuntimeError("injected worker cleanup failed")

    with make_journal(tmp_path, RetryPolicy()) as journal:

        def broken_commit(*args: object, **kwargs: object) -> str:
            raise OSError("injected SQLite write failed")

        monkeypatch.setattr(journal, "commit", broken_commit)
        with pytest.raises(RuntimeError) as error:
            run_durable(
                journal,
                Runtime(clock),
                clock=Clock(
                    clock.utc,
                    clock.monotonic,
                    clock.sleep,
                ),
            )
        assert "SQLite write failed" in str(error.value)
        assert "worker cleanup failed" in str(error.value)
        assert isinstance(error.value.__cause__, OSError)
        assert not (journal.output / "index.json").exists()


def test_exact_deadline_rounding_does_not_strand_a_running_lease(
    tmp_path: Path,
) -> None:
    clock = FakeClock()

    class Runtime(FakeRuntime):
        def submit(self, attempt: AttemptSpec) -> None:
            super().submit(attempt)
            if attempt.number == 1:
                clock.sleep(0.15)

    runtime = Runtime(clock)
    with make_journal(tmp_path, RetryPolicy(2, 0.2, 0)) as journal:
        summary = run_durable(
            journal,
            runtime,
            clock=Clock(
                clock.utc,
                clock.monotonic,
                clock.sleep,
            ),
        )
        assert summary.succeeded == 1
        assert len(runtime.submitted) == 2
        assert len(runtime.cancelled) == 1
        assert journal.all_terminal()


@pytest.mark.parametrize("failure", ["DatasetMismatch", "ImplementationMismatch"])
def test_worker_configuration_mismatch_aborts_without_consuming_retries(
    tmp_path: Path,
    failure: str,
) -> None:
    clock = FakeClock()
    runtime = FakeRuntime(clock, failure)
    with make_journal(tmp_path, RetryPolicy()) as journal:
        with pytest.raises(RuntimeError, match=r"configuration|identity"):
            run_durable(
                journal,
                runtime,
                clock=Clock(
                    clock.utc,
                    clock.monotonic,
                    clock.sleep,
                ),
            )
        assert len(runtime.submitted) == 1
        assert runtime.closed
        assert not list(journal.committed())
        assert not (journal.output / "index.json").exists()


def test_completed_leases_are_resolved_from_journal_not_retained_in_memory(
    tmp_path: Path,
) -> None:
    clock = FakeClock()

    class Runtime(FakeRuntime):
        def submit(self, attempt: AttemptSpec) -> None:
            # Observe actual retention at the dispatch boundary, not a diagnostic
            # counter that could remain bounded while objects accumulate.
            retained = sys._getframe(1).f_locals["leases"]
            assert len(retained) <= self.capacity
            super().submit(attempt)

    runtime = Runtime(clock, "worker_failure")
    with make_journal(tmp_path, RetryPolicy(50, 1, 0)) as journal:
        summary = run_durable(
            journal,
            runtime,
            clock=Clock(
                clock.utc,
                clock.monotonic,
                clock.sleep,
            ),
        )
        assert summary.failed == 1
        assert len(runtime.submitted) == 50


@pytest.mark.parametrize("conflict", [False, True])
def test_duplicate_from_an_earlier_poll_is_resolved_and_checked(
    tmp_path: Path,
    conflict: bool,
) -> None:
    clock = FakeClock()
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    identity = current_implementation()
    jobs, members = expand_grid([1, 2], [3], 1_000_000_000, 10, data.metadata, identity)

    class Runtime(FakeRuntime):
        def __init__(self) -> None:
            super().__init__(clock)
            self.previous: AttemptReply | None = None
            self.duplicated = False

        def poll(self, timeout: float) -> tuple[AttemptReply, ...]:
            if self.previous is not None and not self.duplicated:
                self.duplicated = True
                duplicate = self.previous
                if conflict:
                    assert duplicate.outcome is not None
                    result = duplicate.outcome.result
                    assert result is not None
                    duplicate = replace(
                        duplicate,
                        outcome=replace(
                            duplicate.outcome,
                            result=replace(
                                result,
                                metrics=replace(
                                    result.metrics,
                                    final_equity=result.metrics.final_equity + 1,
                                ),
                            ),
                        ),
                    )
                clock.sleep(timeout)
                return (duplicate,)
            replies = super().poll(timeout)
            if self.previous is None and replies:
                self.previous = replies[0]
            return replies

    with Journal.create(
        tmp_path / "experiment",
        manifest_record(jobs, data.metadata, identity, members),
        RetryPolicy(),
        now=clock.utc(),
    ) as journal:
        runtime = Runtime()
        if conflict:
            with pytest.raises(RuntimeError, match="conflicting"):
                run_durable(
                    journal,
                    runtime,
                    clock=Clock(clock.utc, clock.monotonic, clock.sleep),
                )
            assert not (journal.output / "index.json").exists()
            assert len(list(journal.committed())) == 1
        else:
            summary = run_durable(
                journal, runtime, clock=Clock(clock.utc, clock.monotonic, clock.sleep)
            )
            assert summary.succeeded == 2
            assert len(list(journal.committed())) == 2
        assert runtime.closed
