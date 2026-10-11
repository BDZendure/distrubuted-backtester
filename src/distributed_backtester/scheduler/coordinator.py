"""A single coordinator owns attempt deadlines and durable result commits."""

import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import monotonic, perf_counter, sleep, time

from distributed_backtester.experiment_artifacts import index_record
from distributed_backtester.experiments import ExperimentSummary
from distributed_backtester.jobs import canonical_json
from distributed_backtester.provenance import capture_environment
from distributed_backtester.results import write_text
from distributed_backtester.scheduler.journal import Journal
from distributed_backtester.scheduler.models import AttemptRuntime, AttemptSpec
from distributed_backtester.worker import JobFailure


@dataclass(frozen=True, slots=True)
class Clock:
    utc: Callable[[], float] = time
    monotonic: Callable[[], float] = monotonic
    sleep: Callable[[float], None] = sleep


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _publish(content: str, path: Path) -> None:
    if path.parent.is_symlink():
        raise ValueError("derived export directory must not be a symlink")
    write_text(content, path, overwrite=True)
    _sync_directory(path.parent)


def _export(
    journal: Journal, runtime: AttemptRuntime, elapsed: float
) -> ExperimentSummary:
    output = journal.output
    index_path = output / "index.json"
    index_path.unlink(missing_ok=True)
    _sync_directory(output)
    _publish(canonical_json(journal.manifest) + "\n", output / "manifest.json")
    records: list[dict[str, object]] = []
    for _job_id, content, record in journal.committed():
        _publish(content, output / str(record["artifact"]))
        records.append(record)
    experiment_id = str(journal.manifest["experiment_id"])
    index = index_record(experiment_id, records)
    execution_path = output / "execution.json"
    _publish(
        canonical_json(
            {
                "schema_version": 1,
                "experiment_id": experiment_id,
                "backend": runtime.name,
                "workers": runtime.workers,
                "capacity": runtime.capacity,
                "backend_diagnostics": runtime.diagnostics,
                "elapsed_before_finalization_seconds": elapsed,
                "environment": capture_environment(),
                "generation": journal.generation,
                "retry_policy": journal.policy.to_dict(),
            }
        )
        + "\n",
        execution_path,
    )
    _publish(canonical_json(index) + "\n", index_path)
    journal.mark_complete()
    return ExperimentSummary(
        experiment_id,
        len(records),
        int(str(index["succeeded"])),
        int(str(index["failed"])),
        index_path,
        execution_path,
        elapsed,
    )


def run_durable(
    journal: Journal,
    runtime: AttemptRuntime,
    *,
    clock: Clock | None = None,
) -> ExperimentSummary:
    """Run pending work, then repair/publish exports from authoritative commits.

    The caller owns the journal session and must close it. The runtime is closed
    on every path. Fatal setup, protocol, and storage errors leave resumable state.
    A reply must reach the coordinator before the attempt deadline.
    """
    clock = Clock() if clock is None else clock
    started = perf_counter()
    leases: dict[str, AttemptSpec] = {}
    active: dict[str, float] = {}
    try:
        if not journal.all_terminal():
            runtime.start()
            if type(runtime.capacity) is not int or runtime.capacity < 1:
                raise RuntimeError("attempt runtime needs positive capacity")
            origin_utc, origin_mono = clock.utc(), clock.monotonic()

            def now() -> float:
                return origin_utc + (clock.monotonic() - origin_mono)

            while not journal.all_terminal():
                while len(active) < runtime.capacity:
                    claiming_mono = clock.monotonic()
                    attempt = journal.claim(
                        now=origin_utc + (claiming_mono - origin_mono)
                    )
                    if attempt is None:
                        break
                    leases[attempt.attempt_id] = attempt
                    active[attempt.attempt_id] = (
                        claiming_mono + journal.policy.job_timeout_seconds
                    )
                    runtime.submit(attempt)
                if not active:
                    eligible = journal.next_eligible()
                    if eligible is None:
                        raise RuntimeError("nonterminal journal has no runnable work")
                    clock.sleep(min(0.05, max(0.001, eligible - now())))
                    continue
                wait = max(0.0, min(0.05, min(active.values()) - clock.monotonic()))
                replies = runtime.poll(wait)
                observed = clock.monotonic()
                received_utc = origin_utc + (observed - origin_mono)
                for attempt_id, deadline in tuple(active.items()):
                    if (
                        observed >= deadline
                        or received_utc >= leases[attempt_id].deadline_utc
                    ):
                        runtime.cancel(attempt_id)
                        journal.fail(
                            leases[attempt_id],
                            JobFailure("JobTimeout", "attempt deadline expired"),
                            now=now(),
                            reason="timeout",
                        )
                        del active[attempt_id]
                        del leases[attempt_id]
                for reply in replies:
                    attempt = leases.get(reply.attempt_id)
                    if attempt is None:
                        attempt = journal.find_attempt(reply.attempt_id)
                    if attempt is None or reply.generation != attempt.generation:
                        raise RuntimeError(
                            "unknown attempt or generation in worker reply"
                        )
                    if reply.outcome is not None:
                        if reply.outcome.job_id != attempt.job.job_id:
                            raise RuntimeError("worker reply does not match job")
                        if reply.outcome.failure is None:
                            committed = journal.commit(
                                attempt, reply.outcome, now=received_utc
                            )
                            if committed == "stale" and reply.attempt_id in active:
                                runtime.cancel(reply.attempt_id)
                                journal.fail(
                                    attempt,
                                    JobFailure(
                                        "JobTimeout", "attempt deadline expired"
                                    ),
                                    now=now(),
                                    reason="timeout",
                                )
                        else:
                            if reply.outcome.failure.error_type in {
                                "DatasetMismatch",
                                "ImplementationMismatch",
                            }:
                                raise RuntimeError(
                                    "worker configuration identity mismatch: "
                                    f"{reply.outcome.failure.error_type}: "
                                    f"{reply.outcome.failure.message}"
                                )
                            journal.fail(
                                attempt, reply.outcome.failure, now=received_utc
                            )
                    elif reply.failure is not None:
                        journal.fail(
                            attempt,
                            reply.failure,
                            now=received_utc,
                            reason="worker_failure",
                            diagnostic=reply.diagnostic,
                        )
                    else:
                        raise RuntimeError("worker reply contains no outcome")
                    active.pop(reply.attempt_id, None)
                    leases.pop(reply.attempt_id, None)
    except BaseException as primary:
        try:
            runtime.close()
        except Exception as cleanup:
            raise RuntimeError(
                f"{type(primary).__name__}: {primary}; "
                f"runtime cleanup failed: {cleanup}"
            ) from primary
        raise
    else:
        runtime.close()
    return _export(journal, runtime, perf_counter() - started)
