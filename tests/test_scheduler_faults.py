"""Real Ray deaths and coordinator process restart with a real SQLite journal.

The fault worker is an explicit test injector. Production simulations use the
unchanged engine. Run after freezing all package source files.
"""

import importlib
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from time import monotonic, sleep, time
from typing import cast

import pytest

from distributed_backtester.backends.ray import RayBackend
from distributed_backtester.backends.sequential import SequentialBackend
from distributed_backtester.benchmark import semantic_digest
from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.experiments import collect_experiment
from distributed_backtester.jobs import expand_grid
from distributed_backtester.models import CloseHistory, Portfolio, TargetIntent
from distributed_backtester.provenance import current_implementation
from distributed_backtester.scheduler import ray_runtime
from distributed_backtester.scheduler.coordinator import run_durable
from distributed_backtester.scheduler.experiments import resume_experiment
from distributed_backtester.scheduler.journal import Journal, read_status
from distributed_backtester.scheduler.models import (
    AttemptReply,
    AttemptSpec,
    RetryPolicy,
)
from distributed_backtester.simulation import run_backtest
from distributed_backtester.strategy import MovingAverageCrossover
from distributed_backtester.worker import JobFailure, JobOutcome

_SCRIPT = Path(__file__).resolve()
_SAMPLE = _SCRIPT.parents[1] / "examples/sma.csv"


class _CrashDuringSimulation(MovingAverageCrossover):
    """Run real strategy/engine steps, then terminate during the second close."""

    def on_close(
        self, history: CloseHistory, portfolio: Portfolio
    ) -> TargetIntent | None:
        intent = super().on_close(history, portfolio)
        if len(history) == 2:
            marker = Path(os.environ["PHASE4_FAULT_DIRECTORY"]) / "attempt-1.json"
            marker.write_text(
                json.dumps(
                    {
                        "pid": os.getpid(),
                        "stage": "simulation",
                        "observed_bars": len(history),
                    }
                )
            )
            os._exit(24)
        return intent


class _FaultWorker(ray_runtime._AttemptWorker):
    def execute_attempt(self, attempt: AttemptSpec) -> AttemptReply:
        directory = Path(os.environ["PHASE4_FAULT_DIRECTORY"])
        mode = os.environ["PHASE4_FAULT_MODE"]
        marker = directory / f"attempt-{attempt.number}.json"
        if mode == "repeated":
            return AttemptReply(
                attempt.attempt_id,
                attempt.generation,
                JobOutcome(
                    attempt.job.job_id, None, JobFailure("Injected", "always fails")
                ),
                None,
            )
        if attempt.number == 1:
            marker.write_text(json.dumps({"pid": os.getpid(), "stage": "started"}))
            if mode == "during":
                run_backtest(
                    attempt.job.config,
                    self.dataset,
                    strategy=_CrashDuringSimulation(attempt.job.config.strategy),
                )
                raise AssertionError("simulation did not reach the crash hook")
            if mode == "timeout":
                sleep(30)
            if mode == "lost-reply":
                reply = super().execute_attempt(attempt)
                assert reply.outcome is not None and reply.outcome.result is not None
                marker.write_text(
                    json.dumps(
                        {
                            "pid": os.getpid(),
                            "stage": "finished",
                            "final_equity": reply.outcome.result.metrics.final_equity,
                        }
                    )
                )
                os._exit(25)
        return super().execute_attempt(attempt)


class _BeforeStartRuntime(ray_runtime.RayAttemptRuntime):
    def start(self) -> None:
        super().start()
        info = cast(list[dict[str, object]], self.diagnostics["workers"])[0]
        assert info["jobs_completed"] == 0
        pid = int(str(info["pid"]))
        self._pool.ray.kill(self._pool.actors[0], no_restart=True)
        deadline = monotonic() + 10
        while _exists(pid):
            assert monotonic() < deadline, "killed initial worker did not exit"
            sleep(0.02)


def _exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _child(mode: str, directory: Path, source: str) -> None:
    data = load_csv(_SAMPLE, symbol="SYNTH", price_basis="raw")
    identity = current_implementation()
    assert identity.source_sha256 == source
    jobs, members = expand_grid(
        [1, 2] if mode == "restart" else [2],
        [3],
        1_000_000_000,
        10,
        data.metadata,
        identity,
    )
    manifest = manifest_record(jobs, data.metadata, identity, members)
    os.environ["PHASE4_FAULT_DIRECTORY"] = str(directory)
    os.environ["PHASE4_FAULT_MODE"] = mode
    output = directory / "durable"
    backend = RayBackend(1)
    runtime_type = (
        _BeforeStartRuntime if mode == "before" else ray_runtime.RayAttemptRuntime
    )
    with pytest.MonkeyPatch.context() as patch:
        if mode in {"during", "lost-reply", "timeout", "repeated"}:
            patch.setattr(ray_runtime, "_AttemptWorker", _FaultWorker)
        runtime = runtime_type(backend, data, identity)
        with Journal.create(
            output, manifest, RetryPolicy(3, 5.0, 0), now=time()
        ) as journal:
            if mode == "restart":
                original_submit = runtime.submit
                submitted = 0

                def stop_second(attempt: AttemptSpec) -> None:
                    nonlocal submitted
                    original_submit(attempt)
                    submitted += 1
                    if submitted == 2:
                        (directory / "restart-marker.json").write_text(
                            json.dumps(
                                {
                                    "generation": journal.generation,
                                    "attempt_id": attempt.attempt_id,
                                }
                            )
                        )
                        os._exit(77)

                patch.setattr(runtime, "submit", stop_second)
            summary = run_durable(journal, runtime)
            status = journal.snapshot()
            assert summary.failed == (1 if mode == "repeated" else 0)
    ray = importlib.import_module("ray")
    assert not ray.is_initialized()
    assert runtime.diagnostics["cleanup_errors"] == []
    if mode != "repeated":
        reference = directory / "sequential"
        collect_experiment(
            jobs, data, identity, reference, SequentialBackend(), membership=members
        )
        assert semantic_digest(output) == semantic_digest(reference)
    (directory / "fault-report.json").write_text(
        json.dumps(
            {
                "mode": mode,
                "status": status,
                "diagnostics": runtime.diagnostics,
                "source_sha256": source,
            },
            sort_keys=True,
        )
    )


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX journal and process faults")
@pytest.mark.parametrize(
    "mode", ["before", "during", "lost-reply", "timeout", "repeated", "restart"]
)
def test_real_fault_and_recovery(tmp_path: Path, mode: str) -> None:
    pytest.importorskip("ray")
    source = current_implementation().source_sha256
    log = tmp_path / "child.log"
    with log.open("wb") as stream:
        child = subprocess.Popen(
            [sys.executable, "-W", "error", str(_SCRIPT), mode, str(tmp_path), source],
            stdout=stream,
            stderr=subprocess.STDOUT,
            cwd=tmp_path,
            start_new_session=True,
        )
        try:
            assert child.wait(timeout=120) == (77 if mode == "restart" else 0), (
                log.read_text()
            )
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=10)
    assert "ResourceWarning" not in log.read_text()
    assert "Exception ignored" not in log.read_text()
    output = tmp_path / "durable"
    if mode == "restart":
        status = read_status(output)
        assert status["counts"] == {
            "pending": 0,
            "running": 1,
            "succeeded": 1,
            "failed": 0,
        }
        assert not (output / "index.json").exists()
        summary = resume_experiment(_SAMPLE, output=output, backend=RayBackend(1))
        assert summary.succeeded == 2
        status = read_status(output)
        assert status["generation"] == 2
        data = load_csv(_SAMPLE, symbol="SYNTH", price_basis="raw")
        identity = current_implementation()
        jobs, members = expand_grid(
            [1, 2], [3], 1_000_000_000, 10, data.metadata, identity
        )
        collect_experiment(
            jobs,
            data,
            identity,
            tmp_path / "sequential",
            SequentialBackend(),
            membership=members,
        )
        assert semantic_digest(output) == semantic_digest(tmp_path / "sequential")
    else:
        report = json.loads((tmp_path / "fault-report.json").read_text())
        assert report["source_sha256"] == source
        status = report["status"]
        assert status["publication_complete"] is True
        assert len(status["attempts"]) == (3 if mode == "repeated" else 2)
        if mode == "lost-reply":
            assert (
                json.loads((tmp_path / "attempt-1.json").read_text())["stage"]
                == "finished"
            )
        elif mode == "during":
            marker = json.loads((tmp_path / "attempt-1.json").read_text())
            assert marker["stage"] == "simulation"
            assert marker["observed_bars"] == 2
        elif mode == "before":
            assert report["diagnostics"]["replacements"] == 1
        elif mode == "timeout":
            marker = json.loads((tmp_path / "attempt-1.json").read_text())
            assert marker["stage"] == "started"
            first, second = status["attempts"]
            assert first["reason"] == "timeout"
            assert first["failure"]["error_type"] == "JobTimeout"
            assert second["status"] == "succeeded"
            assert report["diagnostics"]["retirements"] == 1
            assert report["diagnostics"]["replacements"] == 1


if __name__ == "__main__":
    _child(sys.argv[1], Path(sys.argv[2]), sys.argv[3])
