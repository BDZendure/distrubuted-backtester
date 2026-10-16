"""A labeled in-process attempt runtime tests coordinator observation ordering."""

import io
import json
from pathlib import Path

import pytest

from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.jobs import expand_grid
from distributed_backtester.models import MarketDataset
from distributed_backtester.operational_logging import log_session
from distributed_backtester.provenance import (
    ImplementationIdentity,
    current_implementation,
)
from distributed_backtester.scheduler.coordinator import run_durable
from distributed_backtester.scheduler.journal import Journal, read_status
from distributed_backtester.scheduler.models import (
    AttemptReply,
    AttemptSpec,
    RetryPolicy,
)
from distributed_backtester.worker import execute_job


class InProcessRuntime:
    """Test runtime only, without Ray transport or any retry simulation."""

    name = "test-inprocess"
    workers = capacity = 1

    def __init__(
        self, dataset: MarketDataset, identity: ImplementationIdentity
    ) -> None:
        self.dataset, self.identity = dataset, identity
        self.diagnostics: dict[str, object] = {}
        self.replies: list[AttemptReply] = []
        self.closed = False

    def start(self) -> None:
        pass

    def submit(self, attempt: AttemptSpec) -> None:
        self.replies.append(
            AttemptReply(
                attempt.attempt_id,
                attempt.generation,
                execute_job(attempt.job, self.dataset, self.identity),
                None,
            )
        )

    def poll(self, timeout: float) -> tuple[AttemptReply, ...]:
        replies = tuple(self.replies)
        self.replies.clear()
        return replies

    def cancel(self, attempt_id: str) -> None:
        pass

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("fail_export", [False, True])
def test_durable_log_commit_precedes_export_and_keeps_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_export: bool
) -> None:
    dataset = load_csv(Path("examples/sma.csv"), symbol="SYNTH", price_basis="raw")
    identity = current_implementation()
    jobs, _ = expand_grid([2], [3], 1_000_000_000, 10, dataset.metadata, identity)
    output = tmp_path / "durable"
    runtime = InProcessRuntime(dataset, identity)
    stream = io.StringIO()

    if fail_export:

        def failed_export(*args: object, **kwargs: object) -> None:
            raise OSError("injected export failure")

        monkeypatch.setattr(
            "distributed_backtester.scheduler.coordinator._export", failed_export
        )

    with (
        Journal.create(
            output,
            manifest_record(jobs, dataset.metadata, identity),
            RetryPolicy(),
            now=1000,
        ) as journal,
        log_session("INFO", stream=stream),
    ):
        if fail_export:
            with pytest.raises(OSError, match="injected export failure"):
                run_durable(journal, runtime)
        else:
            assert run_durable(journal, runtime).succeeded == 1
    assert runtime.closed
    status = read_status(output)
    assert status["counts"] == {"pending": 0, "running": 0, "succeeded": 1, "failed": 0}
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    events = [record["event"] for record in records]
    assert events[0] == "durable_started"
    claim = next(record for record in records if record["event"] == "attempt_claimed")
    commit = next(record for record in records if record["event"] == "result_committed")
    assert claim["experiment_id"] == commit["experiment_id"] == status["experiment_id"]
    assert claim["attempt_id"] == commit["attempt_id"]
    assert claim["job_id"] == commit["job_id"] == jobs[0].job_id
    assert claim["generation"] == commit["generation"] == 1
    assert claim["attempt_number"] == 1
    assert commit["disposition"] == "committed"
    assert events[-1] == ("durable_failed" if fail_export else "durable_finished")
    assert bool(status["publication_complete"]) is not fail_export
