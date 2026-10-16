"""Operational observations cannot change output or caller logging ownership."""

import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pytest


def test_json_lines_escape_messages_and_include_context() -> None:
    from distributed_backtester.operational_logging import (
        emit_event,
        event_context,
        log_session,
    )

    stream = io.StringIO()
    with log_session("INFO", stream=stream), event_context(experiment_id="experiment"):
        emit_event("job_started", job_id="job", detail='line\n"quoted"')
    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["schema_version"] == 1
    assert record["event"] == "job_started"
    assert record["level"] == "INFO"
    assert record["pid"] == os.getpid()
    assert record["experiment_id"] == "experiment"
    assert record["job_id"] == "job"
    assert record["detail"] == 'line\n"quoted"'
    assert datetime.fromisoformat(record["timestamp"]).utcoffset() is not None


@pytest.mark.parametrize("level", ["OFF", "ERROR", "WARNING", "INFO", "DEBUG"])
def test_severity_filtering(level: str) -> None:
    from distributed_backtester.operational_logging import emit_event, log_session

    stream = io.StringIO()
    with log_session(level, stream=stream):
        for severity in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            emit_event(severity.lower(), level=severity)
    actual = [json.loads(line)["level"] for line in stream.getvalue().splitlines()]
    expected = {
        "OFF": [],
        "ERROR": ["ERROR"],
        "WARNING": ["WARNING", "ERROR"],
        "INFO": ["INFO", "WARNING", "ERROR"],
        "DEBUG": ["DEBUG", "INFO", "WARNING", "ERROR"],
    }
    assert actual == expected[level]


def test_nested_context_and_handler_restoration_on_failure() -> None:
    from distributed_backtester.operational_logging import (
        capture_logging,
        emit_event,
        event_context,
        log_session,
    )

    logger = logging.getLogger("distributed_backtester.operations")
    original = (logger.handlers[:], logger.level, logger.propagate, logger.disabled)
    outer, inner = io.StringIO(), io.StringIO()
    with log_session("INFO", stream=outer), event_context(experiment_id="outer"):
        state = capture_logging()
        with (
            pytest.raises(RuntimeError, match="injected"),
            log_session("DEBUG", stream=inner),
            event_context(attempt_id="inner"),
        ):
            emit_event("inside")
            raise RuntimeError("injected")
        assert capture_logging() == state
        emit_event("after")
    assert (
        logger.handlers,
        logger.level,
        logger.propagate,
        logger.disabled,
    ) == original
    assert json.loads(inner.getvalue())["attempt_id"] == "inner"
    restored = json.loads(outer.getvalue())
    assert restored["experiment_id"] == "outer"
    assert "attempt_id" not in restored
    assert capture_logging().level == "OFF"


def test_captured_worker_state_is_independent_and_serializable() -> None:
    import pickle

    from distributed_backtester.operational_logging import (
        capture_logging,
        emit_event,
        event_context,
        log_session,
    )

    stream = io.StringIO()
    with log_session("INFO", stream=stream), event_context(experiment_id="run"):
        state = pickle.loads(pickle.dumps(capture_logging()))
        with event_context(job_id="later"):
            assert "job_id" not in state.context
        with log_session(state.level, stream=stream), event_context(**state.context):
            emit_event("worker")
    assert json.loads(stream.getvalue())["experiment_id"] == "run"


def test_failed_log_write_does_not_replace_workload_failure() -> None:
    from distributed_backtester.operational_logging import emit_event, log_session

    class BrokenStream(io.StringIO):
        def write(self, value: str) -> int:
            raise OSError("injected log write failure")

    with log_session("INFO", stream=BrokenStream()):
        emit_event("started")
        with pytest.raises(RuntimeError, match="primary workload error"):
            try:
                raise RuntimeError("primary workload error")
            finally:
                emit_event("failed", level="ERROR", error_type="RuntimeError")


@pytest.mark.parametrize("level", ["info", "TRACE", "", "CRITICAL"])
def test_invalid_level_is_rejected(level: str) -> None:
    from distributed_backtester.operational_logging import log_session

    with pytest.raises(ValueError, match="level"), log_session(level):
        pass


def test_reserved_fields_cannot_override_record_identity() -> None:
    from distributed_backtester.operational_logging import event_context

    with pytest.raises(ValueError, match="reserved"), event_context(pid=-1):
        pass


def test_real_worker_job_events_correlate_with_experiment() -> None:
    from distributed_backtester.data import load_csv
    from distributed_backtester.jobs import expand_grid
    from distributed_backtester.operational_logging import (
        emit_event,
        event_context,
        log_session,
    )
    from distributed_backtester.provenance import current_implementation
    from distributed_backtester.worker import execute_job

    dataset = load_csv(Path("examples/sma.csv"), symbol="SYNTH", price_basis="raw")
    identity = current_implementation()
    jobs, _ = expand_grid([2], [3], 1_000_000_000, 10, dataset.metadata, identity)
    stream = io.StringIO()
    with log_session("INFO", stream=stream), event_context(experiment_id="experiment"):
        outcome = execute_job(jobs[0], dataset, identity)
        emit_event("test_finished")
    assert outcome.failure is None
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [record["event"] for record in records] == [
        "job_started",
        "job_finished",
        "test_finished",
    ]
    assert all(record["experiment_id"] == "experiment" for record in records)
    assert all(record["job_id"] == jobs[0].job_id for record in records[:2])
    assert "job_id" not in records[-1]


def test_real_classic_experiment_events_follow_publication(tmp_path: Path) -> None:
    from distributed_backtester.backends.sequential import SequentialBackend
    from distributed_backtester.experiments import run_experiment
    from distributed_backtester.operational_logging import log_session

    stream = io.StringIO()
    with log_session("INFO", stream=stream):
        summary = run_experiment(
            Path("examples/sma.csv"),
            symbol="SYNTH",
            fast=[2],
            slow=[3],
            initial_cash=1_000_000_000,
            fee_bps=10,
            output=tmp_path / "run",
            backend=SequentialBackend(),
        )
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert [record["event"] for record in records] == [
        "experiment_started",
        "job_started",
        "job_finished",
        "job_exported",
        "experiment_finished",
    ]
    assert all(record["experiment_id"] == summary.experiment_id for record in records)
    assert summary.index_path.exists()


def test_worker_session_restores_parent_context() -> None:
    from distributed_backtester.operational_logging import (
        LogState,
        capture_logging,
        event_context,
        log_session,
        worker_session,
    )

    with (
        log_session("INFO", stream=io.StringIO()),
        event_context(experiment_id="outer"),
    ):
        before = capture_logging()
        with worker_session(LogState("DEBUG", {"experiment_id": "worker"})):
            assert capture_logging().context["experiment_id"] == "worker"
        assert capture_logging() == before
