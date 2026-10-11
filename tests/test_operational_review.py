"""Regressions reproduced at the Phase 7 operational ownership boundaries."""

import io
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from distributed_backtester.cli import _parser, main
from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.jobs import expand_grid
from distributed_backtester.operational_logging import emit_event, log_session
from distributed_backtester.provenance import current_implementation
from distributed_backtester.scheduler.journal import Journal
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.worker import execute_job


def test_explicit_resources_replace_file_defaults_and_append_only_explicit() -> None:
    defaults: dict[str, object] = {"worker_resource": ["tag=1"]}
    parser = _parser("sweep", defaults)
    arguments = [
        "sweep",
        "--data",
        "examples/sma.csv",
        "--symbol",
        "SYNTH",
        "--price-basis",
        "raw",
        "--fast",
        "2",
        "--slow",
        "3",
        "--output-dir",
        "unused",
    ]
    explicit = parser.parse_args(
        [*arguments, "--worker-resource", "tag=2", "--worker-resource", "other=3"]
    )
    assert explicit.worker_resource == ["tag=2", "other=3"]
    assert defaults == {"worker_resource": ["tag=1"]}
    assert parser.parse_args(arguments).worker_resource == ["tag=1"]
    assert parser.parse_args(
        [*arguments, "--worker-resource", "tag=4"]
    ).worker_resource == ["tag=4"]


@pytest.mark.parametrize("boundary", ["filter", "factory"])
def test_logging_infrastructure_cannot_replace_primary_error(
    monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    class FailingFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            raise RuntimeError("logging infrastructure failure")

    def failed_factory(*args: object, **kwargs: object) -> logging.LogRecord:
        raise RuntimeError("logging infrastructure failure")

    logger = logging.getLogger("distributed_backtester.operations")
    if boundary == "filter":
        monkeypatch.setattr(logger, "filters", [FailingFilter()])
    else:
        monkeypatch.setattr(logging, "_logRecordFactory", failed_factory)
    with log_session("INFO", stream=io.StringIO()):
        emit_event("started")
        with pytest.raises(ValueError, match="primary workload error"):
            try:
                raise ValueError("primary workload error")
            finally:
                emit_event("failed", level="ERROR")


@pytest.mark.parametrize("cleanup_note", [False, True])
def test_keyboard_interrupt_during_scope_entry_returns_130(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    cleanup_note: bool,
) -> None:
    def interrupt() -> None:
        error = KeyboardInterrupt()
        if cleanup_note:
            error.add_note("cleanup diagnostic")
        raise error

    @contextmanager
    def interrupted_entry() -> Iterator[None]:
        interrupt()
        yield

    monkeypatch.setattr("distributed_backtester.cli.cli_signals", interrupted_entry)
    assert main(["status", "--output-dir", "unused"]) == 130
    diagnostic = capsys.readouterr().err
    assert "interrupted" in diagnostic
    if cleanup_note:
        assert "cleanup diagnostic" in diagnostic


@pytest.mark.parametrize(
    "target", ["manifest.json", "execution.json", "index.json", "result"]
)
@pytest.mark.parametrize("alias", ["direct", "symlink", "hardlink"])
def test_resume_preserves_config_aliasing_mutable_export(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], target: str, alias: str
) -> None:
    source = Path("examples/sma.csv").resolve()
    dataset = load_csv(source, symbol="SYNTH", price_basis="raw")
    identity = current_implementation()
    jobs, _ = expand_grid([2], [3], 1_000_000_000, 10, dataset.metadata, identity)
    output = tmp_path / "durable"
    with Journal.create(
        output,
        manifest_record(jobs, dataset.metadata, identity),
        RetryPolicy(),
        now=1000,
    ) as journal:
        attempt = journal.claim(now=1000)
        assert attempt is not None
        assert (
            journal.commit(attempt, execute_job(jobs[0], dataset, identity), now=1001)
            == "committed"
        )
    destination = output / (
        f"results/{jobs[0].job_id}.json" if target == "result" else target
    )
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(
        f'schema_version=1\ncommand="resume"\ndata="{source}"\noutput_dir="{output}"\n'
    )
    config = destination
    if alias != "direct":
        config = tmp_path / "aliased.toml"
        if alias == "symlink":
            config.symlink_to(destination)
        else:
            config.hardlink_to(destination)
    original = config.read_bytes()
    assert main(["resume", "--config", str(config)]) == 2
    assert config.read_bytes() == original
    assert destination.read_bytes() == original
    assert "config" in capsys.readouterr().err.lower()
