"""Durable command validation does not initialize Ray or mutate old journals."""

import json
import sqlite3
from pathlib import Path

import pytest

from distributed_backtester.backends import ray as ray_backend
from distributed_backtester.cli import main
from distributed_backtester.data import load_csv
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.jobs import expand_grid
from distributed_backtester.provenance import current_implementation
from distributed_backtester.scheduler.journal import Journal
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.worker import execute_job


@pytest.mark.parametrize(
    "flags",
    [
        ["--durable"],
        ["--max-attempts", "3"],
        ["--job-timeout", "10"],
        ["--retry-delay", "0"],
        ["--durable", "--backend", "process", "--workers", "2"],
        ["--durable", "--backend", "ray", "--max-attempts", "0"],
        ["--durable", "--backend", "ray", "--job-timeout", "nan"],
    ],
)
def test_invalid_durability_flags_do_not_create_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    flags: list[str],
) -> None:
    output = tmp_path / "new"
    result = main(
        [
            "sweep",
            "--data",
            "examples/sma.csv",
            "--symbol",
            "SMA",
            "--price-basis",
            "raw",
            "--fast",
            "2",
            "--slow",
            "3",
            "--output-dir",
            str(output),
            *flags,
        ]
    )
    assert result == 2
    assert not output.exists()
    assert "error:" in capsys.readouterr().err


def test_readonly_status_with_active_writer_and_lock_exclusion(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    identity = current_implementation()
    jobs, members = expand_grid([2], [3], 1_000_000_000, 10, data.metadata, identity)
    output = tmp_path / "journal"
    with Journal.create(
        output,
        manifest_record(jobs, data.metadata, identity, members),
        RetryPolicy(),
        now=1000,
    ):
        assert main(["status", "--output-dir", str(output)]) == 0
        status = json.loads(capsys.readouterr().out)
        assert status["counts"]["pending"] == 1
        assert status["generation"] == 1
        assert (
            main(["resume", "--data", "examples/sma.csv", "--output-dir", str(output)])
            == 2
        )
        assert "coordinator" in capsys.readouterr().err.lower()


def test_status_missing_journal_does_not_create_directory(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["status", "--output-dir", str(tmp_path / "absent")]) == 2
    assert "error:" in capsys.readouterr().err
    assert not (tmp_path / "absent").exists()


def test_completed_cli_resume_never_initializes_ray(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    data = load_csv(Path("examples/sma.csv"), symbol="SMA", price_basis="raw")
    identity = current_implementation()
    jobs, _ = expand_grid([2], [3], 1_000_000_000, 10, data.metadata, identity)
    output = tmp_path / "complete"
    with Journal.create(
        output,
        manifest_record(jobs, data.metadata, identity),
        RetryPolicy(),
        now=1000,
    ) as journal:
        attempt = journal.claim(now=1000)
        assert attempt is not None
        journal.commit(attempt, execute_job(attempt.job, data, identity), now=1000)

    def forbid_ray() -> None:
        raise AssertionError("completed resume tried to import Ray")

    monkeypatch.setattr(ray_backend, "_require_ray", forbid_ray)
    assert (
        main(["resume", "--data", "examples/sma.csv", "--output-dir", str(output)]) == 0
    )
    assert json.loads(capsys.readouterr().out)["succeeded"] == 1
    assert (output / "index.json").is_file()


@pytest.mark.parametrize("command", ["status", "resume"])
def test_corrupt_journal_is_a_cli_error_without_mutation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    command: str,
) -> None:
    output = tmp_path / "corrupt"
    output.mkdir()
    database = output / "journal.sqlite3"
    database.write_bytes(b"this is not a SQLite database")
    before = database.read_bytes()
    arguments = [command, "--output-dir", str(output)]
    if command == "resume":
        arguments += ["--data", "examples/sma.csv"]
    assert main(arguments) == 2
    assert "error:" in capsys.readouterr().err
    assert database.read_bytes() == before
    assert not (output / "coordinator.lock").exists()


def test_sqlite_operational_failure_is_reported(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def failed_read(output: Path) -> dict[str, object]:
        raise sqlite3.OperationalError("injected database read failed")

    monkeypatch.setattr("distributed_backtester.cli.read_status", failed_read)
    assert main(["status", "--output-dir", "unused"]) == 2
    assert "database read failed" in capsys.readouterr().err
