import json
import os
import subprocess
import sys
from pathlib import Path
from typing import NoReturn

import pytest

from distributed_backtester.cli import main
from distributed_backtester.models import BacktestConfig, MarketDataset

SOURCE = Path(__file__).resolve().parents[1] / "examples/sma.csv"


def arguments(output: Path) -> list[str]:
    return [
        "sweep",
        "--data",
        str(SOURCE),
        "--symbol",
        "SYNTH",
        "--price-basis",
        "raw",
        "--fast",
        "1",
        "2",
        "1",
        "--slow",
        "3",
        "4",
        "--fee-bps",
        "10",
        "--output-dir",
        str(output),
    ]


@pytest.mark.parametrize("backend,workers", [("sequential", "1"), ("process", "2")])
def test_sweep_runs_from_outside_repository(
    tmp_path: Path, backend: str, workers: str
) -> None:
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    output = tmp_path / backend
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "distributed_backtester",
            *arguments(output),
            "--backend",
            backend,
            "--workers",
            workers,
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    summary = json.loads(completed.stdout)
    assert summary["submitted"] == summary["succeeded"] == 4
    assert summary["failed"] == 0
    assert (output / "index.json").exists()


@pytest.mark.parametrize(
    "extra",
    [
        ["--workers", "2"],
        ["--max-in-flight", "2"],
        ["--backend", "process", "--workers", "0"],
        ["--fast", "3", "--slow", "3"],
    ],
)
def test_invalid_sweep_configuration_returns_two(
    tmp_path: Path, extra: list[str]
) -> None:
    output = tmp_path / "invalid"
    assert main([*arguments(output), *extra]) == 2
    assert not output.exists()


def test_existing_directory_error_explains_new_destination(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(arguments(tmp_path)) == 2
    assert "new output directory" in capsys.readouterr().err


def test_injected_job_failures_return_one_and_explicit_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Inject only simulation failure; the CLI, worker, collector, and files are real.
    def failure(config: BacktestConfig, data: MarketDataset) -> NoReturn:
        raise ValueError("injected simulation failure")

    monkeypatch.setattr("distributed_backtester.worker.run_backtest", failure)
    output = tmp_path / "failed-jobs"
    assert main(arguments(output)) == 1
    summary = json.loads(capsys.readouterr().out)
    assert summary["submitted"] == summary["failed"] == 4
    assert summary["succeeded"] == 0
    index = json.loads((output / "index.json").read_text())
    assert all(
        record["failure"]["error_type"] == "ValueError" for record in index["outcomes"]
    )
