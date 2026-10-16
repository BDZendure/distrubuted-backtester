"""Real conversion, simulation, version checks, and analytical CLI boundaries."""

import importlib
import json
from pathlib import Path

import pytest

from distributed_backtester.cli import main
from distributed_backtester.experiment_artifacts import manifest_record
from distributed_backtester.jobs import expand_grid
from distributed_backtester.market_data import load_dataset
from distributed_backtester.provenance import current_implementation
from distributed_backtester.scheduler.journal import Journal, read_status
from distributed_backtester.scheduler.models import RetryPolicy
from distributed_backtester.worker import execute_job

_SAMPLE = Path(__file__).parents[1] / "examples/sma.csv"


def _convert(path: Path) -> list[str]:
    return [
        "convert-data",
        "--data",
        str(_SAMPLE),
        "--symbol",
        "SYNTH",
        "--price-basis",
        "raw",
        "--output",
        str(path),
        "--row-group-size",
        "2",
    ]


def test_conversion_and_backtest_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.parquet"
    assert main(_convert(path)) == 0
    report = json.loads(capsys.readouterr().out)
    dataset = load_dataset(path, symbol="SYNTH", price_basis="raw")
    assert report["dataset_version"] == dataset.metadata.version_id
    assert report["source_sha256"] == dataset.metadata.source_sha256
    args = [
        "backtest",
        "--data",
        str(path),
        "--symbol",
        "SYNTH",
        "--price-basis",
        "raw",
        "--fee-bps",
        "10",
        "--dataset-version",
        dataset.metadata.version_id,
    ]
    assert main(args) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["metrics"]["final_equity"] == "749.257000"
    assert result["dataset"]["source_format"] == "parquet"
    assert result["dataset"]["parent_source_sha256"] == report["parent_source_sha256"]
    assert main([*args[:-1], "0" * 64]) == 2
    assert "version" in capsys.readouterr().err


def test_conversion_protection_and_overwrite(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.parquet"
    assert main(_convert(path)) == 0
    capsys.readouterr()
    before = path.read_bytes()
    assert main(_convert(path)) == 2
    assert "output exists" in capsys.readouterr().err
    assert path.read_bytes() == before
    assert main([*_convert(path), "--overwrite"]) == 0
    capsys.readouterr()
    csv = tmp_path / "source.csv"
    csv.write_bytes(_SAMPLE.read_bytes())
    args = _convert(csv)
    args[2] = str(csv)
    assert main([*args, "--overwrite"]) == 2
    assert csv.read_bytes() == _SAMPLE.read_bytes()
    assert "input" in capsys.readouterr().err


def test_missing_arrow_has_actionable_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    original = importlib.import_module

    def missing(name: str, package: str | None = None) -> object:
        if name.startswith("pyarrow"):
            raise ImportError("not installed")
        return original(name, package)

    monkeypatch.setattr(importlib, "import_module", missing)
    assert main(_convert(tmp_path / "absent/file.parquet")) == 2
    assert "storage" in capsys.readouterr().err
    assert not (tmp_path / "absent").exists()


def test_parquet_sweep_and_analytics_cli(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("pyarrow")
    pytest.importorskip("duckdb")
    path = tmp_path / "data.parquet"
    assert main(_convert(path)) == 0
    capsys.readouterr()
    output = tmp_path / "experiment"
    assert (
        main(
            [
                "sweep",
                "--data",
                str(path),
                "--symbol",
                "SYNTH",
                "--price-basis",
                "raw",
                "--fast",
                "1",
                "2",
                "--slow",
                "3",
                "4",
                "--output-dir",
                str(output),
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["succeeded"] == 4
    execution = json.loads((output / "execution.json").read_text())
    assert execution["timings"]["parquet_load_seconds"] >= 0
    assert "csv_load_seconds" not in execution["timings"]
    database = tmp_path / "results.duckdb"
    args = [
        "results-import",
        "--experiment-dir",
        str(output),
        "--database",
        str(database),
    ]
    assert main(args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["submitted"] == 4
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["inserted"] is False
    assert (
        main(
            [
                "results-query",
                "--database",
                str(database),
                "--snapshot-id",
                first["snapshot_id"],
                "--limit",
                "2",
            ]
        )
        == 0
    )
    assert len(json.loads(capsys.readouterr().out)["outcomes"]) == 2


def test_completed_parquet_resume_checks_identity_before_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("pyarrow")
    path = tmp_path / "data.parquet"
    assert main(_convert(path)) == 0
    capsys.readouterr()
    dataset = load_dataset(path, symbol="SYNTH", price_basis="raw")
    identity = current_implementation()
    jobs, membership = expand_grid(
        [2], [3], 1_000_000_000, 10, dataset.metadata, identity
    )
    output = tmp_path / "durable"
    with Journal.create(
        output,
        manifest_record(jobs, dataset.metadata, identity, membership),
        RetryPolicy(),
        now=1000,
    ) as journal:
        attempt = journal.claim(now=1000)
        assert attempt is not None
        journal.commit(attempt, execute_job(attempt.job, dataset, identity), now=1000)

    def forbidden() -> None:
        raise AssertionError("completed resume must not initialize Ray")

    monkeypatch.setattr("distributed_backtester.backends.ray._require_ray", forbidden)
    before = read_status(output)
    args = ["resume", "--data", str(path), "--output-dir", str(output)]
    assert main([*args, "--dataset-version", "0" * 64]) == 2
    assert "version" in capsys.readouterr().err
    assert read_status(output) == before

    renamed = path.with_suffix(".binary")
    path.rename(renamed)
    args[2] = str(renamed)
    assert main(args) == 0
    capsys.readouterr()
    assert read_status(output)["attempts"] == before["attempts"]

    # Another valid layout has equal bars/version but different physical bytes.
    from distributed_backtester.data import load_csv
    from distributed_backtester.parquet_data import write_parquet

    original = load_csv(_SAMPLE, symbol="SYNTH", price_basis="raw")
    write_parquet(original, renamed, row_group_size=3, overwrite=True)
    after = read_status(output)
    assert main(args) == 2
    assert "dataset" in capsys.readouterr().err
    assert read_status(output) == after


def test_missing_duckdb_is_a_cli_error_without_creating_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    original = importlib.import_module

    def missing(name: str, package: str | None = None) -> object:
        if name == "duckdb":
            raise ImportError("not installed")
        return original(name, package)

    monkeypatch.setattr(importlib, "import_module", missing)
    database = tmp_path / "new/results.duckdb"
    assert main(["results-query", "--database", str(database)]) == 2
    assert "DuckDB" in capsys.readouterr().err
    assert not database.parent.exists()


def test_native_database_error_is_a_cli_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    pytest.importorskip("duckdb")
    database = tmp_path / "broken.duckdb"
    database.write_bytes(b"invalid database")
    before = database.read_bytes()
    assert main(["results-query", "--database", str(database)]) == 2
    assert "DuckDB query failed" in capsys.readouterr().err
    assert database.read_bytes() == before
