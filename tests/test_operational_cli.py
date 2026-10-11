"""Real CLI configuration, observations and output remain separate."""

import json
from pathlib import Path

import pytest

from distributed_backtester.cli import main

SOURCE = Path(__file__).resolve().parents[1] / "examples/sma.csv"


def config_file(tmp_path: Path, *, command: str = "backtest") -> Path:
    path = tmp_path / "run.toml"
    path.write_text(
        f'schema_version = 1\ncommand = "{command}"\n'
        f'data = "{SOURCE}"\nsymbol = "SYNTH"\nprice_basis = "raw"\n'
        'fast = 2\nslow = 3\ninitial_cash = "1000"\nfee_bps = 10\n'
    )
    return path


def test_file_defaults_and_explicit_override(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = config_file(tmp_path)
    assert main(["backtest", "--config", str(config), "--fast", "1"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["config"]["strategy"]["parameters"]["fast_window"] == 1
    assert result["config"]["strategy"]["parameters"]["slow_window"] == 3


def test_config_data_and_output_are_file_relative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = config_file(tmp_path)
    (tmp_path / "sma.csv").write_bytes(SOURCE.read_bytes())
    config.write_text(
        config.read_text().replace(str(SOURCE), "sma.csv") + 'output = "result.json"\n'
    )
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    assert main(["backtest", "--config", str(config)]) == 0
    assert (tmp_path / "result.json").is_file()
    assert not (elsewhere / "result.json").exists()


def test_cli_output_override_keeps_current_directory_semantics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = config_file(tmp_path)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    assert main(["backtest", "--config", str(config), "--output", "chosen.json"]) == 0
    assert (elsewhere / "chosen.json").is_file()
    assert not (tmp_path / "chosen.json").exists()


@pytest.mark.parametrize(
    "content",
    [
        'schema_version = 1\ncommand = "backtest"\nunknown = 3\n',
        'schema_version = 1\ncommand = "sweep"\n',
        'schema_version = 1\ncommand = "backtest"\nfee_bps = true\n',
        'schema_version = 1\ncommand = "backtest"\nfast = [2]\n',
        "malformed = [\n",
    ],
)
def test_invalid_configuration_is_clear_error_before_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], content: str
) -> None:
    config = tmp_path / "invalid.toml"
    config.write_text(content)
    output = tmp_path / "result.json"
    assert main(["backtest", "--config", str(config), "--output", str(output)]) == 2
    assert not output.exists()
    assert "error:" in capsys.readouterr().err


def test_config_cannot_be_overwritten_as_result(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = config_file(tmp_path)
    original = config.read_bytes()
    assert (
        main(
            [
                "backtest",
                "--config",
                str(config),
                "--output",
                str(config),
                "--overwrite",
            ]
        )
        == 2
    )
    assert config.read_bytes() == original
    assert "config" in capsys.readouterr().err.lower()


def test_json_logs_do_not_contaminate_result_stdout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = config_file(tmp_path)
    assert main(["backtest", "--config", str(config), "--log-level", "INFO"]) == 0
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["metrics"]["final_equity"] == "749.257000"
    logs = [json.loads(line) for line in captured.err.splitlines()]
    assert {record["event"] for record in logs} >= {
        "command_started",
        "command_finished",
        "backtest_started",
        "backtest_finished",
    }
    assert all(record["schema_version"] == 1 for record in logs)


def test_explicit_negative_boolean_overrides_file_default(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = config_file(tmp_path)
    destination = tmp_path / "existing.json"
    destination.write_text("preserved")
    config.write_text(
        config.read_text() + 'overwrite = true\noutput = "existing.json"\n'
    )
    assert main(["backtest", "--config", str(config), "--no-overwrite"]) == 2
    assert destination.read_text() == "preserved"
    assert "output exists" in capsys.readouterr().err


def test_malformed_config_with_explicit_logging_is_json_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = tmp_path / "broken.toml"
    config.write_text("invalid = [\n")
    assert main(["backtest", "--config", str(config), "--log-level", "ERROR"]) == 2
    captured = capsys.readouterr()
    assert not captured.out
    record = json.loads(captured.err)
    assert record["event"] == "command_failed"
    assert record["level"] == "ERROR"
    assert "configuration" in record["detail"]


def test_classic_status_after_actual_sweep(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "sweep"
    assert (
        main(
            [
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
                "--slow",
                "3",
                "--output-dir",
                str(output),
                "--log-level",
                "INFO",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    experiment = json.loads(captured.out)["experiment_id"]
    logs = [json.loads(line) for line in captured.err.splitlines()]
    worker_records = [record for record in logs if record["event"] == "job_started"]
    assert len(worker_records) == 2
    assert all(record["experiment_id"] == experiment for record in worker_records)
    assert main(["status", "--output-dir", str(output)]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["counts"]["succeeded"] == 2
    chosen = status["jobs"][0]["job_id"]
    assert main(["status", "--output-dir", str(output), "--job-id", chosen]) == 0
    selected = json.loads(capsys.readouterr().out)
    assert [job["job_id"] for job in selected["jobs"]] == [chosen]
