import json
import subprocess
import sys
from pathlib import Path

import pytest

CSV = "date,open,high,low,close,volume\n2026-01-02,10,10,10,10,100\n"


def invoke(data: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-I",
            "-m",
            "distributed_backtester",
            "backtest",
            "--data",
            str(data),
            "--symbol",
            "SYNTH",
            "--price-basis",
            "raw",
            *args,
        ],
        cwd=data.parent,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "data.csv"
    path.write_text(CSV)
    return path


def test_cli_runs_outside_repo_and_stdout_is_json(csv_path: Path) -> None:
    result = invoke(csv_path)
    assert result.returncode == 0, result.stderr
    parsed = json.loads(result.stdout)
    assert parsed["metrics"]["final_equity"] == "1000.000000"
    assert parsed["fills"] == []
    assert result.stderr == ""


def test_cli_file_export_and_repeat_are_identical(csv_path: Path) -> None:
    output = csv_path.parent / "result.json"
    first = invoke(csv_path, "--output", str(output))
    assert first.returncode == 0, first.stderr
    expected = output.read_bytes()
    second = invoke(csv_path, "--output", str(output), "--overwrite")
    assert second.returncode == 0, second.stderr
    assert output.read_bytes() == expected


@pytest.mark.parametrize(
    "args",
    [
        ("--fast", "3", "--slow", "3"),
        ("--fast", "0"),
        ("--fee-bps", "10001"),
        ("--fee-bps", "-1"),
        ("--initial-cash", "0"),
        ("--initial-cash", "NaN"),
        ("--initial-cash", "1.0000001"),
        ("--price-basis", "adjusted"),
        ("--symbol", " "),
    ],
)
def test_bad_config_exits_without_success_artifact(
    csv_path: Path, args: tuple[str, ...]
) -> None:
    output = csv_path.parent / "result.json"
    result = invoke(csv_path, *args, "--output", str(output))
    assert result.returncode == 2
    assert "error:" in result.stderr
    assert "Traceback" not in result.stderr
    assert not output.exists()


def test_bad_data_does_not_overwrite_existing_result(csv_path: Path) -> None:
    output = csv_path.parent / "result.json"
    output.write_text("previous success")
    csv_path.write_text("malformed\n")
    result = invoke(csv_path, "--output", str(output), "--overwrite")
    assert result.returncode == 2
    assert "row 1" in result.stderr
    assert output.read_text() == "previous success"


def test_output_protection_reports_clear_error(csv_path: Path) -> None:
    output = csv_path.parent / "result.json"
    output.write_text("previous success")
    result = invoke(csv_path, "--output", str(output))
    assert result.returncode == 2
    assert "--overwrite" in result.stderr
    assert output.read_text() == "previous success"


@pytest.mark.parametrize("alias_kind", ["same", "symlink", "hardlink"])
def test_input_cannot_be_overwritten_through_alias(
    csv_path: Path, alias_kind: str
) -> None:
    output = csv_path.parent / "alias.csv"
    if alias_kind == "same":
        output = csv_path
    elif alias_kind == "symlink":
        output.symlink_to(csv_path)
    else:
        output.hardlink_to(csv_path)
    result = invoke(csv_path, "--output", str(output), "--overwrite")
    assert result.returncode == 2
    assert "input" in result.stderr
    assert csv_path.read_text() == CSV


def test_missing_source_has_clear_error_and_no_artifact(csv_path: Path) -> None:
    output = csv_path.parent / "result.json"
    csv_path.unlink()
    result = invoke(csv_path, "--output", str(output))
    assert result.returncode == 2
    assert "cannot read" in result.stderr
    assert "Traceback" not in result.stderr
    assert not output.exists()
