import json
import os
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import cast

import pytest

from distributed_backtester.execution import execute_intent
from distributed_backtester.models import BacktestConfig, Portfolio, TargetIntent
from distributed_backtester.results import serialize_result, write_result
from distributed_backtester.simulation import run_backtest
from tests.test_simulation import dataset


def result_text() -> str:
    return serialize_result(
        run_backtest(BacktestConfig(fee_bps=10), dataset([10, 11, 12, 9, 9]))
    )


def test_canonical_json_uses_explicit_units_and_complete_semantic_fields() -> None:
    text = result_text()
    parsed = json.loads(text)
    assert parsed["schema_version"] == 1
    assert parsed["config"]["initial_cash"] == "1000.000000"
    assert parsed["config"]["fee_bps"] == 10
    assert parsed["dataset"]["source_sha256"] == "a" * 64
    assert parsed["dataset"]["price_basis"] == "raw"
    assert parsed["config"]["strategy"]["id"] == "sma-crossover-v1"
    assert parsed["fills"][0]["price"] == "9.000000"
    assert parsed["fills"][0]["decision_session"] == "2026-01-03"
    assert parsed["fills"][0]["execution_session"] == "2026-01-04"
    # 111 shares at $9: each side has $999 notional and a $0.999 fee.
    # Initial cash $1000 - buy fee $0.999 - sell fee $0.999 = $998.002.
    assert parsed["metrics"]["total_fees"] == "1.998000"
    assert parsed["metrics"]["total_return"] == "-0.001998000000"
    assert parsed["metrics"]["maximum_drawdown"] == "0.001998000000"
    assert parsed["final_portfolio"] == {"cash": "998.002000", "holdings": 0}
    assert parsed["order_outcomes"][0]["fill_sequence"] == 1
    assert text == json.dumps(parsed, sort_keys=True, indent=2, allow_nan=False) + "\n"
    assert "generated_at" not in parsed
    assert "source_path" not in parsed["dataset"]


def test_repeated_result_bytes_are_equal() -> None:
    assert result_text().encode() == result_text().encode()


def test_complete_file_export_creates_parents_and_cleans_temporary_files(
    tmp_path: Path,
) -> None:
    output = tmp_path / "reports" / "result.json"
    result = run_backtest(BacktestConfig(), dataset([10]))
    write_result(result, output)
    assert output.read_text() == serialize_result(result)
    assert list(output.parent.iterdir()) == [output]


def test_existing_output_is_protected_unless_overwrite_is_explicit(
    tmp_path: Path,
) -> None:
    output = tmp_path / "result.json"
    output.write_text("old result")
    result = run_backtest(BacktestConfig(), dataset([10]))
    with pytest.raises(FileExistsError):
        write_result(result, output)
    assert output.read_text() == "old result"
    assert list(tmp_path.iterdir()) == [output]
    write_result(result, output, overwrite=True)
    assert output.read_text() == serialize_result(result)


def test_directory_output_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(OSError):
        write_result(run_backtest(BacktestConfig(), dataset([10])), tmp_path)
    assert list(tmp_path.parent.glob(f".{tmp_path.name}.*.tmp")) == []


def test_custom_strategy_without_parameter_provenance_cannot_be_exported() -> None:
    from tests.test_simulation import ScriptedStrategy

    result = run_backtest(
        BacktestConfig(), dataset([10]), strategy=ScriptedStrategy({})
    )
    with pytest.raises(ValueError, match="custom strategy"):
        serialize_result(result)


def test_rejected_malformed_date_has_safe_json_diagnostic() -> None:
    result = run_backtest(BacktestConfig(), dataset([10]))
    intent = TargetIntent("invested", cast(date, "bad date"), 1)
    _, outcome = execute_intent(
        intent, date(2026, 1, 2), 10_000_000, Portfolio(1000, 0), 0
    )
    result = replace(result, order_outcomes=(outcome,))
    parsed = json.loads(serialize_result(result))
    assert parsed["order_outcomes"][0]["reason"] == "invalid_timing"
    assert parsed["order_outcomes"][0]["decision_session"] is None


def test_cleanup_failure_after_publication_preserves_cli_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from distributed_backtester.cli import main

    source = tmp_path / "data.csv"
    source.write_text("date,open,high,low,close,volume\n2026-01-02,10,10,10,10,100\n")
    output = tmp_path / "result.json"
    original_unlink = Path.unlink

    def fail_temporary_unlink(path: Path, missing_ok: bool = False) -> None:
        if path.suffix == ".tmp":
            raise PermissionError("injected cleanup failure")
        original_unlink(path, missing_ok=missing_ok)

    # Only cleanup is injected; CSV parsing, simulation, linking, and JSON are real.
    monkeypatch.setattr(Path, "unlink", fail_temporary_unlink)
    status = main(
        [
            "backtest",
            "--data",
            str(source),
            "--symbol",
            "SYNTH",
            "--price-basis",
            "raw",
            "--output",
            str(output),
        ]
    )
    assert status == 0
    assert json.loads(output.read_text())["metrics"]["final_equity"] == "1000.000000"
    assert "cleanup" in caplog.text


@pytest.mark.parametrize("failure", ["fsync", "replace"])
def test_failure_before_publication_preserves_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    result = run_backtest(BacktestConfig(), dataset([10]))
    output = tmp_path / "result.json"
    if failure == "replace":
        output.write_text("previous success")

    def injected_failure(*args: object) -> None:
        raise OSError(f"injected {failure} failure")

    # Inject one syscall failure; all other publication operations use real files.
    monkeypatch.setattr(os, failure, injected_failure)
    with pytest.raises(OSError, match=f"injected {failure}"):
        write_result(result, output, overwrite=failure == "replace")
    if failure == "replace":
        assert output.read_text() == "previous success"
    else:
        assert not output.exists()
    assert list(tmp_path.glob("*.tmp")) == []


@pytest.mark.parametrize("bad_field", ["target", "sequence"])
def test_rejected_malformed_field_uses_null_diagnostic(bad_field: str) -> None:
    result = run_backtest(BacktestConfig(), dataset([10]))
    intent = TargetIntent(
        cast(str, None) if bad_field == "target" else "invested",
        date(2026, 1, 1),
        cast(int, None) if bad_field == "sequence" else 1,
    )
    _, outcome = execute_intent(
        intent, date(2026, 1, 2), 10_000_000, Portfolio(1000, 0), 0
    )
    parsed = json.loads(serialize_result(replace(result, order_outcomes=(outcome,))))
    assert parsed["order_outcomes"][0][bad_field] is None
    assert parsed["order_outcomes"][0]["reason"] == f"invalid_{bad_field}"
