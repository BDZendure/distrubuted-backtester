import json
from pathlib import Path

import pytest

from distributed_backtester.models import BacktestConfig
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


def test_custom_strategy_is_identified_without_claiming_sma_parameters() -> None:
    from tests.test_simulation import ScriptedStrategy

    result = run_backtest(
        BacktestConfig(), dataset([10]), strategy=ScriptedStrategy({})
    )
    strategy = json.loads(serialize_result(result))["config"]["strategy"]
    assert strategy["id"].endswith("ScriptedStrategy")
    assert strategy["parameters"] is None
