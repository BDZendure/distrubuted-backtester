"""Exact typed storage, conversion provenance, and malformed file rejection."""

import json
from dataclasses import replace
from datetime import date
from hashlib import sha256
from pathlib import Path

import pytest

from distributed_backtester.data import DataValidationError, load_csv
from distributed_backtester.dataset_identity import bars_digest
from distributed_backtester.jobs import JobSpec, create_job, metadata_record
from distributed_backtester.models import (
    BacktestConfig,
    Bar,
    DatasetMetadata,
    MarketDataset,
)
from distributed_backtester.parquet_data import load_parquet, write_parquet
from distributed_backtester.provenance import current_implementation
from distributed_backtester.results import result_to_dict
from distributed_backtester.simulation import run_backtest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

_KEY = b"distributed_backtester.market.v1"
_SAMPLE = Path(__file__).parents[1] / "examples/sma.csv"


def _sample() -> MarketDataset:
    return load_csv(_SAMPLE, symbol="SYNTH", price_basis="raw")


def test_exact_roundtrip_and_economics(tmp_path: Path) -> None:
    csv = _sample()
    path = tmp_path / "history.parquet"
    written = write_parquet(csv, path, row_group_size=2)
    data = load_parquet(path, symbol="SYNTH", price_basis="raw")
    assert data.bars == csv.bars
    assert data.metadata == written
    assert written.source_sha256 == sha256(path.read_bytes()).hexdigest()
    assert written.parent_source_sha256 == csv.metadata.source_sha256
    assert written.bars_sha256 == bars_digest(csv.bars)
    assert written.source_format == "parquet"
    assert written.dataset_version is not None
    assert pq.ParquetFile(path).num_row_groups == (len(csv.bars) + 1) // 2
    assert all(not field.nullable for field in pq.read_schema(path))
    for config in (BacktestConfig(fee_bps=10), BacktestConfig(initial_cash=1)):
        left = result_to_dict(run_backtest(config, csv))
        right = result_to_dict(run_backtest(config, data))
        left.pop("dataset")
        right.pop("dataset")
        assert left == right
    job = create_job(BacktestConfig(), written, current_implementation())
    assert JobSpec.from_dict(job.to_dict()) == job
    assert "source_format" not in metadata_record(csv.metadata)


def test_exact_micro_prices_and_int64_boundaries(tmp_path: Path) -> None:
    sample = _sample()
    bars = (
        Bar(date(1, 1, 1), 1, 1, 1, 1, 0),
        Bar(date(9999, 12, 31), 2**63 - 1, 2**63 - 1, 2**63 - 1, 2**63 - 1, 2**63 - 1),
    )
    data = MarketDataset(bars, sample.metadata)
    path = tmp_path / "extremes.parquet"
    write_parquet(data, path)
    assert load_parquet(path, symbol="SYNTH", price_basis="raw").bars == bars


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
def test_overflow_refused_before_destination_creation(
    tmp_path: Path, field: str
) -> None:
    bar = Bar(date(2020, 1, 1), 2**63, 2**63, 2**63, 2**63, 2**63)
    if field == "volume":
        bar = replace(bar, open=1, high=1, low=1, close=1)
    path = tmp_path / "absent" / "overflow.parquet"
    with pytest.raises(ValueError, match="int64"):
        write_parquet(MarketDataset((bar,), _sample().metadata), path)
    assert not path.parent.exists()


@pytest.mark.parametrize("size", [0, -1, True, 1.5, 64 * 1024 * 1024 + 1])
def test_invalid_row_group_size(tmp_path: Path, size: int) -> None:
    with pytest.raises(ValueError, match="row_group"):
        write_parquet(_sample(), tmp_path / "absent/file.parquet", row_group_size=size)
    assert not (tmp_path / "absent").exists()


def test_publication_protects_existing_file_and_cleanup(tmp_path: Path) -> None:
    path = tmp_path / "file.parquet"
    path.write_bytes(b"user content")
    with pytest.raises(FileExistsError):
        write_parquet(_sample(), path)
    assert path.read_bytes() == b"user content"
    assert sorted(tmp_path.iterdir()) == [path]
    write_parquet(_sample(), path, overwrite=True)
    assert load_parquet(path, symbol="SYNTH", price_basis="raw").bars == _sample().bars
    assert sorted(tmp_path.iterdir()) == [path]


def test_layout_changes_preserve_version_but_not_physical_identity(
    tmp_path: Path,
) -> None:
    first = write_parquet(_sample(), tmp_path / "first.parquet", row_group_size=2)
    second = write_parquet(_sample(), tmp_path / "second.parquet", row_group_size=4)
    assert first.dataset_version == second.dataset_version
    assert first.source_sha256 != second.source_sha256
    assert first.parent_source_sha256 == second.parent_source_sha256


@pytest.mark.parametrize("symbol,basis", [("WRONG", "raw"), ("SYNTH", "adjusted")])
def test_symbol_and_price_policy_refused(
    tmp_path: Path, symbol: str, basis: str
) -> None:
    path = tmp_path / "file.parquet"
    write_parquet(_sample(), path)
    with pytest.raises(DataValidationError):
        load_parquet(path, symbol=symbol, price_basis=basis)  # type: ignore[arg-type]


@pytest.mark.parametrize("payload", [b"", b"not parquet", b"PAR1truncatedPAR1"])
def test_malformed_parquet(tmp_path: Path, payload: bytes) -> None:
    path = tmp_path / "bad.parquet"
    path.write_bytes(payload)
    with pytest.raises(DataValidationError):
        load_parquet(path, symbol="SYNTH", price_basis="raw")


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 2),
        ("schema_version", True),
        ("money_unit", "USD"),
        ("row_count", 1),
        ("bars_sha256", "0" * 64),
        ("parent_source_sha256", "bad"),
        ("dataset_version", "0" * 64),
    ],
)
def test_tampered_descriptor(tmp_path: Path, field: str, value: object) -> None:
    path = tmp_path / "file.parquet"
    write_parquet(_sample(), path)
    table = pq.read_table(path)
    descriptor = json.loads(table.schema.metadata[_KEY])
    descriptor[field] = value
    metadata = {
        _KEY: json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
    }
    pq.write_table(table.replace_schema_metadata(metadata), path)
    with pytest.raises(DataValidationError):
        load_parquet(path, symbol="SYNTH", price_basis="raw")


@pytest.mark.parametrize(
    "mutation",
    [
        "float",
        "null",
        "timestamp",
        "missing",
        "extra",
        "empty",
        "order",
        "price",
        "no_metadata",
    ],
)
def test_schema_and_bar_rejection(tmp_path: Path, mutation: str) -> None:
    path = tmp_path / "file.parquet"
    write_parquet(_sample(), path, row_group_size=2)
    table = pq.read_table(path)
    if mutation == "float":
        table = table.set_column(1, "open", table["open"].cast(pa.float64()))
    elif mutation == "null":
        values = table["volume"].to_pylist()
        values[2] = None
        table = table.set_column(
            5,
            pa.field("volume", pa.int64(), nullable=True),
            pa.array(values, type=pa.int64()),
        )
    elif mutation == "timestamp":
        table = table.set_column(0, "date", table["date"].cast(pa.timestamp("ms")))
    elif mutation == "missing":
        table = table.drop(["volume"])
    elif mutation == "extra":
        table = table.append_column("extra", pa.array([1] * table.num_rows))
    elif mutation == "empty":
        table = table.slice(0, 0)
    elif mutation == "order":
        indices = list(range(table.num_rows))
        indices[2], indices[3] = indices[3], indices[2]
        table = table.take(pa.array(indices))
    elif mutation == "price":
        values = table["close"].to_pylist()
        values[0] = 0
        table = table.set_column(
            4,
            pa.field("close", pa.int64(), nullable=False),
            pa.array(values, type=pa.int64()),
        )
    else:
        table = table.replace_schema_metadata(None)
    pq.write_table(table, path, row_group_size=2)
    with pytest.raises(DataValidationError):
        load_parquet(path, symbol="SYNTH", price_basis="raw")


def test_incomplete_metadata_fields_refused() -> None:
    with pytest.raises(ValueError):
        DatasetMetadata("SYNTH", "0" * 64, source_format="parquet")
    with pytest.raises(ValueError):
        replace(_sample().metadata, dataset_version="0" * 64)


def test_publication_error_is_not_masked_by_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os

    original = Path.unlink

    def unlink(path: Path, missing_ok: bool = False) -> None:
        if path.suffix == ".tmp":
            raise OSError("cleanup failed")
        original(path, missing_ok=missing_ok)

    def publish(source: Path, target: Path) -> None:
        raise OSError("publication failed")

    monkeypatch.setattr(Path, "unlink", unlink)
    monkeypatch.setattr(os, "link", publish)
    with pytest.raises(OSError, match="publication failed"):
        write_parquet(_sample(), tmp_path / "file.parquet")
