"""Exact typed storage, conversion provenance, and malformed file rejection."""

import json
from collections.abc import Sequence
from dataclasses import replace
from datetime import date, timedelta
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import cast

import pytest

from distributed_backtester.data import DataValidationError, load_csv
from distributed_backtester.dataset_identity import bars_digest
from distributed_backtester.jobs import (
    JobSpec,
    canonical_json,
    create_job,
    metadata_record,
)
from distributed_backtester.models import (
    BacktestConfig,
    Bar,
    DatasetMetadata,
    MarketDataset,
)
from distributed_backtester.parquet_data import load_parquet, write_parquet
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.results import result_to_dict, serialize_result
from distributed_backtester.simulation import run_backtest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

_KEY = b"distributed_backtester.market.v1"
_SAMPLE = Path(__file__).parents[1] / "examples/sma.csv"
_IDENTITY = ImplementationIdentity("daily-test-v1", "b" * 64)
_PROVENANCE_HASHES = ("dataset_version", "parent_source_sha256", "bars_sha256")


def _sample() -> MarketDataset:
    return load_csv(_SAMPLE, symbol="SYNTH", price_basis="raw")


def _canonical_bytes(record: object) -> bytes:
    return json.dumps(
        record, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _recompute_hash(record: dict[str, object], hash_field: str) -> None:
    payload = {key: value for key, value in record.items() if key != hash_field}
    record[hash_field] = sha256(_canonical_bytes(payload)).hexdigest()


def _independent_bars_digest(bars: Sequence[Bar]) -> str:
    rows = b"".join(
        _canonical_bytes(
            [
                bar.session.isoformat(),
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
            ]
        )
        + b"\n"
        for bar in bars
    )
    return sha256(rows).hexdigest()


def _extended_job_record() -> dict[str, object]:
    metadata = DatasetMetadata(
        "SYNTH",
        "a" * 64,
        source_format="parquet",
        dataset_version="c" * 64,
        parent_source_sha256="d" * 64,
        bars_sha256="e" * 64,
    )
    return create_job(BacktestConfig(), metadata, _IDENTITY).to_dict()


def test_canonical_rows_include_lf_and_exact_integer_prices(tmp_path: Path) -> None:
    bars = (
        Bar(date(2024, 2, 29), 1, 3, 1, 2, 0),
        Bar(
            date(2024, 3, 1),
            1_000_001,
            1_000_003,
            1_000_000,
            1_000_002,
            2**63 - 1,
        ),
    )
    canonical_rows = (
        b'["2024-02-29",1,3,1,2,0]\n'
        b'["2024-03-01",1000001,1000003,1000000,1000002,9223372036854775807]\n'
    )
    expected = "6f6d11b96e61ecca27c295dff5f0378c28ba1852201ae577759ace4a8d168c93"
    assert sha256(canonical_rows).hexdigest() == expected
    assert bars_digest(bars) == expected
    path = tmp_path / "canonical.parquet"
    written = write_parquet(MarketDataset(bars, _sample().metadata), path)
    assert written.bars_sha256 == expected
    assert load_parquet(path, symbol="SYNTH", price_basis="raw").bars == bars


def test_legacy_job_and_result_metadata_have_exact_five_field_shape() -> None:
    metadata = DatasetMetadata("SYNTH", "a" * 64)
    expected_dataset = {
        "symbol": "SYNTH",
        "source_sha256": "a" * 64,
        "currency": "USD",
        "price_basis": "raw",
        "validation_version": 1,
    }
    expected_bytes = (
        b'{"currency":"USD","price_basis":"raw","source_sha256":"'
        + b"a" * 64
        + b'","symbol":"SYNTH","validation_version":1}'
    )
    expected_payload = {
        "schema_version": 1,
        "config": {
            "strategy_id": "sma-crossover-v1",
            "fast_window": 2,
            "slow_window": 3,
            "initial_cash": 1_000_000_000,
            "money_unit": "micro-USD",
            "fee_bps": 0,
        },
        "dataset": expected_dataset,
        "implementation": {"engine_id": "daily-test-v1", "source_sha256": "b" * 64},
    }
    job = create_job(BacktestConfig(), metadata, _IDENTITY)
    expected_job = {
        **expected_payload,
        "job_id": sha256(_canonical_bytes(expected_payload)).hexdigest(),
    }
    assert job.to_dict() == expected_job
    assert canonical_json(job.to_dict()).encode() == _canonical_bytes(expected_job)
    assert JobSpec.from_dict(expected_job) == job
    assert metadata_record(metadata) == expected_dataset
    assert canonical_json(job.to_dict()["dataset"]).encode() == expected_bytes
    data = MarketDataset((Bar(date(2024, 1, 1), 1, 1, 1, 1, 0),), metadata)
    result = run_backtest(BacktestConfig(), data)
    assert result_to_dict(result)["dataset"] == expected_dataset
    serialized_dataset = json.loads(serialize_result(result))["dataset"]
    assert serialized_dataset == expected_dataset
    assert _canonical_bytes(serialized_dataset) == expected_bytes


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
    job = create_job(BacktestConfig(), written, _IDENTITY)
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


@pytest.mark.parametrize(
    "field,bar",
    [
        pytest.param("high", Bar(date(2020, 1, 1), 1, 2**63, 1, 1, 1), id="high-only"),
        pytest.param(
            "volume", Bar(date(2020, 1, 1), 1, 1, 1, 1, 2**63), id="volume-only"
        ),
    ],
)
def test_overflow_refused_before_destination_creation(
    tmp_path: Path, field: str, bar: Bar
) -> None:
    assert getattr(bar, field) == 2**63
    assert all(
        getattr(bar, name) <= 2**63 - 1
        for name in ("open", "high", "low", "close", "volume")
        if name != field
    )
    path = tmp_path / "absent" / "nested" / "overflow.parquet"
    with pytest.raises(ValueError, match="int64"):
        write_parquet(MarketDataset((bar,), _sample().metadata), path)
    assert not (tmp_path / "absent").exists()


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
    "field,value,diagnostic",
    [
        ("schema_version", 2, "unsupported Parquet schema_version"),
        ("schema_version", True, "unsupported Parquet schema_version"),
        ("money_unit", "USD", "unsupported Parquet currency"),
        ("row_count", 1, "row count does not match descriptor"),
        ("bars_sha256", "0" * 64, "bars do not match recorded content hash/count"),
        ("parent_source_sha256", "bad", "parent_source_sha256 must be a lowercase"),
        ("dataset_version", "0" * 64, "dataset version does not match descriptor"),
    ],
)
def test_tampered_descriptor(
    tmp_path: Path, field: str, value: object, diagnostic: str
) -> None:
    path = tmp_path / "file.parquet"
    write_parquet(_sample(), path)
    table = pq.read_table(path)
    descriptor = json.loads(table.schema.metadata[_KEY])
    descriptor[field] = value
    if field in {"bars_sha256", "row_count"}:
        _recompute_hash(descriptor, "dataset_version")
    metadata = {_KEY: _canonical_bytes(descriptor)}
    pq.write_table(table.replace_schema_metadata(metadata), path)
    with pytest.raises(DataValidationError, match=diagnostic):
        load_parquet(path, symbol="SYNTH", price_basis="raw")


def test_valid_bar_mutation_rejected_by_content_hash(tmp_path: Path) -> None:
    original = _sample()
    path = tmp_path / "mutated.parquet"
    write_parquet(original, path)
    table = pq.read_table(path)
    changed_bar = replace(original.bars[0], volume=original.bars[0].volume + 1)
    changed_bars = (changed_bar, *original.bars[1:])
    MarketDataset(changed_bars, original.metadata)
    mutated = table.set_column(
        5,
        table.schema.field("volume"),
        pa.array([bar.volume for bar in changed_bars], type=pa.int64()),
    )
    assert mutated.num_rows == table.num_rows
    assert mutated.schema.equals(table.schema, check_metadata=True)
    assert mutated["volume"].to_pylist() != table["volume"].to_pylist()
    pq.write_table(mutated, path)
    with pytest.raises(
        DataValidationError, match="bars do not match recorded content hash/count"
    ):
        load_parquet(path, symbol="SYNTH", price_basis="raw")


@pytest.mark.parametrize(
    "boundary",
    [pytest.param(2, id="row-group"), pytest.param(65536, id="reader-batch")],
)
@pytest.mark.parametrize(
    "step_back", [pytest.param(0, id="duplicate"), pytest.param(1, id="decreasing")]
)
def test_order_rejected_between_independently_sorted_groups_or_batches(
    tmp_path: Path, boundary: int, step_back: int
) -> None:
    bars = tuple(
        Bar(date(2000, 1, 1) + timedelta(days=index), 1, 1, 1, 1, index)
        for index in range(boundary + 2)
    )
    path = tmp_path / "unordered.parquet"
    row_group_size = boundary if boundary == 2 else len(bars)
    write_parquet(
        MarketDataset(bars, _sample().metadata), path, row_group_size=row_group_size
    )
    changed = list(bars)
    changed[boundary] = replace(
        changed[boundary],
        session=changed[boundary - 1].session - timedelta(days=step_back),
    )
    table = pq.read_table(path)
    table = table.set_column(
        0,
        table.schema.field("date"),
        pa.array([bar.session for bar in changed], type=pa.date32()),
    )
    descriptor = json.loads(table.schema.metadata[_KEY])
    descriptor["bars_sha256"] = _independent_bars_digest(changed)
    _recompute_hash(descriptor, "dataset_version")
    table = table.replace_schema_metadata({_KEY: _canonical_bytes(descriptor)})
    pq.write_table(table, path, row_group_size=row_group_size)
    with pq.ParquetFile(path) as source:
        assert source.metadata.num_rows == descriptor["row_count"] == len(changed)
        assert source.schema_arrow.metadata[_KEY] == _canonical_bytes(descriptor)
        if boundary == 2:
            assert source.num_row_groups == 2
            groups = [
                source.read_row_group(index)["date"].to_pylist()
                for index in range(source.num_row_groups)
            ]
        else:
            assert source.num_row_groups == 1
            groups = [
                batch.column(0).to_pylist()
                for batch in source.iter_batches(batch_size=65536, use_threads=False)
            ]
        assert [len(group) for group in groups] == [boundary, 2]
        assert all(left < right for group in groups for left, right in pairwise(group))
        assert groups[1][0] <= groups[0][-1]
    with pytest.raises(
        DataValidationError, match="sessions must be unique and strictly increasing"
    ):
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


@pytest.mark.parametrize(
    "missing",
    [(field,) for field in (*_PROVENANCE_HASHES, "source_format")]
    + [_PROVENANCE_HASHES],
)
def test_partial_extended_job_metadata_refused(missing: tuple[str, ...]) -> None:
    record = _extended_job_record()
    metadata = cast(dict[str, object], record["dataset"])
    for field in missing:
        del metadata[field]
    _recompute_hash(record, "job_id")
    with pytest.raises(ValueError, match="dataset must contain exactly"):
        JobSpec.from_dict(record)


@pytest.mark.parametrize("field", _PROVENANCE_HASHES)
@pytest.mark.parametrize("value", ["bad", "", "A" * 64, "g" * 64, None, True])
def test_malformed_extended_job_hashes_refused(field: str, value: object) -> None:
    record = _extended_job_record()
    metadata = cast(dict[str, object], record["dataset"])
    metadata[field] = value
    _recompute_hash(record, "job_id")
    diagnostic = (
        "Parquet metadata requires three lowercase SHA-256 hashes"
        if isinstance(value, str)
        else f"{field} must be text"
    )
    with pytest.raises(ValueError, match=diagnostic):
        JobSpec.from_dict(record)


@pytest.mark.parametrize("field", ["storage_version", "source_path"])
def test_unknown_extended_job_metadata_refused(field: str) -> None:
    record = _extended_job_record()
    metadata = cast(dict[str, object], record["dataset"])
    metadata[field] = "unexpected"
    _recompute_hash(record, "job_id")
    with pytest.raises(ValueError, match="dataset must contain exactly"):
        JobSpec.from_dict(record)


@pytest.mark.parametrize("source_format", ["csv", "arrow"])
def test_extended_job_metadata_requires_parquet(source_format: str) -> None:
    record = _extended_job_record()
    metadata = cast(dict[str, object], record["dataset"])
    metadata["source_format"] = source_format
    _recompute_hash(record, "job_id")
    with pytest.raises(ValueError, match="extended dataset metadata requires Parquet"):
        JobSpec.from_dict(record)


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
