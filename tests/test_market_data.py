"""Format selection, expected versions, and content-verified bounded caching."""

import os
from pathlib import Path
from typing import Literal

import pytest

from distributed_backtester import data, market_data, parquet_data
from distributed_backtester.data import DataValidationError, load_csv
from distributed_backtester.market_data import DatasetCache, load_dataset
from distributed_backtester.models import MarketDataset

_SAMPLE = Path(__file__).parents[1] / "examples/sma.csv"


def _source_path(tmp_path: Path, data_format: Literal["csv", "parquet"]) -> Path:
    path = tmp_path / f"file.{data_format}"
    if data_format == "csv":
        path.write_bytes(_SAMPLE.read_bytes())
    else:
        pytest.importorskip("pyarrow")
        parquet_data.write_parquet(
            load_csv(_SAMPLE, symbol="SYNTH", price_basis="raw"), path
        )
    return path


def test_csv_shape_and_explicit_version() -> None:
    expected = load_csv(_SAMPLE, symbol="SYNTH", price_basis="raw")
    assert load_dataset(_SAMPLE, symbol="SYNTH", price_basis="raw") == expected
    assert (
        load_dataset(
            _SAMPLE,
            symbol="SYNTH",
            price_basis="raw",
            expected_version=expected.metadata.source_sha256,
        )
        == expected
    )
    with pytest.raises(ValueError, match="version"):
        load_dataset(
            _SAMPLE, symbol="SYNTH", price_basis="raw", expected_version="0" * 64
        )


@pytest.mark.parametrize("version", ["bad", "", "A" * 64, True])
def test_expected_version_validation(version: str) -> None:
    with pytest.raises(ValueError, match="version"):
        load_dataset(
            _SAMPLE, symbol="SYNTH", price_basis="raw", expected_version=version
        )


def test_cache_hits_symbol_separation_and_bounded_eviction() -> None:
    cache = DatasetCache(max_entries=1)
    first = load_dataset(_SAMPLE, symbol="SYNTH", price_basis="raw", cache=cache)
    assert (
        load_dataset(_SAMPLE, symbol="SYNTH", price_basis="raw", cache=cache) is first
    )
    second = load_dataset(_SAMPLE, symbol="OTHER", price_basis="raw", cache=cache)
    assert second.metadata.symbol == "OTHER"
    assert cache.hits == 1
    assert cache.misses == 2
    assert (
        load_dataset(_SAMPLE, symbol="SYNTH", price_basis="raw", cache=cache)
        is not first
    )
    assert len(cache) == 1
    cache.clear()
    assert len(cache) == 0


@pytest.mark.parametrize("data_format", ["csv", "parquet"])
def test_populated_cache_keeps_formats_separate(
    tmp_path: Path, data_format: Literal["csv", "parquet"]
) -> None:
    pytest.importorskip("pyarrow")
    path = _source_path(tmp_path, data_format)
    cache = DatasetCache(max_entries=2)
    first = load_dataset(
        path, symbol="SYNTH", price_basis="raw", data_format=data_format, cache=cache
    )
    assert (
        load_dataset(
            path,
            symbol="SYNTH",
            price_basis="raw",
            data_format=data_format,
            cache=cache,
        )
        is first
    )
    assert (len(cache), cache.hits, cache.misses) == (1, 1, 1)
    wrong_format: Literal["csv", "parquet"] = (
        "parquet" if data_format == "csv" else "csv"
    )
    diagnostic = "invalid Parquet source" if wrong_format == "parquet" else "CSV"
    with pytest.raises(DataValidationError, match=diagnostic):
        load_dataset(
            path,
            symbol="SYNTH",
            price_basis="raw",
            data_format=wrong_format,
            cache=cache,
        )
    assert (len(cache), cache.hits, cache.misses) == (1, 1, 2)
    assert (
        load_dataset(
            path,
            symbol="SYNTH",
            price_basis="raw",
            data_format=data_format,
            cache=cache,
        )
        is first
    )
    assert (len(cache), cache.hits, cache.misses) == (1, 2, 2)


def test_capacity_two_cache_evicts_least_recently_used_dataset(tmp_path: Path) -> None:
    paths = [tmp_path / f"{name}.csv" for name in ("A", "B", "C")]
    snapshot = _SAMPLE.read_bytes()
    for index, path in enumerate(paths):
        path.write_bytes(snapshot.replace(b"1000", str(1000 + index).encode("ascii")))
    cache = DatasetCache(max_entries=2)

    def load(path: Path) -> MarketDataset:
        return load_dataset(path, symbol="SYNTH", price_basis="raw", cache=cache)

    first = load(paths[0])
    second = load(paths[1])
    assert first.metadata.source_sha256 != second.metadata.source_sha256
    assert (len(cache), cache.hits, cache.misses) == (2, 0, 2)
    assert load(paths[0]) is first
    assert (len(cache), cache.hits, cache.misses) == (2, 1, 2)
    third = load(paths[2])
    assert third.metadata.source_sha256 not in {
        first.metadata.source_sha256,
        second.metadata.source_sha256,
    }
    assert (len(cache), cache.hits, cache.misses) == (2, 1, 3)
    assert load(paths[0]) is first
    assert load(paths[2]) is third
    assert (len(cache), cache.hits, cache.misses) == (2, 3, 3)
    recreated = load(paths[1])
    assert recreated == second
    assert recreated is not second
    assert (len(cache), cache.hits, cache.misses) == (2, 3, 4)


@pytest.mark.parametrize("data_format", ["csv", "parquet"])
def test_uncached_load_decodes_and_recreates_dataset_each_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    data_format: Literal["csv", "parquet"],
) -> None:
    path = _source_path(tmp_path, data_format)
    module = market_data if data_format == "csv" else parquet_data
    decoder_name = f"_load_{data_format}_snapshot"
    original = (
        data._load_csv_snapshot
        if data_format == "csv"
        else parquet_data._load_parquet_snapshot
    )
    snapshots: list[bytes] = []

    def decode(
        path: Path, snapshot: bytes, *, symbol: str, price_basis: Literal["raw"]
    ) -> MarketDataset:
        snapshots.append(snapshot)
        return original(path, snapshot, symbol=symbol, price_basis=price_basis)

    monkeypatch.setattr(module, decoder_name, decode)
    first = load_dataset(
        path, symbol="SYNTH", price_basis="raw", data_format=data_format
    )
    second = load_dataset(
        path, symbol="SYNTH", price_basis="raw", data_format=data_format
    )
    assert first == second
    assert first is not second
    assert first.bars is not second.bars
    assert snapshots == [path.read_bytes(), path.read_bytes()]


def test_same_size_and_mtime_rewrite_invalidates_cache(tmp_path: Path) -> None:
    path = tmp_path / "file.csv"
    original = _SAMPLE.read_bytes()
    path.write_bytes(original)
    before = path.stat()
    cache = DatasetCache()
    first = load_dataset(path, symbol="SYNTH", price_basis="raw", cache=cache)
    changed = original.replace(b"1000", b"1001")
    assert changed != original and len(changed) == len(original)
    path.write_bytes(changed)
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    second = load_dataset(path, symbol="SYNTH", price_basis="raw", cache=cache)
    assert second is not first
    assert second.metadata.source_sha256 != first.metadata.source_sha256
    assert second.bars != first.bars
    path.write_bytes(b"bad")
    with pytest.raises(DataValidationError):
        load_dataset(path, symbol="SYNTH", price_basis="raw", cache=cache)


@pytest.mark.parametrize("size", [0, -1, True])
def test_invalid_cache_capacity(size: int) -> None:
    with pytest.raises(ValueError):
        DatasetCache(max_entries=size)


def test_cache_capacity_is_readonly_and_invalid_hit_inputs_fail() -> None:
    cache = DatasetCache()
    load_dataset(_SAMPLE, symbol="SYNTH", price_basis="raw", cache=cache)
    with pytest.raises(AttributeError):
        cache.max_entries = 0  # type: ignore[misc]
    with pytest.raises(ValueError):
        load_dataset(_SAMPLE, symbol="SYNTH", price_basis="adjusted", cache=cache)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="version"):
        load_dataset(
            _SAMPLE,
            symbol="SYNTH",
            price_basis="raw",
            expected_version="0" * 64,
            cache=cache,
        )


def test_parquet_auto_and_explicit_version(tmp_path: Path) -> None:
    pytest.importorskip("pyarrow")
    from distributed_backtester.parquet_data import write_parquet

    path = tmp_path / "file.parquet"
    metadata = write_parquet(load_csv(_SAMPLE, symbol="SYNTH", price_basis="raw"), path)
    loaded = load_dataset(
        path,
        symbol="SYNTH",
        price_basis="raw",
        expected_version=metadata.dataset_version,
    )
    assert loaded.metadata == metadata
    assert (
        load_dataset(path, symbol="SYNTH", price_basis="raw", data_format="parquet")
        == loaded
    )
    with pytest.raises(ValueError):
        load_dataset(path, symbol="SYNTH", price_basis="raw", data_format="csv")
    with pytest.raises(ValueError, match="format"):
        load_dataset(path, symbol="SYNTH", price_basis="raw", data_format="garbage")  # type: ignore[arg-type]
