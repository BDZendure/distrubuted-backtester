"""Format selection, expected versions, and content-verified bounded caching."""

import os
from pathlib import Path

import pytest

from distributed_backtester.data import DataValidationError, load_csv
from distributed_backtester.market_data import DatasetCache, load_dataset

_SAMPLE = Path(__file__).parents[1] / "examples/sma.csv"


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
