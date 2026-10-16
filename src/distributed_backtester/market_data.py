"""Snapshot-based format selection, version checking, and an explicit bounded LRU."""

import re
from collections import OrderedDict
from hashlib import sha256
from pathlib import Path
from typing import Literal

from distributed_backtester.data import DataValidationError, _load_csv_snapshot
from distributed_backtester.models import DatasetMetadata, MarketDataset
from distributed_backtester.money import require_integer

DataFormat = Literal["auto", "csv", "parquet"]


class DatasetCache:
    """Retain at most max_entries datasets; every hit still reads/hashes the file."""

    def __init__(self, max_entries: int = 1) -> None:
        require_integer(max_entries, "max_entries", minimum=1)
        self._max_entries = max_entries
        self.hits = 0
        self.misses = 0
        self._entries: OrderedDict[tuple[str, str, str], MarketDataset] = OrderedDict()

    @property
    def max_entries(self) -> int:
        return self._max_entries

    def __len__(self) -> int:
        return len(self._entries)

    def clear(self) -> None:
        self._entries.clear()


def load_dataset(
    path: Path,
    *,
    symbol: str,
    price_basis: Literal["raw"],
    data_format: DataFormat = "auto",
    expected_version: str | None = None,
    cache: DatasetCache | None = None,
) -> MarketDataset:
    if data_format not in {"auto", "csv", "parquet"}:
        raise ValueError("data_format must be auto, csv, or parquet")
    if expected_version is not None and (
        not isinstance(expected_version, str)
        or not re.fullmatch(r"[0-9a-f]{64}", expected_version)
    ):
        raise ValueError("expected dataset version must be a lowercase SHA-256 hash")
    if price_basis != "raw":
        raise DataValidationError("only raw prices are supported")
    if cache is not None and not isinstance(cache, DatasetCache):
        raise ValueError("cache must be DatasetCache or None")
    selected = (
        ("parquet" if path.suffix.lower() in {".parquet", ".pq"} else "csv")
        if data_format == "auto"
        else data_format
    )
    try:
        snapshot = path.read_bytes()
    except OSError as error:
        raise DataValidationError(
            f"{path}: cannot read {selected} source: {error}"
        ) from error
    digest = sha256(snapshot).hexdigest()
    DatasetMetadata(symbol=symbol, source_sha256=digest)
    key = (selected, symbol, digest)
    data = None if cache is None else cache._entries.get(key)
    if data is not None:
        assert cache is not None
        cache.hits += 1
        cache._entries.move_to_end(key)
    else:
        if cache is not None:
            cache.misses += 1
        if selected == "csv":
            data = _load_csv_snapshot(
                path, snapshot, symbol=symbol, price_basis=price_basis
            )
        else:
            from distributed_backtester.parquet_data import _load_parquet_snapshot

            data = _load_parquet_snapshot(
                path, snapshot, symbol=symbol, price_basis=price_basis
            )
        if cache is not None:
            cache._entries[key] = data
            while len(cache._entries) > cache.max_entries:
                cache._entries.popitem(last=False)
    if expected_version is not None and data.metadata.version_id != expected_version:
        raise DataValidationError("dataset version does not match expected version")
    return data
