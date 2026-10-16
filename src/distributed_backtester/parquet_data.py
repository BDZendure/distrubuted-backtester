"""Canonical project Parquet: exact int64 micro-USD and verified CSV lineage."""

import importlib
import json
import logging
import os
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import ModuleType
from typing import Any, Literal, cast

from distributed_backtester.data import DataValidationError
from distributed_backtester.dataset_identity import (
    DESCRIPTOR_KEY,
    bars_digest,
    validate_descriptor,
    version_descriptor,
)
from distributed_backtester.jobs import canonical_json
from distributed_backtester.models import Bar, DatasetMetadata, MarketDataset
from distributed_backtester.money import require_integer

_COLUMNS = ("date", "open", "high", "low", "close", "volume")
_INT64_MAX = 2**63 - 1
_LOG = logging.getLogger(__name__)


def _arrow() -> tuple[ModuleType, ModuleType]:
    try:
        return importlib.import_module("pyarrow"), importlib.import_module(
            "pyarrow.parquet"
        )
    except ImportError as error:
        raise RuntimeError(
            'Parquet requires the optional storage extra: pip install ".[storage]"'
        ) from error


def _schema(pa: ModuleType) -> Any:
    return pa.schema(
        [
            pa.field("date", pa.date32(), nullable=False),
            *(pa.field(name, pa.int64(), nullable=False) for name in _COLUMNS[1:]),
        ]
    )


def _metadata(descriptor: dict[str, object], physical_hash: str) -> DatasetMetadata:
    return DatasetMetadata(
        symbol=cast(str, descriptor["symbol"]),
        source_sha256=physical_hash,
        source_format="parquet",
        dataset_version=cast(str, descriptor["dataset_version"]),
        parent_source_sha256=cast(str, descriptor["parent_source_sha256"]),
        bars_sha256=cast(str, descriptor["bars_sha256"]),
    )


def _load_parquet_snapshot(
    path: Path, snapshot: bytes, *, symbol: str, price_basis: Literal["raw"]
) -> MarketDataset:
    pa, pq = _arrow()
    try:
        if price_basis != "raw":
            raise ValueError("only raw prices are supported")
        with pq.ParquetFile(
            pa.BufferReader(snapshot), page_checksum_verification=True
        ) as source:
            schema = source.schema_arrow
            if not schema.equals(_schema(pa), check_metadata=False):
                raise ValueError(
                    "Parquet schema requires nonnullable date32 and int64 columns"
                )
            metadata = schema.metadata
            if metadata is None or set(metadata) != {DESCRIPTOR_KEY}:
                raise ValueError(
                    "Parquet requires the exact project provenance metadata"
                )
            raw = metadata[DESCRIPTOR_KEY]
            descriptor = validate_descriptor(json.loads(raw))
            if raw != canonical_json(descriptor).encode("utf-8"):
                raise ValueError("Parquet descriptor must be canonical JSON")
            if descriptor["symbol"] != symbol:
                raise ValueError("Parquet symbol does not match requested symbol")
            if source.metadata.num_rows != descriptor["row_count"]:
                raise ValueError("Parquet row count does not match descriptor")
            bars: list[Bar] = []
            for batch in source.iter_batches(
                batch_size=65536, columns=list(_COLUMNS), use_threads=False
            ):
                if any(column.null_count for column in batch.columns):
                    raise ValueError("Parquet null fields are not allowed")
                for row in batch.to_pylist():
                    bar = Bar(
                        session=row["date"],
                        open=row["open"],
                        high=row["high"],
                        low=row["low"],
                        close=row["close"],
                        volume=row["volume"],
                    )
                    if bars and bar.session <= bars[-1].session:
                        raise ValueError(
                            "sessions must be unique and strictly increasing"
                        )
                    bars.append(bar)
            if (
                len(bars) != descriptor["row_count"]
                or bars_digest(bars) != descriptor["bars_sha256"]
            ):
                raise ValueError(
                    "Parquet bars do not match recorded content hash/count"
                )
            return MarketDataset(
                tuple(bars), _metadata(descriptor, sha256(snapshot).hexdigest())
            )
    except (pa.ArrowException, ValueError, TypeError, OverflowError, OSError) as error:
        raise DataValidationError(f"{path}: invalid Parquet source: {error}") from error


def load_parquet(
    path: Path, *, symbol: str, price_basis: Literal["raw"]
) -> MarketDataset:
    try:
        snapshot = path.read_bytes()
    except OSError as error:
        raise DataValidationError(
            f"{path}: cannot read Parquet source: {error}"
        ) from error
    return _load_parquet_snapshot(
        path, snapshot, symbol=symbol, price_basis=price_basis
    )


def write_parquet(
    dataset: MarketDataset,
    path: Path,
    *,
    row_group_size: int = 65536,
    overwrite: bool = False,
) -> DatasetMetadata:
    require_integer(row_group_size, "row_group_size", minimum=1)
    if row_group_size > 64 * 1024 * 1024:
        raise ValueError("row_group_size must be <= 67108864")
    if (
        not isinstance(dataset, MarketDataset)
        or dataset.metadata.source_format != "csv"
    ):
        raise ValueError("Parquet conversion requires a validated CSV dataset")
    for bar in dataset.bars:
        if any(getattr(bar, name) > _INT64_MAX for name in _COLUMNS[1:]):
            raise ValueError("Parquet prices and volumes must fit signed int64")
    pa, pq = _arrow()
    descriptor = version_descriptor(dataset.metadata, dataset.bars)
    schema = _schema(pa).with_metadata(
        {DESCRIPTOR_KEY: canonical_json(descriptor).encode()}
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with NamedTemporaryFile(
            dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False
        ) as stream:
            temporary = Path(stream.name)
        with pq.ParquetWriter(
            str(temporary), schema, compression="zstd", write_page_checksum=True
        ) as writer:
            for offset in range(0, len(dataset.bars), row_group_size):
                bars = dataset.bars[offset : offset + row_group_size]
                table = pa.Table.from_arrays(
                    [
                        pa.array([bar.session for bar in bars], type=pa.date32()),
                        *(
                            pa.array(
                                [getattr(bar, name) for bar in bars], type=pa.int64()
                            )
                            for name in _COLUMNS[1:]
                        ),
                    ],
                    schema=schema,
                )
                writer.write_table(table, row_group_size=row_group_size)
        digest = sha256()
        with temporary.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temporary, path)
        else:
            os.link(temporary, path)
        return _metadata(descriptor, digest.hexdigest())
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError as error:
                _LOG.warning(
                    "Temporary Parquet cleanup failed for %s: %s", temporary, error
                )
