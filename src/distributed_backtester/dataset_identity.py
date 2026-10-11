"""Logical bar fingerprints and exact conversion lineage, without Arrow imports."""

import re
from collections.abc import Sequence
from hashlib import sha256

from distributed_backtester.jobs import canonical_json
from distributed_backtester.models import Bar, DatasetMetadata

DESCRIPTOR_KEY = b"distributed_backtester.market.v1"
_FIELDS = {
    "schema_version",
    "symbol",
    "currency",
    "price_basis",
    "validation_version",
    "money_unit",
    "parent_source_sha256",
    "bars_sha256",
    "row_count",
}


def bars_digest(bars: Sequence[Bar]) -> str:
    digest = sha256()
    for bar in bars:
        digest.update(
            (
                canonical_json(
                    [
                        bar.session.isoformat(),
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                    ]
                )
                + "\n"
            ).encode("utf-8")
        )
    return digest.hexdigest()


def version_descriptor(
    metadata: DatasetMetadata, bars: Sequence[Bar]
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "symbol": metadata.symbol,
        "currency": "USD",
        "price_basis": "raw",
        "validation_version": 1,
        "money_unit": "micro-USD",
        "parent_source_sha256": metadata.source_sha256,
        "bars_sha256": bars_digest(bars),
        "row_count": len(bars),
    }
    return {
        **payload,
        "dataset_version": sha256(canonical_json(payload).encode()).hexdigest(),
    }


def validate_descriptor(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != _FIELDS | {"dataset_version"}:
        raise ValueError("invalid Parquet descriptor fields")
    for name in ("schema_version", "validation_version"):
        if type(value[name]) is not int or value[name] != 1:
            raise ValueError(f"unsupported Parquet {name}")
    if type(value["row_count"]) is not int or value["row_count"] < 1:
        raise ValueError("Parquet row_count must be a positive integer")
    if (value["currency"], value["price_basis"], value["money_unit"]) != (
        "USD",
        "raw",
        "micro-USD",
    ):
        raise ValueError(
            "unsupported Parquet currency, adjustment policy, or price unit"
        )
    for name in ("parent_source_sha256", "bars_sha256", "dataset_version"):
        if not isinstance(value[name], str) or not re.fullmatch(
            r"[0-9a-f]{64}", value[name]
        ):
            raise ValueError(f"Parquet {name} must be a lowercase SHA-256 hash")
    DatasetMetadata(symbol=value["symbol"], source_sha256=value["parent_source_sha256"])
    payload = {name: item for name, item in value.items() if name != "dataset_version"}
    if sha256(canonical_json(payload).encode()).hexdigest() != value["dataset_version"]:
        raise ValueError("Parquet dataset version does not match descriptor")
    return payload | {"dataset_version": value["dataset_version"]}
