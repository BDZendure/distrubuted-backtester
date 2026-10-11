"""Canonical immutable job specifications and parameter-grid membership."""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import cast

from distributed_backtester.models import (
    BacktestConfig,
    DatasetMetadata,
    StrategyConfig,
)
from distributed_backtester.provenance import ImplementationIdentity
from distributed_backtester.simulation import SMA_ID


def canonical_json(record: object) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)


def config_record(config: BacktestConfig) -> dict[str, object]:
    return {
        "strategy_id": SMA_ID,
        "fast_window": config.strategy.fast_window,
        "slow_window": config.strategy.slow_window,
        "initial_cash": config.initial_cash,
        "money_unit": "micro-USD",
        "fee_bps": config.fee_bps,
    }


def metadata_record(metadata: DatasetMetadata) -> dict[str, object]:
    return {
        "symbol": metadata.symbol,
        "source_sha256": metadata.source_sha256,
        "currency": metadata.currency,
        "price_basis": metadata.price_basis,
        "validation_version": metadata.validation_version,
    }


def _payload(
    config: BacktestConfig,
    dataset: DatasetMetadata,
    implementation: ImplementationIdentity,
    schema_version: int = 1,
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "config": config_record(config),
        "dataset": metadata_record(dataset),
        "implementation": {
            "engine_id": implementation.engine_id,
            "source_sha256": implementation.source_sha256,
        },
    }


def _job_id(payload: dict[str, object]) -> str:
    return sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def _record(value: object, keys: set[str], name: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError(f"{name} must contain exactly {sorted(keys)}")
    return cast(dict[str, object], value)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be text")
    return value


def _integer(value: object, name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    return value


@dataclass(frozen=True, slots=True)
class JobSpec:
    job_id: str
    config: BacktestConfig
    dataset: DatasetMetadata
    implementation: ImplementationIdentity
    schema_version: int = 1

    def __post_init__(self) -> None:
        if (
            not isinstance(self.config, BacktestConfig)
            or not isinstance(self.dataset, DatasetMetadata)
            or not isinstance(self.implementation, ImplementationIdentity)
        ):
            raise ValueError("job inputs must use validated domain records")
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("job schema_version must be 1")
        expected = _job_id(
            _payload(
                self.config, self.dataset, self.implementation, self.schema_version
            )
        )
        if self.job_id != expected:
            raise ValueError("job_id does not match canonical job inputs")

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            **_payload(
                self.config, self.dataset, self.implementation, self.schema_version
            ),
        }

    @classmethod
    def from_dict(cls, value: object) -> "JobSpec":
        record = _record(
            value,
            {"job_id", "config", "dataset", "implementation", "schema_version"},
            "job",
        )
        config = _record(
            record["config"],
            {
                "strategy_id",
                "fast_window",
                "slow_window",
                "initial_cash",
                "fee_bps",
                "money_unit",
            },
            "config",
        )
        if config["strategy_id"] != SMA_ID or config["money_unit"] != "micro-USD":
            raise ValueError("unsupported strategy or money unit")
        metadata = _record(
            record["dataset"],
            {
                "symbol",
                "source_sha256",
                "currency",
                "price_basis",
                "validation_version",
            },
            "dataset",
        )
        if metadata["currency"] != "USD" or metadata["price_basis"] != "raw":
            raise ValueError("unsupported currency or price basis")
        implementation = _record(
            record["implementation"], {"engine_id", "source_sha256"}, "implementation"
        )
        return cls(
            job_id=_text(record["job_id"], "job_id"),
            config=BacktestConfig(
                strategy=StrategyConfig(
                    _integer(config["fast_window"], "fast_window"),
                    _integer(config["slow_window"], "slow_window"),
                ),
                initial_cash=_integer(config["initial_cash"], "initial_cash"),
                fee_bps=_integer(config["fee_bps"], "fee_bps"),
            ),
            dataset=DatasetMetadata(
                symbol=_text(metadata["symbol"], "symbol"),
                source_sha256=_text(metadata["source_sha256"], "source_sha256"),
                validation_version=_integer(
                    metadata["validation_version"], "validation_version"
                ),
            ),
            implementation=ImplementationIdentity(
                _text(implementation["engine_id"], "engine_id"),
                _text(implementation["source_sha256"], "source_sha256"),
            ),
            schema_version=_integer(record["schema_version"], "schema_version"),
        )


def create_job(
    config: BacktestConfig,
    metadata: DatasetMetadata,
    implementation: ImplementationIdentity,
) -> JobSpec:
    return JobSpec(
        _job_id(_payload(config, metadata, implementation)),
        config,
        metadata,
        implementation,
    )


def expand_grid(
    fast: Sequence[int],
    slow: Sequence[int],
    initial_cash: int,
    fee_bps: int,
    metadata: DatasetMetadata,
    implementation: ImplementationIdentity,
) -> tuple[tuple[JobSpec, ...], tuple[str, ...]]:
    """Reject invalid grid pairs, preserving requested duplicate membership."""
    if not fast or not slow:
        raise ValueError("fast and slow grids must be nonempty")
    unique: dict[str, JobSpec] = {}
    membership: list[str] = []
    for fast_window in fast:
        for slow_window in slow:
            config = BacktestConfig(
                StrategyConfig(fast_window, slow_window), initial_cash, fee_bps
            )
            job = create_job(config, metadata, implementation)
            unique[job.job_id] = job
            membership.append(job.job_id)
    return tuple(unique[key] for key in sorted(unique)), tuple(membership)
