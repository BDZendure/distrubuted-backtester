"""Immutable simulation inputs/results and a read-only observed-history view."""

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date
from fractions import Fraction
from itertools import pairwise
from typing import Literal, overload

from distributed_backtester.money import MICRO_DOLLARS, require_integer


def _require_session(session: date) -> None:
    if type(session) is not date:
        raise ValueError("session must be a date, not a timestamp")


@dataclass(frozen=True, slots=True)
class Bar:
    session: date
    open: int
    high: int
    low: int
    close: int
    volume: int

    def __post_init__(self) -> None:
        _require_session(self.session)
        for name in ("open", "high", "low", "close"):
            require_integer(getattr(self, name), name, minimum=1)
        require_integer(self.volume, "volume")
        if (
            not self.low <= self.open <= self.high
            or not self.low <= self.close <= self.high
        ):
            raise ValueError("OHLC range must contain open and close")


@dataclass(frozen=True, slots=True)
class DatasetMetadata:
    symbol: str
    source_sha256: str
    currency: Literal["USD"] = "USD"
    price_basis: Literal["raw"] = "raw"
    validation_version: int = 1

    def __post_init__(self) -> None:
        if (
            not isinstance(self.symbol, str)
            or not self.symbol
            or self.symbol.strip() != self.symbol
        ):
            raise ValueError("symbol must be nonempty with no surrounding whitespace")
        if not isinstance(self.source_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}", self.source_sha256
        ):
            raise ValueError("source_sha256 must be a lowercase SHA-256 digest")
        if self.currency != "USD":
            raise ValueError("currency must be USD")
        if self.price_basis != "raw":
            raise ValueError("price_basis must be raw")
        if type(self.validation_version) is not int or self.validation_version != 1:
            raise ValueError("validation_version must be 1")


@dataclass(frozen=True, slots=True)
class MarketDataset:
    bars: tuple[Bar, ...]
    metadata: DatasetMetadata

    def __post_init__(self) -> None:
        if type(self.bars) is not tuple:
            raise ValueError("bars must be an immutable tuple")
        if not self.bars:
            raise ValueError("dataset cannot be empty")
        if not isinstance(self.metadata, DatasetMetadata):
            raise ValueError("metadata must be DatasetMetadata")
        if any(not isinstance(bar, Bar) for bar in self.bars):
            raise ValueError("bars must contain Bar records")
        if any(left.session >= right.session for left, right in pairwise(self.bars)):
            raise ValueError("sessions must be unique and strictly increasing")


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    fast_window: int = 2
    slow_window: int = 3

    def __post_init__(self) -> None:
        require_integer(self.fast_window, "fast_window", minimum=1)
        require_integer(self.slow_window, "slow_window", minimum=1)
        if self.fast_window >= self.slow_window:
            raise ValueError("fast_window must be smaller than slow_window")


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    initial_cash: int = 1000 * MICRO_DOLLARS
    fee_bps: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.strategy, StrategyConfig):
            raise ValueError("strategy must be StrategyConfig")
        require_integer(self.initial_cash, "initial_cash", minimum=1)
        require_integer(self.fee_bps, "fee_bps")
        if self.fee_bps > 10_000:
            raise ValueError("fee_bps must be <= 10000")


@dataclass(frozen=True, slots=True)
class Portfolio:
    cash: int
    holdings: int

    def __post_init__(self) -> None:
        require_integer(self.cash, "cash")
        require_integer(self.holdings, "holdings")


@dataclass(frozen=True, slots=True)
class CloseObservation:
    session: date
    close: int

    def __post_init__(self) -> None:
        _require_session(self.session)
        require_integer(self.close, "close", minimum=1)


class CloseHistory(Sequence[CloseObservation]):
    """Read-only prefix bounds over the engine's append-only observed closes.

    Appending observations never extends an earlier view. Strategies have no
    supported API for reading future bars or mutating observations. Python is
    not a security sandbox for hostile introspection of private attributes.
    """

    __slots__ = ("_length", "_observations")

    def __init__(self, observations: Sequence[CloseObservation]) -> None:
        self._observations = observations
        self._length = len(observations)

    def __len__(self) -> int:
        return self._length

    @overload
    def __getitem__(self, index: int) -> CloseObservation: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[CloseObservation, ...]: ...

    def __getitem__(
        self, index: int | slice
    ) -> CloseObservation | tuple[CloseObservation, ...]:
        if isinstance(index, slice):
            return tuple(self[i] for i in range(*index.indices(self._length)))
        position = index if index >= 0 else self._length + index
        if not 0 <= position < self._length:
            raise IndexError("close observation outside available history")
        return self._observations[position]


@dataclass(frozen=True, slots=True)
class TargetIntent:
    """Order boundary; execution validates the target, timing, and sequence."""

    target: str
    decision_session: date
    sequence: int


@dataclass(frozen=True, slots=True)
class Fill:
    sequence: int
    decision_session: date
    execution_session: date
    side: Literal["buy", "sell"]
    quantity: int
    price: int
    fee: int
    cash_after: int
    holdings_after: int


@dataclass(frozen=True, slots=True)
class OrderOutcome:
    intent: TargetIntent
    status: Literal["filled", "no_change", "rejected", "canceled"]
    execution_session: date | None
    reason: str | None = None
    fill: Fill | None = None


@dataclass(frozen=True, slots=True)
class EquityPoint:
    session: date
    cash: int
    holdings: int
    close: int
    equity: int


@dataclass(frozen=True, slots=True)
class Metrics:
    total_return: Fraction
    maximum_drawdown: Fraction
    final_equity: int
    fill_count: int
    total_fees: int


@dataclass(frozen=True, slots=True)
class BacktestResult:
    config: BacktestConfig
    dataset_metadata: DatasetMetadata
    engine_id: str
    fills: tuple[Fill, ...]
    order_outcomes: tuple[OrderOutcome, ...]
    equity: tuple[EquityPoint, ...]
    final_portfolio: Portfolio
    metrics: Metrics
    strategy_id: str
    schema_version: int = 1
