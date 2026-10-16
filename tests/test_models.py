"""Contracts prevent invalid input before it reaches the simulation."""

from collections.abc import Sequence
from dataclasses import FrozenInstanceError
from datetime import date, datetime
from typing import Literal, SupportsIndex, cast, overload

import pytest

from distributed_backtester.models import (
    BacktestConfig,
    Bar,
    CloseHistory,
    CloseObservation,
    DatasetMetadata,
    MarketDataset,
    Portfolio,
    StrategyConfig,
)


def valid_bar(session: date = date(2026, 1, 2)) -> Bar:
    return Bar(session, 10_000_000, 12_000_000, 9_000_000, 11_000_000, 100)


def metadata() -> DatasetMetadata:
    return DatasetMetadata(symbol="SYNTH", source_sha256="a" * 64)


def test_records_are_frozen() -> None:
    changes = (
        (valid_bar(), "close", 9_000_000),
        (Portfolio(cash=10_000_000, holdings=1), "cash", -1),
    )
    for record, attribute, value in changes:
        with pytest.raises(FrozenInstanceError):
            setattr(record, attribute, value)


@pytest.mark.parametrize("bad", [0, -1, True, 1.5])
def test_bar_rejects_invalid_prices(bad: object) -> None:
    with pytest.raises(ValueError, match="open"):
        Bar(date(2026, 1, 2), cast(int, bad), 12_000_000, 1, 10_000_000, 100)


def test_bar_rejects_invalid_ranges_and_timestamp_types() -> None:
    with pytest.raises(ValueError, match="OHLC"):
        Bar(date(2026, 1, 2), 10, 9, 8, 10, 0)
    with pytest.raises(ValueError, match="session"):
        Bar(datetime(2026, 1, 2), 10, 12, 9, 11, 0)


@pytest.mark.parametrize("bad", [-1, 1.5, True])
def test_volume_must_be_a_nonnegative_integer(bad: object) -> None:
    with pytest.raises(ValueError, match="volume"):
        Bar(date(2026, 1, 2), 10, 12, 9, 11, cast(int, bad))


def test_dataset_requires_nonempty_immutable_chronological_bars() -> None:
    with pytest.raises(ValueError, match="empty"):
        MarketDataset(bars=(), metadata=metadata())
    with pytest.raises(ValueError, match="tuple"):
        MarketDataset(bars=cast(tuple[Bar, ...], [valid_bar()]), metadata=metadata())
    with pytest.raises(ValueError, match="increasing"):
        MarketDataset(bars=(valid_bar(), valid_bar()), metadata=metadata())
    with pytest.raises(ValueError, match="increasing"):
        MarketDataset(
            bars=(valid_bar(date(2026, 1, 5)), valid_bar()), metadata=metadata()
        )


@pytest.mark.parametrize("symbol", ["", " ", " SYNTH"])
def test_metadata_rejects_bad_symbol(symbol: str) -> None:
    with pytest.raises(ValueError, match="symbol"):
        DatasetMetadata(symbol=symbol, source_sha256="a" * 64)


def test_metadata_requires_digest_and_supported_price_policy() -> None:
    with pytest.raises(ValueError, match="SHA"):
        DatasetMetadata(symbol="SYNTH", source_sha256="not-a-digest")
    with pytest.raises(ValueError, match="price_basis"):
        DatasetMetadata(
            symbol="SYNTH",
            source_sha256="a" * 64,
            price_basis=cast(Literal["raw"], "adjusted"),
        )


@pytest.mark.parametrize(("fast", "slow"), [(0, 3), (3, 3), (4, 3), (True, 3)])
def test_strategy_windows_are_validated(fast: int, slow: int) -> None:
    with pytest.raises(ValueError, match="window"):
        StrategyConfig(fast_window=fast, slow_window=slow)


@pytest.mark.parametrize("bad", [0, -1, True])
def test_initial_cash_must_be_positive_integer_units(bad: int) -> None:
    with pytest.raises(ValueError, match="initial_cash"):
        BacktestConfig(initial_cash=bad)


@pytest.mark.parametrize("bad", [-1, 10_001, True])
def test_fee_basis_points_are_bounded(bad: int) -> None:
    with pytest.raises(ValueError, match="fee_bps"):
        BacktestConfig(fee_bps=bad)


def test_portfolio_rejects_negative_cash_and_holdings() -> None:
    with pytest.raises(ValueError, match="cash"):
        Portfolio(cash=-1, holdings=0)
    with pytest.raises(ValueError, match="holdings"):
        Portfolio(cash=0, holdings=-1)


def test_close_history_is_a_read_only_prefix_with_stable_bounds() -> None:
    observed = [CloseObservation(date(2026, 1, 2), 10_000_000)]
    prefix = CloseHistory(observed)
    observed.append(CloseObservation(date(2026, 1, 5), 12_000_000))
    assert len(prefix) == 1
    assert prefix[-1].close == 10_000_000
    assert prefix[:] == (observed[0],)
    with pytest.raises(IndexError):
        _ = prefix[1]
    with pytest.raises(IndexError):
        _ = prefix[-2]


@pytest.mark.parametrize("tuple_backing", [False, True])
def test_history_slices_avoid_recursive_integer_bound_checks(
    tuple_backing: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = CloseHistory.__getitem__
    integer_reads = 0

    def counted(
        self: CloseHistory, index: int | slice
    ) -> CloseObservation | tuple[CloseObservation, ...]:
        nonlocal integer_reads
        if isinstance(index, int):
            integer_reads += 1
        return original(self, index)

    monkeypatch.setattr(CloseHistory, "__getitem__", counted)
    observations = [CloseObservation(date(2026, 1, 1), i + 1) for i in range(8)]
    view = CloseHistory(tuple(observations) if tuple_backing else observations)
    assert view[1:7:2] == tuple(observations[1:7:2])
    assert integer_reads == 0


@pytest.mark.parametrize("length", range(9))
@pytest.mark.parametrize("tuple_backing", [False, True])
def test_bounded_slices_match_snapshot_after_future_append(
    length: int, tuple_backing: bool
) -> None:
    observations = [CloseObservation(date(2026, 1, 1), i + 1) for i in range(12)]
    backing: Sequence[CloseObservation] = (
        tuple(observations[:length]) if tuple_backing else observations[:length]
    )
    view = CloseHistory(backing)
    expected = tuple(observations[:length])
    if isinstance(backing, list):
        backing.extend(observations[length:])
    bounds = (None, -1000, -12, -9, -2, -1, 0, 1, 2, 8, 9, 12, 1000)
    for start in bounds:
        for stop in bounds:
            for step in (None, -100, -3, -1, 1, 2, 100):
                selection = slice(start, stop, step)
                assert view[selection] == expected[selection], (length, selection)
    with pytest.raises(ValueError):
        _ = view[::0]


def test_history_preserves_custom_list_integer_access() -> None:
    class IntegerOnly(list[CloseObservation]):
        @overload
        def __getitem__(self, index: SupportsIndex) -> CloseObservation: ...

        @overload
        def __getitem__(self, index: slice) -> list[CloseObservation]: ...

        def __getitem__(
            self, index: SupportsIndex | slice
        ) -> CloseObservation | list[CloseObservation]:
            if isinstance(index, slice):
                raise AssertionError("custom backing must never receive a slice")
            return super().__getitem__(index)

    backing = IntegerOnly([CloseObservation(date(2026, 1, 1), i + 1) for i in range(5)])
    view = CloseHistory(backing)
    backing.append(CloseObservation(date(2026, 1, 2), 999))
    assert view[::-1] == tuple(backing[i] for i in range(4, -1, -1))
    assert view[2:4] == (backing[2], backing[3])


def test_history_preserves_generic_sequence_access() -> None:
    from collections import UserList

    backing = UserList([CloseObservation(date(2026, 1, 1), i + 1) for i in range(5)])
    view = CloseHistory(backing)
    backing.append(CloseObservation(date(2026, 1, 2), 999))
    assert view[::-1] == tuple(backing[i] for i in range(4, -1, -1))


def test_history_subclass_keeps_overridden_integer_access() -> None:
    class AlteredHistory(CloseHistory):
        @overload
        def __getitem__(self, index: int) -> CloseObservation: ...

        @overload
        def __getitem__(self, index: slice) -> tuple[CloseObservation, ...]: ...

        def __getitem__(
            self, index: int | slice
        ) -> CloseObservation | tuple[CloseObservation, ...]:
            if isinstance(index, int):
                observation = super().__getitem__(index)
                return CloseObservation(observation.session, observation.close + 1)
            return super().__getitem__(index)

    backing = [CloseObservation(date(2026, 1, 1), i + 1) for i in range(5)]
    view = AlteredHistory(backing)
    assert view[1:4] == tuple(view[i] for i in range(1, 4))
