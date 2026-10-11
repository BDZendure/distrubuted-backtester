"""Reproducible synthetic bars and distinct, bounded benchmark configurations."""

import csv
import random
import re
from datetime import date, timedelta
from hashlib import sha256
from io import StringIO
from pathlib import Path
from typing import cast

import pytest

from distributed_backtester import workloads
from distributed_backtester.data import load_csv
from distributed_backtester.models import (
    BacktestConfig,
    CloseHistory,
    CloseObservation,
    Portfolio,
    StrategyConfig,
)
from distributed_backtester.money import MICRO_DOLLARS, parse_money
from distributed_backtester.strategy import MovingAverageCrossover


class IntSubclass(int):
    """An integer subclass is outside the exact integer input contract."""


def test_same_seed_produces_identical_bytes(tmp_path: Path) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    workloads.write_synthetic_csv(first, bars=365)
    workloads.write_synthetic_csv(second, bars=365, seed=1729)

    assert first.read_bytes() == second.read_bytes()


@pytest.mark.parametrize("seed", [0, 1, 1730])
def test_changed_seed_changes_source_digest(tmp_path: Path, seed: int) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    workloads.write_synthetic_csv(first, bars=365)
    workloads.write_synthetic_csv(second, bars=365, seed=seed)

    assert sha256(first.read_bytes()).digest() != sha256(second.read_bytes()).digest()


@pytest.mark.parametrize("bars", [1, 2, 365, 2048])
def test_csv_loads_every_row_with_valid_daily_bars(tmp_path: Path, bars: int) -> None:
    path = tmp_path / "nested" / "parents" / "prices.csv"
    workloads.write_synthetic_csv(path, bars=bars)
    snapshot = path.read_bytes()
    dataset = load_csv(path, symbol="SYNTH", price_basis="raw")

    assert len(dataset.bars) == bars
    assert dataset.metadata.source_sha256 == sha256(snapshot).hexdigest()
    assert dataset.bars[0].open == 100 * MICRO_DOLLARS
    for offset, bar in enumerate(dataset.bars):
        assert bar.session == date(2000, 1, 1) + timedelta(days=offset)
        assert MICRO_DOLLARS <= bar.low <= min(bar.open, bar.close)
        assert max(bar.open, bar.close) <= bar.high
        assert bar.volume >= 0
    if bars >= 7:
        assert {bar.session.weekday() for bar in dataset.bars} == set(range(7))


def test_csv_has_ascii_utf8_header_and_exact_money_precision(tmp_path: Path) -> None:
    path = tmp_path / "prices.csv"
    workloads.write_synthetic_csv(path, bars=365)
    snapshot = path.read_bytes()
    text = snapshot.decode("utf-8")
    rows = list(csv.reader(StringIO(text)))
    dataset = load_csv(path, symbol="SYNTH", price_basis="raw")

    assert snapshot.isascii()
    assert snapshot.startswith(b"date,open,high,low,close,volume\n")
    assert b"\r" not in snapshot
    assert snapshot.endswith(b"\n")
    assert len(rows) == 366
    for row, bar in zip(rows[1:], dataset.bars, strict=True):
        assert row[0] == bar.session.isoformat()
        for price, units in zip(
            row[1:5], (bar.open, bar.high, bar.low, bar.close), strict=True
        ):
            assert re.fullmatch(r"[0-9]+(?:\.[0-9]{6})?", price)
            assert parse_money(price) == units
        assert re.fullmatch(r"[0-9]+", row[5])
        assert int(row[5]) == bar.volume
    assert len({bar.close for bar in dataset.bars}) > 100
    assert any(bar.close % MICRO_DOLLARS for bar in dataset.bars)
    assert all(bar.high < 200 * MICRO_DOLLARS for bar in dataset.bars)


def test_bar_prefix_does_not_depend_on_requested_length(tmp_path: Path) -> None:
    short = tmp_path / "short.csv"
    long = tmp_path / "long.csv"
    workloads.write_synthetic_csv(short, bars=257, seed=0)
    workloads.write_synthetic_csv(long, bars=513, seed=0)

    assert long.read_bytes().startswith(short.read_bytes())


def test_generator_preserves_global_random_state(tmp_path: Path) -> None:
    before = random.getstate()
    workloads.write_synthetic_csv(tmp_path / "prices.csv", bars=128, seed=0)

    assert random.getstate() == before


@pytest.mark.parametrize("original", [b"", b"keep these bytes\x00\xff\n"])
def test_existing_output_is_never_overwritten(tmp_path: Path, original: bytes) -> None:
    path = tmp_path / "prices.csv"
    path.write_bytes(original)

    with pytest.raises(FileExistsError):
        workloads.write_synthetic_csv(path, bars=365)

    assert path.read_bytes() == original


@pytest.mark.parametrize("target_exists", [False, True])
def test_existing_symlink_is_protected(tmp_path: Path, target_exists: bool) -> None:
    target = tmp_path / "target.csv"
    if target_exists:
        target.write_bytes(b"keep this target\n")
    path = tmp_path / "prices.csv"
    path.symlink_to(target)

    with pytest.raises(FileExistsError):
        workloads.write_synthetic_csv(path, bars=10)

    assert path.is_symlink()
    assert target.exists() == target_exists
    if target_exists:
        assert target.read_bytes() == b"keep this target\n"


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("bars", 0),
        ("bars", -1),
        ("bars", True),
        ("bars", False),
        ("bars", 1.0),
        ("bars", "1"),
        ("bars", None),
        ("bars", IntSubclass(1)),
        ("bars", (date.max - date(2000, 1, 1)).days + 2),
        ("bars", 10**30),
        ("seed", -1),
        ("seed", True),
        ("seed", False),
        ("seed", 1729.0),
        ("seed", "1729"),
        ("seed", None),
        ("seed", IntSubclass(1729)),
    ],
)
@pytest.mark.parametrize("existing", [False, True])
def test_invalid_csv_arguments_have_no_filesystem_side_effects(
    tmp_path: Path, field: str, bad: object, existing: bool
) -> None:
    path = tmp_path / "nested" / "prices.csv"
    if existing:
        path.parent.mkdir()
        path.write_bytes(b"protected output\n")
    bars = cast(int, bad) if field == "bars" else 10
    seed = cast(int, bad) if field == "seed" else 1729

    with pytest.raises(ValueError, match=field):
        workloads.write_synthetic_csv(path, bars=bars, seed=seed)

    if existing:
        assert path.read_bytes() == b"protected output\n"
    else:
        assert not path.exists()
        assert not path.parent.exists()


@pytest.mark.parametrize(("fast", "slow"), [(1, 2), (2, 3), (8, 32), (16, 80)])
def test_price_trajectory_produces_repeated_sma_crossings(
    tmp_path: Path, fast: int, slow: int
) -> None:
    path = tmp_path / "prices.csv"
    workloads.write_synthetic_csv(path, bars=512)
    dataset = load_csv(path, symbol="SYNTH", price_basis="raw")
    strategy = MovingAverageCrossover(StrategyConfig(fast, slow))
    observations: list[CloseObservation] = []
    portfolio = Portfolio(cash=1_000_000_000, holdings=0)
    targets: list[str] = []
    for bar in dataset.bars:
        observations.append(CloseObservation(bar.session, bar.close))
        intent = strategy.on_close(CloseHistory(observations), portfolio)
        if intent is not None:
            targets.append(intent.target)

    assert targets.count("invested") >= 3
    assert targets.count("flat") >= 3


def test_job_configs_default_to_documented_cash_and_fees() -> None:
    assert workloads.job_configs(1) == (
        BacktestConfig(
            strategy=StrategyConfig(fast_window=1, slow_window=2),
            initial_cash=1_000_000_000,
            fee_bps=10,
        ),
    )


@pytest.mark.parametrize("count", [1, 16, 1024, 1100, 2049])
def test_job_configs_are_distinct_with_bounded_windows(count: int) -> None:
    configs = workloads.job_configs(count)

    assert type(configs) is tuple
    assert len(configs) == count
    assert len(set(configs)) == count
    for config in configs:
        assert isinstance(config, BacktestConfig)
        assert 1 <= config.strategy.fast_window <= 16
        assert 1 <= config.strategy.slow_window - config.strategy.fast_window <= 64
        assert config.initial_cash > 0
        assert config.fee_bps == 10


@pytest.mark.parametrize(
    ("initial_cash", "fee_bps"), [(1, 0), (999_999_999, 25), (10**30, 10_000)]
)
def test_job_configs_follow_exact_index_formula(
    initial_cash: int, fee_bps: int
) -> None:
    configs = workloads.job_configs(2049, initial_cash=initial_cash, fee_bps=fee_bps)

    for index, config in enumerate(configs):
        fast = 1 + index % 16
        assert config == BacktestConfig(
            strategy=StrategyConfig(fast, fast + 1 + (index // 16) % 64),
            initial_cash=initial_cash + (index // 1024) * 1_000_000,
            fee_bps=fee_bps,
        )
    assert len(set(configs)) == 2049


def test_job_config_prefixes_are_repeatable_across_cash_batches() -> None:
    configs = workloads.job_configs(1100, initial_cash=123_456_789, fee_bps=17)

    assert configs == workloads.job_configs(1100, initial_cash=123_456_789, fee_bps=17)
    for count in (1, 16, 1024, 1025):
        assert configs[:count] == workloads.job_configs(
            count, initial_cash=123_456_789, fee_bps=17
        )


@pytest.mark.parametrize("bad", [0, -1, True, False, 1.0, "1", None, IntSubclass(1)])
def test_job_configs_reject_invalid_count(bad: object) -> None:
    with pytest.raises(ValueError, match="count"):
        workloads.job_configs(cast(int, bad))


@pytest.mark.parametrize("bad", [0, -1, True, False, 1.0, "1", None, IntSubclass(1)])
def test_job_configs_validate_initial_cash_before_arithmetic(bad: object) -> None:
    with pytest.raises(ValueError, match="initial_cash"):
        workloads.job_configs(1100, initial_cash=cast(int, bad))


@pytest.mark.parametrize(
    "bad", [-1, 10_001, True, False, 1.0, "10", None, IntSubclass(10)]
)
def test_job_configs_validate_fee_basis_points(bad: object) -> None:
    with pytest.raises(ValueError, match="fee_bps"):
        workloads.job_configs(1100, fee_bps=cast(int, bad))
