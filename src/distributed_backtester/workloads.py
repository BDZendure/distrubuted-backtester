"""Deterministic synthetic data and SMA configurations for Phase 2 benchmarks."""

import csv
import random
from datetime import date, timedelta
from pathlib import Path

from distributed_backtester.models import BacktestConfig, StrategyConfig
from distributed_backtester.money import MICRO_DOLLARS, format_money, require_integer

_START_DAY = date(2000, 1, 1)
_BASE_PRICE = 100 * MICRO_DOLLARS


def write_synthetic_csv(path: Path, *, bars: int, seed: int = 1729) -> None:
    """Create an exact USD CSV using a local RNG and a bounded price cycle.

    Dates are consecutive synthetic days starting at 2000-01-01, including
    weekends, without exchange calendar semantics. The final day must fit in
    Python's date range. Prices use integer micro-dollars, a $100 base, a $1
    floor, and a 128-day rise/fall cycle with seeded daily variation to produce
    SMA crossings. Longer requests preserve the same seed's earlier rows.

    Arguments are validated before creating parents. Exclusive creation refuses
    existing files and symlinks without altering them. Output is ASCII UTF-8
    with LF line endings and six decimal places for every price.
    """
    require_integer(bars, "bars", minimum=1)
    require_integer(seed, "seed")
    if bars > (date.max - _START_DAY).days + 1:
        raise ValueError("bars exceeds the supported synthetic date range")

    generator = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(("date", "open", "high", "low", "close", "volume"))
        previous_close = _BASE_PRICE
        for index in range(bars):
            cycle_day = index % 128
            trend = min(cycle_day, 128 - cycle_day) * 500_000
            open_price = previous_close
            close = max(
                MICRO_DOLLARS,
                _BASE_PRICE + trend + generator.randint(-250_000, 250_000),
            )
            high = max(open_price, close) + generator.randint(0, 750_000)
            low = max(
                MICRO_DOLLARS,
                min(open_price, close) - generator.randint(0, 750_000),
            )
            writer.writerow(
                (
                    (_START_DAY + timedelta(days=index)).isoformat(),
                    format_money(open_price),
                    format_money(high),
                    format_money(low),
                    format_money(close),
                    generator.randint(0, 1_000_000),
                )
            )
            previous_close = close


def job_configs(
    count: int, *, initial_cash: int = 1_000_000_000, fee_bps: int = 10
) -> tuple[BacktestConfig, ...]:
    """Return distinct, valid SMA jobs whose prefixes are independent of count.

    For zero-based index i, fast = 1 + (i % 16), slow = fast + 1 +
    ((i // 16) % 64), and cash = initial_cash + (i // 1024) * 1_000_000.
    The 1024 bounded window pairs repeat with one additional dollar of cash
    per batch, so configurations stay distinct for every positive count.
    Cash is in integer micro-dollars; every configuration has the same fee_bps.
    BacktestConfig validates the supplied cash and fees before cash arithmetic.
    """
    require_integer(count, "count", minimum=1)
    base = BacktestConfig(initial_cash=initial_cash, fee_bps=fee_bps)
    configs: list[BacktestConfig] = []
    for index in range(count):
        fast = 1 + index % 16
        configs.append(
            BacktestConfig(
                strategy=StrategyConfig(
                    fast_window=fast, slow_window=fast + 1 + (index // 16) % 64
                ),
                initial_cash=base.initial_cash + (index // 1024) * MICRO_DOLLARS,
                fee_bps=base.fee_bps,
            )
        )
    return tuple(configs)
