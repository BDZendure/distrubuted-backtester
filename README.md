# Distributed Backtesting Engine

A portfolio engineering project for reproducible strategy simulations, local CPU
parallelism, and eventually distributed execution. The focus is correct event
timing, exact accounting, explicit failure behavior, and measured performance.

**Current stage: Phase 0 foundation.** The repository contains architecture,
development tooling, an installable package, and a packaging smoke test. There is
no simulation engine or backtest CLI yet. Phase 1 requires explicit approval.

## Start with the explanations

- [Clear explanation for beginners](docs/clear%20explanation.md): what a backtest
  does, how the pieces fit, and why correctness comes first.
- [Technical explanation](docs/technical-explanation.md): event timing, exact
  accounting, data contracts, backend equivalence, and recovery tradeoffs.
- [Architecture](docs/architecture.md): full system design, proposed interfaces,
  directory structure, and engineering risks.
- [Roadmap and phase checkpoints](docs/roadmap.md): acceptance and validation for
  all nine phases.
- [Architecture decisions](docs/decisions/README.md), [testing strategy](docs/testing.md),
  [benchmark methodology](docs/benchmarking.md), and [dependency plan](docs/dependencies.md).
- [Phase 0 verification report](docs/phase-0-report.md).

## Development setup

Requires Python 3.12 or newer. From the repository directory:

```bash
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --editable ".[dev]"
```

The virtual environment isolates project tools. Editable installation makes
changes under `src/` available through the installed package. The `dev` extra
installs pytest, Ruff, mypy, and build; there are no runtime dependencies.

## Run the current checks

```bash
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy
python -m pip check
python -m build
```

Check the built wheel in a fresh environment:

```bash
python -m venv .venv-wheel
.venv-wheel/bin/python -m pip install --no-deps dist/*.whl
wheel_python="$PWD/.venv-wheel/bin/python"
(cd /tmp && "$wheel_python" -I -c "import distributed_backtester")
```

These shell commands target macOS/Linux. GitHub Actions is configured to run the
checks on Linux with Python 3.12 and 3.14. The [phase report](docs/phase-0-report.md)
records what was actually executed locally and what remains unobserved remotely.

## Planned simulation model

Phase 1 will support one asset, whole shares, long-only holdings, and validated
daily OHLCV CSV data. A strategy sees information through session T's close.
Its target intent executes at the next supplied session's open, subject to cash
and configured fees. Equity is marked at the close. Price and portfolio accounting
will use exact fixed units; market-data policy and limitations are explicit.

Later phases add parameter sweeps, multiprocessing, Ray, durable retries and
idempotent result commits, Parquet/DuckDB, profiling, and operational tooling.
These capabilities are plans, not existing functionality or performance claims.

The initial execution model has no market impact, volume-based liquidity limits,
intraday execution, dividends, splits, leverage, short selling, or fractional
shares. Synthetic data establishes correctness. An incomplete real-data universe
cannot establish freedom from survivorship bias or investment performance.

## Development workflow

Keep phase changes focused on feature branches, verify before committing, and
update docs alongside implementation. Complete one phase, report evidence, and
wait for explicit approval before the next. Remote pushes require authorization.

Licensed under the existing [MIT license](LICENSE).
